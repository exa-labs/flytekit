import tempfile
from unittest.mock import MagicMock, patch

import pytest

from flytekit import ImageSpec, task, workflow
from flytekit.configuration import ImageConfig, SerializationSettings
from flytekit.constants import CopyFileDetection
from flytekit.core.context_manager import FlyteContextManager, FlyteEntities
from flytekit.remote import FlyteRemote
from flytekit.tools import repo
from flytekit.tools.repo import serialize_load_only


@pytest.mark.parametrize("image_spec_packages", [["package1"], ["package2"]])
def test_register_includes_imagespec_in_version(image_spec_packages):
    """Test that changing ImageSpec packages results in different versions."""
    
    # Create a task with ImageSpec
    image_spec = ImageSpec(
        name="test",
        packages=image_spec_packages,
        registry="test.io",
    )
    
    @task(container_image=image_spec)
    def my_task() -> str:
        return "hello"
    
    @workflow
    def my_workflow() -> str:
        return my_task()
    
    # Mock the remote and file operations
    mock_remote = MagicMock(spec=FlyteRemote)
    mock_remote._get_image_names = FlyteRemote._get_image_names.__get__(mock_remote)
    mock_remote._version_from_hash = FlyteRemote._version_from_hash
    
    # Mock fast_package to return consistent hash
    with patch("flytekit.tools.repo.find_common_root") as mock_find_root, \
         patch("flytekit.tools.repo.list_packages_and_modules") as mock_list_pkgs, \
         patch("flytekit.tools.repo.serialize_load_only") as mock_serialize_load, \
         patch("flytekit.tools.repo.serialize_get_control_plane_entities") as mock_get_entities, \
         patch("flytekit.tools.repo._get_git_repo_url") as mock_git_url:
        
        mock_find_root.return_value = "/test/root"
        mock_list_pkgs.return_value = [("test", "module", "my_task", None), ("test", "module", "my_workflow", None)]
        mock_git_url.return_value = None
        
        # Simulate loading entities
        def simulate_load(*args, **kwargs):
            FlyteEntities.entities.clear()
            FlyteEntities.entities.extend([my_task, my_workflow])
        
        mock_serialize_load.side_effect = simulate_load
        mock_get_entities.return_value = []
        
        # Create a fake md5_bytes for consistent hashing
        md5_bytes = b"test_hash"
        
        # Mock the fast_package call
        with patch.object(mock_remote, "fast_package", return_value=(md5_bytes, "s3://test/path")):
            # Set up serialization settings
            settings = SerializationSettings(
                project="test",
                domain="dev",
                image_config=ImageConfig.auto_default_image(),
            )
            
            # Simulate the version computation from register function
            FlyteContextManager.push_context(mock_remote.context)
            
            # Load entities (simulated)
            serialize_load_only(["test"], settings, "/test/root")
            
            # Extract image names from loaded entities
            image_names = []
            for entity in FlyteEntities.entities.copy():
                if entity is not None and hasattr(entity, 'container_image'):
                    image_names.extend(mock_remote._get_image_names(entity))
            
            # Compute version
            version = mock_remote._version_from_hash(md5_bytes, settings, None, *image_names)
            
            FlyteContextManager.pop_context()
            
            return version


def test_different_imagespecs_produce_different_versions():
    """Verify that different ImageSpec configurations produce different versions."""
    version1 = test_register_includes_imagespec_in_version(["package1"])
    version2 = test_register_includes_imagespec_in_version(["package2"])
    
    # The versions should be different because the ImageSpec packages are different
    assert version1 != version2, "Different ImageSpecs should produce different versions" 