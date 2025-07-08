import os
import tempfile
from pathlib import Path
from unittest import mock

import pytest
import toml

from flytekit.image_spec.image_spec import ImageSpec
from flytekit.image_spec.uv_builder import UvImageBuilder
from flytekit.image_spec.default_builder import remove_local_packages_from_lock_files


def create_test_uv_lock(tmp_path: Path, with_local_deps: bool = False):
    """Create test uv.lock and pyproject.toml files."""
    # Create pyproject.toml
    pyproject_data = {
        "project": {
            "name": "test-project",
            "version": "0.1.0",
            "dependencies": ["requests==2.31.0"],
        },
        "build-system": {
            "requires": ["hatchling"],
            "build-backend": "hatchling.build",
        },
    }
    
    if with_local_deps:
        pyproject_data["project"]["dependencies"].append("local-package")
        pyproject_data["tool"] = {
            "uv": {
                "sources": {
                    "local-package": {"path": "./local-package"}
                }
            }
        }
    
    pyproject_path = tmp_path / "pyproject.toml"
    with open(pyproject_path, "w") as f:
        toml.dump(pyproject_data, f)
    
    # Create uv.lock
    lock_data = {
        "version": 1,
        "requires-python": ">=3.8",
        "package": [
            {
                "name": "certifi",
                "version": "2023.7.22",
            },
            {
                "name": "charset-normalizer",
                "version": "3.3.2",
            },
            {
                "name": "idna",
                "version": "3.4",
            },
            {
                "name": "requests",
                "version": "2.31.0",
                "dependencies": [
                    {"name": "certifi"},
                    {"name": "charset-normalizer"},
                    {"name": "idna"},
                    {"name": "urllib3"},
                ],
            },
            {
                "name": "test-project",
                "version": "0.1.0",
                "source": {"editable": "."},
                "dependencies": [
                    {"name": "requests"},
                ],
            },
            {
                "name": "urllib3",
                "version": "2.0.7",
            },
        ],
    }
    
    if with_local_deps:
        # Add local package to lock
        lock_data["package"].extend([
            {
                "name": "local-package",
                "version": "0.1.0",
                "source": {"directory": "./local-package"},
            },
        ])
        # Update test-project dependencies
        for pkg in lock_data["package"]:
            if pkg["name"] == "test-project":
                pkg["dependencies"].append({"name": "local-package"})
        
        # Create local package directory
        local_pkg_dir = tmp_path / "local-package"
        local_pkg_dir.mkdir()
        
        # Create local package files
        local_pyproject = {
            "project": {
                "name": "local-package",
                "version": "0.1.0",
            },
        }
        with open(local_pkg_dir / "pyproject.toml", "w") as f:
            toml.dump(local_pyproject, f)
        
        # Create __init__.py
        (local_pkg_dir / "local_package").mkdir()
        (local_pkg_dir / "local_package" / "__init__.py").write_text("# Local package")
    
    lock_path = tmp_path / "uv.lock"
    with open(lock_path, "w") as f:
        toml.dump(lock_data, f)
    
    return lock_path


class TestUvImageBuilder:
    def test_validate_uv_lock_only(self):
        """Test that builder only accepts uv.lock files."""
        builder = UvImageBuilder()
        
        # Should raise for non-uv.lock files
        image_spec = ImageSpec(
            name="test",
            requirements="requirements.txt",
        )
        with pytest.raises(ValueError, match="only supports uv.lock"):
            builder.build_image(image_spec)
        
        # Should raise for no requirements
        image_spec = ImageSpec(name="test")
        with pytest.raises(ValueError, match="only supports uv.lock"):
            builder.build_image(image_spec)
    
    def test_validate_supported_parameters(self):
        """Test that builder rejects unsupported parameters."""
        builder = UvImageBuilder()
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            lock_path = create_test_uv_lock(tmp_path)
            
            # Should raise for unsupported parameters
            image_spec = ImageSpec(
                name="test",
                requirements=str(lock_path),
                packages=["numpy"],  # Not supported
                apt_packages=["git"],  # Not supported
            )
            with pytest.raises(ValueError, match="does not support"):
                builder.build_image(image_spec)
    
    def test_prepare_build_context_no_local_deps(self):
        """Test build context preparation without local dependencies."""
        builder = UvImageBuilder()
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            lock_path = create_test_uv_lock(tmp_path, with_local_deps=False)
            
            image_spec = ImageSpec(
                name="test",
                requirements=str(lock_path),
                install_project=False,
            )
            
            with tempfile.TemporaryDirectory() as build_dir:
                build_path = Path(build_dir)
                builder._prepare_build_context(image_spec, build_path)
                
                # Check that remote lock was created
                assert (build_path / "uv_remote.lock").exists()
                assert (build_path / "pyproject_remote.toml").exists()
                assert (build_path / "Dockerfile").exists()
                
                # Check that no local packages directory was created
                assert not (build_path / "local_packages").exists()
                
                # Verify Dockerfile doesn't have local package steps
                dockerfile_content = (build_path / "Dockerfile").read_text()
                assert "COPY --chown=flytekit local_packages" not in dockerfile_content
    
    def test_prepare_build_context_with_local_deps(self):
        """Test build context preparation with local dependencies."""
        builder = UvImageBuilder()
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            lock_path = create_test_uv_lock(tmp_path, with_local_deps=True)
            
            image_spec = ImageSpec(
                name="test",
                requirements=str(lock_path),
                install_project=True,
            )
            
            with tempfile.TemporaryDirectory() as build_dir:
                build_path = Path(build_dir)
                builder._prepare_build_context(image_spec, build_path)
                
                # Check all files were created
                assert (build_path / "uv_remote.lock").exists()
                assert (build_path / "pyproject_remote.toml").exists()
                assert (build_path / "uv.lock").exists()
                assert (build_path / "pyproject.toml").exists()
                assert (build_path / "Dockerfile").exists()
                
                # Check local packages were copied
                assert (build_path / "local_packages").exists()
                assert (build_path / "local_packages" / "local-package").exists()
                
                # Verify Dockerfile has local package steps
                dockerfile_content = (build_path / "Dockerfile").read_text()
                assert "COPY --chown=flytekit local_packages" in dockerfile_content
                
                # Verify paths were updated in lock file
                with open(build_path / "uv.lock") as f:
                    lock_data = toml.load(f)
                
                local_pkg = next(p for p in lock_data["package"] if p["name"] == "local-package")
                assert local_pkg["source"]["directory"].startswith("/root/local_packages/")
    
    def test_dockerfile_structure(self):
        """Test that generated Dockerfile has correct layer structure."""
        builder = UvImageBuilder()
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            lock_path = create_test_uv_lock(tmp_path, with_local_deps=True)
            
            image_spec = ImageSpec(
                name="test",
                requirements=str(lock_path),
                base_image="python:3.11-slim",
            )
            
            with tempfile.TemporaryDirectory() as build_dir:
                build_path = Path(build_dir)
                builder._prepare_build_context(image_spec, build_path)
                
                dockerfile_content = (build_path / "Dockerfile").read_text()
                
                # Check base image
                assert "FROM python:3.11-slim" in dockerfile_content
                
                # Check layer order: remote deps first, then local
                remote_install_pos = dockerfile_content.find("uv_remote.lock")
                local_copy_pos = dockerfile_content.find("COPY --chown=flytekit local_packages")
                
                assert remote_install_pos < local_copy_pos
                
                # Check environment setup
                assert "UV_PYTHON=/root/.venv/bin/python" in dockerfile_content
                assert "PATH=\"/root/.venv/bin:$PATH\"" in dockerfile_content
    
    @mock.patch("flytekit.image_spec.uv_builder.run")
    def test_build_and_push(self, mock_run):
        """Test build and push functionality."""
        builder = UvImageBuilder()
        mock_run.return_value = None
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            lock_path = create_test_uv_lock(tmp_path)
            
            image_spec = ImageSpec(
                name="test-image",
                requirements=str(lock_path),
                registry="ghcr.io/test",
                platform="linux/amd64",
            )
            
            # Test with push enabled
            with mock.patch.dict(os.environ, {"FLYTE_PUSH_IMAGE_SPEC": "True"}):
                result = builder.build_image(image_spec)
                
                assert result == "ghcr.io/test/test-image:" + image_spec.tag
                
                # Verify docker buildx command was called
                mock_run.assert_called_once()
                args = mock_run.call_args[0][0]
                
                assert args[0] == "docker"
                assert args[1] == "buildx"
                assert args[2] == "build"
                assert "--push" in args
                assert "--cache-from" in args
                assert "--cache-to" in args
    
    def test_project_install_flag(self):
        """Test that install_project flag is handled correctly."""
        builder = UvImageBuilder()
        
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            lock_path = create_test_uv_lock(tmp_path)
            
            # Test with install_project=False
            image_spec = ImageSpec(
                name="test",
                requirements=str(lock_path),
                install_project=False,
            )
            
            with tempfile.TemporaryDirectory() as build_dir:
                build_path = Path(build_dir)
                builder._prepare_build_context(image_spec, build_path)
                
                dockerfile_content = (build_path / "Dockerfile").read_text()
                assert "--no-install-project" in dockerfile_content
            
            # Test with install_project=True
            image_spec = ImageSpec(
                name="test",
                requirements=str(lock_path),
                install_project=True,
            )
            
            with tempfile.TemporaryDirectory() as build_dir:
                build_path = Path(build_dir)
                builder._prepare_build_context(image_spec, build_path)
                
                dockerfile_content = (build_path / "Dockerfile").read_text()
                # Should not have --no-install-project flag
                assert "--no-install-project" not in dockerfile_content.replace("--no-install-project", "REPLACED") 


def test_remove_local_packages_from_lock_files():
    """Test removing local packages from uv.lock and pyproject.toml files."""
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        
        # Create a sample uv.lock with both local and external packages
        lock_data = {
            "version": 1,
            "requires-python": ">=3.8",
            "package": [
                {
                    "name": "requests",
                    "version": "2.31.0",
                    "source": {"registry": "https://pypi.org/simple"},
                },
                {
                    "name": "local-package",
                    "version": "0.1.0",
                    "source": {"directory": "../local-package"},
                },
                {
                    "name": "editable-package", 
                    "version": "0.2.0",
                    "source": {"editable": "./src/package"},
                },
                {
                    "name": "numpy",
                    "version": "1.24.0", 
                    "source": {"registry": "https://pypi.org/simple"},
                },
            ],
        }
        
        # Create a sample pyproject.toml with local dependencies
        pyproject_data = {
            "project": {
                "name": "test-project",
                "version": "0.1.0",
                "dependencies": [
                    "requests>=2.0",
                    "numpy",
                    {"path": "../local-package"},
                ],
                "optional-dependencies": {
                    "dev": [
                        "pytest",
                        {"path": "./src/dev-tools"},
                    ]
                }
            },
            "tool": {
                "uv": {
                    "sources": {
                        "local-package": {"path": "../local-package"},
                        "requests": {"registry": "pypi"},
                    }
                }
            }
        }
        
        # Write the test files
        uv_lock_path = tmp_path / "uv.lock"
        pyproject_path = tmp_path / "pyproject.toml"
        
        with open(uv_lock_path, "w") as f:
            toml.dump(lock_data, f)
        
        with open(pyproject_path, "w") as f:
            toml.dump(pyproject_data, f)
        
        # Run the function
        output_lock, output_pyproject = remove_local_packages_from_lock_files(
            uv_lock_path=uv_lock_path,
            pyproject_path=pyproject_path
        )
        
        # Verify outputs exist
        assert output_lock.exists()
        assert output_pyproject.exists()
        assert output_lock.name == "uv_external.lock"
        assert output_pyproject.name == "pyproject_external.toml"
        
        # Load and verify the filtered lock file
        with open(output_lock) as f:
            filtered_lock = toml.load(f)
        
        # Should only have external packages
        assert len(filtered_lock["package"]) == 2
        package_names = {pkg["name"] for pkg in filtered_lock["package"]}
        assert package_names == {"requests", "numpy"}
        
        # Verify no local sources remain
        for pkg in filtered_lock["package"]:
            source = pkg.get("source", {})
            assert "directory" not in source
            assert "editable" not in source
        
        # Load and verify the filtered pyproject.toml
        with open(output_pyproject) as f:
            filtered_pyproject = toml.load(f)
        
        # Check dependencies only contain strings (no dicts with paths)
        deps = filtered_pyproject["project"]["dependencies"]
        assert all(isinstance(dep, str) for dep in deps)
        assert len(deps) == 2
        assert "requests>=2.0" in deps
        assert "numpy" in deps
        
        # Check optional dependencies
        dev_deps = filtered_pyproject["project"]["optional-dependencies"]["dev"]
        assert all(isinstance(dep, str) for dep in dev_deps)
        assert len(dev_deps) == 1
        assert "pytest" in dev_deps
        
        # Check that local sources were removed from tool.uv.sources
        uv_sources = filtered_pyproject.get("tool", {}).get("uv", {}).get("sources", {})
        assert "local-package" not in uv_sources
        assert "requests" in uv_sources


def test_remove_local_packages_custom_output_paths():
    """Test with custom output paths."""
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        
        # Create minimal test files
        lock_data = {
            "version": 1,
            "package": [
                {
                    "name": "pkg1",
                    "version": "1.0.0",
                    "source": {"registry": "https://pypi.org/simple"},
                }
            ],
        }
        
        pyproject_data = {
            "project": {
                "name": "test",
                "dependencies": ["pkg1"],
            }
        }
        
        uv_lock_path = tmp_path / "uv.lock"
        pyproject_path = tmp_path / "pyproject.toml"
        
        with open(uv_lock_path, "w") as f:
            toml.dump(lock_data, f)
        
        with open(pyproject_path, "w") as f:
            toml.dump(pyproject_data, f)
        
        # Custom output paths
        custom_lock = tmp_path / "custom.lock"
        custom_pyproject = tmp_path / "custom.toml"
        
        output_lock, output_pyproject = remove_local_packages_from_lock_files(
            uv_lock_path=uv_lock_path,
            pyproject_path=pyproject_path,
            output_lock_path=custom_lock,
            output_pyproject_path=custom_pyproject
        )
        
        assert output_lock == custom_lock
        assert output_pyproject == custom_pyproject
        assert output_lock.exists()
        assert output_pyproject.exists()


if __name__ == "__main__":
    test_remove_local_packages_from_lock_files()
    test_remove_local_packages_custom_output_paths()
    print("All tests passed!") 