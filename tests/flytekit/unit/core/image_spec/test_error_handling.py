"""Unit tests for improved error handling in image spec."""

import pytest
from unittest.mock import patch, MagicMock
from flytekit.image_spec.image_spec import ImageSpec
from flytekit.image_spec.default_builder import DefaultImageBuilder


class TestImageExistErrorHandling:
    """Test error handling for image existence checks."""
    
    @patch('flytekit.image_spec.image_spec.docker')
    @patch('flytekit.image_spec.image_spec.is_ecr_registry')
    @patch('flytekit.image_spec.image_spec.check_aws_cli_and_creds')
    def test_ecr_image_no_docker_no_aws(self, mock_aws_check, mock_is_ecr, mock_docker):
        """Test that we get a clear error when checking ECR image without Docker or AWS CLI."""
        # Setup
        mock_is_ecr.return_value = True
        mock_aws_check.return_value = False
        mock_docker.from_env.side_effect = Exception("Docker not available")
        
        spec = ImageSpec(
            name="test-image",
            registry="123456789012.dkr.ecr.us-east-1.amazonaws.com",
            packages=["numpy"],
        )
        
        # Test
        with pytest.raises(RuntimeError) as exc_info:
            spec.exist()
        
        # Verify
        assert "Couldn't check if image exists" in str(exc_info.value)
        assert "aws is properly logged in" in str(exc_info.value)
        assert "docker is installed" in str(exc_info.value)
    
    @patch('flytekit.image_spec.image_spec.docker')
    @patch('flytekit.image_spec.image_spec.is_ecr_registry')
    @patch('flytekit.image_spec.image_spec.check_aws_cli_and_creds')
    def test_ecr_image_with_aws_fallback(self, mock_aws_check, mock_is_ecr, mock_docker):
        """Test that we don't raise error if AWS CLI is available for ECR images."""
        # Setup
        mock_is_ecr.return_value = True
        mock_aws_check.return_value = True
        mock_docker.from_env.side_effect = Exception("Docker not available")
        
        spec = ImageSpec(
            name="test-image",
            registry="123456789012.dkr.ecr.us-east-1.amazonaws.com",
            packages=["numpy"],
        )
        
        # Test - should not raise error, just return None
        result = spec.exist()
        assert result is None  # Failed to check, but didn't crash


class TestBuildErrorHandling:
    """Test error handling for image builds."""
    
    @patch('shutil.which')
    def test_docker_not_installed(self, mock_which):
        """Test error when Docker is not installed."""
        # Setup
        mock_which.return_value = None
        
        spec = ImageSpec(
            name="test-build",
            packages=["numpy"],
            use_depot=False,
        )
        
        builder = DefaultImageBuilder()
        
        # Test
        with pytest.raises(RuntimeError) as exc_info:
            builder._build_image(spec, push=False)
        
        # Verify
        assert "Docker is not installed or not in PATH" in str(exc_info.value)
        assert "https://docs.docker.com/get-docker/" in str(exc_info.value)
    
    @patch('shutil.which')
    @patch('flytekit.image_spec.default_builder.run')
    def test_docker_daemon_not_running(self, mock_run, mock_which):
        """Test error when Docker is installed but daemon is not running."""
        # Setup
        mock_which.return_value = "/usr/bin/docker"
        mock_run.return_value = MagicMock(returncode=1, stderr="Cannot connect to Docker daemon")
        
        spec = ImageSpec(
            name="test-build",
            packages=["numpy"],
            use_depot=False,
        )
        
        builder = DefaultImageBuilder()
        
        # Test
        with pytest.raises(RuntimeError) as exc_info:
            builder._build_image(spec, push=False)
        
        # Verify
        assert "Docker daemon is not running" in str(exc_info.value)
    
    @patch('shutil.which')
    def test_depot_not_installed(self, mock_which):
        """Test error when depot is not installed."""
        # Setup
        mock_which.return_value = None
        
        spec = ImageSpec(
            name="test-depot",
            packages=["numpy"],
            use_depot=True,
        )
        
        builder = DefaultImageBuilder()
        
        # Test
        with pytest.raises(RuntimeError) as exc_info:
            builder._build_image(spec, push=False)
        
        # Verify
        assert "Depot is not installed or not in PATH" in str(exc_info.value)
        assert "https://depot.dev/docs/installation" in str(exc_info.value)
        assert "use_depot=False" in str(exc_info.value)


class TestEnvdErrorHandling:
    """Test error handling for envd builder."""
    
    @patch('shutil.which')
    def test_envd_not_installed(self, mock_which):
        """Test error when envd is not installed."""
        # Only run this test if the envd plugin is available
        try:
            from flytekitplugins.envd.image_builder import EnvdImageSpecBuilder
        except ImportError:
            pytest.skip("envd plugin not installed")
        
        # Setup
        mock_which.return_value = None
        
        spec = ImageSpec(
            name="test-envd",
            packages=["numpy"],
            builder="envd",
        )
        
        builder = EnvdImageSpecBuilder()
        
        # Test
        with pytest.raises(RuntimeError) as exc_info:
            builder.build_image(spec)
        
        # Verify
        assert "envd is not installed or not in PATH" in str(exc_info.value)
        assert "https://github.com/tensorchord/envd#installation" in str(exc_info.value)
        assert "builder='default'" in str(exc_info.value)
