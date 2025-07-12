"""Unit tests for ECR image check functionality."""

import json
import subprocess
from unittest import mock

from flytekit.image_spec.image_spec import (
    ImageSpec,
    check_aws_cli_and_creds,
    check_ecr_image_exists,
    is_ecr_registry,
)


class TestECRHelpers:
    """Test ECR helper functions."""
    
    def test_is_ecr_registry(self):
        """Test ECR registry detection."""
        # Valid ECR registries
        assert is_ecr_registry("123456789012.dkr.ecr.us-east-1.amazonaws.com")
        assert is_ecr_registry("987654321098.dkr.ecr.eu-west-1.amazonaws.com")
        assert is_ecr_registry("111111111111.dkr.ecr.ap-southeast-1.amazonaws.com")
        
        # Invalid registries
        assert not is_ecr_registry("docker.io")
        assert not is_ecr_registry("ghcr.io")
        assert not is_ecr_registry("localhost:5000")
        assert not is_ecr_registry("myregistry.com")
        assert not is_ecr_registry("")
    
    @mock.patch('subprocess.run')
    def test_check_aws_cli_and_creds_success(self, mock_run):
        """Test AWS CLI check when everything is configured."""
        # Mock successful responses
        mock_run.side_effect = [
            mock.Mock(returncode=0),  # aws --version
            mock.Mock(returncode=0),  # aws sts get-caller-identity
        ]
        
        assert check_aws_cli_and_creds() is True
        assert mock_run.call_count == 2
    
    @mock.patch('subprocess.run')
    def test_check_aws_cli_and_creds_no_cli(self, mock_run):
        """Test AWS CLI check when CLI is not installed."""
        mock_run.side_effect = FileNotFoundError()
        assert check_aws_cli_and_creds() is False
    
    @mock.patch('subprocess.run')
    def test_check_aws_cli_and_creds_no_creds(self, mock_run):
        """Test AWS CLI check when credentials are not configured."""
        mock_run.side_effect = [
            mock.Mock(returncode=0),  # aws --version
            mock.Mock(returncode=1),  # aws sts get-caller-identity fails
        ]
        assert check_aws_cli_and_creds() is False
    
    @mock.patch('subprocess.run')
    def test_check_ecr_image_exists_found(self, mock_run):
        """Test ECR image check when image exists."""
        mock_result = mock.Mock(
            returncode=0,
            stdout=json.dumps({
                "imageDetails": [
                    {
                        "imageTags": ["latest"],
                        "imageSizeInBytes": 123456789
                    }
                ]
            })
        )
        mock_run.return_value = mock_result
        
        result = check_ecr_image_exists(
            "123456789012.dkr.ecr.us-east-1.amazonaws.com",
            "my-repo",
            "latest"
        )
        assert result is True
    
    @mock.patch('subprocess.run')
    def test_check_ecr_image_exists_not_found(self, mock_run):
        """Test ECR image check when image doesn't exist."""
        mock_result = mock.Mock(
            returncode=1,
            stderr="ImageNotFoundException"
        )
        mock_run.return_value = mock_result
        
        result = check_ecr_image_exists(
            "123456789012.dkr.ecr.us-east-1.amazonaws.com",
            "my-repo",
            "latest"
        )
        assert result is False
    
    @mock.patch('subprocess.run')
    def test_check_ecr_image_exists_error(self, mock_run):
        """Test ECR image check when an error occurs."""
        mock_result = mock.Mock(
            returncode=1,
            stderr="Some other error"
        )
        mock_run.return_value = mock_result
        
        result = check_ecr_image_exists(
            "123456789012.dkr.ecr.us-east-1.amazonaws.com",
            "my-repo",
            "latest"
        )
        assert result is None
    
    def test_check_ecr_image_exists_invalid_registry(self):
        """Test ECR image check with invalid registry format."""
        result = check_ecr_image_exists(
            "not-an-ecr-registry.com",
            "my-repo",
            "latest"
        )
        assert result is None


class TestImageSpecECR:
    """Test ImageSpec ECR functionality."""
    
    @mock.patch('flytekit.image_spec.image_spec.check_aws_cli_and_creds')
    @mock.patch('flytekit.image_spec.image_spec.check_ecr_image_exists')
    @mock.patch('docker.from_env')
    def test_exist_ecr_success(self, mock_docker, mock_ecr_check, mock_aws_check):
        """Test exist() method with ECR when image exists."""
        # Setup mocks
        mock_aws_check.return_value = True
        mock_ecr_check.return_value = True
        
        # Create ECR ImageSpec
        spec = ImageSpec(
            name="my-app",
            registry="123456789012.dkr.ecr.us-east-1.amazonaws.com",
            python_version="3.9"
        )
        
        # Check existence
        result = spec.exist()
        
        # Verify ECR was checked, not Docker
        assert result is True
        mock_ecr_check.assert_called_once_with(
            "123456789012.dkr.ecr.us-east-1.amazonaws.com",
            "my-app",
            spec.tag
        )
        mock_docker.assert_not_called()
    
    @mock.patch('flytekit.image_spec.image_spec.check_aws_cli_and_creds')
    @mock.patch('flytekit.image_spec.image_spec.check_ecr_image_exists')
    @mock.patch('docker.from_env')
    def test_exist_ecr_fallback_to_docker(self, mock_docker, mock_ecr_check, mock_aws_check):
        """Test exist() falls back to Docker when ECR check fails."""
        # Setup mocks
        mock_aws_check.return_value = True
        mock_ecr_check.return_value = None  # ECR check failed
        
        # Mock Docker client
        mock_client = mock.Mock()
        mock_docker.return_value = mock_client
        mock_client.images.get_registry_data.return_value = True
        
        # Create ECR ImageSpec
        spec = ImageSpec(
            name="my-app",
            registry="123456789012.dkr.ecr.us-east-1.amazonaws.com",
            python_version="3.9"
        )
        
        # Check existence
        result = spec.exist()
        
        # Verify both ECR and Docker were checked
        assert result is True
        mock_ecr_check.assert_called_once()
        mock_docker.assert_called_once()
    
    @mock.patch('flytekit.image_spec.image_spec.check_aws_cli_and_creds')
    @mock.patch('flytekit.image_spec.image_spec.check_ecr_image_exists')
    @mock.patch('docker.from_env')
    def test_exist_non_ecr_registry(self, mock_docker, mock_ecr_check, mock_aws_check):
        """Test exist() uses Docker directly for non-ECR registries."""
        # Mock Docker client
        mock_client = mock.Mock()
        mock_docker.return_value = mock_client
        mock_client.images.get_registry_data.return_value = True
        
        # Create non-ECR ImageSpec
        spec = ImageSpec(
            name="my-app",
            registry="docker.io/myuser",
            python_version="3.9"
        )
        
        # Check existence
        result = spec.exist()
        
        # Verify only Docker was checked, not ECR
        assert result is True
        mock_ecr_check.assert_not_called()
        mock_aws_check.assert_not_called()
        mock_docker.assert_called_once()
    
    @mock.patch('flytekit.image_spec.image_spec.check_aws_cli_and_creds')
    @mock.patch('flytekit.image_spec.image_spec.check_ecr_image_exists')
    @mock.patch('docker.from_env')
    def test_exist_ecr_no_aws_cli(self, mock_docker, mock_ecr_check, mock_aws_check):
        """Test exist() uses Docker when AWS CLI is not available."""
        # Setup mocks
        mock_aws_check.return_value = False  # No AWS CLI
        
        # Mock Docker client
        mock_client = mock.Mock()
        mock_docker.return_value = mock_client
        mock_client.images.get_registry_data.return_value = True
        
        # Create ECR ImageSpec
        spec = ImageSpec(
            name="my-app",
            registry="123456789012.dkr.ecr.us-east-1.amazonaws.com",
            python_version="3.9"
        )
        
        # Check existence
        result = spec.exist()
        
        # Verify only Docker was checked since AWS CLI not available
        assert result is True
        mock_ecr_check.assert_not_called()
        mock_docker.assert_called_once()