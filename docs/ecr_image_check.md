# ECR Image Check Feature

## Overview

Flytekit now supports checking Amazon ECR (Elastic Container Registry) directly for image existence before attempting to build. This feature improves performance and reliability when working with ECR-hosted images by avoiding unnecessary Docker daemon interactions.

## How It Works

When `ImageSpec.exist()` is called to check if an image needs to be built, the following logic is applied:

1. **Check if the registry is ECR**: The registry URL is checked against the ECR pattern (`*.dkr.ecr.*.amazonaws.com`)
2. **Check AWS CLI availability**: Verify that AWS CLI is installed and credentials are configured
3. **Try ECR first**: If both conditions are met, use `aws ecr describe-images` to check image existence
4. **Fallback to Docker**: If ECR check fails or conditions aren't met, fall back to the standard Docker check

## Benefits

- **Performance**: ECR API calls are typically faster than Docker daemon interactions
- **Reliability**: Avoids Docker daemon connectivity issues when checking ECR images
- **Seamless fallback**: If ECR check fails for any reason, the system automatically falls back to Docker

## Requirements

For ECR checks to be used, the following must be true:

1. The image registry must be an ECR registry (format: `<account-id>.dkr.ecr.<region>.amazonaws.com`)
2. AWS CLI must be installed (`aws` command available in PATH)
3. AWS credentials must be configured (via environment variables, AWS config files, or IAM roles)

## Example Usage

```python
from flytekit.image_spec.image_spec import ImageSpec

# Create an ImageSpec with ECR registry
spec = ImageSpec(
    name="my-ml-model",
    registry="123456789012.dkr.ecr.us-east-1.amazonaws.com",
    packages=["numpy", "pandas", "scikit-learn"],
    python_version="3.9"
)

# Check if image exists
# This will:
# 1. Detect that it's an ECR registry
# 2. Check if AWS CLI and credentials are available
# 3. If yes, use AWS CLI to check ECR
# 4. If no or if check fails, fall back to Docker
exists = spec.exist()

if exists:
    print("Image found in ECR, skipping build")
else:
    print("Image not found, building...")
```

## Build Flow

When `ImageSpecBuilder.should_build()` is called:

```
ImageSpecBuilder.should_build()
    ↓
ImageSpec.exist()
    ↓
Is ECR registry? → No → Use Docker check
    ↓ Yes
AWS CLI available? → No → Use Docker check
    ↓ Yes
Check ECR with AWS CLI
    ↓
Success? → Yes → Return result
    ↓ No
Fall back to Docker check
```

## Error Handling

The ECR check is designed to be non-disruptive:

- If AWS CLI is not installed: Falls back to Docker
- If AWS credentials are not configured: Falls back to Docker
- If ECR API call fails: Falls back to Docker
- If ECR returns an error: Falls back to Docker

This ensures that the build process continues to work even if ECR checks cannot be performed.

## Debugging

The feature provides colored console output to help debug the check process:

- Blue: "Checking ECR for image..."
- Green: "Image found in ECR"
- Yellow: "Image not found in ECR" or "ECR check failed, falling back to Docker"

## Implementation Details

The implementation adds three helper functions:

1. `is_ecr_registry(registry: str) -> bool`: Checks if a registry URL is an ECR registry
2. `check_aws_cli_and_creds() -> bool`: Verifies AWS CLI and credentials availability
3. `check_ecr_image_exists(registry: str, repository: str, tag: str) -> Optional[bool]`: Performs the actual ECR check

The `ImageSpec.exist()` method is modified to check these conditions and use ECR when appropriate.