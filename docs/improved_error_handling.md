# Improved Error Handling for Image Checks and Builds

## Overview

Flytekit now provides clearer error messages when checking if images exist or when attempting to build images without the necessary tools installed. This helps users quickly identify and resolve configuration issues.

## Changes Made

### 1. Image Existence Check Error Handling

When checking if an image exists (particularly for ECR registries), the system now provides clear error messages if neither Docker nor AWS CLI is available.

**Before**: The system would silently return `None` when it couldn't check image existence, leading to confusion.

**After**: The system raises a descriptive `RuntimeError` with actionable guidance:

```
RuntimeError: Couldn't check if image exists, please make sure either you are using ECR and aws is properly logged in, or otherwise docker is installed and the daemon is accessible
```

### 2. Build Tool Availability Checks

When attempting to build images, the system now checks if the required build tools are installed and accessible before attempting the build.

#### Docker Build Errors

**Scenario 1**: Docker not installed
```
RuntimeError: Docker is not installed or not in PATH. Please install Docker (https://docs.docker.com/get-docker/) or use depot by setting use_depot=True
```

**Scenario 2**: Docker installed but daemon not running
```
RuntimeError: Docker daemon is not running or not accessible. Error: <error details>
Please start Docker daemon or use depot by setting use_depot=True
```

#### Depot Build Errors

When `use_depot=True` but depot is not installed:
```
RuntimeError: Depot is not installed or not in PATH. Please install depot (https://depot.dev/docs/installation) or use Docker instead by setting use_depot=False
```

#### Envd Build Errors

When using the envd builder without envd installed:
```
RuntimeError: envd is not installed or not in PATH. Please install envd (https://github.com/tensorchord/envd#installation) or use a different builder (e.g., Docker) by setting builder='default'
```

## Implementation Details

### Files Modified

1. **`flytekit/image_spec/image_spec.py`**
   - Enhanced `exist()` method to track Docker availability
   - Added error handling for ECR images when neither Docker nor AWS CLI is available

2. **`flytekit/image_spec/default_builder.py`**
   - Added pre-build checks for Docker/depot availability
   - Added Docker daemon connectivity check
   - Provides clear error messages with installation links

3. **`plugins/flytekit-envd/flytekitplugins/envd/image_builder.py`**
   - Added check for envd availability before attempting build
   - Provides clear error message with installation instructions

## Benefits

1. **Faster Debugging**: Users immediately know what's wrong instead of seeing cryptic errors
2. **Actionable Guidance**: Error messages include links to installation guides and suggest alternatives
3. **Better User Experience**: Clear communication about requirements and alternatives
4. **Prevents Silent Failures**: No more assuming images exist when checks fail

## Example Usage

```python
from flytekit.image_spec.image_spec import ImageSpec

# ECR image check
spec = ImageSpec(
    name="my-model",
    registry="123456789012.dkr.ecr.us-east-1.amazonaws.com",
    packages=["numpy"],
)

try:
    exists = spec.exist()
except RuntimeError as e:
    print(f"Error: {e}")
    # Will show clear message about needing Docker or AWS CLI

# Building with Docker
spec = ImageSpec(
    name="my-app",
    packages=["scikit-learn"],
    use_depot=False,
)

# If Docker is not installed/running, will get clear error with instructions

# Building with depot
spec = ImageSpec(
    name="my-app",
    packages=["tensorflow"],
    use_depot=True,
)

# If depot is not installed, will get clear error with installation link
```

## Testing

Unit tests have been added to verify the error handling behavior:
- `tests/flytekit/unit/core/image_spec/test_error_handling.py`

These tests mock various failure scenarios to ensure appropriate error messages are raised.