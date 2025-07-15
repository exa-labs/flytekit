# AWS ECR Command and Raw Output

## AWS ECR Command
The AWS ECR command used in `image_spec.py` (lines 28-36) is:

```bash
aws ecr describe-images --repository-name {repository} --image-ids imageTag={tag} --region {region} --output json
```

**Parameters:**
- `repository`: Repository name (e.g., "my-app")
- `tag`: Image tag (e.g., "latest")
- `region`: AWS region extracted from registry URL (e.g., "us-east-1")

## Raw Output Examples

### Successful Response (Image Exists)
When the image exists in ECR, the command returns JSON with `returncode=0`:

```json
{
    "imageDetails": [
        {
            "imageTags": ["latest"],
            "imageSizeInBytes": 123456789
        }
    ]
}
```

### Image Not Found Response
When the image doesn't exist, the command returns `returncode=1` with stderr containing:
- `"ImageNotFoundException"` - when the specific image tag is not found
- `"RepositoryNotFoundException"` - when the repository itself doesn't exist

### Error Response
For other errors (permission issues, etc.), the command returns `returncode=1` with stderr containing error details like:
```
"Some other error"
```

## Implementation Details
The check is implemented in the `check_ecr_image_exists()` function which:
1. Extracts region from registry URL using regex: `(\d+)\.dkr\.ecr\.(.+?)\.amazonaws\.com`
2. Runs the AWS CLI command with a 10-second timeout
3. Returns `True` if image exists, `False` if not found, `None` if check failed
4. Falls back to Docker-based checking if ECR check fails