"""
Example of using the optimized UV builder for better Docker layer caching.

This example demonstrates how to use the UV builder with ImageSpec to optimize
Docker image builds when using uv.lock files. The UV builder separates remote
dependencies (highly cacheable) from local dependencies (change frequently) into
different layers for optimal caching.
"""

from pathlib import Path

from flytekit import ImageSpec, task, workflow
from flytekit.image_spec.default_builder import remove_local_packages_from_lock_files

# Create an ImageSpec using the UV builder
# The UV builder is automatically selected when:
# 1. You specify a uv.lock file as requirements
# 2. You explicitly set builder="uv"
image_spec = ImageSpec(
    name="my-optimized-app",
    requirements="path/to/uv.lock",  # Must be a uv.lock file
    registry="ghcr.io/myorg",  # Your container registry
    builder="uv",  # Explicitly use the UV builder
    install_project=True,  # Whether to install the current project
)

# Alternative: Let the builder be auto-selected based on priority
# If only uv.lock is specified, the UV builder will be preferred over default
image_spec_auto = ImageSpec(
    name="my-app-auto",
    requirements="uv.lock",
    registry="ghcr.io/myorg",
)


@task(container_image=image_spec)
def process_data(x: int) -> int:
    """A simple task using the optimized image."""
    return x * 2


@workflow
def my_workflow(x: int = 10) -> int:
    """A workflow using the optimized image."""
    return process_data(x=x)


def main():
    # Example 1: Basic usage with default output file names
    uv_lock = Path("uv.lock")
    pyproject = Path("pyproject.toml")

    if uv_lock.exists() and pyproject.exists():
        output_lock, output_pyproject = remove_local_packages_from_lock_files(
            uv_lock_path=uv_lock, pyproject_path=pyproject
        )
        print(f"Created {output_lock} and {output_pyproject}")
    else:
        print("uv.lock or pyproject.toml not found in current directory")

    # Example 2: Custom output file names
    custom_lock_output = Path("dependencies_only.lock")
    custom_pyproject_output = Path("dependencies_only.toml")

    if uv_lock.exists() and pyproject.exists():
        output_lock, output_pyproject = remove_local_packages_from_lock_files(
            uv_lock_path=uv_lock,
            pyproject_path=pyproject,
            output_lock_path=custom_lock_output,
            output_pyproject_path=custom_pyproject_output,
        )
        print(f"Created {output_lock} and {output_pyproject}")


if __name__ == "__main__":
    # Run locally
    print(f"Result: {my_workflow(x=5)}")

    # The UV builder provides several optimizations:
    # 1. Two-layer approach: remote deps cached separately from local deps
    # 2. Uses Docker buildx with inline cache for better layer reuse
    # 3. Minimal Dockerfile focused only on UV installation
    # 4. No support for packages, apt, conda etc. - pure UV only

    # Benefits:
    # - Faster rebuilds when only local code changes
    # - Better cache utilization across builds
    # - Optimized for UV lock file workflows

    main()
