#!/usr/bin/env python3
"""Example script demonstrating how to remove local packages from uv.lock and pyproject.toml files.

This is useful when you want to create a version of your dependencies that only includes
external packages from PyPI or other package registries, excluding any local path dependencies.
"""

from pathlib import Path

from flytekit.image_spec.default_builder import remove_local_packages_from_lock_files


def main():
    """Demonstrate removing local packages from lock files."""

    # Example 1: Basic usage with default output file names
    print("Example 1: Basic usage")
    print("-" * 50)

    uv_lock = Path("uv.lock")
    pyproject = Path("pyproject.toml")

    if uv_lock.exists() and pyproject.exists():
        # This will create uv_external.lock and pyproject_external.toml
        output_lock, output_pyproject = remove_local_packages_from_lock_files(
            uv_lock_path=uv_lock, pyproject_path=pyproject
        )
        print(f"✓ Created {output_lock}")
        print(f"✓ Created {output_pyproject}")
        print("  These files contain only external dependencies (no local packages)")
    else:
        print("✗ uv.lock or pyproject.toml not found in current directory")

    print("\nExample 2: Custom output file names")
    print("-" * 50)

    # Example 2: Custom output file names
    custom_lock_output = Path("external_deps.lock")
    custom_pyproject_output = Path("external_deps.toml")

    if uv_lock.exists() and pyproject.exists():
        output_lock, output_pyproject = remove_local_packages_from_lock_files(
            uv_lock_path=uv_lock,
            pyproject_path=pyproject,
            output_lock_path=custom_lock_output,
            output_pyproject_path=custom_pyproject_output,
        )
        print(f"✓ Created {output_lock}")
        print(f"✓ Created {output_pyproject}")

    # Example 3: Processing files from a different directory
    print("\nExample 3: Processing files from another directory")
    print("-" * 50)

    project_dir = Path("../my_project")
    if project_dir.exists():
        uv_lock = project_dir / "uv.lock"
        pyproject = project_dir / "pyproject.toml"

        if uv_lock.exists() and pyproject.exists():
            # Output to current directory
            output_lock, output_pyproject = remove_local_packages_from_lock_files(
                uv_lock_path=uv_lock,
                pyproject_path=pyproject,
                output_lock_path=Path("./external_only.lock"),
                output_pyproject_path=Path("./external_only.toml"),
            )
            print(f"✓ Created {output_lock}")
            print(f"✓ Created {output_pyproject}")
        else:
            print(f"✗ Lock files not found in {project_dir}")
    else:
        print(f"✗ Directory {project_dir} not found")


if __name__ == "__main__":
    main()

    print("\nWhat gets removed:")
    print("-" * 50)
    print("• Local packages with 'directory' source (e.g., '../my_lib')")
    print("• Local packages with 'editable' source (e.g., '-e ./src/package')")
    print("• Path references in pyproject.toml dependencies")
    print("• tool.uv.sources entries with 'path' specifications")
