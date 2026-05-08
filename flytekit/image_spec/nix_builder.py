from __future__ import annotations

import dataclasses
import json
import os
import platform
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import ClassVar, Dict, List, Optional

import click

from flytekit.image_spec.default_builder import _copy_lock_files_into_context
from flytekit.image_spec.image_spec import ImageSpec, ImageSpecBuilder

_PYTHON_VERSION_TO_NIX: Dict[str, str] = {
    "3.9": "python39",
    "3.10": "python310",
    "3.11": "python311",
    "3.12": "python312",
    "python39": "python39",
    "python310": "python310",
    "python311": "python311",
    "python312": "python312",
}

_PLATFORM_TO_NIX_SYSTEM: Dict[str, str] = {
    "linux/amd64": "x86_64-linux",
    "linux/arm64": "aarch64-linux",
}


class NixImageSpecBuilder(ImageSpecBuilder):
    """Build uv.lock-backed OCI images with Nix and nix2container.

    Uses the project's own flake.nix when present, or generates one that
    wraps makePythonProject.  Requires ``nix`` on PATH.
    """

    _SUPPORTED_IMAGE_SPEC_PARAMETERS: ClassVar[set] = {
        "builder",
        "entrypoint",
        "env",
        "id",
        "install_project",
        "name",
        "nix",
        "platform",
        "python_version",
        "registry",
        "requirements",
        "tag",
        "tag_format",
        # Fields with non-None defaults that are harmless to ignore.
        "use_depot",
        "uv_export_args",
        "vendor_local",
    }

    def build_image(self, image_spec: ImageSpec) -> Optional[str]:
        return self._build_image(
            image_spec,
            push=os.getenv("FLYTE_PUSH_IMAGE_SPEC", "True").lower() in ("true", "1"),
        )

    def _build_image(self, image_spec: ImageSpec, *, push: bool = True) -> str:
        self._validate(image_spec)

        if not shutil.which("nix"):
            raise RuntimeError(
                "Nix is not installed or not in PATH. "
                "Please install Nix (https://nixos.org/download)"
            )

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            lock_dir = Path(image_spec.requirements).resolve().parent
            context_spec = dataclasses.replace(
                image_spec,
                nix=(lock_dir / "flake.nix").exists(),
            )

            _copy_lock_files_into_context(context_spec, "uv.lock", tmp_path)
            _ensure_flake(tmp_path, image_spec)

            command = _build_command(image_spec, tmp_path, push=push)
            log_command = _redact_creds(command)
            click.secho(f"Run command: {' '.join(log_command)} ", fg="blue")

            result = subprocess.run(command)
            if result.returncode != 0:
                raise RuntimeError(
                    f"Build command failed with exit code {result.returncode}: "
                    f"{' '.join(log_command)}"
                )

            return image_spec.image_name()

    def _validate(self, image_spec: ImageSpec) -> None:
        if not image_spec.requirements or Path(image_spec.requirements).name != "uv.lock":
            raise ValueError("NixImageSpecBuilder only supports uv.lock requirements.")

        unsupported = [
            name
            for name, value in vars(image_spec).items()
            if value is not None
            and name not in self._SUPPORTED_IMAGE_SPEC_PARAMETERS
            and not name.startswith("_")
        ]
        if unsupported:
            raise ValueError(
                f"NixImageSpecBuilder does not support: {', '.join(sorted(unsupported))}"
            )

        if image_spec.python_version and image_spec.python_version not in _PYTHON_VERSION_TO_NIX:
            raise ValueError(
                f"Unsupported python_version {image_spec.python_version!r}. "
                f"Supported: {', '.join(sorted(_PYTHON_VERSION_TO_NIX))}"
            )

        if image_spec.platform not in _PLATFORM_TO_NIX_SYSTEM:
            raise ValueError(
                f"Unsupported platform {image_spec.platform!r}. "
                f"Supported: {', '.join(sorted(_PLATFORM_TO_NIX_SYSTEM))}"
            )

def _build_command(image_spec: ImageSpec, tmp_path: Path, *, push: bool) -> List[str]:
    nix_system = _PLATFORM_TO_NIX_SYSTEM[image_spec.platform]
    local_system = _local_nix_system()
    is_cross_build = nix_system != local_system

    if push and image_spec.registry:
        ecr_token = subprocess.run(
            ["aws", "ecr", "get-login-password", "--region", "us-west-2"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()

        if is_cross_build:
            docker_attr = f"packages.{local_system}.docker-{nix_system}.copyTo"
            click.secho(f"Cross-build: {nix_system} image via {local_system} n2c", fg="yellow")
        else:
            docker_attr = f"packages.{nix_system}.docker.copyTo"

        return [
            "nix", "run",
            f"path:{tmp_path}#{docker_attr}", "--",
            f"docker://{image_spec.image_name()}",
            "--dest-creds", f"AWS:{ecr_token}",
            "--image-parallel-copies", "32",
        ]

    return ["nix", "build", f"path:{tmp_path}#packages.{nix_system}.docker"]


def _ensure_flake(tmp_path: Path, image_spec: ImageSpec) -> None:
    if (tmp_path / "flake.nix").exists():
        return
    (tmp_path / "flake.nix").write_text(_render_flake(image_spec))


def _render_flake(image_spec: ImageSpec) -> str:
    python_version = _PYTHON_VERSION_TO_NIX.get(image_spec.python_version or "3.10", "python310")
    docker_image_spec = {
        "name": image_spec.name,
        "tag": image_spec.image_name().rsplit(":", 1)[1],
        "cmd": image_spec.entrypoint or ["pyflyte-execute"],
        "extraEnv": image_spec.env or {},
    }
    flake_path = os.getenv("FLYTEKIT_NIX_PYTHON_FLAKE", "")
    if not flake_path:
        raise RuntimeError(
            "Set FLYTEKIT_NIX_PYTHON_FLAKE to the path of the makePythonProject flake "
            "(e.g. /path/to/monorepo/flakes/python) when using the nix builder without "
            "an existing flake.nix in the project directory."
        )

    return (
        "{\n"
        '  description = "Generated Flyte ImageSpec nix image";\n'
        "\n"
        "  inputs = {\n"
        f'    python-flake.url = "path:{flake_path}";\n'
        "  };\n"
        "\n"
        "  outputs =\n"
        "    { python-flake, ... }:\n"
        "    python-flake.inputs.flake-utils.lib.eachDefaultSystem (\n"
        "      system:\n"
        "      let\n"
        "        project = python-flake.lib.makePythonProject {\n"
        "          inherit system;\n"
        "          workspaceRoot = ./.;\n"
        f'          pythonVersion = "{python_version}";\n'
        f"          dockerImageSpec = {_to_nix(docker_image_spec)};\n"
        "        };\n"
        "      in\n"
        "      { inherit (project) packages devShells; }\n"
        "    );\n"
        "}\n"
    )


def nix_image_spec(requirements: str, **kwargs: object) -> ImageSpec:
    return ImageSpec(builder="nix", requirements=requirements, nix=True, **kwargs)


def _local_nix_system() -> str:
    os_suffix = "darwin" if platform.system() == "Darwin" else "linux"
    machine_map = {
        "aarch64": f"aarch64-{os_suffix}",
        "arm64": f"aarch64-{os_suffix}",
        "x86_64": f"x86_64-{os_suffix}",
    }
    return machine_map.get(platform.machine(), f"x86_64-{os_suffix}")


def _to_nix(value: object, *, indent: int = 10) -> str:
    space = " " * indent
    child_space = " " * (indent + 2)

    if isinstance(value, dict):
        if not value:
            return "{ }"
        lines = ["{"]
        for k, v in value.items():
            lines.append(f"{child_space}{k} = {_to_nix(v, indent=indent + 2)};")
        lines.append(f"{space}}}")
        return "\n".join(lines)

    if isinstance(value, list):
        if not value:
            return "[ ]"
        return "[ " + " ".join(_to_nix(item, indent=indent) for item in value) + " ]"

    if isinstance(value, str):
        return json.dumps(value)

    if isinstance(value, bool):
        return "true" if value else "false"

    if value is None:
        return "null"

    return json.dumps(value)


def _redact_creds(command: List[str]) -> List[str]:
    redacted = list(command)
    for i, arg in enumerate(redacted):
        if arg == "--dest-creds" and i + 1 < len(redacted):
            redacted[i + 1] = "[REDACTED]"
    return redacted
