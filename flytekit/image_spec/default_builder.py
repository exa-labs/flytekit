import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import warnings
from pathlib import Path
from string import Template
from subprocess import run
from typing import ClassVar, List, NamedTuple

import click
import toml

from flytekit.constants import CopyFileDetection
from flytekit.image_spec.image_spec import (
    _F_IMG_ID,
    ImageSpec,
    ImageSpecBuilder,
)
from flytekit.tools.ignore import DockerIgnore, GitIgnore, IgnoreGroup, StandardIgnore
from flytekit.tools.script_mode import ls_files

UV_LOCK_INSTALL_TEMPLATE = Template(
    """\
RUN --mount=type=cache,sharing=locked,mode=0777,target=/root/.cache/uv,id=uv \
    --mount=from=uv,source=/uv,target=/usr/bin/uv \
    --mount=type=bind,target=uv.lock,src=uv.lock \
    --mount=type=bind,target=pyproject.toml,src=pyproject.toml \
    uv sync $PIP_INSTALL_ARGS

# Update PATH and UV_PYTHON to point to the venv created by uv sync
ENV PATH="/root/.venv/bin:$$PATH" \
    UV_PYTHON=/root/.venv/bin/python
"""
)

# Modified UV_LOCK_INSTALL_TEMPLATE that installs only local packages
UV_LOCK_INSTALL_LOCAL_ONLY_TEMPLATE = Template(
    """\
# Install only local packages into existing venv
RUN --mount=type=cache,sharing=locked,mode=0777,target=/root/.cache/uv,id=uv \
    --mount=from=uv,source=/uv,target=/usr/bin/uv \
    --mount=type=bind,target=local_packages.txt,src=local_packages.txt \
    uv pip install $PIP_INSTALL_ARGS --requirement local_packages.txt
"""
)

POETRY_LOCK_TEMPLATE = Template(
    """\
RUN --mount=type=cache,sharing=locked,mode=0777,target=/root/.cache/uv,id=uv \
    --mount=from=uv,source=/uv,target=/usr/bin/uv \
    uv pip install poetry

ENV POETRY_CACHE_DIR=/tmp/poetry_cache \
    POETRY_VIRTUALENVS_IN_PROJECT=true

# poetry install does not work running in /, so we move to /root to create the venv
WORKDIR /root

RUN --mount=type=cache,sharing=locked,mode=0777,target=/tmp/poetry_cache,id=poetry \
    --mount=type=bind,target=poetry.lock,src=poetry.lock \
    --mount=type=bind,target=pyproject.toml,src=pyproject.toml \
    poetry install $PIP_INSTALL_ARGS

WORKDIR /

# Update PATH and UV_PYTHON to point to venv
ENV PATH="/root/.venv/bin:$$PATH" \
    UV_PYTHON=/root/.venv/bin/python
"""
)

UV_PYTHON_INSTALL_COMMAND_TEMPLATE = Template(
    """\
RUN --mount=type=cache,sharing=locked,mode=0777,target=/root/.cache/uv,id=uv \
    --mount=from=uv,source=/uv,target=/usr/bin/uv \
    --mount=type=bind,target=requirements_uv.txt,src=requirements_uv.txt \
    uv pip install $PIP_INSTALL_ARGS
"""
)


APT_INSTALL_COMMAND_TEMPLATE = Template("""\
RUN --mount=type=cache,sharing=locked,mode=0777,target=/var/cache/apt,id=apt \
    apt-get update && apt-get install -y --no-install-recommends \
    $APT_PACKAGES
""")

MICROMAMBA_INSTALL_COMMAND_TEMPLATE = Template("""\
RUN --mount=type=cache,sharing=locked,mode=0777,target=/opt/micromamba/pkgs,\
id=micromamba \
    --mount=from=micromamba,source=/usr/bin/micromamba,target=/usr/bin/micromamba \
    micromamba config set use_lockfiles False && \
    micromamba create -n runtime --root-prefix /opt/micromamba \
    -c conda-forge $CONDA_CHANNELS \
    python=$PYTHON_VERSION $CONDA_PACKAGES
""")

DOCKER_FILE_TEMPLATE = Template("""\
#syntax=docker/dockerfile:1.5
FROM ghcr.io/astral-sh/uv:0.5.31 as uv
FROM mambaorg/micromamba:2.0.3-debian12-slim as micromamba

FROM $BASE_IMAGE

USER root
$APT_INSTALL_COMMAND
RUN --mount=from=micromamba,source=/etc/ssl/certs/ca-certificates.crt,target=/tmp/ca-certificates.crt \
    [ -f /etc/ssl/certs/ca-certificates.crt ] || \
    mkdir -p /etc/ssl/certs/ && cp /tmp/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt

RUN id -u flytekit || useradd --create-home --shell /bin/bash flytekit
RUN chown -R flytekit /root && chown -R flytekit /home

$INSTALL_PYTHON_TEMPLATE

# Configure user space
ENV PATH="$EXTRA_PATH:$$PATH" \
    UV_PYTHON=$PYTHON_EXEC \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    FLYTE_SDK_RICH_TRACEBACKS=0 \
    SSL_CERT_DIR=/etc/ssl/certs \
    $ENV

$UV_VENV_INSTALL

$COPY_LOCAL_PACKAGES

$UV_PYTHON_INSTALL_COMMAND

WORKDIR /

# Adds nvidia just in case it exists
ENV PATH="$$PATH:/usr/local/nvidia/bin:/usr/local/cuda/bin" \
    LD_LIBRARY_PATH="/usr/local/nvidia/lib64:$$LD_LIBRARY_PATH"

$ENTRYPOINT

$COPY_COMMAND_RUNTIME

$EXTRA_COPY_CMDS

RUN --mount=type=cache,sharing=locked,mode=0777,target=/root/.cache/uv,id=uv \
    --mount=from=uv,source=/uv,target=/usr/bin/uv $RUN_COMMANDS

WORKDIR /root
SHELL ["/bin/bash", "-c"]

USER flytekit
RUN mkdir -p $$HOME && \
    echo "export PATH=$$PATH" >> $$HOME/.profile
""")


def get_flytekit_for_pypi():
    """Get flytekit version on PyPI."""
    from flytekit import __version__

    if not __version__ or "dev" in __version__:
        return "flytekit"
    else:
        return f"flytekit=={__version__}"


_PACKAGE_NAME_RE = re.compile(r"^[\w-]+")


def _is_flytekit(package: str) -> bool:
    """Return True if `package` is flytekit. `package` is expected to be a valid version
    spec. i.e. `flytekit==1.12.3`, `flytekit`, `flytekit~=1.12.3`.
    """
    m = _PACKAGE_NAME_RE.match(package)
    if not m:
        return False
    name = m.group()
    return name == "flytekit"


def _find_git_root(start_path: str):
    """Find the root directory of the git repository."""
    current = Path(start_path).resolve()

    while current != current.parent:
        if (current / ".git").exists():
            return current
        current = current.parent


def _copy_local_packages_and_update_lock(image_spec: ImageSpec, tmp_dir: Path):
    """Copy local packages into the Docker build context and update their paths in the lock file."""
    if not image_spec.requirements or not os.path.basename(image_spec.requirements) == "uv.lock":
        return

    # Read the lock file and parse as TOML
    requirements = str(image_spec.requirements)
    with open(requirements) as f:
        lock_content = f.read()
        lock_data = toml.loads(lock_content)

    lock_dir = Path(os.path.dirname(requirements))

    # Copy and update pyproject.toml from lock file directory
    pyproject_toml_src = lock_dir / "pyproject.toml"
    if not pyproject_toml_src.exists():
        raise ValueError("pyproject.toml must exist in the same directory as uv.lock")

    with open(pyproject_toml_src) as f:
        pyproject_content = f.read()

    # Create a directory for local packages
    local_packages_dir = tmp_dir / "local_packages"
    local_packages_dir.mkdir(exist_ok=True)

    # Track local packages for separate installation
    local_packages_list = []

    # Copy each local package from the lock file and update its path
    for package in lock_data["package"]:
        source = package["source"]

        if "directory" in source:
            source_type = "directory"
        elif "editable" in source:
            source_type = "editable"
        else:
            continue

        if source[source_type] == "." and not image_spec.install_project:
            continue

        # Get the absolute path of the package
        package_path = (lock_dir / source[source_type]).resolve()
        if not package_path.exists():
            raise ValueError(f"Local package path does not exist: {package_path}")

        git_root = _find_git_root(package_path)
        if git_root is None:
            raise ValueError(f"Could not find git root for {package_path}")

        # Get the relative path components and sanitize them
        rel_path = os.path.relpath(path=package_path, start=git_root)
        target_path = local_packages_dir / rel_path

        # Create parent directories if they don't exist
        target_path.parent.mkdir(parents=True, exist_ok=True)

        # Copy the package to the build context with ignore patterns
        if package_path.is_dir():
            # Set up ignore patterns for the package directory
            ignore_group = IgnoreGroup(str(package_path), [GitIgnore, DockerIgnore, StandardIgnore])

            # Get list of files to copy respecting ignore patterns
            files_to_copy, _ = ls_files(
                str(package_path),
                CopyFileDetection.ALL,  # Copy all files that aren't ignored
                deref_symlinks=False,
                ignore_group=ignore_group,
            )

            # Copy each file individually
            for file_to_copy in files_to_copy:
                file_rel_path = os.path.relpath(file_to_copy, start=str(package_path))
                file_target_path = target_path / file_rel_path
                file_target_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(file_to_copy, file_target_path)
        else:
            shutil.copy2(package_path, target_path)

        # Update the paths in both files
        old_path = source[source_type]
        new_path = f"/root/local_packages/{rel_path}"
        lock_content = lock_content.replace(f'{source_type} = "{old_path}"', f'{source_type} = "{new_path}"')
        lock_content = lock_content.replace(f'directory = "{old_path}"', f'directory = "{new_path}"')
        lock_content = lock_content.replace(f'editable = "{old_path}"', f'editable = "{new_path}"')
        pyproject_content = pyproject_content.replace(f'path = "{old_path}"', f'path = "{new_path}"')

        # Add to local packages list
        if source_type == "editable":
            local_packages_list.append(f"-e {new_path}")
        else:
            local_packages_list.append(new_path)

    # Write the updated files
    lock_path = tmp_dir / "uv.lock"
    lock_path.write_text(lock_content)

    pyproject_path = tmp_dir / "pyproject.toml"
    pyproject_path.write_text(pyproject_content)

    requirements_path = tmp_dir / "requirements.txt"

    # Export requirements from uv.lock to requirements.txt format
    # This excludes editable installs (-e) and local relative path dependencies
    requirements_export_cmd = rf"uv export --format requirements-txt | grep -v '^\(-e\|\.\./\)' > {requirements_path}"
    subprocess.run(requirements_export_cmd, shell=True, check=True)

    # Write local packages file
    local_packages_path = tmp_dir / "local_packages.txt"
    local_packages_path.write_text("\n".join(local_packages_list))


def _copy_lock_files_into_context(image_spec: ImageSpec, lock_file: str, tmp_dir: Path):
    if image_spec.packages is not None:
        msg = f"Support for {lock_file} files and packages is mutually exclusive"
        raise ValueError(msg)

    # Copy and update local packages first
    _copy_local_packages_and_update_lock(image_spec, tmp_dir)


def prepare_uv_lock_command(image_spec: ImageSpec, pip_install_args: List[str], tmp_dir: Path) -> str:
    warnings.warn("uv.lock support is experimental", UserWarning)

    _copy_lock_files_into_context(image_spec, "uv.lock", tmp_dir)

    # Use the same pip install args
    pip_install_args_str = " ".join(pip_install_args)

    # Return template that only installs local packages
    return UV_LOCK_INSTALL_LOCAL_ONLY_TEMPLATE.substitute(PIP_INSTALL_ARGS=pip_install_args_str)


def prepare_poetry_lock_command(image_spec: ImageSpec, pip_install_args: List[str], tmp_dir: Path) -> str:
    _copy_lock_files_into_context(image_spec, "poetry.lock", tmp_dir)

    # --no-root: Do not install the current project
    pip_install_args.extend(["--no-root"])
    pip_install_args_str = " ".join(pip_install_args)
    return POETRY_LOCK_TEMPLATE.substitute(PIP_INSTALL_ARGS=pip_install_args_str)


def prepare_python_install(image_spec: ImageSpec, tmp_dir: Path) -> str:
    pip_install_args = []
    if image_spec.pip_index:
        pip_install_args.append(f"--index-url {image_spec.pip_index}")

    if image_spec.pip_extra_index_url:
        extra_urls = [f"--extra-index-url {url}" for url in image_spec.pip_extra_index_url]
        pip_install_args.extend(extra_urls)

    if image_spec.pip_extra_args:
        pip_install_args.append(image_spec.pip_extra_args)

    requirements = []
    if image_spec.requirements:
        requirement_basename = os.path.basename(image_spec.requirements)
        if requirement_basename == "uv.lock":
            return prepare_uv_lock_command(image_spec, pip_install_args, tmp_dir)
        elif requirement_basename == "poetry.lock":
            return prepare_poetry_lock_command(image_spec, pip_install_args, tmp_dir)

        # Assume this is a requirements.txt file
        with open(image_spec.requirements) as f:
            requirements.extend([line.strip() for line in f.readlines()])

    if image_spec.packages:
        requirements.extend(image_spec.packages)

    # Adds flytekit if it is not specified
    if not any(_is_flytekit(package) for package in requirements):
        requirements.append(get_flytekit_for_pypi())

    requirements_uv_path = tmp_dir / "requirements_uv.txt"
    requirements_uv_path.write_text("\n".join(requirements))
    pip_install_args.extend(["--requirement", "requirements_uv.txt"])

    pip_install_args = " ".join(pip_install_args)

    return UV_PYTHON_INSTALL_COMMAND_TEMPLATE.substitute(PIP_INSTALL_ARGS=pip_install_args)


class _PythonInstallTemplate(NamedTuple):
    python_exec: str
    template: str
    extra_path: str


def prepare_python_executable(image_spec: ImageSpec) -> _PythonInstallTemplate:
    if image_spec.python_exec:
        if image_spec.conda_channels:
            raise ValueError("conda_channels is not supported with python_exec")
        if image_spec.conda_packages:
            raise ValueError("conda_packages is not supported with python_exec")
        return _PythonInstallTemplate(python_exec=image_spec.python_exec, template="", extra_path="")

    conda_packages = image_spec.conda_packages or []
    conda_channels = image_spec.conda_channels or []

    if conda_packages:
        conda_packages_concat = " ".join(conda_packages)
    else:
        conda_packages_concat = ""

    if conda_channels:
        conda_channels_concat = " ".join(f"-c {channel}" for channel in conda_channels)
    else:
        conda_channels_concat = ""

    if image_spec.python_version:
        python_version = image_spec.python_version
    else:
        python_version = f"{sys.version_info.major}.{sys.version_info.minor}"

    template = MICROMAMBA_INSTALL_COMMAND_TEMPLATE.substitute(
        PYTHON_VERSION=python_version,
        CONDA_PACKAGES=conda_packages_concat,
        CONDA_CHANNELS=conda_channels_concat,
    )
    return _PythonInstallTemplate(
        python_exec="/opt/micromamba/envs/runtime/bin/python",
        template=template,
        extra_path="/opt/micromamba/envs/runtime/bin",
    )


def create_docker_context(image_spec: ImageSpec, tmp_dir: Path):
    """Populate tmp_dir with Dockerfile as specified by the `image_spec`."""
    base_image = image_spec.base_image or "debian:bookworm-slim"

    if image_spec.cuda is not None or image_spec.cudnn is not None:
        msg = (
            "cuda and cudnn do not need to be specified. If you are installed "
            "a GPU accelerated library on PyPI, then it likely will install cuda "
            "from PyPI."
            "With conda you can installed cuda from the `nvidia` channel by adding `nvidia` to "
            "ImageSpec.conda_channels and adding packages from "
            "https://anaconda.org/nvidia into ImageSpec.conda_packages. If you require "
            "cuda for non-python dependencies, you can set a `base_image` with cuda "
            "preinstalled."
        )
        raise ValueError(msg)

    # Check if we're using uv.lock
    is_uv_lock = image_spec.requirements and os.path.basename(image_spec.requirements) == "uv.lock"

    uv_python_install_command = prepare_python_install(image_spec, tmp_dir)
    env_dict = {"PYTHONPATH": "/root", _F_IMG_ID: image_spec.id}

    if image_spec.env:
        env_dict.update(image_spec.env)

    env = " ".join(f"{k}={v}" for k, v in env_dict.items())

    apt_packages = []
    if image_spec.apt_packages:
        apt_packages.extend(image_spec.apt_packages)

    if apt_packages:
        apt_install_command = APT_INSTALL_COMMAND_TEMPLATE.substitute(APT_PACKAGES=" ".join(apt_packages))
    else:
        apt_install_command = ""

    if image_spec.source_copy_mode is not None and image_spec.source_copy_mode != CopyFileDetection.NO_COPY:
        if not image_spec.source_root:
            raise ValueError(f"Field source_root for {image_spec} must be set when copy is set")

        source_path = tmp_dir / "src"
        source_path.mkdir(parents=True, exist_ok=True)
        # todo: See note in we should pipe through ignores from the command line here at some point.
        #  what about deref_symlink?
        ignore = IgnoreGroup(image_spec.source_root, [GitIgnore, DockerIgnore, StandardIgnore])

        ls, _ = ls_files(
            str(image_spec.source_root),
            image_spec.source_copy_mode,
            deref_symlinks=False,
            ignore_group=ignore,
        )

        for file_to_copy in ls:
            rel_path = os.path.relpath(file_to_copy, start=str(image_spec.source_root))
            Path(source_path / rel_path).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy(
                file_to_copy,
                source_path / rel_path,
            )

        copy_command_runtime = "COPY --chown=flytekit ./src /root"
    else:
        copy_command_runtime = ""

    python_install_template = prepare_python_executable(image_spec=image_spec)

    if image_spec.entrypoint is None:
        entrypoint = ""
    else:
        entrypoint = f"ENTRYPOINT {json.dumps(image_spec.entrypoint)}"

    if image_spec.commands:
        run_commands = " && ".join(image_spec.commands)
    else:
        run_commands = ""

    if image_spec.copy:
        copy_commands = []
        for src in image_spec.copy:
            src_path = Path(src)

            if src_path.is_absolute() or ".." in src_path.parts:
                raise ValueError("Absolute paths or paths with '..' are not allowed in COPY command.")

            dst_path = tmp_dir / src_path
            dst_path.parent.mkdir(parents=True, exist_ok=True)

            if src_path.is_dir():
                shutil.copytree(src_path, dst_path, dirs_exist_ok=True)
                copy_commands.append(f"COPY --chown=flytekit {src_path.as_posix()} /root/{src_path.as_posix()}/")
            else:
                shutil.copy(src_path, dst_path)
                copy_commands.append(f"COPY --chown=flytekit {src_path.as_posix()} /root/{src_path.parent.as_posix()}/")

        extra_copy_cmds = "\n".join(copy_commands)
    else:
        extra_copy_cmds = ""

    # Check if local_packages directory exists and is not empty
    local_packages_dir = tmp_dir / "local_packages"
    if local_packages_dir.exists() and any(local_packages_dir.iterdir()):
        copy_local_packages = "COPY --chown=flytekit local_packages /root/local_packages"
    else:
        copy_local_packages = ""

    # Only include the uv venv install section if we're using uv.lock
    if is_uv_lock:
        uv_venv_install = """\
WORKDIR /root
RUN --mount=type=cache,sharing=locked,mode=0777,target=/root/.cache/uv,id=uv \\
    --mount=from=uv,source=/uv,target=/usr/bin/uv \\
    --mount=type=bind,target=requirements.txt,src=requirements.txt \\
    uv venv && uv pip sync requirements.txt"""
    else:
        uv_venv_install = ""

    docker_content = DOCKER_FILE_TEMPLATE.substitute(
        UV_PYTHON_INSTALL_COMMAND=uv_python_install_command,
        APT_INSTALL_COMMAND=apt_install_command,
        INSTALL_PYTHON_TEMPLATE=python_install_template.template,
        EXTRA_PATH=python_install_template.extra_path,
        PYTHON_EXEC=python_install_template.python_exec,
        BASE_IMAGE=base_image,
        ENV=env,
        COPY_COMMAND_RUNTIME=copy_command_runtime,
        ENTRYPOINT=entrypoint,
        RUN_COMMANDS=run_commands,
        EXTRA_COPY_CMDS=extra_copy_cmds,
        COPY_LOCAL_PACKAGES=copy_local_packages,
        UV_VENV_INSTALL=uv_venv_install,
    )

    dockerfile_path = tmp_dir / "Dockerfile"
    dockerfile_path.write_text(docker_content)


class DefaultImageBuilder(ImageSpecBuilder):
    """Image builder using Docker and buildkit."""

    _SUPPORTED_IMAGE_SPEC_PARAMETERS: ClassVar[set] = {
        "id",
        "name",
        "python_version",
        "builder",
        "source_root",
        "source_copy_mode",
        "env",
        "registry",
        "packages",
        "conda_packages",
        "conda_channels",
        "requirements",
        "apt_packages",
        "platform",
        "cuda",
        "cudnn",
        "base_image",
        "pip_index",
        "pip_extra_index_url",
        # "registry_config",
        "commands",
        "copy",
    }

    def build_image(self, image_spec: ImageSpec) -> str:
        return self._build_image(
            image_spec,
            push=os.getenv("FLYTE_PUSH_IMAGE_SPEC", "True").lower() in ("true", "1"),
        )

    def _build_image(self, image_spec: ImageSpec, *, push: bool = True) -> str:
        # For testing, set `push=False`` to just build the image locally and not push to
        # registry
        unsupported_parameters = [
            name
            for name, value in vars(image_spec).items()
            if value is not None and name not in self._SUPPORTED_IMAGE_SPEC_PARAMETERS and not name.startswith("_")
        ]
        if unsupported_parameters:
            msg = f"The following parameters are unsupported and ignored: {unsupported_parameters}"
            warnings.warn(msg, UserWarning, stacklevel=2)

        # Check if build tools are available
        import shutil
        if image_spec.use_depot:
            if not shutil.which("depot"):
                raise RuntimeError(
                    "Depot is not installed or not in PATH. "
                    "Please install depot (https://depot.dev/docs/installation) or use Docker instead by setting use_depot=False"
                )
        else:
            if not shutil.which("docker"):
                raise RuntimeError(
                    "Docker is not installed or not in PATH. "
                    "Please install Docker (https://docs.docker.com/get-docker/) or use depot by setting use_depot=True"
                )
            
            # Check if Docker daemon is running
            try:
                result = run(["docker", "info"], capture_output=True, text=True)
                if result.returncode != 0:
                    raise RuntimeError(
                        f"Docker daemon is not running or not accessible. Error: {result.stderr}\n"
                        "Please start Docker daemon or use depot by setting use_depot=True"
                    )
            except Exception as e:
                raise RuntimeError(
                    f"Failed to check Docker daemon status: {str(e)}\n"
                    "Please ensure Docker is properly installed and running, or use depot by setting use_depot=True"
                )

        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            create_docker_context(image_spec, tmp_path)

            if image_spec.use_depot:
                command = [
                    "depot",
                    "build",
                    "--tag",
                    f"{image_spec.image_name()}",
                    "--platform",
                    image_spec.platform,
                ]
            else:
                command = [
                    "docker",
                    "image",
                    "build",
                    "--tag",
                    f"{image_spec.image_name()}",
                    "--platform",
                    image_spec.platform,
                ]

            if image_spec.registry and push:
                command.append("--push")
            command.append(tmp_dir)

            concat_command = " ".join(command)
            click.secho(f"Run command: {concat_command} ", fg="blue")
            run(command, check=True)
            return image_spec.image_name()
