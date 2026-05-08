from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path
from unittest import mock

import pytest
import toml

from flytekit.image_spec.image_spec import ImageSpec
from flytekit.image_spec.nix_builder import NixImageSpecBuilder, nix_image_spec


class PreservedTemporaryDirectory:
    def __init__(self, path: Path):
        self.path = path

    def __enter__(self) -> str:
        self.path.mkdir(parents=True, exist_ok=True)
        return str(self.path)

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        return None


def _create_uv_project(tmp_path: Path, *, with_flake: bool = False) -> Path:
    pyproject = {
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
    with open(tmp_path / "pyproject.toml", "w") as f:
        toml.dump(pyproject, f)

    lock_data = {
        "version": 1,
        "requires-python": ">=3.10",
        "package": [
            {
                "name": "requests",
                "version": "2.31.0",
                "source": {"registry": "https://pypi.org/simple"},
            },
            {
                "name": "test-project",
                "version": "0.1.0",
                "source": {"editable": "."},
                "dependencies": [{"name": "requests"}],
                "metadata": {
                    "requires-dist": [
                        {"name": "requests", "specifier": "==2.31.0"},
                    ],
                },
            },
        ],
    }
    lock_path = tmp_path / "uv.lock"
    with open(lock_path, "w") as f:
        toml.dump(lock_data, f)

    if with_flake:
        (tmp_path / "flake.nix").write_text('{ outputs = { ... }: {}; }')
        (tmp_path / "flake.lock").write_text("{}")

    subprocess.run(["git", "init", str(tmp_path)], capture_output=True, check=True)
    subprocess.run(["git", "-C", str(tmp_path), "add", "."], capture_output=True, check=True)

    return lock_path


class TestNixImageSpecBuilderValidation:
    def test_rejects_non_uv_lock(self):
        spec = ImageSpec(name="test", requirements="requirements.txt")
        with pytest.raises(ValueError, match="only supports uv.lock"):
            NixImageSpecBuilder().build_image(spec)

    def test_rejects_missing_requirements(self):
        spec = ImageSpec(name="test")
        with pytest.raises(ValueError, match="only supports uv.lock"):
            NixImageSpecBuilder().build_image(spec)

    def test_rejects_unsupported_parameters(self):
        with tempfile.TemporaryDirectory() as d:
            lock_path = _create_uv_project(Path(d))
            spec = ImageSpec(
                name="test",
                requirements=str(lock_path),
                packages=["numpy"],
                apt_packages=["git"],
            )
            with pytest.raises(ValueError, match="does not support"):
                NixImageSpecBuilder().build_image(spec)

    def test_rejects_unsupported_python_version(self):
        with tempfile.TemporaryDirectory() as d:
            lock_path = _create_uv_project(Path(d))
            spec = ImageSpec(
                name="test",
                requirements=str(lock_path),
                python_version="3.8",
            )
            with pytest.raises(ValueError, match="Unsupported python_version"):
                NixImageSpecBuilder().build_image(spec)

    def test_rejects_unsupported_platform(self):
        with tempfile.TemporaryDirectory() as d:
            lock_path = _create_uv_project(Path(d))
            spec = ImageSpec(
                name="test",
                requirements=str(lock_path),
                platform="linux/s390x",
            )
            with pytest.raises(ValueError, match="Unsupported platform"):
                NixImageSpecBuilder().build_image(spec)

    @mock.patch("flytekit.image_spec.nix_builder.subprocess")
    @mock.patch("flytekit.image_spec.default_builder.subprocess.run")
    @mock.patch("flytekit.image_spec.nix_builder.shutil.which", return_value="/nix/bin/nix")
    def test_accepts_all_supported_python_versions(self, _mock_which, _mock_export, mock_subprocess):
        mock_subprocess.run.return_value = mock.Mock(returncode=0)

        for version in ("3.9", "3.10", "3.11", "3.12"):
            with tempfile.TemporaryDirectory() as d:
                lock_path = _create_uv_project(Path(d), with_flake=True)
                spec = ImageSpec(
                    name="test",
                    requirements=str(lock_path),
                    python_version=version,
                    nix=True,
                )
                assert NixImageSpecBuilder().build_image(spec) == spec.image_name()


class TestBuildImage:
    @mock.patch.dict("os.environ", {"FLYTE_PUSH_IMAGE_SPEC": "false"})
    @mock.patch("flytekit.image_spec.nix_builder.subprocess")
    @mock.patch("flytekit.image_spec.default_builder.subprocess.run")
    @mock.patch("flytekit.image_spec.nix_builder.shutil.which", return_value="/nix/bin/nix")
    def test_build_local_no_push(self, _mock_which, _mock_export, mock_subprocess):
        mock_subprocess.run.return_value = mock.Mock(returncode=0)

        with tempfile.TemporaryDirectory() as d:
            lock_path = _create_uv_project(Path(d), with_flake=True)
            spec = ImageSpec(
                name="test",
                requirements=str(lock_path),
                platform="linux/amd64",
                nix=True,
            )
            result = NixImageSpecBuilder().build_image(spec)

        assert result == spec.image_name()
        cmd = mock_subprocess.run.call_args[0][0]
        assert cmd[0] == "nix"
        assert cmd[1] == "build"
        assert "packages.x86_64-linux.docker" in cmd[2]

    @mock.patch.dict("os.environ", {"FLYTE_PUSH_IMAGE_SPEC": "true"})
    @mock.patch("flytekit.image_spec.nix_builder.subprocess")
    @mock.patch("flytekit.image_spec.default_builder.subprocess.run")
    @mock.patch("flytekit.image_spec.nix_builder.shutil.which", return_value="/nix/bin/nix")
    def test_build_push_to_ecr(self, _mock_which, _mock_export, mock_subprocess):
        mock_ecr_result = mock.Mock(returncode=0, stdout="ecr-token-value\n")
        mock_build_result = mock.Mock(returncode=0)
        mock_subprocess.run.side_effect = [mock_ecr_result, mock_build_result]

        with tempfile.TemporaryDirectory() as d:
            lock_path = _create_uv_project(Path(d), with_flake=True)
            spec = ImageSpec(
                name="test",
                requirements=str(lock_path),
                registry="472386928882.dkr.ecr.us-west-2.amazonaws.com/exa",
                platform="linux/amd64",
                nix=True,
            )
            result = NixImageSpecBuilder().build_image(spec)

        assert result == spec.image_name()
        ecr_call, build_call = mock_subprocess.run.call_args_list
        assert "get-login-password" in ecr_call[0][0]
        cmd = build_call[0][0]
        assert cmd[0] == "nix"
        assert cmd[1] == "run"
        assert "copyTo" in cmd[2]
        assert f"docker://{spec.image_name()}" in cmd

    @mock.patch("flytekit.image_spec.nix_builder.shutil.which", return_value=None)
    def test_raises_when_nix_missing(self, _mock_which):
        with tempfile.TemporaryDirectory() as d:
            lock_path = _create_uv_project(Path(d))
            spec = ImageSpec(
                name="test",
                requirements=str(lock_path),
                nix=True,
            )
            with pytest.raises(RuntimeError, match="Nix is not installed"):
                NixImageSpecBuilder().build_image(spec)

    @mock.patch.dict("os.environ", {"FLYTE_PUSH_IMAGE_SPEC": "false"})
    @mock.patch("flytekit.image_spec.nix_builder.subprocess")
    @mock.patch("flytekit.image_spec.default_builder.subprocess.run")
    @mock.patch("flytekit.image_spec.nix_builder.shutil.which", return_value="/nix/bin/nix")
    def test_raises_on_build_failure(self, _mock_which, _mock_export, mock_subprocess):
        mock_subprocess.run.return_value = mock.Mock(returncode=1)

        with tempfile.TemporaryDirectory() as d:
            lock_path = _create_uv_project(Path(d), with_flake=True)
            spec = ImageSpec(
                name="test",
                requirements=str(lock_path),
                nix=True,
            )
            with pytest.raises(RuntimeError, match="Build command failed"):
                NixImageSpecBuilder().build_image(spec)

    @mock.patch.dict(
        "os.environ",
        {
            "FLYTE_PUSH_IMAGE_SPEC": "false",
            "FLYTEKIT_NIX_PYTHON_FLAKE": "/nix/flakes/python",
        },
    )
    @mock.patch("flytekit.image_spec.nix_builder.subprocess")
    @mock.patch("flytekit.image_spec.default_builder.subprocess.run")
    @mock.patch("flytekit.image_spec.nix_builder.shutil.which", return_value="/nix/bin/nix")
    def test_generates_flake_when_project_has_none(self, _mock_which, _mock_export, mock_subprocess):
        mock_subprocess.run.return_value = mock.Mock(returncode=0)

        with tempfile.TemporaryDirectory() as project_dir, tempfile.TemporaryDirectory() as build_dir:
            lock_path = _create_uv_project(Path(project_dir))
            spec = ImageSpec(
                name="my-image",
                requirements=str(lock_path),
                python_version="3.12",
                env={"FOO": "bar"},
                entrypoint=["python", "-m", "myapp"],
                nix=True,
            )
            preserved_tmp = PreservedTemporaryDirectory(Path(build_dir))
            with mock.patch("flytekit.image_spec.nix_builder.tempfile.TemporaryDirectory", return_value=preserved_tmp):
                assert NixImageSpecBuilder().build_image(spec) == spec.image_name()

            flake = (Path(build_dir) / "flake.nix").read_text()

        assert "makePythonProject" in flake
        assert 'pythonVersion = "python312"' in flake
        assert 'FOO = "bar"' in flake
        assert '"python"' in flake
        assert '"myapp"' in flake
        assert 'python-flake.url = "path:/nix/flakes/python"' in flake

    @mock.patch.dict("os.environ", {"FLYTE_PUSH_IMAGE_SPEC": "false"}, clear=True)
    @mock.patch("flytekit.image_spec.nix_builder.subprocess")
    @mock.patch("flytekit.image_spec.default_builder.subprocess.run")
    @mock.patch("flytekit.image_spec.nix_builder.shutil.which", return_value="/nix/bin/nix")
    def test_requires_python_flake_env_when_project_has_no_flake(self, _mock_which, _mock_export, mock_subprocess):
        mock_subprocess.run.return_value = mock.Mock(returncode=0)

        with tempfile.TemporaryDirectory() as d:
            lock_path = _create_uv_project(Path(d))
            spec = ImageSpec(
                name="test",
                requirements=str(lock_path),
                nix=True,
            )
            with pytest.raises(RuntimeError, match="FLYTEKIT_NIX_PYTHON_FLAKE"):
                NixImageSpecBuilder().build_image(spec)


class TestNixImageSpecFactory:
    def test_sets_builder_and_nix(self):
        spec = nix_image_spec("uv.lock", name="test")
        assert spec.builder == "nix"
        assert spec.nix is True
        assert spec.requirements == "uv.lock"

    def test_passes_kwargs(self):
        spec = nix_image_spec(
            "uv.lock",
            name="img",
            registry="example.com/repo",
            platform="linux/arm64",
        )
        assert spec.registry == "example.com/repo"
        assert spec.platform == "linux/arm64"
