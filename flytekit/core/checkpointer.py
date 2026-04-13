import io
import logging
import tempfile
import typing
from abc import abstractmethod
from pathlib import Path

logger = logging.getLogger(__name__)


class Checkpoint(object):
    """
    Base class for Checkpoint system. Checkpoint system allows reading and writing custom checkpoints from user
    scripts
    """

    @abstractmethod
    def prev_exists(self) -> bool:
        raise NotImplementedError("Use one of the derived classes")

    @abstractmethod
    def restore(self, path: typing.Union[Path, str]) -> typing.Optional[Path]:
        """
        Given a path, if a previous checkpoint exists, will be downloaded to this path.
        If download is successful the downloaded path is returned

        .. note:

            Download will not be performed, if the checkpoint was previously restored. The method will return the
            previously downloaded path.

        """
        raise NotImplementedError("Use one of the derived classes")

    @abstractmethod
    def save(self, cp: typing.Union[Path, str, io.BufferedReader]):
        """
        Args:
            cp: Checkpoint file (path, str path or a io.BufferedReader)

        Usage: If you have a io.BufferedReader then the following should work

        .. code-block: python

            with input_file.open(mode="rb") as b:
                checkpointer.save(b)
        """
        raise NotImplementedError("Use one of the derived classes")

    @abstractmethod
    def read(self) -> typing.Optional[bytes]:
        """
        This should only be used if there is a singular checkpoint file written. If more than one checkpoint file is
        found, this will raise a ValueError
        """
        raise NotImplementedError("Use one of the derived classes")

    @abstractmethod
    def write(self, b: bytes):
        """
        This will overwrite the checkpoint. It can be retrieved using read or restore
        """
        raise NotImplementedError("Use one of the derived classes")


class SyncCheckpoint(Checkpoint):
    """
    This class is NOT THREAD-SAFE!
    Sync Checkpoint, will synchronously checkpoint a user given file or folder.
    It will also synchronously download / restore previous checkpoints, when restore is invoked.

    TODO: Implement an async checkpoint system
    """

    SRC_LOCAL_FOLDER = "prev_cp"
    TMP_DST_PATH = "_dst_cp"

    def __init__(
        self,
        checkpoint_dest: str,
        checkpoint_src: typing.Optional[typing.Union[str, typing.List[str]]] = None,
    ):
        """
        Args:
            checkpoint_src: One or more paths to previous checkpoint directories, tried in order.
                Accepts a single path string or a list of path strings (most-recent-attempt first).
                The first path that contains data wins.
            checkpoint_dest: Location where the new checkpoint should be copied to.
        """
        self._checkpoint_dest = checkpoint_dest
        if checkpoint_src is None:
            self._checkpoint_srcs: typing.List[str] = []
        elif isinstance(checkpoint_src, str):
            self._checkpoint_srcs = [checkpoint_src] if checkpoint_src != "" else []
        else:
            self._checkpoint_srcs = [s for s in checkpoint_src if s and s != ""]
        # Keep for backwards-compat: first candidate (or None)
        self._checkpoint_src = self._checkpoint_srcs[0] if self._checkpoint_srcs else None
        self._td = tempfile.TemporaryDirectory()
        self._prev_download_path: typing.Optional[Path] = None

    def __del__(self):
        self._td.cleanup()

    def prev_exists(self) -> bool:
        return len(self._checkpoint_srcs) > 0

    def restore(self, path: typing.Optional[typing.Union[Path, str]] = None) -> typing.Optional[Path]:
        """Download a previous checkpoint, walking back through attempts until one succeeds.

        Tries each candidate in ``self._checkpoint_srcs`` (most-recent first). The first
        path that contains data is used. On success the checkpoint is also copied to
        ``checkpoint_dest`` so the *next* attempt can find it without walking back again.

        Args:
            path: Local directory to download into. A temp directory is used when *None*.

        Returns:
            The local path where the checkpoint was restored, or *None* if no candidates exist.

        Raises:
            ValueError: If *path* is not a directory.
            FlyteDataNotFoundException: If none of the candidates contain data.
        """
        # We have to lazy load, until we fix the imports
        from flytekit.core.context_manager import FlyteContextManager

        if not self._checkpoint_srcs:
            return None

        if self._prev_download_path:
            return self._prev_download_path

        if path is None:
            p = Path(self._td.name)
            path = p / self.SRC_LOCAL_FOLDER
            path.mkdir(exist_ok=True)
        elif isinstance(path, str):
            path = Path(path)

        if not path.is_dir():
            raise ValueError("Checkpoints can be restored to a directory only.")

        fa = FlyteContextManager.current_context().file_access
        last_err: typing.Optional[Exception] = None

        for idx, src in enumerate(self._checkpoint_srcs):
            try:
                fa.download_directory(src, str(path))
                # Check that the download actually produced files
                if any(path.iterdir()):
                    logger.info(f"Checkpoint restored from candidate {idx}: {src}")
                    self._prev_download_path = path
                    self._auto_forward(fa, path)
                    return self._prev_download_path
                # Empty directory — treat as missing and try the next candidate
                logger.debug(f"Checkpoint candidate {idx} was empty: {src}")
            except Exception as e:
                logger.debug(f"Checkpoint candidate {idx} failed ({src}): {e}")
                last_err = e

        # None of the candidates worked. Re-raise the last download error if we had one,
        # otherwise fall through to the original single-source behaviour so existing
        # callers see the same exception they always did.
        if last_err is not None:
            raise last_err

        # All candidates were empty directories — download from the first source so the
        # original behaviour (returning the path) is preserved.
        fa.download_directory(self._checkpoint_srcs[0], str(path))
        self._prev_download_path = path
        return self._prev_download_path

    def _auto_forward(self, fa: typing.Any, local_path: Path) -> None:
        """Copy a successfully restored checkpoint to this attempt's dest path.

        This "auto-forward" ensures that the next retry can always find a valid
        checkpoint at attempt N's path even if N is killed before writing its own.
        """
        try:
            if self._checkpoint_dest:
                fa.upload_directory(str(local_path), self._checkpoint_dest)
                logger.debug(f"Auto-forwarded checkpoint to {self._checkpoint_dest}")
        except Exception:
            # Best-effort — don't let a forwarding failure block the restore.
            logger.warning("Failed to auto-forward checkpoint to dest", exc_info=True)

    def save(self, cp: typing.Union[Path, str, io.BufferedReader]):
        # We have to lazy load, until we fix the imports
        from flytekit.core.context_manager import FlyteContextManager

        fa = FlyteContextManager.current_context().file_access
        if isinstance(cp, (Path, str)):
            if isinstance(cp, str):
                cp = Path(cp)
            if cp.is_dir():
                fa.upload_directory(str(cp), self._checkpoint_dest)
            else:
                fname = cp.stem + cp.suffix
                rpath = fa._default_remote.sep.join([str(self._checkpoint_dest), fname])
                fa.upload(str(cp), rpath)
            return

        if not isinstance(cp, io.IOBase):
            raise ValueError(f"Only a valid path or IOBase type (reader) should be provided, received {type(cp)}")

        p = Path(self._td.name)
        dest_cp = p / self.TMP_DST_PATH
        with dest_cp.open("wb") as f:
            f.write(cp.read())

        rpath = fa._default_remote.sep.join([str(self._checkpoint_dest), self.TMP_DST_PATH])
        fa.upload(str(dest_cp), rpath)

    def read(self) -> typing.Optional[bytes]:
        p = self.restore()
        if p is None:
            return None
        files = list(p.iterdir())
        if len(files) == 0:
            return None
        if len(files) > 1:
            raise ValueError(f"Expected exactly one checkpoint - found {len(files)}")
        f = files[0]
        return f.read_bytes()

    def write(self, b: bytes):
        p = io.BytesIO(b)
        f = typing.cast(io.BufferedReader, p)
        self.save(f)
