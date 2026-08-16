"""``TempFileRegistry`` — 0600 temp files under one registry dir, swept at startup.

This is the v2 fix for H17 ("Temp kubeconfig files may leak on ungraceful
shutdown", ``reference-code/seedpod/review/SUMMARY.md``). The v1 pattern being
replaced lives at ``reference-code/seedpod/seedpod/providers/kubernetes.py``
(``tempfile.NamedTemporaryFile(delete=False)`` + ``os.unlink`` in ``finally``,
repeated per call; the ``apply_manifest`` two-file variant at lines 764-797
leaks the kubeconfig file if creating the manifest file raises, and every file
leaks on hard kill) and ``reference-code/seedpod/seedpod/utils/kubectl.py:127-191``.

Spec (docs/design/seam-c-provider.md, "Temp files"): every temp file
(kubeconfig, manifest, known_hosts, kind config) is created ``0600`` under a
registry dir (``$XDG_RUNTIME_DIR/seedpod/`` or ``~/.seedpod/tmp/``), registered,
unlinked on completion or cancellation, and stale entries are swept at startup
(``App.start`` calls ``TempFileRegistry.sweep()`` — coherence review Conflict 15;
conformance C-21 asserts the hygiene).

This module necessarily touches the filesystem. It is a thin, self-contained,
stdlib-only utility with no other ``seedpod.core`` imports; nothing else in
``core/`` may do IO.
"""

import os
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

__all__ = ["TempFileRegistry", "default_registry_dir"]


def default_registry_dir() -> Path:
    """``$XDG_RUNTIME_DIR/seedpod/`` if set, else ``~/.seedpod/tmp/``."""
    runtime_dir = os.environ.get("XDG_RUNTIME_DIR")
    if runtime_dir:
        return Path(runtime_dir) / "seedpod"
    return Path.home() / ".seedpod" / "tmp"


class TempFileRegistry:
    """Creates 0600 temp files in one private dir; unlinks them deterministically.

    The registry *is* the directory: any file found in it at startup is by
    definition stale (a previous process died before its ``finally`` ran) and
    is removed by :meth:`sweep`.
    """

    def __init__(self, root: Path | None = None) -> None:
        self.root = root or default_registry_dir()

    def _ensure_root(self) -> Path:
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        return self.root

    def create(self, content: str | bytes, *, suffix: str = ".yml") -> Path:
        """Write ``content`` to a new 0600 file under the registry dir."""
        root = self._ensure_root()
        fd, name = tempfile.mkstemp(suffix=suffix, prefix="sp-", dir=root)
        try:
            os.fchmod(fd, 0o600)  # mkstemp already opens 0600; make it explicit
            data = content.encode("utf-8") if isinstance(content, str) else content
            os.write(fd, data)
        except BaseException:
            os.close(fd)
            os.unlink(name)
            raise
        os.close(fd)
        return Path(name)

    def unlink(self, path: Path) -> None:
        """Remove a registered file; already-gone is success."""
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass

    @contextmanager
    def file(self, content: str | bytes, *, suffix: str = ".yml") -> Iterator[Path]:
        """One temp file, unlinked on exit — including exception and
        ``asyncio.CancelledError`` unwinding (``finally`` runs on both)."""
        path = self.create(content, suffix=suffix)
        try:
            yield path
        finally:
            self.unlink(path)

    @contextmanager
    def files(self, *contents: str | bytes, suffix: str = ".yml") -> Iterator[tuple[Path, ...]]:
        """N temp files, all unlinked on exit.

        Fixes the H17 two-file leak ordering: if creating file *k* fails, files
        ``0..k-1`` are unlinked before the exception propagates (v1's
        ``apply_manifest`` leaked the kubeconfig file in that window).
        """
        paths: list[Path] = []
        try:
            for content in contents:
                paths.append(self.create(content, suffix=suffix))
            yield tuple(paths)
        finally:
            for path in paths:
                self.unlink(path)

    @classmethod
    def sweep(cls, root: Path | None = None) -> tuple[str, ...]:
        """Startup sweep (H17): remove every stale file in the registry dir.

        Called once by ``App.start`` before any provider runs. Returns the
        names removed (for the startup log). Missing dir is a no-op.
        """
        directory = root or default_registry_dir()
        removed: list[str] = []
        try:
            entries = list(directory.iterdir())
        except FileNotFoundError:
            return ()
        for entry in entries:
            if entry.is_dir() and not entry.is_symlink():
                continue  # never recurse; the registry writes only flat files
            try:
                entry.unlink()
                removed.append(entry.name)
            except FileNotFoundError:
                pass
        return tuple(removed)
