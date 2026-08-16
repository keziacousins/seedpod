"""``single_instance``/``assert_port_available`` — the server entry point's
process-level guards (DR-0041 Amendment B).

**Why this exists as a module rather than in ``start.py``.** It was in
``start.py`` — a dev convenience script — as ``check_pid_file()``, alongside
``load_dotenv()`` and ``rotate_logs_on_startup()``. ``seedpod/__main__.py``, which
IS the ``seedpod`` console script and therefore the entry point a packaged
artifact ships, had **none of the three**: the packaged path was the *less*
capable one, and everything an operator relied on lived only in the file the
artifact would not include. DR-0041 moves all three across; this is the guard
half, factored here so both entry points share ONE implementation instead of
drifting.

**Why a kernel lock and not the previous pid file.** ``check_pid_file`` read the
stored pid, probed it with ``os.kill(pid, 0)``, unlinked the file if the pid was
gone, then wrote its own — check-then-act, so two starts racing can both observe
"stale" and both proceed. ``flock`` is held by the kernel for as long as the fd is
open and is released when the process dies *however* it dies, so there is no stale
file to clean up, no ``atexit`` dependency, and no window.

**What the lock does NOT fix, stated plainly.** ``rm -f seedpod.pid`` followed by
a start still gets through: ``os.open(..., O_CREAT)`` after an unlink creates a new
inode, and an flock on that is uncontended, because the incumbent holds a lock on an
inode that no longer has a name. There is no filesystem-only fix for
unlink-then-recreate. That is exactly why ``assert_port_available`` below is not
redundant with this -- two servers cannot both hold the port, whatever either of
them believes about a file.

Which matters, because the 2026-08-14 stale-server incident was not a missing guard.
``start.py``'s guard would have caught it; a restart script written in a hurry did
``rm -f seedpod.pid`` first. The lesson is not "add a better file" -- it is that a
file-based guard has a floor set by the least careful script that touches it, and
the backstop has to be something no script can wish away.

**The port check is a separate question from the lock.** The lock answers "is
another *seedpod* running". It cannot answer "is anything at all listening on
8000" — and the stale server that started this whole line of work answered
``/health`` perfectly well, which is what made a restart *look* successful.
"""

from __future__ import annotations

import errno
import fcntl
import os
import socket
import subprocess
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

__all__ = ["AlreadyRunning", "PortUnavailable", "assert_port_available", "single_instance"]


class AlreadyRunning(RuntimeError):
    """Another process holds the instance lock."""


class PortUnavailable(RuntimeError):
    """The configured listen address is already taken."""


@contextmanager
def single_instance(lock_path: Path | str) -> Iterator[int]:
    """Hold an exclusive, non-blocking ``flock`` for the duration of the block.

    Yields this process's pid. Raises ``AlreadyRunning`` if another live process
    holds the lock, naming its pid when the file still records a readable one --
    the file's *contents* are advisory (a human-readable convenience, and stale
    after a hard kill); the ``flock`` is what actually decides.

    The fd is deliberately NOT closed inside the try: closing it drops the lock.
    It is released on exit, and by the kernel if the process dies first.
    """
    path = Path(lock_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    pid = os.getpid()

    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        if exc.errno not in (errno.EACCES, errno.EAGAIN):
            os.close(fd)
            raise
        holder = _recorded_pid(path)
        os.close(fd)
        raise AlreadyRunning(
            f"another seedpod holds {path}"
            + (f" (pid {holder}) -- stop it first: kill {holder}" if holder else "")
        ) from exc

    try:
        os.ftruncate(fd, 0)
        os.write(fd, f"{pid}\n".encode())
        os.fsync(fd)
        yield pid
    finally:
        # Release, then remove. Order matters only for tidiness -- a crash between
        # them leaves a file whose flock is already gone, which the next start
        # takes cleanly.
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)
        try:
            if _recorded_pid(path) == pid:
                path.unlink()
        except OSError:
            pass


def _recorded_pid(path: Path) -> int | None:
    try:
        return int(path.read_text().strip())
    except (OSError, ValueError):
        return None


def assert_port_available(host: str, port: int) -> None:
    """Fail fast, and by name, when the listen address is already taken.

    A bind probe rather than a parse of ``lsof`` output: binding is the same
    question the server is about to ask, so it cannot disagree with it. There is a
    small window between this check and uvicorn's own bind -- accepted, because the
    failure this prevents is not a race but a long-lived survivor, and uvicorn's
    own bind remains the real authority.

    ``0.0.0.0`` is probed as-is: binding the wildcard fails if anything holds the
    port on any address, which is the intended breadth.
    """
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        probe.bind((host, port))
    except OSError as exc:
        if exc.errno not in (errno.EADDRINUSE, errno.EACCES):
            raise
        holder = _listener_pid(port)
        raise PortUnavailable(
            f"{host}:{port} is already in use"
            + (f" by pid {holder} -- stop it first: kill {holder}" if holder else "")
            + " -- refusing to start, because a server that answers /health with"
            " someone else's code is worse than no server"
        ) from exc
    finally:
        probe.close()


def _listener_pid(port: int) -> int | None:
    """Best-effort, for the error message only. ``lsof`` is present on macOS and
    most Linux images; if it is not, the message simply omits the pid."""
    try:
        out = subprocess.run(  # noqa: S603,S607 -- fixed argv, no shell, message-only
            ["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN", "-t"],
            capture_output=True, text=True, timeout=5, check=False,
        ).stdout.split()
        return int(out[0]) if out else None
    except (OSError, ValueError, subprocess.SubprocessError):
        return None
