"""seedpod/runtime/subprocess_manager.py — Round-4 runtime spine, per
docs/decisions/DR-0005-detached-launch-transport.md (RATIFIED). Owns every concrete
subprocess-spawning class in v2: the tracked-process registry and the two
``SubprocessRunner`` (``seedpod/providers/contract.py`` §5.4) transports built on top
of it.

Three classes, one module (DR-0005's "one module owns all spawn code; no
package-layout change"):

- ``SubprocessManager`` — salvaged from ``reference-code/seedpod/seedpod/core/
  subprocess_manager.py``'s ``SubprocessManager`` class (register/unregister,
  cluster-scoped ``terminate_for_cluster``, ``shutdown(timeout)`` terminate→kill
  escalation), minus the module-level global singleton
  (``get_subprocess_manager``/``_subprocess_manager`` — the composition root now owns
  the one instance and injects it everywhere, matching this tree's DI discipline).
  Structurally satisfies ``seedpod/engine/step.py``'s ``SubprocessManagerLike``
  Protocol (``register(process, *, cluster_id=None)`` / ``unregister(process)``) with
  the SAME method shapes — that Protocol and this class were designed as one seam, so
  a ``SubprocessManager`` instance is what the composition root hands
  ``StepServices.subprocess_manager``. It is deliberately NOT also a
  ``SubprocessRunner``: that protocol's ``run()``/``stream()`` take ``argv`` and spawn
  a NEW process, the opposite shape from registering a process the caller already
  spawned (``StepContext.run_subprocess`` spawns its own child directly and only asks
  this registry to track it) — the two protocols do not align and are not meant to,
  per DR-0005's "check if one class satisfies both shapes" instruction.

- ``TrackedSubprocessRunner`` — the default ``SubprocessRunner`` transport behind
  every provider (§5.4's injected-transport construction contract). One bounded
  attempt per ``run()`` call: no internal retry/sleep (H4-H6, the engine's
  ``Schedule`` owns retry). Registers the child with an injected ``SubprocessManager``
  for its duration; process-group SIGTERM→SIGKILL escalation on ``asyncio`` task
  cancellation (H16 — the same escalation shape as ``engine/step.py``'s
  ``StepContext._kill_group``, generalized here for the transport seam) and on
  ``timeout`` expiry. Never raises for a clean non-zero exit; ``FileNotFoundError`` at
  spawn → ``binary_missing=True``; wall-clock ``timeout`` → ``timed_out=True`` (both
  flagged on the returned ``SubprocessResult``, never as exceptions — contract.py
  §5.4's "single ``classify_subprocess`` call downstream handles every case
  uniformly"). ``stream()`` backs the one natively-streaming command
  (``KubeWatchPods``): a cancellation-safe (non-generator — see
  ``_ProcessLineIterator``, mirroring ``tests/conformance/fake_kubectl.py``'s own
  justification) line iterator over the child's stdout, with the terminate→kill
  escalation and final stderr harvest happening in ``__aexit__`` on every exit path
  (normal end, cancellation, or exception) per the protocol's own docstring.

- ``DetachedLaunchRunner`` — DR-0005 verbatim. Wraps an ``inner: SubprocessRunner``
  with ``launch_prefixes: tuple[tuple[str, ...], ...]``; an argv whose
  ``(basename(argv[0]), *argv[1:])`` head matches a prefix is spawned with v1's
  detached semantics EXACTLY (``reference-code/seedpod/seedpod/providers/
  _tart_cli.py:220-253`` ``run_detached``): stdio → DEVNULL ×3,
  ``start_new_session=True``, never awaited, NEVER registered with
  ``SubprocessManager``, and the call returns
  ``SubprocessResult(returncode=0, stdout=b"", stderr=b"")`` immediately after a
  successful spawn. ``FileNotFoundError`` at spawn → ``binary_missing=True``. Every
  non-matching argv delegates to ``inner`` untouched (``run()`` AND ``stream()`` —
  nothing about a detached VM launch changes how, say, a kubectl watch is
  transported).

  **Blast radius (DR-0005, binding — quoted, not paraphrased): "the prefix match
  applies only to argv this transport is about to spawn for the tart provider — it is
  a spawn-mode selector, never a process-table scan. Nothing in this design signals,
  enumerates, or matches EXISTING processes by name." This module contains NO
  process-scanning or kill-by-name code anywhere — ``SubprocessManager`` only ever
  touches handles it was handed via ``register()``, and ``DetachedLaunchRunner`` only
  ever touches argv it is about to spawn itself.**

Composition-root wiring (DR-0005's "Wiring" section, not this module's concern to
enforce): one shared ``SubprocessManager`` backs both ``StepServices.subprocess_manager``
and every provider's ``TrackedSubprocessRunner``; only the tart provider's transport is
additionally wrapped, ``DetachedLaunchRunner(tracked, launch_prefixes=(("tart", "run"),))``.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
from collections import defaultdict
from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from typing import TYPE_CHECKING

from seedpod.providers.contract import SubprocessResult

if TYPE_CHECKING:
    from seedpod.providers.contract import SubprocessRunner

__all__ = ["SubprocessManager", "TrackedSubprocessRunner", "DetachedLaunchRunner"]

logger = logging.getLogger(__name__)

# Escalation grace period between process-group SIGTERM and SIGKILL, shared by
# TrackedSubprocessRunner's cancel/timeout kill path and SubprocessManager.shutdown's
# per-process kill path. Matches engine/step.py's StepContext._kill_group's 10.0s.
_KILL_GRACE_S = 10.0


class SubprocessManager:
    """Registry of live subprocesses, salvaged from
    ``reference-code/seedpod/seedpod/core/subprocess_manager.py``'s
    ``SubprocessManager`` class verbatim minus the module-level global singleton
    (``get_subprocess_manager()``/``create_tracked_subprocess()`` die here — the
    composition root constructs the one instance and injects it, this tree's DI
    discipline). ``shutdown()``/``terminate_for_cluster()`` use the SAME
    per-process ``terminate()``→``kill()`` escalation v1 used (not a process-group
    kill — that is ``TrackedSubprocessRunner``'s own H16 responsibility for the
    children IT spawns with ``start_new_session=True``; this registry tracks
    whatever ``asyncio.subprocess.Process`` handle a caller hands it and does not
    assume group leadership)."""

    def __init__(self) -> None:
        self._active: set[asyncio.subprocess.Process] = set()
        self._cluster_processes: dict[str, set[asyncio.subprocess.Process]] = defaultdict(set)
        self._process_cluster: dict[asyncio.subprocess.Process, str] = {}
        self._shutting_down = False
        # Serializes concurrent shutdown() calls, v1 parity (reference-code/seedpod/seedpod/
        # core/subprocess_manager.py:33 `self._lock`) -- dropped in an earlier draft without
        # comment; restored so two callers racing App.stop() can't double-sweep.
        self._shutdown_lock = asyncio.Lock()

    def register(self, process: asyncio.subprocess.Process, *, cluster_id: str | None = None) -> None:
        """Register an active subprocess for tracking, optionally associated with a
        cluster. A no-op once ``shutdown()`` has started (v1 parity: nothing spawned
        during shutdown gets tracked, since shutdown already swept everything it
        knew about at the moment it started)."""
        if self._shutting_down:
            return
        self._active.add(process)
        if cluster_id is not None:
            self._cluster_processes[cluster_id].add(process)
            self._process_cluster[process] = cluster_id

    def unregister(self, process: asyncio.subprocess.Process) -> None:
        """Remove a subprocess from tracking (called when the process completes
        normally, or from the owning caller's ``finally``)."""
        self._active.discard(process)
        cluster_id = self._process_cluster.pop(process, None)
        if cluster_id is not None:
            bucket = self._cluster_processes.get(cluster_id)
            if bucket is not None:
                bucket.discard(process)
                if not bucket:
                    del self._cluster_processes[cluster_id]

    def active_count(self) -> int:
        return len(self._active)

    def cluster_process_count(self, cluster_id: str) -> int:
        return len(self._cluster_processes.get(cluster_id, ()))

    async def terminate_for_cluster(self, cluster_id: str, *, timeout: float = 5.0) -> int:
        """Terminate every subprocess registered for ``cluster_id`` (targeted
        cancellation, e.g. a deployment cancel). Returns the number of processes
        that were live for this cluster at the start of the call."""
        processes = list(self._cluster_processes.get(cluster_id, ()))
        if processes:
            await self._terminate_then_kill(processes, timeout)
        return len(processes)

    async def shutdown(self, timeout: float = 5.0) -> None:
        """Gracefully terminate every tracked subprocess: SIGTERM all, wait up to
        ``timeout``, SIGKILL whatever is still alive, wait up to a further 2s. Marks
        this manager shut down so any late ``register()`` is a no-op. Serialized by
        ``self._shutdown_lock`` (v1 parity) so concurrent ``App.stop()``-triggered
        calls sweep once, not twice."""
        async with self._shutdown_lock:
            self._shutting_down = True
            processes = list(self._active)
            if processes:
                await self._terminate_then_kill(processes, timeout)
            self._active.clear()
            self._cluster_processes.clear()
            self._process_cluster.clear()

    async def _terminate_then_kill(self, processes: list[asyncio.subprocess.Process], timeout: float) -> None:
        """SIGTERM every live process, wait, SIGKILL survivors. v1 parity
        (reference-code/seedpod/seedpod/core/subprocess_manager.py:74-104): a
        per-process ``except Exception`` warns and continues the sweep rather than
        letting one unexpected failure abort termination of the rest of the batch."""
        for process in processes:
            if process.returncode is None:
                try:
                    process.terminate()
                except ProcessLookupError:
                    pass
                except Exception:
                    logger.warning("error sending SIGTERM to pid %s", process.pid, exc_info=True)
        try:
            await asyncio.wait_for(self._wait_all(processes), timeout=timeout)
            return
        except TimeoutError:
            pass
        for process in processes:
            if process.returncode is None:
                try:
                    process.kill()
                except ProcessLookupError:
                    pass
                except Exception:
                    logger.warning("error sending SIGKILL to pid %s", process.pid, exc_info=True)
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._wait_all(processes), timeout=2.0)

    @staticmethod
    async def _wait_all(processes: list[asyncio.subprocess.Process]) -> None:
        pending = [p.wait() for p in processes if p.returncode is None]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)


class _ProcessLineIterator:
    """Cancellation-safe async iterator over a subprocess's stdout: an ordinary class
    backed by persistent state, NOT an async generator. Per ``contract.py``'s
    ``SubprocessRunner.stream`` docstring cancellation-safety note (and
    ``tests/conformance/fake_kubectl.py``'s own identical justification): a suspended
    generator frame is permanently closed by cancellation, so a later ``__anext__``
    would wrongly raise ``StopAsyncIteration`` instead of resuming. Each call here is
    an independent ``readline()`` against ``asyncio.StreamReader``'s own buffer, which
    outlives any single cancelled read."""

    def __init__(self, stdout: asyncio.StreamReader) -> None:
        self._stdout = stdout

    def __aiter__(self) -> _ProcessLineIterator:
        return self

    async def __anext__(self) -> bytes:
        line = await self._stdout.readline()
        if not line:
            raise StopAsyncIteration
        return line


class TrackedSubprocessRunner:
    """The default ``SubprocessRunner`` (``seedpod/providers/contract.py`` §5.4)
    transport: registers every child it spawns with the injected
    ``SubprocessManager`` for the child's duration, process-group SIGTERM→SIGKILL on
    cancellation or timeout (H16), never raises for a clean non-zero exit."""

    def __init__(self, manager: SubprocessManager) -> None:
        self._manager = manager

    async def run(
        self,
        argv: Sequence[str],
        *,
        stdin: bytes | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        cluster_id: str | None = None,
    ) -> SubprocessResult:
        merged_env = {**os.environ, **env} if env else None
        try:
            process = await asyncio.create_subprocess_exec(
                *argv,
                stdin=asyncio.subprocess.PIPE if stdin is not None else asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=merged_env,
                start_new_session=True,  # own process group; pgid == pid (H16 group kill)
            )
        except FileNotFoundError:
            return SubprocessResult(returncode=0, stdout=b"", stderr=b"", binary_missing=True)

        self._manager.register(process, cluster_id=cluster_id)
        try:
            try:
                stdout, stderr = await asyncio.wait_for(process.communicate(stdin), timeout=timeout)
            except TimeoutError:
                await self._kill_group(process)
                return SubprocessResult(returncode=1, stdout=b"", stderr=b"", timed_out=True)
            except asyncio.CancelledError:
                await self._kill_group(process)
                raise
            assert process.returncode is not None
            return SubprocessResult(returncode=process.returncode, stdout=stdout, stderr=stderr)
        finally:
            self._manager.unregister(process)

    def stream(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        cluster_id: str | None = None,
    ) -> AbstractAsyncContextManager[AsyncIterator[bytes]]:
        return self._stream(argv, env=env, cluster_id=cluster_id)

    @asynccontextmanager
    async def _stream(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None,
        cluster_id: str | None,
    ) -> AsyncIterator[_ProcessLineIterator]:
        merged_env = {**os.environ, **env} if env else None
        process = await asyncio.create_subprocess_exec(
            *argv,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=merged_env,
            start_new_session=True,
        )
        self._manager.register(process, cluster_id=cluster_id)
        try:
            assert process.stdout is not None
            yield _ProcessLineIterator(process.stdout)
        finally:
            # terminate->kill escalation + final stderr harvest, every exit path
            # (normal end, cancellation, exception) -- contract.py's stream() docstring.
            # v1 harvested stream stderr specifically to log it at watch end
            # (reference-code/seedpod/seedpod/providers/kubernetes.py:1157-1163); logged
            # here too so the diagnostic isn't silently dropped.
            await self._kill_group(process)
            if process.stderr is not None:
                with contextlib.suppress(Exception):
                    stderr_output = await process.stderr.read()
                    if stderr_output:
                        logger.info(
                            "stream(%s) ended, stderr: %r",
                            argv[0] if argv else "?",
                            stderr_output[:200],
                        )
            self._manager.unregister(process)

    async def _kill_group(self, process: asyncio.subprocess.Process) -> None:
        """H16: SIGTERM the process group, grace period, SIGKILL escalation. Mirrors
        ``engine/step.py``'s ``StepContext._kill_group`` -- ``start_new_session=True``
        means ``process.pid`` IS the process-group id."""
        pgid = process.pid
        assert pgid is not None
        with contextlib.suppress(ProcessLookupError):
            os.killpg(pgid, signal.SIGTERM)
        try:
            await asyncio.wait_for(process.wait(), timeout=_KILL_GRACE_S)
            return
        except TimeoutError:
            pass
        with contextlib.suppress(ProcessLookupError):
            os.killpg(pgid, signal.SIGKILL)
        await process.wait()


class DetachedLaunchRunner:
    """DR-0005's detached-launch transport wrapper. Constructed with
    ``inner: SubprocessRunner`` (the transport every non-matching argv delegates to
    untouched) and ``launch_prefixes`` -- a tuple of argv-head prefixes; a call whose
    ``(basename(argv[0]), *argv[1:])`` head starts with one of them is spawned with
    v1's detached semantics verbatim (``reference-code/seedpod/seedpod/providers/
    _tart_cli.py:220-253`` ``run_detached``) instead of reaching ``inner`` at all.

    BLAST-RADIUS LAW (DR-0005, binding): this class touches ONLY argv it is about to
    spawn. No process scanning, no kill-by-name, no enumeration of existing processes
    -- anywhere in this class or module."""

    def __init__(self, inner: SubprocessRunner, launch_prefixes: tuple[tuple[str, ...], ...]) -> None:
        self._inner = inner
        self._launch_prefixes = launch_prefixes

    def _matches(self, argv: Sequence[str]) -> bool:
        if not argv:
            return False
        head = (os.path.basename(argv[0]), *argv[1:])
        return any(head[: len(prefix)] == prefix for prefix in self._launch_prefixes)

    async def run(
        self,
        argv: Sequence[str],
        *,
        stdin: bytes | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        cluster_id: str | None = None,
    ) -> SubprocessResult:
        if not self._matches(argv):
            return await self._inner.run(argv, stdin=stdin, env=env, timeout=timeout, cluster_id=cluster_id)

        # v1 run_detached, verbatim: stdio -> DEVNULL x3, start_new_session=True,
        # never awaited, never registered with SubprocessManager.
        merged_env = {**os.environ, **env} if env else None
        try:
            await asyncio.create_subprocess_exec(
                *argv,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
                env=merged_env,
                start_new_session=True,
            )
        except FileNotFoundError:
            return SubprocessResult(returncode=0, stdout=b"", stderr=b"", binary_missing=True)
        return SubprocessResult(returncode=0, stdout=b"", stderr=b"")

    def stream(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        cluster_id: str | None = None,
    ) -> AbstractAsyncContextManager[AsyncIterator[bytes]]:
        # No provider ever detach-launches a streaming command (KubeWatchPods is
        # kubectl-only); every argv here delegates to inner untouched.
        return self._inner.stream(argv, env=env, cluster_id=cluster_id)
