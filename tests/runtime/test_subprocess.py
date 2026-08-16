"""tests/runtime/test_subprocess.py — ``seedpod/runtime/subprocess_manager.py``'s three
classes (``SubprocessManager``, ``TrackedSubprocessRunner``, ``DetachedLaunchRunner``),
per docs/decisions/DR-0005-detached-launch-transport.md. Real short-lived child
processes throughout (``sys.executable -c ...`` sleep scripts); no ``Mock``/``patch``
anywhere (CLAUDE.md). Every test that spawns a process the manager never registers
(the detached-launch cases) holds the child's PID and kills it explicitly in a
``finally`` — the test owns those processes, not ``SubprocessManager``.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import sys
import time
from collections.abc import AsyncIterator, Mapping, Sequence
from pathlib import Path

import pytest

from seedpod.providers.contract import SubprocessResult
from seedpod.runtime.subprocess_manager import (
    DetachedLaunchRunner,
    SubprocessManager,
    TrackedSubprocessRunner,
)

_PY = Path(sys.executable).name


def _pid_writing_sleep_argv(pidfile: Path, seconds: float = 30.0) -> list[str]:
    """A short python program that writes its own PID to ``pidfile`` then sleeps —
    lets a test identify and later kill a subprocess the runner under test never
    hands back a handle for (the detached-launch cases)."""
    code = f"import os,time\nopen({str(pidfile)!r}, 'w').write(str(os.getpid()))\ntime.sleep({seconds})\n"
    return [sys.executable, "-c", code]


def _pid_writing_sleep_with_grandchild_argv(
    pidfile: Path, grandchild_pidfile: Path, seconds: float = 30.0
) -> list[str]:
    """A python program that writes its own PID to ``pidfile``, spawns a grandchild
    (plain ``subprocess.Popen``, no ``start_new_session`` — so it inherits the
    child's process group; stdio redirected to DEVNULL so it does NOT also
    inherit the child's stdout/stderr pipe fds -- otherwise a per-process-only
    kill of the child leaves the grandchild holding the pipe open and
    ``communicate()`` never sees EOF) which writes ITS pid to
    ``grandchild_pidfile`` then sleeps, then sleeps itself. Lets a test assert
    H16's process-GROUP kill (``killpg``) by checking the grandchild dies too,
    not just the direct child (group leader) — a per-process ``terminate()``
    would leave the grandchild alive."""
    grandchild_code = f"import os,time\nopen({str(grandchild_pidfile)!r}, 'w').write(str(os.getpid()))\ntime.sleep({seconds})\n"
    code = (
        f"import os,subprocess,time\n"
        f"open({str(pidfile)!r}, 'w').write(str(os.getpid()))\n"
        f"subprocess.Popen([{sys.executable!r}, '-c', {grandchild_code!r}], "
        f"stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)\n"
        f"time.sleep({seconds})\n"
    )
    return [sys.executable, "-c", code]


def _line_then_sleep_with_grandchild_argv(
    pidfile: Path, grandchild_pidfile: Path, seconds: float = 30.0
) -> list[str]:
    """Like ``_pid_writing_sleep_with_grandchild_argv``, but the child prints
    ``ready`` to stdout before sleeping — gives a ``stream()`` test one line to
    consume (registering the process, proving the stream is live) before it
    cancels mid-read."""
    grandchild_code = f"import os,time\nopen({str(grandchild_pidfile)!r}, 'w').write(str(os.getpid()))\ntime.sleep({seconds})\n"
    code = (
        f"import os,subprocess,sys,time\n"
        f"open({str(pidfile)!r}, 'w').write(str(os.getpid()))\n"
        f"subprocess.Popen([{sys.executable!r}, '-c', {grandchild_code!r}], "
        f"stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)\n"
        f"print('ready', flush=True)\n"
        f"time.sleep({seconds})\n"
    )
    return [sys.executable, "-c", code]


def _line_emitting_argv(pidfile: Path, *, count: int = 20, delay: float = 0.05) -> list[str]:
    """A python program that writes its own PID to ``pidfile``, then prints
    ``count`` numbered lines to stdout (flushed), sleeping ``delay`` seconds
    BEFORE each print after the first — so line 0 is available almost
    immediately and later lines trickle in, giving a test room to cancel or
    time out a read mid-stream. Writes something short to stderr at the end too,
    for the stderr-harvest-and-log tests."""
    code = (
        f"import os,sys,time\n"
        f"open({str(pidfile)!r}, 'w').write(str(os.getpid()))\n"
        f"for i in range({count}):\n"
        f"    if i:\n"
        f"        time.sleep({delay})\n"
        f"    print(i, flush=True)\n"
        f"sys.stderr.write('watch-ended\\n')\n"
        f"sys.stderr.flush()\n"
    )
    return [sys.executable, "-c", code]


async def _wait_for_pidfile(pidfile: Path, *, timeout: float = 5.0) -> int:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pidfile.exists():
            text = pidfile.read_text().strip()
            if text:
                return int(text)
        await asyncio.sleep(0.02)
    raise TimeoutError(f"{pidfile} never appeared within {timeout}s")


async def _wait_until(predicate, *, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.02)
    raise TimeoutError("condition never became true")


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _kill_quietly(pid: int) -> None:
    with contextlib.suppress(ProcessLookupError):
        os.kill(pid, signal.SIGKILL)


class _FakeInnerRunner:
    """Records calls, hands back a canned ``SubprocessResult``/line stream. Structural
    ``SubprocessRunner`` (contract.py) — plain hand-built class, no Mock/patch."""

    def __init__(self) -> None:
        self.run_calls: list[tuple[tuple[str, ...], bytes | None, dict | None, float | None, str | None]] = []
        self.stream_calls: list[tuple[tuple[str, ...], dict | None, str | None]] = []
        self.canned_result = SubprocessResult(returncode=0, stdout=b"fake-inner-stdout", stderr=b"")

    async def run(
        self,
        argv: Sequence[str],
        *,
        stdin: bytes | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        cluster_id: str | None = None,
    ) -> SubprocessResult:
        self.run_calls.append((tuple(argv), stdin, dict(env) if env else None, timeout, cluster_id))
        return self.canned_result

    def stream(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        cluster_id: str | None = None,
    ):
        self.stream_calls.append((tuple(argv), dict(env) if env else None, cluster_id))
        return self._stream_ctx()

    @contextlib.asynccontextmanager
    async def _stream_ctx(self) -> AsyncIterator[AsyncIterator[bytes]]:
        async def _lines() -> AsyncIterator[bytes]:
            yield b"fake-line\n"

        yield _lines()


# ----------------------------------------------------------------------------
# SubprocessManager / TrackedSubprocessRunner — tracked lifecycle
# ----------------------------------------------------------------------------


async def test_tracked_child_terminated_by_shutdown():
    manager = SubprocessManager()
    runner = TrackedSubprocessRunner(manager)
    task = asyncio.ensure_future(runner.run(["sleep", "30"]))
    await _wait_until(lambda: manager.active_count() == 1)

    await manager.shutdown(timeout=2.0)

    result = await asyncio.wait_for(task, timeout=3.0)
    assert result.returncode != 0  # killed by SIGTERM/SIGKILL, not a clean exit
    assert manager.active_count() == 0


async def test_missing_binary_returns_binary_missing():
    manager = SubprocessManager()
    runner = TrackedSubprocessRunner(manager)
    result = await runner.run(["/definitely/not/a/real/binary-seedpod-test"])
    assert result.binary_missing is True
    assert manager.active_count() == 0


async def test_timeout_sets_timed_out_flag_and_kills_child_process_group(tmp_path):
    manager = SubprocessManager()
    runner = TrackedSubprocessRunner(manager)
    pidfile = tmp_path / "timeout.pid"
    grandchild_pidfile = tmp_path / "timeout_grandchild.pid"
    argv = _pid_writing_sleep_with_grandchild_argv(pidfile, grandchild_pidfile)

    task = asyncio.ensure_future(runner.run(argv, timeout=0.2))
    grandchild_pid = await _wait_for_pidfile(grandchild_pidfile)
    assert _pid_alive(grandchild_pid)

    start = time.monotonic()
    result = await task
    elapsed = time.monotonic() - start
    assert result.timed_out is True
    assert elapsed < 5.0  # nowhere near the 30s sleep -> the kill actually happened
    assert manager.active_count() == 0

    # H16: process-GROUP kill, not a per-process terminate -- the grandchild (in
    # the same group, not directly tracked) dies too. A per-process terminate()
    # would leave it alive.
    await _wait_until(lambda: not _pid_alive(grandchild_pid))


async def test_cancellation_kills_tracked_child_process_group(tmp_path):
    manager = SubprocessManager()
    runner = TrackedSubprocessRunner(manager)
    pidfile = tmp_path / "cancelled.pid"
    grandchild_pidfile = tmp_path / "cancelled_grandchild.pid"
    argv = _pid_writing_sleep_with_grandchild_argv(pidfile, grandchild_pidfile)
    task = asyncio.ensure_future(runner.run(argv))
    pid = await _wait_for_pidfile(pidfile)
    assert _pid_alive(pid)
    # start_new_session=True: the child is its own process-group leader.
    assert os.getpgid(pid) == pid
    grandchild_pid = await _wait_for_pidfile(grandchild_pidfile)
    assert _pid_alive(grandchild_pid)
    # the grandchild inherited the child's process group (no start_new_session
    # of its own) -- this is the only setup where group-vs-per-process kill is
    # observably different.
    assert os.getpgid(grandchild_pid) == pid

    start = time.monotonic()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    elapsed = time.monotonic() - start
    assert elapsed < 5.0

    await _wait_until(lambda: not _pid_alive(pid))
    assert manager.active_count() == 0
    # H16: process-GROUP kill -- the grandchild (never directly tracked) dies too.
    await _wait_until(lambda: not _pid_alive(grandchild_pid))


async def test_concurrent_shutdown_calls_are_serialized_and_safe():
    manager = SubprocessManager()
    runner = TrackedSubprocessRunner(manager)
    task = asyncio.ensure_future(runner.run(["sleep", "30"]))
    await _wait_until(lambda: manager.active_count() == 1)

    # v1 parity: shutdown() is serialized by an internal lock so two callers
    # racing App.stop() don't double-sweep. Neither call should raise.
    await asyncio.gather(manager.shutdown(timeout=2.0), manager.shutdown(timeout=2.0))

    result = await asyncio.wait_for(task, timeout=3.0)
    assert result.returncode != 0
    assert manager.active_count() == 0


# ----------------------------------------------------------------------------
# SubprocessManager.terminate_for_cluster — targeted deployment-cancel termination
# ----------------------------------------------------------------------------


async def test_terminate_for_cluster_kills_only_that_clusters_processes():
    manager = SubprocessManager()
    runner = TrackedSubprocessRunner(manager)
    task_a = asyncio.ensure_future(runner.run(["sleep", "30"], cluster_id="cluster-a"))
    task_b = asyncio.ensure_future(runner.run(["sleep", "30"], cluster_id="cluster-b"))
    await _wait_until(lambda: manager.active_count() == 2)
    assert manager.cluster_process_count("cluster-a") == 1
    assert manager.cluster_process_count("cluster-b") == 1

    count = await manager.terminate_for_cluster("cluster-a", timeout=2.0)

    assert count == 1
    result_a = await asyncio.wait_for(task_a, timeout=3.0)
    assert result_a.returncode != 0  # cluster-a's process was killed
    # bucket cleanup: the terminated cluster's entry is gone, the other's is not.
    assert manager.cluster_process_count("cluster-a") == 0
    assert manager.cluster_process_count("cluster-b") == 1
    assert manager.active_count() == 1  # cluster-b's process is still tracked/alive

    # clean up cluster-b's process.
    await manager.shutdown(timeout=2.0)
    await asyncio.wait_for(task_b, timeout=3.0)


async def test_terminate_for_cluster_returns_zero_for_unknown_cluster():
    manager = SubprocessManager()
    count = await manager.terminate_for_cluster("no-such-cluster")
    assert count == 0


async def test_terminate_for_cluster_is_per_process_not_group_kill(tmp_path):
    """v1 parity, pinned by this class's own docstring: ``terminate_for_cluster``
    signals only the tracked handle itself, not its process group -- unlike
    ``TrackedSubprocessRunner``'s H16 cancellation kill. A grandchild the tracked
    process spawned (in the same group) survives."""
    manager = SubprocessManager()
    runner = TrackedSubprocessRunner(manager)
    pidfile = tmp_path / "cluster_kill.pid"
    grandchild_pidfile = tmp_path / "cluster_kill_grandchild.pid"
    argv = _pid_writing_sleep_with_grandchild_argv(pidfile, grandchild_pidfile)
    task = asyncio.ensure_future(runner.run(argv, cluster_id="cluster-c"))
    pid = await _wait_for_pidfile(pidfile)
    grandchild_pid = await _wait_for_pidfile(grandchild_pidfile)

    try:
        count = await manager.terminate_for_cluster("cluster-c", timeout=2.0)
        assert count == 1
        await asyncio.wait_for(task, timeout=3.0)
        await _wait_until(lambda: not _pid_alive(pid))

        assert _pid_alive(grandchild_pid)  # per-process kill, not group kill
    finally:
        _kill_quietly(grandchild_pid)


# ----------------------------------------------------------------------------
# TrackedSubprocessRunner.stream() — real-process line iteration
# ----------------------------------------------------------------------------


async def test_stream_reads_lines_and_registers_for_the_streams_duration(tmp_path):
    manager = SubprocessManager()
    runner = TrackedSubprocessRunner(manager)
    pidfile = tmp_path / "stream_lines.pid"
    argv = _line_emitting_argv(pidfile, count=3, delay=0.0)

    async with runner.stream(argv) as lines:
        assert manager.active_count() == 1  # registered for the stream's duration
        collected = [line async for line in lines]

    assert collected == [b"0\n", b"1\n", b"2\n"]
    assert manager.active_count() == 0  # unregistered on __aexit__


async def test_stream_early_exit_still_kills_child_and_unregisters(tmp_path):
    """Leaving the ``async with`` block before EOF (normal exit path, process
    still running) must still terminate the child and unregister it --
    __aexit__'s cleanup runs on every exit path, not just natural EOF."""
    manager = SubprocessManager()
    runner = TrackedSubprocessRunner(manager)
    pidfile = tmp_path / "stream_early_exit.pid"
    argv = _line_emitting_argv(pidfile, count=1000, delay=0.1)

    async with runner.stream(argv) as lines:
        first = await lines.__anext__()
        assert first == b"0\n"
        # deliberately don't consume the rest -- exit the block early.

    pid = await _wait_for_pidfile(pidfile)
    await _wait_until(lambda: not _pid_alive(pid))
    assert manager.active_count() == 0


async def test_stream_exception_in_body_still_kills_child_and_unregisters(tmp_path):
    manager = SubprocessManager()
    runner = TrackedSubprocessRunner(manager)
    pidfile = tmp_path / "stream_exception.pid"
    argv = _line_emitting_argv(pidfile, count=1000, delay=0.1)

    with pytest.raises(ValueError, match="boom"):
        async with runner.stream(argv) as lines:
            await lines.__anext__()
            raise ValueError("boom")

    pid = await _wait_for_pidfile(pidfile)
    await _wait_until(lambda: not _pid_alive(pid))
    assert manager.active_count() == 0


async def test_stream_cancellation_kills_process_group_including_grandchild(tmp_path):
    manager = SubprocessManager()
    runner = TrackedSubprocessRunner(manager)
    pidfile = tmp_path / "stream_cancel.pid"
    grandchild_pidfile = tmp_path / "stream_cancel_grandchild.pid"
    argv = _line_then_sleep_with_grandchild_argv(pidfile, grandchild_pidfile)

    async def _consume() -> None:
        async with runner.stream(argv) as lines:
            line = await lines.__anext__()
            assert line == b"ready\n"
            await lines.__anext__()  # blocks -- this is what we cancel

    task = asyncio.ensure_future(_consume())
    pid = await _wait_for_pidfile(pidfile)
    grandchild_pid = await _wait_for_pidfile(grandchild_pidfile)
    assert os.getpgid(grandchild_pid) == pid

    start = time.monotonic()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    elapsed = time.monotonic() - start
    assert elapsed < 5.0

    await _wait_until(lambda: not _pid_alive(pid))
    await _wait_until(lambda: not _pid_alive(grandchild_pid))
    assert manager.active_count() == 0


async def test_stream_iterator_resumes_after_a_cancelled_read(tmp_path):
    """``_ProcessLineIterator``'s own justification for being a plain class (not an
    async generator): cancelling one ``__anext__`` call must not corrupt later
    reads, because each read is independent against ``StreamReader``'s own
    buffer -- a suspended generator frame would instead be permanently closed."""
    manager = SubprocessManager()
    runner = TrackedSubprocessRunner(manager)
    pidfile = tmp_path / "stream_resume.pid"
    argv = _line_emitting_argv(pidfile, count=3, delay=0.3)

    async with runner.stream(argv) as lines:
        first = await lines.__anext__()
        assert first == b"0\n"

        # force a cancelled read: line 1 won't be ready for ~0.3s.
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(lines.__anext__(), timeout=0.05)

        # resume: a fresh __anext__ call must still work (not StopAsyncIteration).
        second = await lines.__anext__()
        assert second == b"1\n"


async def test_stream_end_harvests_and_logs_stderr(tmp_path, caplog):
    manager = SubprocessManager()
    runner = TrackedSubprocessRunner(manager)
    pidfile = tmp_path / "stream_stderr.pid"
    argv = _line_emitting_argv(pidfile, count=1, delay=0.0)

    with caplog.at_level("INFO", logger="seedpod.runtime.subprocess_manager"):
        async with runner.stream(argv) as lines:
            async for _line in lines:
                pass

    assert any("watch-ended" in record.getMessage() for record in caplog.records)


# ----------------------------------------------------------------------------
# DetachedLaunchRunner — matching argv (DR-0005 verbatim semantics)
# ----------------------------------------------------------------------------


async def test_detached_call_returns_while_child_still_running(tmp_path):
    manager = SubprocessManager()
    tracked = TrackedSubprocessRunner(manager)
    runner = DetachedLaunchRunner(tracked, launch_prefixes=((_PY, "-c"),))
    pidfile = tmp_path / "detached_returns.pid"
    argv = _pid_writing_sleep_argv(pidfile)

    start = time.monotonic()
    result = await runner.run(argv)
    elapsed = time.monotonic() - start

    try:
        assert result == SubprocessResult(returncode=0, stdout=b"", stderr=b"")
        assert elapsed < 5.0  # returned immediately, did not await the 30s sleep
        pid = await _wait_for_pidfile(pidfile)
        assert _pid_alive(pid)  # child is still running after the call returned
    finally:
        if pidfile.exists():
            _kill_quietly(int(pidfile.read_text().strip()))


async def test_detached_child_survives_shutdown_and_sits_in_own_session(tmp_path):
    manager = SubprocessManager()
    tracked = TrackedSubprocessRunner(manager)
    runner = DetachedLaunchRunner(tracked, launch_prefixes=((_PY, "-c"),))
    pidfile = tmp_path / "detached_survives.pid"
    argv = _pid_writing_sleep_argv(pidfile)

    pid = None
    try:
        result = await runner.run(argv)
        assert result == SubprocessResult(returncode=0, stdout=b"", stderr=b"")
        pid = await _wait_for_pidfile(pidfile)

        # never registered with SubprocessManager
        assert manager.active_count() == 0
        # own session, not ours
        assert os.getsid(pid) != os.getsid(0)

        await manager.shutdown(timeout=0.5)

        assert _pid_alive(pid)  # shutdown() never touched it
    finally:
        if pid is not None:
            _kill_quietly(pid)


async def test_detached_missing_binary_returns_binary_missing():
    manager = SubprocessManager()
    tracked = TrackedSubprocessRunner(manager)
    runner = DetachedLaunchRunner(tracked, launch_prefixes=(("not-a-real-binary-seedpod-test", "run"),))
    result = await runner.run(["not-a-real-binary-seedpod-test", "run", "some-vm"])
    assert result.binary_missing is True
    assert manager.active_count() == 0


# ----------------------------------------------------------------------------
# DetachedLaunchRunner — non-matching argv delegates untouched
# ----------------------------------------------------------------------------


async def test_non_matching_argv_delegates_run_to_inner():
    fake = _FakeInnerRunner()
    runner = DetachedLaunchRunner(fake, launch_prefixes=(("tart", "run"),))

    result = await runner.run(["kubectl", "get", "pods"], timeout=5.0, cluster_id="c-1")

    assert result is fake.canned_result
    assert fake.run_calls == [(("kubectl", "get", "pods"), None, None, 5.0, "c-1")]


async def test_matching_basename_but_different_binary_path_still_matches(tmp_path):
    """DR-0005: prefix matching is on ``basename(argv[0])``, so a full path to the
    same-named binary matches regardless of where it lives on PATH."""
    fake = _FakeInnerRunner()
    runner = DetachedLaunchRunner(fake, launch_prefixes=(("tart", "run"),))
    bogus_tart = tmp_path / "nonexistent-dir" / "tart"  # absolute path, basename "tart"

    result = await runner.run([str(bogus_tart), "run", "--no-graphics", "seedpod-x"])

    # matched -> did NOT delegate to inner, and got the detached (not the plain
    # missing-binary-via-PATH-lookup) code path: spawn was attempted directly.
    assert fake.run_calls == []
    assert result.binary_missing is True  # bogus_tart's parent dir doesn't exist


async def test_non_matching_argv_delegates_stream_to_inner():
    fake = _FakeInnerRunner()
    runner = DetachedLaunchRunner(fake, launch_prefixes=(("tart", "run"),))

    async with runner.stream(["kubectl", "get", "pods", "-w"], env={"KUBECONFIG": "x"}) as lines:
        collected = [line async for line in lines]

    assert collected == [b"fake-line\n"]
    assert fake.stream_calls == [(("kubectl", "get", "pods", "-w"), {"KUBECONFIG": "x"}, None)]
