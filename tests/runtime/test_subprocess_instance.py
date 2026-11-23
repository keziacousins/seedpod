"""``SubprocessManager`` scopes (DR-0055): a subprocess belongs to a cluster OR an instance, and a
cluster and an instance with the same id string never share a bucket. Real child processes, no
mocks.
"""

from __future__ import annotations

import asyncio
import contextlib

import pytest

from seedpod.runtime.subprocess_manager import SubprocessManager


async def _spawn() -> asyncio.subprocess.Process:
    return await asyncio.create_subprocess_exec("sleep", "30")


async def _reap(*processes: asyncio.subprocess.Process) -> None:
    for p in processes:
        if p.returncode is None:
            with contextlib.suppress(ProcessLookupError):
                p.kill()
        await p.wait()


async def test_register_tracks_an_instance_process_under_its_own_scope():
    manager = SubprocessManager()
    proc = await _spawn()
    try:
        manager.register(proc, instance_id="same")
        assert manager.instance_process_count("same") == 1
        assert manager.cluster_process_count("same") == 0  # an instance id is not a cluster id
        assert manager.active_count() == 1
        manager.unregister(proc)
        assert manager.instance_process_count("same") == 0 and manager.active_count() == 0
    finally:
        await _reap(proc)


async def test_register_refuses_both_scopes_at_once():
    manager = SubprocessManager()
    proc = await _spawn()
    try:
        with pytest.raises(ValueError, match="not both"):
            manager.register(proc, cluster_id="c", instance_id="i")
        assert manager.active_count() == 0  # refused before anything was tracked
    finally:
        await _reap(proc)


async def test_terminate_for_instance_kills_only_that_instances_processes():
    manager = SubprocessManager()
    mine, other_instance, same_id_cluster = await _spawn(), await _spawn(), await _spawn()
    try:
        manager.register(mine, instance_id="same")
        manager.register(other_instance, instance_id="other")
        manager.register(same_id_cluster, cluster_id="same")  # the same id string, a cluster

        count = await manager.terminate_for_instance("same", timeout=2.0)

        assert count == 1
        await asyncio.wait_for(mine.wait(), timeout=3.0)
        assert mine.returncode is not None
        assert other_instance.returncode is None and same_id_cluster.returncode is None
        assert manager.cluster_process_count("same") == 1  # untouched
    finally:
        await _reap(mine, other_instance, same_id_cluster)


async def test_terminate_for_cluster_does_not_touch_an_instance_with_the_same_id():
    manager = SubprocessManager()
    cluster_proc, instance_proc = await _spawn(), await _spawn()
    try:
        manager.register(cluster_proc, cluster_id="same")
        manager.register(instance_proc, instance_id="same")

        assert await manager.terminate_for_cluster("same", timeout=2.0) == 1

        await asyncio.wait_for(cluster_proc.wait(), timeout=3.0)
        assert instance_proc.returncode is None
        assert manager.instance_process_count("same") == 1
    finally:
        await _reap(cluster_proc, instance_proc)


async def test_terminate_for_unknown_instance_returns_zero():
    assert await SubprocessManager().terminate_for_instance("nope") == 0


async def test_shutdown_sweeps_instance_processes_too():
    manager = SubprocessManager()
    proc = await _spawn()
    try:
        manager.register(proc, instance_id="i1")
        await manager.shutdown(timeout=2.0)
        await asyncio.wait_for(proc.wait(), timeout=3.0)
        assert manager.active_count() == 0 and manager.instance_process_count("i1") == 0
    finally:
        await _reap(proc)


async def test_terminate_for_run_kills_only_that_runs_processes_even_for_the_same_instance():
    """The engine's hard cancel ends ONE run. The instance's next run (the destroy) has processes of its own."""
    manager = SubprocessManager()
    old_run, next_run, other_instance = await _spawn(), await _spawn(), await _spawn()
    try:
        manager.register(old_run, instance_id="i1", run_id="run-old")
        manager.register(next_run, instance_id="i1", run_id="run-next")
        manager.register(other_instance, instance_id="i2", run_id="run-x")

        assert await manager.terminate_for_run("run-old", timeout=2.0) == 1

        await asyncio.wait_for(old_run.wait(), timeout=3.0)
        assert next_run.returncode is None and other_instance.returncode is None
        manager.unregister(old_run)  # (the step's own `finally` does this)
        assert manager.instance_process_count("i1") == 1  # the next run's, still tracked
        assert await manager.terminate_for_run("run-old", timeout=2.0) == 0
    finally:
        await _reap(old_run, next_run, other_instance)


async def test_a_process_registered_without_a_run_id_is_not_reached_by_terminate_for_run():
    manager = SubprocessManager()
    proc = await _spawn()
    try:
        manager.register(proc, instance_id="i1")
        assert await manager.terminate_for_run("anything", timeout=1.0) == 0
        assert proc.returncode is None
    finally:
        await _reap(proc)


async def test_register_refuses_a_run_id_without_an_instance():
    manager = SubprocessManager()
    proc = await _spawn()
    try:
        with pytest.raises(ValueError, match="run id"):
            manager.register(proc, cluster_id="c", run_id="r")
        assert manager.active_count() == 0
    finally:
        await _reap(proc)
