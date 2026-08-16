"""tests/engine/test_cancel.py — CancelToken semantics (trip one-way/idempotent/
level-triggered, raise_if_cancelled, wait) + StepContext.sleep/run_subprocess
cancellation, using a real short-lived child process. No Mock/patch anywhere.
"""

from __future__ import annotations

import asyncio
import sys
import time

import pytest

from seedpod.engine.cancel import CancelToken
from seedpod.engine.errors import StepCancelled
from tests.engine.fakes import FakeSubprocessManager, make_step_context

# ----------------------------------------------------------------------------
# CancelToken semantics
# ----------------------------------------------------------------------------


def test_token_starts_untripped():
    token = CancelToken()
    assert token.tripped is False
    token.raise_if_cancelled()  # no-op, does not raise


def test_trip_flips_tripped():
    token = CancelToken()
    token.trip()
    assert token.tripped is True


def test_trip_is_idempotent():
    token = CancelToken()
    token.trip()
    token.trip()
    token.trip()
    assert token.tripped is True


def test_trip_is_one_way_no_reset_api():
    token = CancelToken()
    token.trip()
    assert token.tripped is True
    assert not hasattr(token, "reset")


def test_raise_if_cancelled_raises_step_cancelled_once_tripped():
    token = CancelToken()
    token.trip()
    with pytest.raises(StepCancelled):
        token.raise_if_cancelled()


def test_raise_if_cancelled_level_triggered_raises_every_call():
    token = CancelToken()
    token.trip()
    for _ in range(3):
        with pytest.raises(StepCancelled):
            token.raise_if_cancelled()


async def test_wait_resolves_immediately_if_already_tripped():
    token = CancelToken()
    token.trip()
    await asyncio.wait_for(token.wait(), timeout=0.1)  # must not hang


async def test_wait_resolves_when_tripped_later():
    token = CancelToken()

    async def trip_soon():
        await asyncio.sleep(0.01)
        token.trip()

    asyncio.ensure_future(trip_soon())
    await asyncio.wait_for(token.wait(), timeout=1.0)
    assert token.tripped is True


async def test_wait_does_not_resolve_before_trip():
    token = CancelToken()
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(token.wait(), timeout=0.05)


async def test_multiple_waiters_all_released_on_trip():
    token = CancelToken()
    waiters = [asyncio.ensure_future(token.wait()) for _ in range(5)]
    await asyncio.sleep(0.01)
    token.trip()
    await asyncio.wait_for(asyncio.gather(*waiters), timeout=1.0)
    assert all(w.done() for w in waiters)


# ----------------------------------------------------------------------------
# ctx.sleep is cancellation-aware
# ----------------------------------------------------------------------------


async def test_sleep_raises_immediately_if_already_cancelled():
    token = CancelToken()
    token.trip()
    ctx = make_step_context(cancel=token)
    with pytest.raises(StepCancelled):
        await ctx.sleep(10.0)


async def test_sleep_completes_normally_when_not_cancelled():
    token = CancelToken()
    ctx = make_step_context(cancel=token)
    start = time.monotonic()
    await ctx.sleep(0.05)
    assert time.monotonic() - start >= 0.04
    assert token.tripped is False


async def test_sleep_raises_step_cancelled_when_tripped_mid_sleep():
    token = CancelToken()
    ctx = make_step_context(cancel=token)

    async def trip_soon():
        await asyncio.sleep(0.01)
        token.trip()

    asyncio.ensure_future(trip_soon())
    with pytest.raises(StepCancelled):
        await ctx.sleep(10.0)


# ----------------------------------------------------------------------------
# ctx.run_subprocess: real short-lived child process, process-group kill
# ----------------------------------------------------------------------------


async def test_run_subprocess_returns_exec_result_on_normal_completion():
    ctx = make_step_context()
    result = await ctx.run_subprocess([sys.executable, "-c", "print('hi')"])
    assert result.returncode == 0
    assert b"hi" in result.stdout


async def test_run_subprocess_registers_and_unregisters_with_subprocess_manager():
    manager = FakeSubprocessManager()
    from seedpod.engine.step import StepServices

    ctx = make_step_context(services=StepServices(subprocess_manager=manager))
    await ctx.run_subprocess([sys.executable, "-c", "pass"])
    assert len(manager.registered) == 1
    assert len(manager.unregistered) == 1
    assert manager.registered[0] is manager.unregistered[0]


async def test_run_subprocess_kills_process_group_on_cancel():
    token = CancelToken()
    ctx = make_step_context(cancel=token)

    async def trip_soon():
        await asyncio.sleep(0.2)
        token.trip()

    trip_task = asyncio.ensure_future(trip_soon())
    start = time.monotonic()
    with pytest.raises(StepCancelled):
        # sleep 30: a real short-lived-but-slow child; cancellation must interrupt it
        # long before the 30s would otherwise elapse.
        await ctx.run_subprocess(["sleep", "30"])
    elapsed = time.monotonic() - start
    await trip_task
    # SIGTERM within ~1s of the trip (tripped at ~0.2s) + prompt asyncio.wait wakeup:
    # bounded well under the 10s SIGKILL grace, nowhere near the 30s sleep duration.
    assert elapsed < 5.0


async def test_run_subprocess_raises_timeout_error_and_kills_group_on_timeout():
    ctx = make_step_context()
    start = time.monotonic()
    with pytest.raises(TimeoutError):
        await ctx.run_subprocess(["sleep", "30"], timeout=0.2)
    elapsed = time.monotonic() - start
    assert elapsed < 5.0


async def test_run_subprocess_raise_if_cancelled_before_spawn():
    token = CancelToken()
    token.trip()
    ctx = make_step_context(cancel=token)
    with pytest.raises(StepCancelled):
        await ctx.run_subprocess([sys.executable, "-c", "pass"])
