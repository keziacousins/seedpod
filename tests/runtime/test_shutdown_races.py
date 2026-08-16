"""tests/runtime/test_shutdown_races.py — the two chained bugs behind an
intermittent ``App.stop()`` teardown hang (root-caused 2026-08-02 from three
captured task-stack dumps, then reproduced deterministically).

The hang's signature was an event loop parked with ZERO registered fds and ZERO
timers — not a network wait, a deadlock — inside ``tests/conftest.py``'s
``make_app`` finalizer at ``await app.stop()``. Two independent defects chain:

1. **Orphaned DR-0008 write lock** (``seedpod/data/uow.py``). ``_Transaction.
   __aexit__``'s ``finally`` awaited ``asyncio.to_thread(session.close)`` and only
   then released the lock. A ``task.cancel()`` landing on that await raised
   straight past the release, leaving the process-global write lock held by a task
   that no longer existed (dump: ``[locked, waiters:1]``, no live holder). Every
   later ``uow()`` in the process then blocks on ``acquire()`` forever.

2. **Swallowed cancel** (``TimerService``/``EffectExecutor``). Both poll loops wait
   on ``asyncio.wait_for(self._wake.wait(), timeout=...)``. Python 3.11's
   pre-3.12 ``wait_for`` has no ``uncancel`` bookkeeping, so the single
   ``task.cancel()`` their ``stop()`` used to issue is LOST when it lands in the
   same window in which a ``poke()`` completes the inner ``wake.wait()``. The task
   is branded ``cancelling`` yet keeps running, and ``stop()``'s ``await task``
   never returns. Measured: 100% loss when the cancel follows the poke by <=1
   event-loop tick, 0% at >=2.

The correlation that made it fire in real teardowns: ``timers.stop()`` cancels the
timer task mid-transaction (leaking the lock via bug 1), and that pass's final
commit ``poke()``s the executor, opening bug 2's swallow window for
``executor.stop()`` moments later.

These tests are the repro scripts (``/tmp/hang-experiments/e2_lockleak.py``,
``e1_stage2.py``) reduced to deterministic regressions. Zero ``Mock``/``patch``:
the only doubles are two hand-written wrappers that widen the ``session.close()``
window, and they wrap a REAL SQLAlchemy Session behind a real ``Database``.
"""

from __future__ import annotations

import asyncio
import contextlib
import threading

import pytest

from seedpod.data.database import Database
from seedpod.data.migrate import migrate
from seedpod.data.uow import UnitOfWork
from seedpod.runtime.timers import TimerService

# Bounded so a stuck worker thread can never wedge the interpreter's atexit
# ThreadPoolExecutor join, even if the test fails before opening the gate.
_CLOSE_BOUND_SECONDS = 1.0


# ---------------------------------------------------------------------------
# Bug 1 — a cancel landing on session.close() must not orphan the write lock.
# ---------------------------------------------------------------------------


class _SlowCloseSession:
    """Wraps a REAL SQLAlchemy Session, delegating everything except ``close()``,
    which signals then blocks (bounded) so a ``cancel()`` can be landed precisely
    inside ``__aexit__``'s ``await asyncio.to_thread(session.close)``."""

    def __init__(self, real_session, close_started: threading.Event, gate: threading.Event) -> None:
        self._real = real_session
        self._close_started = close_started
        self._gate = gate

    def close(self):
        self._close_started.set()
        self._gate.wait(timeout=_CLOSE_BOUND_SECONDS)
        return self._real.close()

    def __getattr__(self, name):
        return getattr(self._real, name)


class _SlowCloseDatabase:
    """Duck-types the only surface ``_Transaction`` uses of ``Database``: ``.session``
    (``uow.py``: ``await asyncio.to_thread(self._db.session)``)."""

    def __init__(self, real_db: Database, close_started: threading.Event, gate: threading.Event) -> None:
        self._real_db = real_db
        self._close_started = close_started
        self._gate = gate

    def session(self):
        return _SlowCloseSession(self._real_db.session(), self._close_started, self._gate)


async def test_cancel_during_session_close_does_not_orphan_the_write_lock(tmp_path):
    """The regression for bug 1. A task is cancelled while its transaction is
    inside ``session.close()``; afterwards a FRESH task must still be able to open
    a transaction. Before the fix the lock was never released and this probe hung
    forever — which is exactly how a whole test session's teardown deadlocked."""
    real_db = Database(f"sqlite:///{tmp_path / 'lockleak.db'}")
    migrate(real_db.engine)
    close_started = threading.Event()
    gate = threading.Event()
    uow = UnitOfWork(_SlowCloseDatabase(real_db, close_started, gate))  # type: ignore[arg-type]

    async def _holder() -> None:
        async with uow() as session:
            _ = session

    holder = asyncio.create_task(_holder(), name="holder")

    # Wait until the transaction is genuinely inside close() (a real to_thread
    # suspension point), then cancel exactly there.
    while not close_started.is_set():
        await asyncio.sleep(0.005)
    holder.cancel()
    await asyncio.sleep(0)  # let the cancel be delivered while close() still blocks
    gate.set()
    with contextlib.suppress(asyncio.CancelledError):
        await holder

    # The lock must be free. Probing behaviourally (can a new task transact?)
    # rather than poking at UnitOfWork internals.
    async def _probe() -> str:
        async with uow() as session:
            _ = session
        return "acquired"

    assert await asyncio.wait_for(asyncio.create_task(_probe()), timeout=5.0) == "acquired"


# ---------------------------------------------------------------------------
# Bug 2 — stop() must complete even when a poke() immediately precedes it.
# ---------------------------------------------------------------------------


@pytest.fixture
def timer_service(uow, repos, dispatcher, clock):
    return TimerService(uow, repos, dispatcher, clock, poll_interval=10.0)


@pytest.mark.parametrize("ticks_after_poke", [0, 1, 2])
async def test_timer_service_stop_completes_when_a_poke_precedes_it(timer_service, ticks_after_poke):
    await _assert_stop_completes(timer_service, ticks_after_poke)


@pytest.mark.parametrize("ticks_after_poke", [0, 1, 2])
async def test_effect_executor_stop_completes_when_a_poke_precedes_it(executor, ticks_after_poke):
    await _assert_stop_completes(executor, ticks_after_poke)


async def _assert_stop_completes(service, ticks_after_poke: int) -> None:
    """``poke()`` then ``stop()`` separated by exactly ``ticks_after_poke`` event-loop
    ticks. Offsets 0 and 1 are the window in which the old cancel-only ``stop()``
    lost its cancellation 100% of the time; 2 is the control that always worked.

    Detection deliberately uses ``asyncio.wait``, NOT ``asyncio.wait_for``:
    ``wait_for``'s timeout path cancels the future it guards, which would deliver a
    SECOND cancel to the hung loop task — landing long after the race, so it would
    succeed and silently mask the very hang under test. ``asyncio.wait`` cancels
    nothing, so it reports faithfully what a bare ``await service.stop()`` in
    production would experience: no rescuer."""
    await service.start()
    # Let the loop genuinely reach its wake-wait (real to_thread IO precedes it).
    for _ in range(50):
        await asyncio.sleep(0.005)

    service.poke()
    for _ in range(ticks_after_poke):
        await asyncio.sleep(0)

    stop_task = asyncio.create_task(service.stop(), name="stop")
    done, pending = await asyncio.wait({stop_task}, timeout=5.0)

    if pending:  # keep a failure from leaking a live task into the next test
        stop_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await stop_task
    assert done, (
        f"stop() did not complete within 5s with the cancel {ticks_after_poke} tick(s) after poke() "
        "— the loop task swallowed its cancellation"
    )
    assert not service.running
