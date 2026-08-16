"""``seedpod/runtime/timers.py`` -- real tmp SQLite (``migrate()`` onto
``tmp_path``, ``tests/runtime/conftest.py``'s fixtures), a real ``Dispatcher``, and
``FrozenClock``. No Mock/patch anywhere. Every scenario is driven only through
``TimerService``'s public surface (``start``/``stop``/``poke``/``next_fire_at``) --
never a private method -- matching ``tests/engine/fakes.py``'s ``crash_run`` comment
("engine's PUBLIC surface ... never anything retained/underscored").

Covers: a due timer fires exactly once (row deleted + transition applied together);
an injected mid-fire failure (timer pointed at a nonexistent aggregate) rolls back
BOTH the delete and the attempted transition, leaving the row armed for the next poll
(atomic consume, docs/design/seam-a-core.md §D); a bad row does not block a due
sibling in the SAME batch (module docstring, "Per-row isolation"); a genuine
scan-level failure (a real, unmocked SQL error -- the ``timers`` table renamed out
from under the poller for several ticks) does not kill the background poll task
(module docstring, "Scan-level isolation") -- the loop survives every failed pass and
resumes firing once the table is restored; re-arming a timer_key via a second
``ScheduleTimer`` upsert postpones the fire past the original ``fire_at``;
``CancelTimer(timer_key=None)`` clears every armed key for an aggregate so nothing
fires; a stale fire (event no longer valid for the aggregate's current state) Ignores
-- no audit row, no version bump, but the row is still consumed; ``running`` reflects
true task liveness across the ``start``/``stop`` lifecycle; and ``stop()`` is clean
(idempotent, and nothing fires once stopped).

**DR-0009 pinned race tests** (conditional consume,
docs/decisions/DR-0009-conditional-timer-consume.md, RATIFIED): a same-key re-arm or
cancel landing in the real window DR-0008 leaves open between the scan transaction
and a row's own fire transaction. These are constructed deterministically -- no
sleep-race -- by holding the DR-0008 lock open in a background task (mirrors
``tests/data/test_uow.py``'s pinned regression test) until BOTH the service's scan
attempt and the racing re-arm/cancel have queued as waiters on it, in that order;
releasing then forces scan -> race -> fire, exactly the window DR-0009 closes.
Covers: re-arm to a NEW ``fire_at`` between scan and fire -> no apply, the re-armed
row survives untouched and fires at its NEW deadline on a later pass; cancel between
scan and fire -> no apply, nothing left to fire, ever; re-arm to the SAME ``fire_at``
between scan and fire -> fires exactly once (TEXT-equal snapshot still matches).
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest
from sqlalchemy import text

from seedpod.core.effects import CancelTimer, ScheduleTimer
from seedpod.core.events import DestroyDue, TtlExpired
from seedpod.runtime.timers import TimerService
from tests.runtime.conftest import NOW, make_cluster_row

LATER = NOW + timedelta(minutes=5)

_POLL = 0.02  # seconds -- fast enough to keep these tests quick, slow enough not to hot-loop


@pytest.fixture
def service(uow, repos, dispatcher, clock):
    return TimerService(uow, repos, dispatcher, clock, poll_interval=_POLL)


async def _wait_until(check, *, timeout: float = 2.0, interval: float = 0.005) -> None:
    """Poll ``check()`` (an async predicate) until it returns truthy or ``timeout``
    elapses. Standard bounded-wait pattern for asserting on a background task's
    effects without reaching into its private state."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        if await check():
            return
        if loop.time() >= deadline:
            raise AssertionError(f"condition not met within {timeout}s")
        await asyncio.sleep(interval)


async def _arm(uow, repos, aggregate_id: str, timer_key: str, fire_at, event, *, provenance: str = "test") -> None:
    async with uow() as t:
        repos.timers.upsert(
            t,
            ScheduleTimer(
                aggregate_type="cluster", aggregate_id=aggregate_id, timer_key=timer_key, fire_at=fire_at, event=event
            ),
            provenance,
        )


async def _hold_uow_lock(uow):
    """Opens a ``uow()`` transaction and holds it open (mirrors
    ``tests/data/test_uow.py``'s pinned DR-0008 regression test) -- used below to
    force deterministic ordering of later ``uow()`` callers onto the DR-0008 lock's
    FIFO waiter queue. Returns ``(holder_task, release_event)``; the caller sets
    ``release_event`` once every later caller it wants queued has had the chance to
    call ``uow()`` and block on the lock, then awaits ``holder_task``."""
    holding = asyncio.Event()
    release = asyncio.Event()

    async def _holder() -> None:
        async with uow() as t:
            del t
            holding.set()
            await release.wait()

    task = asyncio.ensure_future(_holder())
    await holding.wait()
    return task, release


# ---------------------------------------------------------------------------
# A due timer fires exactly once: row deleted + transition applied, atomically.
# ---------------------------------------------------------------------------


async def test_due_timer_fires_exactly_once(uow, repos, service):
    async with uow() as t:
        repos.clusters.insert(
            t, make_cluster_row("c1", "demo", status="destroy-scheduled", pre_destroy_state="active", version=0)
        )
    await _arm(uow, repos, "c1", "destroy", NOW, DestroyDue(at=NOW, actor="timer:destroy"))

    async def _fired() -> bool:
        async with uow() as t:
            row = repos.clusters.get(t, "c1")
        return row.status == "destroying"

    await service.start()
    try:
        await _wait_until(_fired)
        # give the loop a couple more idle ticks -- nothing left to fire, so the
        # cluster must stay exactly where it landed (no double-fire, no re-fire).
        await asyncio.sleep(_POLL * 3)
    finally:
        await service.stop()

    async with uow() as t:
        row = repos.clusters.get(t, "c1")
        assert row.status == "destroying"
        assert row.version == 1  # exactly one transition, not two

        # the timer row itself is gone (consumed) -- and DESTROYING arms no new timer
        assert repos.timers.get(t, "cluster", "c1", "destroy") is None

        audits = repos.cluster_state_audits.list_for_cluster(t, "c1")
        assert len(audits) == 1
        assert audits[0].from_state == "destroy-scheduled"
        assert audits[0].to_state == "destroying"

        outbox = repos.outbox.list_for_aggregate(t, "cluster", "c1")
        assert {r.kind for r in outbox} == {"persist", "notify", "run_workflow"}


# ---------------------------------------------------------------------------
# Injected mid-fire failure: neither the delete nor the transition lands.
# ---------------------------------------------------------------------------


async def test_mid_fire_failure_rolls_back_delete_and_transition_leaves_timer_armed(uow, repos, service):
    # No cluster row for "ghost" exists -- Dispatcher.apply's internal load raises
    # LookupError, a genuine (unmocked) failure deep inside the fire's transaction.
    await _arm(uow, repos, "ghost", "destroy", NOW, DestroyDue(at=NOW, actor="timer:destroy"))

    await service.start()
    try:
        # Give the loop several real chances to attempt (and keep failing) the fire.
        await asyncio.sleep(_POLL * 5)
    finally:
        await service.stop()

    async with uow() as t:
        # "neither": the delete rolled back right along with the failed apply() --
        # the row is still armed exactly as it was, ready to retry on a later poll.
        timer = repos.timers.get(t, "cluster", "ghost", "destroy")
        assert timer is not None
        assert timer.fire_at == NOW

        assert repos.clusters.get(t, "ghost") is None
        assert repos.outbox.list_for_aggregate(t, "cluster", "ghost") == []


# ---------------------------------------------------------------------------
# Per-row isolation: a bad row in the SAME batch does not block its due sibling.
# ---------------------------------------------------------------------------


async def test_bad_row_does_not_block_due_sibling_in_same_batch(uow, repos, service):
    async with uow() as t:
        repos.clusters.insert(
            t, make_cluster_row("c1", "demo", status="destroy-scheduled", pre_destroy_state="active", version=0)
        )
    # "ghost" has no backing cluster row -- its fire will genuinely fail (the same
    # unmocked LookupError as the mid-fire-failure test above). Both are due at NOW,
    # so a single due() scan returns both in ONE batch; the bad row's failure must
    # not prevent its sibling from firing in that very same tick.
    await _arm(uow, repos, "ghost", "destroy", NOW, DestroyDue(at=NOW, actor="timer:destroy"))
    await _arm(uow, repos, "c1", "destroy", NOW, DestroyDue(at=NOW, actor="timer:destroy"))

    async def _fired() -> bool:
        async with uow() as t:
            row = repos.clusters.get(t, "c1")
        return row.status == "destroying"

    await service.start()
    try:
        await _wait_until(_fired)
        await asyncio.sleep(_POLL * 3)
    finally:
        await service.stop()

    async with uow() as t:
        row = repos.clusters.get(t, "c1")
        assert row.status == "destroying"
        assert row.version == 1

        # the bad row survives untouched, still armed, ready to retry
        ghost_timer = repos.timers.get(t, "cluster", "ghost", "destroy")
        assert ghost_timer is not None
        assert ghost_timer.fire_at == NOW


# ---------------------------------------------------------------------------
# Re-arm via ScheduleTimer upsert postpones the fire.
# ---------------------------------------------------------------------------


async def test_rearm_upsert_postpones_the_fire(uow, repos, dispatcher, clock):
    async with uow() as t:
        repos.clusters.insert(
            t, make_cluster_row("c1", "demo", status="destroy-scheduled", pre_destroy_state="active", version=0)
        )
    # First arm due immediately at NOW, then immediately re-arm the SAME key to
    # fire only at LATER -- the upsert (PK on aggregate_type/aggregate_id/timer_key)
    # must win; the original NOW deadline must never be observed by the poller.
    await _arm(uow, repos, "c1", "destroy", NOW, DestroyDue(at=NOW, actor="timer:destroy"), provenance="first")
    await _arm(uow, repos, "c1", "destroy", LATER, DestroyDue(at=LATER, actor="timer:destroy"), provenance="second")

    service = TimerService(uow, repos, dispatcher, clock, poll_interval=_POLL)
    await service.start()
    try:
        await asyncio.sleep(_POLL * 5)  # clock is still at NOW -- postponed timer must NOT fire
        async with uow() as t:
            row = repos.clusters.get(t, "c1")
            assert row.status == "destroy-scheduled"  # untouched
            assert row.version == 0
            timer = repos.timers.get(t, "cluster", "c1", "destroy")
            assert timer is not None
            assert timer.fire_at == LATER
            assert timer.created_by_effect == "second"

        # Now advance the clock to the postponed deadline and let it fire.
        clock.set(LATER)
        service.poke()

        async def _fired() -> bool:
            async with uow() as t:
                row = repos.clusters.get(t, "c1")
            return row.status == "destroying"

        await _wait_until(_fired)
    finally:
        await service.stop()

    async with uow() as t:
        assert repos.clusters.get(t, "c1").version == 1


# ---------------------------------------------------------------------------
# CancelTimer(timer_key=None) clears all keys -- nothing fires afterward.
# ---------------------------------------------------------------------------


async def test_cancel_all_keys_leaves_nothing_to_fire(uow, repos, service):
    async with uow() as t:
        repos.clusters.insert(t, make_cluster_row("c1", "demo", status="active", expires_at=LATER, version=0))
    await _arm(uow, repos, "c1", "ttl", NOW, TtlExpired(at=NOW, actor="timer:ttl"), provenance="ttl-arm")
    await _arm(uow, repos, "c1", "destroy", NOW, DestroyDue(at=NOW, actor="timer:destroy"), provenance="destroy-arm")

    async with uow() as t:
        repos.timers.delete(t, CancelTimer(aggregate_type="cluster", aggregate_id="c1", timer_key=None))

    async with uow() as t:
        assert repos.timers.get(t, "cluster", "c1", "ttl") is None
        assert repos.timers.get(t, "cluster", "c1", "destroy") is None

    await service.start()
    try:
        await asyncio.sleep(_POLL * 5)  # several ticks with nothing armed
    finally:
        await service.stop()

    async with uow() as t:
        row = repos.clusters.get(t, "c1")
        assert row.status == "active"  # no transition happened -- nothing was armed to fire
        assert row.version == 0
        assert repos.cluster_state_audits.list_for_cluster(t, "c1") == []


# ---------------------------------------------------------------------------
# Stale fire: event no longer valid for the aggregate's current state => Ignore.
# No audit row, no version bump -- but the timer row IS still consumed.
# ---------------------------------------------------------------------------


async def test_stale_fire_ignores_no_audit_no_version_bump(uow, repos, service):
    async with uow() as t:
        repos.clusters.insert(t, make_cluster_row("c1", "demo", status="destroyed", version=0))
    # DESTROYED x TtlExpired is unlisted in the cluster table -> totality-law default
    # Ignore (docs/design/seam-a-core.md §F): a late TTL fire against an
    # already-destroyed cluster.
    await _arm(uow, repos, "c1", "ttl", NOW, TtlExpired(at=NOW, actor="timer:ttl"))

    async def _consumed() -> bool:
        async with uow() as t:
            return repos.timers.get(t, "cluster", "c1", "ttl") is None

    await service.start()
    try:
        await _wait_until(_consumed)
        await asyncio.sleep(_POLL * 3)
    finally:
        await service.stop()

    async with uow() as t:
        row = repos.clusters.get(t, "c1")
        assert row.status == "destroyed"
        assert row.version == 0  # no version bump
        assert repos.cluster_state_audits.list_for_cluster(t, "c1") == []  # no audit row
        assert repos.outbox.list_for_aggregate(t, "cluster", "c1") == []


# ---------------------------------------------------------------------------
# stop() is clean: idempotent, and nothing fires once stopped.
# ---------------------------------------------------------------------------


async def test_stop_is_clean_and_idempotent(uow, repos, service):
    await service.start()
    await service.stop()
    await service.stop()  # idempotent -- no error on a second stop()

    async with uow() as t:
        repos.clusters.insert(
            t, make_cluster_row("c1", "demo", status="destroy-scheduled", pre_destroy_state="active", version=0)
        )
    await _arm(uow, repos, "c1", "destroy", NOW, DestroyDue(at=NOW, actor="timer:destroy"))
    service.poke()  # poking a stopped service must be inert

    await asyncio.sleep(_POLL * 5)

    async with uow() as t:
        row = repos.clusters.get(t, "c1")
        assert row.status == "destroy-scheduled"  # never fired -- the loop is really stopped
        assert row.version == 0
        assert repos.timers.get(t, "cluster", "c1", "destroy") is not None


# ---------------------------------------------------------------------------
# Scan-level isolation: a genuine (unmocked) SQL failure at the scan does not
# permanently kill the background poll task.
# ---------------------------------------------------------------------------


async def test_scan_failure_does_not_kill_poll_loop(uow, repos, service):
    async with uow() as t:
        repos.clusters.insert(
            t, make_cluster_row("c1", "demo", status="destroy-scheduled", pre_destroy_state="active", version=0)
        )
    await _arm(uow, repos, "c1", "destroy", NOW, DestroyDue(at=NOW, actor="timer:destroy"))

    # Break the scan with a REAL, unmocked SQL failure: rename the timers table out
    # from under the poller. repos.timers.due()'s query then genuinely raises
    # OperationalError ("no such table: timers") on every pass -- no Mock/patch
    # anywhere, exactly what a transient disk/locking failure would look like from
    # the poller's point of view.
    async with uow() as t:
        t.execute(text("ALTER TABLE timers RENAME TO timers_hidden"))

    await service.start()
    try:
        assert service.running
        # Several REAL poll passes fail at the scan in a row. Per the module
        # docstring's "Scan-level isolation", the background task must survive
        # every one of them -- not just log and continue, but still BE the same
        # live task afterward (no supervisor could revive a dead one).
        await asyncio.sleep(_POLL * 5)
        assert service.running

        # Restore the table -- the very next pass must resume normal delivery,
        # proving the loop never actually died.
        async with uow() as t:
            t.execute(text("ALTER TABLE timers_hidden RENAME TO timers"))
        service.poke()

        async def _fired() -> bool:
            async with uow() as t:
                row = repos.clusters.get(t, "c1")
            return row.status == "destroying"

        await _wait_until(_fired)
    finally:
        await service.stop()

    async with uow() as t:
        row = repos.clusters.get(t, "c1")
        assert row.status == "destroying"
        assert row.version == 1


# ---------------------------------------------------------------------------
# running: truthful task liveness across the start/stop lifecycle.
# ---------------------------------------------------------------------------


async def test_running_reflects_task_liveness(service):
    assert service.running is False  # never started

    await service.start()
    try:
        assert service.running is True
    finally:
        await service.stop()

    assert service.running is False  # stopped -- task cleared


# ---------------------------------------------------------------------------
# next_fire_at(): cached from the last poll pass.
# ---------------------------------------------------------------------------


async def test_next_fire_at_reflects_earliest_armed_timer(uow, repos, service):
    assert service.next_fire_at() is None  # nothing polled yet

    async with uow() as t:
        repos.clusters.insert(t, make_cluster_row("c1", "demo", status="active", expires_at=LATER, version=0))
    await _arm(uow, repos, "c1", "ttl", LATER, TtlExpired(at=LATER, actor="timer:ttl"))

    async def _seen() -> bool:
        return service.next_fire_at() == LATER

    await service.start()
    try:
        await _wait_until(_seen)
    finally:
        await service.stop()


# ---------------------------------------------------------------------------
# DR-0009: conditional consume -- races landing in the scan-to-fire window.
# ---------------------------------------------------------------------------


async def test_rearm_between_scan_and_fire_skips_apply_and_survives_to_fire_later(uow, repos, dispatcher, clock):
    async with uow() as t:
        repos.clusters.insert(
            t, make_cluster_row("c1", "demo", status="destroy-scheduled", pre_destroy_state="active", version=0)
        )
    await _arm(uow, repos, "c1", "destroy", NOW, DestroyDue(at=NOW, actor="timer:destroy"), provenance="first")

    # A large poll_interval -- only the ONE forced pass below matters; nothing should
    # fire from ordinary periodic polling during this test.
    service = TimerService(uow, repos, dispatcher, clock, poll_interval=10.0)

    holder_task, release_holder = await _hold_uow_lock(uow)
    rearm_done = asyncio.Event()
    try:
        await service.start()  # the scan's uow() call blocks behind the holder

        async def _rearm() -> None:
            await _arm(uow, repos, "c1", "destroy", LATER, DestroyDue(at=LATER, actor="timer:destroy"), provenance="second")
            rearm_done.set()

        race_task = asyncio.ensure_future(_rearm())

        # Give every opportunity for BOTH the service's scan attempt and the race
        # task's uow() call to queue up as waiters on the DR-0008 lock, in that
        # order, while the holder still owns it (tests/data/test_uow.py's pattern).
        for _ in range(20):
            await asyncio.sleep(0)

        release_holder.set()
        await holder_task
        await race_task
        assert rearm_done.is_set()

        # Let the service's queued fire attempt run to completion: scan already saw
        # fire_at=NOW; the conditional consume must now miss (row is LATER) and skip
        # the apply entirely.
        await asyncio.sleep(0.05)

        async with uow() as t:
            row = repos.clusters.get(t, "c1")
            assert row.status == "destroy-scheduled"  # untouched -- stale fire_at=NOW never applied
            assert row.version == 0
            assert repos.cluster_state_audits.list_for_cluster(t, "c1") == []
            timer = repos.timers.get(t, "cluster", "c1", "destroy")
            assert timer is not None  # the re-armed row survived the stale consume attempt
            assert timer.fire_at == LATER
            assert timer.created_by_effect == "second"

        # The surviving row fires at its NEW deadline on a later pass.
        clock.set(LATER)
        service.poke()

        async def _fired() -> bool:
            async with uow() as t:
                row = repos.clusters.get(t, "c1")
            return row.status == "destroying"

        await _wait_until(_fired)
    finally:
        await service.stop()

    async with uow() as t:
        row = repos.clusters.get(t, "c1")
        assert row.version == 1  # exactly one transition -- the LATER fire, not the stale NOW one
        assert repos.timers.get(t, "cluster", "c1", "destroy") is None


async def test_cancel_between_scan_and_fire_skips_apply_and_nothing_fires(uow, repos, dispatcher, clock):
    async with uow() as t:
        repos.clusters.insert(
            t, make_cluster_row("c1", "demo", status="destroy-scheduled", pre_destroy_state="active", version=0)
        )
    await _arm(uow, repos, "c1", "destroy", NOW, DestroyDue(at=NOW, actor="timer:destroy"))

    service = TimerService(uow, repos, dispatcher, clock, poll_interval=10.0)

    holder_task, release_holder = await _hold_uow_lock(uow)
    cancel_done = asyncio.Event()
    try:
        await service.start()

        async def _cancel() -> None:
            async with uow() as t:
                repos.timers.delete(t, CancelTimer(aggregate_type="cluster", aggregate_id="c1", timer_key="destroy"))
            cancel_done.set()

        race_task = asyncio.ensure_future(_cancel())

        for _ in range(20):
            await asyncio.sleep(0)

        release_holder.set()
        await holder_task
        await race_task
        assert cancel_done.is_set()

        await asyncio.sleep(0.05)

        async with uow() as t:
            row = repos.clusters.get(t, "c1")
            assert row.status == "destroy-scheduled"  # never fired
            assert row.version == 0
            assert repos.timers.get(t, "cluster", "c1", "destroy") is None  # stays cancelled

        # Nothing left armed -- advancing the clock and polling further changes nothing.
        clock.set(LATER)
        service.poke()
        await asyncio.sleep(0.05)
    finally:
        await service.stop()

    async with uow() as t:
        row = repos.clusters.get(t, "c1")
        assert row.status == "destroy-scheduled"
        assert row.version == 0
        assert repos.cluster_state_audits.list_for_cluster(t, "c1") == []


async def test_rearm_to_same_fire_at_between_scan_and_fire_still_fires_once(uow, repos, dispatcher, clock):
    async with uow() as t:
        repos.clusters.insert(
            t, make_cluster_row("c1", "demo", status="destroy-scheduled", pre_destroy_state="active", version=0)
        )
    await _arm(uow, repos, "c1", "destroy", NOW, DestroyDue(at=NOW, actor="timer:destroy"), provenance="first")

    service = TimerService(uow, repos, dispatcher, clock, poll_interval=10.0)

    holder_task, release_holder = await _hold_uow_lock(uow)
    rearm_done = asyncio.Event()
    try:
        await service.start()

        async def _rearm_same() -> None:
            # Same fire_at (NOW) -- TEXT-equal to the scan's snapshot, so the
            # conditional consume must still match (DR-0009 §2: harmless re-arm).
            await _arm(uow, repos, "c1", "destroy", NOW, DestroyDue(at=NOW, actor="timer:destroy"), provenance="second")
            rearm_done.set()

        race_task = asyncio.ensure_future(_rearm_same())

        for _ in range(20):
            await asyncio.sleep(0)

        release_holder.set()
        await holder_task
        await race_task
        assert rearm_done.is_set()

        async def _fired() -> bool:
            async with uow() as t:
                row = repos.clusters.get(t, "c1")
            return row.status == "destroying"

        await _wait_until(_fired)
        # A few more idle ticks -- must not fire twice.
        await asyncio.sleep(0.05)
    finally:
        await service.stop()

    async with uow() as t:
        row = repos.clusters.get(t, "c1")
        assert row.status == "destroying"
        assert row.version == 1  # fired exactly once, not twice
        assert repos.timers.get(t, "cluster", "c1", "destroy") is None
        audits = repos.cluster_state_audits.list_for_cluster(t, "c1")
        assert len(audits) == 1
