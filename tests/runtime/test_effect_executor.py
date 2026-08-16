"""``seedpod/runtime/effect_executor.py`` -- the drain loop's non-admission
surface: notify one-attempt/best-effort delivery (proving UI-contract obligation
1's ``deployment_status_changed`` payload shape end-to-end through a real
``Dispatcher`` transition), the backoff ladder + dead-at-8 threshold, hourly
pruning (DR-0002: deletes old ``done`` rows, never touches ``dead`` ones), and
the ``start``/``stop``/``poke`` background-loop lifecycle. Run-admission
(``run_workflow``/``cancel_workflow`` drain rules, coherence-review.md Conflict
2) lives in ``test_run_admission.py``.

Real tmp SQLite (``tests/runtime/conftest.py``'s fixtures), ``FrozenClock``, and
hand-built fakes (``FakeHub``) -- no Mock/patch anywhere (CLAUDE.md).
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from seedpod.core.effects import Notify
from seedpod.core.events import CancelRequested
from seedpod.data.repositories import OutboxRow
from seedpod.runtime.dispatcher import outbox_row
from seedpod.runtime.effect_executor import EffectExecutor
from tests.runtime.conftest import NOW, FakeHub, make_cluster_row, make_deployment_row

_POLL = 0.02  # seconds -- fast enough to keep the lifecycle test quick


async def _wait_until(check, *, timeout: float = 2.0, interval: float = 0.005) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        if await check():
            return
        if loop.time() >= deadline:
            raise AssertionError(f"condition not met within {timeout}s")
        await asyncio.sleep(interval)


# ---------------------------------------------------------------------------
# notify: UI-contract obligation 1's deployment_status_changed payload shape,
# proven end-to-end through a real Dispatcher/core.machine transition.
# ---------------------------------------------------------------------------


async def test_notify_broadcasts_deployment_status_changed_payload_obligation_1(
    uow, repos, dispatcher, executor, hub
):
    async with uow() as t:
        repos.clusters.insert(t, make_cluster_row("c1", "demo", status="active"))
        repos.deployments.insert(t, make_deployment_row("d1", "c1", status="pending"))
    await dispatcher.apply("deployment", "d1", CancelRequested(at=NOW, actor="api:test"))

    await executor.drain_pending()

    assert hub.calls == [
        (
            "deployment_status_changed",
            {"deployment_id": "d1", "cluster_id": "c1", "old_status": "pending", "new_status": "cancelled"},
            "ephemeral",  # DR-0010: Notify.environment threads through as broadcast()'s environment
        )
    ]
    async with uow() as t:
        rows = repos.outbox.list_for_aggregate(t, "deployment", "d1")
    notify_row = next(r for r in rows if r.kind == "notify")
    assert notify_row.status == "done"


# ---------------------------------------------------------------------------
# notify: broadcast failure -> row marked done anyway, NEVER dead.
# ---------------------------------------------------------------------------


async def test_notify_broadcast_failure_marks_done_never_dead(uow, repos, engine, dispatch, clock):
    hub = FakeHub(raise_on=frozenset({"cluster_state_changed"}))
    executor = EffectExecutor(uow, repos, hub, engine, dispatch, clock, poll_interval=10.0)
    effect = Notify(topic="cluster_state_changed", payload={"cluster_id": "c1"}, environment=None)
    row = outbox_row(effect, "cluster", "c1", 1, 0, now=NOW)
    async with uow() as t:
        repos.outbox.insert(t, row)

    await executor.drain_pending()

    async with uow() as t:
        after = repos.outbox.get(t, row.effect_id)
    assert after.status == "done"
    assert after.attempts == 0


# ---------------------------------------------------------------------------
# Decode failures: a payload that fails to decode gets the SAME policy as the
# row's own kind, never a special "abort the whole drain pass" path (adversarial
# review finding -- an undecodable drain-lane row must not starve every row
# behind it in seq order or make executor.start() raise).
# ---------------------------------------------------------------------------


async def test_undecodable_notify_row_marks_done_never_dead(uow, repos, executor):
    bad_row = OutboxRow(
        seq=None, effect_id="e-bad-notify", aggregate_type="cluster", aggregate_id="c1", to_version=1, ordinal=0,
        kind="notify", payload="{not valid json", lane="drain", status="pending", attempts=0,
        available_at=NOW, created_at=NOW, done_at=None, last_error=None,
    )
    async with uow() as t:
        repos.outbox.insert(t, bad_row)

    await executor.drain_pending()  # must not raise

    async with uow() as t:
        after = repos.outbox.get(t, "e-bad-notify")
    assert after.status == "done"
    assert after.attempts == 0


async def test_undecodable_run_workflow_row_degrades_via_backoff_not_starve_later_rows(
    uow, repos, executor, clock, hub
):
    """A malformed run_workflow row (e.g. written by a newer binary, then rolled
    back -- the durable outbox replays across restarts by design) must degrade
    through the ordinary backoff-then-dead policy, not raise out of
    ``drain_pending()`` and starve every row seq-ordered behind it."""
    bad_row = OutboxRow(
        seq=None, effect_id="e-bad-rw", aggregate_type="cluster", aggregate_id="c1", to_version=1, ordinal=0,
        kind="run_workflow", payload="{not valid json", lane="drain", status="pending", attempts=0,
        available_at=NOW, created_at=NOW, done_at=None, last_error=None,
    )
    good_effect = Notify(topic="cluster_state_changed", payload={"cluster_id": "c1"}, environment=None)
    async with uow() as t:
        repos.outbox.insert(t, bad_row)
        good_row = outbox_row(good_effect, "cluster", "c1", 1, 1, now=NOW)
        repos.outbox.insert(t, good_row)

    await executor.drain_pending()  # must not raise

    async with uow() as t:
        after_bad = repos.outbox.get(t, "e-bad-rw")
        after_good = repos.outbox.get(t, good_row.effect_id)
    assert after_bad.status == "pending"
    assert after_bad.attempts == 1
    assert after_bad.available_at == NOW + timedelta(seconds=1.0)
    assert after_bad.last_error is not None
    assert after_good.status == "done"  # NOT starved behind the undecodable row
    assert hub.calls == [("cluster_state_changed", {"cluster_id": "c1"}, None)]


# ---------------------------------------------------------------------------
# Unexpected drain-lane kind: dead-lettered immediately (below attempts >= 8 --
# the outbox lane invariant broke upstream, not a transient failure).
# ---------------------------------------------------------------------------


async def test_unexpected_drain_lane_kind_marks_dead_immediately(uow, repos, executor):
    bad_row = OutboxRow(
        seq=None, effect_id="e-bad-kind", aggregate_type="cluster", aggregate_id="c1", to_version=1, ordinal=0,
        kind="schedule_timer", payload="{}", lane="drain", status="pending", attempts=0,
        available_at=NOW, created_at=NOW, done_at=None, last_error=None,
    )
    async with uow() as t:
        repos.outbox.insert(t, bad_row)

    await executor.drain_pending()

    async with uow() as t:
        after = repos.outbox.get(t, "e-bad-kind")
    assert after.status == "dead"
    assert after.attempts == 0  # dead-lettered below the attempts >= 8 threshold, deliberately
    assert after.last_error is not None


# ---------------------------------------------------------------------------
# Backoff ladder [1s, 5s, 30s, 2m, 10m...] + dead at attempts >= 8.
# ---------------------------------------------------------------------------


async def test_backoff_ladder_and_dead_at_attempt_8(uow, repos, executor, clock):
    from seedpod.core.effects import RunWorkflow

    # No cluster "ghost" exists -- every admission attempt genuinely fails
    # (LookupError), deterministically, forever.
    eff = RunWorkflow(workflow="provision", cluster_id="ghost")
    row = outbox_row(eff, "cluster", "ghost", 1, 0, now=NOW)
    async with uow() as t:
        repos.outbox.insert(t, row)

    expected_delays = [1.0, 5.0, 30.0, 120.0, 600.0, 600.0, 600.0]
    for attempt, delay in enumerate(expected_delays, start=1):
        await executor.drain_pending()
        async with uow() as t:
            after = repos.outbox.get(t, row.effect_id)
        assert after.status == "pending", f"attempt {attempt}"
        assert after.attempts == attempt, f"attempt {attempt}"
        assert after.available_at == clock.now() + timedelta(seconds=delay), f"attempt {attempt}"
        assert after.last_error is not None
        clock.advance(timedelta(seconds=delay))

    # 8th failure -> dead.
    await executor.drain_pending()
    async with uow() as t:
        dead = repos.outbox.get(t, row.effect_id)
    assert dead.status == "dead"
    assert dead.attempts == 8
    assert dead.last_error is not None

    # Dead rows are never retried, however far the clock advances.
    clock.advance(timedelta(days=1))
    await executor.drain_pending()
    async with uow() as t:
        still_dead = repos.outbox.get(t, row.effect_id)
    assert still_dead.status == "dead"
    assert still_dead.attempts == 8


# ---------------------------------------------------------------------------
# Pruning: deletes old 'done' rows, retains 'dead' rows regardless of age
# (docs/decisions/DR-0002-design-lock-ratification.md).
# ---------------------------------------------------------------------------


async def test_prune_deletes_old_done_rows_and_retains_dead_rows(uow, repos, executor, clock):
    old_done = OutboxRow(
        seq=None, effect_id="e-old-done", aggregate_type="cluster", aggregate_id="c1", to_version=1, ordinal=0,
        kind="notify", payload="{}", lane="drain", status="pending", attempts=0,
        available_at=NOW, created_at=NOW, done_at=None, last_error=None,
    )
    recent_done = OutboxRow(
        seq=None, effect_id="e-recent-done", aggregate_type="cluster", aggregate_id="c1", to_version=1, ordinal=1,
        kind="notify", payload="{}", lane="drain", status="pending", attempts=0,
        available_at=NOW, created_at=NOW, done_at=None, last_error=None,
    )
    dead = OutboxRow(
        seq=None, effect_id="e-dead", aggregate_type="cluster", aggregate_id="c1", to_version=1, ordinal=2,
        kind="notify", payload="{}", lane="drain", status="pending", attempts=8,
        available_at=NOW, created_at=NOW, done_at=None, last_error="boom",
    )
    old_done_at = NOW - timedelta(days=10)  # older than the default 7-day retention
    recent_done_at = NOW - timedelta(days=1)
    async with uow() as t:
        repos.outbox.insert(t, old_done)
        repos.outbox.insert(t, recent_done)
        repos.outbox.insert(t, dead)
        repos.outbox.mark_done(t, "e-old-done", done_at=old_done_at)
        repos.outbox.mark_done(t, "e-recent-done", done_at=recent_done_at)
        repos.outbox.mark_dead(t, "e-dead", attempts=8, last_error="boom")

    deleted = await executor.prune_done()

    assert deleted == 1
    async with uow() as t:
        assert repos.outbox.get(t, "e-old-done") is None
        assert repos.outbox.get(t, "e-recent-done") is not None
        still_dead = repos.outbox.get(t, "e-dead")
    assert still_dead.status == "dead"


# ---------------------------------------------------------------------------
# Housekeeping cadence: `_maybe_prune`'s gate prunes on its FIRST pass, then
# withholds until `_HOUSEKEEPING_INTERVAL` (an hour) has elapsed since the last
# pass (docs/decisions/DR-0002-design-lock-ratification.md: "hourly").
# ---------------------------------------------------------------------------


def _old_done_row(effect_id: str) -> OutboxRow:
    return OutboxRow(
        seq=None, effect_id=effect_id, aggregate_type="cluster", aggregate_id="c1", to_version=1, ordinal=0,
        kind="notify", payload="{}", lane="drain", status="pending", attempts=0,
        available_at=NOW, created_at=NOW, done_at=None, last_error=None,
    )


async def test_maybe_prune_hourly_cadence_gate(uow, repos, executor, clock):
    old_done_at = NOW - timedelta(days=10)  # older than the default 7-day retention

    async with uow() as t:
        repos.outbox.insert(t, _old_done_row("e1"))
        repos.outbox.mark_done(t, "e1", done_at=old_done_at)

    # First pass ever: no `_last_prune_at` yet -> prunes immediately.
    await executor._maybe_prune()
    async with uow() as t:
        assert repos.outbox.get(t, "e1") is None

    async with uow() as t:
        repos.outbox.insert(t, _old_done_row("e2"))
        repos.outbox.mark_done(t, "e2", done_at=old_done_at)

    # Less than an hour since the last pass -> gate withholds, e2 survives.
    clock.advance(timedelta(minutes=30))
    await executor._maybe_prune()
    async with uow() as t:
        assert repos.outbox.get(t, "e2") is not None

    # Past the hour mark -> gate opens again, e2 gets pruned.
    clock.advance(timedelta(minutes=31))
    await executor._maybe_prune()
    async with uow() as t:
        assert repos.outbox.get(t, "e2") is None


# ---------------------------------------------------------------------------
# start()/stop()/poke() background-loop lifecycle.
# ---------------------------------------------------------------------------


async def test_start_stop_poke_lifecycle(uow, repos, engine, dispatch, clock):
    hub = FakeHub()
    executor = EffectExecutor(uow, repos, hub, engine, dispatch, clock, poll_interval=_POLL)

    assert executor.running is False
    await executor.start()
    try:
        assert executor.running is True

        effect = Notify(topic="cluster_state_changed", payload={"cluster_id": "c1"}, environment=None)
        row = outbox_row(effect, "cluster", "c1", 1, 0, now=NOW)
        async with uow() as t:
            repos.outbox.insert(t, row)
        executor.poke()

        async def _drained() -> bool:
            async with uow() as t:
                r = repos.outbox.get(t, row.effect_id)
            return r.status == "done"

        await _wait_until(_drained)
        assert hub.calls == [("cluster_state_changed", {"cluster_id": "c1"}, None)]
    finally:
        await executor.stop()

    assert executor.running is False
    await executor.stop()  # idempotent


async def test_start_drains_pending_rows_before_returning_h7(uow, repos, engine, dispatch, clock):
    """H7 crash replay -- Conflict 15's amended App.start order: start() must
    drain everything already-pending BEFORE it returns, not merely schedule it."""
    hub = FakeHub()
    effect = Notify(topic="cluster_state_changed", payload={"cluster_id": "c1"}, environment=None)
    row = outbox_row(effect, "cluster", "c1", 1, 0, now=NOW)
    async with uow() as t:
        repos.outbox.insert(t, row)

    executor = EffectExecutor(uow, repos, hub, engine, dispatch, clock, poll_interval=10.0)
    await executor.start()
    try:
        # No wait_until, no sleep -- if drain-before-return holds, this is already true.
        assert hub.calls == [("cluster_state_changed", {"cluster_id": "c1"}, None)]
        async with uow() as t:
            after = repos.outbox.get(t, row.effect_id)
        assert after.status == "done"
    finally:
        await executor.stop()
