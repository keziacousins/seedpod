"""``TimerService`` and the instance aggregate (DR-0055), and the timer that can never fire.

An instance's ``expiry`` timer fires through the real Dispatcher. A timer whose aggregate type is
unknown, or whose row is missing, used to roll back and retry (and log) on every poll for ever.
Now a timer whose row is missing is consumed once and logged once. A timer of an unknown aggregate
type is KEPT, postponed an hour and logged (a newer build may own it). Neither blocks a sibling. Valid cluster and
deployment timers are unchanged -- ``test_timer_service.py`` still covers them. Real tmp SQLite,
real Dispatcher, no mocks.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import timedelta

import pytest

from seedpod.core.effects import ScheduleTimer
from seedpod.core.events import (
    DestroyDue,
    InstanceCreateRequested,
    InstanceExpired,
    InstanceProvisioned,
    TtlExpired,
)
from seedpod.runtime.timers import TimerService
from tests.runtime.conftest import (
    NOW,
    make_cluster_row,
    make_deployment_row,
    make_instance_row,
    make_owner_key,
)

_POLL = 0.02
HOUR = timedelta(hours=1)


@pytest.fixture
def service(uow, repos, dispatcher, clock):
    return TimerService(uow, repos, dispatcher, clock, poll_interval=_POLL)


async def _arm(uow, repos, aggregate_type, aggregate_id, key, fire_at, event) -> None:
    async with uow() as t:
        repos.timers.upsert(
            t,
            ScheduleTimer(
                aggregate_type=aggregate_type, aggregate_id=aggregate_id, timer_key=key, fire_at=fire_at, event=event
            ),
            "test",
        )


async def _timers(uow, repos):
    async with uow() as t:
        return {(r.aggregate_type, r.aggregate_id, r.timer_key) for r in repos.timers.list_all(t)}


async def _wait_until(check, *, timeout: float = 2.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not await check():
        if loop.time() >= deadline:
            raise AssertionError(f"condition not met within {timeout}s")
        await asyncio.sleep(0.005)


async def _run_for(service, polls: int = 6) -> None:
    await service.start()
    try:
        await asyncio.sleep(_POLL * polls)
    finally:
        await service.stop()


async def _born_instance(uow, repos, dispatcher, clock, instance_id="i1", slug="box"):
    async with uow() as tx:
        owner = make_owner_key(tx)
    row = make_instance_row(instance_id, slug, owner)
    await dispatcher.apply("instance", instance_id, InstanceCreateRequested(at=NOW, actor="api:alice"), record=row)
    await dispatcher.apply(
        "instance", instance_id,
        InstanceProvisioned(at=NOW, actor="engine:run:r", address="192.0.2.7", resource_ids={"vm": "1"}),
    )
    return row


# ---------------------------------------------------------------------------
# an instance timer fires through the dispatcher
# ---------------------------------------------------------------------------


async def test_the_expiry_timer_fires_and_starts_a_destroy(uow, repos, dispatcher, clock, service):
    row = await _born_instance(uow, repos, dispatcher, clock)
    assert ("instance", "i1", "expiry") in await _timers(uow, repos)

    clock.advance(HOUR + timedelta(seconds=1))
    await _run_for(service)

    async with uow() as t:
        stored = repos.instances.get(t, "i1")
        audits = repos.instance_state_audits.list_for_instance(t, "i1")
        outbox = repos.outbox.list_for_aggregate(t, "instance", "i1")
    assert stored.status == "destroying"
    assert audits[0].event == "InstanceExpired" and audits[0].actor == "timer:expiry"
    assert audits[0].created_at == row.expires_at  # the event's own time (the stored fire_at)
    runs = [json.loads(r.payload) for r in outbox if r.kind == "run_workflow"]
    assert [(p["workflow"], p["aggregate_type"], p["aggregate_id"]) for p in runs] == [
        ("provision", "instance", "i1"),  # the birth's
        ("destroy", "instance", "i1"),  # the expiry's
    ]
    assert ("instance", "i1", "expiry") not in await _timers(uow, repos)  # consumed, exactly once


async def test_a_timer_that_is_not_yet_due_does_not_fire(uow, repos, dispatcher, clock, service):
    await _born_instance(uow, repos, dispatcher, clock)
    clock.advance(HOUR - timedelta(seconds=1))
    await _run_for(service, polls=4)
    async with uow() as t:
        assert repos.instances.get(t, "i1").status == "active"
    assert ("instance", "i1", "expiry") in await _timers(uow, repos)


# ---------------------------------------------------------------------------
# a timer that can never fire
# ---------------------------------------------------------------------------


async def test_an_instance_timer_for_a_missing_row_is_dropped_and_logged_once(uow, repos, service, caplog):
    event = InstanceExpired(at=NOW, actor="timer:expiry")
    await _arm(uow, repos, "instance", "ghost", "k", NOW, event)

    with caplog.at_level(logging.ERROR, logger="seedpod.runtime.timers"):
        await _run_for(service, polls=8)

    assert await _timers(uow, repos) == set()
    dropped = [r for r in caplog.records if "timer dropped" in r.getMessage()]
    assert len(dropped) == 1  # once, not once per poll
    message = dropped[0].getMessage()
    assert "instance" in message and "ghost" in message and "does not exist" in message
    assert "InstanceExpired" in message  # the stored event is in the log: the row held the only copy
    assert not [r for r in caplog.records if r.exc_info]  # and no traceback storm


@pytest.mark.parametrize(
    "aggregate_type, event",
    [
        ("cluster", TtlExpired(at=NOW, actor="timer:ttl")),
        ("deployment", DestroyDue(at=NOW, actor="timer:destroy")),
    ],
)
async def test_a_cluster_or_deployment_timer_for_a_missing_row_is_not_dropped(
    uow, repos, service, caplog, aggregate_type, event
):
    """The drop above is the instance rule. A cluster's or deployment's timer for a missing row
    behaves as it did before DR-0055: the fire fails and rolls back, and the timer stays armed
    (``test_timer_service.py`` holds the original, unchanged tests for this)."""
    await _arm(uow, repos, aggregate_type, "ghost", "k", NOW, event)

    with caplog.at_level(logging.ERROR, logger="seedpod.runtime.timers"):
        await _run_for(service, polls=5)

    assert await _timers(uow, repos) == {(aggregate_type, "ghost", "k")}  # still armed
    assert not [r for r in caplog.records if "timer dropped" in r.getMessage()]


@pytest.mark.parametrize("aggregate_type", ["run", "Instance", "instances", ""])
async def test_a_timer_for_an_unknown_aggregate_type_is_kept_postponed_an_hour_and_logged_once(
    uow, repos, service, clock, caplog, aggregate_type
):
    """A newer build may own this aggregate type, so a rollback must not delete its timers. The
    row survives with its event intact and fires an hour later (when this build looks again)."""
    event = InstanceExpired(at=NOW, actor="timer:expiry")
    await _arm(uow, repos, aggregate_type, "x", "k", NOW, event)
    async with uow() as t:
        (before,) = repos.timers.list_all(t)

    with caplog.at_level(logging.ERROR, logger="seedpod.runtime.timers"):
        await _run_for(service, polls=8)

    async with uow() as t:
        (kept,) = repos.timers.list_all(t)  # still there
    assert kept.fire_at == clock.now() + HOUR  # pushed forward one hour from now
    assert (kept.aggregate_type, kept.aggregate_id, kept.timer_key) == (aggregate_type, "x", "k")
    assert kept.event == before.event and kept.created_by_effect == before.created_by_effect  # untouched
    kept_logs = [r for r in caplog.records if "timer kept" in r.getMessage()]
    assert len(kept_logs) == 1 and "no machine for aggregate type" in kept_logs[0].getMessage()  # once, not per poll
    assert "InstanceExpired" in kept_logs[0].getMessage()

    # An hour on, it is looked at again, and kept again.
    clock.advance(HOUR + timedelta(seconds=1))
    await _run_for(service, polls=4)
    async with uow() as t:
        (again,) = repos.timers.list_all(t)
    assert again.fire_at == clock.now() + HOUR


async def test_a_dead_timer_does_not_block_its_due_siblings(uow, repos, dispatcher, clock, service):
    await _born_instance(uow, repos, dispatcher, clock)
    async with uow() as t:
        repos.clusters.insert(
            t, make_cluster_row("c1", "demo", status="destroy-scheduled", pre_destroy_state="active", version=0)
        )
        repos.deployments.insert(t, make_deployment_row("d1", "c1", status="active"))
    # Dead timers FIRST in fire order (earliest fire_at), valid ones after: the old loop would
    # have failed these every pass; it must still reach every sibling in the same batch. The
    # unknown-type one is kept (postponed); the instance one for a missing row is consumed.
    early = NOW - HOUR
    await _arm(uow, repos, "run", "x", "k", early, InstanceExpired(at=early, actor="timer:expiry"))
    await _arm(uow, repos, "instance", "ghost", "expiry", early, InstanceExpired(at=early, actor="timer:expiry"))
    await _arm(uow, repos, "cluster", "c1", "destroy", NOW, DestroyDue(at=NOW, actor="timer:destroy"))
    clock.advance(HOUR + timedelta(seconds=1))  # makes the instance's own expiry due too

    async def _both_fired() -> bool:
        async with uow() as t:
            return (
                repos.clusters.get(t, "c1").status == "destroying"
                and repos.instances.get(t, "i1").status == "destroying"
            )

    await service.start()
    try:
        await _wait_until(_both_fired)
    finally:
        await service.stop()
    assert await _timers(uow, repos) == {("run", "x", "k")}  # kept for a newer build; the missing instance's consumed


async def test_a_valid_timer_for_an_existing_aggregate_still_retries_after_a_real_failure(
    db, uow, repos, dispatcher, clock, service
):
    """The drop is for 'can never fire'. A failure inside a fire for a row that EXISTS is still a
    rollback that leaves the timer armed (``test_timer_service.py``'s atomic-consume law), for
    an instance as for a cluster."""
    await _born_instance(uow, repos, dispatcher, clock)
    with db.engine.begin() as conn:
        conn.exec_driver_sql(
            "CREATE TRIGGER poison BEFORE UPDATE ON instances BEGIN SELECT RAISE(ABORT, 'injected failure'); END"
        )
    clock.advance(HOUR + timedelta(seconds=1))

    await _run_for(service, polls=6)

    async with uow() as t:
        assert repos.instances.get(t, "i1").status == "active"  # the transition rolled back
    assert ("instance", "i1", "expiry") in await _timers(uow, repos)  # and the timer is still armed
