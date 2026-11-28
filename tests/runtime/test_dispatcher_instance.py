"""The Dispatcher drives the ``instance`` aggregate (DR-0055): birth, load, persist with an
optimistic version, audit in the same transaction, outbox rows, timers. Real tmp SQLite, no mocks.
"""

from __future__ import annotations

import json
from datetime import timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from seedpod.core.events import (
    InstanceCreateRequested,
    InstanceDestroyRequested,
    InstanceDestroySucceeded,
    InstanceExpired,
    InstanceProvisioned,
    InstanceProvisionFailed,
    InstanceRenewRequested,
    InstanceResourcesAllocated,
    InstanceRunningObserved,
)
from seedpod.core.machine import InvalidTransition, StaleVersion
from seedpod.core.records import InstanceRecord, InstanceStatus
from tests.runtime.conftest import (
    NOW,
    FakePokeable,
    make_cluster_row,
    make_instance_row,
    make_owner_key,
)

HOUR = timedelta(hours=1)
LATER = NOW + timedelta(minutes=5)


def _counts(db) -> dict[str, int]:
    tables = ("clusters", "instances", "instance_state_audits", "cluster_state_audits", "effects_outbox", "timers")
    with db.engine.begin() as conn:
        return {t: conn.exec_driver_sql(f"SELECT COUNT(*) FROM {t}").scalar() for t in tables}


async def _born(uow, repos, dispatcher, instance_id="i1", slug="box", **overrides):
    async with uow() as tx:
        owner = make_owner_key(tx)
    row = make_instance_row(instance_id, slug, owner, **overrides)
    await dispatcher.apply("instance", instance_id, InstanceCreateRequested(at=NOW, actor="api:alice"), record=row)
    return row


async def _get(uow, repos, instance_id="i1"):
    async with uow() as tx:
        return repos.instances.get(tx, instance_id)


# ---------------------------------------------------------------------------
# birth
# ---------------------------------------------------------------------------


async def test_birth_inserts_the_row_the_audit_the_outbox_rows_and_the_expiry_timer(db, uow, repos, dispatcher):
    fake = FakePokeable()
    dispatcher.attach_executor(fake)
    row = await _born(uow, repos, dispatcher)

    stored = await _get(uow, repos)
    assert (stored.status, stored.version) == ("provisioning", 1)
    # row-only columns pass through from the caller's row untouched
    assert (stored.template, stored.size, stored.owner_key_id, stored.vcpu) == ("ubuntu", "small", row.owner_key_id, 1)
    assert stored.expires_at == row.expires_at and stored.hard_expires_at == row.hard_expires_at

    async with uow() as tx:
        audits = repos.instance_state_audits.list_for_instance(tx, "i1")
        timer = repos.timers.get(tx, "instance", "i1", "expiry")
        outbox = {r.effect_id: r for r in db_outbox(tx)}
    assert len(audits) == 1
    (audit,) = audits
    assert (audit.from_state, audit.to_state, audit.event, audit.actor) == (
        "new", "provisioning", "InstanceCreateRequested", "api:alice"
    )
    assert audit.created_at == NOW  # event.at, not a clock read
    assert audit.context["kind"] == "InstanceCreateRequested"

    assert timer is not None and timer.fire_at == row.expires_at
    assert json.loads(timer.event)["kind"] == "InstanceExpired"

    assert sorted(outbox) == [
        "instance/i1@1#0", "instance/i1@1#1", "instance/i1@1#2", "instance/i1@1#3",
    ]
    by_kind = {r.kind: r for r in outbox.values()}
    assert by_kind["persist"].status == "done" and by_kind["schedule_timer"].status == "done"
    assert by_kind["notify"].status == "pending" and by_kind["run_workflow"].status == "pending"
    assert {r.aggregate_type for r in outbox.values()} == {"instance"}
    assert json.loads(by_kind["run_workflow"].payload)["aggregate_type"] == "instance"
    assert fake.count == 1  # poked once, after the commit


def db_outbox(tx):
    return [
        type("R", (), dict(m))
        for m in tx.execute(text("SELECT * FROM effects_outbox ORDER BY seq")).mappings().all()
    ]


async def test_birth_with_no_row_supplied_is_a_caller_bug_and_writes_nothing(db, uow, dispatcher):
    async with uow() as tx:
        make_owner_key(tx)
    before = _counts(db)
    with pytest.raises(LookupError):
        await dispatcher.apply("instance", "i1", InstanceCreateRequested(at=NOW, actor="api:alice"))
    assert _counts(db) == before


async def test_birth_refused_by_the_machine_writes_nothing(db, uow, dispatcher):
    async with uow() as tx:
        owner = make_owner_key(tx)
    before = _counts(db)
    bad = make_instance_row("i1", "box", owner, lease_seconds=0)
    with pytest.raises(InvalidTransition):
        await dispatcher.apply("instance", "i1", InstanceCreateRequested(at=NOW, actor="api:alice"), record=bad)
    assert _counts(db) == before


async def test_a_second_live_instance_cannot_take_the_slug(uow, repos, dispatcher):
    await _born(uow, repos, dispatcher, "i1", "box")
    async with uow() as tx:
        owner = make_owner_key(tx)
    with pytest.raises(IntegrityError):
        await dispatcher.apply(
            "instance", "i2", InstanceCreateRequested(at=NOW, actor="api:alice"),
            record=make_instance_row("i2", "box", owner),
        )
    # the failed birth left nothing behind
    assert await _get(uow, repos, "i2") is None


# ---------------------------------------------------------------------------
# load + persist
# ---------------------------------------------------------------------------


async def test_provision_success_loads_persists_bumps_the_version_and_audits(uow, repos, dispatcher):
    await _born(uow, repos, dispatcher)
    await dispatcher.apply(
        "instance", "i1",
        InstanceResourcesAllocated(at=NOW + timedelta(seconds=5), actor="engine:run:r1", resource_ids={"vm": "spi-box"}),
    )
    await dispatcher.apply(
        "instance", "i1",
        InstanceProvisioned(at=LATER, actor="engine:run:r1", address="192.0.2.7", resource_ids={"disk": "d1"}),
    )

    stored = await _get(uow, repos)
    assert (stored.status, stored.version) == ("active", 3)
    assert stored.address == "192.0.2.7"
    assert dict(stored.resource_ids) == {"vm": "spi-box", "disk": "d1"}
    async with uow() as tx:
        audits = repos.instance_state_audits.list_for_instance(tx, "i1")
    assert [(a.from_state, a.to_state, a.event) for a in reversed(audits)] == [
        ("new", "provisioning", "InstanceCreateRequested"),
        ("provisioning", "provisioning", "InstanceResourcesAllocated"),
        ("provisioning", "active", "InstanceProvisioned"),
    ]


async def test_provision_failure_records_the_reason_and_keeps_the_timer_armed(uow, repos, dispatcher):
    await _born(uow, repos, dispatcher)
    await dispatcher.apply(
        "instance", "i1", InstanceProvisionFailed(at=LATER, actor="engine:run:r1", reason="disk full")
    )
    stored = await _get(uow, repos)
    assert (stored.status, stored.failure_reason) == ("failed", "disk full")
    async with uow() as tx:
        assert repos.timers.get(tx, "instance", "i1", "expiry") is not None
        (latest, *_rest) = repos.instance_state_audits.list_for_instance(tx, "i1")
    assert latest.reason == "disk full"  # DR-0007: the event's own reason field


async def test_renew_rearms_the_timer_and_leaves_the_hard_limit_alone(uow, repos, dispatcher):
    row = await _born(uow, repos, dispatcher)
    await dispatcher.apply(
        "instance", "i1", InstanceProvisioned(at=NOW, actor="engine:run:r1", address="a", resource_ids={"vm": "1"})
    )
    await dispatcher.apply("instance", "i1", InstanceRenewRequested(at=NOW + timedelta(minutes=30), actor="api:alice"))

    stored = await _get(uow, repos)
    assert stored.expires_at == NOW + timedelta(minutes=30) + HOUR
    assert stored.hard_expires_at == row.hard_expires_at
    async with uow() as tx:
        timer = repos.timers.get(tx, "instance", "i1", "expiry")
    assert timer.fire_at == stored.expires_at


async def test_a_refused_renewal_writes_nothing(db, uow, repos, dispatcher):
    await _born(uow, repos, dispatcher)  # still provisioning: renew is refused
    before = _counts(db)
    with pytest.raises(InvalidTransition):
        await dispatcher.apply("instance", "i1", InstanceRenewRequested(at=LATER, actor="api:alice"))
    assert _counts(db) == before
    assert (await _get(uow, repos)).version == 1


async def test_a_missing_instance_raises_a_lookup_error_and_writes_nothing(db, dispatcher):
    before = _counts(db)
    with pytest.raises(LookupError):
        await dispatcher.apply("instance", "ghost", InstanceExpired(at=NOW, actor="timer:expiry"))
    assert _counts(db) == before


async def test_a_stale_persist_raises_stale_version_and_rolls_the_whole_transition_back(db, uow, repos, dispatcher):
    row = await _born(uow, repos, dispatcher)
    record = InstanceRecord(
        id="i1", slug="box", state=InstanceStatus.PROVISIONING, version=2, provider="fake", environment="ephemeral",
        lease_seconds=3600, expires_at=row.expires_at, hard_expires_at=row.hard_expires_at,
    )
    async with uow() as tx:
        with pytest.raises(StaleVersion):
            repos.instances.persist(tx, record, 1_000, clock=dispatcher._clock)  # noqa: SLF001
        # the CAS matched nothing, so the row is as it was
        assert repos.instances.get(tx, "i1").version == 1


async def test_audit_and_persist_commit_or_roll_back_together(db, uow, repos, dispatcher):
    await _born(uow, repos, dispatcher)
    before = _counts(db)

    class Boom(Exception):
        pass

    async def apply_then_fail() -> None:
        async with uow() as tx:
            await dispatcher.apply(
                "instance", "i1",
                InstanceProvisioned(at=LATER, actor="engine:run:r1", address="a", resource_ids={"vm": "1"}),
                tx=tx,
            )
            raise Boom  # the caller's transaction fails AFTER apply wrote everything

    with pytest.raises(Boom):
        await apply_then_fail()

    assert _counts(db) == before
    assert (await _get(uow, repos)).status == "provisioning"


async def test_destroy_success_cancels_every_timer_and_stamps_destroyed_at(uow, repos, dispatcher):
    await _born(uow, repos, dispatcher)
    await dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=LATER, actor="api:alice"))
    assert (await _get(uow, repos)).status == "destroying"
    await dispatcher.apply("instance", "i1", InstanceDestroySucceeded(at=LATER + HOUR, actor="engine:run:r2"))

    stored = await _get(uow, repos)
    assert stored.status == "destroyed" and stored.destroyed_at == LATER + HOUR
    async with uow() as tx:
        assert repos.timers.list_all(tx) == []


async def test_destroyed_does_not_free_the_slug_so_a_zombie_can_never_collide_with_a_new_instance(
    uow, repos, dispatcher
):
    await _born(uow, repos, dispatcher, "i1", "box")
    await dispatcher.apply("instance", "i1", InstanceDestroyRequested(at=LATER, actor="api:alice"))
    await dispatcher.apply("instance", "i1", InstanceDestroySucceeded(at=LATER, actor="engine:run:r2"))
    with pytest.raises(IntegrityError):
        await _born(uow, repos, dispatcher, "i2", "box")
    async with uow() as tx:
        assert repos.instances.get_by_slug(tx, "box").id == "i1"
        assert repos.instances.get(tx, "i2") is None  # the failed birth left nothing behind
    # and the destroyed row can still be found running: the zombie transition has no slug to lose
    await dispatcher.apply("instance", "i1", InstanceRunningObserved(at=LATER, actor="reconciler"))
    assert (await _get(uow, repos, "i1")).status == "zombie"


# ---------------------------------------------------------------------------
# a cluster and an instance may share an id string: nothing crosses
# ---------------------------------------------------------------------------


async def test_a_cluster_and_an_instance_with_the_same_id_do_not_cross_talk(uow, repos, dispatcher):
    async with uow() as tx:
        repos.clusters.insert(tx, make_cluster_row("same", "box", status="active"))
    await _born(uow, repos, dispatcher, "same", "box")  # the same slug, too

    await dispatcher.apply("instance", "same", InstanceDestroyRequested(at=LATER, actor="api:alice"))

    async with uow() as tx:
        assert repos.clusters.get(tx, "same").status == "active" and repos.clusters.get(tx, "same").version == 0
        assert repos.instances.get(tx, "same").status == "destroying"
        assert repos.cluster_state_audits.list_for_cluster(tx, "same") == []
        assert len(repos.instance_state_audits.list_for_instance(tx, "same")) == 2
        rows = db_outbox(tx)
    assert {r.effect_id.split("/")[0] for r in rows} == {"instance"}  # none minted under "cluster/"
    assert all(r.aggregate_type == "instance" for r in rows)


async def test_timers_are_keyed_by_aggregate_type_so_a_shared_id_has_two_timers(uow, repos, dispatcher):
    from seedpod.core.events import ProvisionSucceeded

    async with uow() as tx:
        repos.clusters.insert(tx, make_cluster_row("same", "box", status="provisioning", expires_at=NOW + HOUR))
    await dispatcher.apply(
        "cluster", "same", ProvisionSucceeded(at=NOW, actor="engine:run:c", public_ip="203.0.113.1", kubeconfig_ref="k")
    )
    await _born(uow, repos, dispatcher, "same", "inst")

    async with uow() as tx:
        keys = {(t.aggregate_type, t.aggregate_id, t.timer_key) for t in repos.timers.list_all(tx)}
    assert keys == {("instance", "same", "expiry"), ("cluster", "same", "ttl")}
    # the instance's cancel-all never touches the cluster's timers
    await dispatcher.apply("instance", "same", InstanceDestroyRequested(at=LATER, actor="api:alice"))
    await dispatcher.apply("instance", "same", InstanceDestroySucceeded(at=LATER, actor="engine:run:r2"))
    async with uow() as tx:
        remaining = {(t.aggregate_type, t.timer_key) for t in repos.timers.list_all(tx)}
    assert ("instance", "expiry") not in remaining
    assert remaining == {("cluster", "ttl")}
