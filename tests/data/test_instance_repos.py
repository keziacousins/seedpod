"""InstanceRepository and InstanceStateAuditRepository (DR-0055; 0005 + 0006) against real tmp
SQLite. No mocks. The Dispatcher-level behaviour is in tests/runtime/test_dispatcher_instance.py;
this covers the repositories' own contract: row round-trip, CAS persist, the slug predicate, and
the audit shape.
"""

from __future__ import annotations

import dataclasses
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from seedpod.core.clock import FrozenClock
from seedpod.core.events import InstanceDestroyFailed, InstanceExpired
from seedpod.core.machine import StaleVersion
from seedpod.core.records import InstanceRecord, InstanceStatus
from seedpod.data.database import Database
from seedpod.data.migrate import migrate
from seedpod.data.repositories import InstanceRepository, InstanceRow, InstanceStateAuditRepository
from seedpod.data.uow import UnitOfWork

NOW = datetime(2026, 8, 1, 12, 0, 0, tzinfo=UTC)
HOUR = timedelta(hours=1)
repo = InstanceRepository()
audit_repo = InstanceStateAuditRepository()


@pytest.fixture
def uow(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 'instances.db'}")
    migrate(db.engine)
    return UnitOfWork(db)


def _row(instance_id="i1", slug="box", owner=1, **over) -> InstanceRow:
    fields = {
        "id": instance_id, "slug": slug, "provider": "tart", "environment": "ephemeral", "template": "ubuntu",
        "size": "small", "status": "new", "version": 0, "owner_key_id": owner, "resource_ids": {},
        "address": None, "known_hosts": "host-key", "lease_seconds": 3600, "expires_at": NOW + HOUR,
        "hard_expires_at": NOW + 24 * HOUR, "hourly_price": 0.25, "vcpu": 2, "memory_mb": 4096,
        "failure_reason": None, "created_at": NOW, "updated_at": NOW, "destroyed_at": None,
    }
    fields.update(over)
    return InstanceRow(**fields)


def _owner(tx) -> int:
    tx.execute(text("INSERT INTO api_keys (key_hash, username, created_at) VALUES ('h', 'u', :t)"), {"t": NOW.isoformat()})
    return tx.execute(text("SELECT MAX(id) FROM api_keys")).scalar()


async def test_a_row_round_trips_every_column(uow):
    async with uow() as tx:
        row = _row(owner=_owner(tx), resource_ids={"vm": "spi-box"}, address="192.0.2.7")
        repo.insert(tx, row)
        assert repo.get(tx, "i1") == row
        assert repo.get(tx, "ghost") is None


async def test_load_narrows_the_row_to_the_pure_record(uow):
    async with uow() as tx:
        repo.insert(tx, _row(owner=_owner(tx), status="active", version=4, resource_ids={"vm": "v"}, address="a"))
        record = repo.load(tx, "i1")
        assert repo.load(tx, "ghost") is None
    assert record == InstanceRecord(
        id="i1", slug="box", state=InstanceStatus.ACTIVE, version=4, provider="tart", environment="ephemeral",
        lease_seconds=3600, expires_at=NOW + HOUR, hard_expires_at=NOW + 24 * HOUR, address="a",
        resource_ids={"vm": "v"}, failure_reason=None, destroyed_at=None,
    )


async def test_persist_writes_only_what_the_machine_owns_and_bumps_the_version(uow):
    clock = FrozenClock(NOW + timedelta(minutes=7))
    async with uow() as tx:
        before = _row(owner=_owner(tx), status="provisioning", version=1)
        repo.insert(tx, before)
        record = dataclasses.replace(
            repo.load(tx, "i1"), state=InstanceStatus.ACTIVE, version=2, address="192.0.2.9",
            resource_ids={"vm": "x"}, expires_at=NOW + 2 * HOUR, failure_reason="r",
        )
        repo.persist(tx, record, 1, clock=clock)
        after = repo.get(tx, "i1")
    assert (after.status, after.version, after.address) == ("active", 2, "192.0.2.9")
    assert (after.expires_at, after.failure_reason, dict(after.resource_ids)) == (NOW + 2 * HOUR, "r", {"vm": "x"})
    assert after.updated_at == clock.now()
    # everything it does not own is exactly as inserted
    for name in ("slug", "provider", "environment", "template", "size", "owner_key_id", "known_hosts",
                 "lease_seconds", "hard_expires_at", "hourly_price", "vcpu", "memory_mb", "created_at"):
        assert getattr(after, name) == getattr(before, name), name


async def test_persist_cannot_move_the_hard_limit_even_if_the_record_says_so(uow):
    async with uow() as tx:
        repo.insert(tx, _row(owner=_owner(tx), status="active", version=1))
        forged = dataclasses.replace(repo.load(tx, "i1"), hard_expires_at=NOW + 999 * HOUR, version=2)
        repo.persist(tx, forged, 1, clock=FrozenClock(NOW))
        assert repo.get(tx, "i1").hard_expires_at == NOW + 24 * HOUR  # not in the SET list


async def test_persist_with_a_stale_version_raises_and_changes_nothing(uow):
    async with uow() as tx:
        repo.insert(tx, _row(owner=_owner(tx), status="active", version=3))
        record = dataclasses.replace(repo.load(tx, "i1"), state=InstanceStatus.FAILED, version=4)
        with pytest.raises(StaleVersion):
            repo.persist(tx, record, 2, clock=FrozenClock(NOW))
        assert repo.get(tx, "i1").status == "active"


async def test_the_status_check_rejects_a_state_the_machine_does_not_have(uow):
    with pytest.raises(IntegrityError):
        async with uow() as tx:
            repo.insert(tx, _row(owner=_owner(tx), status="paused"))


async def test_slug_is_unique_across_every_status_and_never_reused_after_destroyed(uow):
    async with uow() as tx:
        owner = _owner(tx)
        repo.insert(tx, _row("i1", "box", owner, status="destroy-failed"))
        assert repo.get_by_slug(tx, "box").id == "i1"
    with pytest.raises(IntegrityError):
        async with uow() as tx:
            repo.insert(tx, _row("i2", "box", 1))
    async with uow() as tx:
        tx.execute(text("UPDATE instances SET status = 'destroyed' WHERE id = 'i1'"))
    # destroyed does not free it: a zombie must be able to come back to a live state
    for status in ("new", "destroyed"):
        with pytest.raises(IntegrityError):
            async with uow() as tx:
                repo.insert(tx, _row("i2", "box", 1, status=status))
    async with uow() as tx:
        assert repo.get_by_slug(tx, "box").id == "i1"
        assert repo.get_by_slug(tx, "other") is None
        repo.insert(tx, _row("i2", "other", 1))
        assert [r.id for r in repo.list_all(tx)] == ["i1", "i2"]


async def test_audit_rows_carry_the_events_actor_time_reason_and_full_context(uow):
    at = NOW + timedelta(minutes=3)
    async with uow() as tx:
        repo.insert(tx, _row(owner=_owner(tx)))
        audit_repo.add(
            tx, instance_id="i1", from_state="destroying", to_state="destroy-failed",
            event=InstanceDestroyFailed(at=at, actor="engine:run:r1", reason="api 500"), actor="engine:run:r1", at=at,
        )
        audit_repo.add(
            tx, instance_id="i1", from_state="destroy-failed", to_state="destroying",
            event=InstanceExpired(at=at + HOUR, actor="timer:expiry"), actor="timer:expiry", at=at + HOUR,
        )
        rows = audit_repo.list_for_instance(tx, "i1")
        assert audit_repo.list_for_instance(tx, "ghost") == []
    newest, oldest = rows
    assert (oldest.event, oldest.actor, oldest.reason, oldest.created_at) == (
        "InstanceDestroyFailed", "engine:run:r1", "api 500", at
    )
    assert oldest.context["kind"] == "InstanceDestroyFailed" and oldest.context["reason"] == "api 500"
    assert (newest.event, newest.reason, newest.created_at) == ("InstanceExpired", None, at + HOUR)


async def test_list_live_ids_skips_destroyed_and_decodes_nothing(uow):
    """The liveness sweep enumerates with this, so that one row that cannot be decoded cannot hide the others."""
    async with uow() as tx:
        owner = _owner(tx)
        repo.insert(tx, _row("old", "a", owner, created_at=NOW))
        repo.insert(tx, _row("gone", "b", owner, status="destroyed", created_at=NOW + HOUR, destroyed_at=NOW + HOUR))
        repo.insert(tx, _row("poison", "c", owner, created_at=NOW + 2 * HOUR))
        repo.insert(tx, _row("new", "d", owner, created_at=NOW + 3 * HOUR))
        tx.execute(text("UPDATE instances SET resource_ids = '{not json' WHERE id = 'poison'"))
        with pytest.raises(ValueError):
            repo.list_all(tx)  # the full read dies on the poison row
        assert repo.list_live_ids(tx) == ["old", "poison", "new"]
