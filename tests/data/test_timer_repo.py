"""TimerRepository -- the `timers` table (docs/design/coherence-review.md Conflict 1:
Seam A's dedicated timers table, ratified verbatim). Real tmp SQLite, no mocks.

Covers: upsert insert + re-arm-in-place on the PK (aggregate_type, aggregate_id,
timer_key), key-scoped delete vs all-keys delete (CancelTimer(timer_key=None)), due()
ordering, next_fire_at(), and consume() -- DR-0009's conditional-consume delete
(docs/decisions/DR-0009-conditional-timer-consume.md, RATIFIED): matching/mismatched/
same/missing fire_at snapshots. (tests/runtime/test_timer_service.py separately pins
the end-to-end scan-to-fire race through the real TimerService.)
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from seedpod.core.codec import decode_event
from seedpod.core.effects import CancelTimer, ScheduleTimer
from seedpod.core.events import DestroyDue, TtlExpired
from seedpod.data.database import Database
from seedpod.data.migrate import migrate
from seedpod.data.repositories import ClusterRepository, TimerRepository
from seedpod.data.uow import UnitOfWork

from .test_machine_repos import make_cluster_row

NOW = datetime(2026, 7, 15, 12, 0, 0, tzinfo=UTC)
LATER = NOW + timedelta(minutes=5)
EVEN_LATER = NOW + timedelta(minutes=10)

timers = TimerRepository()
clusters = ClusterRepository()


def _iso_text(dt: datetime) -> str:
    """Same fixed-millisecond 'Z' convention every repository write uses (``_iso`` in
    seedpod/data/repositories.py) -- inlined here (mirrors test_feature_repos.py's
    ``_insert_cluster``) so ``consume()`` calls below pass the exact snapshot TEXT a
    real scan pass would see (DR-0009 §2: exact TEXT equality, no private import)."""
    return dt.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


@pytest.fixture
def db(tmp_path):
    database = Database(f"sqlite:///{tmp_path / 't.db'}")
    migrate(database.engine)
    return database


@pytest.fixture
def uow(db):
    return UnitOfWork(db)


def ttl_timer(cluster_id: str, fire_at: datetime) -> ScheduleTimer:
    return ScheduleTimer(
        aggregate_type="cluster",
        aggregate_id=cluster_id,
        timer_key="ttl",
        fire_at=fire_at,
        event=TtlExpired(at=fire_at, actor="timer:ttl"),
    )


def destroy_timer(cluster_id: str, fire_at: datetime) -> ScheduleTimer:
    return ScheduleTimer(
        aggregate_type="cluster",
        aggregate_id=cluster_id,
        timer_key="destroy",
        fire_at=fire_at,
        event=DestroyDue(at=fire_at, actor="timer:destroy"),
    )


async def test_upsert_and_get_round_trips(uow):
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))
        timers.upsert(tx, ttl_timer("c1", LATER), "cluster/c1@1#0")

    async with uow() as tx:
        fetched = timers.get(tx, "cluster", "c1", "ttl")
    assert fetched is not None
    assert fetched.fire_at == LATER
    assert fetched.created_by_effect == "cluster/c1@1#0"
    decoded = decode_event(json.loads(fetched.event))
    assert isinstance(decoded, TtlExpired)
    assert decoded.at == LATER


async def test_get_missing_timer_returns_none(uow):
    async with uow() as tx:
        assert timers.get(tx, "cluster", "no-such", "ttl") is None


async def test_upsert_rearms_in_place_on_same_pk(uow):
    """Re-arming (e.g. TTL extension) is an idempotent upsert on the PK -- it does
    NOT create a second row."""
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))
        timers.upsert(tx, ttl_timer("c1", LATER), "cluster/c1@1#0")

    async with uow() as tx:
        timers.upsert(tx, ttl_timer("c1", EVEN_LATER), "cluster/c1@2#0")

    async with uow() as tx:
        fetched = timers.get(tx, "cluster", "c1", "ttl")
        due_now = timers.due(tx, EVEN_LATER)
    assert fetched.fire_at == EVEN_LATER
    assert fetched.created_by_effect == "cluster/c1@2#0"
    assert len(due_now) == 1  # still just one row, not two


async def test_two_different_timer_keys_coexist_for_same_aggregate(uow):
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))
        timers.upsert(tx, ttl_timer("c1", LATER), "eff-1")
        timers.upsert(tx, destroy_timer("c1", EVEN_LATER), "eff-2")

    async with uow() as tx:
        assert timers.get(tx, "cluster", "c1", "ttl") is not None
        assert timers.get(tx, "cluster", "c1", "destroy") is not None


async def test_delete_specific_key_leaves_others(uow):
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))
        timers.upsert(tx, ttl_timer("c1", LATER), "eff-1")
        timers.upsert(tx, destroy_timer("c1", EVEN_LATER), "eff-2")

    async with uow() as tx:
        timers.delete(tx, CancelTimer(aggregate_type="cluster", aggregate_id="c1", timer_key="ttl"))

    async with uow() as tx:
        assert timers.get(tx, "cluster", "c1", "ttl") is None
        assert timers.get(tx, "cluster", "c1", "destroy") is not None


async def test_delete_all_keys_when_timer_key_is_none(uow):
    """CancelTimer(timer_key=None) = delete ALL timers for the aggregate (Seam A)."""
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))
        timers.upsert(tx, ttl_timer("c1", LATER), "eff-1")
        timers.upsert(tx, destroy_timer("c1", EVEN_LATER), "eff-2")

    async with uow() as tx:
        timers.delete(tx, CancelTimer(aggregate_type="cluster", aggregate_id="c1", timer_key=None))

    async with uow() as tx:
        assert timers.get(tx, "cluster", "c1", "ttl") is None
        assert timers.get(tx, "cluster", "c1", "destroy") is None


async def test_delete_all_keys_does_not_touch_other_aggregates(uow):
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo1"))
        clusters.insert(tx, make_cluster_row("c2", "demo2"))
        timers.upsert(tx, ttl_timer("c1", LATER), "eff-1")
        timers.upsert(tx, ttl_timer("c2", LATER), "eff-2")

    async with uow() as tx:
        timers.delete(tx, CancelTimer(aggregate_type="cluster", aggregate_id="c1", timer_key=None))

    async with uow() as tx:
        assert timers.get(tx, "cluster", "c1", "ttl") is None
        assert timers.get(tx, "cluster", "c2", "ttl") is not None


async def test_delete_nonexistent_timer_is_a_noop(uow):
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))
        timers.delete(tx, CancelTimer(aggregate_type="cluster", aggregate_id="c1", timer_key="ttl"))
    # no error


# ---------------------------------------------------------------------------
# consume() -- DR-0009's conditional consume (RATIFIED). The (later-work)
# TimerService's fire transaction threads its scan-pass fire_at snapshot through
# here; a rowcount-1 delete means "unchanged since the scan, apply()"; rowcount-0
# means a concurrent re-arm/cancel won the scan-to-fire window.
# ---------------------------------------------------------------------------


async def test_consume_matching_fire_at_deletes_and_returns_true(uow):
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))
        timers.upsert(tx, destroy_timer("c1", NOW), "eff-1")

    async with uow() as tx:
        consumed = timers.consume(tx, "cluster", "c1", "destroy", _iso_text(NOW))
        assert consumed is True
        assert timers.get(tx, "cluster", "c1", "destroy") is None


async def test_consume_mismatched_fire_at_leaves_row_and_returns_false(uow):
    """The row was re-armed (a new fire_at) between the caller's scan and this
    consume attempt -- the stale snapshot must not delete the re-armed row."""
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))
        timers.upsert(tx, destroy_timer("c1", NOW), "eff-1")

    async with uow() as tx:
        timers.upsert(tx, destroy_timer("c1", LATER), "eff-2")  # re-arm to a NEW deadline

    async with uow() as tx:
        consumed = timers.consume(tx, "cluster", "c1", "destroy", _iso_text(NOW))  # stale snapshot
        assert consumed is False
        survivor = timers.get(tx, "cluster", "c1", "destroy")
        assert survivor is not None
        assert survivor.fire_at == LATER  # untouched by the stale consume attempt


async def test_consume_same_fire_at_rearm_still_matches_and_returns_true(uow):
    """Re-arming to the IDENTICAL fire_at (harmless per DR-0009 §2) still matches
    the snapshot's TEXT-equal comparison -- consume proceeds."""
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))
        timers.upsert(tx, destroy_timer("c1", NOW), "eff-1")

    async with uow() as tx:
        timers.upsert(tx, destroy_timer("c1", NOW), "eff-2")  # re-arm to the SAME fire_at

    async with uow() as tx:
        consumed = timers.consume(tx, "cluster", "c1", "destroy", _iso_text(NOW))
        assert consumed is True
        assert timers.get(tx, "cluster", "c1", "destroy") is None


async def test_consume_after_cancel_returns_false(uow):
    """The row was cancelled between the caller's scan and this consume attempt --
    there is nothing left to delete, and nothing must be (re-)applied."""
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))
        timers.upsert(tx, destroy_timer("c1", NOW), "eff-1")

    async with uow() as tx:
        timers.delete(tx, CancelTimer(aggregate_type="cluster", aggregate_id="c1", timer_key="destroy"))

    async with uow() as tx:
        consumed = timers.consume(tx, "cluster", "c1", "destroy", _iso_text(NOW))
        assert consumed is False


async def test_consume_nonexistent_timer_returns_false(uow):
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo"))
    async with uow() as tx:
        assert timers.consume(tx, "cluster", "c1", "destroy", _iso_text(NOW)) is False


async def test_due_orders_by_fire_at_and_excludes_future(uow):
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo1"))
        clusters.insert(tx, make_cluster_row("c2", "demo2"))
        timers.upsert(tx, ttl_timer("c1", EVEN_LATER), "eff-1")
        timers.upsert(tx, ttl_timer("c2", LATER), "eff-2")

    async with uow() as tx:
        due = timers.due(tx, LATER)
    assert [t.aggregate_id for t in due] == ["c2"]  # only c2's timer has fired by LATER

    async with uow() as tx:
        due = timers.due(tx, EVEN_LATER)
    assert [t.aggregate_id for t in due] == ["c2", "c1"]  # both, ordered by fire_at


async def test_next_fire_at_returns_earliest(uow):
    async with uow() as tx:
        clusters.insert(tx, make_cluster_row("c1", "demo1"))
        clusters.insert(tx, make_cluster_row("c2", "demo2"))
        timers.upsert(tx, ttl_timer("c1", EVEN_LATER), "eff-1")
        timers.upsert(tx, ttl_timer("c2", LATER), "eff-2")

    async with uow() as tx:
        earliest = timers.next_fire_at(tx)
    assert earliest == LATER


async def test_next_fire_at_none_when_no_timers(uow):
    async with uow() as tx:
        assert timers.next_fire_at(tx) is None
