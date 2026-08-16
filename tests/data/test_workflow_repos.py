"""Pillar 2's data-layer subset: WorkflowRunRepository, WorkflowStepRepository,
OutboxRepository, against real tmp SQLite (0001_initial.sql). No mocks.

Covers: insert/update of run + step rows, the H14 one-active-run-per-cluster
partial unique index, effects_outbox row shapes (generic insert + the
drain-lane Notify helper), and "repositories never commit" (visible only after
the owning UnitOfWork exits -- verified via a SECOND, independent connection
to the same on-disk file, since a single Database's SQLite connection is a
StaticPool of one and can't demonstrate cross-connection isolation on its own).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from seedpod.core.clock import FrozenClock
from seedpod.data.database import Database
from seedpod.data.migrate import migrate
from seedpod.data.repositories import (
    ACTIVE_RUN_STATUSES,
    OutboxRepository,
    OutboxRow,
    WorkflowRunRepository,
    WorkflowRunRow,
    WorkflowStepRepository,
    WorkflowStepRow,
)
from seedpod.data.uow import UnitOfWork

NOW = datetime(2026, 7, 13, 12, 0, 0, tzinfo=UTC)
LATER = NOW + timedelta(minutes=5)

run_repo = WorkflowRunRepository()
step_repo = WorkflowStepRepository()
outbox_repo = OutboxRepository()


def _insert_cluster(session, cluster_id: str, *, now: datetime = NOW) -> None:
    """Minimal raw INSERT satisfying `clusters`' NOT NULL columns -- ClusterRepository
    is LATER work (out of this pillar's scope); workflow_runs.cluster_id is a real FK
    (foreign_keys=ON), so tests need a valid parent row."""
    session.execute(
        text(
            """
            INSERT INTO clusters (id, name, slug, environment, status, provider, created_at, updated_at)
            VALUES (:id, :name, :slug, 'ephemeral', 'active', 'fake', :now, :now)
            """
        ),
        {"id": cluster_id, "name": cluster_id, "slug": cluster_id, "now": now.isoformat()},
    )


@pytest.fixture
def db(tmp_path):
    database = Database(f"sqlite:///{tmp_path / 't.db'}")
    migrate(database.engine)
    return database


@pytest.fixture
def uow(db):
    return UnitOfWork(db)


def make_run(
    run_id: str,
    cluster_id: str,
    *,
    status: str = "pending",
    dedupe_key: str | None = None,
    **overrides,
) -> WorkflowRunRow:
    fields = {
        "id": run_id,
        "workflow": "provision-digitalocean",
        "workflow_version": 1,
        "cluster_id": cluster_id,
        "deployment_id": None,
        "dedupe_key": dedupe_key or f"dedupe:{run_id}",
        "args": {"cluster_id": cluster_id},
        "status": status,
        "cancel_requested": False,
        "failed_step": None,
        "error": None,
        "undo_incomplete": None,
        "initiated_by": "api:test",
        "created_at": NOW,
        "started_at": None,
        "finished_at": None,
    }
    fields.update(overrides)
    return WorkflowRunRow(**fields)


def make_step(run_id: str, step_path: str, **overrides) -> WorkflowStepRow:
    fields = {
        "run_id": run_id,
        "step_path": step_path,
        "verb": "cluster.load_spec",
        "status": "running",
        "attempt": 1,
        "interrupted_count": 0,
        "params": {"cluster_id": "c1"},
        "notes": {},
        "output": None,
        "undo_status": None,
        "error": None,
        "started_at": NOW,
        "finished_at": None,
    }
    fields.update(overrides)
    return WorkflowStepRow(**fields)


# ---------------------------------------------------------------------------
# workflow_runs
# ---------------------------------------------------------------------------


async def test_insert_and_get_run_round_trips(uow):
    async with uow() as tx:
        _insert_cluster(tx, "c1")
        run_repo.insert(tx, make_run("r1", "c1"))

    async with uow() as tx:
        fetched = run_repo.get(tx, "r1")
    assert fetched == make_run("r1", "c1")


async def test_get_missing_run_returns_none(uow):
    async with uow() as tx:
        assert run_repo.get(tx, "does-not-exist") is None


async def test_get_by_dedupe_key(uow):
    async with uow() as tx:
        _insert_cluster(tx, "c1")
        run_repo.insert(tx, make_run("r1", "c1", dedupe_key="run_workflow:effect-1"))

    async with uow() as tx:
        fetched = run_repo.get_by_dedupe_key(tx, "run_workflow:effect-1")
    assert fetched is not None
    assert fetched.id == "r1"


async def test_update_run_partial_fields_only(uow):
    async with uow() as tx:
        _insert_cluster(tx, "c1")
        run_repo.insert(tx, make_run("r1", "c1", status="pending"))

    async with uow() as tx:
        run_repo.update(tx, "r1", status="running", started_at=NOW)

    async with uow() as tx:
        fetched = run_repo.get(tx, "r1")
    assert fetched.status == "running"
    assert fetched.started_at == NOW
    assert fetched.failed_step is None  # untouched
    assert fetched.args == {"cluster_id": "c1"}  # untouched

    async with uow() as tx:
        run_repo.update(
            tx,
            "r1",
            status="failed",
            failed_step="create",
            error={"kind": "permanent", "step": "create", "message": "boom"},
            undo_incomplete=["create"],
            finished_at=LATER,
        )
    async with uow() as tx:
        fetched = run_repo.get(tx, "r1")
    assert fetched.status == "failed"
    assert fetched.failed_step == "create"
    assert fetched.error == {"kind": "permanent", "step": "create", "message": "boom"}
    assert fetched.undo_incomplete == ["create"]
    assert fetched.finished_at == LATER


async def test_update_with_no_fields_is_a_noop(uow):
    async with uow() as tx:
        _insert_cluster(tx, "c1")
        run_repo.insert(tx, make_run("r1", "c1"))
        run_repo.update(tx, "r1")  # nothing passed
        fetched = run_repo.get(tx, "r1")
    assert fetched == make_run("r1", "c1")


async def test_request_cancel_flips_flag_only_when_active(uow):
    async with uow() as tx:
        _insert_cluster(tx, "c1")
        run_repo.insert(tx, make_run("r1", "c1", status="running"))
        flipped = run_repo.request_cancel(tx, "r1")
    assert flipped is True

    async with uow() as tx:
        fetched = run_repo.get(tx, "r1")
    assert fetched.cancel_requested is True

    # a terminal run's cancel_requested is never touched
    async with uow() as tx:
        _insert_cluster(tx, "c2")
        run_repo.insert(tx, make_run("r2", "c2", status="succeeded", dedupe_key="d2"))
        flipped = run_repo.request_cancel(tx, "r2")
    assert flipped is False

    async with uow() as tx:
        fetched = run_repo.get(tx, "r2")
    assert fetched.cancel_requested is False


async def test_resumable_includes_blocked_excludes_terminal(uow):
    async with uow() as tx:
        _insert_cluster(tx, "c1")
        _insert_cluster(tx, "c2")
        _insert_cluster(tx, "c3")
        _insert_cluster(tx, "c4")
        run_repo.insert(tx, make_run("pending", "c1", status="pending", dedupe_key="d-pending"))
        run_repo.insert(tx, make_run("blocked", "c2", status="blocked", dedupe_key="d-blocked"))
        run_repo.insert(tx, make_run("succeeded", "c3", status="succeeded", dedupe_key="d-succeeded"))
        run_repo.insert(tx, make_run("cancelled", "c4", status="cancelled", dedupe_key="d-cancelled"))

    async with uow() as tx:
        resumable = {r.id for r in run_repo.resumable(tx)}
    assert resumable == {"pending", "blocked"}
    assert set(ACTIVE_RUN_STATUSES) == {"pending", "running", "blocked", "compensating"}


async def test_active_for_cluster(uow):
    async with uow() as tx:
        _insert_cluster(tx, "c1")
        run_repo.insert(tx, make_run("r1", "c1", status="running"))

    async with uow() as tx:
        active = run_repo.active_for_cluster(tx, "c1")
    assert active is not None
    assert active.id == "r1"

    async with uow() as tx:
        assert run_repo.active_for_cluster(tx, "no-such-cluster") is None


async def test_one_active_run_per_cluster_partial_unique_index_h14(uow):
    """ux_wr_one_active: a second row for the same cluster_id while one is still
    active (pending/running/blocked/compensating) violates the DDL's partial
    unique index -- this is enforced by the schema, not by application code."""
    async with uow() as tx:
        _insert_cluster(tx, "c1")
        run_repo.insert(tx, make_run("r1", "c1", status="running", dedupe_key="d1"))

    with pytest.raises(IntegrityError):
        async with uow() as tx:
            run_repo.insert(tx, make_run("r2", "c1", status="pending", dedupe_key="d2"))


async def test_second_active_run_allowed_once_first_is_terminal(uow):
    async with uow() as tx:
        _insert_cluster(tx, "c1")
        run_repo.insert(tx, make_run("r1", "c1", status="running", dedupe_key="d1"))

    async with uow() as tx:
        run_repo.update(tx, "r1", status="succeeded", finished_at=NOW)

    # now a new active run for the same cluster is allowed
    async with uow() as tx:
        run_repo.insert(tx, make_run("r2", "c1", status="pending", dedupe_key="d2"))

    async with uow() as tx:
        fetched = run_repo.get(tx, "r2")
    assert fetched is not None


async def test_terminal_runs_coexist_freely(uow):
    """The partial index only restricts ACTIVE statuses; multiple terminal runs
    for one cluster are unremarkable (run history)."""
    async with uow() as tx:
        _insert_cluster(tx, "c1")
        run_repo.insert(tx, make_run("r1", "c1", status="succeeded", dedupe_key="d1"))
        run_repo.insert(tx, make_run("r2", "c1", status="failed", dedupe_key="d2"))
        run_repo.insert(tx, make_run("r3", "c1", status="cancelled", dedupe_key="d3"))
    async with uow() as tx:
        assert {r.id for r in run_repo.list_by_status(tx, ["succeeded", "failed", "cancelled"])} == {
            "r1",
            "r2",
            "r3",
        }


# ---------------------------------------------------------------------------
# workflow_steps
# ---------------------------------------------------------------------------


async def test_insert_and_get_step_round_trips(uow):
    async with uow() as tx:
        _insert_cluster(tx, "c1")
        run_repo.insert(tx, make_run("r1", "c1"))
        step_repo.insert(tx, make_step("r1", "create"))

    async with uow() as tx:
        fetched = step_repo.get(tx, "r1", "create")
    assert fetched == make_step("r1", "create")


async def test_step_path_addresses_foreach_instances(uow):
    """step_path is the materialized instance address ('wave[1].apply') -- an
    integer cursor could not do this (Conflict 4's whole point)."""
    async with uow() as tx:
        _insert_cluster(tx, "c1")
        run_repo.insert(tx, make_run("r1", "c1"))
        step_repo.insert(tx, make_step("r1", "wave[0].apply", verb="kube.apply_docs"))
        step_repo.insert(tx, make_step("r1", "wave[1].apply", verb="kube.apply_docs"))

    async with uow() as tx:
        steps = {s.step_path for s in step_repo.list_for_run(tx, "r1")}
    assert steps == {"wave[0].apply", "wave[1].apply"}


async def test_duplicate_step_path_for_same_run_rejected(uow):
    """PRIMARY KEY (run_id, step_path)."""
    async with uow() as tx:
        _insert_cluster(tx, "c1")
        run_repo.insert(tx, make_run("r1", "c1"))
        step_repo.insert(tx, make_step("r1", "create"))

    with pytest.raises(IntegrityError):
        async with uow() as tx:
            step_repo.insert(tx, make_step("r1", "create"))


async def test_update_step_retry_and_completion_fields(uow):
    async with uow() as tx:
        _insert_cluster(tx, "c1")
        run_repo.insert(tx, make_run("r1", "c1"))
        step_repo.insert(tx, make_step("r1", "create"))

    async with uow() as tx:
        step_repo.update(tx, "r1", "create", attempt=2, interrupted_count=1, notes={"droplet_id": "d-1"})
    async with uow() as tx:
        fetched = step_repo.get(tx, "r1", "create")
    assert fetched.attempt == 2
    assert fetched.interrupted_count == 1
    assert fetched.notes == {"droplet_id": "d-1"}
    assert fetched.status == "running"  # untouched

    async with uow() as tx:
        step_repo.update(
            tx,
            "r1",
            "create",
            status="succeeded",
            output={"resource_ids": {"droplet_id": "d-1"}},
            finished_at=LATER,
        )
    async with uow() as tx:
        fetched = step_repo.get(tx, "r1", "create")
    assert fetched.status == "succeeded"
    assert fetched.output == {"resource_ids": {"droplet_id": "d-1"}}
    assert fetched.finished_at == LATER


async def test_update_step_undo_status(uow):
    async with uow() as tx:
        _insert_cluster(tx, "c1")
        run_repo.insert(tx, make_run("r1", "c1"))
        step_repo.insert(tx, make_step("r1", "create", status="failed"))

    async with uow() as tx:
        step_repo.update(tx, "r1", "create", undo_status="skipped")
    async with uow() as tx:
        fetched = step_repo.get(tx, "r1", "create")
    assert fetched.undo_status == "skipped"


# ---------------------------------------------------------------------------
# effects_outbox
# ---------------------------------------------------------------------------


def make_outbox_row(effect_id: str, **overrides) -> OutboxRow:
    fields = {
        "seq": None,
        "effect_id": effect_id,
        "aggregate_type": "cluster",
        "aggregate_id": "c1",
        "to_version": 1,
        "ordinal": 0,
        "kind": "persist",
        "payload": '{"kind": "Persist"}',
        "lane": "tx",
        "status": "done",
        "attempts": 0,
        "available_at": NOW,
        "created_at": NOW,
        "done_at": NOW,
        "last_error": None,
    }
    fields.update(overrides)
    return OutboxRow(**fields)


async def test_insert_and_get_outbox_row(uow):
    async with uow() as tx:
        outbox_repo.insert(tx, make_outbox_row("cluster/c1@1#0"))

    async with uow() as tx:
        fetched = outbox_repo.get(tx, "cluster/c1@1#0")
    assert fetched is not None
    assert fetched.seq is not None  # AUTOINCREMENT filled it in
    assert fetched.aggregate_type == "cluster"
    assert fetched.kind == "persist"
    assert fetched.lane == "tx"


async def test_outbox_effect_id_is_unique(uow):
    async with uow() as tx:
        outbox_repo.insert(tx, make_outbox_row("cluster/c1@1#0"))

    with pytest.raises(IntegrityError):
        async with uow() as tx:
            outbox_repo.insert(tx, make_outbox_row("cluster/c1@1#0", ordinal=1))


@pytest.mark.parametrize(
    "field,bad_value",
    [
        ("aggregate_type", "not-a-real-aggregate"),
        ("kind", "not-a-real-kind"),
        ("lane", "sideways"),
        ("status", "in-limbo"),
    ],
)
async def test_outbox_check_constraints_enforced(uow, field, bad_value):
    row = make_outbox_row("cluster/c1@1#0", **{field: bad_value})
    with pytest.raises(IntegrityError):
        async with uow() as tx:
            outbox_repo.insert(tx, row)


async def test_outbox_accepts_run_aggregate_type(uow):
    """Conflict 1 amendment: aggregate_type CHECK gains 'run' for engine-origin rows."""
    async with uow() as tx:
        outbox_repo.insert(
            tx,
            make_outbox_row(
                "run/r1@create#0",
                aggregate_type="run",
                aggregate_id="r1",
                to_version=0,
                kind="notify",
                lane="drain",
                status="pending",
            ),
        )
    async with uow() as tx:
        fetched = outbox_repo.get(tx, "run/r1@create#0")
    assert fetched.aggregate_type == "run"
    assert fetched.to_version == 0


async def test_list_for_aggregate_orders_by_seq(uow):
    async with uow() as tx:
        outbox_repo.insert(tx, make_outbox_row("cluster/c1@1#0", ordinal=0))
        outbox_repo.insert(tx, make_outbox_row("cluster/c1@1#1", ordinal=1))
        outbox_repo.insert(tx, make_outbox_row("cluster/c2@1#0", aggregate_id="c2", ordinal=0))

    async with uow() as tx:
        rows = outbox_repo.list_for_aggregate(tx, "cluster", "c1")
    assert [r.effect_id for r in rows] == ["cluster/c1@1#0", "cluster/c1@1#1"]


async def test_insert_run_notify_shape(uow):
    clock = FrozenClock(NOW)
    async with uow() as tx:
        outbox_repo.insert_run_notify(
            tx,
            run_id="r1",
            step_path="wave[0].apply",
            ordinal=3,
            topic="job_started",
            payload={"run_id": "r1", "step": "wave[0].apply"},
            clock=clock,
        )

    async with uow() as tx:
        fetched = outbox_repo.get(tx, "run/r1@wave[0].apply#3")
    assert fetched is not None
    assert fetched.aggregate_type == "run"
    assert fetched.aggregate_id == "r1"
    assert fetched.to_version == 0
    assert fetched.kind == "notify"
    assert fetched.lane == "drain"
    assert fetched.status == "pending"
    assert fetched.available_at == NOW
    assert fetched.created_at == NOW

    import json

    decoded = json.loads(fetched.payload)
    assert decoded["kind"] == "notify"  # Effect leaves tag with EffectKind.value, not the class name
    assert decoded["topic"] == "job_started"
    assert decoded["payload"] == {"run_id": "r1", "step": "wave[0].apply"}


async def test_insert_run_notify_ordinal_collision_rejected(uow):
    """effect_id uniqueness makes re-delivery of the same (run, step, ordinal) a no-op
    failure at the DB layer -- callers are expected to pick a fresh ordinal per note."""
    clock = FrozenClock(NOW)
    async with uow() as tx:
        outbox_repo.insert_run_notify(
            tx, run_id="r1", step_path="create", ordinal=0, topic="job_started",
            payload={}, clock=clock,
        )
    with pytest.raises(IntegrityError):
        async with uow() as tx:
            outbox_repo.insert_run_notify(
                tx, run_id="r1", step_path="create", ordinal=0, topic="job_completed",
                payload={}, clock=clock,
            )


# ---------------------------------------------------------------------------
# repositories never commit -- visible only after the owning UnitOfWork exits
# ---------------------------------------------------------------------------


async def test_repos_never_commit_visible_only_after_uow_exit(tmp_path):
    """Verified across a SECOND, independent Database/connection to the SAME file:
    a single Database's SQLite connection is a StaticPool of one, so peeking from
    inside the same UnitOfWork block would just be a dirty read on one connection,
    not a real test of transaction isolation."""
    db_path = tmp_path / "shared.db"
    writer_db = Database(f"sqlite:///{db_path}")
    migrate(writer_db.engine)
    writer_uow = UnitOfWork(writer_db)

    reader_db = Database(f"sqlite:///{db_path}")
    reader_uow = UnitOfWork(reader_db)

    async with writer_uow() as tx:
        _insert_cluster(tx, "c1")
        run_repo.insert(tx, make_run("r1", "c1"))

        # still inside the writer's transaction: NOT committed, NOT visible elsewhere
        async with reader_uow() as reader_tx:
            assert run_repo.get(reader_tx, "r1") is None

    # writer's `async with` block has exited -> UnitOfWork committed once, on exit
    async with reader_uow() as reader_tx:
        fetched = run_repo.get(reader_tx, "r1")
    assert fetched is not None
    assert fetched.id == "r1"

    reader_db.dispose()
    writer_db.dispose()


async def test_repos_never_commit_rollback_on_exception(uow):
    class Boom(Exception):
        pass

    with pytest.raises(Boom):
        async with uow() as tx:
            _insert_cluster(tx, "c1")
            run_repo.insert(tx, make_run("r1", "c1"))
            raise Boom("simulated failure mid-transaction")

    async with uow() as tx:
        assert run_repo.get(tx, "r1") is None
