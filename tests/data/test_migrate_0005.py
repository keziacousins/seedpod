"""0005_instances.sql (DR-0055 decisions 1 and 4) against a POPULATED v4 database, and
the runner's recovery when a migration fails.

DR-0055 names this the riskiest change: SQLite needs a table rebuild to relax
``workflow_runs.cluster_id`` NOT NULL, on a table every workflow uses, and a plain
rebuild fails on the ``workflow_steps`` foreign key. Real tmp SQLite, no mocks.
"""

from __future__ import annotations

import shutil
import sqlite3
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from seedpod.data.database import Database
from seedpod.data.migrate import MIGRATIONS_DIR, migrate

T = "2026-08-01T00:00:00Z"
ACTIVE = ("pending", "running", "blocked", "compensating")

# Every column 0001 gave workflow_runs; the migration must carry all of them over.
RUN_COLUMNS = (
    "id, workflow, workflow_version, cluster_id, deployment_id, dedupe_key, args, status, "
    "cancel_requested, failed_step, error, undo_incomplete, initiated_by, created_at, "
    "started_at, finished_at"
)


# 0005 as it left the schema. Later migrations (0006 adds a trigger and a table) must not change what
# these tests say about 0005, so they migrate through this directory and stop at 0005.
_THROUGH_0005 = tempfile.TemporaryDirectory(prefix="seedpod-migrations-through-0005-")
for _f in sorted(MIGRATIONS_DIR.glob("000[1-5]_*.sql")):
    shutil.copy(_f, Path(_THROUGH_0005.name) / _f.name)


def migrate_0005(engine) -> None:
    migrate(engine, migrations_dir=Path(_THROUGH_0005.name))


def _v4_dir(tmp_path: Path, name: str = "migrations-v4") -> Path:
    d = tmp_path / name
    d.mkdir()
    for f in sorted(MIGRATIONS_DIR.glob("000[1-4]_*.sql")):
        shutil.copy(f, d / f.name)
    return d


def _run(conn, run_id, cluster_id, status="succeeded", *, deployment_id=None, dedupe_key=None, **extra):
    conn.execute(
        text(
            "INSERT INTO workflow_runs (id, workflow, workflow_version, cluster_id, deployment_id,"
            " dedupe_key, args, status, cancel_requested, failed_step, error, undo_incomplete,"
            " initiated_by, created_at, started_at, finished_at)"
            " VALUES (:id, 'provision-tart', 3, :cluster_id, :deployment_id, :dedupe_key,"
            " :args, :status, :cancel_requested, :failed_step, :error, :undo_incomplete,"
            " :initiated_by, :t, :t, NULL)"
        ),
        {
            "id": run_id, "cluster_id": cluster_id, "deployment_id": deployment_id,
            "dedupe_key": dedupe_key, "status": status, "t": T,
            "args": extra.get("args", '{"k": "v"}'),
            "cancel_requested": extra.get("cancel_requested", 0),
            "failed_step": extra.get("failed_step"),
            "error": extra.get("error"),
            "undo_incomplete": extra.get("undo_incomplete"),
            "initiated_by": extra.get("initiated_by", "api:kezia"),
        },
    )


def _step(conn, run_id, path):
    conn.execute(
        text(
            "INSERT INTO workflow_steps (run_id, step_path, verb, status, params, started_at)"
            " VALUES (:r, :p, 'infra.create_instance', 'succeeded', '{}', :t)"
        ),
        {"r": run_id, "p": path, "t": T},
    )


@pytest.fixture
def v4(tmp_path):
    """A database at user_version 4 holding runs in every shape the rebuild must keep."""
    db = Database(f"sqlite:///{tmp_path / 't.db'}")
    migrate(db.engine, migrations_dir=_v4_dir(tmp_path))
    with db.engine.begin() as conn:
        assert conn.exec_driver_sql("PRAGMA user_version").scalar() == 4
        conn.execute(
            text(
                "INSERT INTO api_keys (key_hash, username, created_at, is_active)"
                " VALUES ('h1', 'kezia', :t, 1), ('h2', 'revoked-bot', :t, 0)"
            ),
            {"t": T},
        )
        for cid in ("c1", "c2"):
            conn.execute(
                text(
                    "INSERT INTO clusters (id, name, slug, environment, status, provider,"
                    " created_at, updated_at) VALUES (:id, :id, :id, 'ephemeral', 'active',"
                    " 'tart', :t, :t)"
                ),
                {"id": cid, "t": T},
            )
        conn.execute(
            text(
                "INSERT INTO deployments (id, cluster_id, environment, status, manifest_version,"
                " created_at, updated_at) VALUES ('d1', 'c1', 'ephemeral', 'active', 'm',"
                " :t, :t)"
            ),
            {"t": T},
        )
        # c1: three finished runs, one live run bound to a deployment. c2: one pending run.
        _run(conn, "r1", "c1", "succeeded", dedupe_key="run/a")
        _run(conn, "r2", "c1", "failed", failed_step="create", error='{"kind":"permanent"}',
             undo_incomplete='["create"]')
        _run(conn, "r3", "c1", "cancelled", cancel_requested=1)
        _run(conn, "r4", "c1", "running", deployment_id="d1", dedupe_key="run/b", initiated_by=None)
        _run(conn, "r5", "c2", "pending")
        for run_id, path in (("r1", "create"), ("r1", "wave[1].apply"), ("r2", "create"),
                             ("r4", "create"), ("r5", "create")):
            _step(conn, run_id, path)
        conn.execute(
            text(
                "INSERT INTO timers (aggregate_type, aggregate_id, timer_key, fire_at, event,"
                " created_by_effect) VALUES ('cluster', 'c1', 'ttl', :t, '{}', 'x')"
            ),
            {"t": T},
        )
        _seed_outbox(conn)
    return db


def _seed_outbox(conn, *, delete_top: bool = True):
    """Outbox rows in every status, then (by default) the highest-seq row deleted, the way
    prune_done leaves things: AUTOINCREMENT's high-water mark (6) is above MAX(seq) (5)."""
    rows = [
        ("cluster/c1@1#0", "cluster", "c1", 1, 0, "run_workflow", "drain", "done", 0, None),
        ("cluster/c1@1#1", "cluster", "c1", 1, 1, "notify", "drain", "pending", 0, None),
        ("deployment/d1@2#0", "deployment", "d1", 2, 0, "cancel_workflow", "drain", "pending", 3, "boom"),
        ("run/r4@_run#0", "run", "r4", 0, 0, "notify", "drain", "dead", 8, "gave up"),
        ("cluster/c2@1#0", "cluster", "c2", 1, 0, "persist", "tx", "done", 0, None),
        ("cluster/c2@2#0", "cluster", "c2", 2, 0, "notify", "drain", "done", 0, None),
    ]
    for eid, agg, aid, ver, ordinal, kind, lane, status, attempts, err in rows:
        conn.execute(
            text(
                "INSERT INTO effects_outbox (effect_id, aggregate_type, aggregate_id, to_version, ordinal,"
                " kind, payload, lane, status, attempts, available_at, created_at, done_at, last_error)"
                " VALUES (:e, :agg, :aid, :ver, :o, :k, '{\"kind\": \"x\"}', :lane, :st, :att,"
                " '2026-08-01T00:00:05Z', :t, NULL, :err)"
            ),
            {"e": eid, "agg": agg, "aid": aid, "ver": ver, "o": ordinal, "k": kind, "lane": lane,
             "st": status, "att": attempts, "err": err, "t": T},
        )
    if delete_top:
        conn.execute(text("DELETE FROM effects_outbox WHERE seq = 6"))


OUTBOX_COLUMNS = (
    "seq, effect_id, aggregate_type, aggregate_id, to_version, ordinal, kind, payload, lane, "
    "status, attempts, available_at, created_at, done_at, last_error"
)


def _outbox(db):
    with db.engine.begin() as conn:
        return conn.exec_driver_sql(f"SELECT {OUTBOX_COLUMNS} FROM effects_outbox ORDER BY seq").fetchall()


def _outbox_sequence(db):
    with db.engine.begin() as conn:
        return conn.exec_driver_sql("SELECT name, seq FROM sqlite_sequence WHERE name LIKE 'effects_outbox%'").fetchall()


def _insert_outbox(conn, effect_id, aggregate_type="cluster", **over):
    vals = {"kind": "notify", "lane": "drain", "status": "pending", **over}
    conn.execute(
        text(
            "INSERT INTO effects_outbox (effect_id, aggregate_type, aggregate_id, to_version, ordinal, kind,"
            " payload, lane, status, available_at, created_at) VALUES (:e, :agg, 'x', 0, 0, :kind, '{}',"
            " :lane, :status, :t, :t)"
        ),
        {"e": effect_id, "agg": aggregate_type, "t": T, **vals},
    )
    return conn.execute(text("SELECT seq FROM effects_outbox WHERE effect_id = :e"), {"e": effect_id}).scalar()


def _snapshot(db):
    with db.engine.begin() as conn:
        runs = conn.exec_driver_sql(f"SELECT {RUN_COLUMNS} FROM workflow_runs ORDER BY id").fetchall()
        steps = conn.exec_driver_sql("SELECT * FROM workflow_steps ORDER BY run_id, step_path").fetchall()
    return runs, steps


def _insert_instance(conn, instance_id, slug, *, status="active", owner=1):
    conn.execute(
        text(
            "INSERT INTO instances (id, slug, provider, environment, template, size, status,"
            " owner_key_id, lease_seconds, expires_at, hard_expires_at, hourly_price, vcpu,"
            " memory_mb, created_at, updated_at)"
            " VALUES (:id, :slug, 'tart', 'ephemeral', 'ubuntu', 'small', :status, :owner,"
            " 3600, :t, :t, 0, 2, 4096, :t, :t)"
        ),
        {"id": instance_id, "slug": slug, "status": status, "owner": owner, "t": T},
    )


def _instance_run(conn, run_id, instance_id, status="pending"):
    conn.execute(
        text(
            "INSERT INTO workflow_runs (id, workflow, workflow_version, instance_id, status,"
            " created_at) VALUES (:id, 'provision-instance-tart', 1, :iid, :status, :t)"
        ),
        {"id": run_id, "iid": instance_id, "status": status, "t": T},
    )


# --------------------------------------------------------------------------------------
# the migration, on a populated database
# --------------------------------------------------------------------------------------


def test_populated_v4_database_keeps_every_run_and_step(v4):
    before = _snapshot(v4)
    migrate_0005(v4.engine)

    runs, steps = _snapshot(v4)
    assert runs == before[0]  # same 5 rows, every original column unchanged
    assert steps == before[1]  # same 5 steps
    assert len(runs) == 5 and len(steps) == 5
    with v4.engine.begin() as conn:
        assert conn.exec_driver_sql("PRAGMA user_version").scalar() == 5
        assert conn.exec_driver_sql("SELECT COUNT(*) FROM workflow_runs WHERE instance_id IS NOT NULL").scalar() == 0
        # untouched neighbours
        assert conn.exec_driver_sql("SELECT COUNT(*) FROM timers").scalar() == 1
        assert conn.exec_driver_sql("SELECT COUNT(*) FROM clusters").scalar() == 2


def test_foreign_key_check_is_empty_and_foreign_keys_are_back_on(v4):
    migrate_0005(v4.engine)
    with v4.engine.begin() as conn:
        assert conn.exec_driver_sql("PRAGMA foreign_key_check").fetchall() == []
        assert conn.exec_driver_sql("PRAGMA foreign_keys").scalar() == 1
        # and the workflow_steps reference points at the REBUILT table, not a dangling name
        with pytest.raises(IntegrityError):
            _step(conn, "no-such-run", "create")


def test_no_leftover_table_or_index_and_nothing_else_is_on_workflow_runs(v4):
    migrate_0005(v4.engine)
    with v4.engine.begin() as conn:
        names = {
            r[0]: (r[1], r[2])
            for r in conn.exec_driver_sql(
                "SELECT name, type, tbl_name FROM sqlite_master"
            ).fetchall()
        }
        assert "workflow_runs_new" not in names
        on_runs = sorted(
            n for n, (typ, tbl) in names.items()
            if tbl == "workflow_runs" and not n.startswith("sqlite_autoindex")
        )
        # no trigger, no view-like extras: the table, its three indexes, nothing else
        assert on_runs == ["ix_wr_cluster", "ux_wr_one_active", "ux_wr_one_active_instance", "workflow_runs"]
        assert not [n for n, (typ, _) in names.items() if typ == "trigger"]


def test_check_requires_exactly_one_aggregate(v4):
    migrate_0005(v4.engine)
    with v4.engine.begin() as conn:
        _insert_instance(conn, "i1", "box-1")
    base = (
        "INSERT INTO workflow_runs (id, workflow, workflow_version, {cols}, status, created_at)"
        " VALUES ('x', 'w', 1, {vals}, 'succeeded', 'T')"
    )
    both = base.format(cols="cluster_id, instance_id", vals="'c1', 'i1'")
    neither = base.format(cols="deployment_id", vals="NULL")
    for sql in (both, neither):
        with pytest.raises(IntegrityError), v4.engine.begin() as conn:
            conn.exec_driver_sql(sql)
    with v4.engine.begin() as conn:  # exactly one is fine, either way round
        conn.exec_driver_sql(base.format(cols="cluster_id", vals="'c1'"))
        conn.exec_driver_sql(base.format(cols="instance_id", vals="'i1'").replace("'x'", "'y'"))


def test_one_active_run_per_cluster_still_holds(v4):
    migrate_0005(v4.engine)
    # c1 already has r4 'running'
    for status in ACTIVE:
        with pytest.raises(IntegrityError), v4.engine.begin() as conn:
            _run(conn, f"dup-{status}", "c1", status)
    with v4.engine.begin() as conn:
        _run(conn, "more-1", "c1", "succeeded")  # finished runs are unlimited
        _run(conn, "more-2", "c1", "failed")
        _run(conn, "more-3", "c1", "cancelled")


def test_one_active_run_per_instance(v4):
    migrate_0005(v4.engine)
    with v4.engine.begin() as conn:
        _insert_instance(conn, "i1", "box-1")
        _insert_instance(conn, "i2", "box-2")
        _instance_run(conn, "ir1", "i1", "running")
        _instance_run(conn, "ir2", "i2", "running")  # a different instance is fine
    for status in ACTIVE:
        with pytest.raises(IntegrityError), v4.engine.begin() as conn:
            _instance_run(conn, f"dup-{status}", "i1", status)
    with v4.engine.begin() as conn:
        for n, status in enumerate(("succeeded", "failed", "cancelled", "succeeded")):
            _instance_run(conn, f"done-{n}", "i1", status)  # finished runs are unlimited
        # a cluster's live run and an instance's live run do not contend
        assert conn.exec_driver_sql(
            "SELECT COUNT(*) FROM workflow_runs WHERE status = 'running'"
        ).scalar() == 3  # r4 + ir1 + ir2


def test_dedupe_key_is_still_unique(v4):
    migrate_0005(v4.engine)
    with pytest.raises(IntegrityError), v4.engine.begin() as conn:
        _run(conn, "r-dup", "c2", "succeeded", dedupe_key="run/a")


# --------------------------------------------------------------------------------------
# the instances table
# --------------------------------------------------------------------------------------


def test_instances_columns(v4):
    migrate_0005(v4.engine)
    with v4.engine.begin() as conn:
        cols = {r[1]: r for r in conn.exec_driver_sql("PRAGMA table_info(instances)").fetchall()}
    assert set(cols) == {
        "id", "slug", "provider", "environment", "template", "size", "status", "version",
        "owner_key_id", "resource_ids", "address", "known_hosts", "lease_seconds", "expires_at",
        "hard_expires_at", "hourly_price", "vcpu", "memory_mb", "failure_reason", "created_at",
        "updated_at", "destroyed_at",
    }
    assert cols["version"][4] == "0" and cols["resource_ids"][4] == "'{}'"


def test_instance_status_check(v4):
    migrate_0005(v4.engine)
    states = ("new", "provisioning", "active", "failed", "destroying", "destroyed",
              "destroy-failed", "zombie")
    with v4.engine.begin() as conn:
        for n, status in enumerate(states):
            _insert_instance(conn, f"i{n}", f"box-{n}", status=status)
    for bad in ("unmanaged", "destroy-scheduled", ""):
        with pytest.raises(IntegrityError), v4.engine.begin() as conn:
            _insert_instance(conn, "bad", "bad-slug", status=bad)


def test_instance_slug_is_unique_until_destroyed(v4):
    migrate_0005(v4.engine)
    with v4.engine.begin() as conn:
        _insert_instance(conn, "i1", "box", status="provisioning")
    for status in ("new", "provisioning", "active", "failed", "destroying", "destroy-failed", "zombie"):
        with pytest.raises(IntegrityError), v4.engine.begin() as conn:
            _insert_instance(conn, "i2", "box", status=status)
    with v4.engine.begin() as conn:
        conn.exec_driver_sql("UPDATE instances SET status = 'destroyed' WHERE id = 'i1'")
        _insert_instance(conn, "i2", "box")  # slug reusable once the old one is destroyed
        _insert_instance(conn, "i3", "box", status="destroyed")  # and many destroyed may share it
        _insert_instance(conn, "i4", "box", status="destroyed")


def test_instance_owner_must_exist_but_a_revoked_key_is_fine(v4):
    migrate_0005(v4.engine)
    with pytest.raises(IntegrityError), v4.engine.begin() as conn:
        _insert_instance(conn, "i1", "box-1", owner=999)
    with v4.engine.begin() as conn:
        _insert_instance(conn, "i2", "box-2", owner=2)  # key 2 is_active = 0
        # revoking an owner afterwards (ApiKeyRepository.revoke is an UPDATE) changes nothing here
        conn.exec_driver_sql("UPDATE api_keys SET is_active = 0 WHERE id = 1")
        _insert_instance(conn, "i3", "box-3", owner=1)
        assert conn.exec_driver_sql("PRAGMA foreign_key_check").fetchall() == []


def test_instance_price_and_sizes_have_no_default(v4):
    migrate_0005(v4.engine)
    with pytest.raises(IntegrityError), v4.engine.begin() as conn:
        conn.exec_driver_sql(
            "INSERT INTO instances (id, slug, provider, environment, template, size, status,"
            " owner_key_id, lease_seconds, expires_at, hard_expires_at, vcpu, memory_mb,"
            " created_at, updated_at) VALUES ('i', 's', 'tart', 'e', 't', 'z', 'new', 1, 60,"
            " 'T', 'T', 1, 1, 'T', 'T')"
        )


# --------------------------------------------------------------------------------------
# a failed migration leaves nothing behind
# --------------------------------------------------------------------------------------


def _failing_dir(tmp_path: Path) -> Path:
    """0001-0004 plus the REAL 0005 with one bad statement injected after the whole rebuild
    and before its COMMIT, so the failure comes with the rebuild already done inside the
    transaction and foreign_keys already off."""
    d = _v4_dir(tmp_path, "migrations-failing")
    real = (MIGRATIONS_DIR / "0005_instances.sql").read_text()
    marker = "\nCOMMIT;\n"
    assert real.count(marker) == 1
    (d / "0005_instances.sql").write_text(real.replace(marker, "\nSELECT * FROM no_such_table;\nCOMMIT;\n"))
    return d


def test_failed_migration_leaves_nothing_partial(v4, tmp_path):
    before = _snapshot(v4)
    outbox_before, sequence_before = _outbox(v4), _outbox_sequence(v4)
    assert sequence_before == [("effects_outbox", 6)]
    with pytest.raises(sqlite3.OperationalError, match="no_such_table"):
        migrate(v4.engine, migrations_dir=_failing_dir(tmp_path))

    with v4.engine.begin() as conn:
        assert conn.exec_driver_sql("PRAGMA user_version").scalar() == 4
        tables = {r[0] for r in conn.exec_driver_sql("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
        assert "instances" not in tables and "workflow_runs_new" not in tables
        cols = {r[1]: r for r in conn.exec_driver_sql("PRAGMA table_info(workflow_runs)").fetchall()}
        assert "instance_id" not in cols and cols["cluster_id"][3] == 1  # still NOT NULL
        indexes = {r[0] for r in conn.exec_driver_sql("SELECT name FROM sqlite_master WHERE type='index'").fetchall()}
        assert {"ux_wr_one_active", "ix_wr_cluster"} <= indexes
        assert "ux_wr_one_active_instance" not in indexes and "ux_instances_slug_live" not in indexes
        assert "effects_outbox_new" not in tables
        assert {"idx_outbox_drain", "idx_outbox_aggregate"} <= indexes
    assert _snapshot(v4) == before
    # the OLD outbox is intact: rows, high-water mark, and the old CHECK (no 'instance' yet)
    assert _outbox(v4) == outbox_before
    assert _outbox_sequence(v4) == sequence_before
    with pytest.raises(IntegrityError), v4.engine.begin() as conn:
        _insert_outbox(conn, "new-1", "instance")
    with v4.engine.begin() as conn:
        assert _insert_outbox(conn, "new-2") == 7


def test_failed_migration_restores_foreign_keys_on_the_shared_connection(v4, tmp_path):
    with pytest.raises(sqlite3.OperationalError):
        migrate(v4.engine, migrations_dir=_failing_dir(tmp_path))
    with v4.engine.begin() as conn:
        assert conn.exec_driver_sql("PRAGMA foreign_keys").scalar() == 1
        # enforced for real, not just reported: the FK on workflow_steps still bites
        with pytest.raises(IntegrityError):
            _step(conn, "no-such-run", "create")
    # no transaction was left open on the shared connection
    assert not v4.engine.raw_connection().driver_connection.in_transaction


def test_migration_can_be_retried_after_a_failure(v4, tmp_path):
    with pytest.raises(sqlite3.OperationalError):
        migrate(v4.engine, migrations_dir=_failing_dir(tmp_path))
    migrate_0005(v4.engine)
    with v4.engine.begin() as conn:
        assert conn.exec_driver_sql("PRAGMA user_version").scalar() == 5
        assert conn.exec_driver_sql("PRAGMA foreign_key_check").fetchall() == []
        assert conn.exec_driver_sql("SELECT COUNT(*) FROM workflow_runs").scalar() == 5


# --------------------------------------------------------------------------------------
# effects_outbox: aggregate_type gains 'instance'; nothing else changes
# --------------------------------------------------------------------------------------


def test_outbox_rows_and_seq_values_are_preserved(v4):
    before = _outbox(v4)
    assert [r[0] for r in before] == [1, 2, 3, 4, 5]
    assert {r[9] for r in before} == {"pending", "done", "dead"}
    migrate_0005(v4.engine)
    assert _outbox(v4) == before  # every column, including attempts, available_at, last_error
    dead = [r for r in _outbox(v4) if r[9] == "dead"][0]
    assert dead[10] == 8 and dead[14] == "gave up"


def test_seq_is_never_reused_when_the_top_row_was_deleted_before_the_migration(v4):
    """The fixture deleted seq 6, so MAX(seq) is 5 but AUTOINCREMENT's mark is 6."""
    migrate_0005(v4.engine)
    assert _outbox_sequence(v4) == [("effects_outbox", 6)]
    with v4.engine.begin() as conn:
        assert _insert_outbox(conn, "after-1") == 7
        assert _insert_outbox(conn, "after-2") == 8


def test_seq_mark_survives_an_empty_outbox(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 't.db'}")
    migrate(db.engine, migrations_dir=_v4_dir(tmp_path))
    with db.engine.begin() as conn:
        _seed_outbox(conn, delete_top=False)
        conn.execute(text("DELETE FROM effects_outbox"))
        assert conn.execute(text("SELECT COUNT(*) FROM effects_outbox")).scalar() == 0
    migrate_0005(db.engine)
    assert _outbox_sequence(db) == [("effects_outbox", 6)]
    with db.engine.begin() as conn:
        assert _insert_outbox(conn, "first-after") == 7


def test_a_never_used_outbox_starts_at_one(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 't.db'}")
    migrate(db.engine, migrations_dir=_v4_dir(tmp_path))
    migrate_0005(db.engine)
    assert _outbox_sequence(db) in ([], [("effects_outbox", 0)])  # never used: no mark, or a zero one
    with db.engine.begin() as conn:
        assert _insert_outbox(conn, "first") == 1


def test_seq_mark_is_unchanged_when_nothing_was_deleted(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 't.db'}")
    migrate(db.engine, migrations_dir=_v4_dir(tmp_path))
    with db.engine.begin() as conn:
        _seed_outbox(conn, delete_top=False)
    migrate_0005(db.engine)
    assert _outbox_sequence(db) == [("effects_outbox", 6)]
    with db.engine.begin() as conn:
        assert _insert_outbox(conn, "next") == 7


def test_outbox_accepts_instance_and_still_rejects_an_unknown_aggregate_type(v4):
    migrate_0005(v4.engine)
    with v4.engine.begin() as conn:
        for n, agg in enumerate(("cluster", "deployment", "run", "instance")):
            _insert_outbox(conn, f"ok-{n}", agg)
    for bad in ("bogus", "Instance", "", "instances"):
        with pytest.raises(IntegrityError), v4.engine.begin() as conn:
            _insert_outbox(conn, "bad", bad)


def test_the_other_outbox_constraints_are_unchanged(v4):
    migrate_0005(v4.engine)
    for over in ({"kind": "bogus"}, {"lane": "bogus"}, {"status": "bogus"}):
        with pytest.raises(IntegrityError), v4.engine.begin() as conn:
            _insert_outbox(conn, "bad", **over)
    with v4.engine.begin() as conn:
        _insert_outbox(conn, "dup")
    with pytest.raises(IntegrityError), v4.engine.begin() as conn:
        _insert_outbox(conn, "dup")  # effect_id is still UNIQUE
    with v4.engine.begin() as conn:
        cols = {r[1]: r for r in conn.exec_driver_sql("PRAGMA table_info(effects_outbox)").fetchall()}
    assert cols["status"][4] == "'pending'" and cols["attempts"][4] == "0"
    assert list(cols) == [
        "seq", "effect_id", "aggregate_type", "aggregate_id", "to_version", "ordinal", "kind",
        "payload", "lane", "status", "attempts", "available_at", "created_at", "done_at", "last_error",
    ]


def test_outbox_indexes_are_recreated_and_nothing_else_hangs_off_it(v4):
    migrate_0005(v4.engine)
    with v4.engine.begin() as conn:
        master = conn.exec_driver_sql(
            "SELECT type, name, tbl_name, sql FROM sqlite_master"
        ).fetchall()
        assert "effects_outbox_new" not in {r[1] for r in master}
        on_outbox = sorted(
            (r[0], r[1]) for r in master
            if r[2] == "effects_outbox" and not r[1].startswith("sqlite_autoindex")
        )
        assert on_outbox == [("index", "idx_outbox_aggregate"), ("index", "idx_outbox_drain"), ("table", "effects_outbox")]
        assert not [r for r in master if r[0] in ("trigger", "view")]
        drain = conn.exec_driver_sql("PRAGMA index_info(idx_outbox_drain)").fetchall()
        agg = conn.exec_driver_sql("PRAGMA index_info(idx_outbox_aggregate)").fetchall()
        assert [r[2] for r in drain] == ["status", "available_at", "seq"]
        assert [r[2] for r in agg] == ["aggregate_type", "aggregate_id", "seq"]
        assert conn.exec_driver_sql("PRAGMA foreign_key_check").fetchall() == []
