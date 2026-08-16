"""seedpod/data/migrate.py: applies 0001, is idempotent, stamps PRAGMA user_version.

Real tmp SQLite, no mocks -- docs/design/seam-d-foundation.md Decision 6 ("the
schema exists in exactly one place: numbered SQL files ... applied in order").
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from seedpod.data.database import Database
from seedpod.data.migrate import MigrationError, migrate


def _user_version(db: Database) -> int:
    with db.engine.begin() as conn:
        return conn.exec_driver_sql("PRAGMA user_version").scalar()


def test_migrate_applies_every_file_and_stamps_user_version(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 't.db'}")
    migrate(db.engine)
    # 0001 initial + 0002 clusters.dns_record_id (DR-0034)
    # + 0003 deployment_presets.default_provider (DR-0046)
    assert _user_version(db) == 3

    with db.engine.begin() as conn:
        tables = {
            row[0]
            for row in conn.exec_driver_sql(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    for expected in (
        "clusters",
        "deployments",
        "workflow_runs",
        "workflow_steps",
        "effects_outbox",
        "timers",
    ):
        assert expected in tables


def test_migrate_is_idempotent(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 't.db'}")
    migrate(db.engine)
    migrate(db.engine)  # second call: every migration file's n <= current, all skipped
    # 0001 initial + 0002 clusters.dns_record_id (DR-0034)
    # + 0003 deployment_presets.default_provider (DR-0046)
    assert _user_version(db) == 3

    # and the schema is still intact / usable, not double-applied
    with db.engine.begin() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM workflow_runs")).scalar()
    assert count == 0


def test_migrate_raises_when_no_migration_files(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 't.db'}")
    empty_dir = tmp_path / "empty-migrations"
    empty_dir.mkdir()
    with pytest.raises(MigrationError):
        migrate(db.engine, migrations_dir=empty_dir)


def test_migrate_pragmas_applied(tmp_path):
    """Seam D Decision 6 per-connection pragmas: foreign_keys, WAL, synchronous, busy_timeout."""
    db = Database(f"sqlite:///{tmp_path / 't.db'}")
    migrate(db.engine)
    with db.engine.connect() as conn:
        assert conn.exec_driver_sql("PRAGMA foreign_keys").scalar() == 1
        assert conn.exec_driver_sql("PRAGMA journal_mode").scalar() == "wal"
        assert conn.exec_driver_sql("PRAGMA synchronous").scalar() == 1  # NORMAL
        assert conn.exec_driver_sql("PRAGMA busy_timeout").scalar() == 30000
