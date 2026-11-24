"""0007_instance_ssh_key.sql (DR-0055): one nullable column on a populated v6 database, and its reverse."""

from __future__ import annotations

import shutil

from sqlalchemy import text

from seedpod.data.database import Database
from seedpod.data.migrate import MIGRATIONS_DIR, migrate

T = "2026-08-01T00:00:00Z"


def test_it_adds_a_null_column_and_the_reverse_sql_removes_it(tmp_path):
    d = tmp_path / "migrations-v6"
    d.mkdir()
    for f in sorted(MIGRATIONS_DIR.glob("000[1-6]_*.sql")):
        shutil.copy(f, d / f.name)
    db = Database(f"sqlite:///{tmp_path / 't.db'}")
    migrate(db.engine, migrations_dir=d)
    with db.engine.begin() as conn:
        conn.execute(text("INSERT INTO api_keys (key_hash, username, created_at) VALUES ('h', 'u', :t)"), {"t": T})
        conn.execute(
            text(
                "INSERT INTO instances (id, slug, provider, environment, template, size, status, owner_key_id,"
                " lease_seconds, expires_at, hard_expires_at, hourly_price, vcpu, memory_mb, created_at, updated_at)"
                " VALUES ('i1', 'box', 'tart', 'e', 't', 's', 'active', 1, 60, :t, :t, 0, 1, 1, :t, :t)"
            ),
            {"t": T},
        )

    migrate(db.engine)
    with db.engine.begin() as conn:
        assert conn.exec_driver_sql("PRAGMA user_version").scalar() == 7
        assert conn.execute(text("SELECT slug, ssh_public_key FROM instances")).all() == [("box", None)]
        conn.execute(text("UPDATE instances SET ssh_public_key = 'k'"))  # the column takes a value

    with db.engine.begin() as conn:  # the reverse SQL from the file's header
        conn.connection.driver_connection.executescript(
            "ALTER TABLE instances DROP COLUMN ssh_public_key; PRAGMA user_version = 6;"
        )
        assert "ssh_public_key" not in [r[1] for r in conn.exec_driver_sql("PRAGMA table_info(instances)")]
