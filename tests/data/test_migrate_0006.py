"""0006_instance_state_audits.sql (DR-0055): one table and a slug index swap.

0005 is not edited and no existing table is rebuilt. These tests run it against a populated v5
database, check nothing else moved, and run the reverse SQL from the file's
own header to prove it is the inverse. Real tmp SQLite, no mocks.
"""

from __future__ import annotations

import hashlib
import shutil
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from seedpod.data.database import Database
from seedpod.data.migrate import MIGRATIONS_DIR, migrate

T = "2026-08-01T00:00:00Z"

# The sha256 of 0005_instances.sql as ratified. "Do not edit 0005": a later change to it fails here.
SHA256_0005 = "3844ef4acb65a30fcc6a9f34991821b52c393c0a0fa0b8e0464f381f85211fa8"


def _through_0005(tmp_path: Path) -> Path:
    d = tmp_path / "migrations-v5"
    d.mkdir()
    for f in sorted(MIGRATIONS_DIR.glob("000[1-5]_*.sql")):
        shutil.copy(f, d / f.name)
    return d


@pytest.fixture
def v5(tmp_path):
    db = Database(f"sqlite:///{tmp_path / 't.db'}")
    migrate(db.engine, migrations_dir=_through_0005(tmp_path))
    with db.engine.begin() as conn:
        assert conn.exec_driver_sql("PRAGMA user_version").scalar() == 5
        conn.execute(
            text("INSERT INTO api_keys (key_hash, username, created_at) VALUES ('h', 'u', :t)"), {"t": T}
        )
        conn.execute(
            text(
                "INSERT INTO instances (id, slug, provider, environment, template, size, status,"
                " owner_key_id, lease_seconds, expires_at, hard_expires_at, hourly_price, vcpu,"
                " memory_mb, created_at, updated_at) VALUES ('i1', 'box', 'tart', 'e', 't', 's', 'active',"
                " 1, 60, :t, :t, 0, 1, 1, :t, :t)"
            ),
            {"t": T},
        )
    return db


def _master(db) -> dict[str, tuple[str, str]]:
    with db.engine.begin() as conn:
        return {
            r[0]: (r[1], r[2])
            for r in conn.exec_driver_sql("SELECT name, type, tbl_name FROM sqlite_master").fetchall()
        }


def test_0005_is_untouched():
    digest = hashlib.sha256((MIGRATIONS_DIR / "0005_instances.sql").read_bytes()).hexdigest()
    # Computed once from the committed file; see SHA256_0005. If this fails, 0005 was edited.
    assert digest == SHA256_0005


def test_it_adds_the_audit_table_and_swaps_the_slug_index(v5):
    before = _master(v5)
    migrate(v5.engine)
    after = _master(v5)

    added = {k: v for k, v in after.items() if k not in before}
    assert added == {
        "instance_state_audits": ("table", "instance_state_audits"),
        "ix_isa_instance_time": ("index", "instance_state_audits"),
        "ux_instances_slug": ("index", "instances"),
    }
    assert {k for k in before if k not in after} == {"ux_instances_slug_live"}  # the only thing dropped
    with v5.engine.begin() as conn:
        assert conn.exec_driver_sql("PRAGMA user_version").scalar() == 6
        assert conn.exec_driver_sql("PRAGMA foreign_key_check").fetchall() == []
        assert conn.exec_driver_sql("PRAGMA foreign_keys").scalar() == 1
        assert conn.exec_driver_sql("SELECT COUNT(*) FROM instances").scalar() == 1  # the row is still there


def test_the_audit_table_has_the_cluster_audit_shape(v5):
    migrate(v5.engine)
    with v5.engine.begin() as conn:
        cols = {r[1]: (r[2], r[3]) for r in conn.exec_driver_sql("PRAGMA table_info(instance_state_audits)")}
        csa = {r[1]: (r[2], r[3]) for r in conn.exec_driver_sql("PRAGMA table_info(cluster_state_audits)")}
    assert set(cols) == {
        "id", "instance_id", "from_state", "to_state", "event", "actor", "reason", "context", "created_at",
    }
    for name in ("from_state", "to_state", "event", "actor", "reason", "context", "created_at"):
        assert cols[name] == csa[name]  # same type, same NOT NULL-ness


def test_an_audit_row_needs_a_real_instance(v5):
    migrate(v5.engine)
    insert = (
        "INSERT INTO instance_state_audits (instance_id, from_state, to_state, event, actor, created_at)"
        " VALUES (:i, 'new', 'provisioning', 'E', 'a', :t)"
    )
    with v5.engine.begin() as conn:
        conn.execute(text(insert), {"i": "i1", "t": T})
    with pytest.raises(IntegrityError), v5.engine.begin() as conn:
        conn.execute(text(insert), {"i": "no-such-instance", "t": T})


@pytest.mark.parametrize(
    "status", ["new", "provisioning", "active", "failed", "destroying", "destroyed", "destroy-failed", "zombie"]
)
def test_an_instance_slug_is_never_reused_in_any_status(v5, status):
    """Defect 5: a slug freed by `destroyed` let a second instance take it, and the first one, found
    running again (a zombie), then could not leave `destroyed`. Now the slug is unique outright."""
    migrate(v5.engine)
    with v5.engine.begin() as conn:
        conn.execute(text("UPDATE instances SET status = :s WHERE id = 'i1'"), {"s": status})
    insert = (
        "INSERT INTO instances (id, slug, provider, environment, template, size, status,"
        " owner_key_id, lease_seconds, expires_at, hard_expires_at, hourly_price, vcpu,"
        " memory_mb, created_at, updated_at) VALUES ('i2', 'box', 'tart', 'e', 't', 's', 'destroyed',"
        " 1, 60, :t, :t, 0, 1, 1, :t, :t)"
    )
    with pytest.raises(IntegrityError), v5.engine.begin() as conn:
        conn.execute(text(insert), {"t": T})


def test_it_is_idempotent(v5):
    migrate(v5.engine)
    migrate(v5.engine)
    with v5.engine.begin() as conn:
        assert conn.exec_driver_sql("PRAGMA user_version").scalar() == 6


def test_the_reverse_sql_in_the_header_undoes_it(v5):
    migrate(v5.engine)
    # the reverse SQL, as written in the file's own header
    header = (MIGRATIONS_DIR / "0006_instance_state_audits.sql").read_text().splitlines()
    reverse = "\n".join(ln[5:] for ln in header if ln.startswith("--   "))
    assert "DROP TABLE" in reverse and "user_version = 5" in reverse
    assert "DROP INDEX ux_instances_slug;" in reverse and "ux_instances_slug_live" in reverse
    with v5.engine.begin() as conn:
        conn.connection.driver_connection.executescript(reverse)
    names = _master(v5)
    assert "instance_state_audits" not in names
    assert "ux_instances_slug_live" in names and "ux_instances_slug" not in names  # 0005's index is back
    migrate(v5.engine)  # and forward again
    assert "instance_state_audits" in _master(v5)
