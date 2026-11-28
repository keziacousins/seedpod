"""The entire migration system (docs/design/seam-d-foundation.md, Decision 6).

The schema exists in exactly one place: numbered SQL files under
seedpod/data/migrations/NNNN_*.sql, applied in order, keyed on PRAGMA user_version.
There is no create_all() anywhere in v2 and no alembic.
"""

from pathlib import Path

from sqlalchemy import Engine

MIGRATIONS_DIR = Path(__file__).parent / "migrations"


class MigrationError(RuntimeError):
    pass


def _recover(raw) -> None:
    """Put the shared connection back as the application expects it after a failed script.

    ``executescript`` leaves an open transaction behind when a script that began its own
    ``BEGIN`` fails, and a script that turned ``foreign_keys`` off (a table rebuild, e.g.
    0005) never reaches its own ``PRAGMA foreign_keys = ON``. Production runs on one
    shared StaticPool connection, so both would outlive the failure. Roll back first:
    the PRAGMA does nothing inside a transaction."""
    raw.rollback()
    raw.execute("PRAGMA foreign_keys = ON")


def migrate(engine: Engine, migrations_dir: Path = MIGRATIONS_DIR) -> None:
    files = sorted(migrations_dir.glob("[0-9][0-9][0-9][0-9]_*.sql"))
    if not files:
        raise MigrationError(f"no migration files in {migrations_dir}")
    with engine.begin() as conn:
        current = conn.exec_driver_sql("PRAGMA user_version").scalar()
    for f in files:
        n = int(f.name[:4])
        if n <= current:
            continue
        with engine.begin() as conn:                       # one txn per migration
            raw = conn.connection.driver_connection
            try:
                raw.executescript(f.read_text())           # file ends with PRAGMA user_version = N
            except BaseException:
                _recover(raw)
                raise
    with engine.begin() as conn:
        if conn.exec_driver_sql("PRAGMA user_version").scalar() != int(files[-1].name[:4]):
            raise MigrationError("migration file did not stamp user_version")
