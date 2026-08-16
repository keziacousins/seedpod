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
            conn.connection.executescript(f.read_text())   # file ends with PRAGMA user_version = N
    with engine.begin() as conn:
        if conn.exec_driver_sql("PRAGMA user_version").scalar() != int(files[-1].name[:4]):
            raise MigrationError("migration file did not stamp user_version")
