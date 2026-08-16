"""``Database`` -- engine + sessionmaker + per-connection pragma listener
(docs/design/seam-d-foundation.md Decision 6, as amended for SQLite by
docs/design/coherence-review.md's data-layer conventions).

Wraps a **sync** ``sqlalchemy.Engine``, not an async one: ``seedpod/data/migrate.py``
(already built, Pillar 1 handoff) takes a sync ``Engine`` and ``App.start`` calls
``migrate(self.db.engine, MIGRATIONS_DIR)`` directly, so this is the one engine type
that keeps that call site working. ``seedpod/data/uow.py`` is where the sync/async
seam is bridged for the engine's ``async with uow() as tx:`` surface -- see its
module docstring for the "sync-in-executor" rationale.

Constructing a ``Database`` opens no connection (SQLAlchemy engines are lazy by
construction); the pragma listener fires once per new DBAPI connection, which for
the ``StaticPool`` SQLite configuration below is effectively once per ``Database``.
"""

from __future__ import annotations

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

__all__ = ["Database"]


class Database:
    """One `database_url` -> one engine + sessionmaker. No global state."""

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url
        is_sqlite = database_url.startswith("sqlite")
        # v1 parity (Seam D): StaticPool + check_same_thread=False for SQLite -- a single
        # shared DBAPI connection, safe because seedpod/data/uow.py serializes access to it
        # through asyncio.to_thread and SQLAlchemy's own Session-per-transaction discipline.
        engine_kwargs: dict[str, object] = {}
        if is_sqlite:
            engine_kwargs["connect_args"] = {"check_same_thread": False}
            engine_kwargs["poolclass"] = StaticPool
        self.engine: Engine = create_engine(database_url, **engine_kwargs)
        if is_sqlite:
            event.listens_for(self.engine, "connect")(_set_sqlite_pragmas)
        self._sessionmaker = sessionmaker(bind=self.engine, expire_on_commit=False)

    def session(self) -> Session:
        """A new, unopened ORM ``Session`` bound to this engine. Callers (``UnitOfWork``)
        own its lifecycle; ``Database`` never commits or closes one itself."""
        return self._sessionmaker()

    def dispose(self) -> None:
        self.engine.dispose()


def _set_sqlite_pragmas(dbapi_connection: object, connection_record: object) -> None:
    """Per-connection pragmas (Seam D Decision 6): foreign_keys=ON, WAL, synchronous=NORMAL,
    busy_timeout=30000. Registered on the engine's ``connect`` event, never run ad hoc."""
    cursor = dbapi_connection.cursor()  # type: ignore[attr-defined]
    try:
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.execute("PRAGMA busy_timeout=30000")
    finally:
        cursor.close()
