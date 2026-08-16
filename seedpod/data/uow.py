"""``UnitOfWork`` -- ``async with uow() as tx:`` with commit-on-exit, rollback-on-error
(docs/design/seam-d-foundation.md Decision 6, "the write discipline").

**Sync-vs-async choice (Seam D taste call, owned by this pillar):** the workflow
engine is a plain-asyncio executor (docs/design/seam-b-engine.md), so its call sites
want ``async with uow() as tx: ...``. But ``seedpod/data/database.py`` wraps a *sync*
``sqlalchemy.Engine`` -- forced by ``seedpod/data/migrate.py`` (already built) taking
a sync ``Engine`` and ``App.start`` calling ``migrate(self.db.engine, ...)`` directly.
Rather than run two engines (a sync one for migrations, an async one for everything
else) or add an async SQLite driver (``aiosqlite``) purely to satisfy a syntax
preference, this ``UnitOfWork`` is **sync-in-executor**: opening, committing, rolling
back, and closing the ``Session`` -- the only calls that actually block on I/O -- are
dispatched off the event loop via ``asyncio.to_thread``. The `tx` object yielded to the
``async with`` body is a plain sync SQLAlchemy ``Session``; repositories
(``seedpod/data/repositories.py``) are ordinary **sync** functions (session-in,
DTO-out) called directly inside the block, exactly like every v1-salvaged repo body.
SQLite queries against the ``StaticPool`` single connection are sub-millisecond, so
the event-loop cost of not thread-hopping per individual query is negligible. Flip to
a real async engine later if a slower backend or true per-query concurrency is ever
needed -- no call site above ``async with uow() as tx:`` would have to change.

Repositories **never commit** -- this is the only component that does. On a clean
exit the session commits once; on any exception it rolls back and the exception
propagates unchanged.

**Serialization (docs/decisions/DR-0008-uow-transaction-serialization.md, RATIFIED
2026-07-15).** ``seedpod/data/database.py`` pins ``StaticPool`` + a single shared
DBAPI connection (Seam D parity), and SQLite transactions are connection-scoped. A
real ``await`` inside an open ``uow()`` block (every ``to_thread`` hop above is one)
lets another task's own ``uow()`` interleave on that SAME connection -- without
serialization, task B's ``commit()`` would commit task A's still-open statements,
and A's later ``rollback()`` would undo nothing. This ``UnitOfWork`` therefore owns
one ``asyncio.Lock``: ``uow()`` acquires it before opening the session and releases
it only after commit/rollback + close -- one writer's transaction runs start-to-
finish before the next begins, for the transaction's WHOLE extent. ``tx=`` chaining
(``Dispatcher.apply(..., tx=t)``, its own recursive ``Cascade`` calls, the engine's
terminal transactions) receives the caller's already-open ``Session`` directly and
never calls ``uow()`` again, so it never re-acquires -- the outermost ``uow()``
context owns the lock for the chained transaction's whole extent; there is no
reentrancy machinery. Binding corollary (DR-0008 law 4): **a transaction encloses
ONLY database statements** -- no provider IO, subprocess, ``sleep()``, or broadcast
await inside an open ``uow()``/chained-``tx`` block; violating this now stalls
(visible) rather than silently corrupting (invisible).
"""

from __future__ import annotations

import asyncio
from types import TracebackType

from sqlalchemy.orm import Session

from seedpod.data.database import Database

__all__ = ["UnitOfWork"]


class UnitOfWork:
    """``uow = UnitOfWork(db)``; ``async with uow() as tx: repos.foo.bar(tx, ...)``.

    Owns the DR-0008 single-writer lock -- one ``asyncio.Lock`` shared by every
    ``_Transaction`` this instance hands out, so every ``uow()`` call site in the
    process serializes on the SAME lock (module docstring)."""

    def __init__(self, db: Database) -> None:
        self._db = db
        self._lock = asyncio.Lock()

    def __call__(self) -> _Transaction:
        return _Transaction(self._db, self._lock)


class _Transaction:
    """One ``Session`` per ``async with`` block. Not reentrant; a fresh instance is
    created by every ``UnitOfWork.__call__()``. Holds the DR-0008 lock for its whole
    extent -- acquired in ``__aenter__`` before the session opens, released in
    ``__aexit__`` only after commit/rollback + close have finished."""

    def __init__(self, db: Database, lock: asyncio.Lock) -> None:
        self._db = db
        self._lock = lock
        self._session: Session | None = None

    async def __aenter__(self) -> Session:
        await self._lock.acquire()
        try:
            self._session = await asyncio.to_thread(self._db.session)
        except BaseException:
            self._lock.release()
            raise
        return self._session

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        session = self._session
        assert session is not None, "UnitOfWork.__aexit__ called before __aenter__"
        try:
            if exc_type is None:
                await asyncio.to_thread(session.commit)
            else:
                await asyncio.to_thread(session.rollback)
        finally:
            # The release is itself in a `finally` so a CANCEL landing on the
            # close-await cannot orphan the DR-0008 process-global write lock.
            # `asyncio.to_thread(...)` is a real suspension point, so a
            # `task.cancel()` (e.g. a `TimerService`/`EffectExecutor` stop
            # cancelling its poll loop mid-transaction) can raise CancelledError
            # exactly here; without this guard the release below is skipped and
            # the lock stays held by a task that no longer exists -- every later
            # `uow()` in the process then blocks forever on `acquire()`, which is
            # how an intermittent `App.stop()` teardown hang was root-caused
            # (captured dump: `[locked, waiters:1]`, no live holder). `__aenter__`
            # has had the symmetric guard since it was written; this is the
            # missing other half. `Lock.release()` never awaits, so it always runs.
            try:
                await asyncio.to_thread(session.close)
            finally:
                self._session = None
                self._lock.release()
