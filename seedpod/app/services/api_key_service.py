"""``ApiKeyService`` -- create/validate/lookup/revoke over the ``api_keys`` table.

Constructor shape: docs/design/seam-d-foundation.md Decision 8 step 9's
``ApiKeyService(repos, uow, clock=clock)``, made concrete: ``repos`` here is the
``ApiKeyRepository`` directly (not in the Dispatcher-facing ``Repositories``
bundle -- see ``SecretService``'s module docstring for the identical reasoning).

Salvaged from ``reference-code/seedpod/seedpod/core/auth.py``'s
``APIKeyManager`` (``generate_api_key``/``_hash_api_key``/``create_api_key``/
``validate_api_key``, lines 224-360): sha256-hashed-at-rest, ``seedpod_<env>_
<32-hex>`` plaintext format, ``environment=None`` normalizes to the ``'all'``
sentinel. ONE deliberate v2 change (not a bug pin -- CLAUDE.md's "don't pin v1
bugs"): ``permissions`` is a JSON **list** of permission strings
(``ApiKeyRepository``'s own docstring: "not v1's ``dict[str, bool]`` shape"),
per this round's brief -- ``create_api_key(..., permissions=[...])`` is the
pinned conftest contract, not v1's dict shape.

**``validate()`` does not write (DR-0044).** It used to: a SELECT by hash plus a
``touch_last_used`` UPDATE and a COMMIT, in one transaction -- which meant EVERY
authenticated request in the system performed a write, took DR-0008's
process-global single-writer lock for its whole extent, and paid three
``asyncio.to_thread`` hops and a real disk write, serialized against the timer
poller, the outbox drain, the health/reconciliation loops and the engine's own
per-step persistence. ``seedpod/api/auth.py``'s docstring had described the
correct behaviour ("``validate()`` is read-only, this module writes nothing")
the whole time the code did the opposite.

``last_used_at`` is a liveness breadcrumb, not a ledger: nothing branches on it,
and both UI surfaces render it at DATE granularity (``ApiKeyList.jsx``,
``ApiKeyDetail.jsx``'s ``formatDate``). So touches are buffered in memory and
flushed periodically in ONE transaction -- N requests in a window cost one write
instead of N. **Revocation is not deferred**: the SELECT still runs on every
request, so ``is_active = 0`` and expiry take effect immediately. Only the
bookkeeping is late, and only by ``flush_interval``.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import hashlib
import logging
import secrets as _secrets
from datetime import datetime, timedelta

from seedpod.core.clock import Clock
from seedpod.data.repositories import ApiKeyRepository, ApiKeyRow
from seedpod.data.uow import UnitOfWork

__all__ = ["ApiKeyService", "ApiKeyNotFound"]

_log = logging.getLogger(__name__)

_KEY_PREFIX = "seedpod"
_DEFAULT_FLUSH_INTERVAL = 60.0  # seconds -- see module docstring


class ApiKeyNotFound(LookupError):
    pass


def _hash_key(plaintext: str) -> str:
    return hashlib.sha256(plaintext.encode("utf-8")).hexdigest()


class ApiKeyService:
    def __init__(
        self,
        api_keys: ApiKeyRepository,
        uow: UnitOfWork,
        clock: Clock,
        flush_interval: float = _DEFAULT_FLUSH_INTERVAL,
    ) -> None:
        self._api_keys = api_keys
        self._uow = uow
        # DR-0044. Mutated only from the event loop, with no `await` between the read
        # and the write in `validate()`, so no lock of its own is needed.
        self._pending_touches: dict[int, datetime] = {}
        self._flush_interval = flush_interval
        self._task: asyncio.Task | None = None
        self._clock = clock

    async def create_api_key(
        self,
        *,
        username: str,
        environment: str | None = None,
        permissions: list[str],
        expires_hours: float | None = None,
        description: str | None = None,
        created_by: str | None = None,
    ) -> tuple[ApiKeyRow, str]:
        env = environment or "all"
        plaintext = f"{_KEY_PREFIX}_{env.lower()}_{_secrets.token_hex(16)}"
        key_hash = _hash_key(plaintext)
        now = self._clock.now()
        expires_at = now + timedelta(hours=expires_hours) if expires_hours else None
        row = ApiKeyRow(
            id=None, key_hash=key_hash, username=username, environment=env, permissions=list(permissions),
            is_active=True, description=description, created_by=created_by, created_at=now,
            expires_at=expires_at, last_used_at=None,
        )
        async with self._uow() as tx:
            self._api_keys.insert(tx, row)
            inserted = self._api_keys.get_by_hash(tx, key_hash)
        assert inserted is not None
        return inserted, plaintext

    async def validate(self, plaintext: str) -> ApiKeyRow | None:
        """Hash -> active+unexpired lookup. Returns ``None`` for no match/inactive/
        expired (never raises) -- the API layer's 401, same discipline v1's
        ``validate_api_key`` used.

        **Read-only (DR-0044).** The ``last_used_at`` touch this used to perform
        inline is recorded in memory and flushed by ``flush_last_used`` -- see the
        module docstring. The lookup itself is unchanged and still runs per request,
        which is what keeps revocation immediate."""
        key_hash = _hash_key(plaintext)
        now = self._clock.now()
        async with self._uow() as tx:
            row = self._api_keys.get_valid_by_hash(tx, key_hash, now=now)
        if row is None:
            return None
        self._pending_touches[row.id] = now  # type: ignore[index]
        return row

    # -------------------------------------------------------------------
    # DR-0044: the deferred last_used_at write
    # -------------------------------------------------------------------

    async def flush_last_used(self) -> int:
        """Write every buffered touch in ONE transaction; returns how many rows were
        touched. Public and directly callable -- the same discipline
        ``ReconciliationService.tick``/``HealthMonitor.tick`` established, so tests
        drive it deterministically instead of waiting on ``_flush_interval``.

        The buffer is swapped out BEFORE the transaction opens, so a touch arriving
        mid-flush lands in the next batch rather than being dropped by a clear() that
        races the write. On failure the un-written entries are folded back in (without
        clobbering anything newer) and retried next tick -- a lost breadcrumb is
        cheap, but losing it silently and forever is not."""
        if not self._pending_touches:
            return 0
        batch, self._pending_touches = self._pending_touches, {}
        try:
            async with self._uow() as tx:
                for key_id, when in batch.items():
                    self._api_keys.touch_last_used(tx, key_id, clock=self._clock, when=when)
        except Exception:
            for key_id, when in batch.items():
                if key_id not in self._pending_touches:
                    self._pending_touches[key_id] = when
            raise
        return len(batch)

    async def start(self) -> None:
        """Idempotent. Spawns the periodic flush loop. Unlike the runtime spine's
        pollers there is no immediate first tick -- there is nothing buffered at
        startup, so it would be a guaranteed no-op."""
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Cancel the loop, then flush what is left -- UNCONDITIONALLY, and before
        ``App.stop`` disposes the database. A clean shutdown must not drop touches
        just because the interval had not elapsed."""
        task, self._task = self._task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        try:
            await self.flush_last_used()
        except Exception:
            # Swallowed so a breadcrumb can never block shutdown -- but SAID, because
            # a silently-dropped write is indistinguishable from a key nobody used,
            # and "the reason is computed then discarded" is this tree's oldest
            # recurring defect. The count is recoverable from the log if it matters.
            _log.exception(
                "final last_used_at flush failed during shutdown; %d touch(es) dropped",
                len(self._pending_touches),
            )

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self._flush_interval)
            try:
                await self.flush_last_used()
            except Exception:
                # Scan-level isolation, same discipline as TimerService/
                # EffectExecutor: one bad pass must not kill this task, since
                # nothing restarts it. The entries are already back in the buffer.
                _log.exception("last_used_at flush failed, will retry next tick")

    def _with_pending_touch(self, row: ApiKeyRow) -> ApiKeyRow:
        """Overlay an unflushed touch onto a row on its way out (DR-0044 decision 5).
        Without this a key created and used seconds ago reads "Never" on its own
        detail page until the next flush -- the one staleness a human would notice."""
        pending = self._pending_touches.get(row.id)  # type: ignore[arg-type]
        if pending is None or (row.last_used_at is not None and row.last_used_at >= pending):
            return row
        return dataclasses.replace(row, last_used_at=pending)

    async def get(self, key_id: int) -> ApiKeyRow:
        async with self._uow() as tx:
            row = self._api_keys.get_by_id(tx, key_id)
        if row is None:
            raise ApiKeyNotFound(str(key_id))
        return self._with_pending_touch(row)  # DR-0044

    async def list(
        self, *, username: str | None = None, environment: str | None = None, active_only: bool = False
    ) -> list[ApiKeyRow]:
        async with self._uow() as tx:
            rows = self._api_keys.list(tx, username=username, environment=environment, active_only=active_only)
        return [self._with_pending_touch(row) for row in rows]  # DR-0044

    async def revoke(self, key_id: int) -> bool:
        async with self._uow() as tx:
            return self._api_keys.revoke(tx, key_id)

    async def update(
        self, key_id: int, *, description: str | None = None, expires_at=None,
    ) -> ApiKeyRow:
        """Round 6, api-features: ``PATCH /api/keys/{id}``
        (``{description, expires_at}``, ui-contract). ``None`` on either kwarg
        means "not provided, leave alone" -- same partial-update convention v1's
        own ``update_api_key`` used (``reference-code/seedpod/seedpod/api/
        auth.py:280-287``, "if request.description is not None: ..."), not
        ported as a bug: a genuine "clear this field" affordance isn't part of
        this round's brief and v1 never offered one either."""
        async with self._uow() as tx:
            if self._api_keys.get_by_id(tx, key_id) is None:
                raise ApiKeyNotFound(str(key_id))
            if description is not None:
                self._api_keys.update(tx, key_id, description=description)
            if expires_at is not None:
                self._api_keys.update(tx, key_id, expires_at=expires_at)
            row = self._api_keys.get_by_id(tx, key_id)
        assert row is not None
        return row
