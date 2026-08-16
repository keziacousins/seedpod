"""``EffectExecutor`` -- the outbox drain loop + run-admitter (docs/design/
coherence-review.md Conflict 2, "the run-admission drain-rules comment block IS
the spec"; Conflict 1's drain policy paragraph; Conflict 15's start-order law).

Salvaged from nothing -- fresh v2 plumbing (Conflict 1: "Seam D's outbox table is
deleted... replaced verbatim by Seam A's DDL"; the drain LOOP itself has no v1
analogue -- v1 executed workflows synchronously inline, no outbox).

**Two jobs, one component (Conflict 2's naming glossary #4: "the drain loop
inside ``EffectExecutor``").**

1. **``notify`` rows** -- one attempt, broadcast through the injected ``hub``,
   mark ``done`` regardless of outcome (seam-a-core.md §D: "broadcast exceptions
   are logged and the row is marked done after 1 attempt ... duplicate SSE on
   crash-replay is harmless"). A payload that fails to decode gets the SAME
   policy -- logged, marked ``done``, never ``dead`` -- rather than a distinct
   failure path: decode is inside the same best-effort ``try`` as the broadcast
   itself (``_drain_notify``). ``Notify.environment`` (resolved AT DECISION TIME
   by ``core/machine.py``, DR-0010) threads straight through as
   ``hub.broadcast(...)``'s ``environment`` kwarg; engine-origin notifies
   (``ctx.progress``/``job_*``, written directly by ``engine/engine.py`` via
   ``OutboxRepository.insert_run_notify``) always carry ``environment=None`` ->
   unscoped, matching that repository method's own comment. UI-contract
   obligation 1 (``deployment_status_changed`` carries ``deployment_id``/
   ``cluster_id``/``old_status``/``new_status``) is satisfied upstream, by
   ``core/machine.py``'s ``_deployment_result`` -- this module broadcasts
   whatever payload the machine built, unchanged (``seedpod/runtime/sse.py``'s
   own docstring: "Payload CONSTRUCTION ... is the effect executor's
   responsibility, not this module's" -- construction already happened in
   Pillar 1; this module's responsibility is passing it through intact, which
   ``tests/runtime/test_effect_executor.py`` proves end-to-end).

2. **``run_workflow``/``cancel_workflow`` rows -- the Conflict 2 admitter, AS
   AMENDED BY docs/decisions/DR-0011-admitter-wait-and-run-conflict.md
   (RATIFIED)**, whose three-branch ``ux_wr_one_active`` conflict rule is
   documented in full at ``_handle_active_run_conflict`` below. Clause 1
   generalizes the wait branch beyond a literal ``workflow == 'destroy'`` test
   (needed for Conflict 12's guarantee -- ``core/machine.py``'s
   ``_deployment_deploying_cancel_requested`` docstring, verbatim: "the
   drain-lane admitter processes them in seq order, so rollback waits for the
   cancelled deploy run to reach terminal" -- which a ``CancelWorkflow`` row
   alone can't deliver, since it only FLIPS ``cancel_requested`` and never
   itself waits for the victim's task to reach a terminal ``status``). Clause 2
   makes the ``run_conflict`` notification a durable, environment-scoped outbox
   row instead of a direct, unscoped broadcast (see ``_handle_active_run_conflict``).

   ``RunWorkflow`` admission:
     1. Load the ``ClusterRecord`` (``repos.clusters.load``) and resolve
        abstract-verb x provider -> concrete definition name + inputs via
        ``dispatch.resolve()`` (Conflict 13). ``workflow_version`` pins
        ``engine.definitions[name].version`` at admission (Conflict 4) -- the
        engine is this component's one source of truth for definition versions
        (see ``WorkflowEngine.definitions``'s docstring).
     2. ``INSERT workflow_runs(..., dedupe_key=row.effect_id) ON CONFLICT
        (dedupe_key) DO NOTHING`` (``WorkflowRunRepository.insert_admitted``) --
        idempotent admission across crash-replay (H7).
     3. A conflict on the DIFFERENT ``ux_wr_one_active`` index (another run
        already live for this cluster) surfaces as ``IntegrityError`` (NOT
        swallowed by the ``ON CONFLICT(dedupe_key)`` target above) and routes to
        ``_handle_active_run_conflict`` -- DR-0011's three branches.
     4. On a successful (or deduped-replay) admission, hand the run to the
        engine via its real committed API -- ``engine.start(run_id)`` (Conflict
        2: "this module only exposes start/resume_inflight/cancel ... for [the
        admitter] to drive"). Best-effort: a failure here is logged, never
        raised -- ``App.start``'s ``engine.resume_inflight()`` (Conflict 15,
        called AFTER ``executor.start()``) is the backstop for any run whose
        hand-off never landed.

   ``CancelWorkflow`` admission: ``UPDATE workflow_runs SET cancel_requested=1
   WHERE cluster_id=? AND status IN (pending,running,blocked,compensating);
   trip the in-memory token; mark row done`` -- implemented by looking up
   ``active_for_cluster`` then delegating BOTH the DB flip and the token trip to
   ``engine.cancel(run_id)`` (its real committed API already does exactly this
   pair, atomically-enough for this purpose: DB write first, durable, THEN the
   in-memory trip -- G1).

3. **Genuine drain failures** (an unexpected exception surfaced while handling
   a ``run_workflow``/``cancel_workflow`` row -- NOT the routine
   ``IntegrityError``/dedupe-hit control flow above, which are handled inline
   and never reach here): ``attempts += 1``; backoff ``[1s, 5s, 30s, 2m,
   10m...]`` (clamped to the ladder's last entry past its length -- the
   ellipsis); ``dead`` once ``attempts >= 8``, ``last_error`` set (dead rows are
   reconciliation's surface, per DR-0002 -- this module never touches them
   again). A payload that fails to decode for one of THESE two kinds is a
   genuine drain failure too, not a special case: ``_process_row`` decodes
   INSIDE this try/except (not before it), so an undecodable run_workflow/
   cancel_workflow row degrades through the same backoff ladder to ``dead``
   instead of raising out of ``drain_pending()`` and starving every row behind
   it in ``seq`` order (Conflict 1's drain policy covers "genuine drain
   failures" without carving out decode as an exception).

**Housekeeping (DR-0002).** Hourly, delete ``done`` rows older than
``retention_days`` (default 7); ``dead`` rows are NEVER auto-pruned. Folded into
the same background loop as the drain pass (no second task) -- ``prune_done()``
is also exposed publicly so a test (or an operator tool) can force a pass
without waiting an hour.

**``start()``/``stop()``/``poke()`` (Conflict 15).** ``start()`` drains
everything ALREADY pending before returning ("H7 crash replay -- Conflict 15":
``await self.executor.start()`` runs before ``self.timers.start()`` and
``self.engine.resume_inflight()`` in ``App.start``'s amended order) via
``drain_pending()``, THEN spawns the periodic poll loop. ``stop()`` cancels that
loop and awaits its demise, idempotently. ``poke()`` is a latency hint only
(sets a wake event) -- correctness never depends on it; the loop's own
``poll_interval`` polling is the backstop, exactly like ``TimerService``'s
``poke()`` (``seedpod/runtime/timers.py``, same discipline, matched here
deliberately for one consistent runtime-spine idiom).

**DR-0008 discipline.** Every ``async with self._uow() as t:`` block below
encloses ONLY database statements -- ``hub.broadcast()``, ``engine.start()``,
and ``engine.cancel()`` (each a real ``await``) always run BETWEEN two such
blocks, never inside one. ``_drain_run_workflow``'s ``IntegrityError`` catch
wraps the WHOLE ``async with self._uow()`` block (not something nested inside
it) so a failed statement's rollback happens through the normal
``UnitOfWork.__aexit__`` exception path, never after a caught-and-swallowed
error tries to ``commit()`` an invalidated session.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Mapping
from datetime import timedelta
from typing import Any, Protocol
from uuid import uuid4

from sqlalchemy.exc import IntegrityError

from seedpod.core.clock import Clock
from seedpod.core.codec import canonical_json, decode_effect
from seedpod.core.effects import CancelWorkflow, Notify, RunWorkflow
from seedpod.core.records import ClusterRecord
from seedpod.data.repositories import OutboxRow, Repositories, WorkflowRunRow
from seedpod.data.uow import UnitOfWork
from seedpod.engine.dispatch_table import WorkflowDispatch

__all__ = ["EffectExecutor", "HubLike", "EngineLike"]

_log = logging.getLogger(__name__)

# seam-a-core.md §D / coherence-review.md Conflict 1: "backoff [1s, 5s, 30s, 2m,
# 10m...]; attempts >= 8 => dead". The ladder's LAST entry repeats for any
# attempt beyond its length (the ellipsis) -- matches TimerService's
# `_unreachable_reprobe_schedule` cap-at-last-entry idiom (seedpod/runtime/timers.py).
_BACKOFF_LADDER: tuple[float, ...] = (1.0, 5.0, 30.0, 120.0, 600.0)
_DEAD_AT_ATTEMPTS = 8

# Conflict 2's destroy-supersede wait: "leave THIS row 'pending' with
# available_at = now + 2s". No spec text ties this number to anything else --
# an implementation choice (short enough that a superseded run is retried
# promptly; long enough not to hot-loop the drain pass against a run that's
# still winding down), same discipline as TimerService's own poll_interval note.
_SUPERSEDE_RETRY_SECONDS = 2.0

_DEFAULT_POLL_INTERVAL = 1.0  # seconds -- same reasoning as TimerService's default

# `stop()`'s cooperative grace before the cancel backstop -- see `stop()` and
# `TimerService`'s identically-shaped constant.
_STOP_GRACE_SECONDS = 5.0
_DEFAULT_RETENTION_DAYS = 7  # DR-0002
_HOUSEKEEPING_INTERVAL = timedelta(hours=1)  # DR-0002: "hourly"


class HubLike(Protocol):
    def broadcast(self, type: str, data: Mapping[str, Any], environment: str | None = None) -> None: ...  # noqa: A002


class EngineLike(Protocol):
    """The ``WorkflowEngine``-shaped dependency the admitter needs (Conflict 2):
    ``start``/``cancel`` are its real committed admission/cancel API;
    ``definitions`` is the read-only accessor added alongside this module (see
    ``seedpod/engine/engine.py``'s ``WorkflowEngine.definitions`` docstring) so
    admission can pin ``workflow_version`` without a second, possibly-drifting
    copy of the definitions mapping."""

    @property
    def definitions(self) -> Mapping[str, Any]: ...

    async def start(self, run_id: str) -> None: ...

    async def cancel(self, run_id: str) -> None: ...


class EffectExecutor:
    """``EffectExecutor(uow, repos, hub, engine, dispatch, clock,
    poll_interval=1.0, retention_days=7)`` -- the outbox drain loop + run-admitter
    (module docstring). Constructed once at the composition root
    (docs/design/coherence-review.md Conflict 15's factory excerpt); tests build
    it directly against a real tmp-SQLite ``Repositories`` bundle and hand-built
    fakes for ``hub``/``engine`` (no Mock/patch anywhere, CLAUDE.md)."""

    def __init__(
        self,
        uow: UnitOfWork,
        repos: Repositories,
        hub: HubLike,
        engine: EngineLike,
        dispatch: WorkflowDispatch,
        clock: Clock,
        poll_interval: float = _DEFAULT_POLL_INTERVAL,
        retention_days: int = _DEFAULT_RETENTION_DAYS,
    ) -> None:
        self._uow = uow
        self._repos = repos
        self._run_repo = repos.workflow_runs
        self._hub = hub
        self._engine = engine
        self._dispatch = dispatch
        self._clock = clock
        self._poll_interval = poll_interval
        self._retention_days = retention_days
        self._wake = asyncio.Event()
        self._task: asyncio.Task[None] | None = None
        self._stopping = False
        self._last_prune_at: Any = None  # datetime | None; Any avoids a forward-ref import cycle

    def poke(self) -> None:
        """Latency hint: wake the poll loop before its next timeout. Never
        load-bearing -- see module docstring."""
        self._wake.set()

    @property
    def running(self) -> bool:
        """Truthful liveness, same discipline as ``TimerService.running``
        (``self._task is not None`` alone would misreport a dead task as
        running)."""
        return self._task is not None and not self._task.done()

    async def start(self) -> None:
        """Idempotent. Drains everything already pending BEFORE returning (H7
        crash replay -- Conflict 15's amended ``App.start`` order calls this
        before ``timers.start()``/``engine.resume_inflight()``), THEN spawns the
        background poll loop."""
        if self._task is not None:
            return
        await self.drain_pending()
        self._stopping = False
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Ask the drain loop to finish, and wait for it to actually do so.
        Idempotent.

        COOPERATIVE, with cancel only as a backstop -- see `TimerService.stop()`
        for the full reasoning. In short: on Python 3.11 a `task.cancel()` that
        lands in the same window in which a `poke()` completes this loop's
        `wait_for(self._wake.wait(), ...)` is SWALLOWED (3.11's pre-3.12 `wait_for`
        has no `uncancel` bookkeeping), leaving the task branded `cancelling` yet
        alive so `await task` never returns. `App.stop()` pokes-then-stops as a
        matter of course, and the drain loop takes the DR-0008 write lock every
        pass, so a lost cancel here also strands that lock. Reproduced
        deterministically at cancel-follows-poke offsets of <=1 tick."""
        task, self._task = self._task, None
        if task is None:
            return
        self._stopping = True
        self._wake.set()  # cooperative wake: ordered AFTER _stopping, see _run
        done, _pending = await asyncio.wait({task}, timeout=_STOP_GRACE_SECONDS)
        if not done:
            task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task

    # -------------------------------------------------------------------------
    # The drain loop
    # -------------------------------------------------------------------------

    async def _run(self) -> None:
        while not self._stopping:
            # Clear BEFORE the pass (TimerService's same reasoning, module
            # docstring): a poke() landing mid-pass survives to wake the
            # subsequent wait immediately rather than being erased by a clear
            # that comes after the pass already missed it.
            self._wake.clear()
            # Re-check AFTER the clear: `stop()` sets `_stopping` BEFORE its
            # `_wake.set()`, so a clear that erased that wake still exits here.
            if self._stopping:
                return
            try:
                await self.drain_pending()
            except Exception:
                _log.exception("outbox drain pass failed, will retry on a later poll")
            try:
                await self._maybe_prune()
            except Exception:
                _log.exception("outbox pruning pass failed, will retry next hour")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._wake.wait(), timeout=self._poll_interval)

    async def drain_pending(self) -> None:
        """Process every currently-due pending row, looping until none remain.
        The primitive both ``start()`` (H7 full drain) and the periodic loop use;
        also exposed publicly so tests can drive deterministic, ``FrozenClock``-
        paced assertions without waiting on the real poll loop (matches
        ``TimerService.next_fire_at()``/``running`` being public for the same
        reason)."""
        while True:
            async with self._uow() as t:
                due = self._repos.outbox.due(t, self._clock.now())
            if not due:
                return
            for row in due:
                await self._process_row(row)

    async def prune_done(self) -> int:
        """Deletes ``'done'`` outbox rows older than ``retention_days``
        (DR-0002); NEVER touches ``'dead'`` rows. Public so a test (or an
        operator tool) can force a pass without waiting the full hourly
        cadence."""
        cutoff = self._clock.now() - timedelta(days=self._retention_days)
        async with self._uow() as t:
            return self._repos.outbox.prune_done_before(t, cutoff)

    async def _maybe_prune(self) -> None:
        now = self._clock.now()
        if self._last_prune_at is not None and now - self._last_prune_at < _HOUSEKEEPING_INTERVAL:
            return
        await self.prune_done()
        self._last_prune_at = now

    # -------------------------------------------------------------------------
    # Per-row dispatch
    # -------------------------------------------------------------------------

    async def _process_row(self, row: OutboxRow) -> None:
        if row.kind == "notify":
            await self._drain_notify(row)
            return
        if row.kind not in ("run_workflow", "cancel_workflow"):
            # Dispatcher.outbox_row() only ever inserts these two kinds (plus
            # 'notify' above) as lane='drain'/status='pending' -- every other
            # kind is tx-lane and inserts 'done' outright (never reaches this
            # loop's `due()` query at all). Seeing one here would mean the
            # outbox's own lane invariant broke somewhere upstream; dead-letter
            # it immediately rather than retrying forever.
            _log.error("drain loop saw unexpected pending kind %r for %s", row.kind, row.effect_id)
            async with self._uow() as t:
                self._repos.outbox.mark_dead(
                    t, row.effect_id, attempts=row.attempts, last_error=f"unexpected drain-lane kind {row.kind!r}"
                )
            return
        try:
            effect = decode_effect(json.loads(row.payload))
            if row.kind == "run_workflow":
                assert isinstance(effect, RunWorkflow)
                await self._drain_run_workflow(row, effect)
            else:
                assert isinstance(effect, CancelWorkflow)
                await self._drain_cancel_workflow(row, effect)
        except Exception as exc:  # noqa: BLE001 -- genuine drain failure, see module docstring
            # Payload decode failures (a malformed/undecodable row -- e.g. written
            # by a newer binary, then rolled back; the durable outbox replays
            # across restarts by design) are a genuine drain failure exactly like
            # any other unexpected exception here, NOT a special case: they must
            # go through the SAME backoff-then-dead policy so one bad row degrades
            # to a dead row (reconciliation's surface) instead of raising out of
            # `drain_pending()` and starving every row behind it in seq order.
            await self._handle_drain_failure(row, exc)

    # -------------------------------------------------------------------------
    # notify
    # -------------------------------------------------------------------------

    async def _drain_notify(self, row: OutboxRow) -> None:
        # Decode failures get the SAME one-attempt, always-done, never-dead policy
        # as a broadcast exception (seam-a-core.md §D) -- a malformed notify row is
        # no more retryable than a hub that rejects the payload, and notify rows
        # never go dead regardless of cause (duplicate/dropped SSE on crash-replay
        # is harmless -- the UI refetches).
        try:
            effect = decode_effect(json.loads(row.payload))
            assert isinstance(effect, Notify)
            self._hub.broadcast(effect.topic, effect.payload, effect.environment)
        except Exception:  # noqa: BLE001 -- one-attempt, best-effort (seam-a-core.md §D)
            _log.exception(
                "notify drain failed for %s (marked done anyway -- UI reconciles by refetch)", row.effect_id
            )
        async with self._uow() as t:
            self._repos.outbox.mark_done(t, row.effect_id, done_at=self._clock.now())

    # -------------------------------------------------------------------------
    # run_workflow (the Conflict 2 admitter)
    # -------------------------------------------------------------------------

    async def _drain_run_workflow(self, row: OutboxRow, eff: RunWorkflow) -> None:
        new_run_id = str(uuid4())
        try:
            async with self._uow() as t:
                cluster = self._repos.clusters.load(t, eff.cluster_id)
                if cluster is None:
                    raise LookupError(f"RunWorkflow admission: no cluster {eff.cluster_id!r} to resolve provider")
                concrete_name, args = self._dispatch.resolve(eff, cluster)
                definitions = self._engine.definitions
                if concrete_name not in definitions:
                    raise LookupError(f"RunWorkflow admission: unknown workflow definition {concrete_name!r}")
                new_row = WorkflowRunRow(
                    id=new_run_id,
                    workflow=concrete_name,
                    workflow_version=definitions[concrete_name].version,
                    cluster_id=eff.cluster_id,
                    deployment_id=eff.deployment_id,
                    dedupe_key=row.effect_id,
                    args=dict(args),
                    status="pending",
                    cancel_requested=False,
                    failed_step=None,
                    error=None,
                    undo_incomplete=None,
                    initiated_by=None,
                    created_at=self._clock.now(),
                    started_at=None,
                    finished_at=None,
                )
                inserted = self._run_repo.insert_admitted(t, new_row)
        except IntegrityError:
            # ux_wr_one_active, NOT dedupe_key (that's ON CONFLICT DO NOTHING,
            # handled above without raising) -- Conflict 2 rule 3 (DR-0011).
            # `cluster` is guaranteed bound here: the only raise reachable before
            # `insert_admitted` is the `cluster is None` LookupError above, which
            # this except clause does NOT catch.
            await self._handle_active_run_conflict(row, eff, cluster)
            return

        target_run_id = new_run_id
        if not inserted:
            # dedupe_key already existed: a replay of THIS exact effect (H7 --
            # a prior pass admitted the run but crashed before this row could be
            # marked done). Re-derive the run this effect_id already admitted.
            async with self._uow() as t:
                existing = self._run_repo.get_by_dedupe_key(t, row.effect_id)
            target_run_id = existing.id if existing is not None else new_run_id

        async with self._uow() as t:
            self._repos.outbox.mark_done(t, row.effect_id, done_at=self._clock.now())

        try:
            await self._engine.start(target_run_id)
        except Exception:  # noqa: BLE001 -- best-effort hand-off; resume_inflight() is the backstop
            _log.exception("engine hand-off failed for run %s (resume_inflight() is the backstop)", target_run_id)

    async def _handle_active_run_conflict(self, row: OutboxRow, eff: RunWorkflow, cluster: ClusterRecord) -> None:
        """docs/decisions/DR-0011-admitter-wait-and-run-conflict.md, Clause 1 --
        Conflict 2 rule 3's ``ux_wr_one_active`` branch, generalized to three
        branches (see the module docstring's numbered section 2 for the
        surrounding citation trail: coherence-review Conflict 2's amended
        pseudocode + ``core/machine.py``'s
        ``_deployment_deploying_cancel_requested`` comment, both asserting the
        SAME Conflict-12 guarantee the literal ``workflow == 'destroy'`` branch
        alone can't deliver).

        1. ``workflow == 'destroy'`` actively INITIATES a supersede: flip the
           blocking run's ``cancel_requested`` + trip its token (via
           ``engine.cancel``, its real committed API for exactly that pair)
           before deciding to wait.
        2. Any OTHER blocked workflow whose blocking run is ALREADY unwinding
           (``cancel_requested`` already true -- e.g. a ``RunWorkflow(rollback)``
           row seq-ordered directly behind the ``CancelWorkflow(deploy)`` row
           that just flipped that same blocking run, Conflict 12) waits on the
           SAME retry cadence, without touching ``attempts`` -- waiting for a
           run that's already on its way out is not a failure, exactly like the
           destroy branch.
        3. Only a blocking run that is NEITHER being freshly superseded NOR
           already unwinding is a genuine H14 conflict: mark this row done AND,
           in the SAME transaction, INSERT a durable drain-lane ``Notify`` row
           (DR-0011 Clause 2 -- replaces the prior build's direct, unscoped
           broadcast): ``effect_id = "{row.effect_id}#run_conflict"``
           (``ON CONFLICT DO NOTHING``, so a crash between this INSERT and the
           done-mark replays without duplicating the Notify), aggregate
           ``cluster/{cluster_id}@0#0``, ``environment := cluster.environment``
           (drain time IS this Notify's decision time -- DR-0010 extension).
           Delivery then follows the universal one-attempt notify drain
           (``_drain_notify`` above) on a later pass -- one Notify delivery
           path, one audit trail (Conflict 1's "every effect is a row").
        """
        async with self._uow() as t:
            active = self._run_repo.active_for_cluster(t, eff.cluster_id)

        if eff.workflow == "destroy" and active is not None:
            await self._engine.cancel(active.id)

        already_unwinding = active is None or active.cancel_requested or eff.workflow == "destroy"
        if already_unwinding:
            async with self._uow() as t:
                self._repos.outbox.defer(
                    t, row.effect_id, available_at=self._clock.now() + timedelta(seconds=_SUPERSEDE_RETRY_SECONDS)
                )
            return

        now = self._clock.now()
        notify = Notify(
            topic="run_conflict",
            payload={
                "workflow": eff.workflow,
                "cluster_id": eff.cluster_id,
                "deployment_id": eff.deployment_id,
                "blocked_by_run_id": active.id if active is not None else None,
            },
            environment=cluster.environment,
        )
        notify_row = OutboxRow(
            seq=None,
            effect_id=f"{row.effect_id}#run_conflict",
            aggregate_type="cluster",
            aggregate_id=eff.cluster_id,
            to_version=0,
            ordinal=0,
            kind="notify",
            payload=canonical_json(notify),
            lane="drain",
            status="pending",
            attempts=0,
            available_at=now,
            created_at=now,
            done_at=None,
            last_error=None,
        )
        async with self._uow() as t:
            self._repos.outbox.insert_if_absent(t, notify_row)
            self._repos.outbox.mark_done(t, row.effect_id, done_at=now)

    # -------------------------------------------------------------------------
    # cancel_workflow
    # -------------------------------------------------------------------------

    async def _drain_cancel_workflow(self, row: OutboxRow, eff: CancelWorkflow) -> None:
        async with self._uow() as t:
            active = self._run_repo.active_for_cluster(t, eff.cluster_id)
        if active is not None:
            await self._engine.cancel(active.id)
        async with self._uow() as t:
            self._repos.outbox.mark_done(t, row.effect_id, done_at=self._clock.now())

    # -------------------------------------------------------------------------
    # Genuine drain failures
    # -------------------------------------------------------------------------

    async def _handle_drain_failure(self, row: OutboxRow, exc: Exception) -> None:
        attempts = row.attempts + 1
        _log.exception("drain failed for outbox row %s (kind=%s), attempt %d", row.effect_id, row.kind, attempts)
        async with self._uow() as t:
            if attempts >= _DEAD_AT_ATTEMPTS:
                self._repos.outbox.mark_dead(t, row.effect_id, attempts=attempts, last_error=str(exc))
            else:
                delay = _BACKOFF_LADDER[min(attempts - 1, len(_BACKOFF_LADDER) - 1)]
                self._repos.outbox.reschedule(
                    t,
                    row.effect_id,
                    attempts=attempts,
                    available_at=self._clock.now() + timedelta(seconds=delay),
                    last_error=str(exc),
                )
