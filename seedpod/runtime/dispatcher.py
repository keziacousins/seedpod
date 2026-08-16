"""``Dispatcher`` -- the ONLY write path for cluster/deployment state in v2.

Salvaged from docs/design/seam-a-core.md §D (``seedpod2/core/apply.py``'s ``apply()``
free function) per docs/design/coherence-review.md Conflict 3, which renames/relocates
it wholesale: **one component, named ``Dispatcher``, living in
``seedpod/runtime/dispatcher.py`` (not ``core/`` -- it does IO)**, with Seam A's body,
Seam D's name, an optional ``tx`` for same-transaction chaining (engine outcomes,
timer fires, API ``DeployRequested``+``ClusterReady`` chains), and an optional
``record`` for NEW births. Audit derives from ``event.actor``; ``trigger``/
``initiated_by`` columns die (Conflict 11). Effect lanes (tx vs drain) and their
outbox ``status`` on insert are Conflict 2's amended ``EffectKind`` table, already
encoded on ``seedpod/core/effects.py``'s ``EffectKind`` members.

Every ``cluster``/``deployment`` state change in v2 -- API commands, engine step
``emit:``/outcome events, timer fires, reconciler observations -- goes through
``Dispatcher.apply()``. No direct ORM/status writes anywhere else; the repositories
this module drives (``seedpod/data/repositories.py``) never commit -- the
``UnitOfWork`` this module opens (or the caller's, when chaining via ``tx=``) is the
only thing that ever does.

**Birth (``record=``) rows, per docs/decisions/DR-0006-birth-rows-through-dispatcher.md
(RATIFIED -- amends Conflict 3's signature).** ``record=`` supplies a NEW birth as
the caller's FULL ``ClusterRow``/``DeploymentRow`` (owner: ``seedpod/data/
repositories.py``) -- the shape only the row tables carry (``slug``,
``provider_config``, ``node_count``, billing/crypto columns, ...) that the pure
``ClusterRecord``/``DeploymentRecord`` the machine transitions deliberately does NOT
(docs/design/coherence-review.md Conflict 11's record/row split). ``apply()`` narrows
that row to its pure record -- the *exact same* row->record mapping
``ClusterRepository.load``/``DeploymentRepository.load`` use internally
(``seedpod.data.repositories._cluster_record_from_row`` /
``_deployment_record_from_row``, imported here rather than reimplemented, so the two
can never drift) -- before calling ``transition()``; the machine only ever sees
``ClusterRecord``/``DeploymentRecord``, Pillar 1 is untouched. When the resulting
``Persist`` effect is a birth (``expected_version is None``), the row this module
INSERTs is the caller's row with every machine-owned field overlaid from
``Persist.record`` -- **machine wins on every field it owns, never the reverse**;
every row-only column (``slug``, ``provider_config``, ``node_count``,
``created_at``/``updated_at``, billing, crypto, ...) passes through from the
caller's row untouched. **This module never synthesizes a column value** -- row
synthesis (slug minting, ``provider_config`` from rules/presets, ...) is the
API-layer service's job (Round 6's ``ClusterService``/``DeploymentService``); until
that pillar exists, callers (and, for now, tests -- see ``tests/data/``) construct
the row by hand. A birth's ``Persist`` with no ``record=`` row supplied is a caller
bug, not a gap this module papers over -- it raises immediately.

**``poke()``.** ``attach_executor``/``attach_timers`` are late wires (composition-root
only, Conflict 15) that hand this Dispatcher a ``.poke()``-shaped collaborator for
latency hints after a commit; ``apply()`` is fully correct with neither attached --
poking is never load-bearing, only the drain/timer loops' own polling is.
"""

from __future__ import annotations

import contextlib
import dataclasses
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Protocol

from sqlalchemy.orm import Session

from seedpod.core.clock import Clock
from seedpod.core.codec import canonical_json
from seedpod.core.effects import (
    CancelTimer,
    CancelWorkflow,
    Cascade,
    Effect,
    EffectKind,
    Notify,
    Persist,
    RunWorkflow,
    ScheduleTimer,
)
from seedpod.core.events import DeploymentPending, DeployRequested, Event
from seedpod.core.machine import TransitionResult, transition
from seedpod.core.records import ClusterRecord, DeploymentRecord
from seedpod.data.repositories import (
    ClusterRow,
    DeploymentRow,
    OutboxRow,
    Repositories,
    _cluster_record_from_row,
    _deployment_record_from_row,
)
from seedpod.data.uow import UnitOfWork

__all__ = ["Dispatcher", "outbox_row"]

# Conflict 2's amended EffectKind lane table (seedpod/core/effects.py's per-member
# comments, restated here as the Dispatcher's own lookup): Persist/ScheduleTimer/
# CancelTimer/Cascade commit inside the transition transaction (outbox row inserted
# 'done' -- the audit trail law, Conflict 1); Notify/RunWorkflow/CancelWorkflow are
# drain-lane (outbox row inserted 'pending', drained post-commit by the EffectExecutor).
_TX_LANE_KINDS = frozenset(
    {EffectKind.PERSIST, EffectKind.SCHEDULE_TIMER, EffectKind.CANCEL_TIMER, EffectKind.CASCADE}
)

# Cascade's own docstring (seedpod/core/effects.py): "Depth is asserted <= 2." The pure
# core has no IO/recursion to assert this in; the Dispatcher's recursive apply() is the
# one place that can.
#
# DR-0031's escalation uses the full budget, deliberately and exactly:
#   depth 0  apply("deployment", DeployRequested)    -- the birth
#   depth 1  apply("cluster",    DeploymentPending)  -- the escalation below
#   depth 2  apply("deployment", ClusterReady)       -- the cluster's Cascade fan-out
# A third hop would breach the assert, which is the intended ceiling: this escalation is
# a single hop to one sibling aggregate, never a general event bus.
_MAX_CASCADE_DEPTH = 2

# DR-0031: a deployment birth must ALSO tell the deployment's cluster, so an
# already-ACTIVE cluster can answer with ClusterReady instead of leaving the deployment
# in `pending` forever.
#
# Escalation lives here rather than in the calling service because `Dispatcher.apply` is
# the only component that sees both aggregates -- `core/` is pure and a deployment
# transition is handed only the deployment record, so it cannot know whether the cluster
# is already ACTIVE. Doing it here also fixes every birth site at once, which is the
# whole argument DR-0031 chose Option B on: BOTH existing sites
# (`deployment_service.version_update` and `.redeploy`) had the same bug, and a fix each
# caller must remember is one the next caller will miss.
#
# The deployment-side event is TRANSLATED to a cluster-side one rather than forwarded
# (DR-0031 Erratum E1): `tests/core/test_totality.py` pins ClusterEvent and
# DeploymentEvent as disjoint, so no kind may address both aggregates. The cluster table
# decides what to do with the result; for every state except ACTIVE that is `_ignore`,
# making this strictly additive to existing behavior.
def _cluster_escalation(event: Event, record: DeploymentRecord) -> Event | None:
    """The cluster-side event a deployment event escalates to, or None for most."""
    if isinstance(event, DeployRequested):
        return DeploymentPending(at=event.at, actor=event.actor, deployment_id=record.id)
    return None


def outbox_row(
    eff: Effect,
    aggregate: str,
    aggregate_id: str,
    to_version: int,
    ordinal: int,
    *,
    now: datetime,
) -> OutboxRow:
    """Build the ``effects_outbox`` row for one effect of a transition's tuple.
    ``effect_id = "{aggregate_type}/{aggregate_id}@{to_version}#{ordinal}"``
    (coherence-review Conflict 1) -- a pure function of its arguments, so two calls
    with identical inputs (e.g. re-deriving what a past transition wrote) always
    agree. ``payload`` is canonical JSON (``core/codec.canonical_json``, sorted keys).
    Lane/status are Conflict 2's table: tx-lane rows insert ``'done'`` (the audit
    trail IS the fact that this row exists); drain-lane rows insert ``'pending'``."""
    kind = EffectKind(eff.kind)
    lane = "tx" if kind in _TX_LANE_KINDS else "drain"
    status = "done" if lane == "tx" else "pending"
    return OutboxRow(
        seq=None,
        effect_id=f"{aggregate}/{aggregate_id}@{to_version}#{ordinal}",
        aggregate_type=aggregate,
        aggregate_id=aggregate_id,
        to_version=to_version,
        ordinal=ordinal,
        kind=str(kind),
        payload=canonical_json(eff),
        lane=lane,
        status=status,
        attempts=0,
        available_at=now,
        created_at=now,
        done_at=now if status == "done" else None,
        last_error=None,
    )


def _overlay_cluster_row(row: ClusterRow, record: ClusterRecord) -> ClusterRow:
    """The DR-0006 birth INSERT row: ``row`` (the caller's) with every
    machine-owned field -- exactly the set ``_cluster_record_from_row`` reads FROM a
    ``ClusterRow`` -- overlaid from the post-transition ``record``. Row-only columns
    (``slug``, ``provider_config``, ``node_count``, billing/crypto,
    ``created_at``/``updated_at``, ...) pass through from ``row`` untouched."""
    return dataclasses.replace(
        row,
        id=record.id,
        name=record.name,
        origin=record.origin,
        environment=record.environment,
        status=record.state.value,
        pre_destroy_state=record.pre_destroy_state.value if record.pre_destroy_state else None,
        version=record.version,
        provider=record.provider,
        provider_resources=record.provider_resources,
        public_ip=record.public_ip,
        kubeconfig_ref=record.kubeconfig_ref,
        failure_reason=record.failure_reason,
        expires_at=record.expires_at,
    )


def _overlay_deployment_row(row: DeploymentRow, record: DeploymentRecord) -> DeploymentRow:
    """The deployment twin of ``_overlay_cluster_row``."""
    return dataclasses.replace(
        row,
        id=record.id,
        cluster_id=record.cluster_id,
        environment=record.environment,
        status=record.state.value,
        version=record.version,
        manifest_version=record.manifest_version,
        spec_ref=record.spec_ref,
        resolved_images=record.resolved_images,
        superseded_by=record.superseded_by,
        failure_reason=record.failure_reason,
    )


class _Pokeable(Protocol):
    def poke(self) -> None: ...


class Dispatcher:
    """``Dispatcher(uow, repos, clock)`` -- the ONLY write path for cluster/deployment
    state in v2. ``apply()`` is fully correct before ``attach_executor``/
    ``attach_timers`` are ever called (they only add a latency hint on top)."""

    def __init__(self, uow: UnitOfWork, repos: Repositories, clock: Clock) -> None:
        self._uow = uow
        self.repos = repos
        self._clock = clock
        self._executor: _Pokeable | None = None
        self._timers: _Pokeable | None = None

    def attach_executor(self, executor: _Pokeable) -> None:
        """Late wire (composition root only, Conflict 15): gives ``apply()`` a
        ``.poke()`` latency hint after a commit. Correctness never depends on this
        being called -- the executor's own polling loop is the backstop."""
        self._executor = executor

    def attach_timers(self, timers: _Pokeable) -> None:
        """Late wire, same discipline as ``attach_executor`` -- the ``TimerService``'s
        own polling loop is the backstop."""
        self._timers = timers

    @contextlib.asynccontextmanager
    async def _tx(self, tx: Session | None) -> AsyncIterator[Session]:
        """``tx is None`` -> open (and commit-on-exit / rollback-on-error) a fresh
        transaction via the injected ``UnitOfWork``; ``tx is not None`` -> chain on
        the caller's already-open transaction, touching neither commit nor rollback
        (the caller owns that lifecycle -- engine outcomes, timer fires, API
        multi-event chains, and this method's own recursive ``Cascade`` calls)."""
        if tx is not None:
            yield tx
            return
        async with self._uow() as t:
            yield t

    async def apply(
        self,
        aggregate: str,
        aggregate_id: str,
        event: Event,
        *,
        tx: Session | None = None,
        record: ClusterRow | DeploymentRow | None = None,
        _cascade_depth: int = 0,
    ) -> TransitionResult:
        """Run the pure ``transition()`` for one aggregate and durably commit every
        effect it returns. ``record=`` supplies a NEW birth as the FULL row DTO
        (``ClusterRow`` | ``DeploymentRow``, DR-0006) in place of the normal
        ``repos.<agg>.load()`` read; it is narrowed to the pure record for
        ``transition()``, and the birth ``Persist`` INSERTs that same row with its
        machine-owned fields overlaid from the post-transition record (module
        docstring). ``Ignore`` (``result.effects == ()``) writes NOTHING: no version
        bump, no audit row, no outbox row, no poke. ``StaleVersion`` from a losing CAS
        propagates to the caller unretried -- the re-read/re-decide loop (<=3
        attempts) is the caller's job (docs/design/seam-a-core.md §D)."""
        assert _cascade_depth <= _MAX_CASCADE_DEPTH, (
            f"Cascade recursion exceeded depth {_MAX_CASCADE_DEPTH} "
            f"({aggregate}/{aggregate_id})"
        )
        async with self._tx(tx) as t:
            rec = self._narrow(aggregate, record) if record is not None else self._load(t, aggregate, aggregate_id)
            result = transition(rec, event)
            if not result.effects:
                return result  # Ignore: nothing written, no SSE, no audit, no poke
            for ordinal, eff in enumerate(result.effects):
                row = outbox_row(
                    eff, aggregate, result.record.id, result.record.version, ordinal, now=self._clock.now()
                )
                match eff:
                    case Persist():
                        self._persist(t, aggregate, eff, birth_row=record)
                        self._audit(t, aggregate, rec, result, event)
                    case ScheduleTimer():
                        self.repos.timers.upsert(t, eff, row.effect_id)
                    case CancelTimer():
                        self.repos.timers.delete(t, eff)
                    case Cascade():
                        for dep in self.repos.deployments.deployments_in(
                            t, eff.cluster_id, eff.where_state, eff.except_id
                        ):
                            await self.apply(
                                "deployment", dep.id, eff.event, tx=t, _cascade_depth=_cascade_depth + 1
                            )
                    case Notify() | RunWorkflow() | CancelWorkflow():
                        pass  # outbox_row() already set lane='drain', status='pending'
                self.repos.outbox.insert(t, row)

            # DR-0031: tell the deployment's cluster. Ordered AFTER the effect loop on
            # purpose -- the deployment's own Persist has already run, so the row is
            # visible to the cluster Cascade's `deployments_in(... PENDING)` query that is
            # about to look for it. Reversing these two would fan out to a deployment that
            # does not exist yet.
            if aggregate == "deployment" and isinstance(result.record, DeploymentRecord):
                escalation = _cluster_escalation(event, result.record)
                if escalation is not None:
                    await self.apply(
                        "cluster", result.record.cluster_id, escalation,
                        tx=t, _cascade_depth=_cascade_depth + 1,
                    )
        self._poke()
        return result

    # -------------------------------------------------------------------------
    # Internals
    # -------------------------------------------------------------------------

    def _poke(self) -> None:
        if self._executor is not None:
            self._executor.poke()
        if self._timers is not None:
            self._timers.poke()

    def _narrow(self, aggregate: str, row: ClusterRow | DeploymentRow) -> ClusterRecord | DeploymentRecord:
        """DR-0006: narrow a birth's full row DTO to the pure record ``transition()``
        accepts, via the SAME mapping ``ClusterRepository.load``/
        ``DeploymentRepository.load`` use internally (imported, not reimplemented)."""
        if aggregate == "cluster":
            return _cluster_record_from_row(row)  # type: ignore[arg-type]
        return _deployment_record_from_row(row)  # type: ignore[arg-type]

    def _load(self, t: Session, aggregate: str, aggregate_id: str) -> ClusterRecord | DeploymentRecord:
        rec = (
            self.repos.clusters.load(t, aggregate_id)
            if aggregate == "cluster"
            else self.repos.deployments.load(t, aggregate_id)
        )
        if rec is None:
            raise LookupError(f"no {aggregate} {aggregate_id!r} to load and transition")
        return rec

    def _persist(
        self,
        t: Session,
        aggregate: str,
        eff: Persist,
        *,
        birth_row: ClusterRow | DeploymentRow | None,
    ) -> None:
        if eff.expected_version is None:
            if birth_row is None:
                raise ValueError(
                    f"birth Persist for {aggregate} {eff.record.id!r} has no record= row "
                    "-- the caller must supply the full ClusterRow/DeploymentRow (DR-0006); "
                    "the Dispatcher never synthesizes one"
                )
            if aggregate == "cluster":
                self.repos.clusters.insert(t, _overlay_cluster_row(birth_row, eff.record))  # type: ignore[arg-type]
            else:
                self.repos.deployments.insert(
                    t, _overlay_deployment_row(birth_row, eff.record)  # type: ignore[arg-type]
                )
            return
        if aggregate == "cluster":
            self.repos.clusters.persist(t, eff.record, eff.expected_version, clock=self._clock)  # type: ignore[arg-type]
        else:
            self.repos.deployments.persist(t, eff.record, eff.expected_version, clock=self._clock)  # type: ignore[arg-type]

    def _audit(
        self,
        t: Session,
        aggregate: str,
        rec: ClusterRecord | DeploymentRecord,
        result: TransitionResult,
        event: Event,
    ) -> None:
        """One state-audit row per non-Ignore transition, same transaction as the
        Persist (docs/design/coherence-review.md Conflict 11): ``actor``/``created_at``
        derive from ``event.actor``/``event.at`` -- never a stamped-at-write-time
        clock read (a late-firing timer or a post-crash outbox replay must audit with
        the EVENT's time, matching ``ClusterStateAuditRepository.add``'s docstring).
        The full ``event`` (not just its class name) is passed through so ``add()``
        can derive ``reason``/``context`` mechanically
        (docs/decisions/DR-0007-audit-reason-context-derivation.md) -- the Dispatcher
        itself never inspects or invents either."""
        from_state = rec.state.value
        to_state = result.record.state.value
        if aggregate == "cluster":
            self.repos.cluster_state_audits.add(
                t,
                cluster_id=result.record.id,
                from_state=from_state,
                to_state=to_state,
                event=event,
                actor=event.actor,
                at=event.at,
            )
        else:
            self.repos.deployment_state_audits.add(
                t,
                deployment_id=result.record.id,
                cluster_id=result.record.cluster_id,  # type: ignore[union-attr]
                from_state=from_state,
                to_state=to_state,
                event=event,
                actor=event.actor,
                at=event.at,
            )
