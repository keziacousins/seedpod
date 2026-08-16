"""``WorkflowRunRepository`` / ``WorkflowStepRepository`` / ``OutboxRepository`` --
the Pillar 2 subset of Seam D's repository layer, against the FINAL schema in
``seedpod/data/migrations/0001_initial.sql`` (``workflow_runs``, ``workflow_steps``,
``effects_outbox``; docs/design/coherence-review.md Conflicts 1 and 4) -- PLUS the
"repos-machine" subset extending it: ``ClusterRepository``, ``DeploymentRepository``,
``ClusterStateAuditRepository``, ``DeploymentStateAuditRepository``,
``DeploymentAuditRepository``, ``TimerRepository`` (Round-4 runtime spine;
docs/design/coherence-review.md Conflict 11 -- the ``clusters``/``deployments``/audit
DDL authority -- and Conflict 1 -- the ``timers`` DDL) -- PLUS the "repos-feature"
subset extending IT: ``ApiKeyRepository``, ``SecretRepository``,
``SecretAuditRepository``, ``PresetRepository``, ``SnapshotRepository``, against the
``api_keys``/``secrets``/``secret_audits``/``deployment_presets``/``snapshots`` DDL
that docs/design/seam-d-foundation.md's Decision 6 wrote and coherence-review.md
leaves standing unamended ("stand as Seam D wrote them"). ``ApiKeyRepository`` and
``PresetRepository`` salvage their method surface from
``reference-code/seedpod/seedpod/data/repositories.py``'s
``SQLAlchemyAPIKeyRepository`` (443-512) and ``SQLAlchemyDeploymentPresetRepository``
(844-965); ``SecretRepository`` salvages ``SQLAlchemySecretRepository`` (534-593) but
closes v1 gotcha 4 (see its own docstring); ``SecretAuditRepository`` salvages
``SQLAlchemySecretAuditRepository`` (752-802); ``SnapshotRepository`` is new
plumbing behind v1's inline ORM queries in
``reference-code/seedpod/seedpod/services/snapshot_service.py``
(``list_snapshots``/``get_snapshot``/``delete_snapshot`` 711-750, the ``Snapshot(...)``
construction at 365-389) -- v1 had no dedicated repository class for snapshots, only
ad hoc ``session.query(Snapshot)`` calls inside the service.

Session-in, DTO-out (docs/design/seam-d-foundation.md Decision 6): every method
takes an already-open ``sqlalchemy.orm.Session`` (the ``tx`` an
``async with uow() as tx:`` block yields -- see ``seedpod/data/uow.py``) and returns
plain dataclass DTOs, never ORM-mapped objects. **No method here calls
``session.commit()``** -- the ``UnitOfWork`` owns that, once, on clean exit.
Timestamps a method needs to *generate* (as opposed to ones the caller already
computed and is persisting) are produced from an injected ``Clock`` parameter, never
``datetime.now()`` -- see ``seedpod/core/clock.py``.

Queries use SQLAlchemy Core (``sqlalchemy.text`` over the ``Session``'s connection),
not a second, drift-prone mirror of the schema as declarative/Core ``Table`` objects
-- ``0001_initial.sql`` stays the single place column names and types are declared.

**The machine-side repositories' row/record split** (``ClusterRow``/``DeploymentRow``
vs ``ClusterRecord``/``DeploymentRecord``): the pure Pillar-1 records
(``seedpod/core/records.py``) carry only the fields the ``transition()`` table
touches -- they are NOT a 1:1 mirror of the ``clusters``/``deployments`` tables,
which also carry descriptive/billing/crypto columns (``slug``, ``provider_config``,
``node_count``, ``encrypted_kubeconfig``, ``cost_per_hour``, ...) the machine never
reads or writes. So each machine repository exposes two shapes: a full-row DTO
(``ClusterRow``/``DeploymentRow``) for births (``insert``) and row-level reads
(``get``/``get_by_slug``/list helpers), and ``load``/``persist`` methods that speak
``ClusterRecord``/``DeploymentRecord`` directly for the (later-work) Dispatcher's
``Persist`` effect -- ``persist`` is a pure CAS ``UPDATE`` over exactly the
record-mapped columns (never touching the row-only columns), raising ``StaleVersion``
(``seedpod/core/machine.py``) on a zero-rowcount match; ``insert`` is the separate,
one-time birth INSERT with no CAS and no audit-writing/ID-derivation inside it (both
were v1 behaviors this pillar deliberately drops per
docs/design/seam-d-foundation.md Decision 6's "two changes" note).
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Final, Literal

from sqlalchemy import bindparam, text
from sqlalchemy.orm import Session

from seedpod.core.clock import Clock
from seedpod.core.codec import canonical_json
from seedpod.core.effects import CancelTimer, Notify, ScheduleTimer
from seedpod.core.events import Event
from seedpod.core.machine import StaleVersion
from seedpod.core.records import (
    TERMINAL_STATES,
    ClusterRecord,
    ClusterState,
    DeploymentRecord,
    DeploymentState,
    Origin,
)
from seedpod.services.crypto import CryptoService

__all__ = [
    "WorkflowRunRow",
    "WorkflowStepRow",
    "OutboxRow",
    "WorkflowRunRepository",
    "WorkflowStepRepository",
    "OutboxRepository",
    "ACTIVE_RUN_STATUSES",
    "ClusterRow",
    "ClusterRepository",
    "DeploymentRow",
    "DeploymentRepository",
    "ClusterStateAuditRow",
    "ClusterStateAuditRepository",
    "DeploymentStateAuditRow",
    "DeploymentStateAuditRepository",
    "DeploymentAuditRow",
    "DeploymentAuditRepository",
    "TimerRow",
    "TimerRepository",
    "Repositories",
    "ApiKeyRow",
    "ApiKeyRepository",
    "SecretRow",
    "SecretMetadataRow",
    "SecretRepository",
    "SecretAuditRow",
    "SecretAuditRepository",
    "PresetRow",
    "PresetRepository",
    "SnapshotRow",
    "SnapshotRepository",
]

# workflow_runs.status values that hold the one-active-run-per-cluster slot
# (ux_wr_one_active partial unique index, H14) -- 'blocked' included per
# docs/design/coherence-review.md Conflict 5 rule 5 ("resume adopts 'blocked'
# runs exactly like 'running' ones").
ACTIVE_RUN_STATUSES: Final[tuple[str, ...]] = ("pending", "running", "blocked", "compensating")


class _Unset:
    def __repr__(self) -> str:  # pragma: no cover -- debugging aid only
        return "UNSET"


UNSET: Final[Any] = _Unset()  # sentinel: "field not provided", distinct from an explicit None


# ---------------------------------------------------------------------------
# datetime <-> ISO-8601 'Z' (matches seedpod/core/codec.py's convention; naive
# datetimes are banned everywhere in v2, docs/design/seam-a-core.md §B)
# ---------------------------------------------------------------------------


def _iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        raise ValueError("repositories cannot persist a naive datetime -- naive datetimes are banned in v2")
    # Fixed millisecond precision (docs/design/seam-d-foundation.md Decision 6
    # Conventions example: '2026-07-12T09:00:00.000Z') -- variable-width output
    # (no fractional part when microsecond == 0) would make lexicographic TEXT
    # comparisons/ORDER BY over these columns unreliable across rows of differing
    # precision.
    return dt.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _iso_or_none(dt: datetime | None) -> str | None:
    return _iso(dt) if dt is not None else None


def _parse(value: str) -> datetime:
    text_value = value[:-1] + "+00:00" if value.endswith("Z") else value
    dt = datetime.fromisoformat(text_value)
    if dt.tzinfo is None:
        raise ValueError(f"read a naive datetime {value!r} back from the database")
    return dt


def _parse_or_none(value: str | None) -> datetime | None:
    return _parse(value) if value is not None else None


def _dump(value: Mapping[str, Any] | Sequence[Any] | None) -> str | None:
    """Dump a nullable JSON column. Same ``Mapping``-coercion discipline as
    ``_dump_nn`` (``dict(value)`` rather than handing a bare ``Mapping`` straight to
    ``json.dumps``, which raises ``TypeError`` on anything that isn't literally a
    ``dict`` -- e.g. a ``MappingProxyType`` or another frozen-record mapping shape) so
    every JSON column -- required or nullable -- serializes any ``Mapping`` the same
    way. (``cluster_state_audits``/``deployment_state_audits.context`` bypasses this --
    it is ``canonical_json(event)`` verbatim, already a JSON string; see
    ``ClusterStateAuditRepository.add``, DR-0007.)"""
    if value is None:
        return None
    return json.dumps(dict(value) if isinstance(value, Mapping) else list(value), sort_keys=True)


def _load(value: str | None) -> Any | None:
    return json.loads(value) if value is not None else None


def _dump_nn(value: Mapping[str, Any] | Sequence[Any], *, kind: Literal["map", "seq"]) -> str:
    """Dump a NOT NULL JSON column. ``kind`` is the column's declared shape, supplied
    by the caller (there is no reliable way to recover it from ``value`` alone: the
    ``Mapping[str, T] = ()`` empty-mapping sentinel -- docs/design/coherence-review.md
    Conflict 11 -- is a bare ``tuple`` at runtime, indistinguishable from a genuinely
    empty ``Sequence`` field's ``()``). ``kind="map"`` always dumps ``dict(value)``, so
    a real mapping AND the ``()`` sentinel both round-trip to ``'{}'``; ``kind="seq"``
    always dumps ``list(value)``, so an empty sequence round-trips to ``'[]'``."""
    return json.dumps(dict(value) if kind == "map" else list(value), sort_keys=True)


# ---------------------------------------------------------------------------
# workflow_runs
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class WorkflowRunRow:
    """One ``workflow_runs`` row (session-in/DTO-out; mirrors 0001_initial.sql exactly)."""

    id: str
    workflow: str  # CONCRETE definition name (Conflict 13), not the abstract verb
    workflow_version: int
    cluster_id: str
    deployment_id: str | None
    dedupe_key: str | None
    args: Mapping[str, Any]
    status: str  # pending|running|blocked|compensating|succeeded|failed|cancelled
    cancel_requested: bool
    failed_step: str | None
    error: Mapping[str, Any] | None
    undo_incomplete: Sequence[str] | None
    initiated_by: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


def _run_params(row: WorkflowRunRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "workflow": row.workflow,
        "workflow_version": row.workflow_version,
        "cluster_id": row.cluster_id,
        "deployment_id": row.deployment_id,
        "dedupe_key": row.dedupe_key,
        "args": _dump(row.args) or "{}",
        "status": row.status,
        "cancel_requested": int(row.cancel_requested),
        "failed_step": row.failed_step,
        "error": _dump(row.error),
        "undo_incomplete": _dump(row.undo_incomplete),
        "initiated_by": row.initiated_by,
        "created_at": _iso(row.created_at),
        "started_at": _iso_or_none(row.started_at),
        "finished_at": _iso_or_none(row.finished_at),
    }


def _run_from_mapping(m: Mapping[str, Any]) -> WorkflowRunRow:
    return WorkflowRunRow(
        id=m["id"],
        workflow=m["workflow"],
        workflow_version=m["workflow_version"],
        cluster_id=m["cluster_id"],
        deployment_id=m["deployment_id"],
        dedupe_key=m["dedupe_key"],
        args=_load(m["args"]) or {},
        status=m["status"],
        cancel_requested=bool(m["cancel_requested"]),
        failed_step=m["failed_step"],
        error=_load(m["error"]),
        undo_incomplete=_load(m["undo_incomplete"]),
        initiated_by=m["initiated_by"],
        created_at=_parse(m["created_at"]),
        started_at=_parse_or_none(m["started_at"]),
        finished_at=_parse_or_none(m["finished_at"]),
    )


class WorkflowRunRepository:
    """``workflow_runs``: session-in, DTO-out, never commits.

    Conflict 2 assigns run *admission* (INSERT on ``RunWorkflow`` drain) to the
    EffectExecutor's run-admitter, which lives in the runtime spine, not here --
    this repository only gives it (and the engine, and tests) the primitives:
    insert an already-decided row, read it back, list/flip status.
    """

    def insert(self, session: Session, row: WorkflowRunRow) -> None:
        session.execute(
            text(
                """
                INSERT INTO workflow_runs
                    (id, workflow, workflow_version, cluster_id, deployment_id, dedupe_key,
                     args, status, cancel_requested, failed_step, error, undo_incomplete,
                     initiated_by, created_at, started_at, finished_at)
                VALUES
                    (:id, :workflow, :workflow_version, :cluster_id, :deployment_id, :dedupe_key,
                     :args, :status, :cancel_requested, :failed_step, :error, :undo_incomplete,
                     :initiated_by, :created_at, :started_at, :finished_at)
                """
            ),
            _run_params(row),
        )

    def insert_admitted(self, session: Session, row: WorkflowRunRow) -> bool:
        """Run-admission INSERT (docs/design/coherence-review.md Conflict 2):
        ``ON CONFLICT(dedupe_key) DO NOTHING`` makes admission idempotent under H7
        crash-replay (``dedupe_key`` = the ``RunWorkflow`` effect's ``effect_id``) --
        returns ``True`` iff THIS call actually inserted a new row (rowcount 1);
        ``False`` means ``dedupe_key`` already existed (a replay, not an error) --
        the caller re-reads the existing row via ``get_by_dedupe_key``. A conflict
        on the DIFFERENT ``ux_wr_one_active`` partial unique index (another run
        already live for this cluster) is NOT covered by this statement's conflict
        target and propagates as a normal ``sqlalchemy.exc.IntegrityError`` -- the
        caller's job (Conflict 2 rule 3, the destroy-supersede / run-conflict
        branch)."""
        result = session.execute(
            text(
                """
                INSERT INTO workflow_runs
                    (id, workflow, workflow_version, cluster_id, deployment_id, dedupe_key,
                     args, status, cancel_requested, failed_step, error, undo_incomplete,
                     initiated_by, created_at, started_at, finished_at)
                VALUES
                    (:id, :workflow, :workflow_version, :cluster_id, :deployment_id, :dedupe_key,
                     :args, :status, :cancel_requested, :failed_step, :error, :undo_incomplete,
                     :initiated_by, :created_at, :started_at, :finished_at)
                ON CONFLICT (dedupe_key) DO NOTHING
                """
            ),
            _run_params(row),
        )
        return result.rowcount > 0

    def get(self, session: Session, run_id: str) -> WorkflowRunRow | None:
        row = session.execute(
            text("SELECT * FROM workflow_runs WHERE id = :id"), {"id": run_id}
        ).mappings().first()
        return _run_from_mapping(row) if row is not None else None

    def get_by_dedupe_key(self, session: Session, dedupe_key: str) -> WorkflowRunRow | None:
        row = session.execute(
            text("SELECT * FROM workflow_runs WHERE dedupe_key = :dedupe_key"),
            {"dedupe_key": dedupe_key},
        ).mappings().first()
        return _run_from_mapping(row) if row is not None else None

    def list_by_status(self, session: Session, statuses: Sequence[str]) -> list[WorkflowRunRow]:
        if not statuses:
            return []
        rows = session.execute(
            text("SELECT * FROM workflow_runs WHERE status IN :statuses").bindparams(
                bindparam("statuses", expanding=True)
            ),
            {"statuses": list(statuses)},
        ).mappings().all()
        return [_run_from_mapping(r) for r in rows]

    def resumable(self, session: Session) -> list[WorkflowRunRow]:
        """Runs `WorkflowEngine.resume_inflight()` (Conflict 2's engine surface) must
        adopt: every status that still holds the one-active-run slot, 'blocked'
        included (Conflict 5 rule 5)."""
        return self.list_by_status(session, ACTIVE_RUN_STATUSES)

    def active_for_cluster(self, session: Session, cluster_id: str) -> WorkflowRunRow | None:
        row = session.execute(
            text(
                "SELECT * FROM workflow_runs WHERE cluster_id = :cluster_id "
                "AND status IN :statuses"
            ).bindparams(bindparam("statuses", expanding=True)),
            {"cluster_id": cluster_id, "statuses": list(ACTIVE_RUN_STATUSES)},
        ).mappings().first()
        return _run_from_mapping(row) if row is not None else None

    def request_cancel(self, session: Session, run_id: str) -> bool:
        """Flips ``cancel_requested`` for a still-active run (Conflict 2's
        ``engine.cancel(run_id)`` surface: "flips cancel_requested + trips the
        in-process token" -- the in-process token is the engine's job, not this
        repository's). Returns whether a row was actually flipped."""
        result = session.execute(
            text(
                "UPDATE workflow_runs SET cancel_requested = 1 "
                "WHERE id = :id AND status IN :statuses"
            ).bindparams(bindparam("statuses", expanding=True)),
            {"id": run_id, "statuses": list(ACTIVE_RUN_STATUSES)},
        )
        return result.rowcount > 0

    def update(
        self,
        session: Session,
        run_id: str,
        *,
        status: str | _Unset = UNSET,
        failed_step: str | None | _Unset = UNSET,
        error: Mapping[str, Any] | None | _Unset = UNSET,
        undo_incomplete: Sequence[str] | None | _Unset = UNSET,
        started_at: datetime | None | _Unset = UNSET,
        finished_at: datetime | None | _Unset = UNSET,
    ) -> None:
        """Partial update of the mutable run columns. Only fields actually passed
        (i.e. not ``UNSET``) are written -- distinguishes "leave alone" from
        "set to NULL"."""
        sets: dict[str, Any] = {}
        if not isinstance(status, _Unset):
            sets["status"] = status
        if not isinstance(failed_step, _Unset):
            sets["failed_step"] = failed_step
        if not isinstance(error, _Unset):
            sets["error"] = _dump(error)
        if not isinstance(undo_incomplete, _Unset):
            sets["undo_incomplete"] = _dump(undo_incomplete)
        if not isinstance(started_at, _Unset):
            sets["started_at"] = _iso_or_none(started_at)
        if not isinstance(finished_at, _Unset):
            sets["finished_at"] = _iso_or_none(finished_at)
        if not sets:
            return
        assignments = ", ".join(f"{k} = :{k}" for k in sets)
        session.execute(
            text(f"UPDATE workflow_runs SET {assignments} WHERE id = :id"),
            {**sets, "id": run_id},
        )

    def list_all(self, session: Session) -> list[WorkflowRunRow]:
        """Every workflow run, newest first -- ``GET /api/workflows``'s read side
        (Round 6, api-edge; docs/decisions/DR-0003, ui-contract obligation 7).
        Same discipline as ``ClusterRepository.list_all``/``DeploymentRepository.
        list_all``: one unfiltered SELECT, no per-filter-combination method."""
        rows = session.execute(
            text("SELECT * FROM workflow_runs ORDER BY created_at DESC")
        ).mappings().all()
        return [_run_from_mapping(r) for r in rows]


# ---------------------------------------------------------------------------
# workflow_steps
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class WorkflowStepRow:
    """One ``workflow_steps`` row -- one row per step INSTANCE, keyed by the
    materialized ``step_path`` (Conflict 4: ``'create'`` | ``'wave[1].apply'``)."""

    run_id: str
    step_path: str
    verb: str
    status: str  # running|gating|succeeded|failed|failed_continued|cancelled
    attempt: int
    interrupted_count: int
    params: Mapping[str, Any]
    notes: Mapping[str, Any]
    output: Mapping[str, Any] | None
    undo_status: str | None  # done|failed|skipped
    error: Mapping[str, Any] | None
    started_at: datetime
    finished_at: datetime | None


def _step_params(row: WorkflowStepRow) -> dict[str, Any]:
    return {
        "run_id": row.run_id,
        "step_path": row.step_path,
        "verb": row.verb,
        "status": row.status,
        "attempt": row.attempt,
        "interrupted_count": row.interrupted_count,
        "params": _dump(row.params) or "{}",
        "notes": _dump(row.notes) or "{}",
        "output": _dump(row.output),
        "undo_status": row.undo_status,
        "error": _dump(row.error),
        "started_at": _iso(row.started_at),
        "finished_at": _iso_or_none(row.finished_at),
    }


def _step_from_mapping(m: Mapping[str, Any]) -> WorkflowStepRow:
    return WorkflowStepRow(
        run_id=m["run_id"],
        step_path=m["step_path"],
        verb=m["verb"],
        status=m["status"],
        attempt=m["attempt"],
        interrupted_count=m["interrupted_count"],
        params=_load(m["params"]) or {},
        notes=_load(m["notes"]) or {},
        output=_load(m["output"]),
        undo_status=m["undo_status"],
        error=_load(m["error"]),
        started_at=_parse(m["started_at"]),
        finished_at=_parse_or_none(m["finished_at"]),
    )


class WorkflowStepRepository:
    """``workflow_steps``: session-in, DTO-out, never commits."""

    def insert(self, session: Session, row: WorkflowStepRow) -> None:
        session.execute(
            text(
                """
                INSERT INTO workflow_steps
                    (run_id, step_path, verb, status, attempt, interrupted_count,
                     params, notes, output, undo_status, error, started_at, finished_at)
                VALUES
                    (:run_id, :step_path, :verb, :status, :attempt, :interrupted_count,
                     :params, :notes, :output, :undo_status, :error, :started_at, :finished_at)
                """
            ),
            _step_params(row),
        )

    def get(self, session: Session, run_id: str, step_path: str) -> WorkflowStepRow | None:
        row = session.execute(
            text(
                "SELECT * FROM workflow_steps WHERE run_id = :run_id AND step_path = :step_path"
            ),
            {"run_id": run_id, "step_path": step_path},
        ).mappings().first()
        return _step_from_mapping(row) if row is not None else None

    def list_for_run(self, session: Session, run_id: str) -> list[WorkflowStepRow]:
        rows = session.execute(
            text("SELECT * FROM workflow_steps WHERE run_id = :run_id ORDER BY started_at"),
            {"run_id": run_id},
        ).mappings().all()
        return [_step_from_mapping(r) for r in rows]

    def update(
        self,
        session: Session,
        run_id: str,
        step_path: str,
        *,
        status: str | _Unset = UNSET,
        attempt: int | _Unset = UNSET,
        interrupted_count: int | _Unset = UNSET,
        notes: Mapping[str, Any] | _Unset = UNSET,
        output: Mapping[str, Any] | None | _Unset = UNSET,
        undo_status: str | None | _Unset = UNSET,
        error: Mapping[str, Any] | None | _Unset = UNSET,
        finished_at: datetime | None | _Unset = UNSET,
    ) -> None:
        """Partial update of one step instance's mutable columns (retries bump
        ``attempt``/``interrupted_count`` on the SAME row -- the primary key is
        ``(run_id, step_path)``, not ``(run_id, step_path, attempt)``)."""
        sets: dict[str, Any] = {}
        if not isinstance(status, _Unset):
            sets["status"] = status
        if not isinstance(attempt, _Unset):
            sets["attempt"] = attempt
        if not isinstance(interrupted_count, _Unset):
            sets["interrupted_count"] = interrupted_count
        if not isinstance(notes, _Unset):
            sets["notes"] = _dump(notes) or "{}"
        if not isinstance(output, _Unset):
            sets["output"] = _dump(output)
        if not isinstance(undo_status, _Unset):
            sets["undo_status"] = undo_status
        if not isinstance(error, _Unset):
            sets["error"] = _dump(error)
        if not isinstance(finished_at, _Unset):
            sets["finished_at"] = _iso_or_none(finished_at)
        if not sets:
            return
        assignments = ", ".join(f"{k} = :{k}" for k in sets)
        session.execute(
            text(
                f"UPDATE workflow_steps SET {assignments} "
                "WHERE run_id = :run_id AND step_path = :step_path"
            ),
            {**sets, "run_id": run_id, "step_path": step_path},
        )


# ---------------------------------------------------------------------------
# effects_outbox
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class OutboxRow:
    """One ``effects_outbox`` row, shaped exactly like 0001_initial.sql. ``seq`` is
    ``None`` for a not-yet-inserted row (AUTOINCREMENT fills it in)."""

    seq: int | None
    effect_id: str
    aggregate_type: str  # cluster|deployment|run
    aggregate_id: str
    to_version: int  # 0 for engine-origin ("run") rows
    ordinal: int
    kind: str  # persist|schedule_timer|cancel_timer|run_workflow|cancel_workflow|cascade|notify
    payload: str  # canonical JSON (core/codec.canonical_json or an equivalent encoding)
    lane: str  # tx|drain
    status: str  # pending|done|dead
    attempts: int
    available_at: datetime
    created_at: datetime
    done_at: datetime | None
    last_error: str | None


def _outbox_params(row: OutboxRow) -> dict[str, Any]:
    return {
        "effect_id": row.effect_id,
        "aggregate_type": row.aggregate_type,
        "aggregate_id": row.aggregate_id,
        "to_version": row.to_version,
        "ordinal": row.ordinal,
        "kind": row.kind,
        "payload": row.payload,
        "lane": row.lane,
        "status": row.status,
        "attempts": row.attempts,
        "available_at": _iso(row.available_at),
        "created_at": _iso(row.created_at),
        "done_at": _iso_or_none(row.done_at),
        "last_error": row.last_error,
    }


def _outbox_from_mapping(m: Mapping[str, Any]) -> OutboxRow:
    return OutboxRow(
        seq=m["seq"],
        effect_id=m["effect_id"],
        aggregate_type=m["aggregate_type"],
        aggregate_id=m["aggregate_id"],
        to_version=m["to_version"],
        ordinal=m["ordinal"],
        kind=m["kind"],
        payload=m["payload"],
        lane=m["lane"],
        status=m["status"],
        attempts=m["attempts"],
        available_at=_parse(m["available_at"]),
        created_at=_parse(m["created_at"]),
        done_at=_parse_or_none(m["done_at"]),
        last_error=m["last_error"],
    )


class OutboxRepository:
    """``effects_outbox``: session-in, DTO-out, never commits.

    Generic ``insert`` covers rows the Dispatcher (runtime spine, later work) builds
    from Pillar-1 ``Effect``s. ``insert_run_notify`` is this pillar's own helper:
    ``ctx.progress``/``job_started``/``job_completed``/``job_failed`` write drain-lane
    ``Notify`` rows *directly* -- they are engine-origin facts, not effects of a
    Pillar-1 transition, so they bypass ``Dispatcher.apply`` entirely
    (docs/design/coherence-review.md Conflict 3's "effect_id = run/{run_id}@{step_path}#{n}").
    """

    def insert(self, session: Session, row: OutboxRow) -> None:
        session.execute(
            text(
                """
                INSERT INTO effects_outbox
                    (effect_id, aggregate_type, aggregate_id, to_version, ordinal, kind,
                     payload, lane, status, attempts, available_at, created_at, done_at, last_error)
                VALUES
                    (:effect_id, :aggregate_type, :aggregate_id, :to_version, :ordinal, :kind,
                     :payload, :lane, :status, :attempts, :available_at, :created_at, :done_at, :last_error)
                """
            ),
            _outbox_params(row),
        )

    def insert_if_absent(self, session: Session, row: OutboxRow) -> bool:
        """``ON CONFLICT(effect_id) DO NOTHING`` -- makes a deterministically-keyed
        insert idempotent under crash-replay (docs/decisions/DR-0011-admitter-wait-
        and-run-conflict.md clause 2's ``run_conflict`` Notify:
        ``effect_id = "{blocked_row.effect_id}#run_conflict"``, replayed on a later
        drain pass after a crash between done-marking and notify-drain). Returns
        ``True`` iff THIS call actually inserted a new row -- same
        rowcount-tells-the-story discipline as ``WorkflowRunRepository.
        insert_admitted``."""
        result = session.execute(
            text(
                """
                INSERT INTO effects_outbox
                    (effect_id, aggregate_type, aggregate_id, to_version, ordinal, kind,
                     payload, lane, status, attempts, available_at, created_at, done_at, last_error)
                VALUES
                    (:effect_id, :aggregate_type, :aggregate_id, :to_version, :ordinal, :kind,
                     :payload, :lane, :status, :attempts, :available_at, :created_at, :done_at, :last_error)
                ON CONFLICT (effect_id) DO NOTHING
                """
            ),
            _outbox_params(row),
        )
        return result.rowcount > 0

    def get(self, session: Session, effect_id: str) -> OutboxRow | None:
        row = session.execute(
            text("SELECT * FROM effects_outbox WHERE effect_id = :effect_id"),
            {"effect_id": effect_id},
        ).mappings().first()
        return _outbox_from_mapping(row) if row is not None else None

    def list_for_aggregate(
        self, session: Session, aggregate_type: str, aggregate_id: str
    ) -> list[OutboxRow]:
        rows = session.execute(
            text(
                "SELECT * FROM effects_outbox WHERE aggregate_type = :aggregate_type "
                "AND aggregate_id = :aggregate_id ORDER BY seq"
            ),
            {"aggregate_type": aggregate_type, "aggregate_id": aggregate_id},
        ).mappings().all()
        return [_outbox_from_mapping(r) for r in rows]

    def insert_run_notify(
        self,
        session: Session,
        *,
        run_id: str,
        step_path: str,
        ordinal: int,
        topic: str,
        payload: Mapping[str, Any],
        clock: Clock,
    ) -> None:
        """Drain-lane ``Notify`` row written directly by the engine (Conflict 3),
        ``effect_id = "run/{run_id}@{step_path}#{ordinal}"``, ``aggregate_type='run'``,
        ``to_version=0``. Reuses ``core.effects.Notify`` + ``core.codec.canonical_json``
        so the payload encoding matches every other outbox row's convention exactly."""
        now = clock.now()
        notify = Notify(topic=topic, payload=payload, environment=None)
        row = OutboxRow(
            seq=None,
            effect_id=f"run/{run_id}@{step_path}#{ordinal}",
            aggregate_type="run",
            aggregate_id=run_id,
            to_version=0,
            ordinal=ordinal,
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
        self.insert(session, row)

    def due(self, session: Session, now: datetime) -> list[OutboxRow]:
        """Drain-lane rows ready to process: ``status='pending' AND available_at <=
        now``, ORDERED BY ``seq`` -- ``seedpod/runtime/effect_executor.py``'s poll
        query. Seq order is load-bearing (docs/design/coherence-review.md Conflict
        12): a ``CancelWorkflow`` row emitted before a ``RunWorkflow`` row in the
        SAME transition's effect tuple must drain in that same order, so a
        cancelled deploy run's rollback admission waits behind its cancel."""
        rows = session.execute(
            text("SELECT * FROM effects_outbox WHERE status = 'pending' AND available_at <= :now ORDER BY seq"),
            {"now": _iso(now)},
        ).mappings().all()
        return [_outbox_from_mapping(r) for r in rows]

    def mark_done(self, session: Session, effect_id: str, *, done_at: datetime) -> None:
        session.execute(
            text("UPDATE effects_outbox SET status = 'done', done_at = :done_at WHERE effect_id = :effect_id"),
            {"done_at": _iso(done_at), "effect_id": effect_id},
        )

    def mark_dead(self, session: Session, effect_id: str, *, attempts: int, last_error: str) -> None:
        """Genuine drain failure exhausted (``attempts >= 8``, docs/decisions/
        DR-0002): dead rows are reconciliation's surface and are NEVER
        auto-pruned (see ``prune_done_before``)."""
        session.execute(
            text(
                "UPDATE effects_outbox SET status = 'dead', attempts = :attempts, last_error = :last_error "
                "WHERE effect_id = :effect_id"
            ),
            {"attempts": attempts, "last_error": last_error, "effect_id": effect_id},
        )

    def reschedule(
        self, session: Session, effect_id: str, *, attempts: int, available_at: datetime, last_error: str
    ) -> None:
        """A genuine drain failure that hasn't hit the dead threshold yet: bump
        ``attempts``, push ``available_at`` per the backoff ladder, record
        ``last_error`` -- ``status`` stays ``'pending'`` (docs/design/seam-a-core.md
        §D's Notify/backoff paragraph)."""
        session.execute(
            text(
                "UPDATE effects_outbox SET attempts = :attempts, available_at = :available_at, "
                "last_error = :last_error WHERE effect_id = :effect_id"
            ),
            {"attempts": attempts, "available_at": _iso(available_at), "last_error": last_error, "effect_id": effect_id},
        )

    def defer(self, session: Session, effect_id: str, *, available_at: datetime) -> None:
        """The destroy-supersede wait (docs/design/coherence-review.md Conflict 2
        rule 3): push ``available_at`` out WITHOUT touching ``attempts`` -- waiting
        for a superseded run to reach terminal is not a failure."""
        session.execute(
            text("UPDATE effects_outbox SET available_at = :available_at WHERE effect_id = :effect_id"),
            {"available_at": _iso(available_at), "effect_id": effect_id},
        )

    def prune_done_before(self, session: Session, cutoff: datetime) -> int:
        """Hourly housekeeping (docs/decisions/DR-0002-design-lock-ratification.md):
        deletes ``'done'`` rows older than the retention window. ``'dead'`` rows are
        NEVER auto-pruned here -- they are reconciliation's surface."""
        result = session.execute(
            text("DELETE FROM effects_outbox WHERE status = 'done' AND done_at < :cutoff"),
            {"cutoff": _iso(cutoff)},
        )
        return result.rowcount

    def count_by_status(self, session: Session, status: str) -> int:
        """Row count for one ``status`` value -- ``/health/detailed``'s
        ``executor.pending_outbox``/``executor.dead_outbox`` (Round 6, api-edge;
        DR-0003: "``dead_outbox`` ... the one number that says 'reconciliation has
        inherited work'"). A plain ``COUNT(*)``, same discipline as
        ``ClusterRepository.count``."""
        return session.execute(
            text("SELECT COUNT(*) FROM effects_outbox WHERE status = :status"), {"status": status}
        ).scalar_one()


# ---------------------------------------------------------------------------
# clusters
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ClusterRow:
    """One ``clusters`` row, shaped exactly like 0001_initial.sql -- the full row,
    a strict superset of what ``ClusterRecord`` (the pure Pillar-1 DTO) carries. Used
    for births (``insert``) and every row-level read; ``ClusterRepository.load``
    narrows a row down to the ``ClusterRecord`` the machine actually transitions."""

    id: str
    name: str
    slug: str
    origin: Origin
    environment: str
    repository: str | None
    branch: str | None
    status: str  # ClusterState value; NO CHECK -- Pillar 1 is the sole authority
    pre_destroy_state: str | None  # ClusterState value | None
    version: int
    provider: str
    provider_config: Mapping[str, Any]  # provisioning INPUTS
    provider_resources: Mapping[str, str]  # provisioning OUTPUTS
    dns_hostname: str | None
    dns_zone: str | None
    dns_record_id: str | None  # migration 0002 (DR-0034); written by cluster.store_dns_record
    public_ip: str | None
    node_count: int
    encrypted_kubeconfig: str | None
    kubeconfig_key_class: str | None
    kubeconfig_ref: str | None
    cost_per_hour: float
    total_cost: float
    consecutive_health_failures: int
    failure_reason: str | None
    last_reconciled_at: datetime | None
    created_at: datetime
    updated_at: datetime
    expires_at: datetime | None


def _cluster_params(row: ClusterRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "name": row.name,
        "slug": row.slug,
        "origin": str(row.origin),
        "environment": row.environment,
        "repository": row.repository,
        "branch": row.branch,
        "status": row.status,
        "pre_destroy_state": row.pre_destroy_state,
        "version": row.version,
        "provider": row.provider,
        "provider_config": _dump_nn(row.provider_config, kind="map"),
        "provider_resources": _dump_nn(row.provider_resources, kind="map"),
        "dns_hostname": row.dns_hostname,
        "dns_zone": row.dns_zone,
        "dns_record_id": row.dns_record_id,
        "public_ip": row.public_ip,
        "node_count": row.node_count,
        "encrypted_kubeconfig": row.encrypted_kubeconfig,
        "kubeconfig_key_class": row.kubeconfig_key_class,
        "kubeconfig_ref": row.kubeconfig_ref,
        "cost_per_hour": row.cost_per_hour,
        "total_cost": row.total_cost,
        "consecutive_health_failures": row.consecutive_health_failures,
        "failure_reason": row.failure_reason,
        "last_reconciled_at": _iso_or_none(row.last_reconciled_at),
        "created_at": _iso(row.created_at),
        "updated_at": _iso(row.updated_at),
        "expires_at": _iso_or_none(row.expires_at),
    }


def _cluster_from_mapping(m: Mapping[str, Any]) -> ClusterRow:
    return ClusterRow(
        id=m["id"],
        name=m["name"],
        slug=m["slug"],
        origin=Origin(m["origin"]),
        environment=m["environment"],
        repository=m["repository"],
        branch=m["branch"],
        status=m["status"],
        pre_destroy_state=m["pre_destroy_state"],
        version=m["version"],
        provider=m["provider"],
        provider_config=_load(m["provider_config"]) or {},
        provider_resources=_load(m["provider_resources"]) or {},
        dns_hostname=m["dns_hostname"],
        dns_zone=m["dns_zone"],
        dns_record_id=m["dns_record_id"],
        public_ip=m["public_ip"],
        node_count=m["node_count"],
        encrypted_kubeconfig=m["encrypted_kubeconfig"],
        kubeconfig_key_class=m["kubeconfig_key_class"],
        kubeconfig_ref=m["kubeconfig_ref"],
        cost_per_hour=m["cost_per_hour"],
        total_cost=m["total_cost"],
        consecutive_health_failures=m["consecutive_health_failures"],
        failure_reason=m["failure_reason"],
        last_reconciled_at=_parse_or_none(m["last_reconciled_at"]),
        created_at=_parse(m["created_at"]),
        updated_at=_parse(m["updated_at"]),
        expires_at=_parse_or_none(m["expires_at"]),
    )


def _cluster_record_from_row(row: ClusterRow) -> ClusterRecord:
    """Narrow a full ``ClusterRow`` down to the pure ``ClusterRecord`` the machine
    transitions -- the inverse of what ``ClusterRepository.persist`` writes back."""
    return ClusterRecord(
        id=row.id,
        name=row.name,
        state=ClusterState(row.status),
        version=row.version,
        provider=row.provider,
        environment=row.environment,
        origin=row.origin,
        expires_at=row.expires_at,
        public_ip=row.public_ip,
        kubeconfig_ref=row.kubeconfig_ref,
        provider_resources=row.provider_resources,
        pre_destroy_state=ClusterState(row.pre_destroy_state) if row.pre_destroy_state else None,
        failure_reason=row.failure_reason,
    )


class ClusterRepository:
    """``clusters``: session-in, DTO-out, never commits.

    Salvaged query surface from ``reference-code/seedpod/seedpod/data/repositories.py``
    (``SQLAlchemyClusterRepository``), adapted to the 0001 columns per
    docs/design/coherence-review.md Conflict 11: ``get_cluster`` (lines 143-146),
    ``get_cluster_by_slug`` (148-165, now driven by ``TERMINAL_STATES`` rather than
    the hardcoded ``['destroyed', 'failed']`` list), ``find_active_cluster_by_branch``
    (335-347), ``find_clusters_by_branch`` (349-358). ``create_cluster`` (205-257) is
    salvaged as plain ``insert`` -- per docs/design/seam-d-foundation.md Decision 6,
    the ``session.commit()``, the ``cluster_uuid``-from-``provider_config`` ID
    derivation, and the inline audit-record write are ALL dropped (ids are uuid4
    always; the Dispatcher writes the birth audit row itself, same transaction).
    """

    def get(self, session: Session, cluster_id: str) -> ClusterRow | None:
        row = session.execute(
            text("SELECT * FROM clusters WHERE id = :id"), {"id": cluster_id}
        ).mappings().first()
        return _cluster_from_mapping(row) if row is not None else None

    def get_by_slug(self, session: Session, slug: str, *, active_only: bool = True) -> ClusterRow | None:
        """v1's ``get_cluster_by_slug``: with ``active_only`` (default), only a LIVE
        cluster's slug is returned -- i.e. ``status NOT IN TERMINAL_STATES``, the same
        predicate ``ux_clusters_slug_live`` enforces, driven by the Pillar-1 constant
        (never a string literal) so this filter cannot drift from the unique index."""
        if active_only:
            row = session.execute(
                text(
                    "SELECT * FROM clusters WHERE slug = :slug AND status NOT IN :terminal "
                    "ORDER BY created_at DESC"
                ).bindparams(bindparam("terminal", expanding=True)),
                {"slug": slug, "terminal": list(TERMINAL_STATES)},
            ).mappings().first()
        else:
            row = session.execute(
                text("SELECT * FROM clusters WHERE slug = :slug ORDER BY created_at DESC"),
                {"slug": slug},
            ).mappings().first()
        return _cluster_from_mapping(row) if row is not None else None

    def get_by_id_or_slug(
        self, session: Session, id_or_slug: str, *, active_only: bool = True
    ) -> ClusterRow | None:
        """The API's id-or-slug lookup convention (docs/design/coherence-review.md
        Conflict 11: "routes accept id-or-slug for lookups"). ``id`` is always a
        globally unique uuid4, so an id match is tried first and returned regardless
        of ``active_only`` (looking a specific cluster up by its real id is never
        ambiguous, even if it is a destroyed one); only the slug fallback -- which CAN
        be ambiguous across a destroyed-and-recreated slug -- honors ``active_only``."""
        by_id = self.get(session, id_or_slug)
        if by_id is not None:
            return by_id
        return self.get_by_slug(session, id_or_slug, active_only=active_only)

    def load(self, session: Session, cluster_id: str) -> ClusterRecord | None:
        """The Dispatcher's read side (docs/design/coherence-review.md Conflict 3:
        ``rec = record or await self.repos.load(t, aggregate, aggregate_id)``) --
        narrows the full row down to the pure record ``transition()`` accepts."""
        row = self.get(session, cluster_id)
        return _cluster_record_from_row(row) if row is not None else None

    def insert(self, session: Session, row: ClusterRow) -> None:
        """Birth INSERT. No CAS (there is no prior version to race), no audit write,
        no id derivation -- both dropped per docs/design/seam-d-foundation.md Decision
        6's "two changes" (the Dispatcher writes the birth audit row itself, in the
        same transaction, via ``ClusterStateAuditRepository.add``)."""
        session.execute(
            text(
                """
                INSERT INTO clusters
                    (id, name, slug, origin, environment, repository, branch, status,
                     pre_destroy_state, version, provider, provider_config, provider_resources,
                     dns_hostname, dns_zone, dns_record_id, public_ip, node_count, encrypted_kubeconfig,
                     kubeconfig_key_class, kubeconfig_ref, cost_per_hour, total_cost,
                     consecutive_health_failures, failure_reason, last_reconciled_at,
                     created_at, updated_at, expires_at)
                VALUES
                    (:id, :name, :slug, :origin, :environment, :repository, :branch, :status,
                     :pre_destroy_state, :version, :provider, :provider_config, :provider_resources,
                     :dns_hostname, :dns_zone, :dns_record_id, :public_ip, :node_count, :encrypted_kubeconfig,
                     :kubeconfig_key_class, :kubeconfig_ref, :cost_per_hour, :total_cost,
                     :consecutive_health_failures, :failure_reason, :last_reconciled_at,
                     :created_at, :updated_at, :expires_at)
                """
            ),
            _cluster_params(row),
        )

    def persist(
        self, session: Session, record: ClusterRecord, expected_version: int, *, clock: Clock
    ) -> None:
        """CAS UPDATE over exactly the columns ``ClusterRecord`` carries -- the
        row-only columns (``slug``, ``provider_config``, ``node_count``, billing,
        crypto, ...) are untouched, matching the pure machine's own field scope.
        ``rowcount == 0`` means another writer's ``Persist`` won the race since
        ``expected_version`` was read -- raises ``StaleVersion`` (docs/design/
        coherence-review.md Conflict 11 / seam-a-core.md's CAS-retry rule; the caller
        re-reads and re-decides, bounded to 3 attempts)."""
        result = session.execute(
            text(
                """
                UPDATE clusters SET
                    name = :name, origin = :origin, status = :status,
                    pre_destroy_state = :pre_destroy_state, provider = :provider,
                    environment = :environment, provider_resources = :provider_resources,
                    public_ip = :public_ip, kubeconfig_ref = :kubeconfig_ref,
                    failure_reason = :failure_reason, expires_at = :expires_at,
                    updated_at = :updated_at, version = version + 1
                WHERE id = :id AND version = :expected_version
                """
            ),
            {
                "id": record.id,
                "name": record.name,
                "origin": str(record.origin),
                "status": record.state.value,
                "pre_destroy_state": record.pre_destroy_state.value if record.pre_destroy_state else None,
                "provider": record.provider,
                "environment": record.environment,
                "provider_resources": _dump_nn(record.provider_resources, kind="map"),
                "public_ip": record.public_ip,
                "kubeconfig_ref": record.kubeconfig_ref,
                "failure_reason": record.failure_reason,
                "expires_at": _iso_or_none(record.expires_at),
                "updated_at": _iso(clock.now()),
                "expected_version": expected_version,
            },
        )
        if result.rowcount == 0:
            raise StaleVersion(
                f"cluster {record.id}: CAS UPDATE matched no row at version {expected_version}"
            )

    def set_health_failures(
        self, session: Session, cluster_id: str, count: int, *, clock: Clock
    ) -> None:
        """The health poll's dedicated write path for ``consecutive_health_failures``
        (v1 ``job_manager.py:634`` ``_reset_health_failure_count`` -- ``count=0`` --
        and ``:647`` ``_increment_health_failure_count``, both a raw
        ``cluster.consecutive_health_failures = ...; session.commit()`` bypass) --
        docs/design/seam-d-foundation.md Decision 6's ``clusters`` DDL comment:
        "mutated ONLY via a dedicated repo method -- the bypass is gone, not the
        column". This counter is health-poll bookkeeping running parallel to, not
        part of, the ``ClusterRecord`` the state machine transitions (Pillar 1 never
        reads or writes it), so this is a plain UPDATE -- no CAS, no ``version``
        bump -- unlike ``persist``."""
        session.execute(
            text(
                "UPDATE clusters SET consecutive_health_failures = :count, updated_at = :updated_at "
                "WHERE id = :id"
            ),
            {"count": count, "updated_at": _iso(clock.now()), "id": cluster_id},
        )

    def update_cost(self, session: Session, cluster_id: str, total_cost: float, *, clock: Clock) -> None:
        """The cost-accrual job's dedicated write path for ``total_cost`` -- v1's
        ``update_cluster_cost`` (``reference-code/seedpod/seedpod/data/repositories.py``
        lines 318-326), salvaged as the same shape as ``set_health_failures``: a
        row-only column (docs/design/seam-d-foundation.md Decision 6's ``clusters``
        DDL comment: "kept (UI cost display; frozen at destroy)") that the pure
        machine never touches, so this is a plain UPDATE, no CAS, no ``version``
        bump. "Frozen at destroy" is the CALLER's job (stop calling this once the
        cluster is terminal) -- this method has no state-awareness of its own."""
        session.execute(
            text("UPDATE clusters SET total_cost = :total_cost, updated_at = :updated_at WHERE id = :id"),
            {"total_cost": total_cost, "updated_at": _iso(clock.now()), "id": cluster_id},
        )

    def set_last_reconciled_at(
        self, session: Session, cluster_ids: Sequence[str], *, clock: Clock
    ) -> None:
        """The reconciler's dedicated write path for ``last_reconciled_at``
        (``seedpod/runtime/reconciliation.py`` -- docs/design/seam-d-foundation.md:104
        ("promoted out of provider_config (staleness check reads it)") makes this a
        bookkeeping column, not a state change: it never rides the Dispatcher). Same
        discipline as ``set_health_failures``/``update_cost``: a plain UPDATE, no
        CAS, no ``version`` bump -- the pure machine never reads this column. A
        no-op for an empty ``cluster_ids`` (v1 salvage note: a provider that covered
        zero clusters this pass has nothing to stamp).

        Deliberately does NOT touch ``updated_at``, unlike its
        ``set_health_failures``/``update_cost`` siblings (2026-08-13). Those record a
        real change to the row; a reconciliation pass that observed no drift changed
        nothing, and stamping ``updated_at`` for it made that column mean "when the
        sweep last ran" for EVERY cluster rather than "when this cluster last
        changed". Every row ended up carrying a millisecond-identical ``updated_at``,
        including clusters destroyed days earlier -- and ``api/routers/clusters.py``
        serialises it straight to the SPA, where it reads as "changed just now".
        ``last_reconciled_at`` is the column that exists to answer "when did we last
        look", so let it be the only one that moves."""
        if not cluster_ids:
            return
        session.execute(
            text(
                "UPDATE clusters SET last_reconciled_at = :now WHERE id IN :ids"
            ).bindparams(bindparam("ids", expanding=True)),
            {"now": _iso(clock.now()), "ids": list(cluster_ids)},
        )

    def find_active_cluster_by_branch(
        self, session: Session, repository: str, branch: str, environment: str
    ) -> ClusterRow | None:
        row = session.execute(
            text(
                "SELECT * FROM clusters WHERE repository = :repository AND branch = :branch "
                "AND environment = :environment AND status = :status"
            ),
            {
                "repository": repository,
                "branch": branch,
                "environment": environment,
                "status": ClusterState.ACTIVE.value,
            },
        ).mappings().first()
        return _cluster_from_mapping(row) if row is not None else None

    def find_clusters_by_branch(
        self, session: Session, repository: str, branch: str, environment: str
    ) -> list[ClusterRow]:
        rows = session.execute(
            text(
                "SELECT * FROM clusters WHERE repository = :repository AND branch = :branch "
                "AND environment = :environment ORDER BY created_at DESC"
            ),
            {"repository": repository, "branch": branch, "environment": environment},
        ).mappings().all()
        return [_cluster_from_mapping(r) for r in rows]

    def list_by_status(self, session: Session, statuses: Sequence[str]) -> list[ClusterRow]:
        if not statuses:
            return []
        rows = session.execute(
            text("SELECT * FROM clusters WHERE status IN :statuses ORDER BY created_at DESC").bindparams(
                bindparam("statuses", expanding=True)
            ),
            {"statuses": list(statuses)},
        ).mappings().all()
        return [_cluster_from_mapping(r) for r in rows]

    def list_expired(self, session: Session, now: datetime) -> list[ClusterRow]:
        """Clusters whose TTL is past due and still LIVE (``TERMINAL_STATES``-driven,
        never a literal) -- the reconciler's/backstop's read side; the actual
        transition is ``TtlExpired`` via the Dispatcher, never a direct write here."""
        rows = session.execute(
            text(
                "SELECT * FROM clusters WHERE expires_at IS NOT NULL AND expires_at <= :now "
                "AND status NOT IN :terminal ORDER BY expires_at"
            ).bindparams(bindparam("terminal", expanding=True)),
            {"now": _iso(now), "terminal": list(TERMINAL_STATES)},
        ).mappings().all()
        return [_cluster_from_mapping(r) for r in rows]

    def list_all(self, session: Session) -> list[ClusterRow]:
        """Every cluster, newest first -- ``ClusterService.list``'s unfiltered read
        side (Round 6, app-services). ``show_destroyed``/``status=active`` (ui-
        contract obligation 6) are plain Python filters over this at the service
        layer, the same discipline ``list_by_status``/``list_expired`` already use
        for their own (narrower) predicates -- adding a method per filter
        combination here would just be this same SELECT with a different WHERE."""
        rows = session.execute(
            text("SELECT * FROM clusters ORDER BY created_at DESC")
        ).mappings().all()
        return [_cluster_from_mapping(r) for r in rows]

    def set_expires_at(self, session: Session, cluster_id: str, expires_at: datetime, *, clock: Clock) -> None:
        """The TTL-extend endpoint's dedicated write path (``POST /api/clusters/
        {id}/extend``, Round 6; DR-0009's "becomes race-proof end to end without
        the API layer knowing anything about it"): a plain UPDATE, no CAS, no
        ``version`` bump -- same discipline as ``set_health_failures``/
        ``update_cost``/``set_last_reconciled_at``. Extending TTL is not a
        ``ClusterState`` transition (Pillar 1's table has no such event), so it
        does not go through ``Dispatcher.apply()``; the caller (``ClusterService.
        extend``) is also responsible for re-arming the ``ttl`` timer via
        ``TimerRepository.upsert`` in the SAME transaction, which is what makes
        the re-arm race-free per DR-0009."""
        session.execute(
            text("UPDATE clusters SET expires_at = :expires_at, updated_at = :updated_at WHERE id = :id"),
            {"expires_at": _iso(expires_at), "updated_at": _iso(clock.now()), "id": cluster_id},
        )

    def set_kubeconfig(
        self, session: Session, cluster_id: str, *, encrypted_kubeconfig: str, key_class: str, clock: Clock
    ) -> bool:
        """The provisioning workflow's dedicated write path for ``encrypted_kubeconfig``/
        ``kubeconfig_key_class`` (``cluster.store_kubeconfig``, DR-0022 -- replaces
        ``kubeconfig.store``). Same discipline as ``set_health_failures``/
        ``update_cost``/``set_expires_at``/``set_last_reconciled_at``: a row-only
        crypto-bookkeeping pair the pure machine never reads (only ``kubeconfig_ref``,
        the opaque handle this pair backs, is machine-owned -- set later by
        ``ProvisionSucceeded`` via the Dispatcher/``persist``), so this is a plain
        UPDATE, no CAS, no ``version`` bump. The caller is responsible for encrypting
        via ``CryptoService`` BEFORE calling this (DR-0008: no crypto-heavy work inside
        an open ``uow()`` transaction) -- this method only ever writes ciphertext plus
        its stamped key_class, never plaintext.

        Returns ``True`` iff the row still existed (rowcount 1) -- same
        rowcount-tells-the-story idiom as ``WorkflowRunRepository.insert_admitted``
        et al. Unlike its bookkeeping siblings, a lost write here is NOT harmless
        (the caller mints ``kubeconfig_ref`` unconditionally right after this call,
        and that ref is what ``ProvisionSucceeded`` carries onward) -- so the
        caller (``cluster.store_kubeconfig``) checks this and raises rather than
        minting a ref for a write that silently affected zero rows."""
        result = session.execute(
            text(
                "UPDATE clusters SET encrypted_kubeconfig = :encrypted_kubeconfig, "
                "kubeconfig_key_class = :key_class, updated_at = :updated_at WHERE id = :id"
            ),
            {
                "encrypted_kubeconfig": encrypted_kubeconfig,
                "key_class": key_class,
                "updated_at": _iso(clock.now()),
                "id": cluster_id,
            },
        )
        return result.rowcount > 0

    def set_dns_record(
        self, session: Session, cluster_id: str, *, hostname: str | None, zone: str, record_id: str, clock: Clock
    ) -> bool:
        """The provisioning workflow's dedicated write path for the DNS triple
        (``cluster.store_dns_record``, DR-0034 decision 4) -- the exact shape and
        discipline of ``set_kubeconfig`` above: three row-only columns the pure
        machine never reads, so a plain UPDATE, no CAS, no ``version`` bump.

        Returns ``True`` iff the row still existed (rowcount 1). As with
        ``set_kubeconfig``, a lost write here is NOT harmless: the record exists at
        Cloudflare and this triple is the only thing that will ever point at it
        again, so the caller raises rather than reporting success (and the
        provision workflow's compensation then deletes the record it created)."""
        result = session.execute(
            text(
                "UPDATE clusters SET dns_hostname = :hostname, dns_zone = :zone, "
                "dns_record_id = :record_id, updated_at = :updated_at WHERE id = :id"
            ),
            {
                "hostname": hostname,
                "zone": zone,
                "record_id": record_id,
                "updated_at": _iso(clock.now()),
                "id": cluster_id,
            },
        )
        return result.rowcount > 0

    def count(self, session: Session) -> int:
        """Total cluster row count -- ``/health/detailed``'s ``database.cluster_count``
        (Round 6, api-edge; docs/decisions/DR-0003). A plain ``COUNT(*)`` rather than
        ``len(list_all(...))`` so a health poll never drags full rows across the wire
        for a number."""
        return session.execute(text("SELECT COUNT(*) FROM clusters")).scalar_one()


# ---------------------------------------------------------------------------
# deployments
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DeploymentRow:
    """One ``deployments`` row, shaped exactly like 0001_initial.sql -- the full row,
    a strict superset of what ``DeploymentRecord`` carries."""

    id: str
    cluster_id: str
    environment: str
    status: str  # DeploymentState value; NO CHECK
    version: int
    manifest_version: str
    spec_ref: str | None
    resolved_images: Mapping[str, str]
    superseded_by: str | None
    deployed_by: str | None
    failure_reason: str | None
    created_at: datetime
    updated_at: datetime


def _deployment_row_params(row: DeploymentRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "cluster_id": row.cluster_id,
        "environment": row.environment,
        "status": row.status,
        "version": row.version,
        "manifest_version": row.manifest_version,
        "spec_ref": row.spec_ref,
        "resolved_images": _dump_nn(row.resolved_images, kind="map"),
        "superseded_by": row.superseded_by,
        "deployed_by": row.deployed_by,
        "failure_reason": row.failure_reason,
        "created_at": _iso(row.created_at),
        "updated_at": _iso(row.updated_at),
    }


def _deployment_row_from_mapping(m: Mapping[str, Any]) -> DeploymentRow:
    return DeploymentRow(
        id=m["id"],
        cluster_id=m["cluster_id"],
        environment=m["environment"],
        status=m["status"],
        version=m["version"],
        manifest_version=m["manifest_version"],
        spec_ref=m["spec_ref"],
        resolved_images=_load(m["resolved_images"]) or {},
        superseded_by=m["superseded_by"],
        deployed_by=m["deployed_by"],
        failure_reason=m["failure_reason"],
        created_at=_parse(m["created_at"]),
        updated_at=_parse(m["updated_at"]),
    )


def _deployment_record_from_row(row: DeploymentRow) -> DeploymentRecord:
    return DeploymentRecord(
        id=row.id,
        cluster_id=row.cluster_id,
        state=DeploymentState(row.status),
        version=row.version,
        environment=row.environment,
        manifest_version=row.manifest_version,
        spec_ref=row.spec_ref,
        resolved_images=row.resolved_images,
        superseded_by=row.superseded_by,
        failure_reason=row.failure_reason,
    )


class DeploymentRepository:
    """``deployments``: session-in, DTO-out, never commits.

    Salvaged from ``reference-code/seedpod/seedpod/data/repositories.py``
    (``SQLAlchemyDeploymentRepository``): ``get_deployments_for_cluster``
    (lines 392-398, kept as ``list_for_cluster``) and ``create_deployment``
    (399-406, salvaged as plain ``insert`` -- ``session.commit()`` dropped, same as
    ``ClusterRepository``). ``deployments_in`` is new-in-v2 machinery the Dispatcher's
    ``Cascade`` effect needs (docs/design/coherence-review.md Conflict 3): it resolves
    the set of sibling deployments a cluster-level event fans out to.
    """

    def get(self, session: Session, deployment_id: str) -> DeploymentRow | None:
        row = session.execute(
            text("SELECT * FROM deployments WHERE id = :id"), {"id": deployment_id}
        ).mappings().first()
        return _deployment_row_from_mapping(row) if row is not None else None

    def load(self, session: Session, deployment_id: str) -> DeploymentRecord | None:
        """The Dispatcher's read side, mirroring ``ClusterRepository.load``."""
        row = self.get(session, deployment_id)
        return _deployment_record_from_row(row) if row is not None else None

    def insert(self, session: Session, row: DeploymentRow) -> None:
        """Birth INSERT -- no CAS, no audit write (dropped per docs/design/
        seam-d-foundation.md Decision 6, same discipline as ``ClusterRepository.insert``)."""
        session.execute(
            text(
                """
                INSERT INTO deployments
                    (id, cluster_id, environment, status, version, manifest_version,
                     spec_ref, resolved_images, superseded_by, deployed_by, failure_reason,
                     created_at, updated_at)
                VALUES
                    (:id, :cluster_id, :environment, :status, :version, :manifest_version,
                     :spec_ref, :resolved_images, :superseded_by, :deployed_by, :failure_reason,
                     :created_at, :updated_at)
                """
            ),
            _deployment_row_params(row),
        )

    def persist(
        self, session: Session, record: DeploymentRecord, expected_version: int, *, clock: Clock
    ) -> None:
        """CAS UPDATE over exactly the columns ``DeploymentRecord`` carries. ``rowcount
        == 0`` -> ``StaleVersion``, same discipline as ``ClusterRepository.persist``."""
        result = session.execute(
            text(
                """
                UPDATE deployments SET
                    status = :status, environment = :environment, manifest_version = :manifest_version,
                    spec_ref = :spec_ref, resolved_images = :resolved_images,
                    superseded_by = :superseded_by, failure_reason = :failure_reason,
                    updated_at = :updated_at, version = version + 1
                WHERE id = :id AND version = :expected_version
                """
            ),
            {
                "id": record.id,
                "status": record.state.value,
                "environment": record.environment,
                "manifest_version": record.manifest_version,
                "spec_ref": record.spec_ref,
                "resolved_images": _dump_nn(record.resolved_images, kind="map"),
                "superseded_by": record.superseded_by,
                "failure_reason": record.failure_reason,
                "updated_at": _iso(clock.now()),
                "expected_version": expected_version,
            },
        )
        if result.rowcount == 0:
            raise StaleVersion(
                f"deployment {record.id}: CAS UPDATE matched no row at version {expected_version}"
            )

    def list_for_cluster(self, session: Session, cluster_id: str) -> list[DeploymentRow]:
        rows = session.execute(
            text("SELECT * FROM deployments WHERE cluster_id = :cluster_id ORDER BY created_at DESC"),
            {"cluster_id": cluster_id},
        ).mappings().all()
        return [_deployment_row_from_mapping(r) for r in rows]

    def active_for_cluster(self, session: Session, cluster_id: str) -> DeploymentRow | None:
        """The latest-per-cluster helper: the one deployment currently serving traffic
        (``DeploymentState.ACTIVE`` is a Pillar-1 invariant: at most one per cluster)."""
        row = session.execute(
            text(
                "SELECT * FROM deployments WHERE cluster_id = :cluster_id AND status = :status "
                "ORDER BY created_at DESC LIMIT 1"
            ),
            {"cluster_id": cluster_id, "status": DeploymentState.ACTIVE.value},
        ).mappings().first()
        return _deployment_row_from_mapping(row) if row is not None else None

    def list_all(self, session: Session, *, show_history: bool = False) -> list[DeploymentRow]:
        """Every deployment, newest first -- ``DeploymentService.list``'s global
        read side (``GET /api/deployments?show_history=true``, ui-contract).
        ``show_history=False`` (the default) hides purely-historical rows
        (``superseded``/``destroyed``) so the list reads as "what's live or
        pending", matching v1's default list view; ``show_history=True`` returns
        every row, same discipline as ``ClusterRepository.list_all`` pushing the
        show/hide filter to a plain Python predicate rather than a new SELECT per
        combination."""
        rows = session.execute(
            text("SELECT * FROM deployments ORDER BY created_at DESC")
        ).mappings().all()
        all_rows = [_deployment_row_from_mapping(r) for r in rows]
        if show_history:
            return all_rows
        hidden = {DeploymentState.SUPERSEDED.value, DeploymentState.DESTROYED.value}
        return [r for r in all_rows if r.status not in hidden]

    def count(self, session: Session) -> int:
        """Total deployment row count -- ``/health/detailed``'s
        ``database.deployment_count`` (Round 6, api-edge; DR-0003). See
        ``ClusterRepository.count``'s docstring for why this is a ``COUNT(*)``,
        not ``len(list_all(...))``."""
        return session.execute(text("SELECT COUNT(*) FROM deployments")).scalar_one()

    def deployments_in(
        self,
        session: Session,
        cluster_id: str,
        where_state: frozenset[DeploymentState],
        except_id: str | None,
    ) -> list[DeploymentRecord]:
        """The Dispatcher's ``Cascade`` primitive (docs/design/coherence-review.md
        Conflict 3): every deployment of ``cluster_id`` whose state is in
        ``where_state``, excluding ``except_id`` -- returned as ``DeploymentRecord``s
        (not rows) since the only thing the Dispatcher does with them is re-enter
        ``transition()`` via a recursive ``apply()`` call."""
        if not where_state:
            return []
        params: dict[str, Any] = {
            "cluster_id": cluster_id,
            "states": [s.value for s in where_state],
        }
        sql = "SELECT * FROM deployments WHERE cluster_id = :cluster_id AND status IN :states"
        if except_id is not None:
            sql += " AND id != :except_id"
            params["except_id"] = except_id
        sql += " ORDER BY created_at"
        rows = session.execute(
            text(sql).bindparams(bindparam("states", expanding=True)), params
        ).mappings().all()
        return [_deployment_record_from_row(_deployment_row_from_mapping(r)) for r in rows]


# ---------------------------------------------------------------------------
# cluster_state_audits / deployment_state_audits
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ClusterStateAuditRow:
    id: int | None
    cluster_id: str
    from_state: str
    to_state: str
    event: str  # the Pillar-1 Event class name
    actor: str  # derived from event.actor -- NO trigger/initiated_by columns (Conflict 11)
    reason: str | None
    context: Mapping[str, Any] | None
    created_at: datetime


def _csa_from_mapping(m: Mapping[str, Any]) -> ClusterStateAuditRow:
    return ClusterStateAuditRow(
        id=m["id"],
        cluster_id=m["cluster_id"],
        from_state=m["from_state"],
        to_state=m["to_state"],
        event=m["event"],
        actor=m["actor"],
        reason=m["reason"],
        context=_load(m["context"]),
        created_at=_parse(m["created_at"]),
    )


class ClusterStateAuditRepository:
    """``cluster_state_audits``: session-in, DTO-out, never commits. Salvaged shape
    from ``reference-code/seedpod/seedpod/data/repositories.py``'s
    ``SQLAlchemyClusterStateAuditRepository`` (``create_audit`` 722-730,
    ``get_audit_trail`` 714-720), re-columned per docs/design/coherence-review.md
    Conflict 11: v1's ``trigger``/``initiated_by`` pair dies: ``actor`` -- derived by
    the CALLER from ``event.actor`` (the Seam A actor grammar) -- is the one column
    that answers "who/what did this". ``created_at`` is stamped from ``at`` -- the
    triggering ``event.at`` the caller passes in, never ``Clock.now()`` -- per the
    0001_initial.sql ``cluster_state_audits`` DDL comment ("created_at = event.at
    (aware UTC 'Z')"): a late-firing timer, a post-crash outbox replay, or a
    ``FrozenClock`` test must all stamp the audit row with the EVENT's time, not
    whenever the write happens to land.

    ``reason``/``context`` derive mechanically from ``event`` itself
    (docs/decisions/DR-0007-audit-reason-context-derivation.md, resolving the void
    Conflict 11's DDL left): ``reason`` is the event's own ``reason`` field when its
    class declares one (``ProvisionFailed``, ``DeployFailed``, ``DestroyFailed``,
    ``DeployRejected``, ``CancelRequested``, ``HealthCheckFailed``, ...), else
    ``NULL`` -- never invented. ``context`` is ``canonical_json(encode(event))``, the
    full tagged event verbatim, on every row -- replay-grade forensics, safe by
    construction because events carry refs only, never secrets (Seam A, Conflict 9).
    The caller (the Dispatcher) supplies no ``reason``/``context`` of its own -- the
    event IS the why."""

    def add(
        self,
        session: Session,
        *,
        cluster_id: str,
        from_state: str,
        to_state: str,
        event: Event,
        actor: str,
        at: datetime,
    ) -> None:
        session.execute(
            text(
                """
                INSERT INTO cluster_state_audits
                    (cluster_id, from_state, to_state, event, actor, reason, context, created_at)
                VALUES
                    (:cluster_id, :from_state, :to_state, :event, :actor, :reason, :context, :created_at)
                """
            ),
            {
                "cluster_id": cluster_id,
                "from_state": from_state,
                "to_state": to_state,
                "event": type(event).__name__,
                "actor": actor,
                "reason": getattr(event, "reason", None),
                "context": canonical_json(event),
                "created_at": _iso(at),
            },
        )

    def list_for_cluster(self, session: Session, cluster_id: str, limit: int = 50) -> list[ClusterStateAuditRow]:
        rows = session.execute(
            text(
                "SELECT * FROM cluster_state_audits WHERE cluster_id = :cluster_id "
                "ORDER BY created_at DESC LIMIT :limit"
            ),
            {"cluster_id": cluster_id, "limit": limit},
        ).mappings().all()
        return [_csa_from_mapping(r) for r in rows]


@dataclass(frozen=True, slots=True)
class DeploymentStateAuditRow:
    id: int | None
    deployment_id: str
    cluster_id: str
    from_state: str
    to_state: str
    event: str
    actor: str
    reason: str | None
    context: Mapping[str, Any] | None
    created_at: datetime


def _dsa_from_mapping(m: Mapping[str, Any]) -> DeploymentStateAuditRow:
    return DeploymentStateAuditRow(
        id=m["id"],
        deployment_id=m["deployment_id"],
        cluster_id=m["cluster_id"],
        from_state=m["from_state"],
        to_state=m["to_state"],
        event=m["event"],
        actor=m["actor"],
        reason=m["reason"],
        context=_load(m["context"]),
        created_at=_parse(m["created_at"]),
    )


class DeploymentStateAuditRepository:
    """``deployment_state_audits`` -- the deployment-machine twin of
    ``ClusterStateAuditRepository``, a table docs/design/seam-d-foundation.md's
    schema was missing entirely (docs/design/coherence-review.md Conflict 11);
    same shape, same ``actor``-not-``trigger``/``initiated_by`` discipline, and the
    same ``created_at = event.at`` (never ``Clock.now()``) discipline -- see
    ``ClusterStateAuditRepository.add``'s docstring, including the DR-0007
    ``reason``/``context`` derivation, which is identical here."""

    def add(
        self,
        session: Session,
        *,
        deployment_id: str,
        cluster_id: str,
        from_state: str,
        to_state: str,
        event: Event,
        actor: str,
        at: datetime,
    ) -> None:
        session.execute(
            text(
                """
                INSERT INTO deployment_state_audits
                    (deployment_id, cluster_id, from_state, to_state, event, actor, reason,
                     context, created_at)
                VALUES
                    (:deployment_id, :cluster_id, :from_state, :to_state, :event, :actor, :reason,
                     :context, :created_at)
                """
            ),
            {
                "deployment_id": deployment_id,
                "cluster_id": cluster_id,
                "from_state": from_state,
                "to_state": to_state,
                "event": type(event).__name__,
                "actor": actor,
                "reason": getattr(event, "reason", None),
                "context": canonical_json(event),
                "created_at": _iso(at),
            },
        )

    def list_for_deployment(
        self, session: Session, deployment_id: str, limit: int = 50
    ) -> list[DeploymentStateAuditRow]:
        rows = session.execute(
            text(
                "SELECT * FROM deployment_state_audits WHERE deployment_id = :deployment_id "
                "ORDER BY created_at DESC LIMIT :limit"
            ),
            {"deployment_id": deployment_id, "limit": limit},
        ).mappings().all()
        return [_dsa_from_mapping(r) for r in rows]


# ---------------------------------------------------------------------------
# deployment_audits (encrypted)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DeploymentAuditRow:
    """One ``deployment_audits`` row, PLAINTEXT shape (session-in/DTO-out still
    applies: the encryption round-trip happens entirely inside the repository, never
    at a call site -- ``seedpod/services/crypto.py`` is "the only crypto site")."""

    id: str
    deployment_id: str | None
    cluster_id: str
    environment: str
    triggering_repo: str
    triggering_branch: str
    triggering_image: str
    commit_sha: str | None
    deployment_profile_name: str
    resolution_strategy: str
    registry_queries: Sequence[Any]
    resolved_images: Mapping[str, str]
    resolved_config: Mapping[str, Any]
    resolved_manifests: str  # PLAINTEXT; encrypted at rest
    resolved_secrets: Mapping[str, str]  # PLAINTEXT; encrypted at rest
    key_class: str  # 'DEV' | 'PROD' -- stamped beside the ciphertext
    template_files_used: Sequence[str]
    created_at: datetime


class DeploymentAuditRepository:
    """``deployment_audits``: session-in, DTO-out, never commits. Salvaged shape from
    ``reference-code/seedpod/seedpod/data/repositories.py``'s
    ``SQLAlchemyDeploymentAuditRepository`` (``create_audit`` 654-672, ``get_audit``
    640-645, ``get_audits_for_cluster`` 674-682): v1 encrypted via three independent
    ``_get_fernet()`` call sites re-deriving a key from ``environment`` (gotcha
    8/H10/H18); v2 routes both directions through the one injected ``CryptoService``,
    and ``key_class`` -- stamped on the row at write time -- is what ``get`` hands
    back to ``decrypt`` (never re-derived from ``environment``, per
    ``seedpod/services/crypto.py``'s module docstring)."""

    def __init__(self, crypto: CryptoService) -> None:
        self._crypto = crypto

    def insert(self, session: Session, row: DeploymentAuditRow) -> None:
        encrypted_manifests = self._crypto.encrypt(row.resolved_manifests, row.key_class)
        encrypted_secrets = self._crypto.encrypt(
            json.dumps(dict(row.resolved_secrets), sort_keys=True), row.key_class
        )
        session.execute(
            text(
                """
                INSERT INTO deployment_audits
                    (id, deployment_id, cluster_id, environment, triggering_repo, triggering_branch,
                     triggering_image, commit_sha, deployment_profile_name, resolution_strategy,
                     registry_queries, resolved_images, resolved_config,
                     encrypted_resolved_manifests, encrypted_resolved_secrets, key_class,
                     template_files_used, created_at)
                VALUES
                    (:id, :deployment_id, :cluster_id, :environment, :triggering_repo, :triggering_branch,
                     :triggering_image, :commit_sha, :deployment_profile_name, :resolution_strategy,
                     :registry_queries, :resolved_images, :resolved_config,
                     :encrypted_resolved_manifests, :encrypted_resolved_secrets, :key_class,
                     :template_files_used, :created_at)
                """
            ),
            {
                "id": row.id,
                "deployment_id": row.deployment_id,
                "cluster_id": row.cluster_id,
                "environment": row.environment,
                "triggering_repo": row.triggering_repo,
                "triggering_branch": row.triggering_branch,
                "triggering_image": row.triggering_image,
                "commit_sha": row.commit_sha,
                "deployment_profile_name": row.deployment_profile_name,
                "resolution_strategy": row.resolution_strategy,
                "registry_queries": _dump_nn(row.registry_queries, kind="seq"),
                "resolved_images": _dump_nn(row.resolved_images, kind="map"),
                "resolved_config": _dump_nn(row.resolved_config, kind="map"),
                "encrypted_resolved_manifests": encrypted_manifests,
                "encrypted_resolved_secrets": encrypted_secrets,
                "key_class": row.key_class,
                "template_files_used": _dump_nn(row.template_files_used, kind="seq"),
                "created_at": _iso(row.created_at),
            },
        )

    def _decrypt_row(self, m: Mapping[str, Any]) -> DeploymentAuditRow:
        key_class = m["key_class"]
        manifests = self._crypto.decrypt(m["encrypted_resolved_manifests"], key_class)
        secrets = json.loads(self._crypto.decrypt(m["encrypted_resolved_secrets"], key_class))
        return DeploymentAuditRow(
            id=m["id"],
            deployment_id=m["deployment_id"],
            cluster_id=m["cluster_id"],
            environment=m["environment"],
            triggering_repo=m["triggering_repo"],
            triggering_branch=m["triggering_branch"],
            triggering_image=m["triggering_image"],
            commit_sha=m["commit_sha"],
            deployment_profile_name=m["deployment_profile_name"],
            resolution_strategy=m["resolution_strategy"],
            registry_queries=_load(m["registry_queries"]) or [],
            resolved_images=_load(m["resolved_images"]) or {},
            resolved_config=_load(m["resolved_config"]) or {},
            resolved_manifests=manifests,
            resolved_secrets=secrets,
            key_class=key_class,
            template_files_used=_load(m["template_files_used"]) or [],
            created_at=_parse(m["created_at"]),
        )

    def get(self, session: Session, audit_id: str) -> DeploymentAuditRow | None:
        row = session.execute(
            text("SELECT * FROM deployment_audits WHERE id = :id"), {"id": audit_id}
        ).mappings().first()
        return self._decrypt_row(row) if row is not None else None

    def get_by_deployment_id(self, session: Session, deployment_id: str) -> DeploymentAuditRow | None:
        row = session.execute(
            text("SELECT * FROM deployment_audits WHERE deployment_id = :deployment_id"),
            {"deployment_id": deployment_id},
        ).mappings().first()
        return self._decrypt_row(row) if row is not None else None

    def list_for_cluster(self, session: Session, cluster_id: str) -> list[DeploymentAuditRow]:
        rows = session.execute(
            text(
                "SELECT * FROM deployment_audits WHERE cluster_id = :cluster_id "
                "ORDER BY created_at DESC"
            ),
            {"cluster_id": cluster_id},
        ).mappings().all()
        return [self._decrypt_row(r) for r in rows]

    def update_rendered_manifests(
        self,
        session: Session,
        audit_id: str,
        *,
        resolved_manifests: str,
        resolved_config: Mapping[str, Any],
        template_files_used: Sequence[str],
        key_class: str,
    ) -> bool:
        """DR-0025 Erratum E2 point (ii): the deploy-time rehydration write path --
        ``seedpod/engine/steps/deploy.py``'s ``DeployLoadAudit`` calls this to
        rewrite a previously-DEFERRED row's ``resolved_manifests``/
        ``resolved_config`` IN PLACE once it has re-rendered against the now-known
        provisioned host. "One row, one truth" (that erratum's own words): this is
        a deliberate, DR-ratified exception to ``0001_initial.sql``'s own
        ``deployment_audits`` header comment ("immutable") -- see that file's
        comment, updated alongside this method, for the one case it now covers.

        Same discipline as ``ClusterRepository.set_kubeconfig``/
        ``set_health_failures``: a plain UPDATE, no CAS, no ``version`` bump (this
        row has none), rowcount reported rather than silently trusted -- a lost
        write here means the applied manifest and the audit record of it
        permanently diverge, exactly what DR-0025's Consequences forbid, so the
        caller must be able to tell "the row was gone" from "it worked" rather
        than minting a rehydrated `ManifestDoc` list for a write that silently
        affected zero rows.

        ``resolved_images``/``resolved_secrets``/``registry_queries`` are
        DELIBERATELY not parameters here -- DR-0025 part 2 re-runs ONLY
        hostname-dependent resolution (``render_only``'s own docstring,
        ``seedpod/services/manifests.py``), never image resolution, so those
        three columns are untouched by this write. ``key_class`` is the row's
        OWN already-stamped value (never re-derived from ``environment`` --
        ``CryptoService``'s own "deviation 2", this class's module docstring),
        threaded in by the caller rather than re-read here to avoid a second
        SELECT inside the same short transaction."""
        encrypted_manifests = self._crypto.encrypt(resolved_manifests, key_class)
        result = session.execute(
            text(
                """
                UPDATE deployment_audits
                SET resolved_config = :resolved_config,
                    encrypted_resolved_manifests = :encrypted_resolved_manifests,
                    template_files_used = :template_files_used
                WHERE id = :id
                """
            ),
            {
                "resolved_config": _dump_nn(resolved_config, kind="map"),
                "encrypted_resolved_manifests": encrypted_manifests,
                "template_files_used": _dump_nn(template_files_used, kind="seq"),
                "id": audit_id,
            },
        )
        return result.rowcount > 0


# ---------------------------------------------------------------------------
# timers
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class TimerRow:
    """One ``timers`` row (docs/design/coherence-review.md Conflict 1: Seam A's
    dedicated timers table, PK ``(aggregate_type, aggregate_id, timer_key)``).
    ``event`` is kept as the raw canonical-JSON string (``core.codec.canonical_json``)
    -- the same at-rest convention ``effects_outbox.payload`` uses -- decoded via
    ``core.codec.decode_event`` only by whoever fires it (the, later-work,
    ``TimerService``), not by this repository.

    ``fire_at`` is the parsed ``datetime`` (for ordinary comparisons/assertions).
    ``fire_at_text`` is the exact, unparsed TEXT this row's ``fire_at`` column held
    at scan time -- carried alongside so a caller threading a conditional-consume
    snapshot (``TimerRepository.consume``, DR-0009 §2: "exact TEXT equality") passes
    the literal bytes this scan saw, not a ``_parse``-then-``_iso`` round trip. A
    round trip is only guaranteed text-equal to the original because every writer
    today is ``_iso``; carrying the raw TEXT makes the law true by construction
    instead of by writer-convention accident."""

    aggregate_type: str
    aggregate_id: str
    timer_key: str
    fire_at: datetime
    fire_at_text: str
    event: str  # canonical_json(ScheduleTimer.event), applied verbatim on fire
    created_by_effect: str


def _timer_from_mapping(m: Mapping[str, Any]) -> TimerRow:
    return TimerRow(
        aggregate_type=m["aggregate_type"],
        aggregate_id=m["aggregate_id"],
        timer_key=m["timer_key"],
        fire_at=_parse(m["fire_at"]),
        fire_at_text=m["fire_at"],
        event=m["event"],
        created_by_effect=m["created_by_effect"],
    )


class TimerRepository:
    """``timers``: session-in, DTO-out, never commits. New in v2 -- no v1 salvage
    source (v1 had no durable per-aggregate timer subsystem); the shape and the
    upsert/all-keys-delete semantics are Seam A's, ratified verbatim by
    docs/design/coherence-review.md Conflict 1."""

    def upsert(self, session: Session, eff: ScheduleTimer, created_by_effect: str) -> None:
        """PK upsert on ``(aggregate_type, aggregate_id, timer_key)`` -- re-arming a
        timer (e.g. TTL extension) updates ``fire_at``/``event``/``created_by_effect``
        in place; the upsert itself is what makes re-arming idempotent by construction."""
        session.execute(
            text(
                """
                INSERT INTO timers (aggregate_type, aggregate_id, timer_key, fire_at, event, created_by_effect)
                VALUES (:aggregate_type, :aggregate_id, :timer_key, :fire_at, :event, :created_by_effect)
                ON CONFLICT (aggregate_type, aggregate_id, timer_key) DO UPDATE SET
                    fire_at = excluded.fire_at,
                    event = excluded.event,
                    created_by_effect = excluded.created_by_effect
                """
            ),
            {
                "aggregate_type": eff.aggregate_type,
                "aggregate_id": eff.aggregate_id,
                "timer_key": eff.timer_key,
                "fire_at": _iso(eff.fire_at),
                "event": canonical_json(eff.event),
                "created_by_effect": created_by_effect,
            },
        )

    def delete(self, session: Session, eff: CancelTimer) -> None:
        """``eff.timer_key is None`` deletes ALL timers for the aggregate (Seam A:
        "``CancelTimer(timer_key=None)`` = delete-all") -- otherwise just the one key."""
        if eff.timer_key is None:
            session.execute(
                text("DELETE FROM timers WHERE aggregate_type = :aggregate_type AND aggregate_id = :aggregate_id"),
                {"aggregate_type": eff.aggregate_type, "aggregate_id": eff.aggregate_id},
            )
        else:
            session.execute(
                text(
                    "DELETE FROM timers WHERE aggregate_type = :aggregate_type "
                    "AND aggregate_id = :aggregate_id AND timer_key = :timer_key"
                ),
                {
                    "aggregate_type": eff.aggregate_type,
                    "aggregate_id": eff.aggregate_id,
                    "timer_key": eff.timer_key,
                },
            )

    def consume(
        self, session: Session, aggregate_type: str, aggregate_id: str, timer_key: str, fire_at_text: str
    ) -> bool:
        """Conditional consume (docs/decisions/DR-0009-conditional-timer-consume.md,
        RATIFIED): ``DELETE ... WHERE (pk) AND fire_at = :fire_at_text`` -- the exact
        ``fire_at`` TEXT the caller's scan pass saw (``TimerRow.fire_at_text``, not a
        re-serialized ``datetime``). ``fire_at`` comparison is exact TEXT equality of
        the ISO-8601 string, compared verbatim with no parse/reformat round trip
        (DR-0009 §2 -- literal, not writer-convention-accidental); ``event`` is
        deliberately NOT part of the condition (a re-arm always rewrites ``fire_at``).
        Returns ``True`` (rowcount 1) when the row was exactly the one scanned -- the
        caller's fire transaction should proceed to ``apply()``; ``False`` (rowcount
        0) means a concurrent same-key re-arm or cancel won the scan-to-fire window --
        the caller must skip the apply entirely (the surviving row, if any, fires at
        its own deadline on a later pass; a cancelled timer stays cancelled)."""
        result = session.execute(
            text(
                "DELETE FROM timers WHERE aggregate_type = :aggregate_type "
                "AND aggregate_id = :aggregate_id AND timer_key = :timer_key AND fire_at = :fire_at_text"
            ),
            {
                "aggregate_type": aggregate_type,
                "aggregate_id": aggregate_id,
                "timer_key": timer_key,
                "fire_at_text": fire_at_text,
            },
        )
        return result.rowcount > 0

    def get(self, session: Session, aggregate_type: str, aggregate_id: str, timer_key: str) -> TimerRow | None:
        row = session.execute(
            text(
                "SELECT * FROM timers WHERE aggregate_type = :aggregate_type "
                "AND aggregate_id = :aggregate_id AND timer_key = :timer_key"
            ),
            {"aggregate_type": aggregate_type, "aggregate_id": aggregate_id, "timer_key": timer_key},
        ).mappings().first()
        return _timer_from_mapping(row) if row is not None else None

    def due(self, session: Session, now: datetime) -> list[TimerRow]:
        """Every timer whose ``fire_at`` has passed, ordered by ``fire_at`` -- the
        (later-work) ``TimerService``'s poll query."""
        rows = session.execute(
            text("SELECT * FROM timers WHERE fire_at <= :now ORDER BY fire_at"),
            {"now": _iso(now)},
        ).mappings().all()
        return [_timer_from_mapping(r) for r in rows]

    def next_fire_at(self, session: Session) -> datetime | None:
        """The earliest ``fire_at`` across every armed timer -- lets the (later-work)
        ``TimerService`` sleep until the next real deadline instead of busy-polling."""
        value = session.execute(text("SELECT MIN(fire_at) FROM timers")).scalar()
        return _parse(value) if value is not None else None

    def list_all(self, session: Session) -> list[TimerRow]:
        """Every armed timer, ordered by ``fire_at`` -- ``GET /api/timers``'s read
        side (Round 6, api-edge; docs/decisions/DR-0003: "Expose the timers table
        ... ordered by fire_at. Read-only in v2.0"). Unlike ``due()``, this carries
        no ``fire_at <= now`` filter -- the API surfaces every future-armed timer,
        not just what's ready to fire this instant."""
        rows = session.execute(text("SELECT * FROM timers ORDER BY fire_at")).mappings().all()
        return [_timer_from_mapping(r) for r in rows]


# ---------------------------------------------------------------------------
# Repositories -- the bundle seedpod/runtime/dispatcher.py needs
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Repositories:
    """The ``repos-machine`` + outbox subset of this module, bundled behind one
    name so ``seedpod/runtime/dispatcher.py`` can be constructed as
    ``Dispatcher(uow, repos: Repositories, clock)`` -- the exact signature
    docs/design/coherence-review.md Conflict 3 gives (``self.repos.load``,
    ``self.repos.persist``, ``self.repos.state_audits.add``,
    ``self.repos.timers.upsert``/``.delete``, ``self.repos.deployments_in``,
    ``self.repos.outbox.insert`` in that spec's pseudocode dispatch by
    per-aggregate-typed attribute instead, per the committed repo split
    documented above this class's neighbors). Deliberately minimal -- holds only
    what the Dispatcher touches; grows additively (never restructures existing
    fields, since this is a plain constructor-injected bundle, not an ORM) as
    later runtime-spine components (``EffectExecutor``, ``TimerService``) are
    built and need their own repos wired alongside it.

    ``workflow_runs`` was added for ``seedpod/runtime/effect_executor.py``'s
    run-admitter (Conflict 2): admission reads/writes ``workflow_runs`` (resolve
    the one-active-run-per-cluster conflict, insert the admitted run) alongside
    every other write this bundle already routes through."""

    clusters: ClusterRepository
    deployments: DeploymentRepository
    cluster_state_audits: ClusterStateAuditRepository
    deployment_state_audits: DeploymentStateAuditRepository
    timers: TimerRepository
    outbox: OutboxRepository
    workflow_runs: WorkflowRunRepository


# ---------------------------------------------------------------------------
# api_keys
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ApiKeyRow:
    """One ``api_keys`` row, shaped exactly like 0001_initial.sql. ``id`` is
    ``None`` for a not-yet-inserted row (AUTOINCREMENT fills it in). ``permissions``
    is a JSON list of permission strings (the DDL's ``DEFAULT '[]'``; not v1's
    ``dict[str, bool]`` shape -- hashing and permission-string shape both live
    above this repository, in the -- later-work -- ``ApiKeyService``)."""

    id: int | None
    key_hash: str
    username: str
    environment: str  # 'all' sentinel kept verbatim (Decision 6)
    permissions: Sequence[str]
    is_active: bool
    description: str | None
    created_by: str | None
    created_at: datetime
    expires_at: datetime | None
    last_used_at: datetime | None


def _api_key_params(row: ApiKeyRow) -> dict[str, Any]:
    return {
        "key_hash": row.key_hash,
        "username": row.username,
        "environment": row.environment,
        "permissions": _dump_nn(row.permissions, kind="seq"),
        "is_active": int(row.is_active),
        "description": row.description,
        "created_by": row.created_by,
        "created_at": _iso(row.created_at),
        "expires_at": _iso_or_none(row.expires_at),
        "last_used_at": _iso_or_none(row.last_used_at),
    }


def _api_key_from_mapping(m: Mapping[str, Any]) -> ApiKeyRow:
    return ApiKeyRow(
        id=m["id"],
        key_hash=m["key_hash"],
        username=m["username"],
        environment=m["environment"],
        permissions=_load(m["permissions"]) or [],
        is_active=bool(m["is_active"]),
        description=m["description"],
        created_by=m["created_by"],
        created_at=_parse(m["created_at"]),
        expires_at=_parse_or_none(m["expires_at"]),
        last_used_at=_parse_or_none(m["last_used_at"]),
    )


class ApiKeyRepository:
    """``api_keys``: session-in, DTO-out, never commits.

    Salvaged method surface from
    ``reference-code/seedpod/seedpod/data/repositories.py``'s
    ``SQLAlchemyAPIKeyRepository`` (443-512): ``get_key_by_hash`` (461-464, here
    ``get_by_hash``), ``list_keys`` (471-484, here ``list``), ``create_api_key``
    (486-492, salvaged as plain ``insert`` -- no ``session.commit()``, no id
    derivation: ``id`` is ``AUTOINCREMENT``), ``update_last_used`` (494-502, here
    ``touch_last_used``), ``revoke_key`` (504-512, here ``revoke``).

    ``get_valid_by_hash`` is new-in-v2 plumbing closing v1's ``APIKeyRecord.is_valid()``
    (``reference-code/seedpod/seedpod/data/models.py:157-171``): v1 called
    ``datetime.now(UTC)`` INSIDE a DTO method on the hot auth path -- banned here
    (every timestamp comparison takes an injected ``now``, matching
    ``ClusterRepository.list_expired``'s discipline) -- so the active+expiry check
    moves into SQL, driven by the caller's ``Clock.now()``, rather than living on the
    DTO.
    """

    def insert(self, session: Session, row: ApiKeyRow) -> None:
        session.execute(
            text(
                """
                INSERT INTO api_keys
                    (key_hash, username, environment, permissions, is_active, description,
                     created_by, created_at, expires_at, last_used_at)
                VALUES
                    (:key_hash, :username, :environment, :permissions, :is_active, :description,
                     :created_by, :created_at, :expires_at, :last_used_at)
                """
            ),
            _api_key_params(row),
        )

    def get_by_hash(self, session: Session, key_hash: str) -> ApiKeyRow | None:
        row = session.execute(
            text("SELECT * FROM api_keys WHERE key_hash = :key_hash"), {"key_hash": key_hash}
        ).mappings().first()
        return _api_key_from_mapping(row) if row is not None else None

    def get_valid_by_hash(self, session: Session, key_hash: str, *, now: datetime) -> ApiKeyRow | None:
        """The auth hot path: a hash match that is ALSO active and unexpired --
        the SQL-side twin of v1's ``APIKeyRecord.is_valid()``."""
        row = session.execute(
            text(
                "SELECT * FROM api_keys WHERE key_hash = :key_hash AND is_active = 1 "
                "AND (expires_at IS NULL OR expires_at > :now)"
            ),
            {"key_hash": key_hash, "now": _iso(now)},
        ).mappings().first()
        return _api_key_from_mapping(row) if row is not None else None

    def get_by_id(self, session: Session, key_id: int) -> ApiKeyRow | None:
        row = session.execute(
            text("SELECT * FROM api_keys WHERE id = :id"), {"id": key_id}
        ).mappings().first()
        return _api_key_from_mapping(row) if row is not None else None

    def list(
        self,
        session: Session,
        *,
        username: str | None = None,
        environment: str | None = None,
        active_only: bool = False,
    ) -> list[ApiKeyRow]:
        sql = "SELECT * FROM api_keys WHERE 1=1"
        params: dict[str, Any] = {}
        if username is not None:
            sql += " AND username = :username"
            params["username"] = username
        if environment is not None:
            sql += " AND environment = :environment"
            params["environment"] = environment
        if active_only:
            sql += " AND is_active = 1"
        sql += " ORDER BY created_at DESC"
        rows = session.execute(text(sql), params).mappings().all()
        return [_api_key_from_mapping(r) for r in rows]

    def touch_last_used(
        self, session: Session, key_id: int, *, clock: Clock, when: datetime | None = None
    ) -> bool:
        """Returns whether a row was touched -- v1's ``update_last_used``
        (reference-code repositories.py:494-502) returned ``False`` for a missing
        key rather than silently no-opping; neighboring mutators here (``revoke``)
        already report a rowcount bool, so this matches them.

        ``when`` (DR-0044) is the moment the key was actually USED, for callers that
        defer the write -- ``ApiKeyService`` buffers touches and flushes them in one
        batch, so stamping ``clock.now()`` at flush time would record when the write
        happened rather than when the key was used, turning a slightly-stale value
        into a wrong one. Defaults to ``clock.now()``, which is every immediate
        caller's case."""
        result = session.execute(
            text("UPDATE api_keys SET last_used_at = :now WHERE id = :id"),
            {"now": _iso(when if when is not None else clock.now()), "id": key_id},
        )
        return result.rowcount > 0

    def revoke(self, session: Session, key_id: int) -> bool:
        result = session.execute(
            text("UPDATE api_keys SET is_active = 0 WHERE id = :id"), {"id": key_id}
        )
        return result.rowcount > 0

    def update(
        self,
        session: Session,
        key_id: int,
        *,
        description: str | None | _Unset = UNSET,
        expires_at: datetime | None | _Unset = UNSET,
    ) -> bool:
        """Round 6, api-features: ``PATCH /api/keys/{id}`` (ui-contract:
        ``{description, expires_at}``, the only two mutable fields v1's own
        ``UpdateAPIKeyRequest`` ever exposed -- ``reference-code/seedpod/seedpod/
        api/auth.py:35-38``). Same partial-update, only-fields-actually-passed
        ``_Unset``-sentinel discipline as ``WorkflowRunRepository.update``/
        ``PresetRepository.update`` -- a thin, additive repo method, not a
        restructure of the class above."""
        sets: dict[str, Any] = {}
        if not isinstance(description, _Unset):
            sets["description"] = description
        if not isinstance(expires_at, _Unset):
            sets["expires_at"] = _iso_or_none(expires_at)
        if not sets:
            return False
        assignments = ", ".join(f"{k} = :{k}" for k in sets)
        result = session.execute(
            text(f"UPDATE api_keys SET {assignments} WHERE id = :id"), {**sets, "id": key_id}
        )
        return result.rowcount > 0

    def count(self, session: Session) -> int:
        """Total API key row count -- ``/health/detailed``'s ``database.api_key_count``
        (Round 6, api-edge; DR-0003). See ``ClusterRepository.count``'s docstring for
        why this is a ``COUNT(*)``, not ``len(list(...))``."""
        return session.execute(text("SELECT COUNT(*) FROM api_keys")).scalar_one()


# ---------------------------------------------------------------------------
# secrets
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SecretRow:
    """One ``secrets`` row, PLAINTEXT shape -- same session-in/DTO-out discipline as
    ``DeploymentAuditRow``: the encryption round-trip happens entirely inside
    ``SecretRepository``, never at a call site."""

    id: int | None
    environment: str
    key_name: str
    value: str  # PLAINTEXT; encrypted at rest
    key_class: str  # 'DEV' | 'PROD' -- stamped at encrypt time
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class SecretMetadataRow:
    """One ``secrets`` row WITHOUT the value -- ciphertext untouched, never passed
    through ``CryptoService.decrypt``. This is the shape ``list_for_environment``
    hands back (v1's ``GET /api/secrets``, "metadata only, no decrypted values",
    ``reference-code/seedpod/seedpod/api/secrets.py:72``); use ``SecretRepository.get``
    for the single-secret, decrypting read (the reveal path)."""

    id: int | None
    environment: str
    key_name: str
    key_class: str  # 'DEV' | 'PROD' -- stamped at encrypt time
    created_at: datetime
    updated_at: datetime


class SecretRepository:
    """``secrets``: session-in, DTO-out, never commits.

    Salvaged from ``reference-code/seedpod/seedpod/data/repositories.py``'s
    ``SQLAlchemySecretRepository`` (534-593): ``get_secrets_for_environment``
    (548-552, here ``list_for_environment``) and ``delete_secret`` (582-593, here
    ``delete``). ``create_secret`` (554-580) is NOT salvaged as-is -- it did its own
    "check if exists, then UPDATE-or-INSERT" in Python (v1 gotcha 4: two concurrent
    callers racing this read-then-write can both see "no existing row" and both
    INSERT, tripping the app-level-only uniqueness). ``upsert`` replaces it with the
    single ``INSERT ... ON CONFLICT(environment, key_name) DO UPDATE`` the ``secrets``
    DDL comment mandates (docs/design/seam-d-foundation.md Decision 6, "gotcha 4
    closed") -- the duplicate-key race is unrepresentable because there is exactly
    one statement, not a read followed by a conditional write.

    ``key_class`` is stamped on the row AT ENCRYPT TIME by the caller (never derived
    here from ``environment``) and is exactly what ``get`` hands back to
    ``CryptoService.decrypt`` -- this repository never calls
    ``key_class_for_environment`` itself, matching ``DeploymentAuditRepository``'s
    "decrypt reads the stamp, never re-derives" discipline.

    ``list_for_environment`` does NOT decrypt: v1's ``get_secrets_for_environment``
    (the method it salvages) returned rows carrying ``encrypted_value``, never
    plaintext -- decryption was opt-in, one row at a time, at the
    ``SecretManager``/reveal layer. Decrypting a whole environment's worth of values
    just to list them would (a) silently regress v1's "secrets:read = metadata only"
    permission model into "secrets:read = every value, decrypted", and (b) raise
    ``PermanentError`` listing a DEV environment containing PROD-stamped rows on an
    instance with no ``prod_key`` configured, where v1 served the metadata fine.
    ``list_for_environment`` therefore returns ``SecretMetadataRow`` (ciphertext
    untouched); only ``get`` -- the single-secret reveal path -- decrypts.
    """

    def __init__(self, crypto: CryptoService) -> None:
        self._crypto = crypto

    def upsert(
        self,
        session: Session,
        *,
        environment: str,
        key_name: str,
        value: str,
        key_class: str,
        clock: Clock,
    ) -> None:
        now = clock.now()
        encrypted = self._crypto.encrypt(value, key_class)
        session.execute(
            text(
                """
                INSERT INTO secrets (environment, key_name, encrypted_value, key_class, created_at, updated_at)
                VALUES (:environment, :key_name, :encrypted_value, :key_class, :created_at, :updated_at)
                ON CONFLICT (environment, key_name) DO UPDATE SET
                    encrypted_value = excluded.encrypted_value,
                    key_class = excluded.key_class,
                    updated_at = excluded.updated_at
                """
            ),
            {
                "environment": environment,
                "key_name": key_name,
                "encrypted_value": encrypted,
                "key_class": key_class,
                "created_at": _iso(now),
                "updated_at": _iso(now),
            },
        )

    def _decrypt_row(self, m: Mapping[str, Any]) -> SecretRow:
        key_class = m["key_class"]
        value = self._crypto.decrypt(m["encrypted_value"], key_class)
        return SecretRow(
            id=m["id"],
            environment=m["environment"],
            key_name=m["key_name"],
            value=value,
            key_class=key_class,
            created_at=_parse(m["created_at"]),
            updated_at=_parse(m["updated_at"]),
        )

    @staticmethod
    def _metadata_row(m: Mapping[str, Any]) -> SecretMetadataRow:
        return SecretMetadataRow(
            id=m["id"],
            environment=m["environment"],
            key_name=m["key_name"],
            key_class=m["key_class"],
            created_at=_parse(m["created_at"]),
            updated_at=_parse(m["updated_at"]),
        )

    def get(self, session: Session, environment: str, key_name: str) -> SecretRow | None:
        """Single-secret, DECRYPTING read -- the reveal path."""
        row = session.execute(
            text("SELECT * FROM secrets WHERE environment = :environment AND key_name = :key_name"),
            {"environment": environment, "key_name": key_name},
        ).mappings().first()
        return self._decrypt_row(row) if row is not None else None

    def list_for_environment(self, session: Session, environment: str) -> list[SecretMetadataRow]:
        """Metadata-only, NON-decrypting list -- see class docstring; ciphertext is
        never touched, matching v1's ``get_secrets_for_environment``."""
        rows = session.execute(
            text("SELECT * FROM secrets WHERE environment = :environment ORDER BY key_name"),
            {"environment": environment},
        ).mappings().all()
        return [self._metadata_row(r) for r in rows]

    def delete(self, session: Session, environment: str, key_name: str) -> bool:
        result = session.execute(
            text("DELETE FROM secrets WHERE environment = :environment AND key_name = :key_name"),
            {"environment": environment, "key_name": key_name},
        )
        return result.rowcount > 0


# ---------------------------------------------------------------------------
# secret_audits
# ---------------------------------------------------------------------------

SecretAuditAction = Literal["create", "update", "delete", "reveal"]


@dataclass(frozen=True, slots=True)
class SecretAuditRow:
    id: int | None
    environment: str
    key_name: str
    action: str  # create|update|delete|reveal -- DDL CHECK
    performed_by: str
    key_class: str
    context: Mapping[str, Any] | None
    created_at: datetime


def _secret_audit_from_mapping(m: Mapping[str, Any]) -> SecretAuditRow:
    return SecretAuditRow(
        id=m["id"],
        environment=m["environment"],
        key_name=m["key_name"],
        action=m["action"],
        performed_by=m["performed_by"],
        key_class=m["key_class"],
        context=_load(m["context"]),
        created_at=_parse(m["created_at"]),
    )


class SecretAuditRepository:
    """``secret_audits``: session-in, DTO-out, never commits. Salvaged from
    ``reference-code/seedpod/seedpod/data/repositories.py``'s
    ``SQLAlchemySecretAuditRepository`` (752-802): ``create_audit`` (768-775, here
    ``create``), ``get_audit_trail`` (777-791, same name, same optional filters),
    ``get_last_reveal`` (793-802, same name) -- the ``action='reveal'`` row this
    method reads back is how ``reveal-secret`` requests get their own audit trail
    (the DDL's ``action`` CHECK includes ``'reveal'`` specifically for this)."""

    def create(
        self,
        session: Session,
        *,
        environment: str,
        key_name: str,
        action: SecretAuditAction,
        performed_by: str,
        key_class: str,
        context: Mapping[str, Any] | None = None,
        clock: Clock,
    ) -> None:
        session.execute(
            text(
                """
                INSERT INTO secret_audits
                    (environment, key_name, action, performed_by, key_class, context, created_at)
                VALUES
                    (:environment, :key_name, :action, :performed_by, :key_class, :context, :created_at)
                """
            ),
            {
                "environment": environment,
                "key_name": key_name,
                "action": action,
                "performed_by": performed_by,
                "key_class": key_class,
                "context": _dump(context),
                "created_at": _iso(clock.now()),
            },
        )

    def get_audit_trail(
        self,
        session: Session,
        *,
        environment: str | None = None,
        key_name: str | None = None,
        action: SecretAuditAction | None = None,
        limit: int = 100,
    ) -> list[SecretAuditRow]:
        sql = "SELECT * FROM secret_audits WHERE 1=1"
        params: dict[str, Any] = {"limit": limit}
        if environment is not None:
            sql += " AND environment = :environment"
            params["environment"] = environment
        if key_name is not None:
            sql += " AND key_name = :key_name"
            params["key_name"] = key_name
        if action is not None:
            sql += " AND action = :action"
            params["action"] = action
        sql += " ORDER BY created_at DESC LIMIT :limit"
        rows = session.execute(text(sql), params).mappings().all()
        return [_secret_audit_from_mapping(r) for r in rows]

    def get_last_reveal(self, session: Session, environment: str, key_name: str) -> SecretAuditRow | None:
        row = session.execute(
            text(
                "SELECT * FROM secret_audits WHERE environment = :environment AND key_name = :key_name "
                "AND action = 'reveal' ORDER BY created_at DESC LIMIT 1"
            ),
            {"environment": environment, "key_name": key_name},
        ).mappings().first()
        return _secret_audit_from_mapping(row) if row is not None else None


# ---------------------------------------------------------------------------
# deployment_presets
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PresetRow:
    """One ``deployment_presets`` row, shaped exactly like 0001_initial.sql --
    ``/api/presets`` is the only Tart provider-override deploy path (Decision 6)."""

    id: str
    name: str
    description: str | None
    profile_name: str
    environment: str
    service_overrides: Mapping[str, Any] | None
    default_branch: str | None
    default_ttl_hours: int | None
    # DR-0046: the provider this preset pins. Nullable -- a preset that does not
    # care falls through to the profile, then the global default, exactly as before.
    default_provider: str | None
    naming_strategy: Mapping[str, Any] | None
    created_by: str
    created_at: datetime
    last_used_at: datetime | None
    use_count: int


def _preset_params(row: PresetRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "name": row.name,
        "description": row.description,
        "profile_name": row.profile_name,
        "environment": row.environment,
        "service_overrides": _dump(row.service_overrides),
        "default_branch": row.default_branch,
        "default_ttl_hours": row.default_ttl_hours,
        "default_provider": row.default_provider,
        "naming_strategy": _dump(row.naming_strategy),
        "created_by": row.created_by,
        "created_at": _iso(row.created_at),
        "last_used_at": _iso_or_none(row.last_used_at),
        "use_count": row.use_count,
    }


def _preset_from_mapping(m: Mapping[str, Any]) -> PresetRow:
    return PresetRow(
        id=m["id"],
        name=m["name"],
        description=m["description"],
        profile_name=m["profile_name"],
        environment=m["environment"],
        service_overrides=_load(m["service_overrides"]),
        default_branch=m["default_branch"],
        default_ttl_hours=m["default_ttl_hours"],
        default_provider=m["default_provider"],
        naming_strategy=_load(m["naming_strategy"]),
        created_by=m["created_by"],
        created_at=_parse(m["created_at"]),
        last_used_at=_parse_or_none(m["last_used_at"]),
        use_count=m["use_count"],
    )


class PresetRepository:
    """``deployment_presets``: session-in, DTO-out, never commits.

    Salvaged method surface from
    ``reference-code/seedpod/seedpod/data/repositories.py``'s
    ``SQLAlchemyDeploymentPresetRepository`` (844-965): ``get_preset`` (865-869, here
    ``get``), ``get_preset_by_name`` (871-875, here ``get_by_name``), ``list_presets``
    (877-887, here ``list``, same ``last_used_at DESC NULLS LAST, created_at DESC``
    ordering), ``create_preset`` (889-910, salvaged as plain ``insert`` -- no
    ``session.commit()``, no ``uuid.uuid4()`` id derivation: the id is the caller's
    to assign, same "two changes" discipline as ``ClusterRepository.insert``),
    ``update_preset`` (912-940, here ``update``, partial-update-only-fields-passed
    shape matching ``WorkflowRunRepository.update``), ``delete_preset`` (942-951,
    here ``delete``), ``record_preset_usage`` (953-964, here ``record_usage``).
    """

    def get(self, session: Session, preset_id: str) -> PresetRow | None:
        row = session.execute(
            text("SELECT * FROM deployment_presets WHERE id = :id"), {"id": preset_id}
        ).mappings().first()
        return _preset_from_mapping(row) if row is not None else None

    def get_by_name(self, session: Session, name: str) -> PresetRow | None:
        row = session.execute(
            text("SELECT * FROM deployment_presets WHERE name = :name"), {"name": name}
        ).mappings().first()
        return _preset_from_mapping(row) if row is not None else None

    def list(self, session: Session, *, profile: str | None = None) -> list[PresetRow]:
        sql = "SELECT * FROM deployment_presets WHERE 1=1"
        params: dict[str, Any] = {}
        if profile is not None:
            sql += " AND profile_name = :profile"
            params["profile"] = profile
        sql += " ORDER BY last_used_at DESC NULLS LAST, created_at DESC"
        rows = session.execute(text(sql), params).mappings().all()
        return [_preset_from_mapping(r) for r in rows]

    def insert(self, session: Session, row: PresetRow) -> None:
        session.execute(
            text(
                """
                INSERT INTO deployment_presets
                    (id, name, description, profile_name, environment, service_overrides,
                     default_branch, default_ttl_hours, default_provider, naming_strategy,
                     created_by, created_at, last_used_at, use_count)
                VALUES
                    (:id, :name, :description, :profile_name, :environment, :service_overrides,
                     :default_branch, :default_ttl_hours, :default_provider, :naming_strategy,
                     :created_by, :created_at, :last_used_at, :use_count)
                """
            ),
            _preset_params(row),
        )

    def update(
        self,
        session: Session,
        preset_id: str,
        *,
        name: str | _Unset = UNSET,
        description: str | None | _Unset = UNSET,
        profile_name: str | _Unset = UNSET,
        environment: str | _Unset = UNSET,
        service_overrides: Mapping[str, Any] | None | _Unset = UNSET,
        default_branch: str | None | _Unset = UNSET,
        default_ttl_hours: int | None | _Unset = UNSET,
        default_provider: str | None | _Unset = UNSET,  # DR-0046
        naming_strategy: Mapping[str, Any] | None | _Unset = UNSET,
    ) -> bool:
        """Partial update -- only fields actually passed are written, same
        ``_Unset``-sentinel discipline as ``WorkflowRunRepository.update``.

        Returns whether a row was updated. v1's ``update_preset``
        (reference-code repositories.py:912-940) returned ``None`` for a missing
        preset vs. the updated DTO, so a caller could tell "updated" from "no such
        preset" apart; neighboring mutators here (``delete``, ``record_usage``)
        already report a rowcount bool, so this matches them rather than silently
        collapsing both outcomes to ``None``. A no-fields-passed call is a no-op
        and reports ``False`` (nothing was written).
        """
        sets: dict[str, Any] = {}
        if not isinstance(name, _Unset):
            sets["name"] = name
        if not isinstance(description, _Unset):
            sets["description"] = description
        if not isinstance(profile_name, _Unset):
            sets["profile_name"] = profile_name
        if not isinstance(environment, _Unset):
            sets["environment"] = environment
        if not isinstance(service_overrides, _Unset):
            sets["service_overrides"] = _dump(service_overrides)
        if not isinstance(default_branch, _Unset):
            sets["default_branch"] = default_branch
        if not isinstance(default_ttl_hours, _Unset):
            sets["default_ttl_hours"] = default_ttl_hours
        if not isinstance(default_provider, _Unset):
            sets["default_provider"] = default_provider
        if not isinstance(naming_strategy, _Unset):
            sets["naming_strategy"] = _dump(naming_strategy)
        if not sets:
            return False
        assignments = ", ".join(f"{k} = :{k}" for k in sets)
        result = session.execute(
            text(f"UPDATE deployment_presets SET {assignments} WHERE id = :id"),
            {**sets, "id": preset_id},
        )
        return result.rowcount > 0

    def delete(self, session: Session, preset_id: str) -> bool:
        result = session.execute(
            text("DELETE FROM deployment_presets WHERE id = :id"), {"id": preset_id}
        )
        return result.rowcount > 0

    def record_usage(self, session: Session, preset_id: str, *, clock: Clock) -> bool:
        """``use_count += 1`` and ``last_used_at`` touch, in one statement (v1's
        ``record_preset_usage`` did a read-then-write in Python; here it's one
        UPDATE, so there is no read/write race to have)."""
        result = session.execute(
            text(
                "UPDATE deployment_presets SET use_count = use_count + 1, last_used_at = :now "
                "WHERE id = :id"
            ),
            {"now": _iso(clock.now()), "id": preset_id},
        )
        return result.rowcount > 0


# ---------------------------------------------------------------------------
# snapshots
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class SnapshotRow:
    """One ``snapshots`` row, shaped exactly like 0001_initial.sql -- rows index
    on-disk storage (``storage_path``); the repository never touches the filesystem
    itself (that stays the -- later-work -- ``SnapshotService``'s job, same split as
    ``ClusterRepository`` never touching a provider).

    NOTE (restore-history): v1's restore-history endpoint was backed by
    ``snapshot_operations``, a proto-``workflow_runs`` table (``operation_type`` ->
    ``workflow``, ``progress`` -> ``step_results``) that 0001_initial.sql
    deliberately drops -- see docs/design/seam-d-foundation.md Decision 6's v1->v2
    delta table ("subsumed by workflow_runs"). This is a recorded decision, not a
    gap: there is no ``SnapshotOperationRepository`` here, and there must not be one
    added later without a DR. Round 6's API serves restore-history by querying
    ``workflow_runs`` (``WorkflowRunRepository.list_by_status`` /
    a cluster-scoped run history query) for runs of the ``snapshot-restore``
    workflow, not from a dedicated snapshot-operations table.
    """

    id: str
    name: str
    description: str | None
    source_cluster_id: str
    source_cluster_slug: str
    branch: str | None
    deployment_profile: str
    services: Sequence[Mapping[str, Any]]
    storage_path: str
    total_size_bytes: int
    is_auto: bool
    created_by: str
    created_at: datetime


def _snapshot_params(row: SnapshotRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "name": row.name,
        "description": row.description,
        "source_cluster_id": row.source_cluster_id,
        "source_cluster_slug": row.source_cluster_slug,
        "branch": row.branch,
        "deployment_profile": row.deployment_profile,
        "services": _dump_nn(row.services, kind="seq"),
        "storage_path": row.storage_path,
        "total_size_bytes": row.total_size_bytes,
        "is_auto": int(row.is_auto),
        "created_by": row.created_by,
        "created_at": _iso(row.created_at),
    }


def _snapshot_from_mapping(m: Mapping[str, Any]) -> SnapshotRow:
    return SnapshotRow(
        id=m["id"],
        name=m["name"],
        description=m["description"],
        source_cluster_id=m["source_cluster_id"],
        source_cluster_slug=m["source_cluster_slug"],
        branch=m["branch"],
        deployment_profile=m["deployment_profile"],
        services=_load(m["services"]) or [],
        storage_path=m["storage_path"],
        total_size_bytes=m["total_size_bytes"],
        is_auto=bool(m["is_auto"]),
        created_by=m["created_by"],
        created_at=_parse(m["created_at"]),
    )


class SnapshotRepository:
    """``snapshots``: session-in, DTO-out, never commits. New plumbing behind v1's
    inline ORM queries in
    ``reference-code/seedpod/seedpod/services/snapshot_service.py`` -- v1 had no
    dedicated repository class here, only ad hoc ``session.query(Snapshot)`` calls:
    ``list_snapshots`` (711-727, here ``list``, same ``branch``/``deployment_profile``
    filters), ``get_snapshot`` (729-733, here ``get``), ``delete_snapshot``
    (735-750ish, here ``delete`` -- the DB half only; deleting the on-disk directory
    stays the service's job), and the ``Snapshot(...)`` construction at 365-389
    (here ``insert``, id supplied by the caller -- v1 generated it with
    ``uuid.uuid4()`` at the service layer, same split as every other ``insert`` in
    this module).
    """

    def get(self, session: Session, snapshot_id: str) -> SnapshotRow | None:
        row = session.execute(
            text("SELECT * FROM snapshots WHERE id = :id"), {"id": snapshot_id}
        ).mappings().first()
        return _snapshot_from_mapping(row) if row is not None else None

    def list(
        self, session: Session, *, branch: str | None = None, profile: str | None = None
    ) -> list[SnapshotRow]:
        sql = "SELECT * FROM snapshots WHERE 1=1"
        params: dict[str, Any] = {}
        if branch is not None:
            sql += " AND branch = :branch"
            params["branch"] = branch
        if profile is not None:
            sql += " AND deployment_profile = :profile"
            params["profile"] = profile
        sql += " ORDER BY created_at DESC"
        rows = session.execute(text(sql), params).mappings().all()
        return [_snapshot_from_mapping(r) for r in rows]

    def insert(self, session: Session, row: SnapshotRow) -> None:
        session.execute(
            text(
                """
                INSERT INTO snapshots
                    (id, name, description, source_cluster_id, source_cluster_slug, branch,
                     deployment_profile, services, storage_path, total_size_bytes, is_auto,
                     created_by, created_at)
                VALUES
                    (:id, :name, :description, :source_cluster_id, :source_cluster_slug, :branch,
                     :deployment_profile, :services, :storage_path, :total_size_bytes, :is_auto,
                     :created_by, :created_at)
                """
            ),
            _snapshot_params(row),
        )

    def delete(self, session: Session, snapshot_id: str) -> bool:
        result = session.execute(
            text("DELETE FROM snapshots WHERE id = :id"), {"id": snapshot_id}
        )
        return result.rowcount > 0
