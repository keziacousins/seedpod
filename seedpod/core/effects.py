"""Inert, frozen, serializable effect union. Zero behavior.

Salvaged from docs/design/seam-a-core.md §A (``seedpod2/core/effects.py`` — the
``seedpod2`` package name is dead per coherence review Conflict 16.1; this module
lives at ``seedpod/core/effects.py``), with ``EffectKind``'s lane comments amended
verbatim per docs/design/coherence-review.md Conflict 2: ``RUN_WORKFLOW`` and
``CANCEL_WORKFLOW`` are DRAIN lane (durability is unchanged — the outbox row commits
atomically with the state change; the run-admitter drains them in ``seq`` order).
That drain lane's row-flipping is runtime (``Dispatcher``) work, not this module's.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any, ClassVar

if TYPE_CHECKING:
    from seedpod.core.events import DeploymentEvent, Event
    from seedpod.core.records import ClusterRecord, DeploymentRecord, DeploymentState

__all__ = [
    "EffectKind",
    "Persist",
    "Notify",
    "RunWorkflow",
    "CancelWorkflow",
    "ScheduleTimer",
    "CancelTimer",
    "Cascade",
    "Effect",
]


class EffectKind(StrEnum):
    PERSIST = "persist"  # tx lane
    SCHEDULE_TIMER = "schedule_timer"  # tx lane
    CANCEL_TIMER = "cancel_timer"  # tx lane
    CASCADE = "cascade"  # tx lane (in-tx pure transitions on sibling records)
    RUN_WORKFLOW = "run_workflow"  # drain lane (row 'pending'; admitter inserts the run — idempotent via dedupe_key)
    CANCEL_WORKFLOW = "cancel_workflow"  # drain lane (row 'pending'; admitter flips cancel_requested + trips token)
    NOTIFY = "notify"  # drain lane


@dataclass(frozen=True, slots=True, kw_only=True)
class Persist:
    kind: ClassVar[str] = EffectKind.PERSIST
    record: ClusterRecord | DeploymentRecord  # full post-transition image, version already +1
    expected_version: int | None  # None => INSERT (birth); else CAS UPDATE ... WHERE version=expected


@dataclass(frozen=True, slots=True, kw_only=True)
class Notify:
    kind: ClassVar[str] = EffectKind.NOTIFY
    topic: str  # "cluster_state_changed" | "deployment_status_changed"  (v1 names, verbatim)
    payload: Mapping[str, Any]  # v1-shaped: {cluster_id, old_status, new_status, ...}; JSON-safe scalars only
    environment: str | None  # SSE env filter, resolved AT DECISION TIME from the record


@dataclass(frozen=True, slots=True, kw_only=True)
class RunWorkflow:
    kind: ClassVar[str] = EffectKind.RUN_WORKFLOW
    workflow: str  # "provision" | "deploy" | "rollback" | "destroy" — closed abstract-verb set (Conflict 13)
    cluster_id: str
    deployment_id: str | None = None
    args: Mapping[str, Any] = ()  # typed per workflow in Pillar 2; refs only, never secrets


@dataclass(frozen=True, slots=True, kw_only=True)
class CancelWorkflow:
    kind: ClassVar[str] = EffectKind.CANCEL_WORKFLOW
    workflow: str
    cluster_id: str
    deployment_id: str | None = None
    reason: str = ""


@dataclass(frozen=True, slots=True, kw_only=True)
class ScheduleTimer:
    kind: ClassVar[str] = EffectKind.SCHEDULE_TIMER
    aggregate_type: str  # "cluster" | "deployment"
    aggregate_id: str
    timer_key: str  # "ttl" | "destroy" — upsert key => re-arming idempotent by construction
    fire_at: datetime  # ABSOLUTE aware-UTC, computed from event.at / record fields — transition never calls now()
    event: Event  # the fact injected through apply() when it fires


@dataclass(frozen=True, slots=True, kw_only=True)
class CancelTimer:
    kind: ClassVar[str] = EffectKind.CANCEL_TIMER
    aggregate_type: str
    aggregate_id: str
    timer_key: str | None  # None = all timers for the aggregate


@dataclass(frozen=True, slots=True, kw_only=True)
class Cascade:
    """In-tx fan-out: apply `event` through the pure deployment transition to every deployment
    of `cluster_id` whose state is in `where_state` (excluding except_id). Nested effects join
    the SAME transaction/outbox. Replaces v1 _mark_deployments_destroyed and the supersede ORM
    writes; the machines stay the single author of every status change. Depth is asserted <= 2."""

    kind: ClassVar[str] = EffectKind.CASCADE
    cluster_id: str
    where_state: frozenset[DeploymentState]
    event: DeploymentEvent
    except_id: str | None = None


Effect = Persist | Notify | RunWorkflow | CancelWorkflow | ScheduleTimer | CancelTimer | Cascade
