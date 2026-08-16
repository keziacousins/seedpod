"""seedpod/providers/kube_types.py — salvaged kubectl-plane DTOs.

Salvaged verbatim (field-for-field) from
``reference-code/seedpod/seedpod/providers/kubernetes.py``: ``PodInfo`` (lines 23-35),
``PodDetails`` (38-54), ``NodeInfo`` (57-64), ``DeploymentInfo`` (67-75), ``EventInfo``
(78-91), ``PodWatchEvent`` (94-104), ``_format_age`` (331-370) — docs/design/seam-c-provider.md
§5.3's closing line: "Salvaged DTOs ... copy verbatim to
``seedpod/providers/kube_types.py``."

Two deliberate, documented deviations from byte-identical salvage:

1. **Frozen, not mutable dataclasses.** v1's ``@dataclass`` bodies were plain mutable
   value objects; v2 holds every DTO crossing the provider seam to the same "inert
   value" discipline as ``seedpod/core/reconciliation_intents.py`` (Conflict 6/7's
   "commands are frozen, inert" rule, extended here to their companion Result payloads
   since both travel through the same ``ProviderEvent``/``Observed`` stream).
2. **``_format_age`` takes an explicit ``now`` instead of calling
   ``datetime.now(timezone.utc)`` internally.** The v1 formatting rules (second/minute/
   hour/day/week thresholds, "Unknown" on parse failure or empty input) are unchanged;
   the only change is that "now" is a parameter with a real-clock default at the call
   site's discretion rather than baked in, so the function stays a pure, unit-testable
   transformation (the project's Clock-injection convention, ``seedpod/core/clock.py``,
   extended here for the same reason it exists in ``core/``: deterministic tests, no
   ``Mock``/``patch``). Callers that want v1's exact behavior pass
   ``datetime.now(timezone.utc)`` (or a shared ``SystemClock``) at the call site;
   nothing here calls the wall clock itself.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

__all__ = [
    "PodInfo",
    "PodDetails",
    "NodeInfo",
    "DeploymentInfo",
    "EventInfo",
    "PodWatchEvent",
    "format_age",
]


@dataclass(frozen=True)
class PodInfo:
    """Pod information"""

    name: str
    namespace: str
    status: str
    ready: str  # e.g., "2/2" or "0/2"
    restarts: int
    age: str
    created: str
    node: str
    ip: str
    image: str


@dataclass(frozen=True)
class PodDetails:
    """Detailed pod information"""

    name: str
    namespace: str
    status: str
    age: str
    created: str
    node: str
    ip: str
    host_ip: str
    labels: dict[str, str]
    annotations: dict[str, str]
    conditions: list[dict[str, Any]]
    init_containers: list[dict[str, Any]]
    containers: list[dict[str, Any]]
    volumes: list[dict[str, Any]]


@dataclass(frozen=True)
class NodeInfo:
    """Node information"""

    name: str
    status: str
    roles: str
    age: str
    version: str


@dataclass(frozen=True)
class DeploymentInfo:
    """Deployment information"""

    name: str
    namespace: str
    ready_replicas: int
    desired_replicas: int
    available_replicas: int
    updated_replicas: int


@dataclass(frozen=True)
class EventInfo:
    """Kubernetes event information"""

    namespace: str
    name: str
    type: str  # Normal, Warning
    reason: str
    message: str
    involved_object_kind: str
    involved_object_name: str
    count: int
    first_timestamp: str
    last_timestamp: str
    source_component: str


@dataclass(frozen=True)
class PodWatchEvent:
    """Event from watching pods"""

    event_type: str  # ADDED, MODIFIED, DELETED
    pod_name: str
    namespace: str
    phase: str  # Pending, Running, Succeeded, Failed, Unknown
    ready: str  # e.g., "2/2" or "0/2"
    conditions: list[dict[str, Any]] = field(default_factory=list)
    containers: list[dict[str, Any]] = field(default_factory=list)  # container statuses
    message: str | None = None  # optional status message


def format_age(created_timestamp: str, *, now: datetime) -> str:
    """Convert ISO timestamp to human-readable age (e.g., '2h', '5d', '3w').

    Salvaged verbatim from ``KubectlProvider._format_age``
    (``reference-code/seedpod/seedpod/providers/kubernetes.py:331-370``); see module
    docstring for the one deviation (``now`` is a parameter, not a wall-clock call).

    Args:
        created_timestamp: ISO format timestamp string
        now: the current instant (aware), supplied by the caller

    Returns:
        Human-readable age string, or "Unknown" for empty/unparseable input.
    """
    if not created_timestamp:
        return "Unknown"

    try:
        if created_timestamp.endswith("Z"):
            created = datetime.fromisoformat(created_timestamp[:-1]).replace(tzinfo=now.tzinfo)
        else:
            created = datetime.fromisoformat(created_timestamp).replace(tzinfo=now.tzinfo)

        delta = now - created
        seconds = delta.total_seconds()

        if seconds < 60:
            return f"{int(seconds)}s"
        elif seconds < 3600:
            return f"{int(seconds / 60)}m"
        elif seconds < 86400:
            return f"{int(seconds / 3600)}h"
        elif seconds < 604800:
            return f"{int(seconds / 86400)}d"
        else:
            return f"{int(seconds / 604800)}w"
    except Exception:
        return "Unknown"
