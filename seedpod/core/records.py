"""Frozen dataclass DTOs the machine transitions. The machine never sees ORM objects
or kubeconfig bytes.

Salvaged from docs/design/seam-a-core.md §E (``seedpod2/core/records.py``; the
``seedpod2`` name is dead per coherence review Conflict 16.1), amended by
docs/design/coherence-review.md Conflict 11:

- ``ClusterRecord.provider_resource_id: str | None`` becomes
  ``provider_resources: Mapping[str, str] = ()`` (JSON map of provisioning
  outputs, fed by ``InfraAllocated``; matches Seam C's ``resource_ids: Mapping``).
- ``DeploymentRecord`` gains ``spec_ref: str | None = None`` (the
  ``DeployRequested.spec_ref`` audit-row pointer) and
  ``resolved_images: Mapping[str, str] = ()`` (was v1's ``services`` column; set
  by ``DeploySucceeded``), alongside the pre-existing ``superseded_by``.
- ``TERMINAL_STATES = ('destroyed', 'failed')`` is exported here — the only
  slug-releasing cluster states (``ux_clusters_slug_live`` in the DDL).

Enum members are underscored Python identifiers; wire values keep v1's hyphens
verbatim (coherence review Conflict 16.11).
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

__all__ = [
    "Origin",
    "ClusterState",
    "ClusterRecord",
    "DeploymentState",
    "DeploymentRecord",
    "TERMINAL_STATES",
]


class Origin(StrEnum):
    MANAGED = "managed"
    DISCOVERED = "discovered"


class ClusterState(StrEnum):  # 10 (v1 had 11); string values keep v1's hyphens verbatim
    NEW = "new"  # pre-persistence; makes birth a real, audited transition (gotcha 15)
    PROVISIONING = "provisioning"  # absorbs v1 CREATING; v1 DEPLOYING is deleted (see disposition table)
    ACTIVE = "active"
    DESTROY_SCHEDULED = "destroy-scheduled"
    DESTROYING = "destroying"
    DESTROYED = "destroyed"
    DESTROY_FAILED = "destroy-failed"
    FAILED = "failed"
    ZOMBIE = "zombie"  # crisp v1 semantics: records say destroyed, provider says running
    UNMANAGED = "unmanaged"


@dataclass(frozen=True, slots=True, kw_only=True)
class ClusterRecord:
    id: str
    name: str
    state: ClusterState
    version: int
    provider: str  # "digitalocean" | "kind" | "tart" | "orbstack"
    environment: str
    origin: Origin
    expires_at: datetime | None = None  # TTL, aware UTC
    public_ip: str | None = None
    kubeconfig_ref: str | None = None  # opaque secret ref -- never kubeconfig bytes
    provider_resources: Mapping[str, str] = ()  # provisioning OUTPUTS; fed by InfraAllocated (Conflict 11)
    pre_destroy_state: ClusterState | None = None  # set on entry to DESTROY_SCHEDULED; cancel returns here
    failure_reason: str | None = None


class DeploymentState(StrEnum):  # v1's raw-string column promoted to a real machine; all 8 statuses kept
    NEW = "new"
    PENDING = "pending"
    DEPLOYING = "deploying"
    ACTIVE = "active"
    FAILED = "failed"
    SUPERSEDED = "superseded"
    CANCELLED = "cancelled"
    REJECTED = "rejected"  # real v1 write sites: presets.py:798, cluster_manager.py:1196
    DESTROYED = "destroyed"


@dataclass(frozen=True, slots=True, kw_only=True)
class DeploymentRecord:
    id: str
    cluster_id: str
    state: DeploymentState
    version: int
    environment: str
    manifest_version: str
    spec_ref: str | None = None  # DeployRequested.spec_ref -- the deployment_audits row (Conflict 11)
    resolved_images: Mapping[str, str] = ()  # was `services`; set by DeploySucceeded (Conflict 11)
    superseded_by: str | None = None
    failure_reason: str | None = None


TERMINAL_STATES = ("destroyed", "failed")  # the only slug-releasing cluster states (Conflict 11)
