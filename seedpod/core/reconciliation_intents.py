"""Intent dataclasses for the three-phase reconciliation system.

Salvaged from ``reference-code/seedpod/seedpod/core/reconciliation_intents.py``:
``IntentType`` and the four intent classes (Zombie / CreateUnmanaged / StatusSync /
Orphan) with their v1 priorities, default reasons, field sets, and log ``__repr__``
preserved exactly. Providers' ``Reconcile`` command returns
``tuple[ReconciliationIntent, ...]`` (docs/design/seam-c-provider.md §5.3).

v2 changes (docs/design/coherence-review.md §2 — "salvaged intent dataclasses",
inert only):

- Dataclasses are **frozen** — intents are inert values carried across the
  provider seam; nothing mutates them. ``type`` and ``priority`` become
  ``ClassVar``s (they were per-class constants in v1's custom ``__init__``s).
- ``ReconciliationPlan`` NOT ported — Phase A/B/C orchestration lives in
  ``seedpod/runtime/reconciliation.py``; this module holds no reconciler logic.
- ``ProviderReconciliationResult`` NOT ported — dead by design: "backend
  unreachable" is now a raised ``InfrastructureUnreachableError``, never a
  ``.unreachable()`` result (seam-c §5.3 Reconcile, deliberate change #1).
"""

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, ClassVar

__all__ = [
    "IntentType",
    "ReconciliationIntent",
    "ZombieIntent",
    "CreateUnmanagedIntent",
    "StatusSyncIntent",
    "OrphanIntent",
]


class IntentType(StrEnum):
    """Types of reconciliation intents"""
    ZOMBIE = "zombie"                      # Cluster DESTROYED in DB but droplet exists
    CREATE_UNMANAGED = "create_unmanaged"  # Droplet exists but no DB record
    STATUS_SYNC = "status_sync"            # Status drift between DB and DO
    ORPHAN = "orphan"                      # Cluster in DB but no droplet


@dataclass(frozen=True)
class ReconciliationIntent:
    """Base class for reconciliation intents.

    ``type`` and ``priority`` (lower = higher priority) are class constants on
    the concrete intents; ``reason`` defaults per class to the v1 message.
    """
    cluster_id: str

    type: ClassVar[IntentType]
    priority: ClassVar[int]

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(cluster_id={self.cluster_id[:8]}..., reason='{self.reason}')"


@dataclass(frozen=True, repr=False)
class ZombieIntent(ReconciliationIntent):
    """
    Intent to destroy a zombie cluster.

    Zombie = cluster marked DESTROYED in DB but droplet still exists in DO.
    Action: Transition cluster to DESTROYING to trigger destruction job.
    """
    droplet_id: str
    droplet_ip: str | None = None
    reason: str = "Zombie cluster detected - DB says DESTROYED but droplet is active"

    type: ClassVar[IntentType] = IntentType.ZOMBIE
    priority: ClassVar[int] = 1  # Highest priority - immediate destruction


@dataclass(frozen=True, repr=False)
class CreateUnmanagedIntent(ReconciliationIntent):
    """
    Intent to create an UNMANAGED cluster record for an untracked droplet.

    Untracked droplet = droplet exists with UUID tag but no DB record.
    Action: Create cluster record in UNMANAGED state.
    """
    droplet: Any  # Provider instance object (v1: DigitalOcean droplet)
    slug: str | None = None
    reason: str = "Untracked droplet discovered - creating UNMANAGED cluster record"

    type: ClassVar[IntentType] = IntentType.CREATE_UNMANAGED
    priority: ClassVar[int] = 2


@dataclass(frozen=True, repr=False)
class StatusSyncIntent(ReconciliationIntent):
    """
    Intent to sync cluster status from DO to DB.

    Status drift = cluster status in DB differs from droplet status in DO.
    Action: Update DB status to match DO (provider is source of truth).

    Note: FAILED and UNMANAGED states are protected and will not be synced.
    """
    from_status: str
    to_status: str
    reason: str = ""

    type: ClassVar[IntentType] = IntentType.STATUS_SYNC
    priority: ClassVar[int] = 3

    def __post_init__(self) -> None:
        if not self.reason:
            object.__setattr__(
                self, "reason",
                f"Status drift detected: {self.from_status} → {self.to_status}",
            )


@dataclass(frozen=True, repr=False)
class OrphanIntent(ReconciliationIntent):
    """
    Intent to mark an orphaned cluster as DESTROYED.

    Orphaned cluster = cluster exists in DB but no matching droplet in DO.
    Action: Mark cluster DESTROYED (infrastructure is gone).
    """
    reason: str = "Cluster exists in DB but droplet not found in DO"

    type: ClassVar[IntentType] = IntentType.ORPHAN
    priority: ClassVar[int] = 4  # Lowest priority - cleanup
