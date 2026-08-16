"""Provider-agnostic cluster specification and deterministic CIDR allocation.

Salvaged from ``reference-code/seedpod/seedpod/core/cluster_spec.py``:

- ``NodeSpecification``, ``ClusterConfiguration``, ``ClusterSpecification`` — verbatim.
- ``allocate_cluster_cidrs()`` — bit-identical (Tailscale-critical hash allocation;
  PLAN-refactor salvage table "CIDR allocation").

Deliberately NOT ported from the v1 module (see docs/design/coherence-review.md §2 and
docs/design/seam-c-provider.md):

- ``ClusterStatus`` — cluster states are owned by ``seedpod/core/records.py``
  (``ClusterState``); this module never defines state.
- ``ClusterInfo``, ``CloudProvider`` ABC, ``ClusterCreationError``,
  ``ClusterNotFoundError`` — replaced wholesale by the Seam C provider contract
  (``seedpod/providers/contract.py``) and the single error taxonomy.
- ``InfrastructureUnreachableError`` class body — the taxonomy's single home is
  ``seedpod/core/errors.py``; re-exported here for plan-letter fidelity
  (coherence review Conflict 6).
- ``generate_cluster_id`` / ``generate_cluster_slug`` / ``generate_cluster_name`` —
  nondeterministic (uuid/random); id/slug generation is injected at the composition
  root (``id_gen`` test seam), not core.
- ``create_cluster_spec_from_template`` — did logging IO; its job is the
  ``cluster.load_spec`` engine domain step (coherence review Conflict 10).
"""

from datetime import datetime, timedelta
from typing import Any

from pydantic import BaseModel, Field, field_validator

from seedpod.core.errors import (
    InfrastructureUnreachableError,  # noqa: F401  (re-export, Conflict 6)
)

__all__ = [
    "NodeSpecification",
    "ClusterConfiguration",
    "ClusterSpecification",
    "InfrastructureUnreachableError",
    "allocate_cluster_cidrs",
]


class NodeSpecification(BaseModel):
    """
    Provider-agnostic specification for cluster nodes.

    These generic specifications get translated to provider-specific
    instance types by the cloud provider implementation.
    """
    cpu_cores: int = Field(..., ge=1, le=64, description="Number of CPU cores")
    memory_gb: int = Field(..., ge=1, le=256, description="Memory in gigabytes")
    disk_gb: int = Field(default=50, ge=20, le=2000, description="Disk size in gigabytes")
    region_hint: str = Field(..., description="Geographic region preference (europe-west, us-east, etc.)")

    @field_validator('region_hint')
    @classmethod
    def validate_region_hint(cls, v: str) -> str:
        """Validate region hint format"""
        valid_regions = [
            "europe-west", "europe-central", "europe-north",
            "us-east", "us-west", "us-central",
            "asia-pacific", "asia-southeast"
        ]
        if v not in valid_regions:
            raise ValueError(f"Region hint must be one of: {', '.join(valid_regions)}")
        return v


class ClusterConfiguration(BaseModel):
    """
    Configuration for the entire cluster.
    """
    node_count: int = Field(default=1, ge=1, le=10, description="Number of nodes in the cluster")
    ttl_hours: float | None = Field(default=None, ge=0.01, le=8760, description="TTL in hours for ephemeral clusters (supports fractions)")
    tags: list[str] = Field(default_factory=list, description="Tags to apply to cluster resources")
    kubernetes_version: str = Field(default="stable", description="Kubernetes/K3s version")
    ingress_strategy: dict[str, Any] | None = Field(default=None, description="Ingress configuration (traefik, nodeport, or none)")
    pod_cidr: str | None = Field(default=None, description="Pod network CIDR (defaults to K3s default 10.42.0.0/16)")
    service_cidr: str | None = Field(default=None, description="Service network CIDR (defaults to K3s default 10.43.0.0/16)")


class ClusterSpecification(BaseModel):
    """
    Complete cluster specification combining node and cluster configuration.
    """
    node_specification: NodeSpecification
    cluster_config: ClusterConfiguration

    def is_ephemeral(self) -> bool:
        """Check if this is an ephemeral cluster with TTL"""
        return self.cluster_config.ttl_hours is not None

    def expires_at(self, created_at: datetime) -> datetime | None:
        """Calculate expiration time if TTL is set.

        ``created_at`` must be timezone-aware (core bans naive datetimes); the
        result carries the same tzinfo.
        """
        if self.cluster_config.ttl_hours:
            return created_at + timedelta(hours=self.cluster_config.ttl_hours)
        return None


def allocate_cluster_cidrs(cluster_id: str) -> tuple[str, str]:
    """
    Allocate unique pod and service CIDRs for a cluster based on its ID.

    Uses a hash of the cluster UUID to generate unique /24 subnets within
    the 10.42.0.0/16 and 10.43.0.0/16 ranges. This ensures:
    - Each cluster gets unique IP ranges (no conflicts in Tailscale routing)
    - Deterministic allocation (same cluster ID always gets same CIDRs)
    - Support for up to 256 concurrent clusters
    - Each cluster can have up to 254 pods and 254 services

    Args:
        cluster_id: Cluster UUID (e.g., "3c8cf9ed-8229-45b1-a188-7cdcd726fe02")

    Returns:
        Tuple of (pod_cidr, service_cidr)

    Example:
        >>> allocate_cluster_cidrs("3c8cf9ed-8229-45b1-a188-7cdcd726fe02")
        ('10.42.60.0/24', '10.43.60.0/24')
    """
    # Extract first segment of UUID and convert to integer
    # Use modulo to get a value 0-255 for the subnet
    first_segment = cluster_id.split('-')[0]
    subnet_id = int(first_segment, 16) % 256

    pod_cidr = f"10.42.{subnet_id}.0/24"
    service_cidr = f"10.43.{subnet_id}.0/24"

    return pod_cidr, service_cidr
