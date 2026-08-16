"""Shared test builders for ``tests/core`` (Seam A §K binding test-ergonomics contract).

Record builders (``a_cluster``, ``a_deployment``) plus one canonical event instance
per registered event kind (``AN_EVENT``). ``tests/core`` imports **no**
``unittest.mock`` -- these builders are the entire harness (docs/design/seam-a-core.md
§K; CLAUDE.md testing posture).

Ownership: the totality-test agent (this file's task) owns this module per the task
brief. A placeholder with identical semantics existed here already (written by the
table-effect-tests agent, which needed *a* harness before this one landed); this is
the merged, authoritative version -- same builder signatures, same
class-keyed ``AN_EVENT`` shape, so nothing that already imports it needs to change.
"""

from __future__ import annotations

from datetime import UTC, datetime

from seedpod.core.events import (
    EVENT_REGISTRY,
    AdoptRequested,
    CancelRequested,
    ClusterGone,
    ClusterReady,
    CreateRequested,
    DeployFailed,
    DeploymentPending,
    DeployRejected,
    DeployRequested,
    DeploySucceeded,
    DestroyCancelled,
    DestroyDue,
    DestroyFailed,
    DestroyRequested,
    DestroySucceeded,
    Discovered,
    DiscoveredInfo,
    EndpointReady,
    Event,
    HealthCheckFailed,
    InfraAllocated,
    InfraMissingObserved,
    InfraRunningObserved,
    ProvisionFailed,
    ProvisionSucceeded,
    RetryRequested,
    RollbackFinished,
    SupersededBy,
    TtlExpired,
)
from seedpod.core.records import (
    ClusterRecord,
    ClusterState,
    DeploymentRecord,
    DeploymentState,
    Origin,
)

__all__ = ["AT", "a_cluster", "a_deployment", "AN_EVENT", "assert_an_event_covers_registry"]

# Fixed instant for every canonical event's `at` field -- aware UTC, never wall-clock
# (core is pure; transition() must never be handed a naive datetime).
AT = datetime(2026, 1, 1, tzinfo=UTC)


def a_cluster(state: ClusterState = ClusterState.ACTIVE, **over) -> ClusterRecord:
    """A ClusterRecord with sane defaults for every field; override anything via kwargs.

    Defaults are deliberately state-agnostic (e.g. ``pre_destroy_state=None`` even
    for ``DESTROY_SCHEDULED``) -- tests that care about a specific invariant pass
    it via ``**over``.
    """
    fields = {
        "id": "cluster-1",
        "name": "test-cluster",
        "state": state,
        "version": 3,
        "provider": "digitalocean",
        "environment": "staging",
        "origin": Origin.MANAGED,
        "expires_at": None,
        "public_ip": None,
        "kubeconfig_ref": None,
        "provider_resources": {},
        "pre_destroy_state": None,
        "failure_reason": None,
    }
    fields.update(over)
    return ClusterRecord(**fields)


def a_deployment(state: DeploymentState = DeploymentState.ACTIVE, **over) -> DeploymentRecord:
    """A DeploymentRecord with sane defaults for every field; override anything via kwargs."""
    fields = {
        "id": "deployment-1",
        "cluster_id": "cluster-1",
        "state": state,
        "version": 3,
        "environment": "staging",
        "manifest_version": "v1",
        "spec_ref": None,
        "resolved_images": {},
        "superseded_by": None,
        "failure_reason": None,
    }
    fields.update(over)
    return DeploymentRecord(**fields)


# One canonical instance per registered event kind. Keys are the classes themselves
# (not strings) so tests can index directly: AN_EVENT[ProvisionSucceeded]. Actors are
# chosen to be *legal* wherever the actor grammar constrains them (Observations get a
# privileged actor) so AN_EVENT represents a valid fact by default; privilege/guard
# violations are constructed ad hoc in the tests that specifically exercise them.
AN_EVENT: dict[type, Event] = {
    CreateRequested: CreateRequested(at=AT, actor="api:alice"),
    Discovered: Discovered(
        at=AT,
        actor="reconciler",
        observed=DiscoveredInfo(
            provider="digitalocean", public_ip="203.0.113.9", provider_resources={"droplet_id": "999"}
        ),
    ),
    RetryRequested: RetryRequested(at=AT, actor="api:alice"),
    AdoptRequested: AdoptRequested(at=AT, actor="api:alice"),
    DestroyRequested: DestroyRequested(at=AT, actor="api:alice"),
    DestroyCancelled: DestroyCancelled(at=AT, actor="api:alice"),
    TtlExpired: TtlExpired(at=AT, actor="timer:ttl"),
    DestroyDue: DestroyDue(at=AT, actor="timer:destroy"),
    ProvisionSucceeded: ProvisionSucceeded(
        at=AT, actor="engine:run:1", public_ip="203.0.113.10", kubeconfig_ref="cluster-kubeconfig:cluster-1"
    ),
    ProvisionFailed: ProvisionFailed(at=AT, actor="engine:run:1", reason="boom"),
    DestroySucceeded: DestroySucceeded(at=AT, actor="engine:run:1"),
    DestroyFailed: DestroyFailed(at=AT, actor="engine:run:1", reason="boom"),
    InfraRunningObserved: InfraRunningObserved(at=AT, actor="reconciler"),
    InfraMissingObserved: InfraMissingObserved(at=AT, actor="reconciler"),
    HealthCheckFailed: HealthCheckFailed(at=AT, actor="health", reason="unresponsive"),
    InfraAllocated: InfraAllocated(at=AT, actor="engine:run:1", resource_ids={"droplet_id": "999"}),
    EndpointReady: EndpointReady(at=AT, actor="engine:run:1", public_ip="203.0.113.10"),
    DeployRequested: DeployRequested(at=AT, actor="api:alice", spec_ref="audit-1"),
    DeployRejected: DeployRejected(at=AT, actor="api:alice", reason="rule engine said no"),
    CancelRequested: CancelRequested(at=AT, actor="api:alice"),
    ClusterReady: ClusterReady(at=AT, actor="cluster-machine"),
    DeploySucceeded: DeploySucceeded(at=AT, actor="engine:run:1", resolved_images={"api": "sha256:abc"}),
    DeployFailed: DeployFailed(at=AT, actor="engine:run:1", reason="boom"),
    SupersededBy: SupersededBy(at=AT, actor="cluster-machine", new_deployment_id="deployment-2"),
    ClusterGone: ClusterGone(at=AT, actor="cluster-machine"),
    # DR-0031 E1: the cluster-side "a deployment is waiting on you". actor is the
    # ORIGINAL requester's, not "cluster-machine" -- Dispatcher.apply's escalation
    # forwards provenance so the cluster's audit row names who asked.
    DeploymentPending: DeploymentPending(at=AT, actor="api:alice", deployment_id="deployment-1"),
    RollbackFinished: RollbackFinished(at=AT, actor="engine:run:1", ok=True),
}


def assert_an_event_covers_registry() -> None:
    """Meta-check (Seam A §K): AN_EVENT has exactly one canonical instance per
    registered event kind -- no registered event forgotten, no stale entry left
    behind. Exposed as a function so ``test_totality.py`` can run it as an
    explicit, reportable test in addition to the import-time assertion below."""
    an_event_classes = set(AN_EVENT)
    registered_classes = set(EVENT_REGISTRY.values())
    missing = registered_classes - an_event_classes
    extra = an_event_classes - registered_classes
    assert not missing, f"AN_EVENT is missing canonical instances for: {sorted(c.__name__ for c in missing)}"
    assert not extra, f"AN_EVENT has stale entries not in EVENT_REGISTRY: {sorted(c.__name__ for c in extra)}"
    for cls, event in AN_EVENT.items():
        assert type(event) is cls, f"AN_EVENT[{cls.__name__}] holds a {type(event).__name__} instance"


# Run the meta-check at import time -- a forgotten event kind fails immediately,
# not only when a test happens to invoke assert_an_event_covers_registry().
assert_an_event_covers_registry()
