"""tests/conformance/kind_harness.py — ``Harness`` implementation for the ``kind`` provider
(Seam C §5.6), backed by ``tests/conformance/fake_kind.py``.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from uuid import uuid4

from seedpod.core.cluster_spec import ClusterConfiguration, ClusterSpecification, NodeSpecification
from seedpod.core.errors import ErrorCode, InfrastructureUnreachableError, ProviderError
from seedpod.providers.contract import (
    CreateInstance,
    FetchKubeconfig,
    ListInstances,
    ProbeInstance,
    Provider,
    ProviderCommand,
)
from seedpod.providers.kind import KindConfig, KindProvider
from tests.conformance.fake_kind import FakeKindBackend, FakeKindTransport
from tests.conformance.harness import Fault, ReconcileCase

__all__ = ["KindHarness"]

_SEED_CLUSTER = "seedpod-seed"


class KindHarness:
    name = "kind"

    def __init__(self) -> None:
        self.backend = FakeKindBackend()
        # Pre-seed one already-running cluster for observe_command() ("cheapest state read"
        # against a resource that already exists — no create round-trip needed per test).
        self.backend.seed_cluster(_SEED_CLUSTER, port=6443, running=True)

    # ------------------------------------------------------------------
    # Harness protocol
    # ------------------------------------------------------------------

    def provider(self, *faults: Fault) -> Provider:
        transport = FakeKindTransport(self.backend, frozenset(faults))
        config = KindConfig(
            api_server_host="minimax.local",
            node_size_mapping={"1,1": 0, "2,4": 1},
        )
        return KindProvider(config, transport)

    @contextmanager
    def broken_environment(self) -> Iterator[Provider]:
        yield self.provider(Fault.MISSING_SOURCE)

    async def backend_resources(self) -> frozenset[str]:
        return self.backend.present_names()

    def backend_attempts(self) -> int:
        return self.backend.attempt_count

    def create_command(self) -> CreateInstance:
        cluster_uuid = str(uuid4())
        slug = "demo-cluster"
        return CreateInstance(
            cluster_uuid=cluster_uuid,
            slug=slug,
            spec=ClusterSpecification(
                node_specification=NodeSpecification(cpu_cores=1, memory_gb=1, region_hint="europe-west"),
                cluster_config=ClusterConfiguration(),
            ),
            pod_cidr="10.42.7.0/24",
            service_cidr="10.43.7.0/24",
            tags=(f"cluster-uuid:{cluster_uuid}", f"cluster-{slug}", "ttl-4"),
        )

    def observe_command(self) -> ProviderCommand:
        return ProbeInstance(resource_ids={"kind_cluster_name": _SEED_CLUSTER, "api_port": "6443"})

    def reconcile_truth_table(self) -> Sequence[ReconcileCase]:
        return (
            ReconcileCase(name="active_db_missing_backend", db_status="active", backend_present=False, expected_intent="orphan"),
            ReconcileCase(
                name="active_db_present_but_stopped", db_status="active", backend_present=True, expected_intent="orphan"
            ),
            ReconcileCase(
                name="destroyed_db_present_backend", db_status="destroyed", backend_present=True, expected_intent="zombie"
            ),
            ReconcileCase(
                name="destroying_db_missing_backend", db_status="destroying", backend_present=False, expected_intent="orphan"
            ),
            # kind has no CreateUnmanagedIntent analogue — a bare untracked kind cluster name
            # cannot be mapped back to a cluster_uuid (module docstring's _reconcile note).
            ReconcileCase(
                name="untagged_backend_no_db_row", db_status=None, backend_present=True, backend_tagged=False, expected_intent=None
            ),
        )

    def rewrite_cases(self) -> Sequence[tuple[str, FetchKubeconfig, str]]:
        return (
            (
                "kind_rewrites_127_0_0_1_host_and_port",
                FetchKubeconfig(
                    rewrite_server_to="minimax.local", resource_ids={"kind_cluster_name": _SEED_CLUSTER, "api_port": "6443"}
                ),
                r"https://minimax\.local:6443",
            ),
            (
                "kind_rewrites_0_0_0_0_host_and_port_distinct_from_source",
                FetchKubeconfig(
                    rewrite_server_to="minimax.local", resource_ids={"kind_cluster_name": _SEED_CLUSTER, "api_port": "6500"}
                ),
                r"https://minimax\.local:6500",
            ),
            (
                "kind_no_rewrite_when_target_empty",
                FetchKubeconfig(rewrite_server_to="", resource_ids={"kind_cluster_name": _SEED_CLUSTER, "api_port": "6443"}),
                r"https://0\.0\.0\.0:6443",
            ),
        )

    def classification_command(self, fault: Fault) -> ProviderCommand:
        """C-04/C-17's representative command per fault (additive Harness extension: the
        Protocol has no generic hook for "which command exercises this fault" since it
        varies per provider — mined verbatim from ``test_kind_smoke.py``'s
        ``test_classification_table``, which always uses ``ListInstances()``)."""
        return ListInstances()

    def classification_cases(self) -> Sequence[tuple[Fault, type[ProviderError], ErrorCode]]:
        # Fault.MISSING_SOURCE deliberately absent here: it is a check_ready-only concept for
        # kind (row 20's "refuse to start" — see broken_environment() above). Hit mid-command
        # instead of at check_ready, a vanished `kind` binary is legitimately
        # Unreachable/DAEMON_UNREACHABLE via the shared classify_subprocess path (the binary
        # might come back — "cannot determine state", not "permanently misconfigured"), not
        # the Permanent/NOT_FOUND check_ready gives it; there is no single (class, code) pair
        # both meanings share, so this fault stays a broken_environment()-only signal (mirrors
        # ssh_k3s_harness.py's identical omission of MISSING_SOURCE from this table).
        return (
            (Fault.UNREACHABLE, InfrastructureUnreachableError, ErrorCode.ENDPOINT_UNREACHABLE),
            (Fault.TRANSIENT_ONCE, InfrastructureUnreachableError, ErrorCode.ENDPOINT_UNREACHABLE),
        )
