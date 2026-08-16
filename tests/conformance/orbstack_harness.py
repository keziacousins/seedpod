"""tests/conformance/orbstack_harness.py — ``Harness`` implementation for the ``orbstack``
provider (Seam C §5.6), backed by ``tests/conformance/fake_orbstack.py``.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from uuid import uuid4

from seedpod.core.cluster_spec import ClusterConfiguration, ClusterSpecification, NodeSpecification
from seedpod.core.errors import (
    ErrorCode,
    InfrastructureUnreachableError,
    ProviderError,
)
from seedpod.providers.contract import (
    CreateInstance,
    FetchKubeconfig,
    ListInstances,
    ProbeInstance,
    Provider,
    ProviderCommand,
)
from seedpod.providers.orbstack import ORBSTACK_CONTEXT, OrbstackConfig, OrbstackProvider
from tests.conformance.fake_orbstack import FakeOrbstackBackend, FakeOrbstackTransport
from tests.conformance.harness import Fault, ReconcileCase

__all__ = ["OrbstackHarness"]


class OrbstackHarness:
    name = "orbstack"

    def __init__(self) -> None:
        self.backend = FakeOrbstackBackend()

    # ------------------------------------------------------------------
    # Harness protocol
    # ------------------------------------------------------------------

    def provider(self, *faults: Fault) -> Provider:
        transport = FakeOrbstackTransport(self.backend, frozenset(faults))
        config = OrbstackConfig(context=ORBSTACK_CONTEXT, host="minimax.local")
        return OrbstackProvider(config, transport)

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
        return ProbeInstance(resource_ids={"orbstack_context": ORBSTACK_CONTEXT})

    def reconcile_truth_table(self) -> Sequence[ReconcileCase]:
        # orbstack "never orphans" (module docstring): every db_status, crossed with a REACHABLE
        # backend, expects no intent at all — the whole point of this table for orbstack is to
        # demonstrate the invariant holds across statuses, not to vary backend presence (there is
        # no per-cluster backend presence to vary — see the module docstring's closing
        # paragraph). `backend_present=False` (global unreachability) is deliberately NOT a row
        # here: for orbstack that means "reconcile raises", not "reconcile returns an intent", so
        # it belongs to C-14 (`test_reconcile_unreachable_touches_nothing`, exercised directly via
        # Fault.UNREACHABLE below) rather than this Result-shaped truth table.
        return (
            ReconcileCase(name="active_db_reachable_backend", db_status="active", backend_present=True, expected_intent=None),
            ReconcileCase(
                name="destroyed_db_reachable_backend", db_status="destroyed", backend_present=True, expected_intent=None
            ),
            ReconcileCase(
                name="destroying_db_reachable_backend", db_status="destroying", backend_present=True, expected_intent=None
            ),
        )

    def rewrite_cases(self) -> Sequence[tuple[str, FetchKubeconfig, str]]:
        return (
            (
                "orbstack_rewrites_127_0_0_1_host_preserves_source_port",
                FetchKubeconfig(rewrite_server_to="minimax.local", resource_ids={"orbstack_context": ORBSTACK_CONTEXT}),
                rf"https://minimax\.local:{self.backend.api_port}",
            ),
            (
                "orbstack_no_rewrite_when_target_empty",
                FetchKubeconfig(rewrite_server_to="", resource_ids={"orbstack_context": ORBSTACK_CONTEXT}),
                rf"https://127\.0\.0\.1:{self.backend.api_port}",
            ),
        )

    def classification_command(self, fault: Fault) -> ProviderCommand:
        """C-04/C-17's representative command per fault (additive Harness extension: mined
        verbatim from ``test_orbstack_smoke.py``'s ``test_classification_table``, which
        always uses ``ListInstances()``)."""
        return ListInstances()

    def classification_cases(self) -> Sequence[tuple[Fault, type[ProviderError], ErrorCode]]:
        # Fault.MISSING_SOURCE deliberately absent here: it is a check_ready-only/
        # broken_environment()-only signal for orbstack (the `orbstack` context missing from the
        # local kubeconfig — a structural "refuse to start" condition, module docstring's
        # opening paragraph), producing PermanentError(SCRIPT_FAILED) rather than a code this
        # table would need to share with a different meaning elsewhere — mirrors
        # kind_harness.py's/ssh_k3s_harness.py's identical omission pattern.
        return (
            (Fault.UNREACHABLE, InfrastructureUnreachableError, ErrorCode.ENDPOINT_UNREACHABLE),
            (Fault.TRANSIENT_ONCE, InfrastructureUnreachableError, ErrorCode.ENDPOINT_UNREACHABLE),
        )
