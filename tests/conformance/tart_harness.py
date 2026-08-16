"""tests/conformance/tart_harness.py — ``Harness`` implementation for the ``tart`` provider
(Seam C §5.6), backed by ``tests/conformance/fake_tart.py``.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from uuid import uuid4

from seedpod.core.cluster_spec import ClusterConfiguration, ClusterSpecification, NodeSpecification
from seedpod.core.errors import (
    ErrorCode,
    InfrastructureUnreachableError,
    PermanentError,
    ProviderError,
)
from seedpod.providers.contract import (
    CreateInstance,
    FetchKubeconfig,
    ProbeInstance,
    Provider,
    ProviderCommand,
)
from seedpod.providers.tart import TartConfig, TartProvider
from tests.conformance.fake_tart import FakeTartBackend, FakeTartTransport
from tests.conformance.harness import Fault, ReconcileCase

__all__ = ["TartHarness"]

_BASE_IMAGE = "local-dev-base-rosetta"
_SEED_VM = "seedpod-seed"


class TartHarness:
    name = "tart"

    def __init__(self) -> None:
        self.backend = FakeTartBackend()
        # Pre-seed one already-running, fully-networked VM for observe_command() ("cheapest
        # state read" against a resource that already exists — no create round-trip per test).
        self.backend.seed_vm(_SEED_VM, running=True, ip="192.168.64.10")

    # ------------------------------------------------------------------
    # Harness protocol
    # ------------------------------------------------------------------

    def provider(self, *faults: Fault) -> Provider:
        transport = FakeTartTransport(self.backend, frozenset(faults))
        config = TartConfig(base_image_name=_BASE_IMAGE, node_size_mapping={"1,1": {"memory_mb": 2048, "cpu_cores": 1, "disk_gb": 30}})
        return TartProvider(config, transport)

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
        return ProbeInstance(resource_ids={"tart_vm_name": _SEED_VM})

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
            # tart has no CreateUnmanagedIntent analogue — a bare untracked VM name cannot be
            # mapped back to a cluster_uuid (module docstring's _reconcile note, mirrors kind).
            ReconcileCase(
                name="untagged_backend_no_db_row", db_status=None, backend_present=True, backend_tagged=False, expected_intent=None
            ),
        )

    def rewrite_cases(self) -> Sequence[tuple[str, FetchKubeconfig, str]]:
        return ()  # tart does not implement FetchKubeconfig (§5.4 plane matrix)

    def classification_command(self, fault: Fault) -> ProviderCommand:
        """C-04/C-17's representative command per fault (additive Harness extension — mined
        verbatim from ``test_tart_smoke.py``'s ``test_classification_table``): MISSING_SOURCE
        only manifests on the create path (clone's source-not-found symptom); every other
        fault is visible on the cheapest read."""
        if fault == Fault.MISSING_SOURCE:
            return self.create_command()
        return self.observe_command()

    def classification_cases(self) -> Sequence[tuple[Fault, type[ProviderError], ErrorCode]]:
        # RATE_LIMIT and AUTH have no literal or structural equivalent for a purely local CLI
        # (no HTTP calls, no credentials) — left unhandled in FakeTartTransport, omitted here,
        # mirroring kind_harness.py's identical omission pattern for faults without an analogue.
        return (
            (Fault.UNREACHABLE, InfrastructureUnreachableError, ErrorCode.API_TIMEOUT),
            (Fault.TRANSIENT_ONCE, InfrastructureUnreachableError, ErrorCode.API_TIMEOUT),
            # MISSING_SOURCE: unlike kind (where a vanished mid-command binary is a DIFFERENT
            # (class, code) pair from check_ready's Permanent/NOT_FOUND), tart's row 2
            # (check_ready, base image absent) and row 4 (clone, source image not found) share
            # the exact same (PermanentError, NOT_FOUND) pair — so this fault has one meaning
            # everywhere and belongs in this table.
            (Fault.MISSING_SOURCE, PermanentError, ErrorCode.NOT_FOUND),
        )
