"""tests/conformance/digitalocean_harness.py — ``Harness`` implementation for the
``digitalocean`` provider (Seam C §5.6), backed by ``tests/conformance/fake_digitalocean.py``.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from uuid import uuid4

import httpx

from seedpod.core.cluster_spec import ClusterConfiguration, ClusterSpecification, NodeSpecification
from seedpod.core.errors import (
    ErrorCode,
    InfrastructureUnreachableError,
    PermanentError,
    ProviderError,
    TransientError,
)
from seedpod.providers.contract import (
    CreateInstance,
    FetchKubeconfig,
    ProbeInstance,
    Provider,
    ProviderCommand,
)
from seedpod.providers.digitalocean import DigitalOceanConfig, DigitalOceanProvider
from tests.conformance.fake_digitalocean import FakeDigitalOceanBackend, FakeDigitalOceanTransport
from tests.conformance.harness import Fault, ReconcileCase

__all__ = ["DigitalOceanHarness"]

_PROJECT_ID = "proj-exampleco"


class DigitalOceanHarness:
    name = "digitalocean"

    def __init__(self) -> None:
        self.backend = FakeDigitalOceanBackend()
        # Pre-seed one already-"active" droplet for observe_command() ("cheapest state read"
        # against a resource that already exists — no create round-trip needed per test).
        self._seed_droplet_id = self.backend.seed_droplet(
            tags=["seedpod-managed", "k3s-cluster", "cluster-uuid:seed-uuid", "cluster-seed"]
        )

    # ------------------------------------------------------------------
    # Harness protocol
    # ------------------------------------------------------------------

    def provider(self, *faults: Fault) -> Provider:
        transport = FakeDigitalOceanTransport(self.backend, frozenset(faults))
        client = httpx.AsyncClient(transport=transport)
        config = DigitalOceanConfig(
            api_token="fake-token",  # pragma: allowlist secret
            project_id=_PROJECT_ID,
            region_mapping={"europe-west": "ams3"},
            node_size_mapping={"1,1": "s-1vcpu-1gb", "2,4": "s-2vcpu-4gb"},
        )
        return DigitalOceanProvider(config, client)

    @contextmanager
    def broken_environment(self) -> Iterator[Provider]:
        yield self.provider(Fault.AUTH)

    async def backend_resources(self) -> frozenset[str]:
        return frozenset(self.backend.droplets.keys())

    def backend_attempts(self) -> int:
        return self.backend.call_count

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
        return ProbeInstance(resource_ids={"droplet_id": self._seed_droplet_id})

    def reconcile_truth_table(self) -> Sequence[ReconcileCase]:
        return (
            ReconcileCase(name="active_db_missing_backend", db_status="active", backend_present=False, expected_intent="orphan"),
            ReconcileCase(
                name="destroyed_db_present_backend", db_status="destroyed", backend_present=True, expected_intent="zombie"
            ),
            ReconcileCase(
                name="destroying_db_missing_backend", db_status="destroying", backend_present=False, expected_intent="orphan"
            ),
            ReconcileCase(
                name="untagged_backend_no_db_row",
                db_status=None,
                backend_present=True,
                backend_tagged=False,
                expected_intent=None,
            ),
            ReconcileCase(
                name="tagged_backend_no_db_row",
                db_status=None,
                backend_present=True,
                backend_tagged=True,
                expected_intent="create_unmanaged",
            ),
        )

    def rewrite_cases(self) -> Sequence[tuple[str, FetchKubeconfig, str]]:
        return ()  # digitalocean does not implement FetchKubeconfig (§5.4 plane matrix)

    def classification_command(self, fault: Fault) -> ProviderCommand:
        """C-04/C-17's representative command per fault (additive Harness extension — mined
        verbatim from ``test_digitalocean_smoke.py``'s ``test_classification_table``):
        MISSING_SOURCE (ssh key lookup) and DIE_MID_CREATE (post-allocation project assign)
        only manifest on the create path; every other fault is visible on the cheapest read."""
        if fault in (Fault.MISSING_SOURCE, Fault.DIE_MID_CREATE):
            return self.create_command()
        return self.observe_command()

    def classification_cases(self) -> Sequence[tuple[Fault, type[ProviderError], ErrorCode]]:
        return (
            (Fault.UNREACHABLE, InfrastructureUnreachableError, ErrorCode.API_TIMEOUT),
            (Fault.AUTH, PermanentError, ErrorCode.AUTH),
            (Fault.RATE_LIMIT, TransientError, ErrorCode.RATE_LIMITED),
            (Fault.TRANSIENT_ONCE, TransientError, ErrorCode.API_5XX),
            (Fault.MISSING_SOURCE, PermanentError, ErrorCode.NOT_FOUND),
            (Fault.DIE_MID_CREATE, InfrastructureUnreachableError, ErrorCode.ENDPOINT_UNREACHABLE),
        )
