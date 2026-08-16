"""tests/conformance/test_digitalocean_smoke.py — smoke coverage proving the
``digitalocean`` provider streams per Seam C §5.2 against its fake transport, and that the
``DigitalOceanHarness`` is wired correctly. The full parametrized C-01..C-24 suite is written
by a later agent against ``tests/conformance/harness.Harness``; this file is deliberately a
narrower, provider-local proof (stream shape, C1 close, absence-vs-unreachable, destroy
vocabulary, reconcile mapping, classification table, unsupported-command rejection) so that
agent's suite has a known-good provider to slot in against.

No ``Mock``/``patch`` anywhere — every fault is injected at ``FakeDigitalOceanTransport``.
"""

from __future__ import annotations

from uuid import uuid4

import pytest

from seedpod.core.cluster_spec import ClusterConfiguration, ClusterSpecification, NodeSpecification
from seedpod.core.errors import InfrastructureUnreachableError, PermanentError, ProviderError
from seedpod.core.reconciliation_intents import CreateUnmanagedIntent, OrphanIntent, ZombieIntent
from seedpod.providers.compensation import undo_for
from seedpod.providers.contract import (
    ApplyFirewalls,
    AssignToProject,
    ClusterSnapshot,
    CreateInstance,
    DestroyInstance,
    DestroyStatus,
    FetchKubeconfig,
    InstanceCreated,
    InstanceState,
    Observed,
    ProbeDestruction,
    ProbeInstance,
    Progress,
    Reconcile,
    Result,
)
from tests.conformance.digitalocean_harness import DigitalOceanHarness
from tests.conformance.harness import Fault

pytestmark = pytest.mark.asyncio


async def _drain(provider, cmd):
    events = []
    async for ev in provider.execute(cmd):
        events.append(ev)
    return events


def _fold_resource_ids(events: list) -> dict[str, str]:
    """Mirrors ``engine/provider_step.py``'s ``ctx.note(**{k: str(v) for k, v in
    d.get("resource_ids", {}).items()})`` fold (Conflict 7) without importing the engine."""
    notes: dict[str, str] = {}
    for ev in events:
        if isinstance(ev, Progress) and ev.phase == "resource-allocated":
            notes.update({str(k): str(v) for k, v in ev.data.get("resource_ids", {}).items()})
    return notes


# ---------------------------------------------------------------------------
# check_ready / C-01
# ---------------------------------------------------------------------------


async def test_check_ready_succeeds_against_healthy_backend():
    harness = DigitalOceanHarness()
    provider = harness.provider()
    await provider.check_ready()  # must not raise


async def test_check_ready_fails_fast_on_broken_environment():
    harness = DigitalOceanHarness()
    with harness.broken_environment() as provider:
        with pytest.raises(PermanentError):
            await provider.check_ready()


# ---------------------------------------------------------------------------
# stream shape / C-02
# ---------------------------------------------------------------------------


async def test_create_stream_shape_progress_then_result():
    harness = DigitalOceanHarness()
    provider = harness.provider()
    events = await _drain(provider, harness.create_command())

    assert events, "create must yield at least the terminal Result"
    *progress_events, terminal = events
    assert all(isinstance(ev, Progress) for ev in progress_events)
    assert isinstance(terminal, Result)
    assert progress_events, "CreateInstance MUST emit Progress(RESOURCE_ALLOCATED)"
    assert progress_events[0].phase == "resource-allocated"
    assert isinstance(terminal.value, InstanceCreated)
    assert terminal.value.resource_ids == progress_events[0].data["resource_ids"]


async def test_probe_stream_shape_result_only():
    harness = DigitalOceanHarness()
    provider = harness.provider()
    events = await _drain(provider, harness.observe_command())
    assert len(events) == 1
    assert isinstance(events[0], Result)
    assert isinstance(events[0].value, InstanceState)


# ---------------------------------------------------------------------------
# create idempotency / C-07, C-08
# ---------------------------------------------------------------------------


async def test_create_idempotent_reinvocation_adopts_not_duplicates():
    harness = DigitalOceanHarness()
    provider = harness.provider()
    cmd = harness.create_command()

    first = await _drain(provider, cmd)
    before = await harness.backend_resources()

    second = await _drain(provider, cmd)
    after = await harness.backend_resources()

    first_result = next(ev.value for ev in first if isinstance(ev, Result))
    second_result = next(ev.value for ev in second if isinstance(ev, Result))

    assert first_result.adopted_existing is False
    assert second_result.adopted_existing is True
    assert second_result.resource_ids == first_result.resource_ids
    assert before == after, "re-invocation must not create a duplicate backend resource"


# ---------------------------------------------------------------------------
# absence vs unreachable / C-05, C-06
# ---------------------------------------------------------------------------


async def test_probe_instance_absence_is_data():
    harness = DigitalOceanHarness()
    provider = harness.provider()
    events = await _drain(provider, ProbeInstance(resource_ids={"droplet_id": "does-not-exist"}))
    (result,) = events
    assert result.value.phase == "absent"


async def test_probe_instance_unreachable_raises_never_absent():
    harness = DigitalOceanHarness()
    provider = harness.provider(Fault.UNREACHABLE)
    with pytest.raises(InfrastructureUnreachableError) as excinfo:
        await _drain(provider, harness.observe_command())
    assert excinfo.value.host == "api.digitalocean.com"


# ---------------------------------------------------------------------------
# destroy vocabulary / C-10, C-11, C-12
# ---------------------------------------------------------------------------


async def test_destroy_idempotent_on_absent_twice():
    harness = DigitalOceanHarness()
    provider = harness.provider()
    cmd = DestroyInstance(slug="ghost", resource_ids={"droplet_id": "never-existed"})

    for _ in range(2):
        events = await _drain(provider, cmd)
        (result,) = events
        assert result.value.status == DestroyStatus.DESTROYED


async def test_destroy_never_lies_when_unreachable():
    harness = DigitalOceanHarness()
    create_events = await _drain(harness.provider(), harness.create_command())
    resource_ids = next(ev.value for ev in create_events if isinstance(ev, Result)).resource_ids

    broken = harness.provider(Fault.UNREACHABLE)
    with pytest.raises(InfrastructureUnreachableError):
        await _drain(broken, DestroyInstance(slug="demo-cluster", resource_ids=resource_ids))


async def test_probe_destruction_vocabulary():
    harness = DigitalOceanHarness()
    provider = harness.provider()

    stuck_id = harness.backend.seed_droplet(tags=["seedpod-managed"], status="active")
    harness.backend.mark_stuck_active(stuck_id)
    (stuck,) = await _drain(provider, ProbeDestruction(resource_ids={"droplet_id": stuck_id}))
    assert stuck.value.status == DestroyStatus.DESTROY_FAILED
    assert stuck.value.stuck_resources == (stuck_id,)

    destroying_id = harness.backend.seed_droplet(tags=["seedpod-managed"], status="archive")
    (destroying,) = await _drain(provider, ProbeDestruction(resource_ids={"droplet_id": destroying_id}))
    assert destroying.value.status == DestroyStatus.DESTROYING

    (gone,) = await _drain(provider, ProbeDestruction(resource_ids={"droplet_id": "long-gone"}))
    assert gone.value.status == DestroyStatus.DESTROYED


# ---------------------------------------------------------------------------
# the C1 close / C-09
# ---------------------------------------------------------------------------


async def test_undo_after_partial_create_cleans_backend():
    harness = DigitalOceanHarness()
    provider = harness.provider(Fault.DIE_MID_CREATE)
    cmd = harness.create_command()

    events = []
    with pytest.raises(ProviderError):
        async for ev in provider.execute(cmd):
            events.append(ev)

    notes = _fold_resource_ids(events)
    assert notes, "RESOURCE_ALLOCATED must have been observed before the stream died"

    observed = Observed(data=notes, value=None)
    inverse = undo_for(cmd, observed)
    assert isinstance(inverse, DestroyInstance)
    assert inverse.resource_ids == notes

    assert notes["droplet_id"] in await harness.backend_resources()
    clean_provider = harness.provider()  # fresh instance, same shared backend
    await _drain(clean_provider, inverse)
    assert notes["droplet_id"] not in await harness.backend_resources()


# ---------------------------------------------------------------------------
# unsupported command / C-24
# ---------------------------------------------------------------------------


async def test_unsupported_command_rejected_with_zero_backend_traffic():
    harness = DigitalOceanHarness()
    provider = harness.provider()
    before = harness.backend_attempts()
    with pytest.raises(PermanentError) as excinfo:
        provider.execute(FetchKubeconfig(rewrite_server_to="https://example.invalid:6443"))
    assert excinfo.value.code == "unsupported"
    assert harness.backend_attempts() == before


# ---------------------------------------------------------------------------
# classification table / C-17
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "fault,expected_cls,expected_code",
    DigitalOceanHarness().classification_cases(),
    ids=lambda v: v.value if isinstance(v, Fault) else str(v),
)
async def test_classification_table(fault, expected_cls, expected_code):
    harness = DigitalOceanHarness()
    provider = harness.provider(fault)
    # MISSING_SOURCE (ssh key lookup) and DIE_MID_CREATE (post-allocation project assign)
    # only manifest on the create path; every other fault is visible on the cheapest read.
    cmd = harness.create_command() if fault in (Fault.MISSING_SOURCE, Fault.DIE_MID_CREATE) else harness.observe_command()
    with pytest.raises(expected_cls) as excinfo:
        await _drain(provider, cmd)
    assert excinfo.value.code == expected_code


# ---------------------------------------------------------------------------
# reconcile / C-13
# ---------------------------------------------------------------------------


async def test_reconcile_orphan_zombie_create_unmanaged():
    harness = DigitalOceanHarness()
    provider = harness.provider()

    zombie_id = harness.backend.seed_droplet(tags=["seedpod-managed", "cluster-uuid:zombie-uuid"])
    unmanaged_id = harness.backend.seed_droplet(tags=["seedpod-managed", "cluster-uuid:untracked-uuid"])
    harness.backend.seed_droplet(tags=["seedpod-managed"])  # no uuid tag: must be skipped entirely

    clusters = (
        ClusterSnapshot(cluster_uuid="orphan-uuid", slug="orphan", status="active", resource_ids={}),
        ClusterSnapshot(cluster_uuid="zombie-uuid", slug="zombie", status="destroyed", resource_ids={}),
    )
    (result,) = await _drain(provider, Reconcile(clusters=clusters))
    intents = result.value

    by_type = {type(i) for i in intents}
    assert OrphanIntent in by_type
    assert ZombieIntent in by_type
    assert CreateUnmanagedIntent in by_type

    orphan = next(i for i in intents if isinstance(i, OrphanIntent))
    assert orphan.cluster_id == "orphan-uuid"
    zombie = next(i for i in intents if isinstance(i, ZombieIntent))
    assert zombie.cluster_id == "zombie-uuid" and zombie.droplet_id == zombie_id
    unmanaged = next(
        i for i in intents if isinstance(i, CreateUnmanagedIntent) and i.cluster_id == "untracked-uuid"
    )
    assert unmanaged.droplet["id"] == int(unmanaged_id)


async def test_reconcile_unreachable_touches_nothing():
    harness = DigitalOceanHarness()
    provider = harness.provider(Fault.UNREACHABLE)
    with pytest.raises(InfrastructureUnreachableError):
        await _drain(provider, Reconcile(clusters=()))


# ---------------------------------------------------------------------------
# VPC placement / firewalls / late project-assign (fix-round-1 findings)
# ---------------------------------------------------------------------------


async def test_create_threads_vpc_uuid_into_droplet_payload():
    """Seam C §5.7.1 names ``cleanup_expired_clusters`` as the only removed v1 provider
    capability; VPC placement is not on that list, so ``CreateInstance`` must still land the
    droplet in a resolved VPC rather than the region's default."""
    harness = DigitalOceanHarness()
    provider = harness.provider()

    events = await _drain(provider, harness.create_command())
    result = next(ev.value for ev in events if isinstance(ev, Result))

    droplet_id = result.resource_ids["droplet_id"]
    droplet = harness.backend.droplets[droplet_id]
    assert droplet["vpc_uuid"] is not None
    assert len(harness.backend.vpcs) == 1
    (vpc,) = harness.backend.vpcs.values()
    assert vpc["region"] == droplet["region"]["slug"]
    assert droplet["vpc_uuid"] == vpc["id"]


async def test_create_reuses_existing_vpc_for_the_region():
    harness = DigitalOceanHarness()
    provider = harness.provider()
    await _drain(provider, harness.create_command())
    assert len(harness.backend.vpcs) == 1

    # A second cluster in the same region must adopt the existing VPC, not create a duplicate.
    cluster_uuid = str(uuid4())
    await _drain(
        provider,
        CreateInstance(
            cluster_uuid=cluster_uuid,
            slug="second-cluster",
            spec=ClusterSpecification(
                node_specification=NodeSpecification(cpu_cores=1, memory_gb=1, region_hint="europe-west"),
                cluster_config=ClusterConfiguration(),
            ),
            pod_cidr="10.42.8.0/24",
            service_cidr="10.43.8.0/24",
            tags=(f"cluster-uuid:{cluster_uuid}", "cluster-second-cluster", "ttl-4"),
        ),
    )
    assert len(harness.backend.vpcs) == 1, "must reuse the region's existing VPC, never duplicate"


async def test_apply_firewalls_ensures_and_attaches_droplet():
    harness = DigitalOceanHarness()
    provider = harness.provider()
    create_cmd = harness.create_command()
    create_events = await _drain(provider, create_cmd)
    result = next(ev.value for ev in create_events if isinstance(ev, Result))
    droplet_id = result.resource_ids["droplet_id"]

    (fw_result,) = await _drain(
        provider, ApplyFirewalls(resource_ids=result.resource_ids, spec=create_cmd.spec)
    )
    assert fw_result.value is None
    assert len(harness.backend.firewalls) == 2, "management + application firewalls"
    for firewall in harness.backend.firewalls.values():
        assert int(droplet_id) in firewall["droplet_ids"]


async def test_apply_firewalls_is_idempotent_ensure_exists():
    harness = DigitalOceanHarness()
    provider = harness.provider()
    create_cmd = harness.create_command()
    create_events = await _drain(provider, create_cmd)
    result = next(ev.value for ev in create_events if isinstance(ev, Result))

    await _drain(provider, ApplyFirewalls(resource_ids=result.resource_ids, spec=create_cmd.spec))
    await _drain(provider, ApplyFirewalls(resource_ids=result.resource_ids, spec=create_cmd.spec))
    assert len(harness.backend.firewalls) == 2, "second call must reuse the existing firewalls, never duplicate"


async def test_assign_to_project_late_command_assigns_droplet():
    harness = DigitalOceanHarness()
    provider = harness.provider()
    create_events = await _drain(provider, harness.create_command())
    result = next(ev.value for ev in create_events if isinstance(ev, Result))
    droplet_id = result.resource_ids["droplet_id"]

    (assign_result,) = await _drain(provider, AssignToProject(resource_ids=result.resource_ids))
    assert assign_result.value is None
    assert f"do:droplet:{droplet_id}" in harness.backend.project_resources["proj-exampleco"]


async def test_assign_to_project_raises_normally_no_in_provider_swallow():
    """Unlike the early inline call, the LATE command does not swallow — the workflow step's
    own ``on_failure: continue`` is the v2 home for warn-and-proceed here (module docstring)."""
    harness = DigitalOceanHarness()
    provider = harness.provider(Fault.AUTH)
    with pytest.raises(PermanentError):
        await _drain(provider, AssignToProject(resource_ids={"droplet_id": "1"}))
