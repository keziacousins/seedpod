"""tests/conformance/test_kind_smoke.py — smoke coverage proving the ``kind`` provider streams
per Seam C §5.2 against its fake transport, and that ``KindHarness`` is wired correctly. The
full parametrized C-01..C-24 suite is written by a later agent against
``tests/conformance/harness.Harness``; this file is a narrower, provider-local proof (stream
shape, the RESOURCE_ALLOCATED-before-backend-call C1 close, adoption idempotency, absence vs
unreachable on docker inspect, destroy vocabulary, kindnet /16 CIDR override, reconcile mapping,
kubeconfig rewrite, classification table, unsupported-command rejection) so that agent's suite
has a known-good provider to slot in against.

No ``Mock``/``patch`` anywhere — every fault is injected at ``FakeKindTransport``.
"""

from __future__ import annotations

import re

import pytest

from seedpod.core.errors import InfrastructureUnreachableError, PermanentError, ProviderError
from seedpod.core.reconciliation_intents import OrphanIntent, ZombieIntent
from seedpod.providers.compensation import undo_for
from seedpod.providers.contract import (
    ClusterSnapshot,
    DestroyInstance,
    DestroyStatus,
    FetchKubeconfig,
    InstanceCreated,
    InstanceState,
    ListInstances,
    Observed,
    ProbeDestruction,
    ProbeInstance,
    ProbeSshPort,
    Progress,
    Reconcile,
    Result,
)
from tests.conformance.harness import Fault
from tests.conformance.kind_harness import KindHarness

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
    harness = KindHarness()
    provider = harness.provider()
    await provider.check_ready()  # must not raise


async def test_check_ready_fails_fast_on_broken_environment():
    harness = KindHarness()
    with harness.broken_environment() as provider:
        with pytest.raises(PermanentError) as excinfo:
            await provider.check_ready()
        assert excinfo.value.code == "not_found"


async def test_check_ready_docker_daemon_down_raises_unreachable():
    harness = KindHarness()
    provider = harness.provider(Fault.UNREACHABLE)
    with pytest.raises(InfrastructureUnreachableError):
        await provider.check_ready()


# ---------------------------------------------------------------------------
# stream shape + the RESOURCE_ALLOCATED-before-backend-call C1 close / C-02, C-09
# ---------------------------------------------------------------------------


async def test_create_stream_shape_progress_then_result():
    harness = KindHarness()
    provider = harness.provider()
    events = await _drain(provider, harness.create_command())

    assert events, "create must yield at least the terminal Result"
    *progress_events, terminal = events
    assert all(isinstance(ev, Progress) for ev in progress_events)
    assert isinstance(terminal, Result)
    resource_allocated = [ev for ev in progress_events if ev.phase == "resource-allocated"]
    assert len(resource_allocated) == 1, "CreateInstance MUST emit exactly one Progress(RESOURCE_ALLOCATED)"
    assert isinstance(terminal.value, InstanceCreated)
    assert terminal.value.resource_ids == resource_allocated[0].data["resource_ids"]


async def test_create_emits_resource_allocated_before_backend_container_exists():
    """The identity (deterministic name + allocated port) is known BEFORE `kind create
    cluster` runs — proving a mid-create death still carries an id for undo_for to compensate
    (module docstring's C1 note)."""
    harness = KindHarness()
    provider = harness.provider()
    cmd = harness.create_command()

    saw_progress = False
    async for ev in provider.execute(cmd):
        if isinstance(ev, Progress) and ev.phase == "resource-allocated":
            saw_progress = True
            name = ev.data["resource_ids"]["kind_cluster_name"]
            assert name not in await harness.backend_resources(), "container must not exist yet at RESOURCE_ALLOCATED"
        if isinstance(ev, Result):
            assert saw_progress
            assert ev.value.resource_ids["kind_cluster_name"] in await harness.backend_resources()


async def test_probe_stream_shape_result_only():
    harness = KindHarness()
    provider = harness.provider()
    events = await _drain(provider, harness.observe_command())
    assert len(events) == 1
    assert isinstance(events[0], Result)
    assert isinstance(events[0].value, InstanceState)


# ---------------------------------------------------------------------------
# crown jewel #7 — kindnet /16 CIDR override, echoed in the result
# ---------------------------------------------------------------------------


async def test_create_overrides_engine_cidrs_with_kindnet_defaults():
    harness = KindHarness()
    provider = harness.provider()
    cmd = harness.create_command()
    assert cmd.pod_cidr == "10.42.7.0/24"  # the engine-supplied Tailscale /24, too small for kindnet

    (terminal,) = [ev for ev in await _drain(provider, cmd) if isinstance(ev, Result)]
    assert terminal.value.effective_pod_cidr == "10.244.0.0/16"
    assert terminal.value.effective_service_cidr == "10.96.0.0/12"
    assert terminal.value.effective_pod_cidr != cmd.pod_cidr


# ---------------------------------------------------------------------------
# create idempotency / C-07, C-08
# ---------------------------------------------------------------------------


async def test_create_idempotent_reinvocation_adopts_not_duplicates():
    harness = KindHarness()
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
    harness = KindHarness()
    provider = harness.provider()
    events = await _drain(provider, ProbeInstance(resource_ids={"kind_cluster_name": "seedpod-ghost"}))
    (result,) = events
    assert result.value.phase == "absent"


async def test_probe_instance_stopped_container_is_data_not_absent():
    harness = KindHarness()
    harness.backend.seed_cluster("seedpod-stopped", running=False)
    provider = harness.provider()
    (result,) = await _drain(provider, ProbeInstance(resource_ids={"kind_cluster_name": "seedpod-stopped"}))
    assert result.value.phase == "stopped"


async def test_probe_instance_unreachable_raises_never_absent():
    harness = KindHarness()
    provider = harness.provider(Fault.UNREACHABLE)
    with pytest.raises(InfrastructureUnreachableError) as excinfo:
        await _drain(provider, harness.observe_command())
    assert excinfo.value.code == "endpoint_unreachable"


# ---------------------------------------------------------------------------
# destroy vocabulary / C-10, C-11, C-12
# ---------------------------------------------------------------------------


async def test_destroy_idempotent_on_absent_twice():
    harness = KindHarness()
    provider = harness.provider()
    cmd = DestroyInstance(slug="ghost", resource_ids={"kind_cluster_name": "seedpod-never-existed"})

    for _ in range(2):
        events = await _drain(provider, cmd)
        (result,) = events
        assert result.value.status == DestroyStatus.DESTROYED


async def test_destroy_succeeds_and_removes_from_backend():
    harness = KindHarness()
    provider = harness.provider()
    create_events = await _drain(provider, harness.create_command())
    resource_ids = next(ev.value for ev in create_events if isinstance(ev, Result)).resource_ids

    (result,) = await _drain(provider, DestroyInstance(slug="demo-cluster", resource_ids=resource_ids))
    assert result.value.status == DestroyStatus.DESTROYED
    assert resource_ids["kind_cluster_name"] not in await harness.backend_resources()


async def test_destroy_never_lies_when_unreachable():
    harness = KindHarness()
    create_events = await _drain(harness.provider(), harness.create_command())
    resource_ids = next(ev.value for ev in create_events if isinstance(ev, Result)).resource_ids

    broken = harness.provider(Fault.UNREACHABLE)
    with pytest.raises(InfrastructureUnreachableError):
        await _drain(broken, DestroyInstance(slug="demo-cluster", resource_ids=resource_ids))


async def test_probe_destruction_vocabulary():
    harness = KindHarness()
    provider = harness.provider()

    harness.backend.seed_cluster("seedpod-stuck", running=True)
    (stuck,) = await _drain(provider, ProbeDestruction(resource_ids={"kind_cluster_name": "seedpod-stuck"}))
    assert stuck.value.status == DestroyStatus.DESTROY_FAILED
    assert stuck.value.stuck_resources == ("seedpod-stuck",)

    (gone,) = await _drain(provider, ProbeDestruction(resource_ids={"kind_cluster_name": "seedpod-long-gone"}))
    assert gone.value.status == DestroyStatus.DESTROYED


# ---------------------------------------------------------------------------
# the C1 close — a partially-created cluster on a mid-create death / C-09
# ---------------------------------------------------------------------------


async def test_undo_after_partial_create_cleans_backend():
    harness = KindHarness()
    provider = harness.provider(Fault.DIE_MID_CREATE)
    cmd = harness.create_command()

    events = []
    with pytest.raises(ProviderError):
        async for ev in provider.execute(cmd):
            events.append(ev)

    notes = _fold_resource_ids(events)
    assert notes, "RESOURCE_ALLOCATED must have been observed before the stream died"
    name = notes["kind_cluster_name"]
    assert name in await harness.backend_resources(), "kind create left a partial container behind"

    observed = Observed(data=notes, value=None)
    inverse = undo_for(cmd, observed)
    assert isinstance(inverse, DestroyInstance)
    assert inverse.resource_ids == notes

    clean_provider = harness.provider()  # fresh instance, same shared backend
    (destroy_result,) = await _drain(clean_provider, inverse)
    assert destroy_result.value.status == DestroyStatus.DESTROYED
    assert name not in await harness.backend_resources()


# ---------------------------------------------------------------------------
# fetch_kubeconfig / C-19 rewrite golden cases (crown jewel #6, kind variant)
# ---------------------------------------------------------------------------


async def test_fetch_kubeconfig_rewrite_cases():
    harness = KindHarness()
    provider = harness.provider()

    for name, cmd, expected_pattern in harness.rewrite_cases():
        (result,) = await _drain(provider, cmd)
        assert re.search(expected_pattern, result.value.yaml_text), name


async def test_fetch_kubeconfig_requires_kind_cluster_name():
    harness = KindHarness()
    provider = harness.provider()
    with pytest.raises(PermanentError) as excinfo:
        await _drain(provider, FetchKubeconfig(rewrite_server_to="x"))
    assert excinfo.value.code == "invalid_input"


# ---------------------------------------------------------------------------
# reconcile / C-13
# ---------------------------------------------------------------------------


async def test_reconcile_orphan_stopped_zombie_backstop():
    harness = KindHarness()
    provider = harness.provider()

    harness.backend.seed_cluster("seedpod-zombie", running=True)
    harness.backend.seed_cluster("seedpod-stopped-active", running=False)

    clusters = (
        ClusterSnapshot(cluster_uuid="missing-uuid", slug="missing", status="active", resource_ids={}),
        ClusterSnapshot(
            cluster_uuid="stopped-uuid",
            slug="stopped-active",
            status="active",
            resource_ids={"kind_cluster_name": "seedpod-stopped-active"},
        ),
        ClusterSnapshot(
            cluster_uuid="zombie-uuid",
            slug="zombie",
            status="destroyed",
            resource_ids={"kind_cluster_name": "seedpod-zombie"},
        ),
        ClusterSnapshot(cluster_uuid="destroying-uuid", slug="gone-destroying", status="destroying", resource_ids={}),
    )
    (result,) = await _drain(provider, Reconcile(clusters=clusters))
    intents = result.value

    by_uuid = {i.cluster_id: i for i in intents}
    assert isinstance(by_uuid["missing-uuid"], OrphanIntent)
    assert isinstance(by_uuid["stopped-uuid"], OrphanIntent)
    assert isinstance(by_uuid["zombie-uuid"], ZombieIntent)
    assert by_uuid["zombie-uuid"].droplet_id == "seedpod-zombie"
    assert isinstance(by_uuid["destroying-uuid"], OrphanIntent)


async def test_reconcile_unreachable_touches_nothing():
    harness = KindHarness()
    provider = harness.provider(Fault.UNREACHABLE)
    with pytest.raises(InfrastructureUnreachableError):
        await _drain(provider, Reconcile(clusters=()))


# ---------------------------------------------------------------------------
# unsupported command / C-24
# ---------------------------------------------------------------------------


async def test_unsupported_command_rejected_with_zero_backend_traffic():
    harness = KindHarness()
    provider = harness.provider()
    before = harness.backend_attempts()
    with pytest.raises(PermanentError) as excinfo:
        provider.execute(ProbeSshPort(host="x"))
    assert excinfo.value.code == "unsupported"
    assert harness.backend_attempts() == before


# ---------------------------------------------------------------------------
# classification table / C-17
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "fault,expected_cls,expected_code",
    KindHarness().classification_cases(),
    ids=lambda v: v.value if isinstance(v, Fault) else str(v),
)
async def test_classification_table(fault, expected_cls, expected_code):
    harness = KindHarness()
    provider = harness.provider(fault)
    # ListInstances hits the `kind` binary (get clusters), the one MISSING_SOURCE intercepts;
    # UNREACHABLE/TRANSIENT_ONCE fire regardless of which binary is invoked.
    with pytest.raises(expected_cls) as excinfo:
        await _drain(provider, ListInstances())
    assert excinfo.value.code == expected_code


# ---------------------------------------------------------------------------
# single attempt, no internal retry / C-15
# ---------------------------------------------------------------------------


async def test_single_attempt_no_internal_retry_then_succeeds_on_reinvocation():
    harness = KindHarness()
    provider = harness.provider(Fault.TRANSIENT_ONCE)

    with pytest.raises(InfrastructureUnreachableError):
        await _drain(provider, ListInstances())
    assert harness.backend_attempts() == 1  # exactly one transport attempt, no internal retry loop

    (result,) = await _drain(provider, ListInstances())
    assert isinstance(result.value, tuple)
