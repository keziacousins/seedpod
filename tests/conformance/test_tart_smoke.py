"""tests/conformance/test_tart_smoke.py — smoke coverage proving the ``tart`` provider streams
per Seam C §5.2 against its fake transport, and that ``TartHarness`` is wired correctly. The full
parametrized C-01..C-24 suite is written by a later agent against
``tests/conformance/harness.Harness``; this file is a narrower, provider-local proof (stream
shape, the RESOURCE_ALLOCATED-before-backend-call C1 close, C-07 adoption, absence vs unreachable
vs "no IP yet", the full destroy vocabulary rows 6-8, reconcile mapping, classification table,
unsupported-command rejection) so that agent's suite has a known-good provider to slot in against.

No ``Mock``/``patch`` anywhere — every fault is injected at ``FakeTartTransport``.
"""

from __future__ import annotations

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
    Progress,
    Reconcile,
    Result,
)
from tests.conformance.harness import Fault
from tests.conformance.tart_harness import TartHarness

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
    harness = TartHarness()
    provider = harness.provider()
    await provider.check_ready()  # must not raise


async def test_check_ready_fails_fast_on_broken_environment():
    harness = TartHarness()
    with harness.broken_environment() as provider:
        with pytest.raises(PermanentError) as excinfo:
            await provider.check_ready()
        assert excinfo.value.code == "not_found"


async def test_check_ready_daemon_unresponsive_raises_unreachable():
    harness = TartHarness()
    provider = harness.provider(Fault.UNREACHABLE)
    with pytest.raises(InfrastructureUnreachableError):
        await provider.check_ready()


# ---------------------------------------------------------------------------
# stream shape + the RESOURCE_ALLOCATED-before-backend-call C1 close / C-02, C-09
# ---------------------------------------------------------------------------


async def test_create_stream_shape_progress_then_result():
    harness = TartHarness()
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


async def test_create_emits_resource_allocated_before_backend_vm_exists():
    """The identity (deterministic `seedpod-{slug}` name) is known BEFORE `tart clone` runs —
    proving a mid-create death still carries an id for undo_for to compensate (module docstring's
    C1 note)."""
    harness = TartHarness()
    provider = harness.provider()
    cmd = harness.create_command()

    saw_progress = False
    async for ev in provider.execute(cmd):
        if isinstance(ev, Progress) and ev.phase == "resource-allocated":
            saw_progress = True
            name = ev.data["resource_ids"]["tart_vm_name"]
            assert name not in await harness.backend_resources(), "VM must not exist yet at RESOURCE_ALLOCATED"
        if isinstance(ev, Result):
            assert saw_progress
            assert ev.value.resource_ids["tart_vm_name"] in await harness.backend_resources()


async def test_create_result_has_no_ip_yet_freshly_booted():
    """A freshly-launched VM has no IP assigned yet — row 5 DATA, not an error; the engine's
    wait-for-readiness gate (repeated ProbeInstance) is what actually waits."""
    harness = TartHarness()
    provider = harness.provider()
    (terminal,) = [ev for ev in await _drain(provider, harness.create_command()) if isinstance(ev, Result)]
    assert terminal.value.address is None


async def test_probe_stream_shape_result_only():
    harness = TartHarness()
    provider = harness.provider()
    events = await _drain(provider, harness.observe_command())
    assert len(events) == 1
    assert isinstance(events[0], Result)
    assert isinstance(events[0].value, InstanceState)


# ---------------------------------------------------------------------------
# create idempotency / C-07, C-08
# ---------------------------------------------------------------------------


async def test_create_idempotent_reinvocation_adopts_not_duplicates():
    harness = TartHarness()
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
    assert before == after, "re-invocation must not create a duplicate backend VM"


# ---------------------------------------------------------------------------
# absence vs unreachable vs "no IP yet" / C-05, C-06
# ---------------------------------------------------------------------------


async def test_probe_instance_absence_is_data():
    harness = TartHarness()
    provider = harness.provider()
    events = await _drain(provider, ProbeInstance(resource_ids={"tart_vm_name": "seedpod-ghost"}))
    (result,) = events
    assert result.value.phase == "absent"


async def test_probe_instance_stopped_vm_is_data_not_absent():
    harness = TartHarness()
    harness.backend.seed_vm("seedpod-stopped", running=False, ip=None)
    provider = harness.provider()
    (result,) = await _drain(provider, ProbeInstance(resource_ids={"tart_vm_name": "seedpod-stopped"}))
    assert result.value.phase == "stopped"


async def test_probe_instance_running_no_ip_is_provisioning_not_error():
    """Row 5: VM running but no IP assigned yet ⇒ typed Result, never an exception."""
    harness = TartHarness()
    harness.backend.seed_vm("seedpod-booting", running=True, ip=None)
    provider = harness.provider()
    (result,) = await _drain(provider, ProbeInstance(resource_ids={"tart_vm_name": "seedpod-booting"}))
    assert result.value.phase == "provisioning"
    assert result.value.address is None


async def test_probe_instance_unreachable_raises_never_absent():
    harness = TartHarness()
    provider = harness.provider(Fault.UNREACHABLE)
    with pytest.raises(InfrastructureUnreachableError) as excinfo:
        await _drain(provider, harness.observe_command())
    assert excinfo.value.code == "api_timeout"
    assert excinfo.value.host == "localhost"


# ---------------------------------------------------------------------------
# destroy vocabulary rows 6-8 / C-10, C-11, C-12
# ---------------------------------------------------------------------------


async def test_destroy_idempotent_on_absent_twice():
    harness = TartHarness()
    provider = harness.provider()
    cmd = DestroyInstance(slug="ghost", resource_ids={"tart_vm_name": "seedpod-never-existed"})

    for _ in range(2):
        events = await _drain(provider, cmd)
        (result,) = events
        assert result.value.status == DestroyStatus.DESTROYED
        assert "already absent" in (result.value.note or "")


async def test_destroy_succeeds_stops_and_deletes():
    harness = TartHarness()
    provider = harness.provider()
    create_events = await _drain(provider, harness.create_command())
    resource_ids = next(ev.value for ev in create_events if isinstance(ev, Result)).resource_ids

    (result,) = await _drain(provider, DestroyInstance(slug="demo-cluster", resource_ids=resource_ids))
    assert result.value.status == DestroyStatus.DESTROYED
    assert resource_ids["tart_vm_name"] not in await harness.backend_resources()


async def test_destroy_stop_idempotent_when_already_stopped():
    """Row 6: `tart stop` on an already-stopped VM is idempotent success, not a failure."""
    harness = TartHarness()
    harness.backend.seed_vm("seedpod-already-stopped", running=False, ip=None)
    provider = harness.provider()
    (result,) = await _drain(
        provider, DestroyInstance(slug="already-stopped", resource_ids={"tart_vm_name": "seedpod-already-stopped"})
    )
    assert result.value.status == DestroyStatus.DESTROYED
    assert "seedpod-already-stopped" not in await harness.backend_resources()


async def test_destroy_delete_after_stop_failure_yields_destroying_not_error():
    """Row 8: delete-after-successful-stop failure ⇒ Transient(RESOURCE_BUSY), folded into
    DESTROYING vocabulary (gate retries) — never a raised error, since the VM IS stopped."""
    harness = TartHarness()
    harness.backend.seed_vm("seedpod-stuck-delete", running=True)
    harness.backend.force_delete_failure("seedpod-stuck-delete")
    provider = harness.provider()

    (result,) = await _drain(
        provider, DestroyInstance(slug="stuck-delete", resource_ids={"tart_vm_name": "seedpod-stuck-delete"})
    )
    assert result.value.status == DestroyStatus.DESTROYING
    assert "seedpod-stuck-delete" in await harness.backend_resources(), "VM stopped but delete failed — still present"


async def test_destroy_respects_delete_on_destroy_false():
    harness = TartHarness()
    harness.backend.seed_vm("seedpod-keep-disk", running=True)
    from seedpod.providers.tart import TartConfig, TartProvider
    from tests.conformance.fake_tart import FakeTartTransport

    provider = TartProvider(TartConfig(delete_on_destroy=False), FakeTartTransport(harness.backend, frozenset()))
    (result,) = await _drain(provider, DestroyInstance(slug="keep-disk", resource_ids={"tart_vm_name": "seedpod-keep-disk"}))
    assert result.value.status == DestroyStatus.DESTROYED
    assert "seedpod-keep-disk" in await harness.backend_resources(), "stop-only: VM stays on disk"


async def test_destroy_never_lies_when_unreachable():
    harness = TartHarness()
    create_events = await _drain(harness.provider(), harness.create_command())
    resource_ids = next(ev.value for ev in create_events if isinstance(ev, Result)).resource_ids

    broken = harness.provider(Fault.UNREACHABLE)
    with pytest.raises(InfrastructureUnreachableError):
        await _drain(broken, DestroyInstance(slug="demo-cluster", resource_ids=resource_ids))


async def test_probe_destruction_vocabulary():
    harness = TartHarness()
    provider = harness.provider()

    harness.backend.seed_vm("seedpod-stuck", running=True)
    (stuck,) = await _drain(provider, ProbeDestruction(resource_ids={"tart_vm_name": "seedpod-stuck"}))
    assert stuck.value.status == DestroyStatus.DESTROY_FAILED
    assert stuck.value.stuck_resources == ("seedpod-stuck",)

    (gone,) = await _drain(provider, ProbeDestruction(resource_ids={"tart_vm_name": "seedpod-long-gone"}))
    assert gone.value.status == DestroyStatus.DESTROYED


# ---------------------------------------------------------------------------
# the C1 close — a partially-created VM on a mid-create death / C-09
# ---------------------------------------------------------------------------


async def test_undo_after_partial_create_cleans_backend():
    harness = TartHarness()
    provider = harness.provider(Fault.DIE_MID_CREATE)
    cmd = harness.create_command()

    events = []
    with pytest.raises(ProviderError):
        async for ev in provider.execute(cmd):
            events.append(ev)

    notes = _fold_resource_ids(events)
    assert notes, "RESOURCE_ALLOCATED must have been observed before the stream died"
    name = notes["tart_vm_name"]
    assert name in await harness.backend_resources(), "tart clone left a partial VM behind"

    observed = Observed(data=notes, value=None)
    inverse = undo_for(cmd, observed)
    assert isinstance(inverse, DestroyInstance)
    assert inverse.resource_ids == notes

    clean_provider = harness.provider()  # fresh instance, same shared backend
    (destroy_result,) = await _drain(clean_provider, inverse)
    assert destroy_result.value.status == DestroyStatus.DESTROYED
    assert name not in await harness.backend_resources()


# ---------------------------------------------------------------------------
# reconcile / C-13
# ---------------------------------------------------------------------------


async def test_reconcile_orphan_stopped_zombie_backstop():
    harness = TartHarness()
    provider = harness.provider()

    harness.backend.seed_vm("seedpod-zombie", running=True)
    harness.backend.seed_vm("seedpod-stopped-active", running=False, ip=None)

    clusters = (
        ClusterSnapshot(cluster_uuid="missing-uuid", slug="missing", status="active", resource_ids={}),
        ClusterSnapshot(
            cluster_uuid="stopped-uuid",
            slug="stopped-active",
            status="active",
            resource_ids={"tart_vm_name": "seedpod-stopped-active"},
        ),
        ClusterSnapshot(
            cluster_uuid="zombie-uuid",
            slug="zombie",
            status="destroyed",
            resource_ids={"tart_vm_name": "seedpod-zombie"},
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
    harness = TartHarness()
    provider = harness.provider(Fault.UNREACHABLE)
    with pytest.raises(InfrastructureUnreachableError):
        await _drain(provider, Reconcile(clusters=()))


# ---------------------------------------------------------------------------
# unsupported command / C-24 (tart has no FetchKubeconfig — §5.4 plane matrix)
# ---------------------------------------------------------------------------


async def test_unsupported_command_rejected_with_zero_backend_traffic():
    harness = TartHarness()
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
    TartHarness().classification_cases(),
    ids=lambda v: v.value if isinstance(v, Fault) else str(v),
)
async def test_classification_table(fault, expected_cls, expected_code):
    harness = TartHarness()
    provider = harness.provider(fault)
    # MISSING_SOURCE only manifests on the create path (clone's source-not-found symptom);
    # every other fault is visible on the cheapest read.
    cmd = harness.create_command() if fault == Fault.MISSING_SOURCE else harness.observe_command()
    with pytest.raises(expected_cls) as excinfo:
        await _drain(provider, cmd)
    assert excinfo.value.code == expected_code


# ---------------------------------------------------------------------------
# single attempt, no internal retry / C-15
# ---------------------------------------------------------------------------


async def test_single_attempt_no_internal_retry_then_succeeds_on_reinvocation():
    harness = TartHarness()
    provider = harness.provider(Fault.TRANSIENT_ONCE)

    with pytest.raises(InfrastructureUnreachableError):
        await _drain(provider, ListInstances())
    assert harness.backend_attempts() == 1  # exactly one transport attempt, no internal retry loop

    (result,) = await _drain(provider, ListInstances())
    assert isinstance(result.value, tuple)
