"""tests/conformance/test_orbstack_smoke.py — smoke coverage proving the ``orbstack`` provider
streams per Seam C §5.2 against its fake transport, and that ``OrbstackHarness`` is wired
correctly. The full parametrized C-01..C-24 suite is written by a later agent against
``tests/conformance/harness.Harness``; this file is a narrower, provider-local proof (stream
shape, the RESOURCE_ALLOCATED-before-verify C1 close, unconditional adoption, the "no absent
phase" deviation, the no-op destroy vocabulary, the "never orphans" reconcile invariant, the
port-preserving kubeconfig rewrite, the classification table, unsupported-command rejection) so
that agent's suite has a known-good provider to slot in against.

No ``Mock``/``patch`` anywhere — every fault is injected at ``FakeOrbstackTransport``.
"""

from __future__ import annotations

import re

import pytest

from seedpod.core.errors import InfrastructureUnreachableError, PermanentError
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
    ProbeSshPort,
    Progress,
    Reconcile,
    Result,
)
from tests.conformance.harness import Fault
from tests.conformance.orbstack_harness import OrbstackHarness

pytestmark = pytest.mark.asyncio


async def _drain(provider, cmd):
    events = []
    async for ev in provider.execute(cmd):
        events.append(ev)
    return events


# ---------------------------------------------------------------------------
# check_ready / C-01
# ---------------------------------------------------------------------------


async def test_check_ready_succeeds_against_healthy_backend():
    harness = OrbstackHarness()
    provider = harness.provider()
    await provider.check_ready()  # must not raise


async def test_check_ready_fails_fast_on_missing_binary():
    harness = OrbstackHarness()
    harness.backend.remove_kubectl()
    provider = harness.provider()
    with pytest.raises(PermanentError) as excinfo:
        await provider.check_ready()
    assert excinfo.value.code == "not_found"


async def test_check_ready_fails_fast_on_broken_environment():
    """``Fault.MISSING_SOURCE`` — orbstack's closest structural equivalent (the ``orbstack``
    context missing from the local kubeconfig entirely) — is a clean non-zero exit, so this is
    ``Permanent(SCRIPT_FAILED)``, not Unreachable (module docstring's opening paragraph)."""
    harness = OrbstackHarness()
    with harness.broken_environment() as provider:
        with pytest.raises(PermanentError) as excinfo:
            await provider.check_ready()
        assert excinfo.value.code == "script_failed"


async def test_check_ready_api_down_raises_unreachable():
    harness = OrbstackHarness()
    provider = harness.provider(Fault.UNREACHABLE)
    with pytest.raises(InfrastructureUnreachableError):
        await provider.check_ready()


# ---------------------------------------------------------------------------
# stream shape + the RESOURCE_ALLOCATED-before-verify C1 close / C-02, C-08
# ---------------------------------------------------------------------------


async def test_create_stream_shape_progress_then_result():
    harness = OrbstackHarness()
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


async def test_create_emits_resource_allocated_before_verifying_reachable():
    """The identity is a fixed constant, known before any backend call — RESOURCE_ALLOCATED
    fires before ``_verify_reachable`` runs, mirroring every other machine provider's tag-before-
    boot ordering even though orbstack has no boot (module docstring's genuinely-NEW note)."""
    harness = OrbstackHarness()
    provider = harness.provider()
    cmd = harness.create_command()

    saw_progress = False
    async for ev in provider.execute(cmd):
        if isinstance(ev, Progress) and ev.phase == "resource-allocated":
            saw_progress = True
        if isinstance(ev, Result):
            assert saw_progress


# ---------------------------------------------------------------------------
# create always adopts / C-07 (genuinely NEW, trivial by construction)
# ---------------------------------------------------------------------------


async def test_create_always_adopts_never_mutates_backend():
    harness = OrbstackHarness()
    provider = harness.provider()

    first = await _drain(provider, harness.create_command())
    second = await _drain(provider, harness.create_command())

    first_result = next(ev.value for ev in first if isinstance(ev, Result))
    second_result = next(ev.value for ev in second if isinstance(ev, Result))

    assert first_result.adopted_existing is True
    assert second_result.adopted_existing is True
    # provider_hostname (v1 reference-code .../orbstack.py:130 storage location) falls back to
    # `host` when `public_hostname` is unset — see OrbstackConfig's module docstring note.
    assert (
        first_result.resource_ids
        == second_result.resource_ids
        == {"orbstack_context": "orbstack", "provider_hostname": "minimax.local"}
    )


async def test_create_echoes_cidrs_unchanged_unlike_kind():
    harness = OrbstackHarness()
    provider = harness.provider()
    cmd = harness.create_command()

    (terminal,) = [ev for ev in await _drain(provider, cmd) if isinstance(ev, Result)]
    assert terminal.value.effective_pod_cidr == cmd.pod_cidr
    assert terminal.value.effective_service_cidr == cmd.service_cidr


# ---------------------------------------------------------------------------
# probe / no "absent" phase (deliberate deviation, see module docstring)
# ---------------------------------------------------------------------------


async def test_probe_instance_running_when_reachable():
    harness = OrbstackHarness()
    provider = harness.provider()
    (result,) = await _drain(provider, harness.observe_command())
    assert result.value.phase == "running"


async def test_probe_instance_unreachable_raises_never_absent():
    harness = OrbstackHarness()
    provider = harness.provider(Fault.UNREACHABLE)
    with pytest.raises(InfrastructureUnreachableError) as excinfo:
        await _drain(provider, harness.observe_command())
    assert excinfo.value.code == "endpoint_unreachable"


async def test_probe_instance_context_missing_raises_permanent_never_absent():
    """The OTHER failure mode (module docstring's opening paragraph): a structural
    context-missing exit is ALSO a raise, never a ``phase="absent"`` Result."""
    harness = OrbstackHarness()
    harness.backend.break_context()
    provider = harness.provider()
    with pytest.raises(PermanentError) as excinfo:
        await _drain(provider, harness.observe_command())
    assert excinfo.value.code == "script_failed"


# ---------------------------------------------------------------------------
# destroy vocabulary — unconditional no-op, zero backend traffic
# ---------------------------------------------------------------------------


async def test_destroy_is_unconditional_no_op_zero_backend_traffic():
    harness = OrbstackHarness()
    provider = harness.provider()
    before = harness.backend_attempts()

    for _ in range(2):
        (result,) = await _drain(provider, DestroyInstance(slug="demo-cluster", resource_ids={"orbstack_context": "orbstack"}))
        assert result.value.status == DestroyStatus.DESTROYED
        assert result.value.note == "OrbStack cluster preserved, only deployed resources cleaned up"

    assert harness.backend_attempts() == before, "destroy must never touch the transport"


async def test_destroy_no_op_even_when_backend_unreachable():
    """Salvaged verbatim from v1's ``destroy_cluster`` (module docstring): it never calls
    ``_verify_orbstack_running`` at all, so even an unreachable/broken backend can't stop this
    from succeeding."""
    harness = OrbstackHarness()
    provider = harness.provider(Fault.UNREACHABLE)
    (result,) = await _drain(provider, DestroyInstance(slug="demo-cluster", resource_ids={"orbstack_context": "orbstack"}))
    assert result.value.status == DestroyStatus.DESTROYED


async def test_probe_destruction_is_also_unconditional_no_op():
    harness = OrbstackHarness()
    provider = harness.provider(Fault.UNREACHABLE)
    (result,) = await _drain(provider, ProbeDestruction(resource_ids={"orbstack_context": "orbstack"}))
    assert result.value.status == DestroyStatus.DESTROYED


async def test_undo_after_create_is_the_same_harmless_no_op():
    """``undo_for`` still maps ``CreateInstance`` -> ``DestroyInstance`` for orbstack (the
    compensation module has no provider-specific special case) — executing that undo is simply
    the same no-op, never a real deletion."""
    harness = OrbstackHarness()
    provider = harness.provider()
    cmd = harness.create_command()
    events = await _drain(provider, cmd)
    terminal = next(ev for ev in events if isinstance(ev, Result))

    observed = Observed(data={}, value=terminal.value)
    inverse = undo_for(cmd, observed)
    assert isinstance(inverse, DestroyInstance)
    assert inverse.resource_ids == terminal.value.resource_ids

    (destroy_result,) = await _drain(provider, inverse)
    assert destroy_result.value.status == DestroyStatus.DESTROYED


# ---------------------------------------------------------------------------
# list_instances — raises rather than swallowing to [] (§5.7.4)
# ---------------------------------------------------------------------------


async def test_list_instances_reports_the_one_builtin_cluster():
    harness = OrbstackHarness()
    provider = harness.provider()
    (result,) = await _drain(provider, ListInstances())
    assert len(result.value) == 1
    assert result.value[0].name == "orbstack"


async def test_list_instances_raises_never_lies_with_empty_list():
    harness = OrbstackHarness()
    provider = harness.provider(Fault.UNREACHABLE)
    with pytest.raises(InfrastructureUnreachableError):
        await _drain(provider, ListInstances())


# ---------------------------------------------------------------------------
# reconcile — "never orphans" / C-13, C-14
# ---------------------------------------------------------------------------


async def test_reconcile_never_orphans_across_every_db_status():
    harness = OrbstackHarness()
    provider = harness.provider()

    clusters = tuple(
        ClusterSnapshot(cluster_uuid=f"uuid-{case.db_status}", slug="x", status=case.db_status, resource_ids={})
        for case in harness.reconcile_truth_table()
    )
    (result,) = await _drain(provider, Reconcile(clusters=clusters))
    assert result.value == (), "orbstack must never emit an Orphan/Zombie/CreateUnmanaged intent"


async def test_reconcile_unreachable_touches_nothing():
    harness = OrbstackHarness()
    provider = harness.provider(Fault.UNREACHABLE)
    with pytest.raises(InfrastructureUnreachableError):
        await _drain(provider, Reconcile(clusters=()))


# ---------------------------------------------------------------------------
# fetch_kubeconfig / C-19 rewrite golden cases (crown jewel #6, orbstack variant)
# ---------------------------------------------------------------------------


async def test_fetch_kubeconfig_rewrite_cases():
    harness = OrbstackHarness()
    provider = harness.provider()

    for name, cmd, expected_pattern in harness.rewrite_cases():
        (result,) = await _drain(provider, cmd)
        assert re.search(expected_pattern, result.value.yaml_text), name


async def test_fetch_kubeconfig_preserves_source_port_never_substitutes_it():
    """The orbstack variant's defining trait vs. kind's (module docstring): the port survives
    the rewrite unchanged even though the host does not."""
    harness = OrbstackHarness()
    provider = harness.provider()
    (result,) = await _drain(
        provider, FetchKubeconfig(rewrite_server_to="minimax.local", resource_ids={"orbstack_context": "orbstack"})
    )
    assert f":{harness.backend.api_port}" in result.value.yaml_text
    assert "127.0.0.1" not in result.value.yaml_text
    assert "minimax.local" in result.value.yaml_text


# ---------------------------------------------------------------------------
# unsupported command / C-24
# ---------------------------------------------------------------------------


async def test_unsupported_command_rejected_with_zero_backend_traffic():
    harness = OrbstackHarness()
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
    OrbstackHarness().classification_cases(),
    ids=lambda v: v.value if isinstance(v, Fault) else str(v),
)
async def test_classification_table(fault, expected_cls, expected_code):
    harness = OrbstackHarness()
    provider = harness.provider(fault)
    with pytest.raises(expected_cls) as excinfo:
        await _drain(provider, ListInstances())
    assert excinfo.value.code == expected_code


# ---------------------------------------------------------------------------
# single attempt, no internal retry / C-15
# ---------------------------------------------------------------------------


async def test_single_attempt_no_internal_retry_then_succeeds_on_reinvocation():
    harness = OrbstackHarness()
    provider = harness.provider(Fault.TRANSIENT_ONCE)

    with pytest.raises(InfrastructureUnreachableError):
        await _drain(provider, ListInstances())
    assert harness.backend_attempts() == 1  # exactly one transport attempt, no internal retry loop

    (result,) = await _drain(provider, ListInstances())
    assert isinstance(result.value, tuple)


# ---------------------------------------------------------------------------
# probes are one iteration, never block until ready / C-16
# ---------------------------------------------------------------------------


async def test_probe_instance_is_one_bounded_iteration():
    harness = OrbstackHarness()
    provider = harness.provider()
    events = await _drain(provider, harness.observe_command())
    assert len(events) == 1
    assert isinstance(events[0], Result)
    assert isinstance(events[0].value, InstanceState)
