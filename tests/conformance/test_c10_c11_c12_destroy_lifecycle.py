"""tests/conformance/test_c10_c11_c12_destroy_lifecycle.py — C-10/C-11/C-12 (Seam C §5.6
table), the typed ``DestroyOutcome``/``DestroyStatus`` vocabulary (§5.3, Proposal 3's fidelity
map).

    C-10 | test_destroy_idempotent_on_absent | machine + services (⊂DNS) | destroy of absent
    resource ⇒ DESTROYED + note / existed=False; twice ⇒ succeeds twice
    C-11 | test_destroy_never_lies_when_unreachable | machine | injected API timeout during
    destroy ⇒ raise Unreachable; never DESTROYED (v1 api_call_succeeded)
    C-12 | test_probe_destruction_vocabulary | machine | in-progress ⇒ DESTROYING; stuck-active
    ⇒ DESTROY_FAILED + stuck_resources; gone ⇒ DESTROYED; transient ⇒ raise Unreachable

``orbstack`` is capability-skipped from C-11/C-12: its ``destroy``/``ProbeDestruction`` are a
salvaged-verbatim unconditional no-op that never touches the backend at all (module docstring:
"never calls _verify_orbstack_running") — there is no destroy vocabulary or unreachable path to
observe, by design, not omission (asserted directly in ``test_orbstack_smoke.py``).
"""

from __future__ import annotations

import pytest

from seedpod.core.errors import InfrastructureUnreachableError
from seedpod.providers.contract import DestroyInstance, DestroyStatus, ProbeDestruction, Result
from tests.conformance._support import drain, skip_if
from tests.conformance.harness import Fault

pytestmark = pytest.mark.asyncio

_ORBSTACK_NO_VOCABULARY = "orbstack destroy/ProbeDestruction is an unconditional no-op that never touches the backend (module docstring)"
_C11_C12_SKIPS = {"orbstack": _ORBSTACK_NO_VOCABULARY}


async def test_destroy_idempotent_on_absent_twice(machine_harness):
    provider = machine_harness.provider()
    resource_ids = {k: f"never-existed-{k}" for k in machine_harness.observe_command().resource_ids}
    cmd = DestroyInstance(slug="ghost", resource_ids=resource_ids)

    for _ in range(2):
        (result,) = await drain(provider, cmd)
        assert result.value.status == DestroyStatus.DESTROYED


async def test_destroy_never_lies_when_unreachable(machine_harness):
    skip_if(_C11_C12_SKIPS, machine_harness.name)
    create_events = await drain(machine_harness.provider(), machine_harness.create_command())
    resource_ids = next(ev.value for ev in create_events if isinstance(ev, Result)).resource_ids

    broken = machine_harness.provider(Fault.UNREACHABLE)
    with pytest.raises(InfrastructureUnreachableError):
        await drain(broken, DestroyInstance(slug="demo-cluster", resource_ids=resource_ids))


# ---------------------------------------------------------------------------
# C-12 — probe-destruction vocabulary: stuck-active/gone are structurally common across
# digitalocean/kind/tart (a resource is either still present-and-running, or gone); DO alone
# additionally exposes a genuine mid-teardown "archive" DESTROYING state (mined from
# test_digitalocean_smoke.py's test_probe_destruction_vocabulary).
# ---------------------------------------------------------------------------


def _seed_stuck_active(harness) -> dict[str, str]:
    """Seeds a resource that is still fully present/running (⇒ DESTROY_FAILED), returning its
    resource_ids. Mined verbatim from each provider's smoke test."""
    if harness.name == "digitalocean":
        stuck_id = harness.backend.seed_droplet(tags=["seedpod-managed"], status="active")
        harness.backend.mark_stuck_active(stuck_id)
        return {"droplet_id": stuck_id}
    if harness.name == "kind":
        harness.backend.seed_cluster("seedpod-stuck", running=True)
        return {"kind_cluster_name": "seedpod-stuck"}
    if harness.name == "tart":
        harness.backend.seed_vm("seedpod-stuck", running=True)
        return {"tart_vm_name": "seedpod-stuck"}
    raise AssertionError(f"no _seed_stuck_active mapping for {harness.name!r}")


def _gone_resource_ids(harness) -> dict[str, str]:
    if harness.name == "digitalocean":
        return {"droplet_id": "long-gone"}
    if harness.name == "kind":
        return {"kind_cluster_name": "seedpod-long-gone"}
    if harness.name == "tart":
        return {"tart_vm_name": "seedpod-long-gone"}
    raise AssertionError(f"no _gone_resource_ids mapping for {harness.name!r}")


async def test_probe_destruction_stuck_active_yields_destroy_failed(machine_harness):
    skip_if(_C11_C12_SKIPS, machine_harness.name)
    provider = machine_harness.provider()
    resource_ids = _seed_stuck_active(machine_harness)

    (result,) = await drain(provider, ProbeDestruction(resource_ids=resource_ids))
    assert result.value.status == DestroyStatus.DESTROY_FAILED
    assert result.value.stuck_resources


async def test_probe_destruction_gone_yields_destroyed(machine_harness):
    skip_if(_C11_C12_SKIPS, machine_harness.name)
    provider = machine_harness.provider()
    resource_ids = _gone_resource_ids(machine_harness)

    (result,) = await drain(provider, ProbeDestruction(resource_ids=resource_ids))
    assert result.value.status == DestroyStatus.DESTROYED


async def test_probe_destruction_in_progress_yields_destroying():
    """DO-specific: the one machine provider with a genuine mid-teardown ("archive") status
    distinct from both "still fully present" and "gone" (rows 9-15's DO-only nuance)."""
    from tests.conformance.digitalocean_harness import DigitalOceanHarness

    harness = DigitalOceanHarness()
    provider = harness.provider()
    destroying_id = harness.backend.seed_droplet(tags=["seedpod-managed"], status="archive")

    (result,) = await drain(provider, ProbeDestruction(resource_ids={"droplet_id": destroying_id}))
    assert result.value.status == DestroyStatus.DESTROYING


async def test_probe_destruction_transient_raises_unreachable(machine_harness):
    skip_if(_C11_C12_SKIPS, machine_harness.name)
    resource_ids = machine_harness.observe_command().resource_ids
    provider = machine_harness.provider(Fault.UNREACHABLE)
    with pytest.raises(InfrastructureUnreachableError):
        await drain(provider, ProbeDestruction(resource_ids=resource_ids))
