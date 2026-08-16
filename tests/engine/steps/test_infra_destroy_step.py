"""tests/engine/steps/test_infra_destroy_step.py — ``infra.destroy_instance``
(Round 8b's last verb), the terminal step of both destroy workflows.

Against the REAL ``DigitalOceanProvider`` over its conformance fake TRANSPORT — no
``Mock``/``patch``. DO is used because it is the one machine provider with a genuine
mid-teardown status (``archive`` ⇒ DESTROYING) distinct from both "still present" and
"gone", so all three ``DestroyStatus`` arms are reachable.

**The centrepiece is coherence-review Conflict 5's park law.** An
``InfrastructureUnreachableError`` from the probe must propagate, and the three ways
of getting that wrong are each pinned below, because each is separately catastrophic:

1. as ``NotReady`` — the gate's timeout would burn down while the state is merely
   unobservable;
2. folded into ``max_consecutive_poll_failures`` — that counter is
   ``TransientError``-only, so an unreachable API would fail a destroy that was fine;
3. as "gone" — the cluster row goes terminal while the droplet keeps billing, and
   nothing points at it any more. This is the one that costs money silently.
"""

from __future__ import annotations

import pytest

from seedpod.core.errors import InfrastructureUnreachableError, PermanentError
from seedpod.engine.step import EmptyOutput, NotReady, Ready, StepServices
from seedpod.engine.steps.infra import DestroyInstanceParams, InfraDestroyInstance
from seedpod.providers.contract import DestroyInstance, DestroyOutcome, DestroyStatus
from tests.conformance.digitalocean_harness import DigitalOceanHarness
from tests.conformance.harness import Fault
from tests.engine.fakes import FakeSubprocessManager, make_step_context


def _ctx(providers):
    return make_step_context(services=StepServices(subprocess_manager=FakeSubprocessManager(), providers=providers))


def _params(resource_ids: dict[str, str], *, provider: str = "digitalocean") -> DestroyInstanceParams:
    return DestroyInstanceParams(provider=provider, slug="demo-cluster", resource_ids=resource_ids)


def test_declares_the_dr_0022_contract():
    step = InfraDestroyInstance()
    assert step.verb == "infra.destroy_instance"
    assert step.plane == "provider"
    assert step.thin is False  # DestroyInstance + ProbeDestruction
    # P3's second named actuate-and-gate verb: gateable, but keeps the actuator name.
    assert step.gateable is True
    assert step.undoable is False


def test_command_carries_the_slug_for_dos_legacy_tag_fallback():
    step = InfraDestroyInstance()
    params = _params({"droplet_id": "12345"})

    first = step.command(params)
    second = step.command(params)

    assert first == second
    assert first == DestroyInstance(slug="demo-cluster", resource_ids={"droplet_id": "12345"})


# ---------------------------------------------------------------------------
# execute() -- initiate the destroy.
# ---------------------------------------------------------------------------


async def test_execute_initiates_the_destroy():
    harness = DigitalOceanHarness()
    droplet_id = harness.backend.seed_droplet(tags=["seedpod-managed"], status="active")
    step = InfraDestroyInstance()

    output = await step.execute(_params({"droplet_id": droplet_id}), _ctx({"digitalocean": harness.provider()}))

    assert isinstance(output, EmptyOutput)
    assert droplet_id not in harness.backend.droplets


async def test_execute_on_an_already_absent_resource_is_success():
    """Idempotent on absence -- the destroy path retries, and absence is the state
    this verb exists to reach."""
    harness = DigitalOceanHarness()
    step = InfraDestroyInstance()

    output = await step.execute(_params({"droplet_id": "long-gone"}), _ctx({"digitalocean": harness.provider()}))

    assert isinstance(output, EmptyOutput)


# ---------------------------------------------------------------------------
# probe() -- the gate. All three DestroyStatus arms.
# ---------------------------------------------------------------------------


async def test_probe_ready_when_the_resource_is_gone():
    harness = DigitalOceanHarness()
    step = InfraDestroyInstance()
    provider = harness.provider()

    result = await step.probe(_params({"droplet_id": "long-gone"}), EmptyOutput(), provider, _ctx({}))

    assert isinstance(result, Ready)


async def test_probe_not_ready_while_the_teardown_is_still_in_progress():
    """DO's `archive` status: genuinely mid-teardown, neither present nor gone."""
    harness = DigitalOceanHarness()
    destroying_id = harness.backend.seed_droplet(tags=["seedpod-managed"], status="archive")
    step = InfraDestroyInstance()

    result = await step.probe(_params({"droplet_id": destroying_id}), EmptyOutput(), harness.provider(), _ctx({}))

    assert isinstance(result, NotReady)


async def test_probe_of_a_still_present_resource_is_NOT_READY_never_a_raise():
    """REGRESSION TEST for a bug this step shipped with and a real destroy caught
    (2026-08-03). DigitalOcean's delete is ASYNCHRONOUS -- the droplet keeps reporting
    `status: active` for seconds after a successful delete, and DO's probe maps active
    -> DESTROY_FAILED. The first version raised on that, failing a destroy 2.2s after
    initiating it; the droplet then died normally, leaving the cluster wrongly marked
    DESTROY_FAILED.

    "Still fully present" only MEANS stuck once enough time has passed, and deciding
    how much is exactly what the gate's `timeout_seconds: 900` is for."""
    harness = DigitalOceanHarness()
    stuck_id = harness.backend.seed_droplet(tags=["seedpod-managed"], status="active")
    harness.backend.mark_stuck_active(stuck_id)
    step = InfraDestroyInstance()

    result = await step.probe(_params({"droplet_id": stuck_id}), EmptyOutput(), harness.provider(), _ctx({}))

    assert isinstance(result, NotReady)


def test_initiate_verdict_still_raises_on_destroy_failed():
    """The other side of that asymmetry: DESTROY_FAILED from the INITIATE call is a
    real terminal answer -- the provider refused the request -- mirroring v1's own
    three-way branch on destruction_result["status"].

    Exercised through `output_from` (the initiate verdict) with a real DestroyOutcome
    rather than through a harness: DO's fake cannot produce a rejected delete (its
    stuck-active state only surfaces on the probe), and inventing a provider double to
    force it would be exactly the Mock the testing posture forbids. `output_from` is a
    pure mapping over a real DTO, so calling it directly is the honest test."""
    step = InfraDestroyInstance()
    outcome = DestroyOutcome(
        status=DestroyStatus.DESTROY_FAILED, error="provider refused", stuck_resources=("12345",)
    )

    with pytest.raises(PermanentError) as exc_info:
        step.output_from(outcome)

    assert "destroy_failed" in str(exc_info.value)
    assert exc_info.value.detail.get("stuck_resources") == "12345"


def test_initiate_verdict_accepts_both_destroyed_and_destroying():
    """v1's two continuing arms: "destroyed" (already gone) and "destroying" (in
    flight) both proceed to the gate rather than failing here."""
    step = InfraDestroyInstance()
    for status in (DestroyStatus.DESTROYED, DestroyStatus.DESTROYING):
        assert isinstance(step.output_from(DestroyOutcome(status=status)), EmptyOutput)


# ---------------------------------------------------------------------------
# Conflict 5's park law -- the three ways to get it wrong.
# ---------------------------------------------------------------------------


async def test_unreachable_probe_propagates_and_is_NEVER_notready_or_gone():
    """All three clauses at once. The step must not catch this: propagating is what
    parks the run (suspending the gate's timeout), keeps it out of
    max_consecutive_poll_failures (TransientError-only), and — most importantly —
    stops a cluster being reported DESTROYED because its API could not be reached,
    which would leave a billing droplet behind with nothing pointing at it."""
    harness = DigitalOceanHarness()
    droplet_id = harness.backend.seed_droplet(tags=["seedpod-managed"], status="active")
    step = InfraDestroyInstance()

    with pytest.raises(InfrastructureUnreachableError):
        await step.probe(
            _params({"droplet_id": droplet_id}), EmptyOutput(), harness.provider(Fault.UNREACHABLE), _ctx({})
        )

    # And the resource is still there: nothing concluded absence from unreachability.
    assert droplet_id in harness.backend.droplets


async def test_poll_ready_template_also_propagates_unreachable():
    """Via the LateBoundProviderStep.poll_ready template (the path the engine
    actually calls), not just probe() directly."""
    harness = DigitalOceanHarness()
    droplet_id = harness.backend.seed_droplet(tags=["seedpod-managed"], status="active")
    step = InfraDestroyInstance()

    with pytest.raises(InfrastructureUnreachableError):
        await step.poll_ready(
            _params({"droplet_id": droplet_id}), EmptyOutput(), _ctx({"digitalocean": harness.provider(Fault.UNREACHABLE)})
        )


# ---------------------------------------------------------------------------
# Gotcha 1: the disabled-provider gap, surfaced rather than hidden.
# ---------------------------------------------------------------------------


async def test_a_disabled_provider_fails_with_a_message_naming_the_cause():
    """v1 destroyed with check_enabled=False "because we need to destroy clusters even
    if provider is now disabled". v2's load_enabled_providers omits disabled providers
    entirely, so this cannot work today -- see `_resolve_provider`'s docstring and the
    DR it calls for. What this test pins is that the failure NAMES the cause instead
    of surfacing as a bare KeyError from a dict lookup."""
    step = InfraDestroyInstance()

    with pytest.raises(PermanentError) as exc_info:
        await step.execute(_params({"droplet_id": "12345"}), _ctx({}))  # provider absent == disabled

    message = str(exc_info.value)
    assert "not enabled" in message
    assert "digitalocean" in message
