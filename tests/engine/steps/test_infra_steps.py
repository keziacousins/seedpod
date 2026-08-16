"""tests/engine/steps/test_infra_steps.py — ``seedpod/engine/steps/infra.py``'s
five verbs (Round 8a, "infra-and-do" component): ``infra.create_instance``,
``infra.await_instance``, ``infra.fetch_kubeconfig``, ``do.apply_firewalls``,
``do.assign_project``.

Against the REAL, already-conformance-tested provider implementations
(``seedpod/providers/{digitalocean,kind,tart,orbstack}.py``), backed by the
shared conformance ``Harness``es' FAKE TRANSPORTS
(``tests/conformance/{digitalocean,kind,tart,orbstack}_harness.py``) — never
``Mock``/``patch`` anywhere (CLAUDE.md testing posture). ``ctx`` is a real
``StepContext`` built via ``tests/engine/fakes.py``'s ``make_step_context``,
with a real ``StepServices`` whose ``providers`` mapping holds real
``Provider`` instances.

Covers this task's own checklist:
- late binding selects the right adapter purely by ``params.provider`` for all
  four machine-plane providers, with NO branching in the step (same
  ``InfraCreateInstance``/``InfraAwaitInstance``/``InfraFetchKubeconfig``
  instance reused across all four; only the ``ctx.services.providers`` mapping
  and ``params.provider`` change).
- ``command(params)`` is pure (same params -> equal command, twice).
- ``infra.await_instance``'s ``poll_ready`` issues exactly ONE probe per call
  (asserted via each harness's own transport-attempt counter) and never
  sleeps.
- ``adopted_existing`` is ``True`` for an adoption path (DO's cluster-uuid-tag
  re-invocation; orbstack's unconditional adopt-the-fixed-cluster) and
  ``False`` for a fresh create.
- ``infra.create_instance``'s ``undo`` destroys the instance and is
  absent-tolerant on a second call.
- ``InfrastructureUnreachableError`` from the provider propagates as itself
  (never converted to absence, never swallowed into compensation — that
  decision is the ENGINE's, per Conflict 5; this module only proves the Step
  layer doesn't get in the way of the propagation).
"""

from __future__ import annotations

import pytest

from seedpod.core.cluster_spec import ClusterConfiguration, ClusterSpecification, NodeSpecification
from seedpod.core.errors import InfrastructureUnreachableError
from seedpod.engine.step import EmptyOutput, NotReady, Ready, StepServices
from seedpod.engine.steps.infra import (
    AddressOutput,
    ApplyFirewallsParams,
    AssignToProjectParams,
    AwaitInstanceParams,
    CreateInstanceParams,
    DoApplyFirewalls,
    DoAssignToProject,
    FetchKubeconfigByResourceIdsParams,
    InfraAwaitInstance,
    InfraCreateInstance,
    InfraFetchKubeconfig,
)
from tests.conformance._support import MACHINE_HARNESS_CLASSES
from tests.conformance.digitalocean_harness import DigitalOceanHarness
from tests.conformance.harness import Fault
from tests.conformance.kind_harness import KindHarness
from tests.conformance.orbstack_harness import OrbstackHarness
from tests.engine.fakes import FakeSubprocessManager, RecordingNoteSink, make_step_context


def _spec(*, ttl_hours: float | None = None) -> ClusterSpecification:
    return ClusterSpecification(
        node_specification=NodeSpecification(cpu_cores=1, memory_gb=1, region_hint="europe-west"),
        cluster_config=ClusterConfiguration(
            pod_cidr="10.42.7.0/24", service_cidr="10.43.7.0/24", ttl_hours=ttl_hours
        ),
    )


def _ctx(providers, *, note_sink=None):
    return make_step_context(
        services=StepServices(subprocess_manager=FakeSubprocessManager(), providers=providers),
        note_sink=note_sink,
    )


# ---------------------------------------------------------------------------
# Declared-contract sanity (mirrors test_domain_steps.py's own such test).
# ---------------------------------------------------------------------------


def test_declares_the_dr_0022_contract_for_all_five():
    # (step, verb, gateable, undoable, idempotent) -- `infra.create_instance` is
    # seam-b-engine.md's one pinned non-idempotent verb (Round-8a review
    # finding: DR-0022 renamed do.create_droplet -> infra.create_instance
    # without ratifying a flip to Step's idempotent=True default).
    cases = [
        (InfraCreateInstance(), "infra.create_instance", False, True, False),
        (InfraAwaitInstance(), "infra.await_instance", True, False, True),
        (InfraFetchKubeconfig(), "infra.fetch_kubeconfig", False, False, True),
        (DoApplyFirewalls(), "do.apply_firewalls", False, False, True),
        (DoAssignToProject(), "do.assign_project", False, False, True),
    ]
    for step, verb, gateable, undoable, idempotent in cases:
        assert step.verb == verb
        assert step.plane == "provider"
        assert step.thin is True
        assert step.gateable is gateable
        assert step.undoable is undoable
        assert step.idempotent is idempotent


# ---------------------------------------------------------------------------
# Late binding: one Step instance, four providers, pure dict lookup.
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("harness_cls", MACHINE_HARNESS_CLASSES, ids=[c.name for c in MACHINE_HARNESS_CLASSES])
async def test_create_instance_late_binds_purely_by_provider_with_no_branching(harness_cls):
    """The SAME `InfraCreateInstance` instance is reused across all four
    providers; only `ctx.services.providers`/`params.provider` vary. If the
    step branched on provider name internally, at least one of these four
    would betray it -- it doesn't, because `command()`/`output_from()` are
    provider-shape-agnostic and the adapter is a dict lookup."""
    step = InfraCreateInstance()
    harness = harness_cls()
    provider = harness.provider()
    ctx = _ctx({harness.name: provider})
    params = CreateInstanceParams(
        provider=harness.name, spec=_spec(), cluster_id="c-late-bind", slug="demo-cluster"
    )

    output = await step.execute(params, ctx)

    assert output.resource_ids
    # orbstack's create is unconditionally an adoption of its one fixed
    # built-in cluster (see the dedicated orbstack test below); every other
    # machine-plane provider's FIRST create for a fresh cluster_id is not.
    assert output.adopted_existing is (harness.name == "orbstack")


async def test_create_instance_command_is_pure():
    step = InfraCreateInstance()
    params = CreateInstanceParams(
        provider="digitalocean", spec=_spec(ttl_hours=4), cluster_id="c1", slug="demo-cluster"
    )

    first = step.command(params)
    second = step.command(params)

    assert first == second
    assert first.cluster_uuid == "c1"
    assert first.slug == "demo-cluster"
    assert set(first.tags) == {"cluster-uuid:c1", "cluster-demo-cluster", "ttl-4"}


async def test_create_instance_omits_ttl_tag_when_spec_has_no_ttl():
    step = InfraCreateInstance()
    params = CreateInstanceParams(provider="digitalocean", spec=_spec(ttl_hours=None), cluster_id="c1", slug="s")

    cmd = step.command(params)

    assert set(cmd.tags) == {"cluster-uuid:c1", "cluster-s"}


# ---------------------------------------------------------------------------
# adopted_existing: True on an adoption path, False on a fresh create.
# ---------------------------------------------------------------------------


async def test_create_instance_adopted_existing_false_then_true_on_reinvocation():
    """DigitalOcean's cluster-uuid tag re-invocation (conformance C-07): the
    SAME cluster_id/slug create twice must adopt, not duplicate."""
    step = InfraCreateInstance()
    harness = DigitalOceanHarness()
    provider = harness.provider()
    ctx = _ctx({"digitalocean": provider})
    params = CreateInstanceParams(provider="digitalocean", spec=_spec(), cluster_id="c-adopt", slug="demo-cluster")

    first = await step.execute(params, ctx)
    before = await harness.backend_resources()
    second = await step.execute(params, ctx)
    after = await harness.backend_resources()

    assert first.adopted_existing is False
    assert second.adopted_existing is True
    assert second.resource_ids == first.resource_ids
    assert before == after, "re-invocation must never create a duplicate backend resource"


async def test_create_instance_orbstack_always_adopts_the_fixed_cluster():
    """Orbstack's create is unconditionally `adopted_existing=True` (module
    docstring: "create" == verify + adopt the single pre-existing cluster) --
    the honesty ruling-1 exists for: every provider's `adopted_existing` is
    real, not just orbstack's."""
    step = InfraCreateInstance()
    harness = OrbstackHarness()
    ctx = _ctx({"orbstack": harness.provider()})
    params = CreateInstanceParams(provider="orbstack", spec=_spec(), cluster_id="c1", slug="demo-cluster")

    output = await step.execute(params, ctx)

    assert output.adopted_existing is True
    assert output.address is not None


# ---------------------------------------------------------------------------
# infra.create_instance undo: destroys, and is absent-tolerant on a 2nd call.
# ---------------------------------------------------------------------------


async def test_create_instance_undo_destroys_and_is_absent_tolerant_on_second_call():
    step = InfraCreateInstance()
    harness = DigitalOceanHarness()
    provider = harness.provider()
    ctx = _ctx({"digitalocean": provider})
    params = CreateInstanceParams(provider="digitalocean", spec=_spec(), cluster_id="c-undo", slug="demo-cluster")

    output = await step.execute(params, ctx)
    assert await harness.backend_resources()

    await step.undo(params, output, {}, ctx)
    leftover = await harness.backend_resources()
    assert not any(rid in leftover for rid in output.resource_ids.values())

    # Absent-tolerant: undoing an already-destroyed instance must not raise.
    await step.undo(params, output, {}, ctx)


@pytest.mark.parametrize("harness_cls", MACHINE_HARNESS_CLASSES, ids=[c.name for c in MACHINE_HARNESS_CLASSES])
async def test_create_instance_undo_across_all_four_providers(harness_cls):
    """DR-0022 ruling 1's undo law extended to all four adapters (Round-8a
    review finding): late-bound undo resolves a DIFFERENT adapter per
    `params.provider` (`late_bound.py`'s `undo`), and each has genuinely
    distinct destroy semantics -- DO/kind/tart destroy by deterministic
    identity (the resource actually disappears from `backend_resources()`);
    orbstack's `DestroyInstance` is an unconditional preserve-no-op
    (`orbstack.py`: "nothing to destroy at the infra level" -- the resource
    stays present) that `provision-orbstack.yml`'s own `on_failure: compensate`
    reasoning depends on being safe, not on the resource vanishing. Either way
    a SECOND undo call must not raise (absent-tolerant/idempotent, C-10/C-23)."""
    step = InfraCreateInstance()
    harness = harness_cls()
    ctx = _ctx({harness.name: harness.provider()})
    params = CreateInstanceParams(provider=harness.name, spec=_spec(), cluster_id="c-undo-all", slug="demo-cluster")

    output = await step.execute(params, ctx)
    assert await harness.backend_resources()

    await step.undo(params, output, {}, ctx)
    after_undo = await harness.backend_resources()
    resource_names = set(output.resource_ids.values())
    if harness.name == "orbstack":
        assert resource_names & after_undo, "orbstack's DestroyInstance preserves the cluster"
    else:
        assert not (resource_names & after_undo), f"{harness.name}: undo must actually destroy the instance"

    # Absent-tolerant/idempotent on every provider, regardless of the first call's outcome.
    await step.undo(params, output, {}, ctx)


async def test_create_instance_undo_after_partial_create_uses_notes_when_output_is_none():
    """The C1 close, at the Step layer: a truncated create (this Step's
    `execute()` never returns, e.g. the run crashed) still leaves the
    resource_ids in `ctx.note()`'s durable notes -- `undo(params, output=None,
    notes=...)` must use THOSE, not the (absent) output, to destroy."""
    step = InfraCreateInstance()
    harness = DigitalOceanHarness()
    dying_provider = harness.provider(Fault.DIE_MID_CREATE)
    note_sink = RecordingNoteSink()
    ctx = _ctx({"digitalocean": dying_provider}, note_sink=note_sink)
    params = CreateInstanceParams(provider="digitalocean", spec=_spec(), cluster_id="c-partial", slug="demo-cluster")

    with pytest.raises(Exception):  # noqa: B017,PT011 -- ProviderError subclass, exact type not the point here
        await step.execute(params, ctx)

    assert note_sink.calls, "RESOURCE_ALLOCATED must have been noted before the stream died"
    notes = note_sink.calls[-1][2]
    assert notes, "notes must carry the flattened resource_ids"
    assert await harness.backend_resources()  # something was actually allocated

    clean_ctx = _ctx({"digitalocean": harness.provider()})
    await step.undo(params, None, notes, clean_ctx)

    leftover = await harness.backend_resources()
    assert not any(rid in leftover for rid in notes.values())


# ---------------------------------------------------------------------------
# infra.await_instance: execute is a true no-op; poll_ready issues ONE probe.
# ---------------------------------------------------------------------------


async def test_await_instance_execute_is_a_noop_never_touches_providers():
    class _ExplodingProviders(dict):
        def __getitem__(self, key):
            raise AssertionError(f"execute() must never look up a provider, got {key!r}")

    step = InfraAwaitInstance()
    ctx = _ctx(_ExplodingProviders())
    params = AwaitInstanceParams(provider="digitalocean", resource_ids={"droplet_id": "1"})

    output = await step.execute(params, ctx)

    assert isinstance(output, AddressOutput)


async def test_await_instance_poll_ready_issues_exactly_one_probe_and_never_sleeps():
    step = InfraAwaitInstance()
    harness = KindHarness()
    ctx = _ctx({"kind": harness.provider()})
    observe = harness.observe_command()  # the pre-seeded, already-running cluster
    params = AwaitInstanceParams(provider="kind", resource_ids=observe.resource_ids)
    provisional = AddressOutput(address="")

    before = harness.backend_attempts()
    result = await step.poll_ready(params, provisional, ctx)
    after = harness.backend_attempts()

    assert after - before == 1, "poll_ready must issue exactly ONE probe per call"
    assert isinstance(result, Ready)
    assert result.outputs is not None
    assert result.outputs.address


async def test_await_instance_not_ready_while_still_provisioning():
    """A freshly-created DO droplet starts life at DO's `status: "new"` --
    `infra.await_instance` must report NotReady, not fabricate an address."""
    step = InfraAwaitInstance()
    create_step = InfraCreateInstance()
    harness = DigitalOceanHarness()
    provider = harness.provider()
    ctx = _ctx({"digitalocean": provider})
    create_params = CreateInstanceParams(
        provider="digitalocean", spec=_spec(), cluster_id="c-provisioning", slug="demo-cluster"
    )
    created = await create_step.execute(create_params, ctx)

    params = AwaitInstanceParams(provider="digitalocean", resource_ids=created.resource_ids)
    result = await step.poll_ready(params, AddressOutput(address=""), ctx)

    assert isinstance(result, NotReady)


async def test_await_instance_unreachable_propagates_as_itself():
    step = InfraAwaitInstance()
    harness = KindHarness()
    ctx = _ctx({"kind": harness.provider(Fault.UNREACHABLE)})
    observe = harness.observe_command()
    params = AwaitInstanceParams(provider="kind", resource_ids=observe.resource_ids)

    with pytest.raises(InfrastructureUnreachableError):
        await step.poll_ready(params, AddressOutput(address=""), ctx)


# ---------------------------------------------------------------------------
# infra.fetch_kubeconfig (resource_ids variant -- kind/orbstack).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("harness_cls", [KindHarness, OrbstackHarness], ids=["kind", "orbstack"])
async def test_fetch_kubeconfig_late_binds_and_returns_a_secret(harness_cls):
    step = InfraFetchKubeconfig()
    harness = harness_cls()
    ctx = _ctx({harness.name: harness.provider()})
    observe = harness.observe_command()
    params = FetchKubeconfigByResourceIdsParams(
        provider=harness.name, resource_ids=observe.resource_ids, rewrite_server_to="minimax.local"
    )

    output = await step.execute(params, ctx)

    assert output.kubeconfig.get_secret_value()
    assert "minimax.local" in output.kubeconfig.get_secret_value()


async def test_fetch_kubeconfig_command_is_pure():
    step = InfraFetchKubeconfig()
    params = FetchKubeconfigByResourceIdsParams(
        provider="kind", resource_ids={"kind_cluster_name": "x", "api_port": "6443"}, rewrite_server_to="host"
    )

    assert step.command(params) == step.command(params)


# ---------------------------------------------------------------------------
# do.apply_firewalls / do.assign_project -- fixed provider_name, DO-only.
# ---------------------------------------------------------------------------


async def test_apply_firewalls_command_is_pure_and_ensures_management_and_app_firewalls():
    step = DoApplyFirewalls()
    assert step.provider_name == "digitalocean"
    harness = DigitalOceanHarness()
    droplet_id = harness.backend.seed_droplet(tags=["seedpod-managed"])
    ctx = _ctx({"digitalocean": harness.provider()})
    params = ApplyFirewallsParams(resource_ids={"droplet_id": droplet_id}, spec=_spec())

    assert step.command(params) == step.command(params)
    output = await step.execute(params, ctx)

    assert isinstance(output, EmptyOutput)
    assert len(harness.backend.firewalls) == 2  # management + application


async def test_assign_project_binds_to_digitalocean_only_and_is_thin():
    step = DoAssignToProject()
    assert step.provider_name == "digitalocean"
    harness = DigitalOceanHarness()
    droplet_id = harness.backend.seed_droplet(tags=["seedpod-managed"])
    ctx = _ctx({"digitalocean": harness.provider()})
    params = AssignToProjectParams(resource_ids={"droplet_id": droplet_id})

    assert step.command(params) == step.command(params)
    output = await step.execute(params, ctx)
    assert isinstance(output, EmptyOutput)
    assigned = harness.backend.project_resources.get(harness.provider().config.project_id, set())
    assert f"do:droplet:{droplet_id}" in assigned
