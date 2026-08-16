"""tests/engine/steps/test_kube_destroy_steps.py — the two ``kube.*`` COMPOSITES the
destroy workflows bind (Round 8b): ``kube.delete_daemonset`` (both destroy files) and
``kube.wipe_namespace`` (``destroy-shared.yml`` only).

Against the REAL ``KubectlProvider`` over the conformance fake TRANSPORT — no
``Mock``/``patch``. Both verbs are ``thin=False``: they issue more than one Seam C
command, which is the sanctioned composite shape (DR-0022).

The two v1 edge behaviours these pin, either of which is silently destructive to lose:

- **gotcha 10** — the Tailscale DaemonSet must be deleted, and observed GONE, before
  infrastructure teardown, or the node lingers in the tailnet for ~48 hours.
- **the built-in ``kubernetes`` service must survive a wipe.** v1 deleted services
  one-by-one by name specifically to skip it; ``delete services --all`` would break
  the shared cluster the wipe is meant to leave standing.
"""

from __future__ import annotations

import pytest
from pydantic import SecretStr

from seedpod.core.errors import InfrastructureUnreachableError
from seedpod.engine.step import EmptyOutput, NotReady, Ready, StepServices
from seedpod.engine.steps.kube import (
    DeleteDaemonsetParams,
    KubeDeleteDaemonset,
    KubeWipeNamespace,
    WipeNamespaceParams,
)
from tests.conformance.harness import Fault
from tests.conformance.kubectl_harness import FAKE_KUBECONFIG, KubectlHarness
from tests.engine.fakes import FakeSubprocessManager, make_step_context

_NS = "default"
_KUBECONFIG = SecretStr(FAKE_KUBECONFIG)


def _ctx(providers):
    return make_step_context(services=StepServices(subprocess_manager=FakeSubprocessManager(), providers=providers))


def _obj(namespace: str, name: str) -> dict:
    return {"metadata": {"name": name, "namespace": namespace}}


def test_declares_the_dr_0022_contract_for_both():
    daemonset = KubeDeleteDaemonset()
    wipe = KubeWipeNamespace()
    # P3's named exception: it ACTUATES and gates, so it keeps the actuator name
    # rather than being called kube.await_*.
    assert (daemonset.verb, daemonset.gateable, daemonset.thin) == ("kube.delete_daemonset", True, False)
    assert (wipe.verb, wipe.gateable, wipe.thin) == ("kube.wipe_namespace", False, False)
    for step in (daemonset, wipe):
        assert step.plane == "provider"
        assert step.provider_name == "kubectl"
        assert step.undoable is False


# ---------------------------------------------------------------------------
# kube.delete_daemonset -- gotcha 10.
# ---------------------------------------------------------------------------


def test_delete_daemonset_command_strips_wait_and_timeout_per_dr_0022_d2():
    """DR-0022 ruling 4: v1 ran `--wait=true --timeout=45s`; no Seam C command waits
    any more, so those move to the workflow's `gate:` block. `--grace-period` STAYS --
    it is how long Tailscale gets to disconnect, not a wait for the command."""
    step = KubeDeleteDaemonset()
    command = step.command(DeleteDaemonsetParams(kubeconfig=_KUBECONFIG, name="tailscale", grace_period_seconds=30))

    assert "--grace-period=30" in command.args
    assert not any(a.startswith("--wait") for a in command.args)
    assert not any(a.startswith("--timeout") for a in command.args)
    assert "--ignore-not-found=true" in command.args


async def test_delete_daemonset_deletes_it():
    harness = KubectlHarness()
    harness.backend.daemonsets = {(_NS, "tailscale"): _obj(_NS, "tailscale")}
    step = KubeDeleteDaemonset()
    params = DeleteDaemonsetParams(kubeconfig=_KUBECONFIG, name="tailscale", namespace=_NS)

    output = await step.execute(params, _ctx({"kubectl": harness.provider()}))

    assert isinstance(output, EmptyOutput)
    assert (_NS, "tailscale") not in harness.backend.daemonsets


async def test_delete_daemonset_of_an_absent_daemonset_is_success():
    """--ignore-not-found: the destroy path retries, and absence is the state this
    step exists to reach."""
    harness = KubectlHarness()
    harness.backend.daemonsets = {}
    step = KubeDeleteDaemonset()

    output = await step.execute(
        DeleteDaemonsetParams(kubeconfig=_KUBECONFIG, name="tailscale", namespace=_NS),
        _ctx({"kubectl": harness.provider()}),
    )

    assert isinstance(output, EmptyOutput)


async def test_delete_daemonset_gate_is_not_ready_until_it_is_actually_gone():
    """Gotcha 10's real protection: the gate must not report Ready while the DaemonSet
    still exists, or teardown proceeds before Tailscale has disconnected."""
    harness = KubectlHarness()
    harness.backend.daemonsets = {(_NS, "tailscale"): _obj(_NS, "tailscale")}
    step = KubeDeleteDaemonset()
    params = DeleteDaemonsetParams(kubeconfig=_KUBECONFIG, name="tailscale", namespace=_NS)
    ctx = _ctx({"kubectl": harness.provider()})

    assert isinstance(await step.poll_ready(params, EmptyOutput(), ctx), NotReady)

    del harness.backend.daemonsets[(_NS, "tailscale")]

    assert isinstance(await step.poll_ready(params, EmptyOutput(), ctx), Ready)


async def test_delete_daemonset_gate_ignores_OTHER_daemonsets_in_the_namespace():
    harness = KubectlHarness()
    harness.backend.daemonsets = {(_NS, "node-exporter"): _obj(_NS, "node-exporter")}
    step = KubeDeleteDaemonset()
    params = DeleteDaemonsetParams(kubeconfig=_KUBECONFIG, name="tailscale", namespace=_NS)

    result = await step.poll_ready(params, EmptyOutput(), _ctx({"kubectl": harness.provider()}))

    assert isinstance(result, Ready)


async def test_delete_daemonset_unreachable_is_never_read_as_absence():
    """CLAUDE.md's error-taxonomy rule: "cannot determine state" must never be
    conflated with absence. The probe lists names rather than doing
    `get daemonset NAME` (whose NotFound arrives as a non-zero exit), so a genuine
    connectivity failure raises instead of being read as "it's gone, proceed"."""
    harness = KubectlHarness()
    harness.backend.daemonsets = {(_NS, "tailscale"): _obj(_NS, "tailscale")}
    step = KubeDeleteDaemonset()
    params = DeleteDaemonsetParams(kubeconfig=_KUBECONFIG, name="tailscale", namespace=_NS)

    with pytest.raises(InfrastructureUnreachableError):
        await step.poll_ready(params, EmptyOutput(), _ctx({"kubectl": harness.provider(Fault.UNREACHABLE)}))


@pytest.mark.parametrize("method", ["execute", "poll_ready"])
async def test_delete_daemonset_with_no_kubeconfig_is_a_clean_no_op(method):
    """A cluster whose provisioning died before `cluster.store_kubeconfig` has no
    kubeconfig but still has infrastructure to tear down -- it must not be blocked
    here. `cluster.load_kubeconfig_optional` yields None for exactly this case."""

    class _ExplodingProviders(dict):
        def __getitem__(self, key):
            raise AssertionError("must not touch a provider with no kubeconfig")

    step = KubeDeleteDaemonset()
    params = DeleteDaemonsetParams(kubeconfig=None, name="tailscale", namespace=_NS)
    ctx = _ctx(_ExplodingProviders())

    if method == "execute":
        assert isinstance(await step.execute(params, ctx), EmptyOutput)
    else:
        assert isinstance(await step.poll_ready(params, EmptyOutput(), ctx), Ready)


# ---------------------------------------------------------------------------
# kube.wipe_namespace -- destroy-shared.yml's sweep.
# ---------------------------------------------------------------------------


async def test_wipe_namespace_removes_deployed_resources():
    harness = KubectlHarness()
    harness.backend.daemonsets = {(_NS, "tailscale"): _obj(_NS, "tailscale")}
    step = KubeWipeNamespace()

    output = await step.execute(
        WipeNamespaceParams(kubeconfig=_KUBECONFIG, namespace=_NS), _ctx({"kubectl": harness.provider()})
    )

    assert isinstance(output, EmptyOutput)
    assert harness.backend.deployments == {}
    assert harness.backend.daemonsets == {}
    assert harness.backend.pods == {}


async def test_wipe_namespace_PRESERVES_the_builtin_kubernetes_service():
    """THE edge behaviour of this verb. v1 deleted services one-by-one by name
    specifically to skip `kubernetes`; `delete services --all` would remove the
    cluster's own service and break the shared cluster this wipe leaves standing."""
    harness = KubectlHarness()
    harness.backend.services = {
        (_NS, "kubernetes"): _obj(_NS, "kubernetes"),
        (_NS, "exampleco-web-2"): _obj(_NS, "exampleco-web-2"),
        (_NS, "redis"): _obj(_NS, "redis"),
    }
    step = KubeWipeNamespace()

    await step.execute(WipeNamespaceParams(kubeconfig=_KUBECONFIG, namespace=_NS), _ctx({"kubectl": harness.provider()}))

    assert set(harness.backend.services) == {(_NS, "kubernetes")}


async def test_wipe_namespace_skips_the_service_delete_when_only_the_builtin_remains():
    """No doomed services => no third command at all, rather than
    `kubectl delete services` with an empty name list (which real kubectl rejects)."""
    harness = KubectlHarness()
    harness.backend.services = {(_NS, "kubernetes"): _obj(_NS, "kubernetes")}
    step = KubeWipeNamespace()

    await step.execute(WipeNamespaceParams(kubeconfig=_KUBECONFIG, namespace=_NS), _ctx({"kubectl": harness.provider()}))

    assert set(harness.backend.services) == {(_NS, "kubernetes")}
    assert not any(call[:2] == ("delete", "services") for call in harness.backend.call_log)


async def test_wipe_namespace_with_no_kubeconfig_is_a_clean_no_op():
    class _ExplodingProviders(dict):
        def __getitem__(self, key):
            raise AssertionError("must not touch a provider with no kubeconfig")

    step = KubeWipeNamespace()

    output = await step.execute(WipeNamespaceParams(kubeconfig=None, namespace=_NS), _ctx(_ExplodingProviders()))

    assert isinstance(output, EmptyOutput)


async def test_wipe_namespace_unreachable_propagates_as_itself():
    harness = KubectlHarness()
    step = KubeWipeNamespace()

    with pytest.raises(InfrastructureUnreachableError):
        await step.execute(
            WipeNamespaceParams(kubeconfig=_KUBECONFIG, namespace=_NS),
            _ctx({"kubectl": harness.provider(Fault.UNREACHABLE)}),
        )
