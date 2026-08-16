"""tests/engine/steps/test_kube_provision_steps.py — ``seedpod/engine/steps/kube.py``'s
two verbs (Round 8a, "kube-shim" component): ``kube.apply_file``,
``kube.await_rollout`` — the Traefik infra-shim verbs ``provision-{kind,orbstack}.yml``
need. With these two, all 14 provision-path verbs exist.

Against the REAL, already-conformance-tested ``KubectlProvider``
(``seedpod/providers/kubectl.py``), backed by the shared conformance harness's FAKE
TRANSPORT (``tests/conformance/fake_kubectl.py`` / ``KubectlHarness``) — never
``Mock``/``patch`` anywhere (CLAUDE.md testing posture). ``ctx`` is a real
``StepContext`` built via ``tests/engine/fakes.py``'s ``make_step_context``.

Covers this task's own checklist:
- ``kube.apply_file`` applies the right manifest (the exact file text at
  ``manifest_path``), and its undo deletes it and is absent-tolerant (a second
  undo is a no-op success) — DR-0022 ruling 3: this is the infra-shim verb,
  ``undoable=True``, NOT ``kube.apply_docs``.
- ``kube.apply_file`` resolves ``manifest_path`` against the INJECTED
  ``config_dir``, never the process cwd (Round-8a gate finding M-2) — the one
  test in here that is a regression test for a silent v1 regression rather
  than a forward contract.
- ``kube.await_rollout`` issues exactly ONE probe per ``poll_ready`` call
  (asserted via the harness's own transport-attempt counter) and never sleeps.
- ``kube.await_rollout`` returns ``NotReady`` (never raises) for an incomplete
  rollout — the CRITICAL non-fatal path (crown jewel #10): a merely-slow
  Traefik rollout must not fail provisioning.
- ``InfrastructureUnreachableError`` from the provider propagates as itself
  rather than becoming ``NotReady`` — for both verbs.
- Both verbs' plane/thin/gateable/undoable match ``tests/engine/declared_verbs.py``.
"""

from __future__ import annotations

import pytest

from seedpod.core.errors import InfrastructureUnreachableError, PermanentError
from seedpod.core.paths import resolve_under_config_dir
from seedpod.engine.step import EmptyOutput, NotReady, Ready, StepServices
from seedpod.engine.steps.kube import (
    ApplyFileParams,
    KubeApplyFile,
    KubeAwaitRollout,
    ProbeRolloutParams,
)
from seedpod.providers.contract import (
    KubeApplyManifest,
    KubeProbeRollout,
    Provider,
    ProviderCommand,
)
from tests.conformance.harness import Fault
from tests.conformance.kubectl_harness import FAKE_KUBECONFIG, KubectlHarness
from tests.engine.fakes import FakeSubprocessManager, make_step_context

_NAMESPACE = "traefik"
_DEPLOYMENT = "traefik"

_TRAEFIK_MANIFEST = """\
apiVersion: apps/v1
kind: Deployment
metadata:
  name: traefik
  namespace: traefik
spec:
  replicas: 1
"""


def _ctx(providers):
    return make_step_context(services=StepServices(subprocess_manager=FakeSubprocessManager(), providers=providers))


class _CountingProvider:
    """Wraps a real ``Provider``, counting ``execute()`` calls -- the Step-layer's
    own responsibility (issue exactly one probe per ``poll_ready``), decoupled
    from however many raw subprocess calls the provider's own implementation
    happens to make per command (mirrors test_k3s_steps.py's own such fake)."""

    def __init__(self, inner: Provider) -> None:
        self._inner = inner
        self.execute_calls = 0

    def execute(self, command: ProviderCommand):
        self.execute_calls += 1
        return self._inner.execute(command)


def _seed_deployment(harness: KubectlHarness, *, ready: bool) -> None:
    """Overrides the harness's default ``web``/``default`` deployment with a
    ``traefik``/``traefik`` one -- matching both shipped workflows' literal
    ``deployment: "traefik", namespace: "traefik"`` binding -- either fully
    rolled out (``ready=True``) or still progressing (``ready=False``, the
    non-fatal row-31/row-26 case: `_probe_rollout` recognizes this from
    stdout, never a raised error)."""
    harness.backend.deployments = {
        (_NAMESPACE, _DEPLOYMENT): {
            "metadata": {"name": _DEPLOYMENT, "namespace": _NAMESPACE},
            "spec": {"replicas": 1},
            "status": {
                "readyReplicas": 1 if ready else 0,
                "availableReplicas": 1 if ready else 0,
                "updatedReplicas": 1 if ready else 0,
            },
        }
    }


# ---------------------------------------------------------------------------
# Declared-contract sanity (mirrors test_infra_steps.py's/test_k3s_steps.py's
# own such test) -- plane/thin/gateable/undoable must match declared_verbs.py.
# ---------------------------------------------------------------------------


def test_declares_the_dr_0022_contract_for_both(tmp_path):
    cases = [
        (KubeApplyFile(config_dir=tmp_path), "kube.apply_file", False, True),
        (KubeAwaitRollout(), "kube.await_rollout", True, False),
    ]
    for step, verb, gateable, undoable in cases:
        assert step.verb == verb
        assert step.provider_name == "kubectl"
        assert step.plane == "provider"
        assert step.thin is True
        assert step.gateable is gateable
        assert step.undoable is undoable
        assert step.idempotent is True  # Step's own default; neither pins non-idempotent.


# ---------------------------------------------------------------------------
# kube.apply_file -- applies the right manifest; undo deletes it, absent-tolerant.
# ---------------------------------------------------------------------------


def test_apply_file_command_reads_the_manifest_path_and_is_deterministic(tmp_path):
    """NOT a purity assertion -- `command()` deliberately does file IO here (see
    `kube.py`'s module docstring). What is asserted is that repeated calls with the
    same params produce the identical command, which is what `ProviderStep.undo`
    re-deriving the inverse from `command(params)` depends on. Renamed from
    "...and_is_pure", which read as a purity proof it never was (gate finding m-8)."""
    manifest_path = tmp_path / "traefik-kind.yaml"
    manifest_path.write_text(_TRAEFIK_MANIFEST)
    step = KubeApplyFile(config_dir=tmp_path)
    params = ApplyFileParams(kubeconfig=FAKE_KUBECONFIG, manifest_path=str(manifest_path))

    first = step.command(params)
    second = step.command(params)

    assert first == second
    assert isinstance(first, KubeApplyManifest)
    assert first.kubeconfig == FAKE_KUBECONFIG
    assert first.manifest_yaml == _TRAEFIK_MANIFEST


def test_apply_file_resolves_manifest_path_against_config_dir_never_cwd(tmp_path):
    """THE M-2 regression test. `manifest_path` is the shipped repo-relative literal
    both `provision-{kind,orbstack}.yml` bind verbatim; the file exists ONLY under the
    injected `config_dir`, and its text differs from the real repo file at the same
    relative path. So a cwd-resolving implementation reads the repo's own
    traefik-kind.yaml (pytest runs from the repo root, so it would silently succeed
    with the WRONG content) and this assertion fails. The leading `config/` segment is
    stripped exactly once -- core/paths.py's one-home convention.

    Why it matters: both shipped steps carry `on_failure: continue`, so a wrong
    resolution does not fail the workflow -- it reports provisioning SUCCESS with no
    ingress controller installed. v1 was cwd-independent here."""
    shipped_literal = "config/manifest-templates/infrastructure/traefik-kind.yaml"
    manifest = tmp_path / "manifest-templates" / "infrastructure" / "traefik-kind.yaml"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(_TRAEFIK_MANIFEST)
    step = KubeApplyFile(config_dir=tmp_path)

    command = step.command(ApplyFileParams(kubeconfig=FAKE_KUBECONFIG, manifest_path=shipped_literal))

    assert command.manifest_yaml == _TRAEFIK_MANIFEST


def test_apply_file_resolution_uses_the_same_join_as_deployment_profiles(tmp_path):
    """Both consumers of a config-relative path literal must agree, or one of them is
    cwd-dependent again. Asserts `kube.apply_file`'s resolution equals a direct call to
    `core/paths.py`'s `resolve_under_config_dir` -- the shared home (`app/services/
    profiles.py`'s `_resolve_manifests_dir` delegates to the same function)."""
    resolved = resolve_under_config_dir(tmp_path, "config/manifest-templates/infrastructure/traefik-kind.yaml")
    resolved.parent.mkdir(parents=True)
    resolved.write_text(_TRAEFIK_MANIFEST)
    step = KubeApplyFile(config_dir=tmp_path)

    command = step.command(
        ApplyFileParams(
            kubeconfig=FAKE_KUBECONFIG,
            manifest_path="config/manifest-templates/infrastructure/traefik-kind.yaml",
        )
    )

    assert command.manifest_yaml == _TRAEFIK_MANIFEST
    assert resolved == tmp_path / "manifest-templates" / "infrastructure" / "traefik-kind.yaml"


def test_apply_file_command_raises_loudly_when_manifest_path_is_missing(tmp_path):
    step = KubeApplyFile(config_dir=tmp_path)
    params = ApplyFileParams(kubeconfig=FAKE_KUBECONFIG, manifest_path=str(tmp_path / "does-not-exist.yaml"))

    with pytest.raises(PermanentError):
        step.command(params)


async def test_apply_file_execute_applies_the_manifest_to_the_backend(tmp_path):
    manifest_path = tmp_path / "traefik-kind.yaml"
    manifest_path.write_text(_TRAEFIK_MANIFEST)
    harness = KubectlHarness()
    step = KubeApplyFile(config_dir=tmp_path)
    ctx = _ctx({"kubectl": harness.provider()})
    params = ApplyFileParams(kubeconfig=FAKE_KUBECONFIG, manifest_path=str(manifest_path))

    output = await step.execute(params, ctx)

    assert isinstance(output, EmptyOutput)
    assert "Deployment/traefik/traefik" in harness.backend.present_manifest_keys()


async def test_apply_file_undo_deletes_the_manifest_and_is_absent_tolerant(tmp_path):
    manifest_path = tmp_path / "traefik-kind.yaml"
    manifest_path.write_text(_TRAEFIK_MANIFEST)
    harness = KubectlHarness()
    step = KubeApplyFile(config_dir=tmp_path)
    ctx = _ctx({"kubectl": harness.provider()})
    params = ApplyFileParams(kubeconfig=FAKE_KUBECONFIG, manifest_path=str(manifest_path))

    output = await step.execute(params, ctx)
    assert "Deployment/traefik/traefik" in harness.backend.present_manifest_keys()

    await step.undo(params, output, {}, ctx)
    assert "Deployment/traefik/traefik" not in harness.backend.present_manifest_keys()

    # A second undo (e.g. a retried compensation scope) is a no-op success --
    # KubeDeleteManifest(ignore_not_found=True), per Seam C §5.5's undo laws.
    await step.undo(params, output, {}, ctx)
    assert "Deployment/traefik/traefik" not in harness.backend.present_manifest_keys()


async def test_apply_file_unreachable_propagates_as_itself(tmp_path):
    manifest_path = tmp_path / "traefik-kind.yaml"
    manifest_path.write_text(_TRAEFIK_MANIFEST)
    harness = KubectlHarness()
    step = KubeApplyFile(config_dir=tmp_path)
    ctx = _ctx({"kubectl": harness.provider(Fault.UNREACHABLE)})
    # A readable manifest so `command()` doesn't fail first -- we only care that the
    # PROVIDER's error propagates. Previously this pointed at a repo-relative path,
    # which made the test pass only when pytest ran from the repo root (gate finding m-7).
    params = ApplyFileParams(kubeconfig=FAKE_KUBECONFIG, manifest_path=str(manifest_path))

    with pytest.raises(InfrastructureUnreachableError):
        await step.execute(params, ctx)


# ---------------------------------------------------------------------------
# kube.await_rollout -- exactly one probe per poll, non-fatal NotReady, and
# InfrastructureUnreachableError propagates as itself (never NotReady).
# ---------------------------------------------------------------------------


def test_await_rollout_command_is_pure_and_maps_all_fields():
    step = KubeAwaitRollout()
    params = ProbeRolloutParams(kubeconfig=FAKE_KUBECONFIG, deployment=_DEPLOYMENT, namespace=_NAMESPACE)

    first = step.command(params)
    second = step.command(params)

    assert first == second
    assert isinstance(first, KubeProbeRollout)
    assert first.kubeconfig == FAKE_KUBECONFIG
    assert first.deployment == _DEPLOYMENT
    assert first.namespace == _NAMESPACE


async def test_await_rollout_execute_is_a_noop_never_touches_providers():
    class _ExplodingProviders(dict):
        def __getitem__(self, key):
            raise AssertionError(f"execute() must never look up a provider, got {key!r}")

    step = KubeAwaitRollout()
    ctx = _ctx(_ExplodingProviders())
    params = ProbeRolloutParams(kubeconfig=FAKE_KUBECONFIG, deployment=_DEPLOYMENT, namespace=_NAMESPACE)

    output = await step.execute(params, ctx)

    assert isinstance(output, EmptyOutput)


async def test_await_rollout_poll_ready_issues_exactly_one_probe_and_never_sleeps():
    harness = KubectlHarness()
    _seed_deployment(harness, ready=True)
    counting = _CountingProvider(harness.provider())
    step = KubeAwaitRollout()
    ctx = _ctx({"kubectl": counting})
    params = ProbeRolloutParams(kubeconfig=FAKE_KUBECONFIG, deployment=_DEPLOYMENT, namespace=_NAMESPACE)

    result = await step.poll_ready(params, EmptyOutput(), ctx)

    assert counting.execute_calls == 1
    assert isinstance(result, Ready)
    assert result.outputs == EmptyOutput()


async def test_await_rollout_not_ready_for_incomplete_rollout_never_raises():
    """The CRITICAL non-fatal path (crown jewel #10, fault table row 26): a
    Traefik rollout that is merely SLOW must surface as NotReady, letting the
    engine's gate time out into the workflow's own `on_failure: continue` --
    never a raised error that would bypass it."""
    harness = KubectlHarness()
    _seed_deployment(harness, ready=False)
    step = KubeAwaitRollout()
    ctx = _ctx({"kubectl": harness.provider()})
    params = ProbeRolloutParams(kubeconfig=FAKE_KUBECONFIG, deployment=_DEPLOYMENT, namespace=_NAMESPACE)

    result = await step.poll_ready(params, EmptyOutput(), ctx)

    assert isinstance(result, NotReady)


async def test_await_rollout_unreachable_propagates_as_itself_never_notready():
    """InfrastructureUnreachableError means "cannot determine state" -- it must
    never be converted into NotReady/absence (CLAUDE.md's error-taxonomy hard
    rule), and never silently swallowed."""
    harness = KubectlHarness()
    _seed_deployment(harness, ready=False)
    step = KubeAwaitRollout()
    ctx = _ctx({"kubectl": harness.provider(Fault.UNREACHABLE)})
    params = ProbeRolloutParams(kubeconfig=FAKE_KUBECONFIG, deployment=_DEPLOYMENT, namespace=_NAMESPACE)

    with pytest.raises(InfrastructureUnreachableError):
        await step.poll_ready(params, EmptyOutput(), ctx)


async def test_await_rollout_deployment_not_found_raises_not_notready():
    """A deployment that doesn't exist yet (e.g. `kube.apply_file` hasn't
    landed it) is a genuine "cannot determine rollout state" symptom for
    `kubectl rollout status`, not a still-progressing rollout -- it must raise,
    not silently report NotReady."""
    harness = KubectlHarness()
    harness.backend.deployments = {}
    step = KubeAwaitRollout()
    ctx = _ctx({"kubectl": harness.provider()})
    params = ProbeRolloutParams(kubeconfig=FAKE_KUBECONFIG, deployment=_DEPLOYMENT, namespace=_NAMESPACE)

    with pytest.raises(PermanentError):
        await step.poll_ready(params, EmptyOutput(), ctx)
