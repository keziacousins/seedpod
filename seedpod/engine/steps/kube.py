"""engine/steps/kube.py — the Traefik infra-shim slice of the ``kube.*`` verb
family (DR-0022's re-normalized vocabulary, Round 8a "kube-shim" component):
``kube.apply_file``, ``kube.await_rollout``. Fixed ``provider_name = "kubectl"``
``ProviderStep`` bindings (never late-bound — DR-0022's own table: kubectl is
one namespace, one provider) over the already-built, already-conformance-tested
``KubectlProvider`` (``seedpod/providers/kubectl.py``). No provider logic is
reimplemented here; no retry/sleep/poll loop is added (Seam C taste call 2) —
``kube.await_rollout``'s gate loop lives entirely in ``engine/engine.py``.

Only these two verbs land here. ``kube.cluster_info``/``kube.apply_docs``/
``kube.rollout_undo``/``kube.delete_daemonset``/``kube.wipe_namespace`` are
distinct, later components of this same round (Round 8a's "kube-shim" scope
covers exactly the two verbs ``provision-{kind,orbstack}.yml`` need for their
Traefik parity shim; this module never registers or imports the rest).

**``kube.apply_file`` is the infra-shim variant, ``undoable=True`` — NOT
``kube.apply_docs``.** Both verbs wrap the identical Seam C ``KubeApplyManifest``
command, but DR-0022 ruling 3 (D1's fix) makes them opposite on ``undoable``:
``kube.apply_docs`` (deploy waves' verb, not built by this module) is
``undoable=False`` so a failed *application* deploy can never delete the
user's own manifests; ``kube.apply_file`` applies a STATIC, seedpod-owned
template (the copied ``traefik-{kind,orbstack}.yaml`` under
``config/manifest-templates/infrastructure/``), so its literal inverse —
``KubeDeleteManifest(ignore_not_found=True)``, absent-tolerant and idempotent
per Seam C §5.5's undo laws — is exactly the sanctioned, non-data-loss undo
``providers/compensation.py``'s own ``KubeApplyManifest`` arm names. See that
module's docstring and ``tests/engine/declared_verbs.py``'s
``ApplyManifestParams``/``"kube.apply_file"`` fixture row for the same point
made twice, independently.

**``manifest_yaml`` is read by THIS module, never bound through YAML — and
resolved against the INJECTED ``config_dir``, never the process cwd.**
``ApplyFileParams.manifest_path`` is a literal workflow constant (both
shipped ``provision-{kind,orbstack}.yml`` bind it as a bare string, never a
``{from: ...}`` ref — there is no upstream step that produces a manifest body
for this single-file shim). ``command()`` reads that path's text itself, via
``core/paths.py``'s ``resolve_under_config_dir(self._config_dir, ...)`` — the
same one-home join ``app/services/profiles.py`` uses for a profile's
``manifests_dir``. ``config_dir`` is constructor-injected at registry build
time (``app/factory.py``'s ``_build_step_registry``, from the env-overridable
``AppConfig.config_dir``), which is what makes this verb cwd-independent.

An earlier revision of this module read ``Path(manifest_path)`` directly,
i.e. against the process's working directory. The Round-8a gate (finding M-2)
caught that as a silent regression of behaviour v1 already got right: v1
resolved both Traefik shims cwd-independently
(``reference-code/seedpod/seedpod/core/paths.py``'s ``get_config_dir()`` for
kind; ``Path(__file__).parent.parent.parent / "config"`` for orbstack), and
because BOTH shipped steps carry ``on_failure: continue``, a wrong cwd would
have made ``provision-{kind,orbstack}`` report SUCCESS with no ingress
controller installed at all — CLAUDE.md's named #1 failure mode.

A missing/unreadable file stays a workflow-configuration defect, raised loudly
as ``PermanentError`` (mirrors ``infra.py``/``k3s.py``'s own ``_expect``/
``_cidrs_for`` convention) rather than surfacing as a raw ``OSError`` two
layers away from its cause. Note the read sits inside ``command()``, which
``provider_step.py`` documents as a "Pure param -> command mapping"; it is
kept there deliberately because ``ProviderStep.undo`` re-derives the inverse
from the same ``command(params)``, and the file is a STATIC, seedpod-owned
template shipped with the app (never user data, never rewritten at runtime),
so the read is deterministic for the life of a run.

**``kube.await_rollout`` is the P3 exemplar and the CRITICAL non-fatal gate
(crown jewel #10).** ``execute()`` is a true no-op (DR-0022 Erratum E4b:
"returns a provisional Output and invokes no provider" — never touches
``ctx.services.providers``); ``poll_ready`` issues exactly ONE
``KubeProbeRollout`` per call. Seam-c-provider.md's fault table row 26
("rollout slow after apply — not an error") and both shipped workflows'
``on_failure: continue`` on this step are the whole point: a Traefik rollout
that is merely SLOW must surface as ``NotReady`` (letting the gate time out
into the workflow's own ``on_failure`` policy), never as a raised error that
would bypass it. This binding does nothing special to achieve that — the
already-built ``KubectlProvider._probe_rollout`` already returns
``RolloutState(complete=False)`` as a typed Result for "still progressing"
(row 31) and raises only for a genuine connectivity/auth/not-found symptom;
this module's only job is to not get in the way: a ``RolloutState`` Result
becomes ``Ready``/``NotReady``, and everything else — in particular
``InfrastructureUnreachableError`` — propagates unmodified, never caught,
never converted to absence (CLAUDE.md's error-taxonomy hard rule)."""

from __future__ import annotations

from pathlib import Path
from typing import ClassVar

from pydantic import BaseModel, SecretStr

from seedpod.core.errors import ErrorCode, PermanentError
from seedpod.core.paths import resolve_under_config_dir
from seedpod.engine.provider_step import ProviderStep
from seedpod.engine.step import EmptyOutput, NotReady, Ready, StepContext
from seedpod.providers.contract import (
    KubeApplyManifest,
    KubectlOutput,
    KubeGetClusterInfo,
    KubeProbeRollout,
    KubeRolloutUndo,
    KubeRun,
    Result,
    RolloutState,
    RolloutUndoResult,
)

__all__ = [
    "ApplyFileParams",
    "KubeApplyFile",
    "ProbeRolloutParams",
    "KubeAwaitRollout",
    "KubeconfigParams",
    "KubeClusterInfo",
    "RolloutUndoParams",
    "KubeRolloutUndoStep",
    "DeleteDaemonsetParams",
    "KubeDeleteDaemonset",
    "WipeNamespaceParams",
    "KubeWipeNamespace",
]


def _read_manifest(config_dir: Path, manifest_path: str, *, verb: str) -> str:
    """``manifest_path`` names a template file shipped with the app (both
    shipped workflows' literal: ``config/manifest-templates/infrastructure/
    traefik-{kind,orbstack}.yaml``), resolved against the INJECTED
    ``config_dir`` via ``core/paths.py``'s one-home join — never against the
    process cwd (see module docstring; Round-8a gate finding M-2)."""
    resolved = resolve_under_config_dir(config_dir, manifest_path)
    try:
        return resolved.read_text()
    except OSError as exc:
        raise PermanentError(
            f"{verb}: could not read manifest_path {manifest_path!r} (resolved to {resolved}): {exc}",
            code=ErrorCode.INVALID_INPUT,
            provider="engine",
            command=verb,
        ) from exc


def _require_kubeconfig(kubeconfig: SecretStr | None, *, verb: str) -> str:
    """Both destroy-path verbs take an Optional kubeconfig and short-circuit on None
    in ``execute()``; if one still reaches ``command()``, that is a wiring bug and is
    raised loudly rather than sending an empty kubeconfig to kubectl."""
    if kubeconfig is None:
        raise PermanentError(
            f"{verb}: command() reached with no kubeconfig (execute() should have short-circuited)",
            code=ErrorCode.INVALID_INPUT,
            provider="engine",
            command=verb,
        )
    return kubeconfig.get_secret_value()


async def _drain(provider: object, command: KubeRun) -> object | None:
    """Run one command to completion, returning its Result value (Progress events are
    ignored -- these commands emit none)."""
    value: object | None = None
    async for ev in provider.execute(command):  # type: ignore[attr-defined]
        if isinstance(ev, Result):
            value = ev.value
    return value


async def _list_names(
    ctx: StepContext, provider_name: str, kubeconfig: SecretStr, *, kind: str, namespace: str, verb: str
) -> frozenset[str]:
    """``kubectl get <kind> -n <ns> -o jsonpath={.items[*].metadata.name}`` -> the set of
    names, as v1's own service sweep parsed it.

    Deliberately a LIST, never ``get <kind> NAME``: a single-object read reports
    absence as a non-zero exit, which the provider (correctly) classifies as an
    error -- so using it as an absence probe would conflate "object is gone" with
    "cannot reach the cluster", the one conflation CLAUDE.md's error taxonomy
    forbids. A list returns 0 with empty stdout for an empty namespace, so absence is
    a positive fact and a genuine failure still raises."""
    output = await _drain(
        ctx.services.providers[provider_name],
        KubeRun(
            kubeconfig=kubeconfig.get_secret_value(),
            args=("get", kind, "-n", namespace, "-o", "jsonpath={.items[*].metadata.name}"),
            timeout_s=_PROBE_TIMEOUT_S,
        ),
    )
    result = _expect(output, KubectlOutput, verb=verb)
    assert isinstance(result, KubectlOutput)  # narrowed by _expect
    stdout = result.stdout
    text = stdout.decode(errors="replace") if isinstance(stdout, bytes) else stdout
    return frozenset(text.split())


def _expect(value: object, expected: type, *, verb: str) -> object:
    """Loud Result-shape guard (mirrors ``infra.py``/``k3s.py``'s own
    ``_expect``: "raised loudly ... rather than silently propagating ... the
    wrong shape into a Seam C command")."""
    if not isinstance(value, expected):
        raise PermanentError(
            f"{verb}: expected Result value of type {expected.__name__}, got {type(value).__name__}",
            code=ErrorCode.INVALID_INPUT,
            provider="engine",
            command=verb,
        )
    return value


# ---------------------------------------------------------------------------
# kube.apply_file -- the Traefik infra-shim apply, undoable=True (ruling 3).
# ---------------------------------------------------------------------------


class ApplyFileParams(BaseModel):
    """``manifest_path`` is a literal workflow constant, never a Ref -- see
    module docstring and ``tests/engine/declared_verbs.py``'s own
    ``ApplyManifestParams`` docstring for why this Params shape is distinct
    from ``kube.apply_docs``'s ``docs: list[ManifestDoc]`` (deploy-waves-only,
    not built by this module)."""

    kubeconfig: SecretStr
    manifest_path: str


class KubeApplyFile(ProviderStep[ApplyFileParams, EmptyOutput]):
    verb = "kube.apply_file"
    provider_name: ClassVar[str] = "kubectl"
    Params = ApplyFileParams
    Output = EmptyOutput
    thin = True
    # undoable=True (ProviderStep's own default) -- DR-0022 ruling 3: this is the
    # infra-shim verb (Traefik parity manifests). Its undo,
    # KubeDeleteManifest(ignore_not_found=True), is the sanctioned, absent-tolerant
    # inverse `providers/compensation.py`'s own KubeApplyManifest arm names --
    # legitimate here precisely because the manifest is seedpod-owned, not a
    # user's application (that is `kube.apply_docs`, undoable=False, NOT this verb).

    def __init__(self, *, config_dir: Path) -> None:
        """``config_dir`` is REQUIRED and constructor-injected at registry build
        time (``engine/registry.py``'s own docstring: "Construction ... happens
        once, at composition-root build time"). No default: a defaulted
        ``Path("config")`` would silently reintroduce the cwd dependence M-2
        found, and this verb's ``on_failure: continue`` means that failure is
        invisible in the workflow's outcome."""
        self._config_dir = config_dir

    def command(self, params: ApplyFileParams) -> KubeApplyManifest:
        manifest_yaml = _read_manifest(self._config_dir, params.manifest_path, verb=self.verb)
        return KubeApplyManifest(kubeconfig=params.kubeconfig.get_secret_value(), manifest_yaml=manifest_yaml)

    def output_from(self, value: object) -> EmptyOutput:
        return EmptyOutput()


# ---------------------------------------------------------------------------
# kube.await_rollout -- gateable, execute() a true no-op (DR-0022 P3/E4b).
# The CRITICAL non-fatal gate (crown jewel #10): NotReady, never raise, for a
# merely-slow rollout -- see module docstring.
# ---------------------------------------------------------------------------


class ProbeRolloutParams(BaseModel):
    kubeconfig: SecretStr
    deployment: str
    namespace: str = "default"


class KubeAwaitRollout(ProviderStep[ProbeRolloutParams, EmptyOutput]):
    verb = "kube.await_rollout"
    provider_name: ClassVar[str] = "kubectl"
    Params = ProbeRolloutParams
    Output = EmptyOutput
    gateable = True
    undoable = False  # ProviderStep hard-defaults True; a pure gate has nothing to
    # compensate (mirrors k3s.await_ssh/k3s.await_api/infra.await_instance --
    # seam-c-provider.md §5.5: "Probe*/Get*/List*/Watch* -> none").
    thin = True

    def command(self, params: ProbeRolloutParams) -> KubeProbeRollout:
        return KubeProbeRollout(
            kubeconfig=params.kubeconfig.get_secret_value(), deployment=params.deployment, namespace=params.namespace
        )

    async def execute(self, params: ProbeRolloutParams, ctx: StepContext) -> EmptyOutput:
        """True no-op (DR-0022 Erratum E4b) -- never touches
        ``ctx.services.providers``. Deliberately does NOT call
        ``super().execute()`` (``ProviderStep``'s inherited template, which
        WOULD emit ``self.command(params)`` immediately -- exactly what an
        ``await_``-named verb must not do)."""
        return EmptyOutput()

    async def poll_ready(
        self, params: ProbeRolloutParams, provisional: EmptyOutput, ctx: StepContext
    ) -> Ready[EmptyOutput] | NotReady:
        provider = ctx.services.providers[self.provider_name]
        value: object | None = None
        async for ev in provider.execute(self.command(params)):
            if isinstance(ev, Result):
                value = ev.value
        state = _expect(value, RolloutState, verb=self.verb)
        if state.complete:
            return Ready(outputs=EmptyOutput())
        # Non-fatal by construction (crown jewel #10, fault table row 26): a
        # still-progressing rollout is NotReady, never raised -- the engine's
        # gate times out into this step's own `on_failure: continue` (both
        # shipped provision-{kind,orbstack}.yml), it does not abort the run.
        # PermanentError/InfrastructureUnreachableError from the provider.execute()
        # call above are never caught here -- they propagate as themselves,
        # unconverted (CLAUDE.md's error-taxonomy hard rule).
        return NotReady(detail=state.message or "rollout not complete")


# ---------------------------------------------------------------------------
# kube.cluster_info -- deploy-waves' connectivity pre-check (v1's, now retried).
# ---------------------------------------------------------------------------


class KubeconfigParams(BaseModel):
    kubeconfig: SecretStr


class KubeClusterInfo(ProviderStep[KubeconfigParams, EmptyOutput]):
    """``deploy-waves.yml``'s ``preflight`` step: v1's connectivity pre-check before
    a wave apply, now with ``retry: kubectl_default`` behind it (H6 -- v1 ran it
    once and failed the whole deploy on a single blip).

    The Output is deliberately ``EmptyOutput``: what matters is whether the command
    SUCCEEDED, not the cluster-info text it prints. Nothing downstream binds it, and
    persisting a kube-apiserver banner into the step row would be noise. A genuine
    connectivity failure surfaces as whatever ``KubectlProvider`` classified it as
    (``InfrastructureUnreachableError`` for "cannot determine state"), which is the
    entire point of the check."""

    verb = "kube.cluster_info"
    provider_name: ClassVar[str] = "kubectl"
    Params = KubeconfigParams
    Output = EmptyOutput
    thin = True
    undoable = False  # a read has no inverse

    def command(self, params: KubeconfigParams) -> KubeGetClusterInfo:
        return KubeGetClusterInfo(kubeconfig=params.kubeconfig.get_secret_value())

    def output_from(self, value: object) -> EmptyOutput:
        return EmptyOutput()


# ---------------------------------------------------------------------------
# kube.rollout_undo -- crown jewel #13's >=1-success rule lives HERE.
# ---------------------------------------------------------------------------


class RolloutUndoParams(BaseModel):
    kubeconfig: SecretStr
    namespace: str = "default"


class KubeRolloutUndoStep(ProviderStep[RolloutUndoParams, EmptyOutput]):
    """``deploy-rollback.yml``'s one actuating step.

    **Crown jewel #13's partial-success rule is already enforced by the PROVIDER --
    this step deliberately does not re-implement it.** ``KubectlProvider._rollout_undo``
    (``providers/kubectl.py``) undoes every deployment in the namespace and raises
    ``PermanentError`` itself when ``succeeded == 0``, carrying the aggregated
    ``errors``; it yields a ``RolloutUndoResult`` tally only on success. (Note
    ``RolloutUndoResult``'s own docstring says "the caller raises" -- that wording
    predates the provider taking the job, and the provider is where the rule actually
    lives. A second copy of a crown-jewel rule in this step would be one more place
    for it to drift.)

    Two subtleties that make the provider's placement the right one, worth stating so
    nobody "fixes" it back:
    - The rule is NOT "succeeded == 0 ⇒ failure". v1 kept an explicit "no deployments
      to undo ⇒ trivial success" case (``reference-code/.../kubernetes.py:965-966``),
      preserved verbatim as an early ``yield`` BEFORE the raise -- so an empty
      namespace returns ``succeeded=0, failed=0`` and never reaches it. An empty
      namespace is not a failure to undo anything in.
    - A genuine connectivity symptom mid-loop raises immediately rather than being
      folded into the ``failed`` tally (crown jewel #1 extended to that loop), so a
      nonzero ``failed`` really does mean per-deployment rejections, never "cannot
      determine state".

    What is left for this step is the shape guard: a Result that is not a
    ``RolloutUndoResult`` is a wiring bug, raised loudly rather than silently
    swallowed as success."""

    verb = "kube.rollout_undo"
    provider_name: ClassVar[str] = "kubectl"
    Params = RolloutUndoParams
    Output = EmptyOutput
    thin = True
    undoable = False  # undoing an undo is not a thing this system may decide to do

    def command(self, params: RolloutUndoParams) -> KubeRolloutUndo:
        return KubeRolloutUndo(kubeconfig=params.kubeconfig.get_secret_value(), namespace=params.namespace)

    def output_from(self, value: object) -> EmptyOutput:
        _expect(value, RolloutUndoResult, verb=self.verb)
        return EmptyOutput()


# ---------------------------------------------------------------------------
# kube.delete_daemonset -- gotcha 10, and DR-0022 ruling 4's gate conversion.
# ---------------------------------------------------------------------------


# v1 ran `kubectl delete daemonset tailscale -n default --grace-period=30 --wait=true
# --timeout=45s`. DR-0022 D2 strips `--wait`/`--timeout`: no Seam C command waits, all
# waiting is an engine gate (the workflow carries the same budget as
# `gate: {timeout_seconds: 45, interval_seconds: 5, settle_seconds: 3}`).
_DELETE_TIMEOUT_S = 30.0
_PROBE_TIMEOUT_S = 15.0


class DeleteDaemonsetParams(BaseModel):
    """``kubeconfig`` is Optional because both destroy workflows bind it from
    ``cluster.load_kubeconfig_optional`` -- a cluster whose provisioning died before
    ``cluster.store_kubeconfig`` has no kubeconfig but still has infrastructure to
    tear down. ``wait``/``wait_timeout_seconds``/``settle_seconds`` are deliberately
    ABSENT (DR-0022 ruling 4): they are gate data now, not Params."""

    kubeconfig: SecretStr | None = None
    name: str
    namespace: str = "default"
    grace_period_seconds: int = 30


class KubeDeleteDaemonset(ProviderStep[DeleteDaemonsetParams, EmptyOutput]):
    """**Gotcha 10: the 48-hour lingering Tailscale node.** Both destroy workflows run
    this BEFORE any infrastructure teardown so the Tailscale DaemonSet can send its
    disconnect to the control plane; without it a destroyed cluster's node lingers in
    the tailnet for ~48h. v1's own budget is preserved as gate data
    (``timeout_seconds: 45``, and ``settle_seconds: 3`` for v1's post-delete
    ``asyncio.sleep(3)`` grace, which ran ONLY on a successful delete).

    **One of DR-0022 P3's two named actuate-and-gate verbs** (with
    ``infra.destroy_instance``): it keeps the actuator name because ``execute()``
    really actuates -- it issues the delete -- and ``poll_ready`` then gates on the
    DaemonSet actually being gone. That is why this is ``gateable=True`` yet NOT named
    ``kube.await_*``.

    **``--ignore-not-found``**: deleting an already-absent DaemonSet is success, not a
    failure. The destroy path retries, and an absent DaemonSet is exactly the state
    this step exists to reach.

    **The absence probe never uses an error as data.** It lists the namespace's
    DaemonSets by name (``-o jsonpath``) and checks membership, rather than
    ``get daemonset NAME`` and treating NotFound-as-nonzero-exit as "gone". A
    connectivity failure and an absent object must never be conflated (CLAUDE.md's
    error-taxonomy rule) -- and with the list form, a genuine failure raises from the
    provider while absence is a plain, positive fact.

    ``thin=False``: two different Seam C commands (delete, then probe)."""

    verb = "kube.delete_daemonset"
    provider_name: ClassVar[str] = "kubectl"
    Params = DeleteDaemonsetParams
    Output = EmptyOutput
    gateable = True
    undoable = False  # deleting a DaemonSet as part of teardown has no inverse
    thin = False

    def command(self, params: DeleteDaemonsetParams) -> KubeRun:
        """The DELETE. ``ProviderStep.execute``'s template issues exactly this."""
        return KubeRun(
            kubeconfig=_require_kubeconfig(params.kubeconfig, verb=self.verb),
            args=(
                "delete", "daemonset", params.name,
                "-n", params.namespace,
                f"--grace-period={params.grace_period_seconds}",
                "--ignore-not-found=true",
            ),
            timeout_s=_DELETE_TIMEOUT_S,
        )

    def output_from(self, value: object) -> EmptyOutput:
        return EmptyOutput()

    async def execute(self, params: DeleteDaemonsetParams, ctx: StepContext) -> EmptyOutput:
        """No kubeconfig => nothing to delete. Both destroy workflows also carry
        ``on_failure: continue`` here, but returning cleanly is more honest than
        raising and relying on that: a half-provisioned cluster genuinely has no
        DaemonSet to remove, which is not a failure of this step."""
        if params.kubeconfig is None:
            return EmptyOutput()
        return await super().execute(params, ctx)

    async def poll_ready(
        self, params: DeleteDaemonsetParams, provisional: EmptyOutput, ctx: StepContext
    ) -> Ready[EmptyOutput] | NotReady:
        if params.kubeconfig is None:
            return Ready(outputs=EmptyOutput())
        names = await _list_names(
            ctx, self.provider_name, params.kubeconfig, kind="daemonsets", namespace=params.namespace, verb=self.verb
        )
        return Ready(outputs=EmptyOutput()) if params.name not in names else NotReady()


# ---------------------------------------------------------------------------
# kube.wipe_namespace -- destroy-shared.yml's pre-teardown sweep.
# ---------------------------------------------------------------------------


# v1's own list, verbatim (reference-code .../destruction_job.py's
# `_cleanup_deployed_resources`): every namespaced type a deployment can leave behind.
_WIPE_TYPES = (
    "deployments,configmaps,secrets,daemonsets,statefulsets,jobs,cronjobs,"
    "ingresses,persistentvolumeclaims,replicasets,pods"
)
_WIPE_TIMEOUT_S = 70.0  # v1's own outer wait around its 60s kubectl --timeout
_SERVICE_DELETE_TIMEOUT_S = 35.0

# The built-in service every cluster has. v1 deleted services ONE BY ONE BY NAME
# specifically to skip it -- `delete services --all` would remove the cluster's own
# kubernetes service and break the cluster this wipe is meant to leave standing.
_BUILTIN_SERVICE = "kubernetes"


class WipeNamespaceParams(BaseModel):
    kubeconfig: SecretStr | None = None
    namespace: str = "default"


class KubeWipeNamespace(ProviderStep[WipeNamespaceParams, EmptyOutput]):
    """``destroy-shared.yml``'s wipe: for kind/orbstack the underlying cluster
    PERSISTS after "destruction", so deployed resources must be removed explicitly or
    they leak into the next cluster that reuses it.

    Salvaged from v1's ``_cleanup_deployed_resources`` in three commands, and the
    THREE-command shape is the whole point:

    1. ``delete <11 types> --all -n <ns>`` -- v1's type list verbatim.
    2. ``get services -o jsonpath={.items[*].metadata.name}``.
    3. ``delete services <names except 'kubernetes'> -n <ns>``.

    Services are swept separately **because ``--all`` would delete the built-in
    ``kubernetes`` service** and break the very cluster this wipe leaves standing.
    v1's own comment says so; folding services into step 1 is the obvious
    "simplification" that silently breaks a shared cluster.

    ``thin=False``: three commands. ``command()`` is still implemented (it returns the
    first, the bulk delete) so the ``ProviderStep`` contract stays honest, but
    ``execute()`` is overridden to issue all three -- the sanctioned composite shape
    (DR-0022: "composites like kube.wipe_namespace ... are ProviderSteps issuing N
    commands")."""

    verb = "kube.wipe_namespace"
    provider_name: ClassVar[str] = "kubectl"
    Params = WipeNamespaceParams
    Output = EmptyOutput
    undoable = False  # a wipe has no inverse
    thin = False

    def command(self, params: WipeNamespaceParams) -> KubeRun:
        return KubeRun(
            kubeconfig=_require_kubeconfig(params.kubeconfig, verb=self.verb),
            args=("delete", _WIPE_TYPES, "--all", "-n", params.namespace, "--timeout=60s"),
            timeout_s=_WIPE_TIMEOUT_S,
        )

    def output_from(self, value: object) -> EmptyOutput:
        return EmptyOutput()

    async def execute(self, params: WipeNamespaceParams, ctx: StepContext) -> EmptyOutput:
        if params.kubeconfig is None:
            # v1: "No kubeconfig found for {cluster}, skipping resource cleanup".
            return EmptyOutput()
        provider = ctx.services.providers[self.provider_name]

        await _drain(provider, self.command(params))

        names = await _list_names(
            ctx, self.provider_name, params.kubeconfig, kind="services", namespace=params.namespace, verb=self.verb
        )
        doomed = [name for name in names if name != _BUILTIN_SERVICE]
        if doomed:
            await _drain(
                provider,
                KubeRun(
                    kubeconfig=params.kubeconfig.get_secret_value(),
                    args=("delete", "services", *doomed, "-n", params.namespace, "--timeout=30s"),
                    timeout_s=_SERVICE_DELETE_TIMEOUT_S,
                ),
            )
        return EmptyOutput()
