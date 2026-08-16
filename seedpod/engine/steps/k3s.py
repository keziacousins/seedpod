"""engine/steps/k3s.py — the ``k3s.*`` verb family (DR-0022's re-normalized
vocabulary, Round 8a "k3s-family" component): ``k3s.await_ssh``,
``k3s.trust_host_keys``, ``k3s.install``, ``k3s.await_api``,
``k3s.fetch_kubeconfig``. Five THIN ``ProviderStep`` bindings, fixed
``provider_name = "ssh-k3s"`` (DR-0022's own table: "one namespace for the
``ssh-k3s`` provider" — never late-bound, unlike ``infra.*``) over the
already-built, already-conformance-tested ``SshK3sProvider``
(``seedpod/providers/ssh_k3s.py``). No provider logic is reimplemented here;
no retry/sleep/poll loop is added (Seam C taste call 2) — the two gate verbs'
loops live entirely in ``engine/engine.py``.

Both ``provision-digitalocean.yml`` and ``provision-tart.yml`` drive this same
plane with identical step ids/verbs/``with:`` shapes from ``k3s.await_ssh``
onward (DR-0022 ruling 6 added the gate symmetrically to both) — one Step
instance per verb, reused unchanged across both workflows, exactly like
``infra.*``'s late-bound family is one instance reused across four providers
(``engine/steps/infra.py``).

**TOFU ordering (crown jewel #2) — this binding does not own it, only
preserves it.** ``k3s.trust_host_keys`` maps 1:1 to ``CaptureHostKeys``
(``command()`` builds exactly one command, no wrapping calls before/after),
so the atomic, ordered (cloud-init wait THEN keyscan) TOFU pair is entirely
the provider's own responsibility (``ssh_k3s.py``'s ``_capture_host_keys``) —
this module's only obligation is to never split it into two commands or
reorder anything around it.

**known_hosts threading (coherence-review.md Conflicts 14 and 9).**
``k3s.trust_host_keys``'s ``Output`` carries ``known_hosts: str`` (from
``CaptureHostKeys``'s ``HostKeys`` Result), consumed verbatim by
``k3s.install``/``k3s.await_api``/``k3s.fetch_kubeconfig``'s ``Params`` — the
already-shipped ``provision-digitalocean.yml``/``provision-tart.yml`` (Round
8a's "domain-steps"/"infra-and-do"/component-1 work) already bind
``known_hosts: {from: trust_host.known_hosts}`` into all three; this module's
job is only to consume the field honestly (never re-derive/re-capture host
keys, never accept a stale/mismatched value silently).

**``k3s.await_ssh``/``k3s.await_api`` are DR-0022 P3's exemplar pair**: both
``gateable=True`` with a genuinely no-op ``execute()`` (Erratum E4b: "returns a
provisional Output and invokes no provider" — neither touches
``ctx.services.providers`` at all), and both ``poll_ready`` issue exactly ONE
probe command per call, no in-step loop, no ``ctx.sleep``.

**No in-step retry loop, anywhere in this family (explicit design point, not
an oversight).** v1's ``SSHBasedK3sInstaller.execute()`` had an internal
``retry_attempts``/``retry_delay`` loop; the ALREADY-BUILT ``ssh_k3s.py``
provider deliberately stripped it (its own module docstring: "the
``retry_attempts``/``retry_delay`` loop **stripped** per the task's explicit
instruction — the engine's ``ssh_default`` ``Schedule`` now owns that retry").
This binding layer must not reintroduce it either — every actuating verb here
(``trust_host_keys``, ``install``, ``fetch_kubeconfig``) is a single
``provider.execute(cmd)`` drain via the inherited ``ProviderStep.execute()``,
one bounded attempt per call, exactly like every other ``ProviderStep`` in
this codebase; the shipped workflow YAML supplies ``retry: ssh_default`` at
the step level for the engine's ``Schedule`` to own.

**SSH identity (DR-0023).** Every command below except ``ProbeSshPort``
(``host``/``port`` only) requires a full Seam C ``SSHTarget`` (``host``,
``user``, ``private_key_path``, ``port``, ``connection_timeout_s``,
``command_timeout_s`` — the last four have dataclass defaults, but
``user``/``private_key_path`` do NOT). DR-0023 settles the mechanism an
earlier revision of this module flagged as an open spec gap: SSH identity is
provider configuration, threaded as typed data through ``cluster.load_spec``
— that step's Output now carries ``ssh_user``/``ssh_private_key_path``
(sourced from each provider's own ``config/providers/<provider>.yml``,
``app/factory.py``'s ``_ssh_identities()``), and every verb below that builds
an ``SSHTarget`` takes both as ``Params``, bound in
``provision-{digitalocean,tart}.yml`` from that same head step exactly like
``provider``/``slug`` already are. ``kind``/``orbstack`` have no SSH plane, so
``cluster.load_spec`` resolves them to ``None``/``None`` there.

That optionality is NOT what keeps an SSH-less provider out of a ``k3s.*``
verb — DR-0023's **Erratum E1** retracts that rationale (the Output type is a
single global ``str | None``, so requiring ``str`` here would reject
DigitalOcean's and tart's own legitimate bindings; and
``provision-{kind,orbstack}.yml`` contain no ``k3s.*`` step in the first
place). The plane matrix is enforced by workflow COMPOSITION. What this
module enforces is DR-0023 point 5, "no fallback identity may survive": no
identity is defaulted anywhere here, and ``_target()`` below raises a loud
``PermanentError`` on a ``None`` rather than constructing an ``SSHTarget``
with a wrong-but-plausible credential.
"""

from __future__ import annotations

from typing import ClassVar

from pydantic import BaseModel, SecretStr

from seedpod.core.acme import AcmeConfig
from seedpod.core.cluster_spec import ClusterSpecification
from seedpod.core.errors import ErrorCode, PermanentError
from seedpod.engine.provider_step import ProviderStep
from seedpod.engine.step import EmptyOutput, NotReady, Ready, StepContext
from seedpod.providers.contract import (
    CaptureHostKeys,
    FetchKubeconfig,
    HostKeys,
    IngressConfig,
    InstallK3s,
    K3sReadiness,
    Kubeconfig,
    ProbeK3s,
    ProbeSshPort,
    Result,
    SshPortState,
    SSHTarget,
)

__all__ = [
    "HostParams",
    "K3sAwaitSsh",
    "TrustHostKeysParams",
    "KnownHostsOutput",
    "K3sTrustHostKeys",
    "InstallK3sParams",
    "K3sInstall",
    "K3sAwaitReadyParams",
    "K3sAwaitApi",
    "FetchKubeconfigParams",
    "KubeconfigOutput",
    "K3sFetchKubeconfig",
]


def _target(host: str, ssh_user: str | None, ssh_private_key_path: str | None, *, verb: str) -> SSHTarget:
    """Pure Params -> SSHTarget mapping (DR-0023): identity arrives as typed
    ``Params`` fields (threaded from ``cluster.load_spec`` via workflow YAML),
    never a module-level constant or a ``ctx``/config lookup -- ``command()``
    stays pure on every caller below.

    ``ssh_user``/``ssh_private_key_path`` are typed ``str | None`` on every
    caller's ``Params`` because ``cluster.load_spec``'s ``Output`` is itself
    ``str | None`` (``None`` for ``kind``/``orbstack``, which have no SSH
    plane) -- V4's Optional-binds-Optional rule requires the same optionality
    on both sides of a binding, so a ``k3s.*`` verb's ``Params`` cannot narrow
    to a required ``str`` without making ``provision-{digitalocean,tart}.yml``'s
    own (legitimate) bindings fail to validate. No k3s.* verb is ever bound
    from a kind/orbstack workflow (those workflows have no ssh-k3s plane at
    all), so this guard is the honest, loud backstop for that
    structurally-unreachable case -- DR-0023 point 5's "no fallback identity"
    holds here too: a ``None`` never silently reaches ``SSHTarget``."""
    if ssh_user is None or ssh_private_key_path is None:
        raise PermanentError(
            f"{verb}: ssh_user/ssh_private_key_path must be set by cluster.load_spec "
            "(None means this cluster's provider has no ssh-k3s plane)",
            code=ErrorCode.INVALID_INPUT,
            provider="engine",
            command=verb,
        )
    return SSHTarget(host=host, user=ssh_user, private_key_path=ssh_private_key_path)


def _expect(value: object, expected: type, *, verb: str) -> object:
    """Loud Result-shape guard (mirrors ``engine/steps/infra.py``'s own
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


def _cidrs_for(spec: ClusterSpecification, *, verb: str) -> tuple[str, str]:
    """Mirrors ``engine/steps/infra.py``'s ``_cidrs_for`` exactly (same
    invariant: ``cluster.load_spec`` always overlays real CIDRs before any
    ``infra.*``/``k3s.*`` verb sees ``spec``) -- duplicated rather than
    imported since ``infra.py``'s copy is module-private (not in its
    ``__all__``), matching this codebase's existing convention of small,
    self-contained per-module pure helpers (``ssh_k3s.py``'s own
    ``_rewrite_server``/``_traefik_hostport_manifest`` are equally
    module-local)."""
    pod_cidr = spec.cluster_config.pod_cidr
    service_cidr = spec.cluster_config.service_cidr
    if pod_cidr is None or service_cidr is None:
        raise PermanentError(
            f"{verb}: spec.cluster_config.pod_cidr/service_cidr must be set by cluster.load_spec",
            code=ErrorCode.INVALID_INPUT,
            provider="engine",
            command=verb,
        )
    return pod_cidr, service_cidr


def _ingress_for(spec: ClusterSpecification, acme: AcmeConfig | None = None) -> IngressConfig:
    """Salvaged verbatim from v1's own translation
    (``reference-code/seedpod/seedpod/providers/_ssh_k3s_installer.py:446-478``,
    ``install_k3s``): ``cluster_config.ingress_strategy`` is a raw
    ``{"type": ..., "traefik": {"enabled": ..., "expose_method": ...}}`` dict
    (``config/deployment-profiles/*.yml``'s own shape) that never reached a
    typed model anywhere upstream (``ClusterConfiguration.ingress_strategy``
    is deliberately ``dict[str, Any] | None`` — see ``core/cluster_spec.py``).
    ``http_port``/``https_port`` are NOT part of ``IngressConfig`` (they are
    hardcoded 80/443 inside ``ssh_k3s.py`` itself — that module's own
    docstring: "the 80/443 defaults are hardcoded here rather than
    configurable, since that YAML shape does not survive into the v2
    contract") so this function never reads them, matching that decision."""
    ingress_strategy = spec.cluster_config.ingress_strategy or {}
    ingress_type = ingress_strategy.get("type", "none")
    if ingress_type != "traefik":
        # No traefik, no certresolver to configure: `acme` is deliberately dropped here
        # rather than carried into a config nothing will read.
        return IngressConfig(ingress_type=ingress_type, enabled=True, expose_method="loadbalancer")
    traefik_cfg = ingress_strategy.get("traefik", {}) or {}
    return IngressConfig(
        ingress_type="traefik",
        enabled=bool(traefik_cfg.get("enabled", True)),
        expose_method=traefik_cfg.get("expose_method", "loadbalancer"),
        acme=acme,
    )


# ---------------------------------------------------------------------------
# k3s.await_ssh -- gateable, execute() a true no-op (DR-0022 P3/E4b).
# ---------------------------------------------------------------------------


class HostParams(BaseModel):
    host: str


class K3sAwaitSsh(ProviderStep[HostParams, EmptyOutput]):
    verb = "k3s.await_ssh"
    provider_name: ClassVar[str] = "ssh-k3s"
    Params = HostParams
    Output = EmptyOutput
    gateable = True
    undoable = False  # ProviderStep hard-defaults True; a pure gate has nothing to
    # compensate (seam-c-provider.md §5.5: Probe*/Get*/List*/Watch* -> none).
    thin = True

    def command(self, params: HostParams) -> ProbeSshPort:
        return ProbeSshPort(host=params.host)

    async def execute(self, params: HostParams, ctx: StepContext) -> EmptyOutput:
        """True no-op (DR-0022 Erratum E4b): never touches
        ``ctx.services.providers``. Deliberately does NOT call
        ``super().execute()`` (``ProviderStep``'s inherited template, which
        WOULD emit ``self.command(params)`` immediately)."""
        return EmptyOutput()

    async def poll_ready(self, params: HostParams, provisional: EmptyOutput, ctx: StepContext) -> Ready[EmptyOutput] | NotReady:
        provider = ctx.services.providers[self.provider_name]
        value: object | None = None
        async for ev in provider.execute(self.command(params)):
            if isinstance(ev, Result):
                value = ev.value
        state = _expect(value, SshPortState, verb=self.verb)
        if state.open:
            return Ready(outputs=EmptyOutput())
        # DR-0033: branch on `open` and ONLY on `open` -- `detail` is diagnostic, never a
        # decision input. It rides into the gate's timeout message so a run that gives up
        # names the error it kept hitting ("[Errno 65] No route to host" for the macOS Local
        # Network denial of backlog #15) instead of just "ssh port not open yet".
        return NotReady(
            detail=f"ssh port not open yet: {state.detail}" if state.detail else "ssh port not open yet"
        )


# ---------------------------------------------------------------------------
# k3s.trust_host_keys -- the TOFU pair, atomic and ordered inside the provider.
# ---------------------------------------------------------------------------


class TrustHostKeysParams(BaseModel):
    """Unlike ``k3s.await_ssh`` (``ProbeSshPort``, host/port only),
    ``CaptureHostKeys`` requires a full ``SSHTarget`` (DR-0023) -- so this verb
    cannot share ``HostParams`` with ``k3s.await_ssh`` any longer."""

    host: str
    ssh_user: str | None
    ssh_private_key_path: str | None


class KnownHostsOutput(BaseModel):
    known_hosts: str


class K3sTrustHostKeys(ProviderStep[TrustHostKeysParams, KnownHostsOutput]):
    verb = "k3s.trust_host_keys"
    provider_name: ClassVar[str] = "ssh-k3s"
    Params = TrustHostKeysParams
    Output = KnownHostsOutput
    undoable = False  # ProviderStep hard-defaults True; seam-c-provider.md §5.5:
    # "CaptureHostKeys/InstallK3s/FetchKubeconfig -> none -- subsumed by the instance undo".
    thin = True

    def command(self, params: TrustHostKeysParams) -> CaptureHostKeys:
        return CaptureHostKeys(ssh=_target(params.host, params.ssh_user, params.ssh_private_key_path, verb=self.verb))

    def output_from(self, value: object) -> KnownHostsOutput:
        keys = _expect(value, HostKeys, verb=self.verb)
        return KnownHostsOutput(known_hosts=keys.known_hosts)


# ---------------------------------------------------------------------------
# k3s.install -- consumes known_hosts (Conflict 14). No in-step retry loop.
# ---------------------------------------------------------------------------


class InstallK3sParams(BaseModel):
    host: str
    spec: ClusterSpecification
    extra_tls_san: str
    known_hosts: str
    ssh_user: str | None
    ssh_private_key_path: str | None
    # DR-0036: Optional, bound from `cluster.load_spec`'s `acme` output. Present only
    # when the profile enabled BOTH ssl and dns, which is exactly when the Ingress
    # templates render `router.tls.certresolver: letsencrypt` -- the two halves are
    # pinned to agree by a test, because an annotation naming a resolver nobody
    # configures is the bug this closes (backlog #24).
    acme: AcmeConfig | None = None


class K3sInstall(ProviderStep[InstallK3sParams, EmptyOutput]):
    verb = "k3s.install"
    provider_name: ClassVar[str] = "ssh-k3s"
    Params = InstallK3sParams
    Output = EmptyOutput
    undoable = False  # seam-c-provider.md §5.5: no inverse, subsumed by infra.create_instance's undo.
    thin = True

    def command(self, params: InstallK3sParams) -> InstallK3s:
        pod_cidr, service_cidr = _cidrs_for(params.spec, verb=self.verb)
        return InstallK3s(
            ssh=_target(params.host, params.ssh_user, params.ssh_private_key_path, verb=self.verb),
            known_hosts=params.known_hosts,
            pod_cidr=pod_cidr,
            service_cidr=service_cidr,
            tls_sans=(params.extra_tls_san,),
            ingress=_ingress_for(params.spec, params.acme),
        )

    def output_from(self, value: object) -> EmptyOutput:
        return EmptyOutput()


# ---------------------------------------------------------------------------
# k3s.await_api -- gateable, execute() a true no-op (DR-0022 P3/E4b).
# ---------------------------------------------------------------------------


class K3sAwaitReadyParams(BaseModel):
    host: str
    known_hosts: str
    ssh_user: str | None
    ssh_private_key_path: str | None


class K3sAwaitApi(ProviderStep[K3sAwaitReadyParams, EmptyOutput]):
    verb = "k3s.await_api"
    provider_name: ClassVar[str] = "ssh-k3s"
    Params = K3sAwaitReadyParams
    Output = EmptyOutput
    gateable = True
    undoable = False
    thin = True

    def command(self, params: K3sAwaitReadyParams) -> ProbeK3s:
        return ProbeK3s(
            ssh=_target(params.host, params.ssh_user, params.ssh_private_key_path, verb=self.verb), known_hosts=params.known_hosts
        )

    async def execute(self, params: K3sAwaitReadyParams, ctx: StepContext) -> EmptyOutput:
        """True no-op (DR-0022 Erratum E4b) -- never touches ``ctx.services.providers``."""
        return EmptyOutput()

    async def poll_ready(
        self, params: K3sAwaitReadyParams, provisional: EmptyOutput, ctx: StepContext
    ) -> Ready[EmptyOutput] | NotReady:
        provider = ctx.services.providers[self.provider_name]
        value: object | None = None
        async for ev in provider.execute(self.command(params)):
            if isinstance(ev, Result):
                value = ev.value
        readiness = _expect(value, K3sReadiness, verb=self.verb)
        if readiness.ready:
            return Ready(outputs=EmptyOutput())
        return NotReady(detail=readiness.detail)


# ---------------------------------------------------------------------------
# k3s.fetch_kubeconfig -- the ssh variant; consumes known_hosts, rewrites the
# server to the public address (the provider does the rewrite; this binding
# only threads the fields through).
# ---------------------------------------------------------------------------


class FetchKubeconfigParams(BaseModel):
    """Fixed ``ssh-k3s`` provider_name, never late-bound (only ``infra.*``
    late-binds) -- distinct from ``infra.fetch_kubeconfig``'s
    ``resource_ids`` variant (``engine/steps/infra.py``)."""

    host: str
    rewrite_server_to: str
    known_hosts: str
    ssh_user: str | None
    ssh_private_key_path: str | None


class KubeconfigOutput(BaseModel):
    kubeconfig: SecretStr


class K3sFetchKubeconfig(ProviderStep[FetchKubeconfigParams, KubeconfigOutput]):
    verb = "k3s.fetch_kubeconfig"
    provider_name: ClassVar[str] = "ssh-k3s"
    Params = FetchKubeconfigParams
    Output = KubeconfigOutput
    undoable = False  # seam-c-provider.md §5.5: no inverse, subsumed by the instance undo.
    thin = True

    def command(self, params: FetchKubeconfigParams) -> FetchKubeconfig:
        return FetchKubeconfig(
            rewrite_server_to=params.rewrite_server_to,
            ssh=_target(params.host, params.ssh_user, params.ssh_private_key_path, verb=self.verb),
            known_hosts=params.known_hosts,
        )

    def output_from(self, value: object) -> KubeconfigOutput:
        kubeconfig = _expect(value, Kubeconfig, verb=self.verb)
        return KubeconfigOutput(kubeconfig=SecretStr(kubeconfig.yaml_text))
