"""seedpod/providers/contract.py — Seam C §5.2-5.4 (Decision 5), amended by
docs/design/coherence-review.md Conflicts 5-7, 12.

THE home of:

- the ``ProviderEvent`` stream vocabulary (``Progress``/``Result``/``Observed``) and
  ``RESOURCE_ALLOCATED``, the one contractual progress phase (§5.2, Conflict 7's
  amendment: ``Observed.data`` is normatively "the persisted ``workflow_steps.notes``",
  rehydratable after a crash, not just an in-memory fold);
- the complete ``ProviderCommand`` union — every command dataclass, the shared value
  types, and each command's typed ``Result`` (§5.3);
- the ``Provider`` protocol and its construction contract, plus the injected
  ``SubprocessRunner`` transport protocol (§5.4).

Commands are frozen, inert, serializable dataclasses with **no methods** — the same
discipline Pillar-1 holds effects to. Error classes are never defined here: they come
from ``seedpod.core.errors`` only (Conflict 6) and are raised, never yielded — an
``execute()`` stream is zero-or-more ``Progress`` then exactly one ``Result``, or a
raised taxonomy error; never both (§5.2 stream rules).

Salvage note: the ``DestroyStatus``/``DestroyOutcome`` vocabulary and every command
shape below are Proposal 3's "typed ``DestroyOutcome`` vocabulary" + fidelity map,
synthesized with Proposal 2's ``Observed``/``RESOURCE_ALLOCATED`` compensation idea and
Proposal 1's taxonomy, per Seam C's verdict section. Nothing here talks to a real
backend; concrete providers (``seedpod/providers/digitalocean.py`` etc.) implement
``Provider`` against these types in a later task.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Literal, Protocol, runtime_checkable

from seedpod.core.acme import AcmeConfig
from seedpod.core.cluster_spec import ClusterSpecification
from seedpod.core.reconciliation_intents import ReconciliationIntent
from seedpod.providers.kube_types import (
    DeploymentInfo,
    EventInfo,
    NodeInfo,
    PodDetails,
    PodInfo,
    PodWatchEvent,
)

__all__ = [
    # stream vocabulary
    "RESOURCE_ALLOCATED",
    "Progress",
    "Result",
    "ProviderEvent",
    "Observed",
    "jsonable",
    # re-exported salvaged DTOs (each command's "# Result: ..." comment below names
    # one of these directly rather than a wrapper dataclass; re-exported here so
    # ``seedpod.providers.contract`` is the one import surface for a command's full
    # request+result shape) — Reconcile's Result likewise names the salvaged intent
    # union from ``core/reconciliation_intents.py`` verbatim
    "PodInfo",
    "PodDetails",
    "NodeInfo",
    "DeploymentInfo",
    "EventInfo",
    "PodWatchEvent",
    "ReconciliationIntent",
    # shared value types
    "SSHTarget",
    "IngressConfig",
    "ClusterSnapshot",
    "DestroyStatus",
    "DestroyOutcome",
    # result value types
    "InstanceCreated",
    "InstanceState",
    "InstanceSummary",
    "SshPortState",
    "HostKeys",
    "K3sInstalled",
    "K3sReadiness",
    "Kubeconfig",
    "PodDetailsResult",
    "RolloutState",
    "RolloutUndoResult",
    "KubectlOutput",
    "WatchEnded",
    # commands — machine plane
    "CreateInstance",
    "ProbeInstance",
    "DestroyInstance",
    "ProbeDestruction",
    "ListInstances",
    "Reconcile",
    # commands — digitalocean-only extras
    "ApplyFirewalls",
    "AssignToProject",
    # commands — k3s plane
    "ProbeSshPort",
    "CaptureHostKeys",
    "InstallK3s",
    "ProbeK3s",
    "FetchKubeconfig",
    # commands — kubernetes plane
    "KubeGetClusterInfo",
    "KubeGetNodes",
    "KubeGetPods",
    "KubeGetPodDetails",
    "KubeGetPodLogs",
    "KubeApplyManifest",
    "KubeDeleteManifest",
    "KubeGetDeployments",
    "KubeRestartDeployment",
    "KubeProbeRollout",
    "KubeGetEvents",
    "KubeRolloutUndo",
    "KubeRun",
    "KubeWatchPods",
    # unions
    "MachineCommand",
    "DigitalOceanCommand",
    "K3sCommand",
    "KubectlCommand",
    "ProviderCommand",
    # protocol + transport
    "Provider",
    "SubprocessResult",
    "SubprocessRunner",
]


# ============================================================================
# 5.2 — ProviderEvent union
# ============================================================================

RESOURCE_ALLOCATED = "resource-allocated"  # the ONE contractual phase name (see §5.5)


@dataclass(frozen=True)
class Progress:
    """Non-terminal stream item. Replaces v1 ``advance_cluster_provisioning()`` calls,
    per-job SSE broadcasting, and ``watch_pods`` yields. ``phase`` is a stable
    machine-readable dotted string ("droplet.waiting_active", "k3s.installing",
    "pods.watch"); free-form EXCEPT that a create command MUST emit
    ``Progress(phase=RESOURCE_ALLOCATED, data={"resource_ids": {...}})`` as soon as the
    backend has assigned identifiers, before any readiness activity."""

    phase: str
    message: str = ""
    data: Mapping[str, object] = field(default_factory=dict)


@dataclass(frozen=True)
class Result:
    """Terminal. Exactly one per successful ``execute()``; nothing may follow it."""

    value: object  # the typed result declared per command below


ProviderEvent = Progress | Result  # errors are RAISED, never yielded


@dataclass(frozen=True)
class Observed:
    """Engine-side fold of a (possibly truncated) stream, fed to ``undo_for()`` (§5.5).

    Per Conflict 7's amendment: ``data`` is normatively "the persisted
    ``workflow_steps.notes``", not an in-memory fold — ``engine/provider_step.py``
    writes ``RESOURCE_ALLOCATED`` progress through ``ctx.note()`` (durable, pre-return)
    so ``Observed`` is rehydratable from the DB after a crash, closing C1 for both the
    mid-stream-death and process-crash windows through this one path.
    """

    data: Mapping[str, object]
    value: object | None  # terminal Result value, or None if the stream died


def jsonable(data: Mapping[str, object]) -> Mapping[str, object]:
    """Coerce a ``Progress``/``Result`` event's free-form ``data`` into JSON-safe kwargs
    for ``ctx.progress(**fields)`` (``engine/step.py``'s ``JsonValue``). Leaves already
    JSON-safe scalars/containers alone; anything else (dataclass instances such as
    ``PodWatchEvent``, arbitrary objects) is stringified so ``ctx.progress`` — which
    must never raise to the step (Seam B §2.1) — cannot choke on it."""

    def _coerce(value: object) -> object:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, Mapping):
            return {str(k): _coerce(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [_coerce(v) for v in value]
        return str(value)

    return {str(k): _coerce(v) for k, v in data.items()}


# ============================================================================
# 5.3 — shared value types
# ============================================================================


@dataclass(frozen=True)
class SSHTarget:
    host: str
    user: str
    private_key_path: str  # ~ pre-expanded by the config loader
    port: int = 22
    connection_timeout_s: int = 30
    command_timeout_s: int = 600  # NO retry fields: retry_attempts/retry_delay die here


@dataclass(frozen=True)
class IngressConfig:
    ingress_type: str  # "traefik" | "none"
    enabled: bool = True
    expose_method: str = "loadbalancer"  # "hostport" ⇒ HelmChartConfig base64-piped BEFORE install
    # DR-0036: the Let's Encrypt certresolver, when the profile enabled BOTH ssl and dns
    # (v1's `use_acme_certs`). None for every other profile. Defaulted, so every existing
    # construction -- including the conformance harness's -- is unaffected. It rides here
    # rather than in its own command because it configures the SAME HelmChartConfig the
    # hostport strategy already writes, before k3s starts (DR-0036 decision 1).
    acme: AcmeConfig | None = None


@dataclass(frozen=True)
class ClusterSnapshot:  # the DB view, passed IN (providers stay DB-free)
    cluster_uuid: str
    slug: str
    status: str  # coarse Pillar-1 state name
    resource_ids: Mapping[str, str]


class DestroyStatus(StrEnum):  # v1's load-bearing string protocol, now typed
    DESTROYED = "destroyed"
    DESTROYING = "destroying"
    DESTROY_FAILED = "destroy_failed"  # v1 kind's "error" maps here


@dataclass(frozen=True)
class DestroyOutcome:
    status: DestroyStatus
    note: str | None = None  # "VM was already absent", "OrbStack cluster preserved..."
    error: str | None = None
    stuck_resources: tuple[str, ...] = ()


# ============================================================================
# result value types (transcribed from each command's "# Result: ..." comment)
# ============================================================================


@dataclass(frozen=True)
class InstanceCreated:
    resource_ids: Mapping[str, str]
    address: str | None
    effective_pod_cidr: str
    effective_service_cidr: str
    adopted_existing: bool = False


@dataclass(frozen=True)
class InstanceState:
    phase: Literal["provisioning", "running", "stopped", "absent"]
    address: str | None
    detail: str = ""


@dataclass(frozen=True)
class InstanceSummary:
    """Deliberately has NO ``cluster_id`` field, making v1's "placeholder
    cluster_id=name persisted as real" bug unrepresentable."""

    name: str
    resource_ids: Mapping[str, str]


@dataclass(frozen=True)
class SshPortState:
    """``open`` is the ONLY decision input -- ``ProbeSshPort`` cannot classify-fail, so
    every connect error collapses to ``open=False`` (seam-c §5.1's decision row), which is
    right because "not booted yet" is the common case.

    ``detail`` (DR-0033) carries the failed dial's error for humans -- ``[Errno 65] No route
    to host`` (the macOS Local Network denial of backlog #15), ``[Errno 61] Connect call
    failed (...)``, ``connect timed out after 3.0s`` -- so a gate that gives up can say WHICH
    failure it kept hitting. The errno NUMBER is the identifying part: ``asyncio`` wraps the
    refusal but passes ``strerror`` through for 65, and never emits the constant names. It is **diagnostic-only and
    MUST NEVER be read for control flow**; a VM that is merely still booting and a host
    that is denying the network are indistinguishable in ``open``, and that is deliberate.
    Empty when the port is open, and empty on any provider that never sets it."""

    open: bool
    detail: str = ""


@dataclass(frozen=True)
class HostKeys:
    known_hosts: str


@dataclass(frozen=True)
class K3sInstalled:
    pass


@dataclass(frozen=True)
class K3sReadiness:
    ready: bool
    detail: str = ""


@dataclass(frozen=True)
class Kubeconfig:
    yaml_text: str


@dataclass(frozen=True)
class PodDetailsResult:
    found: bool
    details: PodDetails | None


@dataclass(frozen=True)
class RolloutState:
    complete: bool
    message: str = ""


@dataclass(frozen=True)
class RolloutUndoResult:
    """Partial-success semantics preserved EXACTLY (crown jewel #13): undoes EVERY
    deployment in the namespace; success iff >=1 undo succeeded.

    **The PROVIDER enforces that rule, not the caller.** ``KubectlProvider._rollout_undo``
    raises ``PermanentError`` itself (carrying the aggregated ``errors``) when
    ``succeeded == 0``, and yields this Result only on success — so a
    ``RolloutUndoResult`` reaching a step ALWAYS means the undo succeeded, and
    ``kube.rollout_undo`` deliberately re-implements nothing. (An earlier revision of
    this docstring said "the caller raises"; that predates the provider taking the job
    and cost a Round-8b implementation pass, which wrote the rule into the step before
    finding the real one. One crown jewel, one home.)

    Two subtleties that keep the rule from being "simplified" wrongly:

    - It is NOT ``succeeded == 0 ⇒ failure``. v1's explicit "no deployments to undo ⇒
      trivial success" case (``reference-code/.../kubernetes.py:965-966``) yields
      ``succeeded=0, failed=0`` from an early return BEFORE the raise — an empty
      namespace is not a failure to undo anything in.
    - A genuine connectivity symptom mid-loop raises immediately rather than being
      folded into ``failed`` (crown jewel #1 extended to that loop), so a nonzero
      ``failed`` here always means per-deployment rejections, never "cannot determine
      state"."""

    succeeded: int
    failed: int
    outputs: str
    errors: str


@dataclass(frozen=True)
class KubectlOutput:
    """``stdout`` may be ``bytes`` — required for ``pg_dump -Fc`` snapshot streaming
    (crown jewel #14)."""

    stdout: str | bytes
    stderr: str


@dataclass(frozen=True)
class WatchEnded:
    reason: str


# ============================================================================
# 5.3 — commands: machine plane (digitalocean | kind | tart | orbstack)
# ============================================================================


@dataclass(frozen=True)
class CreateInstance:
    """MUST emit ``Progress(RESOURCE_ALLOCATED)`` the moment ids exist, and MUST write
    the cluster-uuid identity tag/name atomically with resource creation
    (tag-before-boot). MUST be re-invocation-safe: same ``cluster_uuid`` again ⇒ adopt
    the tagged/named resource (``adopted_existing=True``), never a duplicate. NEW
    obligation, not v1 salvage — see Seam C §5.7."""

    cluster_uuid: str  # == the idempotency key (conformance C-07)
    slug: str
    spec: ClusterSpecification  # translate_node_spec runs INSIDE the impl, no longer public
    pod_cidr: str  # from salvaged allocate_cluster_cidrs(); kind MAY override
    service_cidr: str  #   (kindnet /16 — crown jewel #7); overrides echoed in result
    tags: tuple[str, ...]  # engine supplies cluster-uuid:{uuid}, cluster-{slug}, ttl-{h}
    tls_sans: tuple[str, ...] = ()
    api_host: str | None = None  # kind/orbstack SAN + kubeconfig rewrite target
    api_port: int | None = None  # None ⇒ provider allocates (kind port scan, salvaged as-is)
    # Result: InstanceCreated


@dataclass(frozen=True)
class ProbeInstance:  # ONE iteration of every v1 readiness/status poll
    resource_ids: Mapping[str, str]
    # Result: InstanceState. "absent" is AUTHORITATIVE (API said so). Cannot-answer ⇒
    # raise Unreachable. Never conflate.


@dataclass(frozen=True)
class DestroyInstance:
    """Semantics preserved exactly: idempotent on absence (DESTROYED + note, but for DO
    ONLY when the API call succeeded — otherwise raise Unreachable, v1
    ``api_call_succeeded``); tart stop-ok-delete-failed ⇒ DESTROYING (gate retries);
    orbstack ⇒ DESTROYED no-op + note."""

    slug: str  # DO legacy cluster-{slug} tag fallback preserved
    resource_ids: Mapping[str, str]
    # Result: DestroyOutcome


@dataclass(frozen=True)
class ProbeDestruction:
    """ONE iteration of ``poll_destruction_status``. DO: archive/off ⇒ DESTROYING;
    active ⇒ DESTROY_FAILED + stuck_resources; gone ⇒ DESTROYED. Transient/garbage-body
    ⇒ raise Unreachable (engine keeps polling — v1's "stay destroying", expressed as
    park-and-reprobe)."""

    resource_ids: Mapping[str, str]
    # Result: DestroyOutcome


@dataclass(frozen=True)
class ListInstances:
    pass
    # Result: tuple[InstanceSummary, ...]


@dataclass(frozen=True)
class Reconcile:
    """Result: tuple[ReconciliationIntent, ...] (salvaged Orphan/Zombie/CreateUnmanaged/
    StatusSync dataclasses, verbatim). Intent-mapping logic copied per provider
    unchanged (DO Phase A/B incl. unmanaged-droplet skip + CreateUnmanagedIntent; kind
    container-stopped ⇒ Orphan; tart docstring matrix; orbstack never orphans;
    DESTROYING+missing ⇒ Orphan completion backstop everywhere). Backend unreachable ⇒
    RAISE InfrastructureUnreachableError — the engine skips every cluster in the
    command and touches nothing. TWO deliberate changes from v1: the internal
    catch-to-``.unreachable()`` becomes a raise (same net behavior, uniform rule), and
    the "any other exception ⇒ success([])" swallow becomes a raise (logged, retried
    next tick)."""

    clusters: tuple[ClusterSnapshot, ...]
    # Result: tuple[ReconciliationIntent, ...]


@dataclass(frozen=True)
class ApplyFirewalls:
    """DigitalOcean-only, best-effort, warn-and-continue by WORKFLOW policy (Seam B §2.2
    Proof 2 / ``seam-b-engine.md:330-334`` and the shipped
    ``config/workflows/provision-digitalocean.yml``'s ``firewalls`` step declare
    ``on_failure: continue``, citing v1 reference-code .../digitalocean.py:477's inline
    try/except-warn) — this command itself raises normally on failure like any other
    command; it does not swallow. ``spec`` (not a bare region string) matches the already-
    pinned workflow step shape (``with: {resource_ids: ..., spec: {from: spec.spec}}``);
    the region is derived internally the same way ``CreateInstance`` derives it
    (``_translate_node_spec``). Ensures the management (SSH + K3s API) and application
    (HTTP/HTTPS) firewalls exist for the droplet's region (create-if-missing, shared/
    named-by-region resources — v1 ``_ensure_firewall_exists``, reference-code
    .../digitalocean.py:589-650) and attaches the droplet to both (v1
    ``firewall.add_droplets``, lines 476-477). NOT undoable: ensure-exists is itself
    idempotent and the firewall is a shared per-region resource, not owned by any one
    droplet."""

    resource_ids: Mapping[str, str]
    spec: ClusterSpecification
    # Result: None


@dataclass(frozen=True)
class AssignToProject:
    """DigitalOcean-only. §5.7.4 collapses v1's triple fire-and-forget project assignment
    (reference-code .../digitalocean.py: lines 353, 451, 454) to ONE early, best-effort,
    ``await``ed attempt inside ``CreateInstance`` (unchanged — still closes the C1
    mid-create-death window, conformance C-09) PLUS this ONE additional late attempt, fired
    from its own workflow step positioned after K3s install (mirroring v1's own placement:
    the reference code's second/third assignment calls both fire only after
    ``get_kubeconfig`` succeeds, "AFTER successful K3s install" — by which point the
    droplet has existed for minutes, structurally past any "not yet fully created" window
    v1's ``asyncio.sleep(5)`` existed to avoid without this provider ever sleeping, per
    H4-H6). NOT undoable (no side effect to reverse on failure)."""

    resource_ids: Mapping[str, str]
    # Result: None


# ============================================================================
# 5.3 — commands: k3s plane ("ssh-k3s" provider — wraps salvaged
# _ssh_k3s_installer bodies)
# ============================================================================


@dataclass(frozen=True)
class ProbeSshPort:  # raw TCP connect_ex; gate polls; 5s settle = gate param
    host: str
    port: int = 22
    # Result: SshPortState


@dataclass(frozen=True)
class CaptureHostKeys:
    """TOFU pair, atomic, order load-bearing (crown jewel #2): (1) the ONLY
    ``StrictHostKeyChecking=no`` call runs ``cloud-init status --wait || true``;
    (2) ``ssh-keyscan -t ed25519,rsa,ecdsa``. Empty scan ⇒
    Transient(HOST_KEYS_PENDING)."""

    ssh: SSHTarget
    cloud_init_wait_timeout_s: int = 300
    keyscan_timeout_s: int = 10
    # Result: HostKeys — the per-instance installer cache becomes typed data-flow.


@dataclass(frozen=True)
class InstallK3s:
    """One ``curl | sh`` attempt over strict-checked SSH. Body salvaged verbatim:
    ``--write-kubeconfig-mode=644``, ``--disable=servicelb``, CIDR flags,
    ``--tls-san`` per SAN, traefik disable logic, hostport HelmChartConfig
    base64-piped BEFORE install."""

    ssh: SSHTarget
    known_hosts: str  # empty ⇒ PermanentError(INVALID_INPUT): install-before-keys is unrepresentable
    pod_cidr: str
    service_cidr: str
    tls_sans: tuple[str, ...]
    ingress: IngressConfig
    # Result: K3sInstalled


@dataclass(frozen=True)
class ProbeK3s:  # ONE iteration: systemctl is-active AND `k3s kubectl get nodes`
    ssh: SSHTarget  # (active-but-API-down is a real distinct state — both kept)
    known_hosts: str
    # Result: K3sReadiness


@dataclass(frozen=True)
class FetchKubeconfig:
    """ssh-k3s: ``sudo cat /etc/rancher/k3s/k3s.yaml`` over strict SSH. kind: ``kind get
    kubeconfig``. orbstack: ``kubectl config view --raw --minify --context orbstack``.
    All THREE salvaged rewrite variants preserved (crown jewel #6): generic
    scheme/port-preserving; kind incl. 0.0.0.0 + allocated-port substitution; orbstack
    ``\\2`` port-preserving backreference (TLS cert validity). In-memory rewrite,
    never sed-over-SSH."""

    rewrite_server_to: str
    resource_ids: Mapping[str, str] = field(default_factory=dict)  # kind/orbstack variants
    ssh: SSHTarget | None = None  # ssh-k3s variant
    known_hosts: str | None = None
    # Result: Kubeconfig — the engine encrypts + persists; providers never store it.


# ============================================================================
# 5.3 — commands: kubernetes plane ("kubectl" provider) — kubeconfig ALWAYS a
# field (closes H18)
# ============================================================================
# Every command carries `kubeconfig: str` (decrypted YAML, bound by the engine's step
# runner from the cluster repository). The provider writes it to a 0600 registered temp
# file and unlinks in finally. Magic strings die: "kubeconfig_not_found" is the
# caller's impossibility.


@dataclass(frozen=True)
class KubeGetClusterInfo:
    kubeconfig: str
    # Result: str


@dataclass(frozen=True)
class KubeGetNodes:
    kubeconfig: str
    # Result: tuple[NodeInfo, ...]


@dataclass(frozen=True)
class KubeGetPods:
    kubeconfig: str
    namespace: str | None = None  # None ⇒ -A
    # Result: tuple[PodInfo, ...]


@dataclass(frozen=True)
class KubeGetPodDetails:
    kubeconfig: str
    pod_name: str
    namespace: str = "default"
    # Result: PodDetailsResult


@dataclass(frozen=True)
class KubeGetPodLogs:
    kubeconfig: str
    pod_name: str
    namespace: str = "default"
    container: str | None = None
    tail_lines: int = 100
    previous: bool = False
    # Result: str


@dataclass(frozen=True)
class KubeApplyManifest:
    kubeconfig: str
    manifest_yaml: str
    timeout_s: float = 120.0
    # Result: str


@dataclass(frozen=True)
class KubeDeleteManifest:
    """NEW command (v1 had no manifest inverse) — see Seam C §5.7."""

    kubeconfig: str
    manifest_yaml: str
    ignore_not_found: bool = True
    # Result: str


@dataclass(frozen=True)
class KubeGetDeployments:
    kubeconfig: str
    namespace: str = "default"
    # Result: tuple[DeploymentInfo, ...]


@dataclass(frozen=True)
class KubeRestartDeployment:
    kubeconfig: str
    deployment: str
    namespace: str = "default"
    # Result: str


@dataclass(frozen=True)
class KubeProbeRollout:
    """``kubectl rollout status --watch=false``; gate polls. Replaces blocking
    ``wait_for_rollout`` (Seam C taste call 2: no command waits, all waiting is an
    engine gate)."""

    kubeconfig: str
    deployment: str
    namespace: str = "default"
    # Result: RolloutState


@dataclass(frozen=True)
class KubeGetEvents:
    kubeconfig: str
    namespace: str | None = None
    limit: int = 100
    # Result: tuple[EventInfo, ...] (sorted last_timestamp desc, then limited — salvaged)


@dataclass(frozen=True)
class KubeRolloutUndo:
    kubeconfig: str
    namespace: str = "default"
    # Result: RolloutUndoResult


@dataclass(frozen=True)
class KubeRun:
    """Exposed only via the reviewed ``kubectl`` step type, never via config
    strings."""

    kubeconfig: str
    args: tuple[str, ...]
    timeout_s: float = 30.0
    binary: bool = False
    stdin: bytes | None = None
    """Bytes fed to the child process's stdin — the INPUT counterpart to
    ``binary``'s output handling, and the half snapshot restore needed.

    ``binary=True`` exists so ``pg_dump -Fc``'s output survives undecoded
    (crown jewel #14). Nothing carried the reverse direction, so
    ``SnapshotService.restore`` could exec ``pg_restore`` but had no way to hand
    it the dump: it checked the file existed and then ran the command against an
    empty stdin, which fails with ``input file is too short`` — a message that
    reads as a corrupt dump rather than as v2 never sending one.

    **A caller passing ``stdin`` to ``kubectl exec`` must also pass ``-i``**, or
    kubectl does not attach the pod's stdin and the bytes are discarded by the
    remote end rather than by this layer. That is the caller's job because
    ``args`` is opaque here; ``SnapshotService.restore`` is the one caller that
    does it today and its own test pins the flag.
    """
    # Result: KubectlOutput


@dataclass(frozen=True)
class KubeWatchPods:
    """The one natively streaming command: each watch event ⇒
    ``Progress(phase="pods.watch", data={"event": PodWatchEvent})``. ALL v1 hardening
    salvaged: ``--output-watch-events`` framing, skip non-JSON/non-dict lines, 30s
    readline heartbeat, stderr harvest at stream end, terminate→kill in finally,
    ``CancelledError`` re-raised."""

    kubeconfig: str
    namespace: str = "default"
    timeout_s: int = 300
    # Result: WatchEnded


# ============================================================================
# 5.3 — plane unions
# ============================================================================

MachineCommand = (
    CreateInstance | ProbeInstance | DestroyInstance | ProbeDestruction | ListInstances | Reconcile
)
# DigitalOcean-only extras (§5.7.1 amendment — not part of the generic machine plane every
# machine provider implements; `Provider.supported` gates which provider instance accepts
# them, same convention as FetchKubeconfig's kind/orbstack-only membership below).
DigitalOceanCommand = ApplyFirewalls | AssignToProject
K3sCommand = ProbeSshPort | CaptureHostKeys | InstallK3s | ProbeK3s | FetchKubeconfig
KubectlCommand = (
    KubeGetClusterInfo
    | KubeGetNodes
    | KubeGetPods
    | KubeGetPodDetails
    | KubeGetPodLogs
    | KubeApplyManifest
    | KubeDeleteManifest
    | KubeGetDeployments
    | KubeRestartDeployment
    | KubeProbeRollout
    | KubeGetEvents
    | KubeRolloutUndo
    | KubeRun
    | KubeWatchPods
)
ProviderCommand = MachineCommand | DigitalOceanCommand | K3sCommand | KubectlCommand

# FetchKubeconfig is in both the machine plane's kind/orbstack subset and the k3s plane
# (§5.4 "Plane matrix": digitalocean/tart ⇒ machine plane minus FetchKubeconfig;
# kind/orbstack ⇒ machine plane incl. FetchKubeconfig; ssh-k3s ⇒ k3s plane). It appears
# once in ``K3sCommand`` above (its Python identity is the same regardless of which
# union names it); ``Provider.supported`` — not this module — is what actually gates
# which commands a given provider instance accepts.


# ============================================================================
# 5.4 — the Provider protocol + injected transport
# ============================================================================


@runtime_checkable
class Provider(Protocol):
    name: str  # "digitalocean" | "kind" | "tart" | "orbstack" | "ssh-k3s" | "kubectl"
    supported: frozenset[type]  # command classes this provider accepts

    async def check_ready(self) -> None:
        """Startup preflight, called once by the composition root before serving:
        binary on PATH, tart base image present, docker up, token present. Raises
        PermanentError (NOT_FOUND/AUTH/INVALID_INPUT) or
        InfrastructureUnreachableError. Replaces v1's sync-subprocess-in-``__init__``
        checks: fail at startup, not mid-provision."""
        ...

    def execute(self, cmd: ProviderCommand) -> AsyncIterator[ProviderEvent]:
        """Stateless. All context in ``cmd`` (kubeconfig passed in, never fetched —
        H18 closed). No DB, no state_manager, no scheduler, no retry loop, no poll
        loop, no backoff sleep — one bounded attempt (engine owns Schedule: H4-H6
        closed). Streams per §5.2. Unsupported command ⇒ PermanentError(UNSUPPORTED)
        immediately, no backend traffic."""
        ...


# --------------------------------------------------------------------------------------
# Injected transport. ``Provider.__init__(config, transport)`` is IO-free (enforced by
# conformance, not this Protocol) — it stores config and an injected transport: this
# ``SubprocessRunner`` (wraps the salvaged ``create_tracked_subprocess`` pattern,
# reference-code/seedpod/seedpod/core/subprocess_manager.py) or a shared
# ``httpx.AsyncClient`` for HTTP-speaking providers/services. Fault injection for the
# conformance suite sits at this seam — never Mock/patch (CLAUDE.md).
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class SubprocessResult:
    """One bounded attempt's outcome. Never raises for a clean non-zero exit — that is
    an AUTHORITATIVE answer the caller (a provider, via ``classify_subprocess``)
    classifies. ``timed_out``/``binary_missing`` are exclusive signals distinguishing
    "no answer" from "binary not on PATH", both of which providers.classify.py maps to
    ``InfrastructureUnreachableError`` when ``observing_infra=True``."""

    returncode: int
    stdout: bytes
    stderr: bytes
    timed_out: bool = False
    binary_missing: bool = False


class SubprocessRunner(Protocol):
    """Injected transport for provider subprocess IO. One bounded attempt per call: no
    internal retry, no internal sleep (§5.4's construction contract — the engine's
    ``Schedule`` owns retry, H4-H6). All long subprocesses go through the salvaged
    ``create_tracked_subprocess`` pattern under the hood (§5.2 stream rules): child
    processes get a terminate→kill escalation on cancellation, and every temp file a
    provider hands this runner is unlinked in ``finally`` by the caller
    (``seedpod/core/tempfiles.py``), not by the runner itself.
    """

    async def run(
        self,
        argv: Sequence[str],
        *,
        stdin: bytes | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        cluster_id: str | None = None,
    ) -> SubprocessResult:
        """Run ``argv`` to completion (or ``timeout``) and collect its output. Never
        raises for a clean non-zero exit; ``FileNotFoundError`` (binary missing) and
        wall-clock timeout are reported via the result's flags, not exceptions, so a
        single ``classify_subprocess`` call downstream handles every case
        uniformly."""
        ...

    def stream(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        cluster_id: str | None = None,
    ) -> AbstractAsyncContextManager[AsyncIterator[bytes]]:
        """Long-lived subprocess (``KubeWatchPods``): an async-context-managed
        line-oriented byte iterator over the child's stdout. Cancellation — cooperative
        asyncio cancellation per §5.2 — kills the process group (terminate→kill
        escalation, salvaged) before ``CancelledError`` propagates; the context
        manager's ``__aexit__`` is where that cleanup and the final stderr harvest
        happen, in every exit path (normal end, cancellation, or exception)."""
        ...
