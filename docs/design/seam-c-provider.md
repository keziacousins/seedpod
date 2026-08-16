---
title: Seam C — Provider contract (Pillar 3)
type: design
status: active
created: 2026-07-12
updated: 2026-08-09
amended-by: coherence-review.md   # Conflicts 5–7, 12 override where they touch this spec (incl. errors moving to core/errors.py)
# DR-0033 adds SshPortState.detail (diagnostic-only); edited in place per DR-0001.
---

# Seam C — Provider contract (Decision 5): FINAL SPECIFICATION

## Verdicts

**Proposal 1 (taxonomy-first) — winner on the error model, loser on compensation.** Its 41-row classification decision table is the single best artifact submitted: every observed v1 failure mode lands in exactly one cell, and the fourth rule ("not every failure is an error") correctly promotes v1's hardest-won lesson to a law. Making `InfrastructureUnreachableError` a *sibling* of Transient/Permanent — with pinned engine semantics of park-never-compensate — is the only reading that makes crown jewel #1 structurally unregressable; the subclass designs in P2/P3 let a generic `except TransientError` retry policy exhaust its budget and fall into compensation on a network blip, which is precisely the mass-false-orphaning failure the v1 docstring warns against. Fatal flaw: `undo_for(cmd, result)` keyed on the *terminal* Result cannot compensate a create whose stream died after the droplet was allocated — the actual C1 failure mode — so P1 quietly re-opens C1 and leans on the reconciliation backstop alone. Its nullable `MachineCreated.kubeconfig` and the third event type `StreamItem` are avoidable concepts.

**Proposal 2 (suite-first) — winner on compensation and testability, loser on the taxonomy shape.** The contractual `resource-allocated` progress phase + engine-side `Observed` fold + compensation-from-a-truncated-stream is the most important single idea across all three proposals: it is the only design in which C1 is closed *structurally* for mid-create failures rather than by backstop. Transport injection (fakes at the subprocess/HTTP seam, never `Mock`/`patch`) is what makes the conformance suite actually writable under the plan's testing rules, and the DNS "only delete if `created`" nuance is a real correctness improvement over v1. Fatal flaws: `InfrastructureUnreachableError(TransientError)` conflates the epistemic state with ordinary retry (see above); promoting Cloudflare/GHCR to full `Provider`s contradicts the plan's explicit "supporting services, not Providers"; `cmd.compensation()` puts logic on what the plan insists is inert data; and collapsing the kubectl surface into a stringly `kind=` parameter throws away the typed salvaged DTOs.

**Proposal 3 (fidelity-first) — winner on completeness, loser on structure.** Its v1→v2 fidelity map is the best regression insurance in the pile and is adopted nearly wholesale as the salvage annex; the typed `DestroyOutcome` vocabulary, the verbatim docstring salvage, the removal of `cleanup_expired_clusters` to a Pillar-1 timer, and the honest flagging of genuinely-new code (create re-invocation safety) are all correct calls. Fatal flaws: the subclass taxonomy (same defect as P2); `undo_for(CreateInstance, None) → None` concedes the truncated-create case that P2 solves; `Reconcile` catching Unreachable *internally* breaks the otherwise-uniform "cannot answer ⇒ raise" rule; and `KubeWaitForRollout` keeps one blocking command in a contract whose whole point is that no command waits.

**Synthesis:** P1's taxonomy and decision table + P2's Observed/resource-allocated compensation and transport-injected harness + P3's command shapes, destroy vocabulary, and fidelity map.

---

# THE FINAL SPEC — Decision 5: the Provider contract

All v2 paths are absolute homes in the new tree. Salvage references are `reference-code/seedpod/seedpod/...`.

## 5.1 Error taxonomy — `seedpod/core/cluster_spec.py`

Lives beside the salvaged `allocate_cluster_cidrs` and `ClusterSpecification`, per the plan's letter. Providers classify **at the edge**; the engine dispatches on **type** only; `code` is machine-readable detail for logs, UI, and conformance asserts — never for engine control flow.

```python
from enum import StrEnum

class ErrorCode(StrEnum):
    API_TIMEOUT          = "api_timeout"           # HTTP/CLI call exceeded its deadline
    DAEMON_UNREACHABLE   = "daemon_unreachable"    # tart/docker binary missing or hung
    ENDPOINT_UNREACHABLE = "endpoint_unreachable"  # conn refused/reset/no-route/TLS-handshake-timeout
    MALFORMED_RESPONSE   = "malformed_response"    # empty/garbage body where JSON expected
    RATE_LIMITED         = "rate_limited"
    API_5XX              = "api_5xx"
    RESOURCE_BUSY        = "resource_busy"         # tart delete-after-stop failure, docker busy
    HOST_KEYS_PENDING    = "host_keys_pending"     # ssh-keyscan returned empty output
    AUTH                 = "auth"                  # 401/403-auth, bad kubeconfig creds
    INVALID_INPUT        = "invalid_input"         # bad manifest, missing resource_ids, bad zone
    NOT_FOUND            = "not_found"             # required referent absent (base image, DNS zone)
    ALREADY_EXISTS       = "already_exists"
    CAPACITY             = "capacity"              # kind port-range exhausted, DO quota
    SCRIPT_FAILED        = "script_failed"         # ssh/k3s/kubectl non-zero exit, non-network
    UNSUPPORTED          = "unsupported"           # command outside provider's supported set
    READINESS_TIMEOUT    = "readiness_timeout"     # ENGINE-synthesized: wait-gate budget exhausted
    RETRY_EXHAUSTED      = "retry_exhausted"       # ENGINE-synthesized: Schedule budget exhausted

class ProviderError(Exception):
    """Base. Never raised directly — one of the three leaves only."""
    def __init__(self, message: str, *, code: ErrorCode, provider: str,
                 command: str, detail: dict[str, str] | None = None):
        super().__init__(message)
        self.code, self.provider, self.command = code, provider, command
        self.detail = detail or {}     # raw stderr / http_status / exit_code live HERE only

class TransientError(ProviderError):
    """The same call may succeed if repeated. Engine retries per the step's Schedule."""
    def __init__(self, *args, retry_after: float | None = None, **kw):
        super().__init__(*args, **kw)
        self.retry_after = retry_after            # e.g. GHCR Retry-After header

class PermanentError(ProviderError):
    """Retrying is provably useless. Engine fails the step and runs the undo scope."""

class InfrastructureUnreachableError(ProviderError):     # SIBLING, not a Transient subclass
    """
    Raised when we cannot determine infrastructure state.

    This is NOT an error indicating infrastructure is gone - it means
    we cannot authoritatively determine the current state due to
    connectivity issues, timeouts, or other transient failures.

    Reconciliation should SKIP clusters when this is raised, not
    mark them as orphaned.
    """                                            # docstring salvaged VERBATIM from v1 cluster_spec.py:298
    def __init__(self, *args, host: str | None = None, **kw):
        super().__init__(*args, **kw)
        self.host = host       # api.digitalocean.com / docker host / apiserver URL / "localhost"
```

**The taxonomy in four sentences (normative):**

1. **Transient** — the operation failed but the world is fine; repeating may work (429, 5xx, SSH conn-refused during boot, busy resource, empty keyscan).
2. **Permanent** — repeating cannot help (auth, validation, missing base image, non-network script failure, capacity, unsupported command).
3. **Unreachable** — we could not get an *authoritative answer about managed infrastructure state* (cloud API / docker daemon / tart CLI / k8s apiserver timeout or connection failure). Raised only by the four machine providers and kubectl. **GHCR and Cloudflare never raise it** — no infra-state inference hangs on them; their connectivity failures are Transient.
4. **Not every failure is an error.** "Not ready yet" (no IP, k3s active-but-API-down, rollout progressing), "already absent" (destroy idempotency), and "image/record not found" on a read are **typed Result values**, never exceptions.

**Engine behavior per class (pinned — this table is part of the seam):**

| Class | Engine response |
|---|---|
| `TransientError` | Retry the step per its `Schedule` (default: exponential 5s×2, cap 60s, max 5 attempts; `retry_after` overrides the delay). Budget exhausted ⇒ synthesize `PermanentError(code=RETRY_EXHAUSTED)` and proceed as Permanent. |
| `PermanentError` | No retry. Fail the step, abort the run, execute the undo scope (§5.5) in reverse completion order. |
| `InfrastructureUnreachableError` | **Reconcile:** skip every cluster covered by the command; touch nothing (v1 `.unreachable()` behavior). **Mutation workflows:** park the run `BLOCKED` at its step cursor; re-probe on a slow schedule (5s, 15s, 30s, 60s cap) up to `unreachable_budget` (default 15 min); on exhaustion mark `BLOCKED_TIMEOUT`. **Compensation is NEVER triggered by this class** — undo would also fail; reconciliation cleans up when reachability returns. |

**Shared classifier + salvaged phrase lists** (string-sniffing survives *only* here, converting raw symptoms to types at the edge):

```python
TRANSIENT_STDERR_PHRASES = frozenset({          # installer + DO SSH + kind docker lists, merged
    "connection refused", "connection timed out", "no route to host",
    "network is unreachable", "cannot connect", "i/o timeout",
})
TART_NOT_FOUND_PHRASES = frozenset({            # _tart_cli._classify_not_found, verbatim
    "not found", "does not exist", "doesn't exist", "no such virtual machine",
})

def classify_subprocess(*, provider: str, command: str, host: str, rc: int, stderr: str,
                        timed_out: bool, binary_missing: bool,
                        observing_infra: bool) -> ProviderError:
    """observing_infra=True for machine providers + kubectl talking to their control plane:
    timeout / missing daemon / connection phrases ⇒ InfrastructureUnreachableError.
    observing_infra=False (SSH to a booting guest, supporting services): same symptoms ⇒ TransientError.
    A clean non-zero exit is an AUTHORITATIVE answer ⇒ PermanentError(SCRIPT_FAILED) — unless the
    caller pre-mapped it to absence-as-data (docker inspect rc≠0, TartNotFound)."""

def classify_http(*, provider: str, command: str, host: str, status: int,
                  rate_limited: bool = False, retry_after: float | None = None) -> ProviderError:
    """401/403-auth ⇒ Permanent(AUTH); 403+rate-limit signal / 429 ⇒ Transient(RATE_LIMITED, retry_after);
    408/5xx ⇒ Transient; garbage body ⇒ Transient(MALFORMED_RESPONSE) for services,
    Unreachable(MALFORMED_RESPONSE) for machine providers (v1's DO 'Expecting value' rule).
    404 never reaches here: reads map it to absence-as-data before classifying."""
```

### Classification decision table (every observed v1 failure mode → exactly one cell)

| # | Site | Symptom | Classification | Engine does |
|---|---|---|---|---|
| 1 | tart, any | binary missing / subprocess timeout (`TartDaemonUnreachable`) | Unreachable / `DAEMON_UNREACHABLE`\|`API_TIMEOUT`, host=localhost | park / skip |
| 2 | tart, `check_ready` | base image absent | Permanent / `NOT_FOUND` | refuse to start |
| 3 | tart, clone | "already exists" for **our** idempotency name | **Result** `InstanceCreated(adopted_existing=True)` | resume-safe create |
| 4 | tart, clone | source image not-found | Permanent / `NOT_FOUND` | abort + undo |
| 5 | tart, get_ip | rc≠0, VM exists, no IP yet | **Result** `InstanceReadiness(ready=False)` | gate polls (2s) |
| 6 | tart, stop | "not running"/"already stopped" | **Result** ok (idempotent) | continue |
| 7 | tart, stop/delete | `TartNotFound` | **Result** `DestroyOutcome(DESTROYED, note="already absent")` | destroy succeeds |
| 8 | tart, delete | non-zero after successful stop | Transient / `RESOURCE_BUSY` | retry destroy |
| 9 | DO, any API | timeout from `_run_sync` | Unreachable / `API_TIMEOUT`, host=api.digitalocean.com | park / skip; never report destroyed |
| 10 | DO, any API | garbage/empty JSON body | Unreachable / `MALFORMED_RESPONSE` (v1: "treat like timeout") | as row 9 |
| 11 | DO, any API | 401/403 auth | Permanent / `AUTH` (v1: "auth errors are real errors") | abort + undo |
| 12 | DO, any API | 429 | Transient / `RATE_LIMITED` | retry |
| 13 | DO, probe | droplet absent, API call **succeeded** | **Result** `phase="absent"` — authoritative | destroy done / Orphan material |
| 14 | DO, create | quota exceeded | Permanent / `CAPACITY` | abort + undo |
| 15 | DO, probe | droplet exists, status≠active | **Result** `ready=False` | gate polls; deadline ⇒ engine `READINESS_TIMEOUT` ⇒ Permanent ⇒ undo (**the C1 close**) |
| 16 | ssh-k3s, execute | stderr in `TRANSIENT_STDERR_PHRASES`, or timeout | Transient / `ENDPOINT_UNREACHABLE` | retry (replaces the in-provider loop, H4–H6) |
| 17 | ssh-k3s, execute | other non-zero exit | Permanent / `SCRIPT_FAILED` (stderr in `detail`) | abort + undo (v1: fail immediately) |
| 18 | ssh-k3s, keyscan | empty output | Transient / `HOST_KEYS_PENDING` | retry (host still booting) |
| 19 | ssh-k3s, probe | k3s active but `kubectl get nodes` fails | **Result** `K3sReadiness(ready=False)` | gate polls (10s) |
| 20 | kind, `check_ready` | binary missing | Permanent / `NOT_FOUND` | refuse to start |
| 21 | kind, command | subprocess timeout | Unreachable / `API_TIMEOUT`, host=docker_host | park / skip |
| 22 | kind, docker inspect | rc≠0 (container gone) | **Result** `phase="absent"` — authoritative | Orphan material |
| 23 | kind, docker | conn-refused/timeout/cannot-connect stderr | Unreachable / `ENDPOINT_UNREACHABLE` | park / skip (never false-orphan) |
| 24 | kind, create | non-zero exit | Permanent / `SCRIPT_FAILED` | abort + undo (generalizes `retain_on_failure=false`) |
| 25 | kind, port alloc | range 6443–6543 exhausted | Permanent / `CAPACITY` | abort |
| 26 | kind/orbstack, Traefik | rollout slow after apply | **not an error** — `ProbeRollout ⇒ complete=False` | non-fatal gate in workflow config (v1: "might just be slow") |
| 27 | kubectl, any | conn refused / i/o timeout / no route to apiserver | Unreachable / `ENDPOINT_UNREACHABLE`, host=apiserver URL | park (dead cluster: undo would fail too) |
| 28 | kubectl, any | 401/403 (bad kubeconfig) | Permanent / `AUTH` | abort + undo |
| 29 | kubectl, apply | validation / immutable-field error | Permanent / `INVALID_INPUT` | abort + undo |
| 30 | kubectl, get | resource NotFound | **Result** `found=False` | caller decides |
| 31 | kubectl, rollout | still progressing | **Result** `RolloutState(complete=False)` | gate polls; deadline ⇒ `READINESS_TIMEOUT` |
| 32 | GHCR | 401 | Permanent / `AUTH` | abort |
| 33 | GHCR | 403 rate limit | Transient / `RATE_LIMITED` (+retry_after) | retry (closes H4) |
| 34 | GHCR | 404 on list_tags/find_image | **Result** `[]` / `image=None` | fallback chain proceeds (crown jewel #5) |
| 35 | GHCR | 5xx / conn error / JSON garbage | Transient (service: never Unreachable) | retry |
| 36 | Cloudflare | conn error / timeout | Transient / `ENDPOINT_UNREACHABLE` | retry (closes H5) |
| 37 | Cloudflare | API errors payload (bad token / invalid record / missing zone) | Permanent / `AUTH`\|`INVALID_INPUT`\|`NOT_FOUND` | abort |
| 38 | Cloudflare, delete | record 404 | **Result** `DnsDeleted(existed=False)` (idempotent) | undo succeeds |

## 5.2 ProviderEvent union — `seedpod/providers/contract.py`

```python
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field

RESOURCE_ALLOCATED = "resource-allocated"   # the ONE contractual phase name (see §5.5)

@dataclass(frozen=True)
class Progress:
    """Non-terminal stream item. Replaces v1 advance_cluster_provisioning() calls, per-job SSE
    broadcasting, and watch_pods yields. `phase` is a stable machine-readable dotted string
    ("droplet.waiting_active", "k3s.installing", "pods.watch"); free-form EXCEPT that a create
    command MUST emit Progress(phase=RESOURCE_ALLOCATED, data={"resource_ids": {...}}) as soon
    as the backend has assigned identifiers, before any readiness activity."""
    phase: str
    message: str = ""
    data: Mapping[str, object] = field(default_factory=dict)

@dataclass(frozen=True)
class Result:
    """Terminal. Exactly one per successful execute(); nothing may follow it."""
    value: object                           # the typed result declared per command in §5.3

ProviderEvent = Progress | Result           # errors are RAISED, never yielded

@dataclass(frozen=True)
class Observed:
    """Engine-side fold of a (possibly truncated) stream, fed to undo_for() in §5.5."""
    data: Mapping[str, object]              # merged Progress.data, later wins
    value: object | None                    # terminal Result value, or None if the stream died
```

**Stream rules (pinned):** zero+ `Progress`, then exactly one `Result`, then end — or a raised taxonomy error; never both. Consumption is `async for` + `try` and nothing else. Cancellation is cooperative asyncio cancellation: implementations kill child processes (terminate→kill escalation, salvaged), unlink temp files in `finally`, and re-raise `CancelledError` (v1 `watch_pods` pattern, now universal). All long subprocesses go through the salvaged `create_tracked_subprocess`.

## 5.3 ProviderCommand union (complete) — `seedpod/providers/contract.py`

Frozen dataclasses; inert, serializable, **no methods** — the same discipline as Pillar-1 effects.

```python
# ── shared value types ─────────────────────────────────────────────────────
@dataclass(frozen=True)
class SSHTarget:
    host: str
    user: str
    private_key_path: str                   # ~ pre-expanded by the config loader
    port: int = 22
    connection_timeout_s: int = 30
    command_timeout_s: int = 600            # NO retry fields: retry_attempts/retry_delay die here

@dataclass(frozen=True)
class IngressConfig:
    ingress_type: str                       # "traefik" | "none"
    enabled: bool = True
    expose_method: str = "loadbalancer"     # "hostport" ⇒ HelmChartConfig base64-piped BEFORE install (salvaged)

@dataclass(frozen=True)
class ClusterSnapshot:                      # the DB view, passed IN (providers stay DB-free)
    cluster_uuid: str
    slug: str
    status: str                             # coarse Pillar-1 state name
    resource_ids: Mapping[str, str]

class DestroyStatus(StrEnum):               # v1's load-bearing string protocol, now typed
    DESTROYED = "destroyed"
    DESTROYING = "destroying"
    DESTROY_FAILED = "destroy_failed"       # v1 kind's "error" maps here

@dataclass(frozen=True)
class DestroyOutcome:
    status: DestroyStatus
    note: str | None = None                 # "VM was already absent", "OrbStack cluster preserved..."
    error: str | None = None
    stuck_resources: tuple[str, ...] = ()

# ── machine plane (digitalocean | kind | tart | orbstack) ──────────────────
@dataclass(frozen=True)
class CreateInstance:
    cluster_uuid: str                       # == the idempotency key (conformance C-07)
    slug: str
    spec: ClusterSpecification              # translate_node_spec runs INSIDE the impl, no longer public
    pod_cidr: str                           # from salvaged allocate_cluster_cidrs(); kind MAY override
    service_cidr: str                       #   (kindnet /16 — crown jewel #7); overrides echoed in result
    tags: tuple[str, ...]                   # engine supplies cluster-uuid:{uuid}, cluster-{slug}, ttl-{h}
    tls_sans: tuple[str, ...] = ()
    api_host: str | None = None             # kind/orbstack SAN + kubeconfig rewrite target
    api_port: int | None = None             # None ⇒ provider allocates (kind port scan, salvaged as-is)
# Result: InstanceCreated(resource_ids: Mapping[str,str], address: str | None,
#                         adopted_existing: bool = False,
#                         effective_pod_cidr: str, effective_service_cidr: str)
# MUST emit Progress(RESOURCE_ALLOCATED) the moment ids exist, and MUST write the
# cluster-uuid identity tag/name atomically with resource creation (tag-before-boot).
# MUST be re-invocation-safe: same cluster_uuid again ⇒ adopt the tagged/named resource
# (adopted_existing=True), never a duplicate. NEW obligation — see §5.7.

@dataclass(frozen=True)
class ProbeInstance:                        # ONE iteration of every v1 readiness/status poll
    resource_ids: Mapping[str, str]
# Result: InstanceState(phase: Literal["provisioning","running","stopped","absent"],
#                       address: str | None, detail: str = "")
# "absent" is AUTHORITATIVE (API said so). Cannot-answer ⇒ raise Unreachable. Never conflate.

@dataclass(frozen=True)
class DestroyInstance:
    slug: str                               # DO legacy cluster-{slug} tag fallback preserved
    resource_ids: Mapping[str, str]
# Result: DestroyOutcome. Semantics preserved exactly: idempotent on absence (DESTROYED + note,
# but for DO ONLY when the API call succeeded — otherwise raise Unreachable, v1 api_call_succeeded);
# tart stop-ok-delete-failed ⇒ DESTROYING (gate retries); orbstack ⇒ DESTROYED no-op + note.

@dataclass(frozen=True)
class ProbeDestruction:                     # ONE iteration of poll_destruction_status
    resource_ids: Mapping[str, str]
# Result: DestroyOutcome. DO: archive/off ⇒ DESTROYING; active ⇒ DESTROY_FAILED + stuck_resources;
# gone ⇒ DESTROYED. Transient/garbage-body ⇒ raise Unreachable (engine keeps polling —
# v1's "stay destroying", expressed as park-and-reprobe).

@dataclass(frozen=True)
class ListInstances:  pass
# Result: tuple[InstanceSummary, ...] — carries name + resource_ids and deliberately has NO
# cluster_id field, making v1's "placeholder cluster_id=name persisted as real" bug unrepresentable.

@dataclass(frozen=True)
class Reconcile:
    clusters: tuple[ClusterSnapshot, ...]
# Result: tuple[ReconciliationIntent, ...] (salvaged Orphan/Zombie/CreateUnmanaged/StatusSync
# dataclasses, verbatim). Intent-mapping logic copied per provider unchanged (DO Phase A/B incl.
# unmanaged-droplet skip + CreateUnmanagedIntent; kind container-stopped ⇒ Orphan; tart docstring
# matrix; orbstack never orphans; DESTROYING+missing ⇒ Orphan completion backstop everywhere).
# Backend unreachable ⇒ RAISE InfrastructureUnreachableError — the engine skips every cluster in
# the command and touches nothing. TWO deliberate changes from v1: the internal catch-to-
# .unreachable() becomes a raise (same net behavior, uniform rule), and the
# "any other exception ⇒ success([])" swallow becomes a raise (logged, retried next tick).

# ── k3s plane ("ssh-k3s" provider — wraps salvaged _ssh_k3s_installer bodies) ─
@dataclass(frozen=True)
class ProbeSshPort:                         # raw TCP connect_ex; gate polls; 5s settle = gate param
    host: str
    port: int = 22
# Result: SshPortState(open: bool, detail: str = "")
#   `open` is the ONLY decision input -- this command cannot classify-fail, so every connect
#   error collapses to open=False ("not booted yet" is the common case and must not fail a run).
#   `detail` (DR-0033) carries that error for humans -- "[Errno 65] No route to host" (the macOS
#   Local Network denial), "[Errno 61] Connect call failed (...)", "connect timed out after 3.0s"
#   -- so a gate that gives up can name what it kept hitting. The errno NUMBER is the stable part;
#   asyncio never emits the constant names. DIAGNOSTIC ONLY: never branch on it.

@dataclass(frozen=True)
class CaptureHostKeys:
    """TOFU pair, atomic, order load-bearing (crown jewel #2): (1) the ONLY
    StrictHostKeyChecking=no call runs `cloud-init status --wait || true`;
    (2) ssh-keyscan -t ed25519,rsa,ecdsa. Empty scan ⇒ Transient(HOST_KEYS_PENDING)."""
    ssh: SSHTarget
    cloud_init_wait_timeout_s: int = 300
    keyscan_timeout_s: int = 10
# Result: HostKeys(known_hosts: str) — the per-instance installer cache becomes typed data-flow.

@dataclass(frozen=True)
class InstallK3s:
    """One `curl | sh` attempt over strict-checked SSH. Body salvaged verbatim:
    --write-kubeconfig-mode=644, --disable=servicelb, CIDR flags, --tls-san per SAN,
    traefik disable logic, hostport HelmChartConfig base64-piped BEFORE install."""
    ssh: SSHTarget
    known_hosts: str                        # empty ⇒ PermanentError(INVALID_INPUT): install-before-keys is unrepresentable
    pod_cidr: str
    service_cidr: str
    tls_sans: tuple[str, ...]
    ingress: IngressConfig
# Result: K3sInstalled()

@dataclass(frozen=True)
class ProbeK3s:                             # ONE iteration: systemctl is-active AND `k3s kubectl get nodes`
    ssh: SSHTarget                          # (active-but-API-down is a real distinct state — both kept)
    known_hosts: str
# Result: K3sReadiness(ready: bool, detail: str = "")

@dataclass(frozen=True)
class FetchKubeconfig:
    """ssh-k3s: `sudo cat /etc/rancher/k3s/k3s.yaml` over strict SSH. kind: `kind get kubeconfig`.
    orbstack: `kubectl config view --raw --minify --context orbstack`. All THREE salvaged rewrite
    variants preserved (crown jewel #6): generic scheme/port-preserving; kind incl. 0.0.0.0 +
    allocated-port substitution; orbstack \\2 port-preserving backreference (TLS cert validity).
    In-memory rewrite, never sed-over-SSH."""
    rewrite_server_to: str
    resource_ids: Mapping[str, str] = field(default_factory=dict)   # kind/orbstack variants
    ssh: SSHTarget | None = None                                    # ssh-k3s variant
    known_hosts: str | None = None
# Result: Kubeconfig(yaml_text: str) — the engine encrypts + persists; providers never store it.

# ── kubernetes plane ("kubectl" provider) — kubeconfig ALWAYS a field (closes H18) ─
# Every command carries `kubeconfig: str` (decrypted YAML, bound by the engine's step runner
# from the cluster repository). The provider writes it to a 0600 registered temp file and
# unlinks in finally. Magic strings die: "kubeconfig_not_found" is the caller's impossibility.
@dataclass(frozen=True)
class KubeGetClusterInfo:  kubeconfig: str                                          # Result: str
@dataclass(frozen=True)
class KubeGetNodes:        kubeconfig: str                                          # Result: tuple[NodeInfo, ...]
@dataclass(frozen=True)
class KubeGetPods:         kubeconfig: str; namespace: str | None = None            # Result: tuple[PodInfo, ...] (None ⇒ -A)
@dataclass(frozen=True)
class KubeGetPodDetails:   kubeconfig: str; pod_name: str; namespace: str = "default"
# Result: PodDetailsResult(found: bool, details: PodDetails | None)
@dataclass(frozen=True)
class KubeGetPodLogs:
    kubeconfig: str; pod_name: str; namespace: str = "default"
    container: str | None = None; tail_lines: int = 100; previous: bool = False     # Result: str
@dataclass(frozen=True)
class KubeApplyManifest:   kubeconfig: str; manifest_yaml: str; timeout_s: float = 120.0   # Result: str
@dataclass(frozen=True)
class KubeDeleteManifest:  kubeconfig: str; manifest_yaml: str; ignore_not_found: bool = True
# Result: str. NEW command (v1 had no manifest inverse) — see §5.7.
@dataclass(frozen=True)
class KubeGetDeployments:  kubeconfig: str; namespace: str = "default"              # Result: tuple[DeploymentInfo, ...]
@dataclass(frozen=True)
class KubeRestartDeployment: kubeconfig: str; deployment: str; namespace: str = "default"  # Result: str
@dataclass(frozen=True)
class KubeProbeRollout:                     # `kubectl rollout status --watch=false`; gate polls
    kubeconfig: str; deployment: str; namespace: str = "default"
# Result: RolloutState(complete: bool, message: str = "")  — replaces blocking wait_for_rollout
@dataclass(frozen=True)
class KubeGetEvents:       kubeconfig: str; namespace: str | None = None; limit: int = 100
# Result: tuple[EventInfo, ...] (sorted last_timestamp desc, then limited — salvaged)
@dataclass(frozen=True)
class KubeRolloutUndo:     kubeconfig: str; namespace: str = "default"
# Result: RolloutUndoResult(succeeded: int, failed: int, outputs: str, errors: str).
# Partial-success semantics preserved EXACTLY (crown jewel #13): undoes EVERY deployment in the
# namespace; success iff ≥1 undo succeeded, else PermanentError carrying aggregated errors.
@dataclass(frozen=True)
class KubeRun:             kubeconfig: str; args: tuple[str, ...]; timeout_s: float = 30.0; binary: bool = False
# Result: KubectlOutput(stdout: str | bytes, stderr: str) — bytes channel REQUIRED for
# pg_dump -Fc snapshot streaming (crown jewel #14). Exposed only via the reviewed `kubectl`
# step type, never via config strings.
@dataclass(frozen=True)
class KubeWatchPods:       kubeconfig: str; namespace: str = "default"; timeout_s: int = 300
# The one natively streaming command: each watch event ⇒ Progress(phase="pods.watch",
# data={"event": PodWatchEvent}). ALL v1 hardening salvaged: --output-watch-events framing,
# skip non-JSON/non-dict lines, 30s readline heartbeat, stderr harvest at stream end,
# terminate→kill in finally, CancelledError re-raised. Result: WatchEnded(reason: str).

MachineCommand = (CreateInstance | ProbeInstance | DestroyInstance | ProbeDestruction
                  | ListInstances | Reconcile | FetchKubeconfig)
K3sCommand     = ProbeSshPort | CaptureHostKeys | InstallK3s | ProbeK3s | FetchKubeconfig
KubectlCommand = (KubeGetClusterInfo | KubeGetNodes | KubeGetPods | KubeGetPodDetails
                  | KubeGetPodLogs | KubeApplyManifest | KubeDeleteManifest | KubeGetDeployments
                  | KubeRestartDeployment | KubeProbeRollout | KubeGetEvents | KubeRolloutUndo
                  | KubeRun | KubeWatchPods)
DigitalOceanCommand = ApplyFirewalls | AssignToProject   # DO-only, `supported`-set gated (DR-0022 P7/ruling 7)
ProviderCommand = MachineCommand | DigitalOceanCommand | K3sCommand | KubectlCommand
```

**`ApplyFirewalls`/`AssignToProject`** (DR-0022 ruling 7's doc debt, settled here — both are real, implemented
(`providers/contract.py:414,437`; `providers/digitalocean.py:127-128`) and back the shipped
`do.apply_firewalls`/`do.assign_project` verbs, but had no §5.3/§5.5 home until now):

```python
@dataclass(frozen=True)
class ApplyFirewalls:      resource_ids: Mapping[str, str]; spec: ClusterSpecification
# DigitalOcean-only. Ensures the management (SSH+K3s API) and application (HTTP/HTTPS)
# firewalls exist for the droplet's region (create-if-missing, shared/named-by-region) and
# attaches the droplet to both. Best-effort by WORKFLOW policy (on_failure: continue), not
# by this command swallowing — it raises normally on failure like any other command.
# NOT undoable: ensure-exists is itself idempotent and the firewall is a shared per-region
# resource, not owned by any one droplet. Result: None

@dataclass(frozen=True)
class AssignToProject:     resource_ids: Mapping[str, str]
# DigitalOcean-only. One late, best-effort project-assignment attempt, fired from its own
# workflow step positioned after K3s install (mirrors v1's post-install placement).
# NOT undoable: project membership is not itself infrastructure to tear down. Result: None
```

Both survive the DR-0022 re-normalization under P7 ("a vendor prefix is permitted ONLY for a
capability no other provider has, and requires a Seam C command plus `supported`-set gating") —
`do.apply_firewalls`/`do.assign_project` are the two verbs P7 names as surviving unchanged.

Salvaged DTOs (`PodInfo`, `PodDetails`, `NodeInfo`, `DeploymentInfo`, `EventInfo`, `PodWatchEvent`, `_format_age`) copy verbatim to `seedpod/providers/kube_types.py`. The salvaged intent dataclasses stay in `seedpod/core/reconciliation_intents.py`.

## 5.4 The Provider protocol + provider registry

```python
@runtime_checkable
class Provider(Protocol):
    name: str                               # "digitalocean" | "kind" | "tart" | "orbstack" | "ssh-k3s" | "kubectl"
    supported: frozenset[type]              # command classes this provider accepts

    async def check_ready(self) -> None:
        """Startup preflight, called once by the composition root before serving: binary on
        PATH, tart base image present, docker up, token present. Raises PermanentError
        (NOT_FOUND/AUTH/INVALID_INPUT) or InfrastructureUnreachableError. Replaces v1's
        sync-subprocess-in-__init__ checks: fail at startup, not mid-provision."""

    def execute(self, cmd: ProviderCommand) -> AsyncIterator[ProviderEvent]:
        """Stateless. All context in cmd (kubeconfig passed in, never fetched — H18 closed).
        No DB, no state_manager, no scheduler, no retry loop, no poll loop, no backoff sleep —
        one bounded attempt (engine owns Schedule: H4–H6 closed). Streams per §5.2.
        Unsupported command ⇒ PermanentError(UNSUPPORTED) immediately, no backend traffic."""
```

**Construction contract** (enforced by conformance, not the Protocol): `__init__(config, transport)` is IO-free — it stores config and an **injected transport**: a `SubprocessRunner` (wraps salvaged `create_tracked_subprocess`) or a shared `httpx.AsyncClient`. Fault injection for the conformance suite sits at the transport seam — never `Mock`/`patch`. Global singletons (`get_kubernetes_provider`, `get_cloudflare_dns_provider`) are deleted; explicit construction only.

**Plane matrix:** `digitalocean`/`tart` ⇒ machine plane minus FetchKubeconfig · `kind`/`orbstack` ⇒ machine plane incl. FetchKubeconfig · `ssh-k3s` ⇒ k3s plane · `kubectl` ⇒ kubernetes plane. kind/orbstack provision workflows simply **omit** the ssh-k3s steps — conditionals dissolve into presence/absence of workflow config, per the grammar rule. Traefik parity shims leave provider code entirely: they become `kubectl-apply` workflow steps over the copied `traefik-kind.yaml`/`traefik-orbstack.yaml` with a **non-fatal** `KubeProbeRollout` gate (`on_failure: warn` — crown jewel #10 preserved as workflow policy).

**Temp files** — `seedpod/core/tempfiles.py` `TempFileRegistry`: every temp file (kubeconfig, manifest, known_hosts, kind config) is created `0600` under a registry dir (`$XDG_RUNTIME_DIR/seedpod/` or `~/.seedpod/tmp/`), registered, unlinked on completion or cancellation, and stale entries are swept at startup. Fixes H17 including the `apply_manifest` two-file leak ordering.

**Physics constants** (30s DO post-active warmup, 5s sshd settle, 10s k3s interval, 2s tart IP interval, DO project-assign settle) become named `interval`/`settle_seconds` parameters on the workflow's `wait-for-readiness` gates and DO's create config — preserved as data, deleted as sleeps (crown jewel #17).

**Supporting services — NOT Providers (per plan §Pillar 3):** `seedpod/services/ghcr.py` (`GhcrService`) and `seedpod/services/dns.py` (`DnsService`). Same three error classes (table rows 32–38), invoked through the engine's step machinery (`resolve-images` / `resolve-dns` verbs) so `Schedule` retry applies identically. GHCR crown jewels salvaged byte-for-byte: `/`→`-` normalization, `^{branch}-[a-f0-9]+$` newest-first, `updated_at` tiebreak, mutable→immutable digest re-resolution via the version-name-is-the-sha256 quirk, `dev/main/master` fallback chain, per-repo failure ⇒ `None` never aborting the batch, 404 ⇒ `[]`/`None` as data; no-pagination-past-100 limitation documented. Cloudflare: upsert (GET→PUT-or-POST), 404-delete ⇒ `existed=False`, `{name}.{zone}` suffixing, `subdomain_pattern` default `"{cluster_slug}"` — all salvaged; shared client with explicit timeout replaces per-call clients. `DnsService.upsert_record` returns `DnsRecordUpserted(record_id, fqdn, zone, created: bool)`; the id dict is persisted for the delete path exactly as in v1. Dead code not copied: GHCR `_repository_cache`, no-op `_rate_limit`.

## 5.5 Compensation mapping — `seedpod/providers/compensation.py`

A pure module-level function the engine consults when pushing a step's undo onto the Scope. **It takes `Observed` (the folded stream, §5.2), not the terminal result** — so a create that died mid-stream after `RESOURCE_ALLOCATED` still yields a real destroy. This is the structural C1 close.

```python
def undo_for(cmd: ProviderCommand, observed: Observed) -> ProviderCommand | None:
    match cmd:
        case CreateInstance(slug=slug):
            ids = (observed.value.resource_ids if observed.value is not None
                   else observed.data.get("resource_ids"))
            if ids:
                return DestroyInstance(slug=slug, resource_ids=dict(ids))
            return None      # ids never escaped ⇒ nothing was allocated OR tag-before-boot +
                             # reconciliation (Zombie/CreateUnmanaged next cycle) is the backstop
        case KubeApplyManifest(kubeconfig=kc, manifest_yaml=y):
            return KubeDeleteManifest(kubeconfig=kc, manifest_yaml=y, ignore_not_found=True)
        case _:
            return None
```

| Command | Inverse | Note |
|---|---|---|
| `CreateInstance` | `DestroyInstance(resource_ids)` — ids from the Result **or** the `RESOURCE_ALLOCATED` progress data | closes **C1** + the tart VM leak, incl. mid-create death |
| `KubeApplyManifest` | `KubeDeleteManifest(same yaml, ignore_not_found)` | literal inverse — reachable ONLY via `kube.apply_file` (the infra-shim verb, e.g. Traefik), which is `undoable=True` at the registry. **`kube.apply_docs`** (the same `KubeApplyManifest` command, but deploy waves' verb) is declared **`undoable=False` at the registry** (DR-0022 ruling 3, D1's fix) — `ProviderStep.undo` (and therefore this function) is never even called for it, making the regression this row once depended on a `on_failure:` YAML key to avoid **structurally unrepresentable** instead. Deploy-cancel rollback runs as its own machine-decided workflow (`deploy-rollback.yml`, `KubeRolloutUndo`, ≥1-success semantics), not this function's inverse of the deploy step. |
| `DnsService.upsert_record` | `delete_record(zone, record_id)` **iff `created=True`**, else none | never delete a record we merely updated (P2 graft — improves on v1) |
| `CaptureHostKeys` / `InstallK3s` / `FetchKubeconfig` | none | subsumed by the instance undo |
| `DestroyInstance` / `KubeDeleteManifest` / DNS delete | none | destruction IS compensation; never auto-undone |
| `ApplyFirewalls` / `AssignToProject` | none | DO-only (DR-0022 ruling 7); ensure-exists / late best-effort assignment, neither owns a resource to tear down |
| all `Probe*` / `Get*` / `List*` / `Watch*` / `Reconcile` / `KubeRestartDeployment` / `KubeRolloutUndo` / `KubeRun` | none | reads / already-compensating / escape hatch |

**Undo laws (pinned):** every undo command is idempotent and absent-tolerant (`DESTROYED + already-absent note`, `existed=False`, `ignore_not_found` are all success); undos run in reverse completion order; `TransientError` during undo retries on the step's Schedule; `PermanentError` during undo is recorded and reconciliation inherits the leak (the backstop the plan keeps); **`InfrastructureUnreachableError` never starts compensation** (§5.1 table). v1's `retain_on_failure: true` survives as a workflow-level skip-compensation debug flag, not provider code.

## 5.6 Shared conformance suite — `tests/conformance/`

Parametrized over all six providers via a per-provider `Harness`; fault injection at the injected transport (canned frames mined from v1 behavior). kind/tart/orbstack additionally run under a `live` pytest marker. Supporting services get the applicable subset (marked ⊂). Each provider registers a **capability skip list** for structurally inapplicable cases (e.g. orbstack's no-op destroy); the skip list is reviewed like a verb addition.

```python
class Fault(StrEnum):
    UNREACHABLE = "unreachable"; TRANSIENT_ONCE = "transient-once"; AUTH = "auth"
    MISSING_SOURCE = "missing-source"; RATE_LIMIT = "rate-limit"; DIE_MID_CREATE = "die-mid-create"

class Harness(Protocol):
    name: str
    def provider(self, *faults: Fault) -> Provider: ...          # fake-transport-backed
    def broken_environment(self) -> AbstractContextManager: ...  # for check_ready
    async def backend_resources(self) -> frozenset[str]: ...     # raw backend truth (leak check)
    def backend_attempts(self) -> int: ...                       # transport call counter
    def create_command(self) -> CreateInstance: ...
    def observe_command(self) -> ProviderCommand: ...            # cheapest state read
    def reconcile_truth_table(self) -> Sequence[ReconcileCase]: ...
    def rewrite_cases(self) -> Sequence[tuple[str, FetchKubeconfig, str]]: ...
    def classification_cases(self) -> Sequence[tuple[Fault, type[ProviderError], ErrorCode]]: ...
```

| ID | Test | Applies | Asserts |
|---|---|---|---|
| C-01 | `test_check_ready_fails_fast` | all + ⊂ | broken environment (missing binary / base image / token) ⇒ `check_ready` raises Permanent or Unreachable before any command runs |
| C-02 | `test_stream_shape` | all | every supported command: ≥0 Progress then exactly one Result, nothing after; errors raised never yielded |
| C-03 | `test_stateless_no_upward_imports` | all + ⊂ | static: provider/service modules import nothing from `seedpod.data`, `core.database`, session providers, scheduler, or state manager (H18 by construction); same command on two fresh instances behaves identically (host keys travel in commands, not caches) |
| C-04 | `test_unreachable_raises` | machine+kubectl | injected control-plane outage on any state-determining call ⇒ `InfrastructureUnreachableError` with `host` set — never Permanent, never an absent-looking Result |
| C-05 | `test_absence_is_data` | all + ⊂ | reachable backend, nonexistent thing ⇒ typed Result (`phase="absent"`, `found=False`, `ready=False`, `[]`, `existed=False`), no exception |
| C-06 | `test_absent_vs_unreachable_never_conflated` | machine | authoritative absence (docker rc≠0, DO empty list w/ 200) ⇒ `absent`; connectivity failure ⇒ raise — parametrized over both, asserting they diverge (crown jewel #1) |
| C-07 | `test_create_idempotent_reinvocation` | machine | `CreateInstance` twice with same `cluster_uuid` ⇒ second returns same `resource_ids`, `adopted_existing=True`, no duplicate backend resource |
| C-08 | `test_create_emits_resource_allocated_and_tags_before_boot` | machine | `RESOURCE_ALLOCATED` progress precedes any readiness activity and equals terminal ids; killing create right after allocation still leaves the cluster-uuid tag/name on the backend resource |
| C-09 | `test_undo_after_partial_create` | machine | `DIE_MID_CREATE` ⇒ stream raises; `undo_for(cmd, fold(events))` returns `DestroyInstance`; executing it on a fresh provider ⇒ backend clean (**the C1 test**) |
| C-10 | `test_destroy_idempotent_on_absent` | machine + ⊂DNS | destroy of absent resource ⇒ `DESTROYED` + note / `existed=False`; twice ⇒ succeeds twice |
| C-11 | `test_destroy_never_lies_when_unreachable` | machine | injected API timeout during destroy ⇒ raise Unreachable; **never** `DESTROYED` (v1 `api_call_succeeded`) |
| C-12 | `test_probe_destruction_vocabulary` | machine | in-progress ⇒ `DESTROYING`; stuck-active ⇒ `DESTROY_FAILED` + `stuck_resources`; gone ⇒ `DESTROYED`; transient ⇒ raise Unreachable |
| C-13 | `test_reconcile_intent_matrix` | machine | parametrized (db_status × backend reality): active+missing⇒Orphan; active+stopped⇒Orphan (kind/tart); DESTROYED+present⇒Zombie; DESTROYING+missing⇒Orphan(completion); no-uuid-tag⇒skipped; uuid-tag-no-DB-row⇒CreateUnmanaged (DO) |
| C-14 | `test_reconcile_unreachable_touches_nothing` | machine | injected outage ⇒ raises Unreachable, zero intents produced, zero backend mutations |
| C-15 | `test_single_attempt_no_internal_retry` | all + ⊂ | `TRANSIENT_ONCE` fault ⇒ exactly one transport attempt then TransientError; wall time shows no internal sleep; second execution succeeds (H4–H6) |
| C-16 | `test_probes_are_one_iteration` | machine+k3s+kubectl | `ProbeInstance`/`ProbeK3s`/`KubeProbeRollout` return not-ready promptly; they never block until ready |
| C-17 | `test_error_classification_table` | all + ⊂ | each harness `classification_cases()` row: injected fault ⇒ expected (class, code); envelope complete (`code`/`provider`/`command` set, raw stderr/status only in `detail`) |
| C-18 | `test_kubeconfig_is_parameter` | kubectl | every kubectl command works given only `kubeconfig=` against a fake transport, no env/DB access; garbage kubeconfig ⇒ Permanent(`AUTH`\|`INVALID_INPUT`); no "kubeconfig_not_found" string anywhere |
| C-19 | `test_kubeconfig_rewrite_variants` | machine+k3s | golden tests: kind matches `127.0.0.1\|localhost\|0.0.0.0` and substitutes host **and** port; orbstack preserves source port; ssh variant preserves scheme+port, count=1 per entry |
| C-20 | `test_tofu_ordering` | ssh-k3s | fake sshd asserts `cloud-init status --wait` (the sole non-strict call) precedes keyscan; `InstallK3s(known_hosts="")` ⇒ Permanent(`INVALID_INPUT`); all post-capture SSH uses strict checking |
| C-21 | `test_tempfile_hygiene` | all that touch disk | temp files live under the registry dir, mode `0600`; after completion the dir is empty; simulated hard-kill + startup sweep removes strays (H17) |
| C-22 | `test_cancellation_cleanup` | all | cancelling mid-execute (`KubeWatchPods`, `InstallK3s`) ⇒ tracked subprocess terminated, temp files unlinked, `CancelledError` re-raised |
| C-23 | `test_undo_mapping_total_and_idempotent` | all | `undo_for` returns `None` or an in-union command for every supported command; every returned undo executed twice succeeds twice |
| C-24 | `test_unsupported_command_rejected` | all | command outside `supported` ⇒ Permanent(`UNSUPPORTED`) with zero backend traffic |

Mined-not-ported: intent expectations from `reference-code/tests/unit/test_tart_provider.py` feed C-13; destroy-vocabulary expectations feed C-12; the `Mock`/`patch` scaffolding stays in `reference-code/`.

## 5.7 Surfaced loudly: removals, non-fits, and genuinely new code

1. **`cleanup_expired_clusters` is removed from the seam.** TTL expiry is a lifecycle *decision* ⇒ Pillar 1: `ScheduleTimer(after=ttl)` effect → `Expired` event → destroy workflow. The `ttl-{h}` tag is still written by `CreateInstance` (informational); no v2 code parses it for expiry. This is the only v1 provider capability with no v2 command.
2. **Provider self-scheduling and `state_manager` upward calls are structurally impossible** (C-03). `ProvisioningEvent.DROPLET_READY/K3S_INSTALLED` survive only as workflow-step boundaries + `Progress` phases the engine translates into Pillar-1 events.
3. **New provider obligations that are not salvage** (need real review, not silent invention): `CreateInstance` re-invocation safety / adoption-by-tag (C-07 — v1 never retried creates; tart `AlreadyExists` and DO adopt-by-`cluster-uuid` tag are new code); `KubeDeleteManifest` (v1 had no manifest inverse); the `RESOURCE_ALLOCATED` early-emit in each create path.
4. **v1 bugs deliberately not pinned:** DO `get_cluster_status` NameError; triple project-assignment (collapses to one step); reconcile swallow-to-`success([])`; kind `list_clusters` swallow-to-`[]` on unreachable (violates unreachable≠absent — now raises); GHCR dead cache / no-op rate limit; `_get_or_create_ssh_key` that never creates; DO/tart droplet-leak-on-failure (the C1 class itself).
5. **Tart specifics carried intact inside the adapter:** detached `run --no-graphics --rosetta=rosetta` (`start_new_session=True`, DEVNULL stdio, never awaited), the virtio-fs `rosetta` magic tag, the 6.8-kernel/`AT_HWCAP3` pin documented on the impl; `_tart_cli` typed errors map at the edge (`TartDaemonUnreachable`→Unreachable, `TartNotFound`→absence-as-data, `TartAlreadyExists`→C-07 adoption).

---

## Taste calls for the human

1. **Chose `InfrastructureUnreachableError` as a sibling leaf over a `TransientError` subclass** (P2/P3's choice) because its engine consequence — skip/park, *never* compensate, absence-of-answer ≠ absence — must be un-conflatable with ordinary retry: a subclass lets a generic retry policy exhaust its budget and fall through to `RETRY_EXHAUSTED → Permanent → undo` on a network blip, which is exactly the mass-false-orphaning regression the v1 docstring exists to prevent. Cost: the engine needs a third named branch. Flip if you'd rather have two-branch engine policy and trust every handler to special-case the subclass.

2. **Chose single-shot `KubeProbeRollout` (engine gate polls) over v1's blocking `kubectl rollout status --timeout`** (P3's choice) because the contract's one uniform law — no command waits, all waiting is an engine gate — is worth more than reusing one battle-tested blocking call; the underlying `--watch=false` invocation is trivial. Flip if you value keeping the v1 subprocess shape byte-for-byte and accept one waiting command as an exception.

3. **Chose `undo_for(KubeApplyManifest) = KubeDeleteManifest` with `RolloutUndo` as an explicit workflow-declared compensator for deploy waves, over pinning `RolloutUndo` as apply's command-level inverse** (P3's choice) because the command table should state literal inverses while the deploy-cancel semantics (undo *all* deployments, ≥1-success) is a workflow policy — but this makes preserving v1's cancel behavior depend on the deploy workflow config declaring its override. Flip if you'd rather hard-wire the v1 cancel semantics at the seam and accept that Traefik-shim applies then have no clean inverse.
