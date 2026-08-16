---
title: Seam B — Workflow engine (Pillar 2)
type: design
status: active
created: 2026-07-12
updated: 2026-07-20
amended-by: coherence-review.md   # Conflicts 2, 4–10, 12–14 override where they touch this spec
# Also amended by DR-0022 (verb vocabulary): Proofs 1-3's `uses:` verb names were renamed in place
# per ruling 8 + Erratum E9; Erratum E3 corrected Proof 3's Tailscale namespace (kube-system ->
# default). The Proofs remain superseded on other points by coherence-review Conflicts 8/9/10/14.
---

# Seam B — Final Specification: Workflow Engine (Pillar 2)

## 1. Verdicts

**Proposal 1 (Minimal-Mechanism)** has the best grammar skeleton: `foreach` over a planned wave list handles variable wave counts without templating, the per-step `emit:` key is the only proposal that preserves v1's mid-run persistence of droplet_id/IP onto the cluster record (a real UI/reconciliation behavior, not cosmetics), and its step-success-plus-event single transaction extends H7 discipline to steps. Its subprocess-group cancellation is the cleanest H16 closure. Two weaknesses: readiness gates as hand-rolled verb loops re-scatters the five wait loops v1 had and leaves the poll-failure hysteresis (gotcha 3) to per-verb discipline — a silent-regression vector; and crash-mid-step handling (a bare `idempotent` bool) leaves the C1 window between "API returned droplet_id" and "step row committed" covered only by tag-lookup convention.

**Proposal 2 (Resume-First)** has the most rigorous crash calculus — DB-serialized cancel-vs-attempt-start, interrupted attempts exempted from the retry budget with a separate replay limit, and the only proposal that specifies the reconciliation interaction properly (resume as a priority-0 intent; Orphan/Zombie intents suppressed for clusters with a live run). Its named retry policies encode v1's tuned values once. Fatal excess: leases/owners for a single-process system, an append-only attempt table, and the `EffectClass`+`probe()` triple — three mechanisms where one non-idempotent verb exists. Its grammar also bends visibly: two statically-named wave blocks to sneak snapshot-restore between them, and no per-step `on_failure: continue`, which forces v1's warn-and-continue edges into inconsistent verb-internal downgrades (its own `harden` step argues with itself).

**Proposal 3 (Grammar-First)** contributes the two best sub-ideas: `gate:` as an engine-owned construct (interval, timeout, consecutive-poll-failure hysteresis, and cancel-between-polls implemented exactly once — this is what makes gotcha 3 structural) and `ctx.note()`, the smallest durable closure of C1's created-but-unrecorded window. Its written-out destroy workflow proves the two-phase initiate-then-observe destroy (gotcha 9) fits the grammar, and "undo runs on a fresh non-tripped token" is a detail the others missed. Fatal flaw: static `apply_wave_0..3` steps freeze `deploy_wave` to an ordinal domain 0–3, silently regressing PLAN-wave's open `deploy_wave: N` and turning "add a wave" into a workflow-file edit; per-resource `each` fan-out also multiplies gate loops needlessly.

**Synthesis:** P1's skeleton (grammar, tables, transactions, emit, subprocess cancellation) + P3's `gate:` and `ctx.note()` and undo-token rule + P2's resume/reconciliation calculus, named policies, and DB-serialized cancel.

---

## 2. THE FINAL SPEC

### 2.0 Concepts (all of them)

**Step (verb)** · **Run** · **Cursor** (one persisted row per step instance) · **Schedule** (retry policy) · **Gate** (engine-owned readiness poll) · **Undo** (compensation) · **CancelToken** · **Note** (durable write-ahead scratchpad). The engine is a plain-asyncio executor (one task per active run, in a TaskGroup the engine owns) over two Postgres tables. APScheduler is retired for workflow execution; `ScheduleTimer` (TTL) remains a Pillar-1 effect outside this seam. v2 is single-process: no leases, no owners; the DB `cancel_requested` flag and the run tables are the multi-process extension points.

Two step families share one contract: **provider steps** (wrap Pillar-3 `Provider` IO; stateless, all context in params — H18 stays closed) and **domain steps** (engine-side, may use injected repositories, e.g. loading a deployment audit or decrypting a kubeconfig). Steps are constructed with explicit DI at registry build time; no globals.

**Pillar-1 seam:** Pillar 1 emits `RunWorkflow(workflow, cluster_id, args)` and `CancelWorkflow(cluster_id)` effects; the engine returns exactly one terminal event per run (`outcome.succeeded/failed/cancelled`) plus any per-step `emit:` events, all via the effects outbox.

### 2.1 Decision 3 — The Step contract

```python
# engine/errors.py — taxonomy is defined in core (shared with Pillar 3); re-exported here
class TransientError(Exception): ...     # engine retries per Schedule (H4–H6)
class PermanentError(Exception): ...     # engine fails the step immediately
class StepCancelled(Exception): ...      # raised by ctx primitives when the token trips; never retried
# InfrastructureUnreachableError is treated as Transient inside steps/gates;
# its reconciliation semantics (zero intents) are untouched.

# engine/cancel.py
class CancelToken:
    """Cooperative cancellation. Trip is one-way, idempotent, level-triggered."""
    def trip(self) -> None: ...
    @property
    def tripped(self) -> bool: ...
    def raise_if_cancelled(self) -> None: ...   # raises StepCancelled if tripped
    async def wait(self) -> None: ...           # resolves when tripped; for select-style races

# engine/step.py
from pydantic import BaseModel, SecretStr

class StepContext:
    run_id: uuid.UUID
    cluster_id: str
    workflow: str
    step_path: str          # materialized cursor path: 'create' | 'wave[1].apply'
    attempt: int            # 1-based, read from the DB — survives restart
    cancel: CancelToken
    services: StepServices  # injected: repositories, SecretManager, provider registry,
                            # salvaged SubprocessManager — never globals

    async def note(self, **facts: str) -> None:
        """Durable write-ahead scratchpad, committed to workflow_steps.notes BEFORE
        returning. e.g. provider.create_server notes server_id the instant the API
        responds, so undo/resume can find the resource even if execute never
        completes (closes the C1 window structurally)."""

    async def progress(self, message: str, /, **fields: JsonValue) -> None:
        """Writes Notify(topic='workflow_progress') to the effects outbox with payload
        {run_id, cluster_id, workflow, step_path, attempt, message, **fields}.
        Never raises to the step; never touches the cursor. Replaces per-job SSE
        and _job_wrapper's 36-char-arg scanning (gotcha 15)."""

    async def sleep(self, seconds: float) -> None:
        """Cancellation-aware sleep: raises StepCancelled if the token trips.
        All in-step waits MUST use this, never asyncio.sleep."""

    async def run_subprocess(
        self, argv: list[str], *, stdin: bytes | None = None,
        env: dict[str, str] | None = None, timeout: float | None = None,
    ) -> ExecResult:
        """THE only way steps spawn kubectl/ssh. Runs argv in its own process group,
        registered with the salvaged subprocess_manager. If the token trips:
        SIGTERM the group within ~1s, SIGKILL after a 10s grace, raise StepCancelled.
        This is the structural H16 fix: a step cannot spawn an uninterruptible
        subprocess because this is the only subprocess API it has."""

class NotReady(BaseModel):
    detail: str = ""

class Ready(BaseModel, Generic[O]):
    outputs: O | None = None    # if set, REPLACES the step's persisted outputs
                                # (e.g. droplet gate enriches with public_ip)

P = TypeVar("P", bound=BaseModel); O = TypeVar("O", bound=BaseModel)

class Step(ABC, Generic[P, O]):
    verb: ClassVar[str]                        # registry key, e.g. "kube.apply_docs"
    Params: ClassVar[type[BaseModel]]          # validates YAML with:-bindings at load & run time
    Output: ClassVar[type[BaseModel]]          # EmptyOutput if none; SecretStr fields encrypted
    idempotent: ClassVar[bool] = True          # governs crash-mid-step resume (§2.3.4)
    gateable: ClassVar[bool] = False           # may carry a gate: block in YAML
    undoable: ClassVar[bool] = False           # participates in compensation
    default_retry: ClassVar[Schedule] = NAMED_POLICIES["none"]
    default_timeout_seconds: ClassVar[int] = 300

    @abstractmethod
    async def execute(self, params: P, ctx: StepContext) -> O:
        """One attempt. Engine enforces the per-attempt timeout (asyncio.timeout) and
        the Schedule — do not self-timeout, do not self-retry. Raise TransientError
        to request retry, PermanentError to fail; any other exception ≡ Permanent."""

    async def poll_ready(self, params: P, provisional: O, ctx: StepContext) -> Ready[O] | NotReady:
        """gateable verbs only. ONE cheap idempotent probe; the engine owns the loop,
        interval, overall timeout, transient-failure hysteresis, and cancel checks
        between polls. May raise PermanentError for definitive failure (a K8s Job
        with condition=Failed). May call ctx.progress per poll."""

    async def undo(self, params: P, output: O | None, notes: Mapping[str, str],
                   ctx: StepContext) -> None:
        """undoable verbs only. output is None if execute never succeeded (partial
        external effect possible) — undo must then work from notes and/or the
        cluster-uuid tag. MUST be idempotent, tolerate 'already gone', use
        check_enabled=False semantics on provider calls (gotcha 1), and never
        enqueue new runs. Runs on a FRESH non-tripped token."""
```

**Typed named bindings — how outputs flow.** On step success the engine validates the return value against `Output` and persists `Output.model_dump()` in `workflow_steps.output` (fields typed `SecretStr` are Fernet-encrypted via the salvaged `SecretManager` and redacted from all events). To start a later step, the engine builds its params from `with:` — literals plus each `{from: path}` binding resolved by reading the **persisted** output rows (never in-memory state, so resume gets byte-identical inputs), then validates via `Params(**merged)`. Type compatibility is proven at config-load time (§2.2 rules), so runtime resolution cannot produce a type surprise. `{from: ...}` is only legal as the *entire* value of a param — string interpolation does not exist.

### 2.2 Decision 4 — The workflow config grammar (frozen)

The grammar is **sequence + foreach + gate + typed bindings**. Mapping to the plan's named grammar: *sequence* = `steps`; *readiness gate* = `gate:`; *fan-out-within-a-wave* is realized as **data**, exactly as PLAN-wave specifies — a wave's docs go down in one `kube.apply_docs`, and one gate poll checks all of the wave's resources concurrently (join at the gate). No grammar-level parallelism exists. Conditionals dissolve into data (Optional params ⇒ typed no-op; workflow *selection* at dispatch); loops dissolve into `foreach`.

```yaml
# ---- GRAMMAR (a validator can be written from this block alone) ----------
workflow: <ident>                    # unique name
version:  <positive int>             # bumped on any edit; runs pin the version they started on
inputs:                              # typed run args, supplied by the RunWorkflow effect
  <name>: {type: <registered pydantic model or scalar>, secret: <bool, default false>}
on_failure: compensate | report      # workflow-wide: does failure/cancel trigger the undo pass
outcome:                             # exactly one fires per run, via the effects outbox
  succeeded: {event: <Pillar-1 Event>, payload: {<k>: <Value>}?}
  failed:    {event: <...>}          # engine auto-attaches error, failed_step, undo_incomplete
  cancelled: {event: <...>}
steps: [<Entry>+]                    # strictly ordered, executed top to bottom

Entry := StepDef | ForeachDef
StepDef:
  id: <ident, unique within its scope>
  uses: <verb in the registry>
  with: {<param>: <Value>}           # must satisfy the verb's Params exactly (no extras)
  retry: <policy name> | {max_attempts, base_delay_seconds, factor, max_delay_seconds}
                                     # default: the verb's default_retry
  timeout_seconds: <positive int>    # per-ATTEMPT bound; default: verb's default_timeout_seconds
  gate:                              # OPTIONAL; verb must declare gateable
    timeout_seconds: <int> | <Ref>   # overall gate bound (Ref must type-check to int)
    interval_seconds: <int, default 5>
    max_consecutive_poll_failures: <int, default 3>   # transient hysteresis (gotcha 3)
  on_failure: abort | continue       # default abort; continue = record 'failed_continued',
                                     #   proceed (v1 best-effort: firewalls, cleanup phases)
  emit: {event: <Pillar-1 Event>, payload: {<k>: <Value>}}   # posted on step success,
                                     #   in the SAME transaction as the success row
ForeachDef:
  id: <ident>
  foreach: {items: <Ref to a list[T] output>, as: <ident>}
  body: [<StepDef>+]                 # StepDefs ONLY — no nesting. Iterations run
                                     #   SEQUENTIALLY in list order.
Value := <YAML literal (scalar/list/map, no Ref inside)> | <Ref>
Ref   := {from: <path>}
path  := run.<input>(.<field>)*                    # workflow input
       | <step_id>.<field>(.<field>)*              # earlier step, same or enclosing scope
       | <as>(.<field>)*                           # current foreach item, body only
```

**Validator rules (exhaustive):** V1 every `uses` names a registered verb; V2 `with` keys exactly satisfy the verb's `Params` (required params present, no unknown keys); V3 every Ref resolves to a lexically earlier step in the same or an enclosing scope, or to `run.*` / the loop alias — steps outside a foreach may not reference steps inside it; V4 field paths are checked against the source `Output` model (or input type / list element type) and the resolved type must be assignable to the target `Params` field annotation (`Optional[T]` sources bind only `Optional[T]` params); V5 `foreach.items` must type-check to `list[T]`; V6 `gate:` only on `gateable` verbs; V7 `undoable` is a verb property, not YAML — `on_failure: continue` steps are never compensated; V8 `outcome.*.event` and `emit.event` must be members of the Pillar-1 Event enum; `outcome` payload Refs may reference top-level scope only; V9 ids unique per scope; retry values positive and bounded; V10 unknown keys anywhere = load error, and any scalar containing `${` anywhere in the file is a hard error. There is no `if`, `when`, `for`, `env`, `run:`, or templating. **The grammar is hereby frozen**; only verbs grow.

#### Proof 1 — `workflows/deploy-waves.yml` (PLAN-wave-orchestration, in full)

```yaml
workflow: deploy-waves
version: 1
inputs:
  deployment_id: {type: str}
on_failure: report                    # PLAN-wave: abort-and-report; NO rollback of prior waves
outcome:
  succeeded: {event: DeploySucceeded, payload: {deployment_id: {from: run.deployment_id}}}
  failed:    {event: DeployFailed,    payload: {deployment_id: {from: run.deployment_id}}}
  # DeployFailed drives the deployment machine -> failed; cluster stays ACTIVE
  # (v1 gotcha 8's intent, now clean via the separate deployment machine)
  cancelled: {event: DeployCancelled, payload: {deployment_id: {from: run.deployment_id}}}
steps:
  - id: audit                         # domain step: loads DeploymentAudit via repository;
    uses: deploy.load_audit           # tolerates str/dict resolved_manifests (gotcha 12)
    with: {deployment_id: {from: run.deployment_id}}
    # Output: manifests: list[ManifestDoc], profile: DeploymentProfile,
    #         rollout_timeout_seconds: int

  - id: kubecfg                       # domain step: decrypts kubeconfig from DB
    uses: cluster.load_kubeconfig     # (providers never fetch it — H18)
    # Output: kubeconfig: SecretStr

  - id: preflight                     # v1 connectivity pre-check, now retried (H6)
    uses: kube.cluster_info
    with: {kubeconfig: {from: kubecfg.kubeconfig}}
    retry: kubectl_default
    timeout_seconds: 30

  - id: plan                          # pure split: metadata.name/app-label -> service ->
    uses: deploy.plan_waves           # deploy_wave (default 3 = back-compat single apply);
    with:                             # unmatched docs -> wave 0 (RBAC/ConfigMaps/Secrets/
      manifests: {from: audit.manifests}              # ghcr-secret FIRST — see DR-0029)
      profile: {from: audit.profile}
      rollout_timeout_seconds: {from: audit.rollout_timeout_seconds}
    timeout_seconds: 30
    # Output: waves: list[Wave]
    #   Wave{index:int, docs:list[ManifestDoc], jobs:list[str], deployments:list[str],
    #        gate_timeout_seconds:int, restore: SnapshotRestoreSpec|None}
    #   `restore` attached to the persistence wave only when the profile declares
    #   data_initialization — v1's phased DB-first deploy, as data, one loop.

  - id: wave
    foreach: {items: {from: plan.waves}, as: w}
    body:
      - id: prep                      # delete immutable Jobs (--ignore-not-found) +
        uses: deploy.prepare_wave      # force-delete CrashLoop/Init: pods
        with: {kubeconfig: {from: kubecfg.kubeconfig}, jobs: {from: w.jobs}}
        on_failure: continue          # best-effort, as in v1 (gotcha 5)

      - id: apply                     # fan-out-within-the-wave = one apply of all docs
        uses: kube.apply_docs
        with: {kubeconfig: {from: kubecfg.kubeconfig}, docs: {from: w.docs}}
        retry: kubectl_default
        timeout_seconds: 300
        # Output: changes: ApplyChangeSummary   # configured/created/unchanged per
        #                                       # resource; unknown => assume changed

      - id: restore                   # typed no-op when spec is None (conditional-as-data)
        uses: deploy.restore_snapshot
        with: {kubeconfig: {from: kubecfg.kubeconfig}, spec: {from: w.restore}}
        timeout_seconds: 600

      - id: restart                   # unchanged-vs-configured heuristic, verbatim salvage
        uses: deploy.ensure_rollouts    # (deployment_job.py:609-636, gotcha 4)
        with:
          kubeconfig: {from: kubecfg.kubeconfig}
          deployments: {from: w.deployments}
          changes: {from: apply.changes}
        retry: kubectl_default
        timeout_seconds: 60

      - id: ready                     # THE readiness gate; execute is a no-op returning
        uses: deploy.await_wave      # provisional output; each poll_ready probes ALL of
        with:                         # the wave's resources once (Deployments: rollout
          kubeconfig: {from: kubecfg.kubeconfig}      # status --watch=false; Jobs:
          deployments: {from: w.deployments}          # condition=complete — Failed =>
          jobs: {from: w.jobs}                        # PermanentError naming wave+resource)
        gate:
          timeout_seconds: {from: w.gate_timeout_seconds}
          interval_seconds: 5
        # poll_ready emits ctx.progress per poll with per-resource status
        # (replaces the bespoke watch_pods SSE task)
```

No step in this file is `undoable`; `on_failure: report` means any failure records `failed_step` (e.g. `wave[2].ready`) and the failing resources — that IS "abort and report which wave/service failed". Marking the deployment active / superseding previous actives is Pillar 1's response to `DeploySucceeded`, not a step.

##### LOUD callouts — v1 deploy-path behaviors deliberately NOT ported (§J's format, this seam's own list)

1. **v1's parse-error fail-open** (`reference-code/seedpod/seedpod/jobs/state/deployment_job.py:127-129`, `_split_manifests_by_service`'s `except yaml.YAMLError: ...; return "", rendered_manifests`): a malformed rendered manifest silently downgrades to "no database manifests, apply everything as one wave, skip the restore phase entirely", logged but never surfaced to the caller. DR-0028's Consequences flagged this as a candidate not-ported and required a deliberate, loud call either way (`docs/decisions/DR-0028-deploy-path-dtos.md`). NOT ported: `seedpod/core/deploy_wave.py`'s `parse_manifest_documents` — the ONE place a rendered manifest's raw YAML text is parsed in v2's redesigned pipeline — raises a `PermanentError(ErrorCode.INVALID_INPUT)` instead of falling open, matching this codebase's one error-taxonomy home (CLAUDE.md).
2. **A restore requested against a profile with no persistence services** (`deployment_job.py:530`'s compound `if data_initialization and database_services:` gate): v1 silently drops the requested restore and runs an unsplit apply with no error. NOT ported (DR-0028 Erratum E2): `deploy.plan_waves` raises a `PermanentError` naming the mismatch instead — see `seedpod/engine/steps/deploy.py`'s `_restore_requested_without_persistence_service`.

#### Proof 2 — `workflows/provision-digitalocean.yml`

Per-provider provisioning is a per-provider *file* (Tart gets a sibling with its own verbs and its tuned timeouts) — provider choice is data at dispatch, not grammar. All timeout literals are v1's tuned values from `config/providers/digitalocean.yml`, imported not re-guessed.

```yaml
workflow: provision-digitalocean
version: 1
inputs:
  spec: {type: ClusterSpecification}          # carries CIDRs from salvaged allocate_cluster_cidrs
on_failure: compensate                         # failure OR cancel destroys what was made (C1)
outcome:
  succeeded: {event: ProvisionSucceeded,
              payload: {kubeconfig: {from: kubeconfig.kubeconfig}, public_ip: {from: droplet.ip}}}
  failed:    {event: ProvisionFailed}          # emitted AFTER compensation settles
  cancelled: {event: ProvisionCancelled}
steps:
  - id: create                        # API call only; undoable, idempotent=False.
    uses: infra.create_instance           # execute: ctx.note(droplet_id=...) the instant the
    with: {spec: {from: run.spec}}    # API responds. undo: destroy by droplet_id from
    timeout_seconds: 60               # output/notes, else by cluster-uuid TAG lookup —
    emit:                             # tolerant of "never created". Closes C1.
      event: InfraAllocated           # persists droplet_id onto the cluster record via
      payload: {droplet_id: {from: create.droplet_id}}   # Pillar 1, same tx as step success
    # Output: droplet_id: str

  - id: droplet                       # gate-only verb: execute no-op; poll: droplet active?
    uses: infra.await_instance            # final Ready enriches outputs with the IP
    with: {droplet_id: {from: create.droplet_id}}
    gate: {timeout_seconds: 600, interval_seconds: 10}    # droplet_ready_timeout
    emit: {event: DropletReady, payload: {public_ip: {from: droplet.ip}}}
    # Output: ip: str

  - id: ssh                           # gate-only; v1's inner SSH retry loop is STRIPPED
    uses: k3s.await_ssh             # from the installer — the gate loop replaces it
    with: {host: {from: droplet.ip}}
    gate: {timeout_seconds: 420, interval_seconds: 10}    # ssh_ready_timeout

  - id: trust_host                    # TOFU keyscan — canonical _ssh_k3s_installer copy
    uses: k3s.trust_host_keys         # ONLY (never the dead StrictHostKeyChecking=no forks,
    with: {host: {from: droplet.ip}}  # gotcha 13)
    retry: ssh_default
    timeout_seconds: 30

  - id: k3s                           # salvaged SSHBasedK3sInstaller body: CIDR flags,
    uses: k3s.install             # extra TLS SANs = ip. undoable=False (seam-c-provider.md
                                  # §5.5: InstallK3s has no inverse, subsumed by the instance undo)
    with: {host: {from: droplet.ip}, spec: {from: run.spec}, extra_tls_san: {from: droplet.ip}}
    retry: ssh_default                # replaces the installer's internal 3x backoff
    timeout_seconds: 300              # install_timeout_seconds

  - id: k3s_ready
    uses: k3s.await_api             # gate-only
    with: {host: {from: droplet.ip}}
    gate: {timeout_seconds: 600, interval_seconds: 10}    # k3s_ready_timeout

  - id: kubeconfig
    uses: k3s.fetch_kubeconfig        # rewrites server: to the public IP
    with: {host: {from: droplet.ip}, rewrite_server_to: {from: droplet.ip}}
    retry: ssh_default
    timeout_seconds: 60
    # Output: kubeconfig: SecretStr   # Fernet-encrypted in the step row; never on disk

  - id: firewalls                     # v1 warns-and-continues (digitalocean.py:477)
    uses: do.apply_firewalls
    with: {droplet_id: {from: create.droplet_id}, spec: {from: run.spec}}
    retry: api_default
    timeout_seconds: 60
    on_failure: continue
```

This retires provider-internal orchestration entirely: `_provision_cluster`, its APScheduler self-scheduling, the `create_task` fallback, and the `DROPLET_READY / K3S_INSTALLED / K3S_FAILED` ProvisioningEvents — the sub-events are cursor positions plus the two `emit:` lines; only the outcome touches the coarse machine. The one dynamic need the plan names — droplet IP → k3s install — is `{from: droplet.ip}`, statically type-checked.

#### Proof 3 (bonus) — `workflows/destroy-cloud.yml` (destroy semantics need a home)

```yaml
workflow: destroy-cloud
version: 1
inputs:
  cluster_id: {type: str}                      # DR-0022 ruling 2: shrinks to cluster_id, mirroring
                                                # provision — see the `infra` head step below
on_failure: report                             # teardown steps are best-effort themselves
outcome:
  succeeded: {event: DestroyCompleted}
  failed:    {event: DestroyStalled}           # cluster stays DESTROYING; reconciliation
  cancelled: {event: DestroyStalled}           #   orphan backstop completes it (gotcha 9)
steps:
  - id: infra                                  # domain step: cluster row + provider_config ->
    uses: cluster.load_infra                   # {provider, slug, resource_ids, dns_record} —
    with: {cluster_id: {from: run.cluster_id}} # read FRESH at run time (ruling 2), replacing the
    timeout_seconds: 30                        # dispatch table's dns_record_ref(cluster) snapshot,
                                                # which was stale on any retry/crash-resumed run
    # Output: provider: str, slug: str, resource_ids: Mapping[str, str],
    #         dns_record: Optional[DnsRecordRef]
  - id: kubecfg
    uses: cluster.load_kubeconfig_optional     # Output: kubeconfig: Optional[SecretStr]
    on_failure: continue
  - id: tailscale                              # clean disconnect BEFORE infra teardown
    uses: kube.delete_daemonset             # (gotcha 10: else 48h Tailscale lingering)
    with: {kubeconfig: {from: kubecfg.kubeconfig}, name: tailscale, namespace: default,
           grace_period_seconds: 30}
    # namespace CORRECTED kube-system -> default (DR-0022 Erratum E3, 2026-07-20): v1's DaemonSet
    # manifest is `namespace: default` throughout and v1 deleted it with `-n default`; deleting from
    # kube-system finds nothing, "succeeds" as NotFound, and silently reintroduces gotcha 10's
    # 48h lingering Tailscale node. Verb renamed kubectl.delete_daemonset -> kube.delete_daemonset.
    gate: {timeout_seconds: 45, interval_seconds: 5, settle_seconds: 3}
    # DR-0022 ruling 4 + Erratum E2: wait/wait_timeout_seconds/settle_seconds LEAVE Params (D2's
    # fix — no command waits, all waiting is an engine gate) for this gate: block. settle_seconds
    # is a post-Ready grace (NOT a poll interval), preserving v1's `--grace-period=30 --wait=true
    # --timeout=45s` then, only once Ready, a few extra seconds for Tailscale's disconnect.
    on_failure: continue                       # Optional kubeconfig=None => no-op
  - id: dns
    uses: dns.delete_record
    with: {record: {from: infra.dns_record}}   # None => no-op
    retry: api_default
    on_failure: continue
  - id: destroy                                # verb resolves provider with
    uses: infra.destroy_instance              # check_enabled=False ALWAYS (gotcha 1)
    with: {provider: {from: infra.provider}, slug: {from: infra.slug},
           resource_ids: {from: infra.resource_ids}}
    # P8: no EmptyParams provider verb — every fact this step needs comes from the `infra`
    # head above, bound here, so V4 type-checks it and `command(params)` stays pure.
    gate: {timeout_seconds: 900, interval_seconds: 15, max_consecutive_poll_failures: 5}
    # execute: initiate destroy (v1 "destroying"). poll_ready: provider absence check;
    # InfrastructureUnreachableError raises TransientError — unreachable ≠ absent
    # (gotchas 7/9): it feeds the hysteresis counter, never flips to "gone".
```

`destroy-shared` (orbstack/kind) is this file plus one `kube.wipe_namespace` step (`on_failure: continue`) before `destroy` — chosen at dispatch by provider kind, never by an `if`. Zombie handling stays DESTROYED → DESTROY_SCHEDULED → this same workflow (gotcha 6).

### 2.3 Decision 7 — Engine semantics

#### 2.3.1 Schedule (retry policy)

```python
@dataclass(frozen=True)
class Schedule:
    max_attempts: int = 1            # total attempts, not retries
    base_delay_seconds: float = 5.0
    factor: float = 2.0              # delay_n = min(max_delay, base*factor**(n-1)) * uniform(1±jitter)
    max_delay_seconds: float = 60.0
    jitter: float = 0.1

NAMED_POLICIES = {
    "none":            Schedule(),
    "api_default":     Schedule(3, 2.0, 2.0, 30.0),  # closes H4/H5 (GHCR/CF now engine-retried)
    "ssh_default":     Schedule(3, 5.0, 2.0, 60.0),  # replicates _ssh_k3s_installer 3x/5s/exp exactly
    "kubectl_default": Schedule(3, 2.0, 2.0, 15.0),  # closes H6
}
```

Classification is fixed, not configurable: `TransientError` and per-attempt timeout expiry → retry until `max_attempts`, then the step fails with the last error; `PermanentError` and any other exception → fail immediately; `StepCancelled` → never retried. Providers are stripped of internal retry (Pillar 3); the named policies are the behavioral replacement carrying v1's tuned values, so retries never compound. Backoff sleeps are in-memory and cancel-aware; a crash during backoff loses nothing (the attempt count is in the DB). **Gate polls are not Schedule-retried**: a poll raising `TransientError`/unreachable increments a consecutive-failure counter (reset on any successful poll) and fails the gate only at `max_consecutive_poll_failures` — the salvaged health-check hysteresis (gotcha 3), engine-owned. The same Schedule machinery wraps `undo` calls (verb default, min `max_attempts=2`): a transient API blip during compensation must not leak a droplet. The ACTIVE-cluster monitoring loop's transient/permanent counter is *not* a Schedule; it stays as salvaged logic in the monitoring component, outside this seam.

#### 2.3.2 Persistence model (cursor granularity)

Granularity: **one row per step instance per run** — a foreach iteration's body step is its own instance, keyed by materialized path (`wave[1].apply`). Attempts mutate the row.

```sql
CREATE TABLE workflow_runs (
    id                 UUID PRIMARY KEY,
    workflow           TEXT NOT NULL,
    workflow_version   INT  NOT NULL,          -- pins the YAML the run started under
    cluster_id         UUID NOT NULL REFERENCES clusters(id),
    args               JSONB NOT NULL,         -- secret:true inputs Fernet-encrypted
    status             TEXT  NOT NULL CHECK (status IN
                        ('pending','running','compensating','succeeded','failed','cancelled')),
    cancel_requested   BOOLEAN NOT NULL DEFAULT FALSE,   -- durable cancel intent
    failed_step        TEXT,                   -- step_path — abort-and-report
    error              TEXT,
    undo_incomplete    JSONB,                  -- step_paths whose undo failed permanently
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    started_at         TIMESTAMPTZ,
    finished_at        TIMESTAMPTZ
);
-- H14 closed by construction: admission control in the schema, not APScheduler dedup
CREATE UNIQUE INDEX one_active_run_per_cluster ON workflow_runs (cluster_id)
    WHERE status IN ('pending','running','compensating');

CREATE TABLE workflow_steps (
    run_id            UUID NOT NULL REFERENCES workflow_runs(id),
    step_path         TEXT NOT NULL,           -- 'create' | 'wave[1].apply'
    verb              TEXT NOT NULL,
    status            TEXT NOT NULL CHECK (status IN
                       ('running','gating','succeeded','failed','failed_continued','cancelled')),
    attempt           INT  NOT NULL DEFAULT 1,
    interrupted_count INT  NOT NULL DEFAULT 0, -- crash-replays; separate budget from attempt
    params            JSONB NOT NULL,          -- resolved-bindings snapshot (secrets encrypted)
    notes             JSONB NOT NULL DEFAULT '{}',  -- ctx.note() write-ahead facts
    output            JSONB,                   -- Output dump; feeds later bindings
    undo_status       TEXT CHECK (undo_status IN ('done','failed','skipped')),
    error             TEXT,
    started_at        TIMESTAMPTZ NOT NULL,
    finished_at       TIMESTAMPTZ,
    PRIMARY KEY (run_id, step_path)
);
```

**Persistence points** (each an awaited transaction; nothing else ever moves the cursor):

| # | Moment | Transaction contents |
|---|---|---|
| 1 | Admission | `INSERT workflow_runs(status='pending')` **in the same transaction** as the outbox drain of the `RunWorkflow` effect. Partial-unique-index conflict ⇒ effect is a no-op + `Notify(run_conflict)` (duplicate suppressed — H14). Destroy admits with `supersede=true`: flip the active run's `cancel_requested`, await its terminal state, then insert. |
| 2 | Step start | `INSERT workflow_steps(status='running', params=resolved)` + **re-read `cancel_requested` in the same transaction** — cancel-vs-step-start is DB-serialized; abort before work if set. |
| 3 | Retry | bump `attempt`, then cancel-aware backoff sleep |
| 4 | Each `ctx.note()` | merge into `notes` (committed before `note()` returns) |
| 5 | Execute done | `output` + status `gating` (if gate) else `succeeded`; if `emit:`, the Pillar-1 event goes to the effects outbox **in the same transaction** — a step's completion and its announced event can never diverge (H7 discipline at step granularity) |
| 6 | Gate passes | final `output` (Ready enrichment) + `succeeded` (+ `emit:` as above) |
| 7 | Step failure/cancel | `failed`/`cancelled` + error; run → `compensating` (if `on_failure: compensate`) or straight to terminal; `on_failure: continue` records `failed_continued` and proceeds |
| 8 | Each undo result | `undo_status` = `done`/`failed` |
| 9 | Run terminal | run status + `finished_at` + the `outcome.*` event via outbox, one transaction |

Nothing *inside* an attempt is persisted (a crashed gate re-polls with a fresh timeout budget; waits are idempotent — accepted cost).

#### 2.3.3 Resume (the total function over crash states)

The engine keeps an in-process registry of run-id → asyncio task. At startup — **before** reconciliation's periodic passes, as a priority-0 phase of `reconcile_three_phase(startup_mode=True)` — and on every periodic pass, any non-terminal run without a live task is adopted:

- `pending` → start forward execution.
- `compensating` → resume compensation over remaining un-compensated undoable steps, LIFO (undos are idempotent by contract, so an interrupted undo simply re-runs).
- `running` → rebuild bindings from persisted `workflow_steps.output` (decrypting secrets), then settle the cursor step:
  - step row `succeeded`/`failed_continued` → advance past it;
  - step row `running`/`gating` (crash mid-step): if the verb is `idempotent` → re-enter `execute` (gates re-poll), bump `interrupted_count`; if **not** idempotent → mark `failed("interrupted; non-idempotent")` and go to the failure policy — `undo(params, None, notes)` plus the cluster-uuid-tag lookup covers any partial effect;
  - `interrupted_count` has its own budget (`resume_replay_limit = 5`, separate from `max_attempts` — a crash is not the step's fault, but a crash-*loop* must converge to `failed`).
- If `cancel_requested` → skip forward execution, go straight to the failure policy's cancel path (G4 below).

**This replaces "mark FAILED and hope" surgically:** startup recovery's `CREATING/PROVISIONING → always FAILED` becomes "resume the run if an adoptable one exists; FAILED only if none does" (old behavior remains the backstop for corrupted/pre-cursor runs); `DEPLOYING`/`DESTROYING` rows likewise consult runs first, keep their fallbacks. **Phase C filters destructive intents**: an `OrphanIntent`/`ZombieIntent` targeting a cluster with a live non-terminal run is skipped that pass — the run owns that infra; once the run terminates, the next pass cleans up exactly as today. Unreachable-provider semantics are untouched: zero intents, and a resumed run on an unreachable provider just accumulates TransientErrors under its Schedule.

#### 2.3.4 Compensation

Triggered when a step fails with `on_failure: abort` in an `on_failure: compensate` workflow, or on cancel of such a workflow. Ordering: **strict LIFO** over this run's step instances — first the failed/interrupted/cancelled step itself with `undo(params, output_or_None, notes)` (it may have partial external effect; this is where C1's half-created droplet dies), then every earlier `succeeded` `undoable` step in reverse completion order (foreach iterations reverse). Skipped: non-undoable verbs and `failed_continued` steps. Each undo runs under Schedule with the forward step's timeout, on a **fresh non-tripped token** (compensation is not cancellable; only process death pauses it, and resume finishes it). Failure semantics: **record-and-continue** — a permanently failed undo is logged, marked `undo_status='failed'`, appended to `run.undo_incomplete`, and the remaining undos still run (v1's destruction job proves best-effort-continue is the right teardown posture; halting strands *more*). The run still reaches a terminal status; the `failed`/`cancelled` outcome event carries `undo_incomplete`, and reconciliation's orphan/zombie pass is the guarantee record-and-continue leans on. Undo implementations use `check_enabled=False` provider resolution (gotcha 1) and never enqueue runs. `on_failure: report` workflows skip all of this: run → `failed` with `failed_step`, infra untouched (PLAN-wave abort-and-report).

#### 2.3.5 Cancellation — the token's exact promises

`cancel(run_id)` = commit `cancel_requested=TRUE`, **then** trip the in-memory token.

- **G1 (durable):** the request is persisted before it is acknowledged; a restart cannot lose it — resume reads the flag and goes straight to the cancel path.
- **G2 (between units, hard):** no new step, foreach iteration, retry attempt, or gate poll starts after the cancel commit — the step-start transaction re-reads the flag (DB-serialized, zero TOCTOU at boundaries), and the engine checks the token before every retry, backoff sleep, and poll.
- **G3 (within a step, bounded):** any `ctx.sleep`/token-wait raises `StepCancelled` immediately; any subprocess spawned via `ctx.run_subprocess` gets process-group SIGTERM within ~1s and SIGKILL after 10s grace — a running kubectl apply is *interrupted*, not raced (H16 closed); as a backstop the step's asyncio task is hard-cancelled after `cancel_grace_seconds=30`. Worst case between "cancel committed" and "no step code running" is min(30s, remaining attempt timeout) — bounded, never "apply runs for minutes".
- **G4 (post-cancel is policy, not chaos):** the interrupted step is recorded `cancelled`; then per `on_failure`: `compensate` ⇒ full LIFO compensation (cancelling a provision destroys the droplet — no leak), `report` ⇒ stop and mark. Terminal status `cancelled`; exactly one `outcome.cancelled` event fires via the outbox regardless of where cancel landed.
- **G5 (not promised):** preemption of arbitrary Python between ctx primitives (bound: G3), un-happening of completed external effects in `report` workflows, or multi-process propagation (the DB flag is the extension point).

#### 2.3.6 Progress and SSE compatibility

`ctx.progress` → outbox `Notify(topic='workflow_progress', ...)`. The engine itself emits outbox `job_started` / `job_completed` / `job_failed` events at run start/terminal with real `{workflow, cluster_id, run_id}` fields — preserving the UI contract (gotcha 15) while retiring `_job_wrapper`'s 36-char-arg scanning; `workflow_runs` replaces the in-memory `_job_history`.

**Test ergonomics.** The validator is pure (YAML → typed AST or errors: table-driven tests, no IO). Verbs are typed classes testable in isolation with a stub `StepContext`. Engine integration tests run real workflows against instant fake verbs and an in-memory outbox, asserting on the two tables — crash-resume is tested by killing the run task at each persistence point and re-adopting; cancellation by tripping the token at each point; the wave and provision YAMLs above double as fixtures.

---

## 3. Taste calls for the human

1. **Chose `gate:` in the grammar (engine-owned poll loop) over gates-as-ordinary-verbs** because one implementation of interval/timeout/hysteresis/cancel-between-polls replaces the five hand-rolled wait loops v1 had and makes the poll-failure hysteresis (gotcha 3) structural rather than per-verb discipline — at the cost of a third grammar construct and a second contract method. Flip if you want the two-construct grammar and will accept a shared `poll_loop()` helper convention inside wait verbs.
2. **Chose `idempotent: bool` + write-ahead `ctx.note()` + tag-based undo over Proposal 2's `EffectClass`+`probe()`** because exactly one verb is non-idempotent (`do.create_droplet`) and compensate-on-crash (destroy the maybe-created droplet, fail the run) is a rare, cheap outcome — probe-and-adopt would rescue such runs in place but adds a third contract method and a three-way enum for one verb. Flip if wasting a nearly-provisioned droplet on a mid-create crash offends.
3. **Chose one-active-run-per-cluster (any workflow, with destroy-supersede) over per-(cluster, workflow) uniqueness** because full serialization is the safest H14 closure and matches Pillar 1's coarse lifecycle — but it means a future auto-snapshot workflow blocks a concurrent deploy request until it finishes. Flip to per-(cluster, workflow) if independent concurrent workflows per cluster are wanted.
