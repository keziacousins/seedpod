---
title: Coherence review — cross-seam resolutions, type glossary, package layout
type: design
status: active
created: 2026-07-12
updated: 2026-07-15
supersedes: seam specs A–D wherever they conflict (highest design authority)
amended-by: DR-0006 (Conflict 3 `record=` birth signature; §2 glossary ClusterRow/DeploymentRow)
---

# Coherence Review — Cross-Seam Conflicts, Resolutions, Glossary, Layout

## 1. Conflicts found and resolved

### Conflict 1 — Two incompatible durable-effect stores: Seam A's `effects_outbox` + `timers` tables vs Seam D's single `outbox` with future-dated timer rows
Seam A specifies `effects_outbox` (effect_id/lane/ordinal, every effect audited as a row) plus a dedicated `timers` table (PK `(aggregate_type, aggregate_id, timer_key)`, upsert re-arm, atomic delete+apply on fire, `CancelTimer(timer_key=None)` = delete-all). Seam D specifies a thinner `outbox` whose CHECK admits only `('notify','run_workflow','schedule_timer')` — it cannot store `Cascade` or `CancelWorkflow` rows at all — and models timers as future-dated outbox rows with `dedupe_key`, cancel as inline DELETE on the outbox.

**Resolution: Seam A's two-table design wins wholesale** (Seam D's own taste call 2 concedes the flip when rescheduling semantics grow — they already have: TTL re-arm on destroy-cancel, key-scoped and all-keys cancel, per-timer event payloads). Seam D's `outbox` table is deleted from migration `0001` and replaced verbatim by Seam A's DDL with three amendments: (i) `aggregate_type` CHECK gains `'run'` because the engine writes drain-lane `Notify` rows directly (`ctx.progress`, `job_started/…`) with `effect_id = "run/{run_id}@{step_path}#{n}"`; (ii) an explicit `kind` CHECK; (iii) lane assignments per Conflict 2.

```sql
-- seedpod/data/migrations/0001_initial.sql  (replaces Seam D's `outbox`; Seam A DDL, amended)
CREATE TABLE effects_outbox (
    seq            INTEGER PRIMARY KEY AUTOINCREMENT,
    effect_id      TEXT    NOT NULL UNIQUE,      -- "{aggregate_type}/{aggregate_id}@{to_version}#{ordinal}"
                                                 -- engine-origin rows: "run/{run_id}@{step_path}#{n}"
    aggregate_type TEXT    NOT NULL CHECK (aggregate_type IN ('cluster','deployment','run')),
    aggregate_id   TEXT    NOT NULL,
    to_version     INTEGER NOT NULL,             -- 0 for engine-origin rows
    ordinal        INTEGER NOT NULL,
    kind           TEXT    NOT NULL CHECK (kind IN ('persist','schedule_timer','cancel_timer',
                                                    'run_workflow','cancel_workflow','cascade','notify')),
    payload        TEXT    NOT NULL,             -- canonical JSON from core/codec.encode()
    lane           TEXT    NOT NULL CHECK (lane IN ('tx','drain')),
    status         TEXT    NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','done','dead')),
    attempts       INTEGER NOT NULL DEFAULT 0,
    available_at   TEXT    NOT NULL,
    created_at     TEXT    NOT NULL,
    done_at        TEXT,
    last_error     TEXT
);
CREATE INDEX idx_outbox_drain     ON effects_outbox (status, available_at, seq);
CREATE INDEX idx_outbox_aggregate ON effects_outbox (aggregate_type, aggregate_id, seq);

CREATE TABLE timers (
    aggregate_type    TEXT NOT NULL,
    aggregate_id      TEXT NOT NULL,
    timer_key         TEXT NOT NULL,             -- 'ttl' | 'destroy'
    fire_at           TEXT NOT NULL,
    event             TEXT NOT NULL,             -- codec.encode(event), applied verbatim on fire
    created_by_effect TEXT NOT NULL,
    PRIMARY KEY (aggregate_type, aggregate_id, timer_key)
);
CREATE INDEX idx_timers_fire ON timers (fire_at);
```

Drain policy (merges the two seams' texts): `notify` — one attempt, broadcast exceptions logged, mark `done` (never `dead`); `run_workflow` — see Conflict 2 (a row *waiting* for supersede re-arms `available_at` without incrementing `attempts`); genuine drain failures back off `[1s,5s,30s,2m,10m…]` and go `dead` at `attempts ≥ 8`, surfaced by reconciliation. Timestamps are written by the injected clock (Seam D convention kept; no DB-side defaults). **Seams changed: D (DDL), and Seam D's "timers are outbox rows" effect-mapping table is void.**

### Conflict 2 — `RunWorkflow`/`CancelWorkflow` lane and run-admission timing (A: tx-lane insert of the run row; B: insert at effect drain, with supersede-await; D: drain via outbox)
Seam A puts `RunWorkflow` in the tx lane (`uow.insert_run` inside the transition transaction). Seam B's persistence point 1 inserts the run row when the effect is *drained*, and its destroy-supersede ("flip the active run's `cancel_requested`, **await its terminal state**, then insert") cannot execute inside a transition transaction. Seam B's one-active-run-per-cluster partial unique index would also abort the whole transition (e.g. `DestroyDue → DESTROYING`) whenever a deploy run is still active.

**Resolution: `RunWorkflow` and `CancelWorkflow` move to the drain lane.** Durability is unchanged — the outbox row commits atomically with the state change and replays after a crash — so Seam A's "no separate `DestroyStarted` event" argument still holds (the *intent* is committed atomically; the run row follows idempotently via `dedupe_key = effect_id`). The EffectExecutor's run-admitter drains them in `seq` order:

```python
# seedpod/runtime/effect_executor.py — run-admission drain rules (normative)
# CancelWorkflow row: UPDATE workflow_runs SET cancel_requested=1 WHERE cluster_id=? AND status IN
#   ('pending','running','blocked','compensating'); trip the in-memory token; mark row done.
# RunWorkflow row:
#   1. resolve (verb, provider) -> definition name + inputs via WorkflowDispatch (Conflict 13)
#   2. INSERT workflow_runs(status='pending', dedupe_key=row.effect_id) ON CONFLICT(dedupe_key) DO NOTHING
#   3. blocked by ux_wr_one_active (another run live) — three branches (DR-0011):
#        - workflow == 'destroy'  -> flip that run's cancel_requested + trip the token, leave
#          THIS row 'pending' with available_at = now + 2s (attempts NOT incremented — waiting
#          is not failure); insert succeeds on a later pass once the victim is terminal
#        - victim.cancel_requested == 1 (already unwinding, whoever set it) -> same wait/re-arm,
#          attempts NOT incremented. This is what makes Conflict 12 true: CW(deploy) at seq N
#          flips the flag, RW(rollback) at seq N+1 waits, admits once the victim is terminal.
#        - otherwise (healthy blocker) -> mark row 'done' + emit run_conflict (H14, Seam B) as a
#          DURABLE drain-lane Notify row inserted in the SAME transaction (DR-0011 clause 2):
#          effect_id = "{blocked_row.effect_id}#run_conflict" (ON CONFLICT DO NOTHING — replay-
#          idempotent), aggregate cluster/{cluster_id}@0#0, environment := cluster.environment
#          (drain time IS this Notify's decision time — DR-0010 extension); delivered by the
#          universal notify drain (one attempt, done, never dead).
```

Amended `EffectKind` lanes (Seam A §A comments):

```python
class EffectKind(StrEnum):
    PERSIST         = "persist"          # tx lane
    SCHEDULE_TIMER  = "schedule_timer"   # tx lane
    CANCEL_TIMER    = "cancel_timer"     # tx lane
    CASCADE         = "cascade"          # tx lane
    RUN_WORKFLOW    = "run_workflow"     # drain lane (row 'pending'; admitter inserts the run — idempotent via dedupe_key)
    CANCEL_WORKFLOW = "cancel_workflow"  # drain lane (row 'pending'; admitter flips cancel_requested + trips token)
    NOTIFY          = "notify"           # drain lane
```

Sequencing this preserves: `DEPLOYING × CancelRequested` emits `CancelWorkflow` before `RunWorkflow(rollback)` (Conflict 12) in the same effect tuple — the admitter processes them in `seq` order, so the rollback run waits for the cancelled deploy run to reach terminal. **Seams changed: A (§A, §D), B (persistence point 1 wording: admission happens at drain of the durable effect row, not "in the same transaction as" it), D (executor).**

### Conflict 3 — Two transition-appliers: Seam A's `apply()` free function vs Seam D's `Dispatcher`, with different effect coverage and no in-tx reuse for the engine
Seam A's `apply()` handles all seven effects (including recursive `Cascade`) but is a bare function in `core/` (which must stay pure) and always owns its transaction. Seam D's `Dispatcher.apply()` handles only `Persist`/`CancelTimer`/outbox-else — it cannot execute `Cascade` (in-tx recursion) at all — and offers no way for the engine to apply an outcome event inside the run-terminal transaction (Seam B point 9 requires run-terminal + outcome atomically). Their audit signatures also differ (`trigger`/`initiated_by` vs `event.actor`).

**Resolution: one component, named `Dispatcher`, living in `seedpod/runtime/dispatcher.py` (not `core/` — it does IO), with Seam A's body, Seam D's name, an optional `tx` for same-transaction chaining (engine outcomes, timer fires, API `DeployRequested`+`ClusterReady` chains), and an optional `record` for NEW births.** Audit derives from `event.actor`; `trigger`/`initiated_by` columns die (Conflict 11).

```python
# seedpod/runtime/dispatcher.py — the ONLY write path for cluster/deployment state in v2
class Dispatcher:
    def __init__(self, uow: UnitOfWork, repos: Repositories, clock: Clock): ...
    def attach_executor(self, executor: EffectExecutor) -> None: ...   # gives .poke(); latency only

    async def apply(self, aggregate: str, aggregate_id: str, event: Event, *,
                    tx: Tx | None = None,
                    record: ClusterRow | DeploymentRow | None = None) -> TransitionResult:
        # DR-0006: births pass the FULL row DTO. The Dispatcher narrows it to the pure
        # ClusterRecord/DeploymentRecord for transition() (same row->record mapping the repo's
        # load() uses); the birth INSERT is the caller's row with the Persist.record's
        # machine-owned fields overlaid (machine wins on shared fields, never the reverse).
        # The Dispatcher NEVER synthesizes column values — row synthesis (slug minting,
        # provider_config from rules/presets) is the API-layer service's job.
        async with self._tx(tx) as t:                       # commit on exit iff tx was None
            rec = narrow(record) if record else await self.repos.load(t, aggregate, aggregate_id)
            result = transition(rec, event)                 # Pillar 1, pure
            if not result.effects:
                return result                               # Ignore: nothing written, no SSE, no audit
            for ordinal, eff in enumerate(result.effects):
                row = outbox_row(eff, aggregate, result.record.id, result.record.version, ordinal)
                match eff:
                    case Persist():
                        await self.repos.persist(t, eff)    # INSERT or CAS UPDATE; rowcount 0 -> StaleVersion
                        await self.repos.state_audits.add(t, aggregate, rec, result, event)
                    case ScheduleTimer(): await self.repos.timers.upsert(t, eff, row.effect_id)
                    case CancelTimer():   await self.repos.timers.delete(t, eff)
                    case Cascade():
                        for dep in await self.repos.deployments_in(t, eff.cluster_id,
                                                                   eff.where_state, eff.except_id):
                            await self.apply("deployment", dep.id, eff.event, tx=t)   # depth <= 2
                    case Notify() | RunWorkflow() | CancelWorkflow():
                        row.lane, row.status = "drain", "pending"
                await self.repos.outbox.insert(t, row)      # tx-lane rows insert status='done'
        self.executor.poke(); self.timers_service.poke()    # hints; polling is the backstop
        return result
```

The engine's step-`emit:` and run-terminal transactions call `dispatcher.apply(..., tx=step_tx)` — Seam B's "event via the effects outbox" is hereby clarified: **events are never outbox rows; they enter through `Dispatcher.apply` and their *effects* hit the outbox** (the only place a serialized Event lives at rest is `timers.event` and inside a `ScheduleTimer` payload). Seam A's `core/apply.py` is renamed to this module; Seam A's `StaleVersion` retry rule (re-read, re-decide, ≤3 attempts, at the caller) is unchanged. **Seams changed: A (module home), D (Dispatcher body, signature), B (clarified wording).**

### Conflict 4 — Two incompatible `workflow_runs` models: Seam B's two-table step-path cursor vs Seam D's single-row `cursor INTEGER + step_results JSON`
Seam D's single-row model (cursor index, `step_results` blob, `heartbeat_at`, extra `'compensated'` status, per-`(cluster_id, workflow)` uniqueness) contradicts Seam B's judged core: one row per step instance keyed by materialized `step_path` (foreach iterations are instances — an integer cursor cannot even address `wave[1].apply`), `notes` write-ahead, `interrupted_count`, one-active-run-per-**cluster** (Seam B taste call 3), no leases/heartbeats (single process; in-process task registry). Seam B's DDL is also Postgres-flavored (UUID/JSONB/TIMESTAMPTZ) against Seam D's SQLite conventions, and Seam A separately requires `dedupe_key TEXT UNIQUE`.

**Resolution: Seam B's structure, Seam D's SQLite conventions, plus `dedupe_key` (A), `deployment_id`/`initiated_by`/`workflow_version` (D), and `'blocked'` (Conflict 5). Dropped: `heartbeat_at`, `'compensated'` (subsumed by `undo_incomplete` empty-vs-nonempty), `cursor`, `step_results`, `version` (rows are engine-private, single writer), per-(cluster,workflow) index.**

```sql
-- seedpod/data/migrations/0001_initial.sql (replaces both prior workflow_runs definitions)
CREATE TABLE workflow_runs (
    id                TEXT PRIMARY KEY,                 -- uuid4
    workflow          TEXT NOT NULL,                    -- CONCRETE definition name (Conflict 13)
    workflow_version  INTEGER NOT NULL,                 -- pins the YAML version at admission
    cluster_id        TEXT NOT NULL REFERENCES clusters(id),
    deployment_id     TEXT REFERENCES deployments(id),
    dedupe_key        TEXT UNIQUE,                      -- RunWorkflow effect_id (exactly-once admission)
    args              TEXT NOT NULL DEFAULT '{}',       -- JSON; secret:true inputs Fernet-encrypted
    status            TEXT NOT NULL CHECK (status IN
                        ('pending','running','blocked','compensating',
                         'succeeded','failed','cancelled')),
    cancel_requested  INTEGER NOT NULL DEFAULT 0,
    failed_step       TEXT,                             -- step_path
    error             TEXT,                             -- JSON {kind:'transient'|'permanent'|'unreachable', step, message}
    undo_incomplete   TEXT,                             -- JSON [step_path]; non-empty == v1 "failed dirty"
    initiated_by      TEXT,
    created_at        TEXT NOT NULL,
    started_at        TEXT,
    finished_at       TEXT
);
CREATE UNIQUE INDEX ux_wr_one_active ON workflow_runs (cluster_id)
    WHERE status IN ('pending','running','blocked','compensating');       -- H14
CREATE INDEX ix_wr_cluster ON workflow_runs (cluster_id, created_at DESC);

CREATE TABLE workflow_steps (
    run_id            TEXT NOT NULL REFERENCES workflow_runs(id),
    step_path         TEXT NOT NULL,                    -- 'create' | 'wave[1].apply'
    verb              TEXT NOT NULL,
    status            TEXT NOT NULL CHECK (status IN
                        ('running','gating','succeeded','failed','failed_continued','cancelled')),
    attempt           INTEGER NOT NULL DEFAULT 1,
    interrupted_count INTEGER NOT NULL DEFAULT 0,
    params            TEXT NOT NULL,                    -- resolved bindings (secrets encrypted)
    notes             TEXT NOT NULL DEFAULT '{}',       -- ctx.note() write-ahead facts
    output            TEXT,                             -- Output.model_dump(); SecretStr fields encrypted
    undo_status       TEXT CHECK (undo_status IN ('done','failed','skipped')),
    error             TEXT,
    started_at        TEXT NOT NULL,
    finished_at       TEXT,
    PRIMARY KEY (run_id, step_path)
);
```

`snapshot-create`/`snapshot-restore` in Seam D's workflow-name comment are future definitions, not v2.0 registry members. **Seams changed: B (types only), D (structure).**

### Conflict 5 — `InfrastructureUnreachableError` engine semantics: Seam B retries it as Transient (which can exhaust into compensation); Seam C mandates park-never-compensate with `BLOCKED` statuses that exist in no schema
Seam B: "InfrastructureUnreachableError is treated as Transient inside steps/gates" and its destroy gate feeds it to the hysteresis counter — under Seam B's own Schedule rules, budget exhaustion becomes Permanent and triggers compensation. Seam C pins the opposite (park `BLOCKED`, slow re-probe, **compensation NEVER triggered by this class**) and its taste call 1 exists precisely to forbid Seam B's reading. Seam C's `BLOCKED`/`BLOCKED_TIMEOUT` appear in neither Seam B's nor Seam D's `workflow_runs` CHECK.

**Resolution: Seam C's law wins; Seam B's mechanism hosts it.**

```
Engine law (replaces Seam B's "treated as Transient" sentence and the destroy-gate hysteresis note):
1. InfrastructureUnreachableError raised from execute(), poll_ready(), or undo() NEVER consumes
   the step's Schedule budget and NEVER triggers or continues into compensation.
2. The run parks: workflow_runs.status='blocked' (step row untouched at its cursor); the engine
   re-probes on a slow schedule (5s, 15s, 30s, 60s cap) up to unreachable_budget (default 15 min).
   Reachability restored -> status back to 'running' (or 'compensating'), attempt unchanged.
3. Budget exhausted:
   - forward step  -> run status='failed', error={kind:'unreachable',...}, compensation SKIPPED
     (undo would also fail; every step's undo_status='skipped'); reconciliation is the cleanup owner.
   - during an undo -> that undo_status='failed', appended to undo_incomplete; remaining undos still
     run (record-and-continue); reconciliation inherits.
4. Gate polls: an Unreachable poll does NOT feed max_consecutive_poll_failures (that counter is for
   TransientError only); it invokes rule 2 directly. The gate's overall timeout clock is suspended
   while blocked.
5. Resume adopts 'blocked' runs exactly like 'running' ones (the re-probe schedule restarts).
6. Reconciliation semantics unchanged: Reconcile raising Unreachable => skip all covered clusters,
   zero intents (crown jewel #1); Phase C already suppresses destructive intents for clusters with
   a live run — 'blocked' counts as live.
```

Terminal mapping for a destroy run that exhausts the blocked budget: outcome `failed` fires `DestroyFailed(reason="unreachable")` → `DESTROY_FAILED` (see Conflict 8 — Seam B's "cluster stays DESTROYING / DestroyStalled" is overridden; `DESTROY_FAILED × InfraMissingObserved → DESTROYED` and `DESTROY_FAILED × DestroyRequested` preserve the backstop and retry paths). **Seams changed: B (§2.0, §2.3.1, destroy-cloud comment), C (BLOCKED_TIMEOUT name dies; 'blocked'/'failed' are the statuses), D (CHECK already amended in Conflict 4).**

### Conflict 6 — Error taxonomy: two homes (B: `engine/errors.py` "defined in core"; C: `core/cluster_spec.py`) and a constructor domain steps can't satisfy
Seam C's leaves require `code=`, `provider=`, `command=` kwargs; Seam B's domain steps (`deploy.plan_waves`, `cluster.load_kubeconfig`) raise `TransientError`/`PermanentError` with no provider envelope. Seam B also lists `StepCancelled` beside the taxonomy as if co-located.

**Resolution: one home, `seedpod/core/errors.py`; `core/cluster_spec.py` re-exports `InfrastructureUnreachableError` (plan-letter fidelity for the salvaged docstring's address) and `engine/errors.py` re-exports all three. Envelope kwargs get defaults so domain steps and engine synthesis type-check. `StepCancelled` is engine-owned and is NOT a `ProviderError`.**

```python
# seedpod/core/errors.py — THE single home of the error taxonomy (all seams import from here)
class ErrorCode(StrEnum):
    ...                                  # Seam C's 17 members, verbatim, unchanged

class ProviderError(Exception):
    """Base. Never raised directly — one of the three leaves only."""
    def __init__(self, message: str, *, code: ErrorCode,
                 provider: str = "engine", command: str = "",       # defaults: domain steps / engine synthesis
                 detail: dict[str, str] | None = None):
        super().__init__(message)
        self.code, self.provider, self.command = code, provider, command
        self.detail = detail or {}

class TransientError(ProviderError):
    def __init__(self, *args, retry_after: float | None = None, **kw):
        super().__init__(*args, **kw); self.retry_after = retry_after

class PermanentError(ProviderError): ...

class InfrastructureUnreachableError(ProviderError):                # SIBLING leaf (Seam C verdict)
    """<v1 docstring salvaged verbatim — Seam C §5.1>"""
    def __init__(self, *args, host: str | None = None, **kw):
        super().__init__(*args, **kw); self.host = host

# seedpod/core/cluster_spec.py:   from seedpod.core.errors import InfrastructureUnreachableError  # re-export
# seedpod/engine/errors.py:       re-exports Transient/Permanent/Unreachable; defines StepCancelled(Exception)
# seedpod/core/machine.py:        InvalidTransition, StaleVersion (machine-layer, NOT ProviderErrors)
```

Seam C's inline retry default ("exponential 5s×2, cap 60s, max 5 attempts") in its engine-behavior table is deleted; **Seam B's `NAMED_POLICIES` and per-verb `default_retry` are the sole retry authority** (`retry_after` still overrides the delay). **Seams changed: B, C.**

### Conflict 7 — Two compensation contracts: Seam B's `Step.undo(params, output, notes, ctx)` vs Seam C's `undo_for(cmd, Observed)` — and two C1-closing scratchpads (`ctx.note` vs the `RESOURCE_ALLOCATED` fold) that nothing connects
Seam C's `Observed` folds an in-memory stream; after a crash the stream is gone, so `undo_for` alone re-opens the crash half of C1. Seam B's `notes` are durable but nothing specifies who writes `resource_ids` into them.

**Resolution: a single `ProviderStep` adapter in the engine bridges them — `Progress(RESOURCE_ALLOCATED)` is written through `ctx.note()` (durable, pre-return), and `Step.undo` is *implemented as* `undo_for(cmd, Observed(data=notes, value=output))`. `Observed` is thus rehydratable from the DB; both C1 windows (mid-stream death and process crash) close through one path.** Seam C's conformance C-09 asserts through this adapter.

```python
# seedpod/engine/provider_step.py — the ONE bridge between Seam B's Step and Seam C's Provider
class ProviderStep(Step[P, O]):
    provider_name: ClassVar[str]                     # registry key into ctx.services.providers
    undoable = True                                  # iff undo_for(command) can be non-None

    def command(self, params: P) -> ProviderCommand: ...          # pure param -> command mapping
    def output_from(self, value: object) -> O: ...                # Result.value -> Output

    async def execute(self, params: P, ctx: StepContext) -> O:
        provider = ctx.services.providers[self.provider_name]
        value = None
        async for ev in provider.execute(self.command(params)):
            match ev:
                case Progress(phase=RESOURCE_ALLOCATED, data=d):
                    await ctx.note(**{k: str(v) for k, v in d.get("resource_ids", {}).items()})
                    await ctx.progress(ev.message or ev.phase, **jsonable(ev.data))
                case Progress():
                    await ctx.progress(ev.message or ev.phase, **jsonable(ev.data))
                case Result():
                    value = ev.value
        return self.output_from(value)

    async def poll_ready(self, params, provisional, ctx):         # gateable subclasses: one probe command
        ...

    async def undo(self, params: P, output: O | None, notes: Mapping[str, str], ctx: StepContext) -> None:
        observed = Observed(data=dict(notes),
                            value=self.result_value_from(output) if output is not None else None)
        inverse = undo_for(self.command(params), observed)        # seedpod/providers/compensation.py
        if inverse is None:
            return
        async for _ in ctx.services.providers[self.provider_name].execute(inverse):
            pass                                                  # idempotent, absence-tolerant by C-10/C-23
```

`Observed.data` is therefore normatively "the persisted `workflow_steps.notes`", not an in-memory fold — Seam C's §5.2 docstring is amended to say so. Domain steps (non-provider) keep hand-written `undo` where needed. **Seams changed: B (adapter added), C (Observed provenance).**

### Conflict 8 — Workflow outcome/emit events that don't exist in Seam A's union (`InfraAllocated`, `DropletReady`, `DestroyCompleted`, `DestroyStalled`, `ProvisionCancelled`, `DeployCancelled`)
Seam B's validator V8 requires every `emit:`/`outcome.*.event` to be a Pillar-1 event; six of Seam B's aren't. Seam B's `DestroyStalled` ("cluster stays DESTROYING") also contradicts Seam A's `DESTROYING × DestroyFailed → DESTROY_FAILED`.

**Resolution — the event union is Seam A's, amended as follows; everything else in Seam B renames:**

```python
# seedpod/core/events.py — additions/changes to Seam A §F (all else unchanged)

# NEW cluster Reports (mid-provision facts; Ignore everywhere but PROVISIONING per the totality law):
class InfraAllocated(Report):  resource_ids: Mapping[str, str]   # was Seam B's emit 'InfraAllocated(droplet_id)'
class EndpointReady(Report):   public_ip: str                    # was Seam B's 'DropletReady' — provider-neutral

# AMENDED (kubeconfig never rides an event — Conflict 9; resource_ids owned by InfraAllocated):
class ProvisionSucceeded(Report): public_ip: str; kubeconfig_ref: str

# NEW deployment Report, deliberately total-Ignore (satisfies "exactly one terminal event per run"
# for the rollback workflow, Conflict 12, without moving any machine):
class RollbackFinished(Report): ok: bool

# DELETED from all specs: DestroyCompleted, DestroyStalled, ProvisionCancelled, DeployCancelled, DropletReady
```

New cluster-table rows (Seam A §G; both are same-state Persists — the "P and N always together" law is restated as *"every non-Ignore row emits exactly one Persist and one Notify"*, covering same-state field updates):

| State | Event | → State | Effects |
|---|---|---|---|
| PROVISIONING | InfraAllocated | PROVISIONING | P (merge `provider_resources`), N |
| PROVISIONING | EndpointReady | PROVISIONING | P (sets `public_ip`), N |

Outcome-event mapping (amends Seam B's three YAML files):

| Run | succeeded | failed | cancelled |
|---|---|---|---|
| provision-\* | `ProvisionSucceeded` | `ProvisionFailed` | `ProvisionFailed(reason="cancelled")` — unreachable in v2.0 (Seam A taste call 3) but grammar-required |
| deploy-waves | `DeploySucceeded(resolved_images={from: audit.resolved_images})` | `DeployFailed` | `DeployFailed(reason="cancelled")` — load-bearing: destroy-supersede cancels a live deploy while the deployment is still DEPLOYING → FAILED; machine-initiated cancels find CANCELLED/DESTROYED and Ignore |
| destroy-\* | `DestroySucceeded` | `DestroyFailed` → DESTROY_FAILED (Seam B's stay-DESTROYING is **overridden**; retry via `DestroyRequested`, completion backstop via `DESTROY_FAILED × InfraMissingObserved`) | `DestroyFailed(reason="cancelled")` |
| deploy-rollback | `RollbackFinished(ok=True)` | `RollbackFinished(ok=False)` | `RollbackFinished(ok=False)` |

Event targeting rule (previously unstated): the engine applies deploy/rollback events to aggregate `("deployment", run.deployment_id)` and provision/destroy events to `("cluster", run.cluster_id)`, via `dispatcher.apply(..., tx=terminal_tx)`. `deploy.load_audit`'s Output gains `resolved_images: Mapping[str, str]`. **Seams changed: A (union + 2 rows + law wording), B (all outcome/emit blocks).**

### Conflict 9 — Secrets in the outbox: Seam B's provision outcome carries the raw kubeconfig (`SecretStr`) in a Pillar-1 event; Seam A mandates `kubeconfig_ref` ("refs only, never secrets") and JSON-scalar Notify payloads
**Resolution: a domain step persists the ciphertext and mints the ref; the event carries only the ref.** Tail of every provision workflow (amends `provision-digitalocean.yml` and siblings):

```yaml
  - id: kubeconfig
    uses: k3s.fetch_kubeconfig
    with: {host: {from: droplet.address}, rewrite_server_to: {from: droplet.address},
           known_hosts: {from: trust_host.known_hosts}}
    retry: ssh_default
    timeout_seconds: 60
    # Output: kubeconfig: SecretStr

  - id: store                          # domain step: Fernet-encrypt via CryptoService, write
    uses: cluster.store_kubeconfig     # clusters.encrypted_kubeconfig + kubeconfig_key_class
    with: {cluster_id: {from: run.cluster_id}, kubeconfig: {from: kubeconfig.kubeconfig}}
    timeout_seconds: 30
    # Output: kubeconfig_ref: str      # "cluster-kubeconfig:{cluster_id}" — opaque handle

outcome:
  succeeded: {event: ProvisionSucceeded,
              payload: {public_ip: {from: droplet.address}, kubeconfig_ref: {from: store.kubeconfig_ref}}}
  failed:    {event: ProvisionFailed}
  cancelled: {event: ProvisionFailed}
```

`cluster.load_kubeconfig` resolves the ref by decrypting `clusters.encrypted_kubeconfig` (checking `kubeconfig_key_class`). `ClusterRecord.kubeconfig_ref` stores the ref string. **Seams changed: B (YAML + new verb `cluster.store_kubeconfig`), C (no change — providers still never store it).**

### Conflict 10 — `RunWorkflow.args` vs provision inputs: Seam B's provision takes `spec: ClusterSpecification`, but the pure machine emitting `RW(provision)` cannot build one (and Seam A says args are "refs only")
**Resolution: provision inputs shrink to `cluster_id`; a domain step builds the spec.** Head of every provision workflow:

```yaml
inputs:
  cluster_id: {type: str}
steps:
  - id: spec                           # domain step: cluster row + provider_config ->
    uses: cluster.load_spec            # ClusterSpecification, incl. salvaged allocate_cluster_cidrs()
    with: {cluster_id: {from: run.cluster_id}}
    timeout_seconds: 30
    # Output: spec: ClusterSpecification

  - id: create
    uses: infra.create_instance
    with: {provider: {from: spec.provider}, spec: {from: spec.spec}}
    timeout_seconds: 60
    emit: {event: InfraAllocated, payload: {resource_ids: {from: create.resource_ids}}}
    # Output: resource_ids: Mapping[str, str]      # was droplet_id: str — aligned with
    #                                              # Seam C InstanceCreated.resource_ids
  - id: droplet
    uses: infra.await_instance
    with: {provider: {from: spec.provider}, resource_ids: {from: create.resource_ids}}
    gate: {timeout_seconds: 600, interval_seconds: 10}
    emit: {event: EndpointReady, payload: {public_ip: {from: droplet.address}}}
    # Output: address: str
```

`infra.create_instance`'s Output mirrors `InstanceCreated` (`resource_ids`, `address: str | None`,
`adopted_existing: bool`) — DR-0022 P6 (glossary nouns: *address*, never *ip*) and ruling 1
(`provider` is the late-binding key every `infra.*` verb's Params carries, bound here from
`cluster.load_spec`'s `provider: str` output). **Seams changed: A (RunWorkflow docstring example:
args carry ids only), B.**

### Conflict 11 — `clusters`/`deployments`/audit DDL vs the Pillar-1 records: missing `pre_destroy_state`, wrong state-set comment, `kind`-vs-`Origin`, `error_message`-vs-`failure_reason`, `services`-vs-`resolved_images`, missing `superseded_by`/`environment`/`spec_ref`, no `deployment_state_audits`, audit `trigger/initiated_by` vs `event.actor`
Seam D's comment enumerates v1's state set (`creating`, `deploying`, no `new`/`unmanaged`) — Seam A deleted those; Seam D's `kind IN ('managed','discovered','unmanaged')` conflicts with Seam A's `Origin{MANAGED,DISCOVERED}` + `UNMANAGED`-as-state; `pre_destroy_state` (load-bearing for destroy-cancel) has no column; the deployment machine's `superseded_by` has no column; Seam A requires `deployment_state_audits`; Seam A's record has scalar `provider_resource_id` while Seam D (correctly, per Seam C's `resource_ids: Mapping`) has a JSON map.

**Resolution (amended DDL; Seam A's `ClusterRecord.provider_resource_id` becomes `provider_resources: Mapping[str, str]`; glossary standardizes `failure_reason`):**

```sql
CREATE TABLE clusters (
    id                   TEXT PRIMARY KEY,          -- uuid4 always; routes accept id-or-slug
    name                 TEXT NOT NULL,
    slug                 TEXT NOT NULL,
    origin               TEXT NOT NULL DEFAULT 'managed'
                         CHECK (origin IN ('managed','discovered')),   -- Seam A Origin; UNMANAGED is a STATUS
    environment          TEXT NOT NULL,
    repository           TEXT,
    branch               TEXT,
    status               TEXT NOT NULL,             -- NO CHECK; owned by Pillar 1. The set is Seam A's ten:
                                                    -- new/provisioning/active/destroy-scheduled/destroying/
                                                    -- destroyed/destroy-failed/failed/zombie/unmanaged
    pre_destroy_state    TEXT,                      -- set on entry to destroy-scheduled; cancel returns here
    version              INTEGER NOT NULL DEFAULT 0,
    provider             TEXT NOT NULL,
    provider_config      TEXT NOT NULL DEFAULT '{}',-- provisioning INPUTS (JSON)
    provider_resources   TEXT NOT NULL DEFAULT '{}',-- provisioning OUTPUTS (JSON); fed by InfraAllocated
    dns_hostname         TEXT,
    dns_zone             TEXT,
    public_ip            TEXT,
    node_count           INTEGER NOT NULL DEFAULT 1,
    encrypted_kubeconfig TEXT,
    kubeconfig_key_class TEXT CHECK (kubeconfig_key_class IN ('DEV','PROD')),
    kubeconfig_ref       TEXT,                      -- opaque handle from cluster.store_kubeconfig (Conflict 9)
    cost_per_hour        REAL NOT NULL DEFAULT 0,
    total_cost           REAL NOT NULL DEFAULT 0,
    consecutive_health_failures INTEGER NOT NULL DEFAULT 0,
    failure_reason       TEXT,                      -- was error_message; one name system-wide
    last_reconciled_at   TEXT,
    created_at           TEXT NOT NULL,
    updated_at           TEXT NOT NULL,
    expires_at           TEXT
);
-- Seam D's four indexes unchanged. TERMINAL_STATES = ('destroyed','failed') exported by Pillar 1:
CREATE UNIQUE INDEX ux_clusters_slug_live ON clusters(slug)
    WHERE status NOT IN ('destroyed','failed');     -- destroy-failed & zombie stay live (own real infra)

CREATE TABLE deployments (
    id               TEXT PRIMARY KEY,
    cluster_id       TEXT NOT NULL REFERENCES clusters(id),
    environment      TEXT NOT NULL,
    status           TEXT NOT NULL,                 -- NO CHECK; Seam A's nine deployment states
    version          INTEGER NOT NULL DEFAULT 0,
    manifest_version TEXT NOT NULL,
    spec_ref         TEXT REFERENCES deployment_audits(id),  -- DeployRequested.spec_ref = the audit row
    resolved_images  TEXT NOT NULL DEFAULT '{}',    -- was `services`; set by DeploySucceeded
    superseded_by    TEXT REFERENCES deployments(id),
    deployed_by      TEXT,
    failure_reason   TEXT,                          -- was error_message
    created_at       TEXT NOT NULL,
    updated_at       TEXT NOT NULL
);
CREATE INDEX ix_deployments_cluster ON deployments(cluster_id, created_at DESC);

-- One audit shape for both machines; actor replaces trigger/initiated_by (derivable from the
-- Seam A actor grammar: 'api:<user>' | 'reconciler' | 'health' | 'engine:run:<id>' | 'timer:<key>'
-- | 'cluster-machine'); created_at = event.at (aware UTC 'Z').
CREATE TABLE cluster_state_audits (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    cluster_id   TEXT NOT NULL REFERENCES clusters(id),
    from_state   TEXT NOT NULL,
    to_state     TEXT NOT NULL,
    event        TEXT NOT NULL,
    actor        TEXT NOT NULL,
    reason       TEXT,
    context      TEXT,
    created_at   TEXT NOT NULL
);
CREATE INDEX ix_csa_cluster_time ON cluster_state_audits(cluster_id, created_at DESC);

CREATE TABLE deployment_state_audits (               -- was missing from Seam D entirely
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    deployment_id TEXT NOT NULL REFERENCES deployments(id),
    cluster_id    TEXT NOT NULL REFERENCES clusters(id),
    from_state    TEXT NOT NULL,
    to_state      TEXT NOT NULL,
    event         TEXT NOT NULL,
    actor         TEXT NOT NULL,
    reason        TEXT,
    context       TEXT,
    created_at    TEXT NOT NULL
);
CREATE INDEX ix_dsa_deployment_time ON deployment_state_audits(deployment_id, created_at DESC);
```

`deployment_audits`, `api_keys`, `secrets`, `secret_audits`, `deployment_presets`, `snapshots` stand as Seam D wrote them. Amended records: `ClusterRecord.provider_resources: Mapping[str, str] = ()`, `DeploymentRecord.spec_ref: str | None` and `resolved_images: Mapping[str, str] = ()`, `DeployRejected` births still audited via `deployment_state_audits`. **Seams changed: A (record fields), D (DDL).**

### Conflict 12 — Deploy-cancel rollback: Seam C requires the deploy workflow to "declare `undo: rollout-undo` as a workflow-level compensator", but Seam B's frozen grammar has no such key and its `deploy-waves.yml` is `on_failure: report`
Verified in v1: `api/deployments.py:1090-1104` runs `rollout_undo` best-effort on every cancel — this behavior must not silently regress, and adding a grammar key would violate the freeze.

**Resolution: rollback is a machine *decision*, not a grammar feature.** Seam A's deployment row gains one effect; a fourth (tiny) workflow joins the registry; `emit`-order + the drain-lane admitter (Conflict 2) guarantee it runs only after the cancelled deploy run is terminal:

| State | Event | → State | Effects (amended row, Seam A §H) |
|---|---|---|---|
| DEPLOYING | CancelRequested | CANCELLED | **CW(deploy), RW(rollback, deployment_id=self.id)** |
| DEPLOYING | ClusterGone | DESTROYED | CW(deploy) only — no rollback on a dying cluster (v1 parity) |

```yaml
# workflows/deploy-rollback.yml
workflow: deploy-rollback
version: 1
inputs: {deployment_id: {type: str}}
on_failure: report                    # v1 warns-and-continues on undo failure
outcome:
  succeeded: {event: RollbackFinished}       # total-Ignore Report (Conflict 8)
  failed:    {event: RollbackFinished}
  cancelled: {event: RollbackFinished}
steps:
  - id: kubecfg
    uses: cluster.load_kubeconfig
  - id: undo
    uses: kube.rollout_undo           # ProviderStep -> KubeRolloutUndo; >=1-success semantics
    with: {kubeconfig: {from: kubecfg.kubeconfig}, namespace: default}   # (crown jewel #13)
    retry: kubectl_default
    timeout_seconds: 120
```

Seam C's `KubeApplyManifest → KubeDeleteManifest` default inverse remains for infra shims only; `deploy-waves` stays `on_failure: report`, so that inverse can never fire on app manifests. Seam C's "workflow-declared compensator" phrasing is void. **Seams changed: A (one row), B (new file + verb `kube.rollout_undo`), C (§5.5 note rewritten to point here).**

### Conflict 13 — Workflow naming: abstract verbs (`provision`/`deploy`/`destroy`, Seam A) vs concrete definition files (`provision-digitalocean`, `deploy-waves`, `destroy-cloud`/`destroy-shared`, Seam B) vs Seam D's mixed list
**Resolution: effects carry abstract verbs (the machine stays provider-ignorant); the run-admitter resolves verb × provider → concrete definition + inputs via a `WorkflowDispatch` table built in the composition root; `workflow_runs.workflow` stores the concrete name (pinned with `workflow_version`).**

```python
# seedpod/engine/dispatch_table.py
@dataclass(frozen=True)
class WorkflowDispatch:
    """RunWorkflow.workflow ∈ {'provision','deploy','rollback','destroy'} — the closed verb set;
    grows only with new machine effects."""
    destroy_by_provider: Mapping[str, str]     # {'digitalocean':'destroy-cloud', 'tart':'destroy-cloud',
                                               #  'kind':'destroy-shared', 'orbstack':'destroy-shared'}
    def resolve(self, eff: RunWorkflow, cluster: ClusterRecord) -> tuple[str, dict]:
        match eff.workflow:
            case "provision": return (f"provision-{cluster.provider}", {"cluster_id": eff.cluster_id})
            case "deploy":    return ("deploy-waves",    {"deployment_id": eff.deployment_id})
            case "rollback":  return ("deploy-rollback", {"deployment_id": eff.deployment_id})
            case "destroy":   return (self.destroy_by_provider[cluster.provider],
                                      {"cluster_id": eff.cluster_id})   # matches provision (DR-0022 ruling 2)
```

DR-0022 ruling 2 amends this destroy arm: it no longer smuggles a dispatch-time `dns_record_ref(cluster)`
snapshot (stale on any retry or crash-resumed run) — `cluster.load_infra`, a new domain-step head on both
`destroy-cloud.yml`/`destroy-shared.yml`, reads `{provider, slug, resource_ids, dns_record}` FRESH at run
time instead, and the `DnsRecordRefResolver` Protocol this arm used to call through is deleted.

**Seams changed: A (registry comment: verbs are `provision|deploy|rollback|destroy`), B (definition names unchanged; admission consults this table), D (factory constructs it, workflow-name comment corrected).**

### Conflict 14 — `known_hosts` not threaded through the provision YAML: `k3s.install`/`k3s.await_api`/`k3s.fetch_kubeconfig` take no `known_hosts`, but Seam C's `InstallK3s(known_hosts="")` is a hard `PermanentError` (TOFU, crown jewel #2)
**Resolution: `k3s.trust_host_keys` declares `Output: known_hosts: str` (from `CaptureHostKeys → HostKeys`) and every subsequent SSH step binds it (amended `provision-digitalocean.yml`):**

```yaml
  - id: trust_host
    uses: k3s.trust_host_keys
    with: {host: {from: droplet.address}}
    retry: ssh_default
    timeout_seconds: 330               # covers cloud-init wait (300) + keyscan (10)
    # Output: known_hosts: str
  - id: k3s
    uses: k3s.install
    with: {host: {from: droplet.address}, spec: {from: spec.spec}, extra_tls_san: {from: droplet.address},
           known_hosts: {from: trust_host.known_hosts}}
    retry: ssh_default
    timeout_seconds: 300
  - id: k3s_ready
    uses: k3s.await_api
    with: {host: {from: droplet.address}, known_hosts: {from: trust_host.known_hosts}}
    gate: {timeout_seconds: 600, interval_seconds: 10}
  # kubeconfig step: known_hosts binding added — see Conflict 9 block
```

Validator V4 now proves at load time what Seam C enforced only at runtime. **Seams changed: B.**

### Conflict 15 — Composition-root gaps: no `TimerService`, `Provider.check_ready` never called, `Dispatcher`/engine wiring predates Conflicts 2–3, `nudge` vs `poke`
Seam C requires `check_ready` "called once by the composition root before serving"; Seam D's factory/`App.start` never does. The `timers` table (Conflict 1) has no polling owner in Seam D. Seam A says `nudge()`, Seam D says `poke()`.

**Resolution (amended factory/App excerpts; `poke()` is the name):**

```python
    # factory — after step 6 (dispatcher) and before the engine:
    timers = TimerService(uow=uow, repos=repos, dispatcher=dispatcher, clock=clock)
        # polls timers WHERE fire_at <= now; per timer ONE transaction:
        # conditional consume (Seam A §D as amended by DR-0009): DELETE … AND fire_at = :snapshot;
        # rowcount 1 -> dispatcher.apply(decode_event(row.event), tx=t); rowcount 0 -> skip (re-arm/cancel won)
    ...
    engine = WorkflowEngine(definitions, steps, uow=uow, repos=repos, dispatcher=dispatcher, clock=clock)
    dispatch_table = WorkflowDispatch(destroy_by_provider=DESTROY_BY_PROVIDER)
    executor = EffectExecutor(uow=uow, repos=repos, hub=hub, engine=engine, dispatch=dispatch_table,
                              clock=clock, poll_interval=config.outbox_poll_interval)
    dispatcher.attach_executor(executor)          # sole late wire; .poke() latency hint only
    dispatcher.attach_timers(timers)              # ditto

# App.start — amended order:
    async def start(self) -> None:
        migrate(self.db.engine, MIGRATIONS_DIR)
        TempFileRegistry.sweep()                                  # H17 startup sweep (Seam C)
        for p in self.providers.values():
            await p.check_ready()                                 # fail at startup, not mid-provision
        await self.executor.start()                               # drain pending outbox FIRST (H7 replay)
        await self.timers.start()                                 # timers are correctness, like the executor
        if self.config.background_tasks:
            await self.engine.resume_inflight()                   # pending/running/blocked/compensating
            await self.services.reconciliation.start()
    # stop(): reverse — reconciliation, engine, timers, executor, subprocesses, hub, db
```

**Seams changed: A (`nudge`→`poke`), D.**

### Conflict 16 — Naming synonyms (one glossary rule per pair; all seams adopt)
1. **Package root:** `seedpod/` (Seam C/D) — Seam A's `seedpod2/` dies.
2. **`StaleVersion`** (Seam A) — Seam D's `StaleVersionError` dies.
3. **`effects_outbox`** — Seam D's `outbox` dies (Conflict 1); the repository is `OutboxRepository`.
4. **`EffectExecutor`** (Seam D) — Seam A's "drainer" dies as a name; it is the drain loop inside `EffectExecutor`.
5. **`Dispatcher`** — Seam A's "executor contract"/`apply.py` naming dies (Conflict 3).
6. **run** (never "execution"/"job") for workflow instances; the SSE topics `job_started`/`job_completed`/`job_failed` survive **as wire-topic strings only** (UI contract, Seam B §2.3.6) and appear nowhere else.
7. **`failure_reason`** (Seam A) — `error_message` dies (Conflict 11).
8. **`origin`** (Seam A) — Seam D's `kind` column dies (Conflict 11).
9. **`resolved_images`** — Seam D's `services` column dies (Conflict 11).
10. **`resource_ids`** (Seam C) — Seam B's `droplet_id` output/param naming dies (Conflict 10); `EndpointReady` replaces `DropletReady` (Conflict 8).
11. **State-string spellings:** v1's hyphenated values verbatim (`destroy-scheduled`, `destroy-failed`); Python enum members underscored (Seam A already does both).
12. **`workflow`** for a definition, **verb** for a step type, **`workflow` field of `RunWorkflow`** = abstract verb (Conflict 13).

---

## 2. Unified TYPE GLOSSARY (owning module under `seedpod/`)

| Type | One line | Owner |
|---|---|---|
| `ClusterState` | 10-state coarse cluster lifecycle enum, hyphenated wire values | `core/records.py` |
| `DeploymentState` | 9-state deployment machine enum | `core/records.py` |
| `Origin` | `managed \| discovered` cluster provenance | `core/records.py` |
| `ClusterRecord` | frozen cluster DTO the machine transitions (`provider_resources: Mapping`, `pre_destroy_state`, `kubeconfig_ref`) | `core/records.py` |
| `DeploymentRecord` | frozen deployment DTO (`spec_ref`, `resolved_images`, `superseded_by`) | `core/records.py` |
| `TERMINAL_STATES` | `('destroyed','failed')` — the only slug-releasing states | `core/records.py` |
| `Event` + classes `Command/Report/TimerFired/Observation/Cascaded` | tagged frozen event union; totality law rides the class | `core/events.py` |
| `InfraAllocated`, `EndpointReady`, `RollbackFinished` | Reports added by this review (Conflicts 8/12) | `core/events.py` |
| `Effect` = `Persist \| Notify \| RunWorkflow \| CancelWorkflow \| ScheduleTimer \| CancelTimer \| Cascade` | inert serializable effect union; lanes per Conflict 2 | `core/effects.py` |
| `EffectKind` | StrEnum tag for effects (outbox `kind` column) | `core/effects.py` |
| `transition()` | the pure total function; the whole of Pillar 1 | `core/machine.py` |
| `TransitionResult` | `(record, effects)`; `()` effects = Ignore | `core/machine.py` |
| `InvalidTransition`, `StaleVersion` | machine-layer errors (409 / CAS-retry) | `core/machine.py` |
| `encode/decode_event/decode_effect` | registry codec, canonical JSON, aware-UTC `Z` | `core/codec.py` |
| `ErrorCode`, `ProviderError`, `TransientError`, `PermanentError`, `InfrastructureUnreachableError` | THE error taxonomy (single home; re-exported by `core/cluster_spec.py`, `engine/errors.py`) | `core/errors.py` |
| `ClusterSpecification`, `allocate_cluster_cidrs` | salvaged spec + CIDR hashing | `core/cluster_spec.py` |
| `ReconciliationIntent` (`Orphan/Zombie/CreateUnmanaged/StatusSync`) | salvaged intent dataclasses | `core/reconciliation_intents.py` |
| `TempFileRegistry` | 0600 temp files, startup sweep (H17) | `core/tempfiles.py` |
| `Clock`, `SystemClock`, `FrozenClock` | injected time source; nothing else calls `now()` | `core/clock.py` |
| `Step`, `StepContext`, `StepServices`, `Ready/NotReady` | the verb contract (execute/poll_ready/undo, note/progress/sleep/run_subprocess) | `engine/step.py` |
| `ProviderStep` | the Step↔Provider bridge: RESOURCE_ALLOCATED→note, undo→`undo_for(Observed)` | `engine/provider_step.py` |
| `CancelToken`, `StepCancelled` | cooperative cancellation (G1–G5) | `engine/cancel.py` |
| `Schedule`, `NAMED_POLICIES` | retry policy; sole retry authority | `engine/schedule.py` |
| `WorkflowDefinition` + validator (V1–V10) | frozen-grammar YAML AST | `engine/config.py` |
| `WorkflowEngine` | run executor: admission, gates, resume, compensation, blocked-park | `engine/engine.py` |
| `WorkflowDispatch` | abstract verb × provider → concrete definition + inputs | `engine/dispatch_table.py` |
| `StepRegistry` | DI-built verb registry (provider + domain steps) | `engine/registry.py` |
| `Provider` protocol, `ProviderCommand` union, `Progress/Result/ProviderEvent`, `Observed`, `RESOURCE_ALLOCATED` | Pillar-3 contract | `providers/contract.py` |
| `CreateInstance`…`Reconcile`, `KubeApplyManifest`…`KubeWatchPods`, `DestroyOutcome/DestroyStatus`, `SSHTarget`, `ClusterSnapshot` | command/value types | `providers/contract.py` |
| `undo_for()` | pure command→inverse mapping over `Observed` | `providers/compensation.py` |
| `classify_subprocess/classify_http` + phrase lists | edge classifiers (string-sniffing's only home) | `providers/classify.py` |
| `PodInfo/PodDetails/NodeInfo/DeploymentInfo/EventInfo/PodWatchEvent` | salvaged kube DTOs | `providers/kube_types.py` |
| `GhcrService`, `DnsService`, `DnsRecordUpserted` | supporting services (never Providers) | `services/ghcr.py`, `services/dns.py` |
| `RuleEngine` | salvaged rules, fail-fast load | `services/rules.py` |
| `ManifestResolver` | salvaged GHCR-discovery + Jinja render | `services/manifests.py` |
| `CryptoService` | Fernet DEV/PROD, key_class stamping; only crypto site | `services/crypto.py` |
| `Dispatcher` | the ONLY transition write path; full effect coverage, `tx=`/`record=` params | `runtime/dispatcher.py` |
| `EffectExecutor` | outbox drain loop + run-admitter (Conflict 2 rules) | `runtime/effect_executor.py` |
| `TimerService` | polls `timers`; atomic delete+apply per fire | `runtime/timers.py` |
| `SSEHub` | in-memory pub/sub; topics `cluster_state_changed`, `deployment_status_changed`, `workflow_progress`, `job_*` | `runtime/sse.py` |
| `SubprocessManager` | salvaged tracked-subprocess registry; `register`/`unregister`/cluster-scoped `terminate_for_cluster`/`shutdown` | `runtime/subprocess_manager.py` |
| `TrackedSubprocessRunner` | default `SubprocessRunner` transport; registers with `SubprocessManager`, process-group SIGTERM→SIGKILL on cancel/timeout (H16, DR-0005) | `runtime/subprocess_manager.py` |
| `DetachedLaunchRunner` | `SubprocessRunner` wrapper for `tart run`'s detached-launch semantics; never registered, never awaited (DR-0005) | `runtime/subprocess_manager.py` |
| `UnitOfWork`, `Repositories`, `OutboxRepository`, … | session-in/DTO-out repos; never commit | `data/uow.py`, `data/repositories.py` |
| `ClusterRow`, `DeploymentRow` | full-row DTOs (strict supersets of the pure records); the `apply(record=)` birth contract (DR-0006) | `data/repositories.py` |
| `migrate()` | numbered-SQL `PRAGMA user_version` runner; sole schema authority | `data/migrate.py` |
| `AppConfig`, `build_app()`, `App`, `Services` | composition root; three test seams (`providers`, `clock`, `id_gen`) | `app/config.py`, `app/factory.py`, `app/app.py` |
| `Fault`, `Harness`, C-01…C-24 | provider conformance suite | `tests/conformance/` |

## 3. Proposed v2 package layout

```
seedpod-v2/
├── PLAN-refactor.md
├── reference-code/                      # read-only v1 (unchanged)
├── config/
│   ├── manifest-templates/              # copied wholesale from v1
│   ├── deployment-profiles/
│   ├── deployment-rules.yml
│   ├── providers/                       # tuned timeouts/constants (imported, not re-guessed)
│   └── workflows/
│       ├── provision-digitalocean.yml   ├── provision-kind.yml
│       ├── provision-tart.yml           ├── provision-orbstack.yml
│       ├── deploy-waves.yml             ├── deploy-rollback.yml
│       ├── destroy-cloud.yml            └── destroy-shared.yml
├── seedpod/
│   ├── __main__.py
│   ├── core/                            # Pillar 1 — pure, zero IO, zero deps upward
│   │   ├── records.py  events.py  effects.py  machine.py  codec.py
│   │   ├── errors.py                    # THE taxonomy (Conflict 6)
│   │   ├── cluster_spec.py              # salvage + re-export
│   │   ├── reconciliation_intents.py  tempfiles.py  clock.py
│   ├── engine/                          # Pillar 2
│   │   ├── step.py  provider_step.py  cancel.py  schedule.py
│   │   ├── config.py  registry.py  engine.py  dispatch_table.py  errors.py
│   │   └── steps/                       # domain verbs: cluster_load_spec.py, kubeconfig_store.py,
│   │                                    #   deploy_load_audit.py, deploy_plan_waves.py, ...
│   ├── providers/                       # Pillar 3
│   │   ├── contract.py  compensation.py  classify.py  kube_types.py
│   │   ├── digitalocean.py  kind.py  tart.py  orbstack.py  ssh_k3s.py  kubectl.py
│   │   └── _tart_cli.py                 # salvaged
│   ├── services/                        # supporting, not Providers
│   │   ├── ghcr.py  dns.py  rules.py  manifests.py  crypto.py  secrets.py
│   ├── runtime/                         # the impure spine
│   │   ├── dispatcher.py  effect_executor.py  timers.py
│   │   ├── reconciliation.py  sse.py  subprocess_manager.py
│   ├── data/
│   │   ├── migrate.py  database.py  uow.py  repositories.py
│   │   └── migrations/0001_initial.sql
│   ├── api/
│   │   ├── factory.py  deps.py
│   │   └── routers/  (clusters, deployments, auth, secrets, config, events,
│   │                  presets, registry, snapshots, health, workflows)
│   └── app/
│       ├── config.py  factory.py  app.py  services.py
└── tests/
    ├── conftest.py                      # build_app + three seams; no patch anywhere
    ├── core/                            # exhaustive machine tests, codec properties — no unittest.mock
    ├── engine/                          # validator tables, fake-verb integration, crash/cancel matrices
    ├── conformance/                     # C-01..C-24 over six providers + service subsets
    ├── data/                            # migration + repo tests
    └── acceptance/test_deployment_flow.py   # ported v1 e2e — the parity gate
```
