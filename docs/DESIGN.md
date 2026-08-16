---
title: Design Lock — the 8 pinned decisions
type: design
status: active   # ratified by DR-0002 (all taste calls blessed as chosen)
created: 2026-07-12
updated: 2026-07-12
---

# Seedpod v2 — Design Lock

**Status:** ratified (DR-0002 — all taste calls blessed as chosen) · **Companion to:** `PLAN-refactor.md` (the constitution; this document pins the interfaces the plan left as prose)

**Provenance:** produced by a 20-agent design-lock workflow (2026-07-12): per seam, one miner extracted v1 ground truth from `reference-code/`, three proposers drafted interfaces from deliberately different angles, a judge synthesized one spec; a final coherence pass reconciled shared types across all four seams.

## Authority & precedence

The full normative specs live in `design/`:

| Doc | Owns |
|---|---|
| `design/seam-a-core.md` | Decisions 1–2 (Effect union, codec, state sets, both transition tables, totality law) |
| `design/seam-b-engine.md` | Decisions 3, 4, 7 (Step contract, frozen YAML grammar + validator, engine semantics) |
| `design/seam-c-provider.md` | Decision 5 (error taxonomy, ProviderCommand/Event unions, compensation map, conformance suite C-01…C-24) |
| `design/seam-d-foundation.md` | Decisions 6, 8 (schema conventions, migration runner, composition root) |
| `design/coherence-review.md` | **Supersedes all of the above wherever they conflict.** 16 resolved conflicts, the unified type glossary, and the package layout. |
| `design/ui-contract.md` | UI contract: v1 SPA consumption audit, binding server obligations (SSE payload/keepalive/auth), SPA migration worklist (DR-0002: UI adapts, no shims). |

**Precedence rule (binding on every build agent and human):** coherence-review.md > seam spec > this summary. In particular, the coherence review *replaces* these seam-spec sections outright: the outbox + timers DDL (Conflict 1), `RunWorkflow`/`CancelWorkflow` lanes and run admission (Conflict 2), the `Dispatcher` (Conflict 3), `workflow_runs`/`workflow_steps` DDL (Conflict 4), `InfrastructureUnreachableError` engine semantics (Conflict 5 — Seam C's park-never-compensate law wins over Seam B's retry-as-Transient), the error-taxonomy home `seedpod/core/errors.py` (Conflict 6), the `ProviderStep` bridge (Conflict 7), the event-union amendments (Conflict 8), kubeconfig-ref handling (Conflict 9), provision inputs (Conflict 10), the clusters/deployments/audits DDL (Conflict 11), deploy-cancel rollback (Conflict 12), workflow dispatch naming (Conflict 13), known_hosts threading (Conflict 14), factory/App.start wiring (Conflict 15), and the naming glossary (Conflict 16).

---

## The 8 decisions, pinned

**1. Effect representation.** Seven frozen, slotted, kw-only dataclasses — `Persist | Notify | RunWorkflow | CancelWorkflow | ScheduleTimer | CancelTimer | Cascade` — serialized by one registry codec (canonical JSON, aware-UTC `Z`, property-tested round-trip) into a durable `effects_outbox` table plus a dedicated `timers` table. Two lanes: `Persist/ScheduleTimer/CancelTimer/Cascade` execute inside the transition transaction; `Notify/RunWorkflow/CancelWorkflow` are drained post-commit (rows commit atomically with the state change, so H7 is closed by persistence). Deterministic `effect_id = {aggregate}/{id}@{version}#{ordinal}` gives exactly-once commitment. Pruning (DR-0002): `EffectExecutor` housekeeping deletes `done` rows older than `AppConfig.outbox_retention_days` (default 7) hourly; `dead` rows are never auto-pruned (they are reconciliation's surface). → Seam A §A–D, amended by Conflicts 1–3.

**2. State sets, events, transition maps.** Two pure machines sharing one `transition(record, event) -> TransitionResult` in `core/machine.py`. Cluster: 10 coarse states (v1's CREATING/DEPLOYING deleted — provisioning sub-events become workflow steps; deployment progress never touches cluster state). Deployment: 9 states, its own complete table. The **totality law**: events are classed `Command | Report | TimerFired | Observation | Cascaded`; only Commands can be Invalid, everything else defaults to explicit Ignore — this one rule closes every stale-event race and retires `force=True` entirely ("set state to X" is unrepresentable). Optimistic CAS (`version` column, `StaleVersion` re-decide ≤3) replaces the lock dict (H8/H9). Complete 10×15 and 9×8 tables in Seam A §G–H; two rows added and three event renames per Conflicts 8/12.

**3. Step contract.** `Step[Params, Output]` (pydantic-typed) with `execute(params, ctx)`, optional `poll_ready` (gateable verbs), optional `undo(params, output|None, notes, ctx)`. `StepContext` provides the only allowed primitives: `note()` (durable write-ahead scratchpad — the structural C1 close), `progress()` (SSE via outbox), `sleep()` and `run_subprocess()` (both cancel-aware; process-group SIGTERM/SIGKILL is the structural H16 close). Typed named bindings: outputs persisted per step row; later steps' `with: {x: {from: step.field}}` resolved from the DB (resume gets byte-identical inputs), type-checked at config load. `ProviderStep` (Conflict 7) is the single bridge to Pillar 3: `RESOURCE_ALLOCATED` progress → `ctx.note()`, `undo` = `undo_for(cmd, Observed(notes, output))`. → Seam B §2.1.

**4. Workflow config grammar (frozen).** `sequence + foreach + gate + typed bindings` — nothing else, ever. No `if`/`when`/expressions/interpolation (`${` anywhere is a load error); conditionals are presence/absence of config (Optional params ⇒ typed no-op), loops are `foreach` over a planned list, provider choice is dispatch data. Validator rules V1–V10 prove bindings and types at load time. Proven sufficient by four written-out workflows: `deploy-waves` (the wave-orchestration proving ground — waves fit with zero escape hatches), `provision-digitalocean` (amended per Conflicts 9/10/14), `destroy-cloud`, `deploy-rollback` (Conflict 12). → Seam B §2.2.

**5. Provider contract.** `Provider` protocol: `check_ready()` at startup + `execute(cmd) -> AsyncIterator[Progress | Result]` — stateless, kubeconfig always a parameter (H18), no internal retry/poll/sleep (engine owns Schedule; H4–H6), errors raised never yielded. Complete typed `ProviderCommand` union across three planes (machine / ssh-k3s / kubectl). Error taxonomy in `seedpod/core/errors.py` (Conflict 6): `TransientError`, `PermanentError`, and `InfrastructureUnreachableError` as a **sibling** leaf — park the run `blocked`, re-probe slowly, **never compensate** (Conflict 5); "not ready / already absent / not found" are typed Results, never exceptions. 38-row classification decision table; pure `undo_for()` compensation map (C1 closed even for mid-create death); conformance suite C-01…C-24 parametrized over six providers with transport-seam fault injection, no mocks. GHCR/Cloudflare are supporting services, not Providers. → Seam C.

**6. Fresh schema.** Numbered SQL migrations under `seedpod/data/migrations/` applied by a `PRAGMA user_version` runner — the sole schema authority (no `create_all`, no alembic). SQLite conventions: TEXT ISO-8601 aware-UTC timestamps written only via the injected clock; JSON as TEXT; no CHECK on machine-owned status columns. Tables: clusters (+`version`, `pre_destroy_state`, `provider_resources`, `kubeconfig_ref`), deployments (+`spec_ref`, `resolved_images`, `superseded_by`), both audit tables, deployment_audits, api_keys, secrets(+UNIQUE), secret_audits, deployment_presets, snapshots, `workflow_runs` + `workflow_steps` (step-path cursor, one row per step instance), `effects_outbox` + `timers`. **Authoritative DDL for clusters/deployments/audits/workflow tables/outbox/timers is in the coherence review (Conflicts 1, 4, 11)**, not Seam D. → Seam D Decision 6 for conventions and untouched tables.

**7. Engine semantics.** `Schedule` retry policy (named policies carry v1's tuned values; classification is fixed: Transient/timeout → retry, Permanent/other → fail, `StepCancelled` → never); engine-owned `gate:` loop with consecutive-poll-failure hysteresis; cursor = one persisted row per step instance keyed by materialized `step_path`, nine enumerated persistence points; resume is a total function over crash states (priority-0 phase of reconciliation, which stays the backstop — Phase C suppresses destructive intents for clusters with a live run); compensation is strict LIFO, record-and-continue, on a fresh non-tripped token; cancellation guarantees G1–G5 (durable flag, DB-serialized step boundaries, bounded in-step interruption). Unreachable-park semantics per Conflict 5. → Seam B §2.3.

**8. Composition root.** `build_app(config, *, providers=, clock=, id_gen=) -> App` — pure construction, zero import-time side effects, construction order = dependency DAG, the one runtime cycle closed through the database. `App.start()` order (Conflict 15): migrate → temp-file sweep → `check_ready()` every provider → executor (drain outbox first, H7 replay) → timers → engine resume + reconciliation. Route DI is one seam (`api.state.app`); tests pass the three keyword seams and never patch. `Dispatcher` (`runtime/dispatcher.py`, Conflict 3) is the only write path for machine state. → Seam D Decision 8, amended by Conflict 15.

Package layout and the full type glossary (every shared type, one line, owning module): coherence review §2–3.

---

## Taste calls — RESOLVED (all ten blessed as chosen, DR-0002, 2026-07-12)

Each was flagged by a judge as defensibly flippable; all were reviewed and kept. Retained for the record — re-opening any of these requires a new DR. Three seam-level calls were already settled by the coherence pass and need no review: Seam D's timers-as-outbox-rows (flipped to the two-table design, Conflict 1 — Seam D's own spec conceded this), Seam C's workflow-declared rollback compensator (superseded by the machine-decision design, Conflict 12), and Seam B's Unreachable-as-Transient (overridden by Seam C's park law, Conflict 5).

1. **(A1) Cluster `DEPLOYING` state deleted; UI composes `cluster.state × latest deployment.state`.** The plan orders deployment progress off cluster state, and keeping DEPLOYING preserves the exact `DEPLOYMENT_FAILED→ACTIVE` smear v2 exists to kill. **Flip if** UI/SSE consumers can't absorb the composed query at cutover.
2. **(A2) Destroy-cancel returns to `pre_destroy_state`, not unconditionally ACTIVE.** v1 silently resurrects FAILED/ZOMBIE/DESTROYED clusters on cancel. **Flip if** you want zero drift from v1's exact cancel semantics.
3. **(A3) No `DestroyRequested` during PROVISIONING** (v1 parity; provisions aren't abortable until the run terminates). **Flip if** operators need a mid-provision kill switch in v2.0.
4. **(B1) `gate:` is a grammar construct** (engine-owned poll loop — one implementation of interval/timeout/hysteresis/cancel) rather than gates-as-ordinary-verbs. **Flip if** you want a two-construct grammar and accept a shared helper convention inside wait verbs.
5. **(B2) Crash mid-`do.create_droplet` compensates (destroys the maybe-created droplet) rather than probe-and-adopt.** One non-idempotent verb doesn't justify a third contract method. **Flip if** wasting a nearly-provisioned droplet on a rare crash offends.
6. **(B3) One active run per cluster, any workflow** (destroy supersedes; safest H14 closure). A future auto-snapshot workflow would block a concurrent deploy. **Flip to** per-(cluster, workflow) if concurrent independent workflows are wanted.
7. **(C1) `InfrastructureUnreachableError` is a sibling leaf, not a `TransientError` subclass.** A subclass lets a generic retry policy exhaust into compensation on a network blip — the mass-false-orphaning regression v1's docstring warns about. Affirmed by the coherence pass. **Flip only if** you'd rather have two-branch engine policy and trust every handler.
8. **(C2) Single-shot `KubeProbeRollout` polled by the engine gate** replaces v1's blocking `kubectl rollout status --timeout`. One uniform "no command waits" law. **Flip if** you want the battle-tested blocking call kept byte-for-byte as the one exception.
9. **(D1) No `CHECK` constraint on `clusters.status`/`deployments.status`** — Pillar 1 is the sole authority (a proposer's CHECK misspelled v1's hyphenated values, demonstrating the failure mode). **Flip if** you want belt-and-braces DB integrity plus a migration per state-set change.
10. **(D3) Full v1 feature-table surface kept in `0001`** (presets, snapshots, secret_audits, health counter, cost columns) — they back live endpoints and on-disk data. **Flip if** presets/snapshots are out of scope for v2 parity.

---

## What this unlocks (build plan)

With interfaces frozen, the three pillars build **concurrently**, converging at the seams:

- **Phase 0 (mechanical, next):** git init with allowlist (`reference-code/` gitignored — it still contains `.env`, `admin-api-key.txt`, `db/`, `logs/`, and the embedded v1 `.git`); package skeleton per the coherence layout; `0001_initial.sql`; copy `config/` wholesale; port `tests/acceptance/test_deployment_flow.py` (red until parity); secrets scan before first commit.
- **Pillar 1** (core machine + codec + exhaustive no-mock tests) ∥ **Pillar 2** (engine + validator + fake-verb integration tests) ∥ **Pillar 3** (six providers + conformance suite) — all against the frozen types.
- Then: workflows wired end-to-end, `Dispatcher`/`EffectExecutor`/`TimerService`, API + composition root, acceptance spec green = cutover-ready.

Stop signals from the plan remain in force: any temptation to add grammar (an `if`, an expression, interpolation) is a design regression, not an implementation detail — it comes back here, not into a YAML file.
