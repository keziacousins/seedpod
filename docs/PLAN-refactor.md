---
title: Clean-Room Rebuild Plan
type: plan
status: active
created: 2026-07-12
updated: 2026-07-12
---

# Seedpod v2 — Clean-Room Rebuild Plan (Python)

**Status:** adopted (design lock complete — see `DESIGN.md`) · **Supersedes:** `PLAN-rust-refactor.md` (rejected — see below) and folds in `PLAN-wave-orchestration.md` (becomes the first workflow).

Seedpod is the control-plane orchestrator for Exampleco's K3s deployments: branch-pattern rules → provision an ephemeral cluster on some provider (DigitalOcean / kind / Tart / OrbStack) → resolve multi-service images from GHCR → render Jinja manifests → deploy → TTL → destroy. This document is the primary onboarding artifact for the rebuild.

**Approach: clean-room v2 in a new repository.** Nothing is currently load-bearing and no persisted state matters except the templates (which we keep), so we do **not** do an in-place strangler or a shared-DB migration. We stand up a fresh project with a clean schema, and the current codebase is copied in as **`reference-code/`** — read-only source material, not something we edit. Everything the plan calls "salvage" is *copied forward from `reference-code/`* behind new interfaces; everything else is rebuilt.

The whole rebuild is organised around one idea: **the state machine decides; it does not do.**

### Read this first: skeleton is greenfield, logic is salvaged

The single most important rule of this rebuild: **clean-room *skeleton*, not clean-room *logic*.** Seedpod's real value is in its messy-correct edges — reconciliation intents, `InfrastructureUnreachableError` handling, CIDR allocation, GHCR branch-discovery fallbacks, the Tart/Rosetta quirks, provider IO bodies. Those get **copied forward from `reference-code/`**, not re-derived from a blank page. The one way this rebuild fails is by silently regressing edge behavior we already got right. See "Salvage vs rebuild" below for the explicit lists.

## Why not the Rust rewrite (and why v2-in-Python isn't the same mistake)

The Rust plan's one great idea — a pure `transition(state, event) -> (state, [effect])` with effects as inert data — is a *design*, not a language feature, and ports to Python directly. Everything else the rewrite buys (single binary, provider-as-subprocess isolation) is either not a real requirement for an internal control plane or achievable in-process. A rewrite also throws away the two things the review says are already good: the `ALLOWED_TRANSITIONS` contract and the three-phase reconciliation backstop.

A clean-room v2 *in Python* avoids the rewrite's central danger precisely because we keep the language and **copy the hard-won domain logic forward** rather than re-implement it. We rebuild the *wiring* (the god object, the dual job systems, the global-singleton DI, the inline-side-effect state manager) and salvage the *bodies*. Steal the idea; rebuild the skeleton; keep the edges.

## The three pillars (and the seam that ties them)

1. **Core state machine (pure).** A minimal, coarse lifecycle. Decides transitions, emits effects, does zero IO.
2. **Workflow engine (constrained job control).** Executes effects by running steps. Owns retry, timeout, progress, cancellation, and compensation. Config-driven with a *closed* vocabulary.
3. **Providers (typed IO adapters).** A single clean contract. Stateless, no DB, uniform error taxonomy.

The seam between all three is **`Effect` as data**. The machine returns effects; the engine executes them by running provider steps; providers do the IO. This one seam is what makes the top findings *structural impossibilities* rather than patches — see the finding map per phase below.

There are deliberately **two different "effect" concepts**, kept apart:
- **State-machine `Effect`** — a dumb, serializable data enum (command pattern). No logic rides on it. This is what keeps Pillar 1 pure and mock-free to test.
- **The engine's execution** — this is where an *Effect-TS-shaped* runtime lives (Schedule=retry, Scope=compensation, typed error channel, interruption=cancel). We steal those four ideas as small concrete mechanisms; we do **not** build an effect monad in Python.

---

## Pillar 1 — Core state machine (pure)

Today there are *two* transition mechanisms — `transition_cluster_state` (via `ALLOWED_TRANSITIONS`) and `advance_cluster_provisioning` (via `ProvisioningEvent`). That duplication is the same disease as inline effects. Unify into one pure function:

```python
def transition(cluster: ClusterRecord, event: Event) -> tuple[ClusterRecord, list[Effect]]:
    """Pure. No IO. Total over (state × event): returns new state + effects, or raises InvalidTransition."""
```

Design rules:
- **Coarse cluster lifecycle only.** Keep `ALLOWED_TRANSITIONS` — it's good — but shrink what counts as a *state*. "Installing K3s", "waiting for SSH" are **workflow steps**, not cluster states. Provisioning sub-events fold into a workflow, not a parallel state mechanism.
- **Deployment is its own small machine.** This resolves the `DEPLOYMENT_FAILED → ACTIVE` awkwardness (state-machine.md): the *cluster* stays a coarse infra lifecycle; deployment success/failure lives in the deployment machine and its record, surfaced to the UI, not smeared onto cluster state.
- **Effects, not side effects.** `transition` returns `[Persist(...), Notify(...), SpawnWorkflow(...), ScheduleTimer(...)]`. It never broadcasts, never spawns a task, never touches a lock.
- **Optimistic concurrency instead of a lock dict.** Add a `version` column; the `Persist` effect updates `WHERE version = expected`. This retires the process-local `asyncio.Lock` dictionary (and its unbounded-growth leak, H9) and the 30s lock-timeout silent-failure path (H8).

```python
class Effect: ...                     # inert data, ideally serializable
class Persist(Effect): cluster: ClusterRecord
class Notify(Effect): topic: Topic; payload: Payload
class RunWorkflow(Effect): workflow: str; cluster_id: str; args: dict
class ScheduleTimer(Effect): cluster_id: str; after: timedelta; event: Event
class CancelTimer(Effect): cluster_id: str
```

**Testing:** pure `transition` is tested with **zero mocks** — build a state, apply an event, assert on `(new_state, effects)`. Enumerate every `(state × event)` for exhaustive coverage. **Design sensor:** if a state-machine test still needs `Mock`/`patch`, the seam has leaked and Pillar 1 isn't pure yet.

---

## Pillar 2 — Workflow engine (constrained job control)

This is where the two current job systems (`jobs/state/*` bespoke, used in prod; `jobs/operations/*` + `jobs/framework/job_executor.py` generic, half-migrated with dead code) **converge into one**. Finish the generic framework, express everything on it, delete the bespoke path and `convenience_jobs.py`.

A workflow is **declarative config**: an ordered set of steps with dependencies and readiness gates. The engine provides the Effect-TS-shaped machinery:

| Engine capability | Mechanism | Closes |
|---|---|---|
| Retry/backoff | `Schedule` policy applied *around* each step | H4–H6 (retry now lives once, in the engine, not per-provider) |
| Compensation | each step declares an `undo`; engine runs undos on abort | **C1** (DO droplet leak), kind cleanup, partial-K3s leak |
| Typed errors | `TransientError` vs `PermanentError` drive retry-vs-fail | provider error-handling mess |
| Cancellation | cooperative cancel token checked between/within steps | H16 (cancel-vs-apply race) |
| Resumability | persisted **step cursor** per run | stuck PROVISIONING, fire-and-forget tasks, H14 |

### Constrained vocabulary — the hard rule

**Grow the verbs, freeze the grammar.**
- **Verbs (step types)** — `provider.create`, `wait-for-readiness`, `kubectl-apply`, `resolve-dns`, `resolve-secrets`, … — may grow freely. The set is bounded by what Seedpod actually does (≈ the provider surface). Adding one is a reviewed capability.
- **Grammar (control flow)** — `if`/`when`/`for`/expressions/interpolation — stays closed. It converges: sequence + readiness gate + fan-out-within-a-wave is nearly the whole language.

**The escape hatch exists — it's Python behind the step contract, not script in the config.** A genuinely bespoke need becomes a new *step type* (a typed, tested `Step` implementation), never a scripting block in YAML. Kubernetes model (write an operator in Go, not YAML-script), not GitHub Actions model (`run: <bash>`). The two things that tempt grammar both dissolve into **data**: conditionals → presence/absence of config; loops → fan-out over a list. The one legitimately dynamic need — a step's output feeding a later step (droplet IP → k3s-install) — is **data-flow**, added narrowly as typed named bindings, *not* string interpolation.

### Wave orchestration = the proving ground

`PLAN-wave-orchestration.md` is exactly a workflow: declared ordering (`deploy_wave`), a readiness gate between waves (`kubectl rollout status` / `kubectl wait --for=complete`), abort-and-report on failure, fan-out within a wave. Build the engine against waves **first**. If waves can be expressed with the closed vocabulary and no escape into scripting, the Pillar-2 shape is validated before we touch provisioning. If waves tempt us to add `if`/expressions, that's the stop signal.

### Resumability builds on reconciliation (don't replace it)

The three-phase reconciliation (Provider→Seedpod, Seedpod→Provider, Execute Intents) with `OrphanIntent`/`ZombieIntent` is a genuine asset — intents are already effect-shaped. The engine persists a per-run step cursor; on restart, reconciliation **resumes in-flight workflows from their cursor** instead of today's "mark FAILED and hope" backstop. Reconciliation stays the backstop; the cursor makes resume the common case.

---

## Pillar 3 — Provider contract (typed IO adapters)

Today: `CloudProvider` ABC (DO, kind, tart), a separate `KubernetesProvider` ABC (tuple returns), and GHCR/Cloudflare with no interface — five different error styles, retry in some but not others, and `kubernetes.py` reaching into the DB (**H18**). Unify under one contract:

```python
class Provider(Protocol):
    async def execute(self, cmd: ProviderCommand) -> AsyncIterator[ProviderEvent]:
        """Stateless. All context in cmd. Streams progress; ends with Result or raises Transient/PermanentError."""
```

Contract rules:
- **Stateless, no DB.** Everything by parameter; kubeconfig is *passed in*, never fetched by the provider (closes H18). Keep the Rust plan's "all context in the command" discipline in-process.
- **Uniform error taxonomy** in `core/cluster_spec.py`: `TransientError` / `PermanentError` (+ keep the good `InfrastructureUnreachableError`). Retry/backoff is **not** a provider concern — the engine applies it via `Schedule`. This is *why* GHCR/CF/kubectl lack retry today: it was a per-provider afterthought. Move it up once.
- **Progress as a stream**, so job-progress SSE is uniform (drop the bespoke per-job broadcasting).
- **Compensation-aware.** Provider commands expose their inverse (create→destroy) so the engine can undo. Directly closes C1 and the kind/partial-K3s leaks.
- **Secure temp files** for kubeconfig: `0600`, plus a startup registry that sweeps stale temp files (closes H17). DNS (Cloudflare) and registry (GHCR) stay *supporting services*, not `Provider`s — but adopt the same error taxonomy + engine retry.

**Testing:** a **shared provider conformance suite** parametrized over all providers (fail-fast on missing binary, cleanup-on-destroy, reconcile-intent mapping, `InfrastructureUnreachableError` on unreachable) replaces the N bespoke per-provider test files. That suite doubles as the spec for "what a new provider must implement."

---

## Salvage vs rebuild

The rebuild lives or dies on getting this split right. **Salvage** = copy the logic forward from `reference-code/` behind the new interfaces, changing only what the new boundaries require. **Rebuild** = do not copy; these are the wiring pathologies the review documented, and dragging them across defeats the point.

### Salvage (copy forward from `reference-code/`)

| What | Where in `reference-code/` | Note |
|---|---|---|
| Transition contract | `seedpod/core/state_manager.py` (`ALLOWED_TRANSITIONS`) | Port the *map* into the pure `transition()`; drop the class wiring around it. |
| Three-phase reconciliation + intents | `seedpod/core/reconciliation.py`, `seedpod/core/reconciliation_intents.py` | Recovery backstop; intents are already effect-shaped. Extend for workflow resume. |
| Rule evaluation | `seedpod/orchestrator/rule_engine.py` | Already a clean config→decision function; near-verbatim. |
| Manifest resolution | `seedpod/orchestrator/manifest_resolver.py` | GHCR branch discovery + fallbacks, Jinja render, hostname strategies. High edge-value. |
| Provider IO bodies | `seedpod/providers/{digitalocean,kind,tart,orbstack,kubernetes,ghcr,cloudflare_dns}.py`, `_ssh_k3s_installer.py`, `_tart_cli.py` | Copy the *IO*, re-house behind the new `Provider` protocol; strip retry (moves to engine) and DB access. |
| CIDR allocation | `seedpod/core/cluster_spec.py` (`allocate_cluster_cidrs`) | Hash-based per-cluster CIDRs; Tailscale-critical. |
| Secret management | `SecretManager` + DEV/PROD Fernet key separation | Salvage as-is. |
| Subprocess tracking | `seedpod/core/subprocess_manager.py` | Graceful-shutdown of long-running kubectl/ssh. |
| `InfrastructureUnreachableError` semantics | `seedpod/core/cluster_spec.py` | The unreachable-vs-absent distinction reconciliation relies on. |
| Repository/DTO boundary | `seedpod/data/` | The *pattern* is right; rebuild the callers so nothing bypasses it (kills H10's 7 sites by construction). |
| **Templates & config** | `config/manifest-templates/`, `config/deployment-profiles/`, `config/deployment-rules.yml`, `config/providers/` | The work we explicitly care about. Copied wholesale. |
| e2e behavior | `tests/e2e/test_deployment_flow.py` | Ported as the **acceptance spec** (see Testing). |

### Rebuild (do NOT copy — these are the pathologies)

- `ClusterStateManager` wiring → replaced by pure `transition()` + effect executor.
- `ClusterManager` god object (~1786 lines) → dissolved into the three pillars.
- Both job systems (`jobs/state/*` **and** `jobs/operations/*` + `jobs/framework/`) → one workflow engine.
- Global-singleton DI + `conftest` global wiring → explicit construction / injection.
- Inline SSE in transitions, the `asyncio.Lock` dict, fire-and-forget `create_task` follow-ups → effects + engine + optimistic concurrency.
- Every direct-ORM site that bypasses the repositories (H10).

## Fresh schema (the clean-DB bonus)

A disposable DB means we design the schema *for the new model* instead of bolting onto the old one. Two tables that were awkward as migrations are natural here:

- **`workflow_runs` / step cursor** — persisted per-run step position, so restart *resumes* an in-flight workflow (provision/deploy) from its cursor rather than restarting or orphaning. This is what upgrades reconciliation from "mark FAILED on restart" to "resume."
- **Effects outbox** — `Notify`/`Persist` effects written transactionally with the state change and drained by the executor. This is the *durable* form of "SSE can't fail a transition": **H7 is closed by persistence, not merely by not-raising** — a crash mid-broadcast replays from the outbox.

Otherwise the schema is a cleaned-up version of the current one (clusters, deployments, deployment_audits, cluster_state_audits, api_keys, secrets) plus the `version` column for optimistic concurrency.

## Dissolving the god object

`ClusterManager` (~1786 lines) stops being a thing. Its responsibilities land where they belong: lifecycle → the state machine + a thin `ClusterService`; deployment orchestration → workflows; auto-snapshot → a workflow; SSE emission → `Notify` effects; job scheduling → the engine; DNS/kubeconfig → provider commands. The god object is retired as a *consequence* of the three pillars, not as a separate task.

---

## Testing strategy for the rebuild

1. **Port the e2e as the acceptance spec.** `reference-code/tests/e2e/test_deployment_flow.py` exercises the one contract that must survive (`POST /api/version-update`). Port it to run against v2's API early — v2 reaching green on this suite *is* the definition of parity / cutover-ready. It replaces the strangler's "regression gate" role: instead of guarding in-place edits, it's the target the fresh build aims at.
2. **Mine white-box tests for intent, then leave them in `reference-code/`.** Extract the transition table (from `reference-code/tests/unit/test_state_manager.py`) and provider behaviors (`test_tart_provider.py`) into specs for the new pure machine / conformance suite. Do **not** port the `Mock`/`patch` scaffolding — it exists only because of inline IO + global singletons, both of which v2 deletes; in v2 those tests need no mocks at all.
3. **Don't pin bugs.** Cross-check extracted intent against `review/SUMMARY.md`. A test asserting SSE-coupled transition semantics is pinning **H7**; a test tolerating `manifest_resolution_failed` as success encodes a bug. Mine for *intended* behavior, not observed.
4. **New style:** pure transition tests (no mocks, exhaustive), shared provider conformance suite, engine integration tests with mock providers that respond immediately.

---

## Phased build (each phase closes named findings)

**Phase 0 — Stand up the project.** New repo; current code copied in as read-only `reference-code/`; templates/config copied into their real home; fresh schema (clusters/deployments/audits + `workflow_runs` + effects outbox + `version` column); port `test_deployment_flow.py` as the acceptance spec (red until parity); generate a state diagram from the salvaged `ALLOWED_TRANSITIONS`. *Enables everything; closes nothing yet.*

**Phase 1 — Pure core.** Extract `transition()` as a pure function returning effects; add an effect executor; make `Persist` use optimistic concurrency; move SSE to a `Notify` effect that logs-not-raises on failure.
*Closes:* **H7** (SSE can't fail a transition), **H8/H9** (lock dict gone), the `advance_cluster_provisioning` duplication.

**Phase 2 — Workflow engine on waves.** Build the engine (Schedule/Scope/cancel/cursor) with a closed vocabulary; implement wave orchestration as the first workflow; remove busybox init containers.
*Closes:* wave-orchestration goals; **H16** (cancel token); groundwork for retry/compensation.

**Phase 3 — Provider contract.** Introduce the `Provider` protocol + error taxonomy; move retry/timeout into the engine; remove DB access from `kubernetes.py`; secure temp-file handling + startup sweep; build the conformance suite.
*Closes:* **H4–H6** (retry), **H17/H18** (temp files, DB-in-provider), C1 groundwork.

**Phase 4 — Provisioning & destroy as workflows.** Express provision (create → ssh → k3s → kubeconfig) and destroy on the engine with compensation; resume via reconciliation cursor.
*Closes:* **C1** (compensating undo destroys leaked droplets), partial-K3s leak, stuck-PROVISIONING recovery.

**Phase 5 — Retire the god object & repo violations.** Split `ClusterManager`; route all 7 direct-ORM sites through repositories; deployment orchestration becomes workflows.
*Closes:* **H10, H13, H14, H15**.

**Phase 6 — Security & polish.** Re-encrypt secrets in `deployment_audits`; auth on `/health/detailed`; input validation; fix H11 (return-in-loop), H12 (SSE disconnect cleanup).
*Closes:* **H1/H2/H3, H11, H12**.

Phases 1–3 are the load-bearing ones; 4–6 fall out of the architecture. Each is a vertical slice, validated against the ported acceptance spec as v2 approaches parity.

## Success criteria

- Green against the ported acceptance spec (`test_deployment_flow.py`) — this is parity / cutover-ready.
- No salvaged edge behavior regressed: reconciliation intents, `InfrastructureUnreachableError`, CIDR allocation, GHCR fallbacks, Tart/Rosetta all behave as in `reference-code/`.
- `transition()` is pure, has exhaustive `(state × event)` coverage, and needs no mocks.
- No state/notification mismatch is representable (H7 class is structurally impossible).
- Every provider passes one shared conformance suite; adding a provider = implementing one protocol.
- Provisioning failures leave no orphaned infra (compensation, verified by reconciliation).
- The workflow vocabulary's *grammar* has not grown; only its *verbs* have.
- `ClusterManager` no longer exists as a god object.
