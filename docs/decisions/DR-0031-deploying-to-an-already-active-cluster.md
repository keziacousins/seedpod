---
title: DR-0031 — a deployment born onto an already-ACTIVE cluster must start; where ClusterReady comes from
type: decision
status: active
created: 2026-08-09
updated: 2026-08-09
amended-by: "DR-0031 Erratum E1 (below)"
---

# DR-0031: deploying to an already-ACTIVE cluster

**Status: ACTIVE — ratified by Kezia, 2026-08-09, adopting Option B.** Drafted the same day from
smoke 4's finding (backlog #13) plus a primary-source read of v1. **Erratum E1 (below) amends the
mechanism** — the ratified text proposed giving the cluster table a `DeployRequested` row;
implementation proved that violates an invariant this DR had not checked, so the escalation carries
a new cluster-side event instead. The decision (Option B: the machine owns it, escalation happens
once in the Dispatcher) is unchanged.

## The defect

A deployment born onto a cluster that is **already ACTIVE never starts**. It sits in `pending` with
**zero** workflow runs, forever.

Smoke 4 (2026-08-09) saw this on real infrastructure: deployment `64db05b5`, created against the
live cluster `898acba8` moments after that cluster's first deployment went `active`. No
`deploy-waves` run was ever created. The deployment only left `pending` when the cluster was
destroyed and the `ClusterGone` cascade moved it to `destroyed`.

**The mechanism.** `PENDING → DEPLOYING` has exactly one driver — `ClusterReady`
(`core/machine.py`'s deployment table, `(DeploymentState.PENDING, ClusterReady)`). `ClusterReady`
has exactly one emitter in the entire tree: `core/machine.py:314`, inside
`_cluster_provisioning_provision_succeeded` — the `provisioning × ProvisionSucceeded` cascade. A
cluster that is already ACTIVE has, by definition, already passed through that transition and will
never re-enter it. Nothing else emits `ClusterReady`; nothing else advances PENDING.

## Scope: this is not a `version_update` special case

Two dispatch sites birth a deployment with `DeployRequested`, and **both** are affected:

| Site | Path | When it strands |
|---|---|---|
| `deployment_service.py:640` (`version_update`) | reuses the one ACTIVE cluster for repo/branch/env | whenever a cluster already exists — i.e. every redeploy |
| `deployment_service.py:800` (`redeploy`) | births onto `original.cluster_id`, **always** the same cluster | **unconditionally**, whenever that cluster is ACTIVE |

`retrigger` (`:806`) delegates to `version_update`, so it inherits the fault — three user-facing
entry points, two code sites. `redeploy` is the starkest: it exists *only* to redeploy to an
existing cluster, so it is broken in its entire intended use.

## Primary source: v1 had no such branch to fall into

Per this repo's own rule — write DRs from v1's code, not from seam prose — v1 was read directly.
**v1 does not use an event cascade here at all.**

`cluster_manager._schedule_deployment_work` (`reference-code/seedpod/seedpod/orchestrator/
cluster_manager.py:1443`) creates the deployment row as `pending` and then **unconditionally**
schedules `_execute_deployment_workflow` as a background job. Whether the cluster needs provisioning
is a branch *inside* that already-scheduled workflow (`_ensure_target_cluster`), not a precondition
for scheduling it. v1's own `version_update` even estimates "1-2 minutes # Redeployment" for the
existing-cluster case — the path was deliberate and supported.

v2 replaced "always schedule the work; decide about provisioning inside it" with "wait for the
cluster machine to announce readiness". That is a better design — it is how the whole event-driven
spine works — but the translation **dropped a branch v1 had**: the case where readiness is already
true on arrival. This is precisely the failure mode `CLAUDE.md` names as the one that matters
("silently regressing edge behavior v1 already got right").

## Not part of this decision: superseding already works

The obvious adjacent question — what happens to the previously-`active` deployment — **needs no
decision, and no code.** `core/machine.py:701` (`_deployment_deploying_deploy_succeeded`) already
cascades `SupersededBy` to every ACTIVE deployment on the cluster except the new one, which is
exactly v1's rule (`jobs/state/deployment_job.py:655-664`: mark all previous active deployments
superseded, at deploy **success**, not at request time). It is correct and unreachable only because
nothing gets that far. Once a redeploy starts, superseding follows for free.

*(An earlier draft of backlog #13 said `superseded_by` "is currently never set" and implied this DR
had to settle it. That was wrong — it is never set because the redeploy never runs. Corrected here
and in the backlog.)*

## The decision to make

Something must emit `ClusterReady` (or an equivalent advancing event) for a deployment born onto an
ACTIVE cluster. Two defensible homes:

### Option A — the API service chains it

After `DeployRequested`, when the target cluster is ACTIVE, call `Dispatcher.apply()` a second time
in the same transaction with `ClusterReady`.

- **For:** `runtime/dispatcher.py`'s own module docstring already names this as an intended usage —
  "API `DeployRequested`+`ClusterReady` chains" — and `apply()` carries the optional `tx=` parameter
  specifically to support same-transaction chaining. Mechanically it is a few lines, and the
  precedent for chained applies in one `uow()` already exists at `:800`.
- **Against:** it puts the decision "this cluster is ready" in an application service, duplicating
  authority the cluster machine otherwise holds exclusively. `ClusterReady` is a `Cascaded` event
  whose sole existing emitter stamps `actor="cluster-machine"`; an API-emitted one is either a lie
  about its own provenance or a second actor value for the same event. And with **two** dispatch
  sites plus any future one, the rule has to be re-remembered at each — the shape of bug that
  recurs.

### Option B — the cluster machine emits it

Give the cluster table an `(ACTIVE, DeployRequested)` row that cascades `ClusterReady` to the
cluster's PENDING deployments — the mirror of the `provisioning × ProvisionSucceeded` row that
already exists.

- **For:** every `ClusterReady` stays inside `core/machine.py`, one emitter concept, one actor. The
  core's exhaustive `(state × event)` totality tests would cover the new row by construction, which
  is exactly the guarantee that would have caught this originally. Fixing it once at the machine
  covers all present and future birth sites, rather than each service remembering.
- **Against:** it requires `DeployRequested` to reach the *cluster* aggregate, which today it does
  not — `apply()` targets one aggregate per call and this event targets `("deployment", id)`. That
  is a real change to event routing, not just a table row, and it is the larger piece of work.

**Recommendation: Option B**, with the routing change scoped tightly to delivering `DeployRequested`
to the cluster aggregate as well. The deciding argument is the table above — two sites today, both
wrong the same way, neither noticed for a full round. A fix that each caller must remember is a fix
that the *next* caller will miss, and Option A's cheapness is exactly what makes it easy to forget.
If the routing change proves more invasive than it looks, Option A is an acceptable interim **only**
with both sites fixed together and a test that fails if a third appears unchained.

## Consequences

- **This unblocks `ensure_rollouts` only halfway — verified 2026-08-09, and the second half is a
  NEW finding.** The rule restarts only when every resource in a wave was `unchanged`, which needs a
  second deploy of an unchanged stack; before this fix no second deploy could run at all. With the
  fix, two consecutive redeploys onto the live cluster produced *identical* summaries:

  | wave | `kube.apply_docs` result | `ensure_rollouts` |
  |---|---|---|
  | 0 | all `unchanged` (`secret/ghcr-secret`) | `{}` — correct, but wave 0 has **no Deployments**, so there was nothing to restart |
  | 3 | **mixed**: `secret/tailscale-auth` + `daemonset.apps/tailscale` `configured`, the other six `unchanged` | `{}` — correct: not all-unchanged, so do not restart |

  So the **non-restart** branch is now proven right on real infrastructure, and the **restart**
  branch is still unproven — and on this profile it is *structurally unreachable*, because
  `tailscale-auth` reports `configured` on every single apply (a `stringData` Secret never round-trips
  identically), which drags the DaemonSet consuming it along too. Reproduced on two consecutive
  deploys, so it is a stable property, not a first-redeploy artifact.

  **Proving the restart branch therefore needs a profile whose wave contains a Deployment and no
  `stringData` Secret** — a new backlog item, not something a redeploy of `exampleco-web-2` can ever
  deliver. The `configured`-every-time behavior is worth a root-cause look in its own right: it means
  any wave containing tailscale can never satisfy the restart precondition.
- **Test the consequence, not the decision.** The reason 2192 green tests missed this:
  `test_reuses_existing_active_cluster_for_same_repo_branch_environment` asserts the cluster is
  reused and the deployment id differs — never that the second deployment *deploys*. Whichever
  option is ratified must land a test that asserts the deployment reaches DEPLOYING and a run is
  created, for **both** dispatch sites.
- **Verify on real infrastructure.** The fix is cheap to prove: a second `seedpodctl deploy` against
  a live cluster, which also finally exercises `ensure_rollouts`. Do it before smoke 5 on `tart` —
  carrying a known-broken redeploy onto an unexercised provider re-creates the
  two-variables-at-once problem that sent smoke 4 to DigitalOcean.

---

## Erratum E1 — the escalation carries `DeploymentPending`, not `DeployRequested`

**Ratified 2026-08-09 (Kezia), during implementation.** Amends the mechanism only; Option B and every
argument for it stand.

**What the ratified text said.** "Give the cluster table an `(ACTIVE, DeployRequested)` row." That is
now wrong on one point.

**Why.** `tests/core/test_totality.py`'s `test_event_type_unions_partition_an_event_exactly` pins
that `ClusterEvent` and `DeploymentEvent` are **disjoint** — every event kind addresses exactly one
aggregate. Routing `DeployRequested` to clusters breaks that partition. This DR did not check the
invariant before recommending the mechanism.

**What changed.** A new cluster-side `Cascaded` event, **`DeploymentPending(deployment_id)`**
("a deployment is waiting on you"). `Dispatcher.apply` **translates** rather than forwards:
a deployment's `DeployRequested` becomes the cluster's `DeploymentPending`. The cluster table's
`(ACTIVE, DeploymentPending)` row cascades `ClusterReady` back to the cluster's PENDING deployments;
all nine other states are spelled out explicitly and `_ignore` (`NEW` is `_invalid` — pre-persistence).
The partition survives, and the name is more honest: the cluster is not being asked to deploy
anything, it is being told a dependant is waiting.

**Rejected alternative:** amend the test to allow `{DeployRequested}` as a named overlap. That saves
one class at the cost of making "an event belongs to one aggregate" conditional for every future
reader — the same trade this repo declined elsewhere, and not one to make for convenience.

**Two further drafts died to existing tests, recorded because each names a real invariant:**

1. **No `Persist`.** The first cut returned the `Cascade` alone, reasoning that a cluster which stays
   ACTIVE should not bump its version. `test_retrigger` failed with
   `UNIQUE constraint failed: effects_outbox.effect_id`: identity is
   `"{aggregate}/{id}@{to_version}#{ordinal}"`, so an effect-producing transition that does not
   advance `version` collides with itself on the second run. The version bump is mandatory.
2. **No `Notify`.** The second cut kept `Persist` but suppressed `Notify` to avoid an SSE
   `cluster_state_changed` with `old_status == new_status`.
   `test_cluster_transitions_never_double_persist_or_mismatch_expected_version` failed: this table's
   law is **exactly one `Notify` per `Persist`** (restated in `core/machine.py`'s own module
   docstring). A redundant refresh signal is harmless; a conditional invariant is not.

**Consequence for the record:** three separate existing tests rejected three wrong implementations of
a ratified decision. The DR was right about *where* the fix belongs and wrong about *how* to express
it, and the test suite — not review — is what established that.
