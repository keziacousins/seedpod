---
title: DR-0040 — auto_snapshot is inert; making a TTL destroy honour it needs a column, a verb, and a naming rule
type: decision
status: active
created: 2026-08-13
updated: 2026-08-16
amended-by: DR-0043
---

# DR-0040: `auto_snapshot` on a TTL destroy

**Status: ACTIVE — ratified by Kezia, 2026-08-14.** Raised by the 2026-08-13 TTL-expiry test of
the dev exampleco stack on `tart`. Decision items 1 and 2 are ratified as written.

## Context

Three shipped profiles — `exampleco-dev-stack-nodns`, `exampleco-staging-stack-nodns`,
`exampleco-staging-stack` — declare:

```yaml
auto_snapshot:
  enabled: true
  name_pattern: "auto-{cluster_slug}-{date}"
```

**Nothing reads it.** `grep auto_snapshot` over `seedpod/` returns three docstrings citing v1's
`_attempt_auto_snapshot` and no code. `name_pattern` has **zero** references in `seedpod/` or
`tests/`. The only snapshot-on-destroy path in v2 is the explicit API flag
`snapshot_before_destroy=true` (DR-0020), which a TTL expiry never sets.

**Observed, not inferred.** Cluster `e2fb8a85` was created with an 8-hour TTL, ran the full dev
stack, and expired unattended. The TTL machinery itself was flawless — `expires_at`
`06:19:35.328Z`, the `destroy-cloud` run created at `06:19:35.383Z` (**55ms** later), succeeded
in 4.5s, VM removed, nothing left behind. A second cluster whose provision had *failed* TTL'd 8
hours later and destroyed cleanly too, proving the destroy path is idempotent against infra
compensation had already removed. **The `snapshots` table was empty.**

Nothing was lost — that cluster held only placeholder data. The hazard is the shape, not this
instance: a stale design doc misleads a developer, but a **shipped profile** promising
`auto_snapshot` misleads an *operator* into believing data survives an unattended deletion. This
is the repo's "described as real, actually absent" pattern (PARITY-BACKLOG's table of three) one
rung worse, because the audience is whoever is running the thing rather than whoever is building
it.

## Why this is not a small fix

The obvious move — "reuse DR-0020's pre-destroy snapshot" — is not available:

1. **The TTL route never touches a service.** It is pure machine:
   `ACTIVE × TtlExpired → DESTROY_SCHEDULED`, then `DestroyDue → DESTROYING` emitting
   `RunWorkflow(workflow="destroy")` (`core/machine.py:419,442`). `seedpod/core/` is pure — no
   IO, no profile read, no snapshot. DR-0020's snapshot is a **service-layer** call inside
   `ClusterService.destroy`, on the explicit API path only.
2. **Neither destroy workflow has a snapshot step.** `destroy-cloud.yml` and
   `destroy-shared.yml` run `cluster.load_infra` → `cluster.load_kubeconfig_optional` →
   `kube.delete_daemonset` → `dns.delete_record` → (`kube.wipe_namespace`) →
   `infra.destroy_instance`. Adding one means a **new step verb**, which CLAUDE.md requires be
   "a typed, tested `Step` — reviewed, with a DR", and which moves DR-0022's catalog **32 → 33**
   against `test_registry_verb_set_is_exactly_the_dr_0022_catalog`, a hard assertion.
3. **The cluster does not know its own profile.** `clusters` carries `repository`/`branch` and
   no profile column; `deployment_profile_name` lives on the deployment side. At destroy time
   there is no source for "which profile made this, and did it want a snapshot".
4. **The naming rule is unbuilt.** `name_pattern`'s `{cluster_slug}`/`{date}`/`{time}`
   placeholders exist only in profile comments.

## Decision

**1. Where the profile comes from at destroy time.** ~~Recommended: **add
`deployment_profile_name` to `clusters`** (migration `0003`), written once at cluster birth in
`DeploymentService`, where the profile is already in hand. This is exactly DR-0032's **row-only
column** precedent — `ClusterRepository.persist` CAS-updates only the columns the record
carries, so a value written at birth survives every later transition verbatim — and it means the
cluster row answers for itself forever, including after its deployments are gone.~~
**WITHDRAWN — see Erratum E1 below.**

- *Rejected:* resolve backwards through the latest deployment at destroy time. No migration, but
  it couples destroy to deployment history and yields nothing for a cluster whose deployments
  were pruned.
- *Rejected:* stash the `auto_snapshot` block in `provider_config`. DR-0034 already recorded why
  that blob is the wrong home for anything that is not provider input — it is bound wholesale
  into `infra.destroy_instance`'s `resource_ids`.

**2. Where the snapshot happens.** Recommended: a **new verb `cluster.auto_snapshot`** in both
`destroy-cloud.yml` and `destroy-shared.yml`, placed **after `cluster.load_kubeconfig_optional`
and before `kube.delete_daemonset`** — the cluster must still be alive and reachable — carrying
`on_failure: continue`. Catalog becomes **33**; DR-0022's completeness test is widened in the
same change, which is the signal working as designed, not a nuisance.

**3. Fail-open, and say so.** DR-0020 already ratified that a pre-destroy snapshot's own success
or failure is swallowed and the destroy proceeds regardless. Same posture here, for the stronger
reason that a TTL destroy is a *deadline*: a snapshot failure must never strand a cluster the
TTL says must die. `on_failure: continue` plus a `ctx.progress` line so the operator can see
whether it ran.

**4. Absence is data.** A cluster that failed to provision has no kubeconfig and nothing to
snapshot — `cluster.load_kubeconfig_optional` already returns `Optional`. The verb no-ops
cleanly rather than failing. The 2026-08-13 run exercised exactly this case.

**5. `name_pattern` gets a real implementation** — `{cluster_slug}`, `{date}`, `{time}`,
`{branch}` — with the resulting snapshot marked `is_auto`, matching the flag DR-0020's
pre-destroy snapshots already set.

## Consequences

- One migration, one verb, one catalog bump, two workflow files, and the profile-name column
  threaded from cluster birth. A round, not a patch.
- Until it lands, **an unattended TTL destroy takes no snapshot.** If that gap is not acceptable
  in the meantime, the honest interim is to **delete the `auto_snapshot` block from the three
  profiles** so nothing shipped promises what does not exist — recorded here as the explicit
  alternative rather than left implicit.
- v1 parity: v1's `_attempt_auto_snapshot` (`orchestrator/cluster_manager.py:681`) is the
  salvage source and should be read before implementing, per CLAUDE.md's diagnose-first rule and
  the repeated lesson that seam prose is not evidence a thing exists.

## What would pin it

A test that a TTL expiry on a profile with `auto_snapshot.enabled: true` leaves a row in
`snapshots` with `is_auto` set and a name matching the pattern; and its converse, that
`enabled: false` leaves the table empty. Both are reachable with the existing engine harness and
fake verbs — no real cluster required.

---

## Erratum E1 — decision 1 is WITHDRAWN (2026-08-14, during implementation)

**Decision 1 (a new `clusters.deployment_profile_name` column, migration 0003) was wrong
and is withdrawn.** It was built, the suite went green with it, and it was then reverted.

`SnapshotService._kubeconfig_and_profile` (`snapshot_service.py:262-274`) **already
resolves a cluster's profile** — `DeploymentRepository.active_for_cluster`, falling back to
the newest deployment row, then reading its `manifest_version`. It is implemented, tested,
and load-bearing for every `create()` call that ships today. That module's own docstring
records the reasoning: v1 read `provider_config["deployment_profile"]`, "a field the actual
v2 birth path never populates", so v2 resolves through the deployment instead — the same
approach this DR's decision 1 rejected.

So the column was a second source of truth for a question v2 already answers, and the two
could disagree.

**How the error happened, because it is the interesting part.** This DR was written from
`core/machine.py` and the `clusters` schema — the layers the TTL path runs through — and
not from the service that would have to take the snapshot. That is precisely the failure
mode PARITY-BACKLOG already records ("write DRs from primary sources… every contradiction
traced to drafting from a seam summary or backlog prose rather than from the code"), and it
survived ratification because the DR's own §"Why this is not a small fix" was persuasive
about everything *except* the one file that mattered.

The two cases the column would genuinely cover — a cluster whose deployments were pruned,
and one that never deployed at all — are both cases where there is nothing to snapshot
anyway, so nothing is lost.

**Kept from decision 1:** nothing. Migration 0003 was deleted before it was ever committed;
`user_version` stays at 2.

## Erratum E2 — DR-0022 ruling 2 gains one input

Ruling 2 shrank both destroy workflows' inputs to `cluster_id` alone. They now also take
`trigger` (`"ttl_expiry" | "operator"`), and `DispatchTable.resolve` supplies it from
`RunWorkflow.args` — which had been a declared, unused field until now.

This does not weaken ruling 2, whose actual concern is that the dispatch table never
smuggles **resolvable state** into run args: `cluster.load_infra` re-reads
provider/slug/resource_ids/dns_record FRESH at run time, so a dispatch-time DNS snapshot
would be a staler second source. `trigger` is the opposite kind of thing — provenance that
nothing downstream can re-derive, because both destroy routes converge on `DestroyDue`
before the workflow starts. Carried on the timer's own injected event
(`ScheduleTimer.event`), which is the only channel that survives the convergence.

## What actually landed

- `DestroyDue.trigger` (defaulted `"operator"`), stamped `"ttl_expiry"` by the two
  TTL-expired transitions only.
- `RunWorkflow.args` threaded through `DispatchTable.resolve` for the destroy arm.
- `cluster.auto_snapshot` — the **33rd** verb; catalog and its hard completeness assertion
  widened 32 → 33.
- `SnapshotService.attempt_auto_snapshot`, fail-open, with **no `status == "active"` gate**:
  its sibling `attempt_pre_destroy_snapshot` has one and is right to, because it runs before
  dispatch — this runs from inside the destroy workflow, by which point the machine has
  already moved the cluster to DESTROYING, so the same guard would have skipped 100% of the
  time. That would have shipped as "still inert", the exact bug this DR exists to fix.
- `_format_snapshot_name`, salvaged from v1, giving `name_pattern` its first reader.
- The step in both destroy workflows with `on_failure: continue`, placed after
  `cluster.load_kubeconfig_optional` and before `kube.delete_daemonset`.

Suite: 2441 passed, 44 skipped.
