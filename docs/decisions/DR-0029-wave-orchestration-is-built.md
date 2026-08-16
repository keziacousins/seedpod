---
title: DR-0029 — v2 builds wave orchestration, realising a v1 plan that was never implemented
type: decision
status: active
created: 2026-08-08
updated: 2026-08-08
supersedes: DR-0028 decision 5 and DR-0028 Erratum E1
---

# DR-0029: wave orchestration is built, not ported

**Status: ACTIVE — ratified by Kezia, 2026-08-08.** Raised by Round 10's third consecutive halt on the
`load-and-plan` component, which traced to a defect in the specs rather than in any build.

## Problem

`deploy.plan_waves` halted three times. All three halts have one root cause, and it is not the build.

**`docs/design/seam-b-engine.md:214-218` specifies a v1 feature that v1 never built.** It describes a
per-service `deploy_wave` ranking (default 3) with unmatched documents falling to a leading wave 0.
Searching v1:

- `deploy_wave` appears **only** in `reference-code/seedpod/PLAN-wave-orchestration.md` — a design
  plan. **Zero** occurrences in v1's code, v1's config, or any shipped v2 profile.
- That plan's own problem statement records what v1 *actually* does: "Current approach uses busybox
  init containers in each pod to poll dependencies (e.g. `nc -z postgres:5432`)". Those init containers
  are still in the shipped templates — **20+ across 11 files** in `config/manifest-templates/exampleco-stack/`.

**And seam-b's citation is wrong.** `seam-b:218` annotates the wave-0 tier "gotcha 17". The real
gotcha 17 (`docs/design/seam-a-core.md:405`) is about Cloudflare DNS and Traefik HelmChartConfig
dispatch being best-effort on `DEPLOYING`. It has nothing to do with resource ordering.

DR-0028 decision 5 then made this worse by saying "`Wave` stays as Seam B specifies" while *also*
framing the model as generalizing v1's binary split — two different instructions — and DR-0028
Erratum E1 made the unbuilt ranking binding. The build could not compute "matched to a service"
because `DeploymentProfile` carries only `persistence_services` (DR-0028 decision 2), so it substituted
a document-kind test, which puts `StatefulSet`/`DaemonSet`/`CronJob` on the wrong side. It halted
rather than paper over that. Correctly.

This is the **third instance of one class in two rounds**: `${SERVICE.INTERNAL_URL}` (DR-0028's own
Problem section), the `DnsRecordRef` stand-in (Round 8b), and now `deploy_wave`. **This project's
design documents describe intended features as though they were built.** Only a direct grep of v1's
source distinguishes the two.

## Decision

**v2 builds wave orchestration**, realising `PLAN-wave-orchestration.md`. It is not a port, and it must
not be described as one. DR-0028 decision 5 and Erratum E1 are superseded.

The rationale is the plan's own, and it is a real defect rather than an aesthetic preference: init
containers "work for first deploy but break on redeploy — when all pods restart simultaneously,
circular waits and race conditions occur". v2 would otherwise be porting a mechanism known to be broken
on the path that matters most.

`seam-b-engine.md:214-226` is therefore **correct as written** and needs no amendment. Its
"gotcha 17" annotation is a mis-citation and should be corrected in place to cite
`PLAN-wave-orchestration.md` instead.

### 1. The profile schema gains `deploy_wave`

Per service, an integer, **default 3** — so a profile that declares nothing behaves exactly like
today's single apply (the plan's own back-compat rule, migration step 1). The five shipped profiles
gain explicit values following the plan's worked example: datastores wave 1, migration/init Jobs
wave 2, application services wave 3.

### 2. `DeploymentProfile` carries the service-to-wave mapping

DR-0028 decision 2 trimmed `DeploymentProfile` to `persistence_services`, which structurally prevents
computing "matched to any service". It gains the full service-name-to-`deploy_wave` mapping, and
`_build_resolved_config` writes it into `resolved_config` so `deploy.load_audit` reads it back off the
audit like every other resolved fact. `persistence_services` remains — it drives restore attachment.

### 3. Wave grouping is by service name, never by document kind

The plan is explicit (its "Manifest Grouping" section): match a document's `metadata.name` to a service
in the profile to get its `deploy_wave`; **documents matching no service go to wave 0**. Wave 0 is
implicit and always applied first — RBAC, ConfigMaps, Secrets, ghcr-secret.

Reuse the three-heuristic matcher DR-0028's Consequences already require
(`metadata.name` equality-or-prefix, `metadata.labels.app` / `spec.template.metadata.labels.app`,
`spec.selector.matchLabels.app`) — the same classification, now answering "which service" rather than
"is it a database service".

**Not a kind test.** A kind test cannot express this: `StatefulSet`, `DaemonSet` and `CronJob` are
workloads that belong to services, and a `Secret` belonging to a named service is not wave-0
infrastructure. Kind answers a different question — what `Wave.jobs`/`Wave.deployments` can gate on.

### 4. Per-wave readiness, per the plan

Deployments gate on rollout status; Jobs gate on `condition=complete`. `Wave.deployments` and
`Wave.jobs` are populated by document kind — this *is* the question kind answers.

### 5. The restore attaches to the wave carrying persistence services

`seam-b:225` says restore attaches to the persistence wave when the profile declares
`data_initialization`. Under waves that is the wave containing `persistence_services` — wave 1 in the
plan's example. No separate empty-docs wave (DR-0028 Erratum E1 point 2 survives; only its wave-model
framing is superseded). DR-0028 Erratum E2 also survives unchanged: a restore requested against a
profile with no persistence services raises rather than being silently dropped.

### 6. Init containers are NOT removed in this round

The plan sequences their removal as migration step 4, after wave values are added and the grouping
works. They are harmless once waves order correctly — each `nc -z` check passes immediately because the
dependency is already up — and removing 20+ waits across 11 templates is a separate, independently
testable change that deserves its own smoke.

Record it as a tracked follow-up. Do not let this round quietly become that one.

## Consequences

- **This enlarges Round 10 beyond "the deploy half of P0 #0".** That is accepted deliberately: the
  alternative is porting a mechanism v1's own authors documented as broken.
- `kube.apply_docs` remains total on empty input (DR-0028 Erratum E1 point 3 survives).
- A test must pin wave-0 membership by the *service-name* rule specifically: a `StatefulSet` belonging
  to a declared service lands in that service's wave, not wave 0; a `Secret` belonging to no service
  lands in wave 0. A test that only exercises `Deployment` and `ConfigMap` cannot distinguish the
  service-name rule from the kind test this DR exists to reject.
- The default-3 back-compat rule must be pinned too: a profile declaring no `deploy_wave` anywhere
  produces exactly one wave, behaving like today's single apply.
- **Correct `seam-b-engine.md:218`'s "gotcha 17" annotation in place** to cite
  `PLAN-wave-orchestration.md`. `docs/design/` is normative and edited in place; leaving a known
  mis-citation is how the next round inherits this same halt.
- The redeploy optimisation the plan sketches (skipping unchanged waves) is explicitly **out of scope**
  — the plan itself labels it "Future".

## Alternatives rejected

- **Port what v1 actually did** (single apply, plus the DB-first split only for restores). Smallest and
  faithful, and rejected because it ports the redeploy defect the plan was written to fix, leaving 20+
  init containers as the ordering mechanism.
- **Wave 0 only, no ranking.** Adds the infrastructure tier without the ordering that makes it useful;
  the deadlock it guards against is v1-equivalent, so it buys little and still leaves the redeploy bug.
- **Amend seam-b to drop the wave model.** Would have made the specs self-consistent by deleting the
  better design. Seam-b was right; the failure was that nothing recorded its source as an unbuilt plan.
