---
title: DR-0028 — the five deploy-path DTOs become real types; four of the five fixture stand-ins were wrong
type: decision
status: active
created: 2026-08-07
updated: 2026-08-08
amended-by: DR-0028 Erratum E1 and E2 (self, 2026-08-08)
superseded-by: DR-0029 (decision 5 and Erratum E1 only; decisions 1-4 and Erratum E2 remain ACTIVE)
---

> **Partial supersession, 2026-08-08.** [DR-0029](DR-0029-wave-orchestration-is-built.md) supersedes
> **decision 5** and **Erratum E1's wave-model framing**: `deploy_wave` is a v1 plan that was never
> built, and v2 now *builds* wave orchestration rather than porting a binary split. Decisions 1-4
> (typed docs, `data_initialization` in `resolved_config`, the restore spec, `ApplyChangeSummary`),
> Erratum E1 point 3 (`kube.apply_docs` is total on empty input) and Erratum E2 (a requested restore
> is never silently dropped) all remain ACTIVE and unchanged.

# DR-0028: the deploy-path DTOs

**Status: ACTIVE — ratified by Kezia, 2026-08-07.** Drafted from a diagnose-first audit of
`tests/engine/declared_verbs.py`'s five remaining stand-ins against v1, run before Round 10's build so
the types are shaped against real consumers rather than invented ahead of them.

## Problem

Round 10 builds the seven deploy-path verbs. Five of the types they bind exist only as **fixture
stand-ins** in `tests/engine/declared_verbs.py`, each carrying a `TODO` and an honest admission that
the shape is inferred: `ManifestDoc`, `DeploymentProfile`, `Wave`, `ApplyChangeSummary`,
`SnapshotRestoreSpec`. Each must become a real type registered in `engine/registry.py`'s `NAMED_TYPES`
(which today holds only `ClusterSpecification` and `DnsRecordRef`).

Round 8b established the precedent and the warning: `DnsRecordRef`'s stand-in was `{zone, name}`, but
v1 deletes DNS records **by ID** and v2's `DnsService.delete_record` is keyed on `record_id` — a verb
built to the declared shape could not have called its own service. **A fixture stand-in is a
hypothesis, not a spec.**

Audited against v1, **four of the five are wrong**, one of them worse than `DnsRecordRef` was.

### The audit

| Stand-in | Verdict |
|---|---|
| `DeploymentProfile.data_initialization: bool` | **Wrong on three counts** (below) |
| `ApplyParams.docs: list[ManifestDoc]` | **Mismatches the provider** — `KubeApplyManifest.manifest_yaml` is a `str` |
| `SnapshotRestoreSpec.snapshot_id: str` | **Incomplete** — v1 has two modes, one needing a lookup |
| `ApplyChangeSummary` | Shape plausible; **the load-bearing semantic is undocumented** |
| `Wave` | Correct as spec'd (Seam B §2.2 verbatim), but generalizes a v1 **binary** split |

**`data_initialization` is not a bool.** In v1 it is a mapping — `restore_from_snapshot: <id>` or
`restore_from_latest: {branch, profile, max_age_days}`
(`reference-code/.../jobs/state/deployment_job.py:244-265`). It is read from **`resolved_config`**, not
the profile (`deployment_job.py:526`), and it originates in the **preset deploy request**
(`DeployFromPresetRequest.data_initialization`, `reference-code/.../api/presets.py:386,554-564`), not
profile YAML. No shipped profile declares the field. A verb built to `profile.data_initialization: bool`
could not find the snapshot it exists to restore.

**`ApplyChangeSummary`'s real semantic**, which no stand-in or spec comment records
(`deployment_job.py:598-614`): *force a rollout restart only if **every** resource was `unchanged`* —
because if anything was `configured` or `created`, kubectl has already triggered the rollout. v1
decides this with a substring scan of kubectl's stdout, which v2's `Result(stdout)` from
`_apply_manifest` does return.

## Decision

### 1. Typed docs, serialized at the provider edge

`ManifestDoc` becomes a real type (`kind`, `name`, `namespace`, `body`). The deploy verbs pass
`list[ManifestDoc]`; `kube.apply_docs` serializes back to YAML for `KubeApplyManifest.manifest_yaml`.

`deploy.plan_waves` must inspect each document's metadata to split it anyway, so parsing once and
passing typed documents beats re-parsing an opaque string at every step, and it keeps Seam B §2.2's
`Wave{docs: list[ManifestDoc]}` field list intact. The frozen `seedpod/providers/` tree is **not**
touched — the serialization happens in the verb, not the provider.

### 2. `data_initialization` is a typed mapping in `resolved_config`, sourced from the deploy request

Mirroring v1: `deploy_direct` (v2's preset path, the one v1 used) accepts it, `_build_resolved_config`
carries it, and `deploy.load_audit` reads it back off the audit like every other resolved fact. This
keeps `deployment_audits` the single reproducibility record.

The stand-in's `bool` is replaced by a typed model expressing v1's two modes. It is **not** a profile
field: restore-from-snapshot is a per-deployment choice, not a property of a profile, and no shipped
profile declares it.

### 3. `SnapshotRestoreSpec` carries both modes; the verb resolves the criteria

`SnapshotRestoreSpec` expresses both `restore_from_snapshot` (explicit id) and `restore_from_latest`
(criteria). `deploy.restore_snapshot` resolves criteria to a concrete snapshot **at execute time**, via
the existing snapshot service.

Latest-matching is only meaningful at restore time. Resolving it at deployment birth would freeze a
choice that a newer snapshot may supersede before the restore actually runs — and on the provision
path, birth can precede restore by minutes.

### 4. `ApplyChangeSummary` records the buckets; the restart rule is pinned by test

The three-way split (`configured` / `created` / `unchanged`) stands, parsed from the apply command's
stdout. The v1 rule — restart **only** when everything was unchanged — is salvaged behaviour and must
be pinned by a test that would fail if the condition were inverted.

Seam B's "unknown ⇒ assume changed" is retained, but note what it means and make it deliberate:
assume-changed implies *do not restart*, so an unparseable apply output silently skips a restart that
may have been needed. If the build cannot make that safe, it is a finding, not something to paper over.

### 5. `Wave` stays as Seam B specifies; the v1 behaviour it generalizes is a binary split

`Wave{index, docs, jobs, deployments, gate_timeout_seconds, restore}` is spec'd verbatim and does not
change. What Round 10 must reproduce is v1's actual sequence
(`deployment_job.py:526-560`): apply database manifests → wait for database pods ready (v1's
`timeout=180`, which is what `gate_timeout_seconds` generalizes) → restore → apply the rest.
`persistence_services` drives the split and already lands in `resolved_config` (ported in Round 9).

## Consequences

- All five types land in `seedpod/core/` or `seedpod/services/` as appropriate and are registered in
  `NAMED_TYPES`; `tests/engine/declared_verbs.py`'s stand-ins are replaced by imports of the real types
  so the fixture and production registries cannot drift.
- **`_split_manifests_by_service`'s classification is non-obvious salvage and must be ported
  faithfully** (`deployment_job.py:66-127`): a document belongs to a database service if its
  `metadata.name` equals or starts with the service name, OR `metadata.labels.app` /
  `spec.template.metadata.labels.app` matches, OR `spec.selector.matchLabels.app` matches. Three
  heuristics, each carrying real cases. A naive reimplementation silently regresses it.
- **v1's parse-error fail-open is a candidate not-ported**: on a YAML error it returns everything as
  "other" (`deployment_job.py:127-129`), silently skipping the database phase — and therefore the
  restore. Decide deliberately and record it loudly either way.
- The coverage-boundary test `test_workflows_fully_covered_by_the_real_registry_are_exactly_these`
  will fail on purpose as the deploy workflows come into range. Widening
  `FULLY_REGISTERED_WORKFLOWS` is the signal, not a nuisance.
- **DR-0025 part 2 is Round 10's carried obligation**: hostname-dependent values re-resolve at deploy
  time, and that DR's open question — whether `deployment_audits` stores the re-resolved manifest or
  records that it holds the pre-provision one — must be answered rather than inherited. The audit must
  not silently diverge from what was applied.

## Alternatives rejected

- **YAML strings end to end (v1 verbatim).** Closest salvage and matches the provider signature
  directly, but every step re-parses, and `Wave.docs` would have to become a string — contradicting
  Seam B §2.2's verbatim field list for no gain beyond avoiding one serialization.
- **A provider command that accepts typed docs natively.** Most internally consistent, rejected as
  disproportionate: it means editing the frozen providers tree and re-running the C-01…C-24
  conformance suite to move a serialization boundary.
- **`data_initialization` as a profile field** (where the stand-in put it). Simpler plumbing, and wrong:
  it makes a per-deployment choice look like a property of the profile, diverging from v1 for no reason
  beyond the stand-in having guessed it.
- **Building `deploy.restore_snapshot` as a stub.** Shrinks the round and leaves a registered verb that
  lies about what it does — the failure mode `UnknownVerbError` at least announces itself.

## Erratum E1 — decision 5 contradicted seam-b's actual wave model; seam-b wins

**Ratified by Kezia, 2026-08-08.** Raised by Round 10's second adversarial judge on `load-and-plan`.
**Binding; supersedes decision 5 wherever they differ.**

Decision 5 said "`Wave` stays as Seam B specifies" while simultaneously framing the model as
generalizing v1's **binary** split. Those are not the same instruction, and the second half won: the
build produced a two-wave database/application split plus an invented empty-docs wave to carry the
restore. Both halves of that are wrong against the normative spec.

`docs/design/seam-b-engine.md:214-226` specifies a **three-tier** model, and it is more specific than
decision 5 acknowledged:

- a per-service `deploy_wave` ranking, default 3 (back-compat single apply);
- **unmatched docs go to wave 0** — RBAC / ConfigMaps / Secrets / ghcr-secret applied FIRST
  (annotated "gotcha 17");
- **`restore` attaches to the persistence wave**, "only when the profile declares
  data_initialization — v1's phased DB-first deploy, as data, one loop" (`:225-226`).

### The decision

**Follow seam-b.** Decision 5's binary framing is withdrawn.

1. **Wave 0 is a real, leading infrastructure tier.** Non-workload documents apply before workloads.
   This is not cosmetic: with the database/application split otherwise unconditional, a persistence
   workload whose pod needs a Secret or ServiceAccount that lands in a later wave would **deadlock its
   own readiness gate** — waiting for pods that can never start. Wave 0 is what prevents it.
2. **The restore attaches to the persistence wave**, exactly as `:225` states. There is **no**
   separate empty-docs wave. This dissolves the empty-wave question rather than answering it.
3. **`kube.apply_docs` is nonetheless total**: an empty `docs` list is a typed no-op returning an empty
   `ApplyChangeSummary`, issuing no `KubeApplyManifest`. The frozen workflow grammar has no conditional
   with which to skip a step (CLAUDE.md — and wanting one is the stop signal, not a judgment call), so
   a verb reachable with empty input must be total. Defensive, not load-bearing, once (2) holds.
4. **v1's `deploy_wave` ranking is implemented as seam-b specifies**, default 3.

## Erratum E2 — a requested restore is never silently dropped

**Ratified by Kezia, 2026-08-08**, same halt.

v1 enters its phased path only when `data_initialization` **and** `database_services` are both present
(`reference-code/.../jobs/state/deployment_job.py:530`). So a deploy request that explicitly asks for a
snapshot restore against a profile declaring no `persistence_services` gets **no restore and no error**.

**That is a v1 bug and is deliberately NOT ported.** `deploy.plan_waves` raises a `PermanentError`
naming the mismatch — the request asked for a restore, and the profile cannot host one. Silently
dropping an explicitly requested data operation is the failure class this rebuild exists to eliminate;
it is strictly worse than failing, because the operator believes their data was restored.

Record it on the not-ported list in the established LOUD style, citing `deployment_job.py:530`, and pin
it with a test that asserts the raise rather than asserting the deployment merely proceeds.
