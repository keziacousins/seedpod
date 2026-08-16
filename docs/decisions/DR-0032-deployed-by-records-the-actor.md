---
title: DR-0032 — `deployed_by` records the actor, uniformly, at every deployment birth site
type: decision
status: active
created: 2026-08-09
updated: 2026-08-09
---

# DR-0032: `deployed_by` records the actor

**Status: ACTIVE — ratified by Kezia 2026-08-09, after smoke 7 proved it on real infrastructure.**
Raised by smoke 6 (2026-08-09), the first run of the SPA against a live v2: the "Deployed By"
column was empty on every row of every deployment table, which is what prompted reading the write
path rather than the prose.

**Proven, not just implemented (smoke 7, 2026-08-09, DigitalOcean).** `api:kezia` rendered at all
four SPA sites, from two of the three birth sites — `_deploy`'s success branch (first deploy) and
`redeploy` (second deploy) — and the value **survived the full transition sequence**
`pending → deploying → active → superseded → destroyed` unchanged. That is the row-only-column
argument under "Why the blast radius is one function" observed rather than reasoned. The
manifest-resolution-failure branch remains covered by the service tests only: reaching it on real
infrastructure still births a cluster row in the same transaction, so it is not worth a droplet.

## Context

`deployments.deployed_by` is **never populated in v2**. `_birth_deployment_row`
(`seedpod/app/services/deployment_service.py:1348-1356`) hardcodes `deployed_by=None` and takes no
parameter for it, and there is no other writer anywhere in `seedpod/`. Meanwhile three API responses
serialize the field (`api/routers/deployments.py:171,208`, `api/routers/clusters.py:210`) and four
SPA sites render it (`DeploymentList.jsx:116`, `DeploymentDetail.jsx:352`, `ClusterDetail.jsx:377,766`),
so the column is *structurally* always empty — not empty because nobody has deployed, empty because
nothing can ever fill it.

**v1 populated it, but not with one consistent kind of value.** Read from v1's source, not its prose:

| v1 path | v1 wrote | source |
|---|---|---|
| webhook / version-update | the triggering **repo** (`"exampleco-web-2"`) | `cluster_manager.py:1200,1465` — `context.repo or "system"` |
| preset / ad-hoc deploy | the **username** (`"kezia"`) | `presets.py:807,827` — `context.initiated_by` |
| redeploy from audit | the **username** | `cluster_manager.py:1787` |
| `execute_deployment` | the **repo**, read back from the stored audit | `cluster_manager.py:1896` |

v1's `DeploymentContext` (`cluster_manager.py:71-79`) carried *both* `repo` and `initiated_by`, and
different entry points reached for different ones. The webhook path had `initiated_by` available and
still stored `repo`. So one column held two different kinds of thing — "what pushed this" for
CI-driven deploys, "who clicked this" for human ones — and a reader could not tell which without
knowing the trigger.

v2 already computes a single uniform actor string at the HTTP boundary — `f"api:{api_key.username}"`
(`api/routers/deployments.py:230,371,385`, `api/routers/presets.py:207`) — whose format is pinned by
`Event.actor`'s docstring (`core/events.py:92`): `'api:<user>' | 'reconciler' | 'health' |
'engine:run:<id>' | 'timer:<key>' | 'cluster-machine'`. It is threaded to every deployment birth site
already, because each one builds a `DeployRequested`/`DeployRejected` from it immediately afterwards.
It is also the value the cluster state audit records for the very same request — the `api:kezia` seen
against a deploy in smoke 6.

## Decision

**`deployed_by` records the actor string, uniformly, at every entry point** — the same value the
state audit records for the same request. One notion of "who did this", spanning the audit trail and
the column, replacing v1's two-kinds-of-thing-in-one-column.

This is a **deliberate divergence from salvaged v1 behavior** (DR-0001's trigger for a DR), not a
port. It is consistent with DR-0002's precedent: we own the UI, so the SPA adapts to the clean v2
contract rather than v2 reproducing a v1 shape that was never coherent.

**This DR unfreezes `DeploymentService` for this change only.** DR-0030 unfroze
`SnapshotService.restore` and said in terms that it "is not a general licence to edit committed
services — the next such request needs its own answer." This is that next request, and this is its
answer.

### The change

Add `deployed_by: str | None = None` to `_birth_deployment_row` and pass `deployed_by=actor` at its
three call sites in `deployment_service.py`:

- `_deploy`, manifest-resolution-failed branch (~:612) — a *rejected* deployment still records who
  triggered it; that is when the question is most likely to be asked.
- `_deploy`, success branch (~:633) — covers `version_update`, `deploy_direct` (hence
  `PresetService.deploy`) and `retrigger`, which all funnel through `_deploy`.
- `redeploy` (~:795).

Nothing else changes. `actor: str` is already a parameter of both `_deploy` and `redeploy`, so no
signature is threaded through any new layer, and no router changes.

### Why the blast radius is one function

`deployed_by` is **not** a field on the core `DeploymentRecord` (`core/records.py:87`) — it is
data-layer only. `DeploymentRepository.insert` writes it;
`DeploymentRepository.persist` CAS-updates *only* the columns `DeploymentRecord` carries. So the
value is written once at birth and **preserved verbatim across every subsequent state transition**,
with no risk of being nulled by a later persist. `tests/runtime/test_dispatcher.py:518-551`
(`test_deployment_birth_via_row_uniform`) already pins exactly this round-trip, per DR-0006 point 4.
No `core/` change, no machine-table change, no `Dispatcher` change.

## Consequences

- The column no longer answers "what pushed this" for CI-driven deploys, which v1's webhook path
  did. The triggering repo is not lost — `repo` is a parameter of `_deploy` and the triggering repo
  and branch are already recorded on the deployment audit row (`triggering_repo`,
  `triggering_branch`). If a "trigger source" display is ever wanted it belongs in its own field;
  putting it back into `deployed_by` would re-create precisely the ambiguity this DR removes.
- **Verify flag 11 (restore-history `initiated_by`) is the same family and is not in this scope.**
  When it is addressed it should adopt these semantics rather than inventing a third convention.
- Existing tests are unaffected: none asserts `deployed_by is None`, and the two API tests that name
  the field (`tests/api/test_version_update.py:459`, `tests/api/test_clusters.py:420`) assert only
  the response *key set*. New service-level tests pin that each of the three birth sites records the
  actor, and that it equals the cluster audit's actor for the same request — that equality *is* the
  decision, so it is pinned rather than left incidental.
- No other Round-6 service is unfrozen by this DR.

## Alternatives rejected

- **Strict v1 parity** (repo for webhooks, username for presets). Rejected: it faithfully reproduces
  an incoherence. A column whose meaning depends on an unrecorded trigger type cannot be rendered
  honestly in a UI — the SPA has one "Deployed By" header for all rows.
- **Leave it null and drop the column from the API and SPA.** Rejected: "who deployed this" is a real
  question, the answer is already in scope at the write site, and v1 could answer it.
- **Fix it inline without a DR.** Rejected on two independent grounds: `DeploymentService` is a
  committed, frozen service (DR-0030), and this is a deliberate divergence from salvaged v1 behavior
  (DR-0001).
