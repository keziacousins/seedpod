---
title: DR-0016 — Deployment API exposes deployed_at, derived from the row's created_at
type: decision
status: active
created: 2026-07-17
updated: 2026-07-17
---

# DR-0016: Deployment API `deployed_at` is the row's `created_at`

**Status: ACTIVE — ratified by Kezia, 2026-07-17. Round-6 halt (api-deployments crown judge, run
`wf_3c2e5583-540`, 2026-07-17).**

## Problem

`docs/design/ui-contract.md` lists `deployed_at` as an SPA-consumed field on three deployment
endpoints — `GET /api/deployments` (row 43, DeploymentList.jsx:118), `GET /api/deployments/{id}`
(row 44, DeploymentDetail.jsx:345), `GET /api/clusters/{id}/deployments` (row 40) — and its
"Required change" column lists **no** rename for it. But the ratified v2 schema
(`seedpod/data/migrations/0001_initial.sql`) has no `deployed_at` column: the deployments table
carries `deployed_by`, `created_at`, `updated_at`. Nothing in the specs pinned how the API
produces `deployed_at`, so the api-deployments build silently exposed `created_at` under its own
key (diverging from the contract shape the SPA reads) and locked it in with a test. Needs a
ratified rule so the value AND the key are correct and the SPA migration team knows whether to
touch those two read sites.

## Decision (PROPOSED)

**The deployment API DTOs expose `deployed_at`, sourced verbatim from the deployment row's
`created_at`. No schema change; no SPA change for this field.**

This is a faithful 1:1 rename, not a semantic substitution: v1's `deployed_at` was
`Column(DateTime, nullable=False, default=utc_now)` (`reference-code/seedpod/seedpod/core/
database.py:165`) — i.e. stamped at row **creation**, never at deploy-completion. v2 canonicalized
that creation timestamp as `created_at` (consistent with every other v2 table); the API surface
keeps v1's public name `deployed_at`. So:

- the response JSON key stays `deployed_at` (ui-contract contract preserved, its no-rename column
  was correct); its value is `record.created_at`.
- applies to all three deployment endpoints above and any deployment DTO the API emits.
- the SPA keeps reading `deployed_at` unchanged — **no new worklist item** for this field
  (recorded here so the SPA team does not add one).

## Consequences

- The DeploymentResponse / deployment DTO maps `created_at -> deployed_at`; the build's
  `created_at`-keyed field and its test are corrected to `deployed_at`.
- `deployed_by` already exists as a column and is exposed as-is.
- If a future need arises for a genuine "activated/deployed-complete" timestamp (distinct from
  creation), that is `updated_at`-at-ACTIVE or a new field — out of scope here; v1 never had one.

## Alternatives considered

- **API returns `created_at`; SPA renames its two `deployed_at` reads to `created_at`** — rejected:
  breaks ui-contract's stated (unchanged) contract, forces an un-inventoried SPA change at
  DeploymentList.jsx:118 / DeploymentDetail.jsx:345, for zero benefit — the value is identical.
- **Add a `deployed_at` column to the schema** — rejected: the Phase-0-ratified schema deliberately
  uses `created_at` as the single creation timestamp; v1's `deployed_at` was creation-time anyway,
  so a separate column would just duplicate `created_at`.
