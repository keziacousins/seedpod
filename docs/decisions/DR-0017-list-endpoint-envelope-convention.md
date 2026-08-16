---
title: DR-0017 — List endpoints return a {<resource>: [...]} object envelope, uniformly
type: decision
status: active
created: 2026-07-17
updated: 2026-07-17
---

# DR-0017: List endpoints use a `{<resource>: [...]}` object envelope

**Status: ACTIVE — ratified by Kezia, 2026-07-17. Round-6 halt (api-deployments judge, run
`wf_3c2e5583-540`, 2026-07-17). Cross-cutting — settles the same question for api-clusters and
api-features.**

## Problem

The api-deployments build made `GET /api/deployments` return `{"deployments": [...]}`, but v1
returned a **bare top-level array** (`reference-code/seedpod/seedpod/api/deployments.py:516`
`return deployment_list`; the SPA does `setDeployments(data)` and renders `data` as an array,
DeploymentList.jsx:31,146). `ui-contract.md` §1 rows list response *fields* but not *envelopes*,
and its §6 worklist has no item for a shape change — so the wrapper silently breaks the SPA
absent an un-inventoried `data` → `data.deployments` edit. The same question recurs for every
list endpoint in api-clusters and api-features; it needs one pinned convention, not three halts.

## Decision (PROPOSED)

**Every v2 collection endpoint returns a JSON object envelope `{<resource>: [...]}` — never a
bare top-level array.** Uniform across the whole API. `GET /api/deployments` →
`{"deployments": [...]}`, `GET /api/clusters` → `{"clusters": [...]}`, and likewise
presets/snapshots/secrets/keys/registry list responses.

Rationale:

1. **It is the only globally-consistent rule.** Many responses are necessarily multi-field
   objects (`/health/detailed`, `/api/config/overview`, the `/api/jobs`→`/api/workflows` +
   `/api/timers` split) — a bare array can never be the uniform convention, so a wrapper is.
2. **Consistent with ratified surface.** DR-0003's `GET /api/timers → {timers: [...]}` and the
   `/api/workflows` shape already wrap; api-edge and api-deployments already build every list as
   a wrapper. This ratifies what is built rather than reverting it.
3. **Extensible.** Pagination/metadata (`total`, `next`) can be added later without a breaking
   reshape — impossible on a bare array.
4. **DR-0002 authorizes it.** We own the UI; the SPA adapts to the clean v2 contract. This is a
   deliberate, recorded contract choice, not a silent drift.

## SPA obligation (the un-inventoried change, now recorded)

`ui-contract.md` §6 gains worklist items: every list endpoint the v1 SPA consumed as a bare array
adapts `data` → `data.<resource>` — at minimum `GET /api/deployments` (DeploymentList.jsx:31,146)
and `GET /api/clusters` (ClusterList.jsx), plus any other list the v1 audit read bare. This is the
break the judge caught; recording it here (and in ui-contract) is the whole point — the SPA
migration team must not rediscover it at runtime.

## Consequences

- All three Round-6 router components (api-deployments, api-clusters, api-features) wrap every
  collection response uniformly; a bare top-level array is a contract violation the final gate
  checks.
- `ui-contract.md` is updated: a note on the envelope convention + the §6 worklist items.
- No change to the parity gate (it exercises the deployment *flow* and single-object responses,
  not list shapes) — this is SPA-contract fidelity, not a cutover blocker.

## Alternatives considered

- **Preserve v1's per-endpoint envelope (bare where v1 was bare)** — rejected: no globally
  consistent rule exists (multi-field responses force wrappers), so this yields a mixed API where
  each endpoint's envelope must be memorized; it also reverts already-built, DR-0003-consistent
  wrappers for no gain but "matching v1" on an endpoint we own anyway.
- **Lean on DR-0002 blanket latitude, record nothing** — rejected: an un-inventoried shape change
  silently breaks the SPA — the exact failure mode the ui-contract consumption audit exists to
  prevent. The latitude is real; the obligation to record the resulting SPA delta is also real.
