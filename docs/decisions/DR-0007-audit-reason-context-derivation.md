---
title: DR-0007 — Audit `reason`/`context` derive from the event
type: decision
status: active
created: 2026-07-15
updated: 2026-07-15
---

# DR-0007: Audit `reason`/`context` derive from the event

**Status: ACTIVE — ratified by Kezia, 2026-07-15. The Round-4 dispatcher/audit-repo rework may
proceed against this derivation.**
(Origin: second dispatcher halt of Round-4 run `wf_3c420b88-f97`, adversarial judge,
2026-07-15 — the spec-gap protocol.)

## Problem

Conflict 11's audit DDL deliberately keeps `reason` and `context` on both
`cluster_state_audits` and `deployment_state_audits` (while killing `trigger`/`initiated_by`
in favor of `actor`), but no spec assigns their derivation. v1 wrote both on every transition
from a caller-supplied context object (`state_manager.py:406-409`: `context.reason`,
`context.metadata`). The Round-4 dispatcher build resolved the silence by writing `NULL`
always — which silently regresses a UI-consumed surface: the SPA's cluster audit page reads
`reason` (`ui-contract.md` REST inventory, `GET /api/clusters/{id}/audit`,
ClusterDetail.jsx:89). Caught by the judge; workflow halted per the spec-gap protocol.

## Decision

Both columns derive **from the event, mechanically, inside the audit repositories' `add()`**
(which already receives the event per Conflict 3's call shape — no signature change):

1. **`reason` := the event's own `reason` field** when the event class declares one (six do:
   `ProvisionFailed`, `DeployFailed`, `DestroyFailed`, `DeployRejected`, `CancelRequested`,
   `HealthCheckFailed`-class Reports — whatever the committed union carries), else `NULL`.
   The Dispatcher never invents a reason; `NULL` is honest.
2. **`context` := `canonical_json(encode(event))`** — the full tagged event, verbatim
   (`seedpod/core/codec.py`). Every audit row thus carries the exact event that caused it:
   strictly more forensic value than v1's free-form metadata grab-bag, and safe by
   construction because events carry refs only, never secrets (Seam A law, Conflict 9).
3. Same rule for both audit tables. `created_at = event.at` and `actor = event.actor` are
   unchanged (Conflict 11).
4. API consequence (Round 6): `reason` flows to the audit endpoints at v1 parity; the DTO may
   expose `context` as-is (it is already JSON).

## Consequences

- The Round-4 dispatcher/audit-repo rework is a few lines in `add()` plus tests (a
  reason-carrying event lands `reason` + full event JSON in `context`; a reasonless event
  lands `NULL` + full event JSON).
- No amendment to Conflict 11's DDL or Conflict 3's signature — this fills a void.
- The audit trail becomes replay-grade: `(from_state, event-as-JSON, to_state)` per row.

## Alternatives considered

- **Kill both columns** like `trigger`/`initiated_by` (rejected: the SPA reads `reason` today —
  ui-contract inventory — and `context` is the only forensic record of *what happened* once
  events stop being reconstructible from logs).
- **Thread a caller-supplied metadata dict through `apply()`** — v1's shape (rejected:
  reintroduces a second, uncontracted way to smuggle "why" past the event; the event IS the
  why, and callers who want richer audit data should put fields on a reviewed event class).
- **Fallback `reason := str(event)` for reasonless events** (rejected: invents data; `NULL`
  plus the full event in `context` loses nothing).
