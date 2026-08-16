---
title: DR-0010 — SSE per-connection environment scoping survives (v1 parity)
type: decision
status: active
created: 2026-07-16
updated: 2026-07-16
---

# DR-0010: SSE per-connection environment scoping survives (v1 parity)

**Status: ACTIVE — ratified by Kezia, 2026-07-16. The sse-hub rework, the effect-executor's
`Notify.environment → broadcast()` wiring, and the Round-6 events-router obligation are
binding.**
(Origin: fifth Round-4 halt — sse-hub judge, run `wf_3c420b88-f97`, 2026-07-16.)

## Problem

`Notify.environment` exists in ratified core (`seedpod/core/effects.py`) annotated "SSE env
filter, resolved AT DECISION TIME" — but no spec pins the hub surface that would consume it.
`ui-contract.md` §2 is silent on scoping (the audited SPA authenticates with `'all'`-scoped
keys, so the audit never observed the filter), and the Round-4 brief — written from that
audit — produced an unscoped `subscribe()` and a topic+payload-only `broadcast()`, against
which v1's per-connection environment filtering (`sse_manager.py:21,23,85`) is
unimplementable. Either the filter is consciously dropped or the hub API must carry scope now;
neither call belongs to a build agent. Caught by the judge per the spec-gap protocol.

## Decision

**The filter survives, v1 semantics verbatim.** Rationale: REST responses are filtered by the
API key's environment scope (the `api_keys.environment` `'all'` sentinel and its three auth
helpers, kept by Seam D); an unscoped event stream would let an environment-scoped key watch
resources it cannot GET — an authorization asymmetry v1 did not have. `Notify.environment`
was put on the effect at decision time precisely to feed this filter; dropping the filter
orphans a ratified core field.

1. **Hub API**: `subscribe(..., environment: str | None)` — the authenticated key's scope,
   threaded by Round 6's events router; `broadcast(type, data, environment: str | None =
   None)`.
2. **Filter rule, salvaged verbatim from v1 `sse_manager.py:85`**: skip a connection iff the
   broadcast carries an environment AND the connection is scoped AND the scope is not `'all'`
   AND the scope does not match. Consequences of the verbatim rule, kept knowingly:
   unscoped broadcasts (`environment=None`) reach every connection; unscoped connections
   (`environment=None`) receive everything; `'all'` behaves as unscoped.
3. **Drain side**: the EffectExecutor passes `Notify.environment` through as `broadcast()`'s
   environment. Engine-origin notifies (`workflow_progress`, `job_*`) carry no environment
   and are therefore unscoped — v1 parity for job events.
4. The SPA needs no change (it uses `'all'`-scoped keys); `ui-contract.md` is untouched.

## Consequences

- `SSEHub` gains one routing dimension (scope on the connection, optional environment on
  broadcast); tests pin the four filter cases (match, mismatch, `'all'`, unscoped-broadcast).
- The Round-6 events router must resolve the key's environment at connect time and pass it to
  `subscribe()` — recorded here so the API build inherits it as a requirement, not a rediscovery.
- The effect-executor component (next in this round) wires `Notify.environment` → `broadcast()`.

## Alternatives considered

- **Drop per-connection scoping** (rejected: silent security regression — REST/SSE
  authorization asymmetry for environment-scoped keys; also orphans `Notify.environment`,
  a ratified decision-time field, as dead weight).
- **Filter in the events router per-message instead of in the hub** (rejected: splits
  fan-out from filtering across two modules that must agree; v1 kept the filter where the
  connections live, and the hub already owns per-connection queues).
- **Strict scoping (scoped keys receive NO unscoped events)** (rejected: breaks
  `server_shutdown`/`reconciliation_skipped`/job events for scoped connections — v1 delivered
  these to everyone, and the SPA's reconnect logic depends on `server_shutdown`).
