---
title: DR-0003 — Timers exposed via API; /health/detailed shape
type: decision
status: active
created: 2026-07-12
updated: 2026-07-12
---

# DR-0003 — Resolve the two open UI calls: expose the timers table; new /health/detailed shape

## Context

`ui-contract.md` (DR-0002) left two product calls open, both consequences of retiring APScheduler: the Jobs page's scheduled-jobs tab (fed by APScheduler's `next_run`/`trigger`) loses its data source, and `/health/detailed`'s `scheduler{running, job_count}` block dies. The UI rework happens in one pass, so both are resolved now rather than left dangling.

## Decision

1. **Expose the `timers` table** via `GET /api/timers` → `{timers: [{aggregate_type, aggregate_id, timer_key, fire_at}]}`, ordered by `fire_at`. This replaces the scheduled-jobs tab's data: `next_run` ≈ `fire_at`, `trigger` ≈ `timer_key` (`ttl` | `destroy`), `metadata.cluster_id` ≈ `aggregate_id`. Read-only in v2.0 (no create/cancel via this endpoint — timers are machine decisions; TTL changes go through `POST /api/clusters/{id}/extend`). Periodic loops (reconciler, health poll) are NOT timers and are not listed here; their liveness lives in `/health/detailed`.
2. **`/health/detailed` replaces the `scheduler` block** with three engine-truth blocks, `database` and `reconciler` unchanged:

```json
{
  "database":  {"connected": true, "cluster_count": 0, "deployment_count": 0, "api_key_count": 0},
  "executor":  {"running": true, "pending_outbox": 0, "dead_outbox": 0},
  "timers":    {"running": true, "next_fire_at": null},
  "engine":    {"active_runs": 0},
  "reconciler": {"running": true, "last_sync": null}
}
```

(`dead_outbox` added beyond the original proposal: it is the one number that says "reconciliation has inherited work" — cheap to compute, valuable on a health page.)

## Consequences

- The Workflows page rewrite (ui-contract worklist item 2) is fully specified: runs tab from `GET /api/workflows`, schedules tab from `GET /api/timers`.
- Health.jsx:138-155 adapts to the new blocks in the same UI pass; the 5s polling loop is unchanged.
- Two small server obligations added to `ui-contract.md`: the `GET /api/timers` endpoint and the health shape above.
