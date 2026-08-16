---
title: DR-0002 — Design lock ratified; UI adapts; outbox pruning
type: decision
status: active
created: 2026-07-12
updated: 2026-07-12
---

# DR-0002 — Design lock ratified: taste calls blessed, UI-adapts direction, outbox pruning

## Context

The 2026-07-12 design-lock workflow left ten taste calls open in `DESIGN.md` and two questions were raised in review: (a) does the design change contracts toward the UI, and (b) is the DB outbox the right event-propagation mechanism? A full audit of the v1 SPA's server-contract consumption was run to answer (a) — results in `docs/design/ui-contract.md`, including verification that the SPA already re-fetches on SSE reconnect (the one assumption the outbox design made about the UI).

## Decision

1. **All ten taste calls in `DESIGN.md` are blessed as chosen** (A1–A3, B1–B3, C1–C2, D1, D3). No flips. `DESIGN.md` moves `proposal` → `active`.
2. **The UI adapts to the clean v2 contract — no compatibility shims.** We own the SPA, so no server-side `display_status` synthesis, no legacy field aliases. The migration worklist and the binding server obligations it produces (notably: `deployment_status_changed` payloads carry `deployment_id`; SSE keepalives ≤120s; query-param SSE auth; the `pod_status_changed`/`snapshot_restore_completed`/`reconciliation_skipped` topics survive) are normative in `docs/design/ui-contract.md`.
3. **Outbox confirmed as the event-propagation mechanism** (transactional outbox; the only design that closes H7 by persistence without broker infra). One addition: **pruning policy** — the `EffectExecutor` runs housekeeping that deletes `effects_outbox` rows with `status='done'` older than a retention window (default **7 days**, `AppConfig.outbox_retention_days`), hourly. `dead` rows are never auto-pruned (they are reconciliation's surface). This amends `DESIGN.md` Decision 1.

## Consequences

- The build fans out against a fully ratified design; further changes require new DRs.
- The SPA port becomes a scoped, ordered worklist (2 M items, 1 L, rest S) instead of an unknown.
- Two open UI product calls are deferred, tracked in `ui-contract.md`: the scheduled-jobs tab's replacement (expose `timers` or drop) and the `/health/detailed` scheduler-block shape. Both must be decided before the Workflows page rewrite.
- Outbox growth is bounded; `dead`-row visibility is preserved for reconciliation.
