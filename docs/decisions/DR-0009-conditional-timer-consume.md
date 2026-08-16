---
title: DR-0009 — Conditional timer consume (fire only if the row is unchanged)
type: decision
status: active
created: 2026-07-15
updated: 2026-07-16
---

# DR-0009: Conditional timer consume (fire only if the row is unchanged)

**Status: ACTIVE — ratified by Kezia, 2026-07-16. Seam A §D's per-fire law and the
coherence-review Conflict 15 excerpt amended per §4 below; the timer-service rework may
proceed.**
(Origin: fourth Round-4 halt — timer-service judge, second pass, run `wf_3c420b88-f97`,
2026-07-15.)

## Problem

Seam A §D's per-fire law is "delete row + apply(decode_event(row.event)) + commit", defended by
the absorption argument: any late/raced firing is absorbed by the machine's Ignore rows. That
argument covers state-moved-on staleness only. It does **not** cover a same-key re-arm
committed between the due-scan and the fire transaction (Conflict 1's upsert re-arm; DR-0003's
TTL-extend endpoint) — a window that legitimately exists because DR-0008's lock is released
between the scan transaction and each per-fire transaction:

1. The stale event can still be **valid** at the current state (`ACTIVE × TtlExpired`,
   `DESTROY_SCHEDULED × DestroyDue`) — a cluster whose TTL was just extended gets
   destroy-scheduled at the *old* deadline. The machine cannot see the difference; no Ignore
   row can absorb this.
2. The unconditional PK DELETE destroys the re-armed row — the *new* deadline never fires.
   At-least-once delivery is broken for the re-armed timer.

v1 had no such race class: APScheduler reschedule replaced the job atomically in-process. v2's
timers-as-rows need the DB-native equivalent. The build implemented the spec's letter and
explicitly reasoned a staleness check away by citing the absorption law — the spec itself is
what's short. Caught by the judge per the spec-gap protocol.

## Decision

1. **The per-fire law becomes conditional consume.** Inside the fire transaction:

   ```sql
   DELETE FROM timers
   WHERE aggregate_type = :at AND aggregate_id = :aid AND timer_key = :key
     AND fire_at = :snapshot          -- the fire_at this scan pass saw
   ```

   - rowcount 1 → the row is exactly the one scanned: `dispatcher.apply(decode_event(event),
     tx=t)` in the same transaction, as before.
   - rowcount 0 → a concurrent re-arm or cancel won the window: **skip the apply entirely**.
     The surviving row (if any) fires at its own deadline on a later pass; a cancelled timer
     stays cancelled.
2. **Comparison is exact TEXT equality of `fire_at`** (ISO-8601, single writer format). A
   re-arm to the *same* `fire_at` fires anyway — semantically identical deadline, harmless.
   `event` is not part of the condition: upsert re-arm always rewrites `fire_at`; an
   event-payload change without a deadline change does not occur in the design.
3. **The absorption law's scope is corrected in place**: machine Ignore absorbs fires whose
   state moved on; conditional consume covers the same-state re-arm the machine cannot see.
   Together they make "at-least-once, never stale-destructive" a true statement.
4. Amendments on ratification: Seam A §D's per-fire sentence and the coherence-review
   Conflict 15 factory-excerpt comment restating it.

## Consequences

- `TimerRepository` gains the conditioned delete (or its `delete` grows the optional
  `fire_at=` guard); TimerService threads the scan-snapshot through; a few lines.
- Pinned tests: re-arm-between-scan-and-fire → no apply, re-armed row survives and fires at
  the new deadline; cancel-between-scan-and-fire → no apply, nothing fires; same-fire_at
  re-arm → fires once.
- The TTL-extend flow (`POST /api/clusters/{id}/extend`, Round 6) becomes race-proof end to
  end without the API layer knowing anything about it.

## Alternatives considered

- **Rely on the absorption argument** (rejected: `ACTIVE × TtlExpired` is valid at the current
  state — the race destroys a just-extended cluster AND loses the re-armed deadline; this is
  precisely the class of silent edge regression the rebuild exists to prevent).
- **Re-read the row inside the fire transaction and compare in Python** (rejected: two
  statements where one conditioned DELETE is the atomic idiom; equivalent semantics, more
  moving parts).
- **Scan + fire all due timers in one big transaction** (rejected: holds the DR-0008 lock
  across every due fire and its apply cascades; one poisoned fire aborts all fires in the
  batch; contradicts the ratified per-timer-one-transaction law).
- **Include `event` in the delete condition** (rejected: adds nothing — re-arm always moves
  `fire_at`; comparing serialized JSON invites false negatives on canonicalization drift).
