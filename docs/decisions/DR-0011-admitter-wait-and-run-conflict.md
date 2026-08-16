---
title: DR-0011 — Run-admitter: generalized supersede-wait; durable, scoped run_conflict
type: decision
status: active
created: 2026-07-16
updated: 2026-07-16
---

# DR-0011: Run-admitter — generalized supersede-wait; durable, scoped `run_conflict`

**Status: ACTIVE — ratified by Kezia, 2026-07-16. Conflict 2's run-admission drain rules
amended per both clauses; the effect-executor rework may proceed.**
(Origin: sixth Round-4 halt — effect-executor judge, run `wf_3c420b88-f97`, 2026-07-16.)

## Problem

**(1)** Conflict 2's admission rule is two-branch: a `RunWorkflow` blocked by
`ux_wr_one_active` either supersede-waits (destroy only) or is dropped as a `run_conflict`
(H14). Conflict 12 simultaneously guarantees "the rollback run waits for the cancelled deploy
run to reach terminal" — but a `CancelWorkflow` row only flips the victim's
`cancel_requested`; it never waits. So the seq-ordered `RW(rollback)` directly behind it
*always* hits `ux_wr_one_active` on its first pass, and the literal rule drops it — silently
regressing v1's rollback-on-every-cancel (`reference-code/seedpod/api/deployments.py:
1090-1104`), the behavior Conflict 12 exists to preserve. The two normative texts are
unsatisfiable together.

**(2)** Nothing pins the `run_conflict` notification's shape, durability, or environment. It
is the one Notify minted *by the admitter at drain time* rather than at machine decision time,
so DR-0010's decision-time scoping never attached to it. A direct broadcast (the build's
paper-over) is unscoped — leaking the event to environment-scoped keys of other
environments — and violates Conflict 1's own audit law ("every effect is a row"): a crash
between marking the RW row done and broadcasting loses the only record that an operator's run
was dropped.

## Decision

### Clause 1 — the wait branch generalizes to "blocker already unwinding"

Conflict 2's admission rules become three-branch. On `ux_wr_one_active` conflict:

1. `workflow == 'destroy'` → flip the victim's `cancel_requested` + trip the engine's token,
   then **wait**: leave the row `'pending'`, `available_at = now + 2s`, attempts NOT
   incremented. (Unchanged — destroy retains its unique power to *initiate* the supersede.)
2. **victim run has `cancel_requested = 1`** (already unwinding, whoever set it) → **wait**,
   same re-arm, attempts NOT incremented. Waiting is not failure when the blocker is already
   terminal-bound. This is what makes Conflict 12's guarantee true: `CW(deploy)` at seq N
   flips the flag, `RW(rollback)` at seq N+1 waits, admission succeeds once the victim is
   terminal.
3. Otherwise (blocked by a healthy live run, workflow not destroy) → mark row `'done'` + emit
   the `run_conflict` Notify (H14, unchanged in spirit).

The invariant is stated, not the verb list — a future machine row emitting `CW + RW(x)` in one
tuple inherits correct sequencing with no further amendment.

### Clause 2 — `run_conflict` is a durable, scoped outbox Notify

The admitter, inside the same drain transaction that marks the blocked RW row `'done'`,
INSERTs a drain-lane `Notify` row into `effects_outbox`:

- `effect_id = "{blocked_row.effect_id}#run_conflict"` (deterministic; UNIQUE makes
  crash-replay idempotent — insert with ON CONFLICT DO NOTHING);
- `aggregate_type = 'cluster'`, `aggregate_id = cluster_id`, `to_version = 0`, `ordinal = 0`;
- `environment` := the cluster row's environment (the admitter already loads it for dispatch
  resolution — drain time IS this Notify's decision time, extending DR-0010's rule);
- payload: `{topic: 'run_conflict', workflow, cluster_id, deployment_id?, blocked_by_run_id}`.

Delivery then follows the universal notify drain law (one attempt, `done`, never `dead`).
One delivery path, one audit trail, correctly scoped.

## Consequences

- Conflict 2's normative comment block is amended to the three-branch rule + the durable
  Notify (on ratification).
- Pinned tests: cancel-then-rollback tuple admits the rollback after the victim terminates
  (attempts stayed 0 while waiting); healthy-blocker conflict still drops with a durable,
  environment-scoped `run_conflict` row; crash between done-marking and notify-drain replays
  without duplicating the Notify.
- Round 6's Jobs/Workflows UI can render dropped runs from the outbox row if ever desired —
  the record exists.

## Alternatives considered

- **Rollback joins destroy as an enumerated supersede-wait verb** (rejected: encodes the verb
  list instead of the invariant; the next CW+RW-emitting machine row regresses silently — the
  exact failure shape that produced this gap).
- **CancelWorkflow waits synchronously for the victim's terminal state** (rejected: the drain
  loop must never block on engine progress — a wedged run would freeze all effect delivery;
  the re-arm pattern exists precisely to avoid this).
- **Direct best-effort broadcast for run_conflict** (rejected: unscoped — DR-0010 leak; not
  durable — a crash loses the only record of the drop; and it forks Notify delivery into two
  paths, violating Conflict 1's every-effect-is-a-row law).
