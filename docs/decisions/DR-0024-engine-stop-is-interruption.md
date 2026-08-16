---
title: DR-0024 — WorkflowEngine.stop() quiesces by interruption, never by cancellation
type: decision
status: active
created: 2026-08-03
updated: 2026-08-03
---

# DR-0024: `WorkflowEngine.stop()` — shutdown is an *interruption*, not a cancellation

**Status: ACTIVE — ratified by Kezia, 2026-08-03; implemented the same day.** Raised by `App.stop()`'s own docstring, which has
flagged this as "a DR-worthy follow-up" since Round 6, and forced by the 2026-08-02 teardown-deadlock
investigation (fixed in `3ea5c94`), whose root-cause write-up named leaked run tasks as the source of
the teardown traffic that made the deadlock fire.

## Problem

`App.stop()` stops every collaborator except the one that generates the most work. Its docstring is
candid about it:

> the committed `WorkflowEngine` exposes no `stop()`/cancel-all primitive … There is therefore
> nothing to await here for the engine; any live in-process run tasks are left to finish on their own
> event-loop schedule.

`WorkflowEngine` *does* track its tasks — `self._runs: dict[run_id, _RunHandle(task, cancel_token)]`,
populated by `_adopt()` and pruned by a done-callback — so the absence is an omission, not a
structural limit. Three consequences, in increasing severity:

1. **Unbounded teardown traffic.** Live run tasks keep opening `uow()` transactions and calling
   `poke()` during and after `App.stop()`. That traffic is what put `TimerService`/`EffectExecutor`
   into the cancel-swallow window and left the DR-0008 write lock orphaned (`3ea5c94`). Those two
   bugs are fixed, but the *source* is not: shutdown still races an unbounded number of live steps.

2. **Ordered teardown racing unordered work.** `App.stop()` calls `subprocesses.shutdown()`
   (SIGTERM every tracked child, then SIGKILL) and `db.dispose()` while run tasks may be mid-step. A
   live `k3s.install` has its `ssh` child killed out from under it and reports a confusing transport
   error rather than an interruption; a live step can be inside a transaction when the engine is
   disposed. Nothing in `App.stop()`'s "exact reverse of `start()`" ordering can help, because the
   engine is not in the ordering at all.

3. **Tasks outliving their App.** A dangling run task holds references to repositories, providers and
   a disposed `Database`. In the test suite each `make_app` builds a fresh App per test, so this
   accumulates across a session.

Note the engine is admitted into work regardless of `background_tasks`: `EffectExecutor` calls
`engine.start(run_id)` from its drain loop, and the executor "always runs" (Conflict 15). Only
`resume_inflight()` sits behind the `background_tasks` flag. So this is not a production-only
concern — it is live in every test that admits a run.

### The question this DR actually settles

The tempting implementation is to reuse what already exists: `cancel(run_id)` commits
`cancel_requested=TRUE`, trips the in-memory `CancelToken`, and hard-cancels the task after
`cancel_grace_seconds` (G1/G3). Calling that for every live run at shutdown would be **catastrophic
and silent**: tripping the token is the *user cancelled this run* signal, and every
`provision-*.yml` declares `on_failure: compensate`. **Restarting the seedpod process would destroy
every cluster it was mid-way through provisioning** — deleting a real droplet that a user asked for,
because an operator restarted a service. That is precisely the class of silent edge-behaviour
regression CLAUDE.md names as the one failure mode that matters.

The correct model is already built and already tested. The engine treats "the process died mid-step"
as first-class: `workflow_steps.interrupted_count`, `_resume_replay_limit`, `run_repo.resumable()`,
`resume_inflight()` adopting every non-terminal run at boot, and the rule that an interrupted
**non-idempotent** step fails permanently rather than being blindly replayed. Shutdown should
therefore *become an interruption* — deliberately, bounded and ordered — instead of being one
accidentally, at process exit, in whatever order the event loop happens to unwind.

## Decision

**1. Add `WorkflowEngine.stop(*, grace_seconds: float = 5.0) -> None`.** Idempotent. Same shape as
`TimerService.stop()`/`EffectExecutor.stop()` after `3ea5c94`:

- Set `self._stopping = True`. `_adopt()` returns immediately while set, so neither `start(run_id)`
  nor `resume_inflight()` — nor an in-flight executor drain pass — can spawn a new run task during
  shutdown.
- `await asyncio.wait(live_tasks, timeout=grace_seconds)`. **`asyncio.wait`, never `asyncio.wait_for`**:
  `wait_for`'s timeout path cancels the future it guards, which is exactly the footgun `3ea5c94`
  documents. `asyncio.wait` cancels nothing on timeout.
- `task.cancel()` whatever is still pending, then await it with `CancelledError` suppressed.

**2. `stop()` never trips a `CancelToken` and never writes `cancel_requested`.** A run interrupted by
shutdown stays non-terminal (`running`/`blocked`/`compensating`) and is re-adopted by the existing
`resume_inflight()` on the next boot. No compensation is triggered; no infrastructure is destroyed.

**3. No new persistence and no schema change.** `interrupted_count` + `resumable()` already model
this state exactly. A run interrupted by shutdown is indistinguishable — by design — from one
interrupted by `kill -9`, so it exercises a path that already carries crash-matrix tests.

**4. `App.stop()` calls it unconditionally**, positioned after `reconciliation.stop()` and **before**
`timers.stop()` / `executor.stop()` / `subprocesses.shutdown()` / `db.dispose()`. Unconditional
because the executor admits runs whether or not `background_tasks` is set. This makes the engine the
first thing quiesced and the subprocess/DB teardown the last, so no live step can have its child
process killed or its connection disposed underneath it.

**5. `_stopping` is one-way for the engine instance's lifetime.** A stopped engine refuses to adopt;
it is not restartable in-process. `App` has no restart-after-stop path today (`build_app()` is the
only constructor and tests build a fresh App per test), so adding a reset would be speculative.

## Consequences

- Shutdown becomes **bounded** (`grace_seconds`) and **ordered**, and the teardown-traffic source
  behind `3ea5c94` is removed rather than merely survived.
- Most runs in flight at shutdown will be cancelled at the grace boundary rather than finishing —
  and that is the intended outcome, not a failure. 5s is deliberately far below step timeouts (a
  measured real DigitalOcean provision is 185s; gates allow up to 600s). The grace exists to let a
  step that is already at a persistence point commit it, not to let a step complete.
- **Restarting seedpod no longer risks destroying in-flight clusters** — the property this DR exists
  to protect. It was never *observed* to happen, because no shutdown path existed to do it; this
  forecloses the obvious implementation that would have.
- **Cost, stated plainly:** repeated restarts during one long run burn `interrupted_count`, and once
  it exceeds `_resume_replay_limit` the step fails permanently. That is the existing, correct trade
  (better than replaying a non-idempotent step and creating a second droplet) — but it means a
  restart loop can permanently fail a provision. Operators should know shutdown is not free.
- `App.stop()`'s docstring must lose the paragraph documenting the absence, and cite this DR instead.
  `App.start()`'s ordering comment gains the mirrored note.
- Tests to add alongside the implementation: `stop()` with an idle engine is a no-op; `stop()` with a
  live run leaves the row non-terminal and `cancel_requested` **false** (the anti-compensation
  assertion — the load-bearing one); a run interrupted by `stop()` is re-adopted by
  `resume_inflight()`; `start(run_id)` after `stop()` adopts nothing.

## Alternatives considered

1. **Status quo — leave run tasks dangling.** Rejected: it is the teardown-traffic source behind the
   deadlock, and it leaves subprocess/DB teardown racing live steps. The docstring already conceded
   it was a follow-up, not a decision.
2. **Reuse `cancel(run_id)` for every live run.** Rejected — the compensation catastrophe above. This
   is the alternative most likely to be reached for by someone implementing from the symptom rather
   than the model, which is the main reason this DR is written before any code.
3. **Drain to completion (no grace).** Rejected: unbounded. A single `k3s.await_api` gate can hold
   shutdown for 600s, and a provision run for 185s+; `SIGTERM` handlers and test teardown cannot wait
   that long.
4. **Persist a distinct `interrupted` run status.** Rejected: it adds a schema migration and a state
   the machine must totalize over, to express something `interrupted_count` + `resumable()` already
   express — and it would make shutdown-interruption *distinguishable* from crash-interruption, which
   is precisely the distinction we do not want the resume path to have to make.
5. **Have `App.stop()` cancel the tasks directly, without an engine method.** Rejected: `_runs` is
   private engine state, and the "stop admitting" half (`_stopping`) cannot be expressed from
   outside. It would also put lifecycle logic in the composition root, which Decision 8 reserves for
   construction.
