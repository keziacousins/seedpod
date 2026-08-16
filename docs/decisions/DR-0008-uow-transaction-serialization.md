---
title: DR-0008 — UnitOfWork serializes transactions (single-writer asyncio lock)
type: decision
status: active
created: 2026-07-15
updated: 2026-07-15
---

# DR-0008: UnitOfWork serializes transactions (single-writer asyncio lock)

**Status: ACTIVE — ratified by Kezia, 2026-07-15. The UoW lock, the transactions-enclose-only-
DB-statements law, and the pinned regression test are binding on every runtime-spine component
and everything built after.**
(Origin: third Round-4 halt — timer-service judge, run `wf_3c420b88-f97`, 2026-07-15.)

## Problem

Seam D pins the SQLite engine as `StaticPool` + `check_same_thread=False` (v1 parity). Under
StaticPool every Session shares ONE DBAPI connection, and SQLite transactions are
connection-scoped. v1 survived this because its sync driver never yielded mid-transaction —
each transaction completed within one event-loop slice by construction. The v2 runtime spine
breaks that accident: TimerService, EffectExecutor, the engine, and API handlers all run
concurrently on one event loop, and any real await inside an open `uow()` block lets another
task interleave on the same connection. Then task B's `commit()` commits task A's uncommitted
statements, and A's later `rollback()` undoes nothing.

Every atomicity law the spine asserts rests on this: the timer fire's "DELETE + apply, both or
neither" (Seam A §D), the Dispatcher's "state + audit + outbox rows in one transaction"
(Conflict 3), the executor's admission idempotency. The timer-service judge correctly refused
to accept the module docstring's guarantee, because the data layer does not currently provide
it. Caught per the spec-gap protocol.

## Decision

1. **`UnitOfWork` owns an `asyncio.Lock`; every transaction runs under it.** `uow()` acquires
   the lock before opening the session/transaction and releases it after commit/rollback.
   One writer at a time, cooperatively scheduled — which is all SQLite can physically deliver
   anyway (single-writer engine).
2. **`tx=` chaining runs under the already-held lock.** Chained calls
   (`dispatcher.apply(..., tx=t)`, Cascade recursion, engine terminal transactions) receive
   the open transaction object and never re-acquire — the outermost `uow()` context owns the
   lock for the transaction's whole extent. No reentrancy machinery.
3. **The StaticPool convention stands** (Seam D parity; in-memory test databases keep working —
   they are per-connection and need the shared connection).
4. **New law (binding on all current and future components): a transaction encloses ONLY
   database statements.** No provider IO, no subprocess, no `sleep()`, no SSE/broadcast await,
   no engine step execution inside an open `uow()` block. This was already the design's
   implicit discipline (providers never touch the DB; engine persistence points are short
   transactions bracketing step execution, never enclosing it); it is now explicit, and
   holding the lock makes violations *visible* as stalls instead of silent corruption.
5. Regression test pinned: two concurrent tasks interleaving writes through `uow()` — the
   loser's rollback must leave zero rows from its transaction; the judge's scenario
   (B commits during A's open transaction) must be unrepresentable.

## Consequences

- The timer fire, Dispatcher, and executor atomicity guarantees become true statements about
  the data layer, not aspirations.
- `seedpod/data/uow.py` gains the lock (a few lines); no repository, Dispatcher, or engine
  signature changes.
- Throughput ceiling of one write transaction at a time — irrelevant for an internal control
  plane with tiny write volume, and identical to SQLite's physical ceiling regardless.
- If a future migration moves to Postgres, the lock is deleted and connection-per-transaction
  becomes natural; nothing else changes shape.

## Alternatives considered

- **Connection-per-transaction pool (NullPool/QueuePool)** (rejected: SQLite is single-writer,
  so this buys zero concurrency — it relocates serialization into the sync driver's
  `busy_timeout` wait, which BLOCKS the whole event loop for up to the pinned 30s under
  contention and surfaces the remainder as `SQLITE_BUSY` errors; it also breaks in-memory test
  databases, which are per-connection).
- **Async driver (aiosqlite) + connection-per-transaction** (rejected: rewrites the committed
  data layer and every repository for the same single-writer end state; out of all proportion
  to the problem).
- **Rely on "no real awaits inside transactions today"** (rejected: undocumented luck — the
  exact class of silent invariant this rebuild exists to kill; the executor and API rounds add
  await surfaces immediately).
