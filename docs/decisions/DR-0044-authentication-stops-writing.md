---
title: DR-0044 — authenticating a request stops being a database write
type: decision
status: active
created: 2026-08-16
updated: 2026-08-16
---

# DR-0044: authentication stops writing

**Status: ACTIVE — ratified by Kezia, 2026-08-16.** Found while investigating the destroy hang
(DR-0043), 2026-08-15. Independent of it: this is not a destroy problem, it is every-request
problem.

## Context

`seedpod/api/auth.py`'s module docstring says, of the dependency every non-public route in the
tree depends on:

> `validate()` is read-only, this module writes nothing.

It is not, and it does. `ApiKeyService.validate` opens a `uow()`, does a SELECT by hash, then
calls `touch_last_used` — an `UPDATE api_keys SET last_used_at = …` — and commits. So **every
authenticated request performs a write transaction**, and under DR-0008 that transaction takes
the process-global single-writer `asyncio.Lock` and holds it for its whole extent, against
`Database`'s single StaticPool connection.

Per request, that is: one global lock acquire, three `asyncio.to_thread` hops (open, commit,
close), and a real disk write — strictly serialized against every other transaction in the
process. The other things holding that same lock, continuously: `TimerService` (1 s poll),
`EffectExecutor` (1 s poll), `HealthMonitor` (60 s), `ReconciliationService` (600 s), and the
workflow engine's roughly ten separate persistence points per step. A SPA page load fanning out
eight API calls serializes all eight behind whatever the engine happens to be writing.

The sharp edge is `busy_timeout=30000` (`data/database.py:61`). If that connection's commit ever
blocks, it blocks **in a worker thread while the process-global lock is still held** — for up to
thirty seconds, during which every request in the process hangs. Authentication is the last place
that risk should be reachable from.

## Problem

`last_used_at` is a **liveness breadcrumb**, not a ledger. It is surfaced in exactly two places,
both of which render it coarsely: `ApiKeyList.jsx:127` as a sortable column and
`ApiKeyDetail.jsx:235` through `formatDate` — *date* granularity, not timestamp. Nothing branches
on it, no audit record depends on it, no expiry is computed from it.

It is being paid for at the price of a serialized, fsync-class write on the hot path of every
authenticated request in the system. The value and the cost are wildly mismatched, and the
docstring that describes the correct behaviour has been sitting above the incorrect code the
whole time.

## Decision

**1. `validate()` becomes a real read.** Its `uow()` block contains only the SELECT. On a
successful lookup it records the use in a plain in-memory dict,
`self._pending_touches[row.id] = self._clock.now()`. The dict is mutated only from the event loop
with no `await` between read and write, so it needs no lock of its own.

**2. A public `flush_last_used()` drains the dict in ONE transaction.** Public deliberately, in
the shape `ReconciliationService.tick()` and `HealthMonitor.tick()` already established — so
tests drive it directly and deterministically rather than waiting on a timer.

**3. `touch_last_used` gains an optional `when`.** `data/repositories.py:2235` currently stamps
`clock.now()` internally, which for a deferred flush would record the *flush* time rather than
the *use* time — quietly turning a slightly-stale value into a wrong one. It gains
`when: datetime | None = None`, defaulting to `clock.now()`, so existing callers and
`tests/data/test_feature_repos.py` are unaffected.

**4. The periodic flush follows the existing runtime-loop discipline.** Idempotent
`start()`/`stop()`, `_DEFAULT_FLUSH_INTERVAL = 60.0`, wired in `app/factory.py` and `app/app.py`.
The loop is gated on `config.background_tasks` (off in tests, keeping them deterministic); the
final flush in `stop()` is **unconditional** and runs before `db.dispose()`, so a clean shutdown
never drops touches. That start/stop asymmetry is deliberate and has precedent — `App.stop()`
already documents the same shape for `engine.stop()` (DR-0024).

**5. Reads overlay what has not been flushed yet.** `get()`/`list()` apply any pending touch to
the returned `ApiKeyRow`. Cheap, and without it a freshly created key that has just been used
reads "Never" on its own detail page for up to a minute — the one staleness a human would
actually notice.

## Consequences

- **Revocation is NOT delayed.** This is the property to check first, and it holds: the SELECT
  still runs on every single request, so `is_active = 0` and expiry take effect immediately. Only
  the *bookkeeping* is deferred. This DR deliberately does **not** cache validated keys — that
  would remove the lock acquire entirely, and it was rejected precisely because it opens a
  revocation window. Recorded here as the considered-and-declined alternative, not an oversight.
- **`last_used_at` becomes eventually consistent** within the flush window (60 s), and exact
  again after a clean shutdown. Both UI surfaces render at date granularity, so the lag is
  invisible where it is actually read.
- **DR-0008 is untouched.** `validate()` still goes through `uow()`; the flush is an ordinary
  transaction; law 4 ("a transaction encloses ONLY database statements") still holds everywhere.
  This DR removes a *writer* from the hot path — it does not weaken the serialization discipline,
  and it makes no claim about the lock itself.
- **The write is not eliminated, it is amortized.** N authenticated requests inside a window cost
  one transaction instead of N. Under any real load that is the entire point; under no load it is
  identical.
- **`auth.py`'s docstring becomes true.** It has described this behaviour correctly since it was
  written; the code catches up.
- **A pinned test changes.** `tests/app/test_services_api_key.py:34
  test_validate_touches_last_used_at` must call `flush_last_used()` before asserting, and should
  be renamed to say what it now pins. Flagged rather than buried: changing a pinned test is a
  thing this repo requires a reason for, and this is the reason.

## What is explicitly out of scope

The **broader** finding behind this one: DR-0008's lock covers reads as well as writes, so every
read endpoint also serializes process-wide. Giving reads their own connection — WAL permits
readers alongside a writer — would fix that generally, and would be a larger change requiring its
own DR and a careful proof that no "read" path is secretly a chained write. Not attempted here.
This DR removes the single worst offender (a write on every request) without touching the
serialization model.

Also out of scope, recorded so it is not lost: `App.start()` blocks the ASGI lifespan on a serial
chain of live IO — `check_ready()` per provider, a full outbox drain, `resume_inflight()`, a
complete `reconciliation.tick()` and a complete `health.tick()` (sequential `kubectl cluster-info`
per ACTIVE cluster, 15 s each) — before uvicorn accepts a single request. On a host with several
clusters that is tens of seconds of unreachable API after every restart, which in a tree where
"restart the server after a code change" is routine reads as exactly the same symptom as
everything else in this DR. It is a separate decision.

## What would pin it

1. `validate()` performs no UPDATE — assert against the transaction, not the eventual value.
2. `last_used_at` reaches the DB after `flush_last_used()`, stamped with the **use** time rather
   than the flush time (decision 3).
3. A revoked key is rejected on the very next request, with pending touches outstanding — the
   security property, tested at its edge rather than assumed.
4. `App.stop()` flushes: use a key, stop the app without waiting for the interval, and assert the
   value persisted.
5. `get()`/`list()` reflect an unflushed touch (decision 5).
6. N validations inside one window produce exactly one flush transaction, not N.

---

## What actually landed

All six pins above exist, in `tests/app/test_services_api_key.py` and (for the shutdown flush)
`tests/app/test_app_lifecycle.py`. Three notes worth keeping:

**The old pinned test did not fail — it silently changed meaning.** The DR predicted
`test_validate_touches_last_used_at` would need updating. What actually happened is worse and more
instructive: it kept *passing*, because it asserted through `ApiKeyService.get()`, and decision 5
had just taught `get()` to overlay unflushed touches. So it went on looking like a pin on the
database write while pinning the in-memory buffer instead. The replacement tests read
`last_used_at` straight from the repository via a `_last_used_in_db` helper whose docstring records
why going through the service cannot pin this.

**Two behaviours were added that the decisions did not specify**, both because the failure mode is
silent:

- `flush_last_used` swaps the buffer out *before* opening its transaction and folds the batch back
  in on failure, so a failed flush retries next tick rather than dropping the touches — and a touch
  arriving mid-flush lands in the next batch rather than being lost to a racing `clear()`.
- `stop()`'s final flush logs on failure rather than suppressing silently. A dropped breadcrumb is
  acceptable; a dropped breadcrumb indistinguishable from "nobody used this key" is not. This is
  the tree's oldest recurring defect shape (the reason is computed, then discarded) in its mildest
  form, and it costs one log line to avoid.

**Not done, deliberately:** no validated-key cache (the revocation window, as recorded above), and
no change to DR-0008's lock. The out-of-scope section stands as written.

Suite: 2506 passed, 44 skipped.
