---
title: DR-0039 — a terminal outcome's reason carries the failure message, not just the error kind
type: decision
status: active
created: 2026-08-13
updated: 2026-08-13
---

# DR-0039: `failure_reason` says why

**Status: ACTIVE — ratified by Kezia, 2026-08-13.** Amends coherence-review.md Conflict 8's
outcome-event mapping (`reason` ← the run's error kind). Sixth instance of the recurring defect
shape DR-0033 and backlog #18 each closed one layer of.

## Context

The 2026-08-12 tart run — the first deployment of the dev exampleco stack on `tart` — failed its
provision. The cluster row read:

```json
{ "status": "failed", "failure_reason": "permanent" }
```

`GET /api/clusters/{id}` serialises `failure_reason` directly, and the SPA renders it as the
error text on both `ClusterDetail` and `DeploymentDetail`. So the single word `permanent` was
the entire operator-visible account of a dead cluster.

The actual reason existed, fully formed, one table over:

```json
{ "kind": "permanent",
  "message": "ssh-k3s.install_k3s: exited 1; stderr: ... [ERROR]  Download failed",
  "step": "k3s" }
```

Recovering it meant opening `db/seedpod.db` and querying `workflow_runs.error` by hand. That is
what diagnosing this run actually required.

**Why the message was already there and still lost.** Backlog #18 (`3dc5dc0`) carried
`ProviderError.detail["stderr"]` into `workflow_steps.error` and `workflow_runs.error`.
`_RunFailed` carries `step_path`, `kind` **and** `message` (`engine/engine.py:116`).
`_handle_failure` had the message in hand and passed only `fail.kind` to `_finalize`, which
passed only `error_kind` to `_build_outcome_event`, which set `reason` from it. Every layer had
the reason; the last one dropped it. **This is the shape the errors-know-why memory names —
and the instruction it gives ("when you find one drop, check the layer above and below it
before you stop") is exactly what surfaced it, since #18 fixed the layer below this one.**

**Why Conflict 8's original mapping was defensible.** Its two worked examples are
`cancelled → reason="cancelled"` and unreachable-exhausted destroy → `reason="unreachable"`.
In both, the kind *is* the whole reason — there is no further message to add. The rule
generalised from two cases where it happened to be lossless.

## Decision

`reason` on a terminal outcome event becomes `"{kind}: {message}"` when a message exists, and
stays exactly `kind` when one does not.

1. **The kind stays the prefix.** Anything reading `reason` as a bucket still finds it at the
   front. `cancelled` still reads `cancelled`.
2. **A workflow's own `reason` in its YAML payload still wins**, unchanged — the engine only
   fills `reason` when the payload omits it. Every shipped `provision-*.yml`/`destroy-*.yml`
   omits it; test fixtures that supply `"n/a"` are unaffected.
3. **Capped at 500 characters** (`_MAX_REASON_CHARS`), tighter than #18's 2000-char stderr cap,
   because this string is rendered as one line of SPA error text. Truncation says so. The
   untruncated message always remains in `workflow_runs.error` — this is the operator's first
   answer, not their only one.
4. **The unreachable-exhausted path now passes its richer message** (`_exhausted_message`, which
   #18 built to carry the last probe's reason) rather than the terse
   `"unreachable_budget exhausted"` the run row keeps.
5. **The resume path** (`compensating_resume`) reads `message` out of the persisted run error
   alongside `kind`, so a run finalised after a crash reports the same reason as one that never
   crashed.

## Consequences

- No schema change, no migration, no event-class change. `reason: str` is already a required
  field on `ProvisionFailed`/`DeployFailed`/`DestroyFailed`.
- The UI contract is unchanged in shape; `failure_reason` simply becomes informative.
- Rejected: a separate message field on the event classes and both record types. Semantically
  cleaner, but it touches the Conflict-8 event shapes, `clusters`/`deployments` records, a
  migration, the API serialisers and the UI contract — a large blast radius to say something
  the existing field can say.
- Rejected: leaving `failure_reason` alone and exposing `workflow_runs.error` through the API
  instead. That answers "why" on a different page from the one showing the failure.

## What pins it

`tests/engine/test_gates_schedule_park.py::test_outcome_reason_carries_the_failure_message_not_just_the_kind`,
which uses an outcome block that omits `reason` exactly as the shipped workflows do. **Verified
failing against the pre-fix engine**, where the emitted event is literally
`ProvisionFailed(reason='permanent')` — the production symptom, reproduced.
