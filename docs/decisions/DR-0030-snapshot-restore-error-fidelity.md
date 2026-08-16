---
title: DR-0030 — SnapshotService.restore stops conflating "unreachable" with "failed", and regains v1's pre-flight compatibility check
type: decision
status: active
created: 2026-08-08
updated: 2026-08-08
---

# DR-0030: snapshot restore error fidelity

**Status: ACTIVE — ratified by Kezia, 2026-08-08.** Raised by Round 10's adversarial judge on
`restore-and-rehydrate`, which correctly declined to reach into a collaborator its brief had declared
frozen and asked instead.

## Problem

Round 10 builds `deploy.restore_snapshot`, the first caller that depends on `SnapshotService.restore`
telling the truth about *why* a restore did not happen. It has two defects, both invisible until now
because nothing consumed the distinction.

**1. A blanket `except Exception` collapses the error taxonomy.**
`seedpod/app/services/snapshot_service.py:369-370`:

```python
except Exception as exc:  # noqa: BLE001 -- recorded below, never crashes the request
    error = str(exc)
```

That catches `InfrastructureUnreachableError` — "cannot determine state" — and folds it into a string
on `RestoreResult.error`, where it is indistinguishable from a definitive failure. `CLAUDE.md`'s hard
rule is unambiguous: `InfrastructureUnreachableError` "never triggers compensation and is never
conflated with absence". This is that conflation, in committed code.

**2. v1's pre-flight compatibility check was silently dropped.**
v1 (`reference-code/seedpod/seedpod/jobs/state/deployment_job.py:300-313`) compares the snapshot's
service names against the target profile's services **that declare persistence**, and fails early with
a message naming the missing services *and* both profile names. v2 has no equivalent, so restoring a
snapshot into a profile that cannot host it fails late and opaquely — `pod_name is None` appends to
`failed`, which reads exactly like "the pod isn't up yet".

Neither is fixable inside `deploy.restore_snapshot`: by the time a `RestoreResult` exists, the signal
is already gone. Both must be fixed at the source.

## Decision

**Round 10 amends `SnapshotService.restore`.** The "Round-6 services are frozen" constraint in the
round's brief is lifted for this method specifically, for these two changes.

### 1. `InfrastructureUnreachableError` propagates

Narrow the handler so `InfrastructureUnreachableError` is not swallowed into `RestoreResult.error`. A
restore that could not determine whether it succeeded is not a restore that failed, and the caller
must be able to tell the difference — that is the whole point of the type existing.

The blanket catch's stated intent ("never crashes the request") is preserved for genuine failures;
what changes is that one class of signal stops being flattened into a string.

### 2. The pre-flight compatibility check returns

Port v1's check: before attempting anything, compare the snapshot's service names against the target's
services declaring persistence. On a mismatch, fail immediately with a message naming the missing
services and both the snapshot's and the target's profile — v1's message is good and should be
salvaged close to verbatim.

This is a **restored v1 edge behaviour**, not new invention. Its absence is exactly the failure mode
`CLAUDE.md` names first: "silently regressing edge behavior v1 already got right."

## Consequences

- The change is confined to `SnapshotService.restore` and its tests. No other Round-6 service is
  unfrozen by this DR, and it is not a general licence to edit committed services — the next such
  request needs its own answer.
- **Two tests are required, and neither is optional**: an unreachable target surfaces as unreachable
  rather than as `RestoreResult(status="failed")`; and a snapshot whose services have no persistence
  counterpart in the target fails pre-flight, naming them, rather than failing late and generically.
- `deploy.restore_snapshot` can now distinguish the three outcomes it actually needs — restored,
  definitively failed, and could-not-determine — which is what makes its own error handling honest.
- Round 10 grows again. That is accepted: this is a correctness defect in the path the round exists to
  build, and deferring it would ship a new verb structurally unable to honour a `CLAUDE.md` law.

## Alternatives rejected

- **Fix only the taxonomy leak, defer the pre-flight check.** Tempting on scope grounds, and rejected
  because the two defects share a root — a restore that cannot work is reported the same way as one
  that merely hasn't finished — and fixing half of that leaves the confusing failure in place.
- **Defer both, ship the verb as built.** Rejected: it knowingly ships a verb blind to the
  unreachable-versus-failed distinction, in the one round whose brief made that distinction a blocker
  everywhere else.
- **Work around it inside `deploy.restore_snapshot`.** Structurally impossible — `RestoreResult` has
  already discarded the information by the time the verb sees it.
