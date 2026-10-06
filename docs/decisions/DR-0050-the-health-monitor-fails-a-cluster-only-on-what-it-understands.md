---
title: DR-0050 — the health monitor fails a cluster only on what it understands, and says when it cannot see
type: decision
status: proposal
created: 2026-10-06
updated: 2026-10-06
---

# DR-0050: the health monitor fails a cluster only on what it understands

**Status: PROPOSAL — decisions 1 and 3 were agreed with Kezia on 2026-10-06. Decision 2 is the
author's proposal and was not confirmed in so many words. Not yet ratified.** This is the first
open point of DR-0048.

## Context

The health probe is one `kubectl cluster-info`. The monitor classified its result by the type of
the error:

| The probe | The monitor did |
|---|---|
| succeeds | reset the counter |
| `InfrastructureUnreachableError` | skipped the tick |
| `TransientError` | counted it; failed the cluster at 3 in a row |
| `PermanentError`, any code | failed the cluster at once |

Two facts make the last row dangerous.

**1. `PermanentError` includes everything unknown.** A non-zero exit whose stderr matches no
phrase is `PermanentError(SCRIPT_FAILED)`. So the monitor failed a cluster on the first sentence
that nobody had classified. DR-0048 found three such sentences in one day, and each of them only
says "cannot connect".

**2. v1 had the opposite default.** v1 classified by regex. A message that it did not know was
transient (`reference-code/seedpod/seedpod/core/health_check.py:130`, "safer - gives retry
chance"). Permanent needed an explicit pattern: auth, not found, deleted. `runtime/health.py`
salvaged the shape "fail on permanent". When the classification moved to the typed taxonomy, the
default changed, and nobody decided that.

Also, `KubectlProvider` raises no `TransientError`. For the real probe the 3-in-a-row counter did
nothing.

A wrong failure is expensive. The monitor reads `ACTIVE` clusters only, so it never looks at a
failed cluster again. Before DR-0049 there was no way back.

## Decision

**1. An error that the classifier does not know never fails a cluster.** The monitor has an
allowlist, `_FAILING_CODES = {AUTH, INVALID_INPUT}`. A `PermanentError` with any other code means
that health cannot be determined. The monitor skips the tick, as for an unreachable cluster. It
does not count the tick and does not reset the counter. It logs at ERROR with the stderr, because
the repair is to add the sentence to `providers/`, and that log line is the only record of it.

**2. One failure is never enough.** A `PermanentError` with a code in `_FAILING_CODES` is counted
like a `TransientError`. The cluster fails when `max_consecutive_failures` (3) arrive in a row.
The parameter was `max_transient_failures`.

**3. The cluster records when its health was last confirmed.** Migration `0004` adds
`clusters.last_healthy_at`. The monitor stamps it on each healthy probe through
`ClusterRepository.set_last_healthy_at`. This is bookkeeping, like `last_reconciled_at`: a plain
UPDATE, no `version` bump, no `updated_at`, and the state machine does not read it.

The cluster API returns `last_healthy_at` and `health_stale`. `health_stale` is true for an
`ACTIVE` cluster whose last healthy probe is more than 300 seconds old. It is derived when the API
reads the row. A cluster that was never confirmed is not stale: `last_healthy_at` is null and the
SPA shows "Never". The cluster page shows the time, and the page and the list show a warning when
the value is stale.

## Consequences

- The verdicts are now:

| The probe | The monitor |
|---|---|
| succeeds | resets the counter, stamps `last_healthy_at` |
| unreachable | skips the tick (warning) |
| fails with an unknown sentence | skips the tick (error, with the stderr) |
| fails with `AUTH` or `INVALID_INPUT`, or a `TransientError` | counts it; fails the cluster at 3 in a row |

- A cluster that seedpod cannot see stays `ACTIVE`. This was already true for an unreachable
  cluster. It is now visible: after 5 minutes the API and the SPA say so.
- A cluster that is dead, with a message that nobody classified, is not failed by the health
  monitor. The reconciler finds a cluster whose machine is gone (`InfraMissingObserved`). The ttl
  removes the rest.
- A real auth failure takes 3 minutes to fail a cluster, not 1.
- A sentence that is in a phrase list in error still fails a cluster, after 3 ticks. The
  DR-0048 failure was of this kind: the discovery line was listed as `INVALID_INPUT`. Decision 1
  does not prevent that. DR-0048 removed the line, and DR-0049 is the repair if it happens again.
- Two differences from v1 are deliberate. v1 failed an unknown error after 3 ticks; v2 never does.
  v1 failed an auth error at once; v2 asks for 3.
- Each healthy tick now writes one row. Before, a healthy tick with a zero counter wrote nothing.
- Every cluster that exists when `0004` runs has a null `last_healthy_at` until its next healthy
  tick.
- A release from before `0004` does not start against a database that has it. The migration
  runner stops when `user_version` is not the number of its own last file. This was already true
  of `0002` and `0003`. To go back, set `PRAGMA user_version = 3`; the extra column does no harm.

## Not decided here

- **Automatic recovery from `FAILED`.** The monitor still reads `ACTIVE` only. DR-0049 is the
  manual way back.
- **An alert.** `health_stale` is a field that someone must look at. Nothing sends it anywhere.

## What pins it

- `tests/runtime/test_health.py` — `test_unrecognised_error_never_fails_and_is_logged_with_its_stderr`,
  `test_real_provider_unknown_kubectl_failure_leaves_the_cluster_active` (the real
  `KubectlProvider`), `test_recognised_permanent_error_counts_and_does_not_fail_at_once`,
  `…_fails_the_cluster_at_the_threshold`, and
  `test_healthy_probe_stamps_last_healthy_at_and_a_skipped_one_does_not`.
- `tests/data/test_machine_repos.py::test_set_last_healthy_at_moves_only_its_own_column`.
- `tests/api/test_clusters.py::test_health_stale_flips_after_threshold_for_an_active_cluster` and
  `test_health_stale_is_only_for_active_clusters`.
