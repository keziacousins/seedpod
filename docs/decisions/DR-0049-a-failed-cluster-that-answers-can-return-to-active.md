---
title: DR-0049 — a failed cluster that answers can return to active
type: decision
status: proposal
created: 2026-10-06
updated: 2026-10-06
---

# DR-0049: a failed cluster that answers can return to active

**Status: PROPOSAL — the change was agreed with Kezia on 2026-10-06; this record is not yet
ratified.** Found on 2026-10-06 (DR-0048): the health monitor failed a healthy cluster in error,
and the only way to get it back was to destroy it and deploy again.

## Context

`FAILED` has two causes.

| Cause | The cluster | The exit that fits |
|---|---|---|
| `ProvisionFailed` | never worked | `RetryRequested`, which runs the provision workflow again |
| `HealthCheckFailed` | was working | none |

The exits from `FAILED` were `RetryRequested`, `DestroyRequested`, `TtlExpired` and
`InfraMissingObserved`. For a cluster that was working, each of them ends with the cluster gone.
`RetryRequested` provisions again, and nothing emits it. `clusters rehabilitate` sends
`AdoptRequested`, which was legal from `DESTROY_FAILED`, `DESTROYED`, `ZOMBIE` and `UNMANAGED`,
and was rejected from `FAILED`.

This was v1 parity. v1's rehabilitatable set was `destroyed`, `destroy-failed`, `zombie`
(`reference-code/seedpod/seedpod/api/clusters.py:902`). So this is a deliberate divergence from v1.

v1 did one thing that v2 dropped: before it changed the state, it called `get_cluster_info` with
the stored kubeconfig, and refused if the cluster did not answer (`:920-937`). v2's `rehabilitate`
sends the event and checks nothing.

## Decision

**1. `FAILED × AdoptRequested → ACTIVE`.** The rule clears `failure_reason`. It arms the ttl timer
again if the cluster has a ttl, because `FAILED` does not guarantee a timer: a destroy that was
requested and then cancelled returns to `FAILED` without one. Timers are upserts, so a second arm
is safe.

**2. A `FAILED` cluster must answer first.** `ClusterService.rehabilitate` reads the row. If the
state is `FAILED`, it runs one `KubeGetClusterInfo` with the stored kubeconfig, with no transaction
open (DR-0008), and sends the event only if that succeeds.

| The cluster | Result | HTTP |
|---|---|---|
| answers | `ACTIVE` | 200 |
| has no kubeconfig (provisioning did not finish) | refused, `ClusterNotRehabilitatable` | 409 |
| cannot be reached, or rejects the kubeconfig | refused, the probe's own error | 502 |

The check is in the service and not in the machine. The machine is pure and cannot tell the two
causes apart. Without the check, the rule would mark a cluster `ACTIVE` that does not exist.

**3. The check is for `FAILED` only.** The three states that v2 already adopted from keep their
behaviour. v1 checked them too. To add the check there is a different change: an `UNMANAGED`
cluster has no kubeconfig, so the check would stop its adoption.

**4. The SPA shows the Rehabilitate button for a `failed` cluster.** The text of the dialog
already says that the cluster must be running and reachable.

## Consequences

- An operator can undo a wrong `HealthCheckFailed` with one request. The deployments need no
  repair: `HealthCheckFailed` does not cascade to them.
- The check shows that the cluster answers. It does not show that provisioning finished. A
  cluster that failed late in provisioning, after its kubeconfig was stored, passes if its
  compensation left the cluster running.
- `rehabilitate` now also accepts a slug, because it reads the row before it sends the event.
- A cluster that left `FAILED` between the check and the event is not a problem. `apply()`
  validates the transition against the row it reads.
- The seam A table gains one row. That spec is a generated artifact and is not rewritten
  (DR-0001); this record is the amendment.

## Not decided here

- **Why a cluster gets failed in error.** The health monitor fails a cluster on the first
  `PermanentError` of any code. That is the other open point of DR-0048 and it is still open.
- **Automatic recovery.** The health monitor reads `ACTIVE` clusters only, so it never looks at a
  failed cluster again. This record adds the manual way back and no automatic one.

## What pins it

- `tests/core/test_cluster_table.py::test_failed_adopt_requested_returns_to_active_with_ttl` and
  `…_without_ttl` — the rule and its effects.
- `tests/api/test_clusters.py::test_rehabilitate_failed_cluster_that_answers_returns_to_active`,
  `…_that_cannot_be_reached_is_502_and_stays_failed`, and
  `test_rehabilitate_cluster_that_failed_provisioning_is_409_and_stays_failed` — the three rows of
  the table above, through the router, the service and the real `KubectlProvider`.
