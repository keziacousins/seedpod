---
title: DR-0020 — snapshot_before_destroy performs a real fail-open snapshot, owned by api-features
type: decision
status: active
created: 2026-07-18
updated: 2026-08-16
amended-by: DR-0043
---

# DR-0020: `snapshot_before_destroy` is a real fail-open pre-destroy snapshot

**Status: ACTIVE — ratified by Kezia, 2026-07-18 (option A). Round-6 halt (api-clusters judge,
run `wf_3c2e5583-540`, 2026-07-18).**

## Problem

v1's `DELETE /clusters/{id}?snapshot_before_destroy=true` took a real, best-effort data snapshot
before destroying (`reference-code/.../orchestrator/cluster_manager.py:681`
`_attempt_auto_snapshot` — `pg_dump -Fc` streaming per the persistence config; destroy proceeds
whether or not the snapshot succeeds — **fail-open**). It is a genuine data-protection edge.

The api-clusters build accepts the flag, returns `{"snapshot": "skipped"}`, and destroys anyway.
But the SPA **does not read the destroy response body** (ui-contract row 34: DestroyClusterModal
"sends query flags only"), so the operator who ticked "snapshot before destroy" gets **no snapshot
and no signal** — the skip is effectively silent data loss. Meanwhile the snapshot subsystem
(SnapshotService + router; `SnapshotRepository` already exists) lands in the *next* component,
api-features — so at the api-clusters stage the capability genuinely isn't wired.

Because the SPA ignores the body, the two easy answers both fail: "accept-and-skip" is silent
data loss, and "reject with 501" hard-*blocks* a destroy the operator explicitly requested. Only
actually taking the snapshot is honest.

## Decision (PROPOSED)

**`snapshot_before_destroy=true` performs a real, fail-open pre-destroy snapshot (v1 parity),
wired by the api-features component that owns the snapshot subsystem.**

- **Fail-open semantics, verbatim v1:** best-effort snapshot first; the destroy proceeds whether
  the snapshot succeeds or fails (a failed snapshot never blocks destruction). The default
  (`snapshot_before_destroy=false`) destroys with no snapshot.
- **Ownership — api-features.** `ClusterService.destroy` depends on the snapshot capability, which
  api-features builds. api-features injects that capability into `ClusterService` and wires
  `destroy` to snapshot-then-destroy. (api-features already edits `app/services/`; this is one
  more collaborator.)
- **No silent no-op at any stage.** In api-clusters (built before the snapshot subsystem),
  `ClusterService.destroy` must **not** silently skip: `snapshot_before_destroy=true` with no
  snapshot capability injected raises (HTTP 501) rather than returning a false "skipped" — so the
  flag is never a silent lie. api-features then replaces that with the real fail-open snapshot, and
  the final gate verifies the end-state (the flag triggers a snapshot).

## Consequences

- api-features: `ClusterService` gains an injected snapshot collaborator; `destroy` runs the
  snapshot (fail-open) before emitting `DestroyRequested`. Tests: snapshot-taken-then-destroy;
  snapshot-fails-still-destroys (fail-open); `snapshot_before_destroy=false` takes no snapshot.
  Uses the FakeProvider/fake-snapshot seam — a wiring test, not a real `pg_dump`.
- api-clusters (interim): the flag raises 501 without the capability; a test pins that it is not a
  silent skip.
- Final gate checks: `snapshot_before_destroy=true` invokes the snapshot path, fail-open.

## Alternatives considered

- **Accept-and-skip, return `{"snapshot": "skipped"}`** (the build's choice) — rejected: the SPA
  never reads the destroy body, so this is indistinguishable from silent data loss — the exact
  v1 data-protection regression the charter forbids.
- **Reject `snapshot_before_destroy=true` with 501, permanently (defer the feature)** — rejected:
  it hard-blocks a destroy the operator explicitly requested *with* a snapshot; snapshots are
  parity-intended and api-features builds the subsystem anyway, so the marginal cost is wiring one
  fail-open call, not a new subsystem.
- **Wire the integration in api-clusters** (before the snapshot subsystem exists) — rejected:
  dependency inversion; api-features is the snapshot owner and the correct home for the wiring.
