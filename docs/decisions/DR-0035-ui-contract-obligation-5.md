---
title: DR-0035 — discharging ui-contract obligation 5: workflow_progress is the in-workflow live signal, and pod_status_changed is consciously dropped
type: decision
status: active
created: 2026-08-10
updated: 2026-08-10
amends: ui-contract.md
---

# DR-0035: obligation 5, discharged

**Status: ACTIVE — ratified by Kezia, 2026-08-10** (option (a) for `pod_status_changed`; close the
restore-path gap in the same change). Closes backlog **#23**.

## Context

`docs/design/ui-contract.md`'s obligation 5 reads: *"Topics that must survive (consumed today, not
in the design's kept-list — **port or consciously drop with a DR**): `pod_status_changed` (live pod
pages depend on it), `snapshot_restore_completed`, `reconciliation_skipped`."* Smoke 10 found the
obligation had been recorded and then neither honoured nor closed, and live pod pages never refresh.

### First: the item's own premise was two-thirds wrong

Backlog #23 and the obligation-5 note both state that **none** of the three is emitted by v2, and
that v2's complete emitted set is `cluster_state_changed`, `deployment_status_changed`,
`workflow_progress`, `run_conflict`, `keepalive`. Checked against the source rather than the prose:

| topic | claimed | actual |
|---|---|---|
| `reconciliation_skipped` | never emitted | **emitted and tested** — `runtime/reconciliation.py:497` (`_broadcast_skipped`), environment-scoped per DR-0010, covered by `tests/runtime/test_reconciliation.py:137`. Payload `{cluster_id, provider, reason}` matches what `MiniEventHud.jsx:162` reads. |
| `snapshot_restore_completed` | never emitted | **emitted and tested on the REST path** — `api/routers/snapshots.py:197`, covered by `tests/api/test_features.py:333`. Payload matches `ClusterDetail.jsx`/`MiniEventHud.jsx` field-for-field. **Not** emitted on the workflow path (`deploy.restore_snapshot` inside `deploy-waves.yml`) — which is the path smoke 10's restore actually took. |
| `pod_status_changed` | never emitted | correct — absent everywhere. |

So the real gap was **one whole topic and one path**, not three topics. This is the third instance
of this repo's standing lesson (design docs describing intent as though it were built) — recorded
here in the *opposite* direction for once: prose under-describing what exists.

### What the SPA actually needs

`ui-contract.md`'s own verdict already answers this: *"Domain events are used purely as refetch
triggers; REST is always the store of record."* Confirmed at every listener:

- `PodDetail.jsx:50-57` / `ContainerDetail.jsx:88-93` — `if (cluster_id === clusterId && (!pod_name
  || pod_name === podName)) reload(silent)`.
- `ClusterDetail.jsx:210-240` — the same shape for pods, and for restore completion a refetch of the
  restore history.

None of them reads `phase`, `ready`, `containers` or `services_restored` to render. Only
`MiniEventHud`'s one-line formatter does — and its `pod_status_changed` case reads `data.status`,
which **v1 never sent** (v1's payload had `phase`; `deployment_job.py:816-828`). That line rendered
`?` in v1 too: the same conflation class as the `|| "updated"` bug smoke 6 fixed.

### The mechanism is not missing, and that is the whole point

v2 **deliberately** replaced v1's per-deployment `_watch_pods_and_emit_events` background task with
`deploy.await_wave`'s per-poll `ctx.progress` → `workflow_progress` (`deploy_apply.py:533`,
5s cadence, per-resource status), and `deploy-waves.yml`'s `ready` step says so in a comment. The
server side works. **The SPA was never told**: it listens for `workflow_progress` only on the
Workflows page and in the HUD, never on the three pod pages.

## Decisions

### 1. `pod_status_changed` is consciously DROPPED

There is no `pod_status_changed` topic in v2 and there will not be one. `KubeWatchPods` stays built
and unused for now — it is the mechanism a future watcher would use, and deleting it would throw
away salvaged v1 hardening for no gain.

The alternative (a `runtime/` watcher over `KubeWatchPods`, strictly better than v1 in that it would
cover idle clusters too) was weighed and declined: it is a new long-lived task per cluster needing a
lifecycle owner, and `HealthMonitor` is explicitly not that home (`runtime/health.py:129` — "never
touches the hub"). If idle-cluster pod churn ever becomes a felt problem, that is the design to
revisit, and this DR is what it would supersede.

### 2. `workflow_progress` is the in-workflow live signal, adopted by the pod pages

`PodDetail`, `ContainerDetail` and `ClusterDetail` listen for `workflow_progress` and refetch when
`data.cluster_id` matches, replacing their dead `pod_status_changed` listeners. The payload already
carries `cluster_id` (`engine/engine.py:1074`), so no server change is needed.

**The limitation is stated, not hidden**: progress flows only *during a workflow run*. Pod churn on
an idle ACTIVE cluster — a crash-loop hours after a deploy — produces no event, and the page updates
on its next manual refresh or reconnect. v1 was no better here (it watched only during a rollout);
what v1 had and this gives up is per-pod granularity *within* that window, which no page rendered
anyway.

The pod pages deliberately do **not** filter on pod name, unlike their old `pod_status_changed`
handlers: `workflow_progress` is a cluster-scoped signal, and over-filtering it would reintroduce
exactly the dead-listener silence this DR exists to fix.

### 3. The workflow restore path emits progress, not a second `snapshot_restore_completed`

`deploy.restore_snapshot` now calls `ctx.progress` on both outcomes — success, and each failed
attempt (with the reason, per DR-0033's "say why"). `ClusterDetail` refetches restore history on
`workflow_progress`, so **the SPA behaves identically however the restore was triggered**, which is
what closing this gap means.

**Why not simply broadcast `snapshot_restore_completed` from the step.** The workflow's `restore`
step carries `retry: {max_attempts: 19}` — a ~180s budget replicating v1's
`_wait_for_database_pods_ready(180)` — and restoring into a not-yet-ready database is a *normal*
early outcome, not an error (see the step's own docstring). Emitting a terminal-sounding
"restore completed / failed" per attempt would put up to 18 spurious failure events in the HUD for a
run that then succeeds. `ctx.progress` is per-attempt **by design** and reads as such.

This also avoids adding a general "step emits an arbitrary SSE topic" mechanism to `StepContext`,
which is engine surface the frozen-grammar discipline should not grow for one topic.

`api/routers/snapshots.py` keeps its `snapshot_restore_completed` broadcast unchanged: it is a
single-attempt, out-of-band, user-initiated operation, and terminal language is honest there. The
resulting rule is one sentence: **bespoke topics report out-of-band operations; `workflow_progress`
reports what a workflow is doing.**

### 4. `reconciliation_skipped` needs no work — the record needed correcting

Already emitted, environment-scoped, and tested. `ui-contract.md`'s obligation 5 and backlog #23 are
corrected in place.

### 5. Dead listeners are removed, not left harmless

The three topics come out of `event-store.js`'s default subscription list and `MiniEventHud`'s topic
list, and `pod_status_changed`'s formatter case goes with them — including the `data.status` read
that never matched any payload v1 or v2 ever sent. `snapshot_restore_completed` and
`reconciliation_skipped` **keep** their HUD cases and their `ClusterDetail` listener, because both
are still real topics.

## Consequences

- `seedpod/engine/steps/deploy_restore.py` — `ctx.progress` on success and on each failed attempt.
- `ui/src/pages/{PodDetail,ContainerDetail,ClusterDetail}.jsx` — `workflow_progress` listeners
  replace `pod_status_changed`.
- `ui/src/lib/event-store.js`, `ui/src/components/MiniEventHud.jsx` — drop `pod_status_changed`.
- `docs/design/ui-contract.md` — obligation 5 marked **discharged**, with the corrected inventory.
- the parity backlog (not published) — #23 closed, with the two-thirds-wrong premise recorded.

**Verification.** The step's progress emission is unit-testable and is. The SPA half is not: there is
no SPA test suite (backlog #20), so the evidence is a live run — the next smoke should watch a pod
page refresh by itself during a deploy, and confirm the HUD shows restore progress rather than
silence for the 39s smoke 10 spent restoring.
