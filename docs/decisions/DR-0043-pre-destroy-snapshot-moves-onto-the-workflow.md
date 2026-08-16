---
title: DR-0043 — the operator's pre-destroy snapshot moves out of the request and onto the destroy workflow
type: decision
status: active
created: 2026-08-16
updated: 2026-08-16
---

# DR-0043: the pre-destroy snapshot moves onto the destroy workflow

**Status: ACTIVE — ratified by Kezia, 2026-08-16.** Raised by an operator report of the API
hanging during destroy on both `digitalocean` and `tart`, 2026-08-15.

**Amends DR-0020 and DR-0040** (see Consequences). Recorded in this DR's body as well as in their
`amended-by` frontmatter: DR-0040 set the precedent, recording its own amendment of DR-0022 in
its body (Erratum E2).

## Context

`DELETE /api/clusters/{id}?snapshot_before_destroy=true` awaits the **entire** snapshot inline in
the request. `ClusterService.destroy` calls `SnapshotService.attempt_pre_destroy_snapshot` before
it dispatches anything (DR-0020's own decision), and that call runs, per persistable service:

1. `kubectl exec … pg_dump` with `_DUMP_TIMEOUT_S = 300` — five minutes, each;
2. a `gzip.compress` + `write_bytes` of the result.

Two different failures, often mistaken for one:

**The request hangs.** Up to 300s per service before the response. `ui/src/lib/api-client.js`
sets no timeout, so the browser simply spins with no feedback. The SPA's own destroy modal
(`DestroyClusterModal.jsx:23`) is what sets the flag, so this is the normal operator path, not an
exotic one.

**The whole process hangs.** Step 2 ran on the event loop. Measured 2026-08-16 on a 30 MB dump of
realistic-entropy rows: **1232 ms** of total event-loop stall, against 9.5 ms once moved to a
thread — roughly 40 ms of frozen process per MB, per service. Nothing else in the process ran for
that whole window: no requests served, no SSE keepalives, no timer polls, no health checks. A
300 MB dump freezes everything for ~12 s.

The second failure is already fixed (`SnapshotService` now crosses `asyncio.to_thread` once for
each compress/write and read/decompress; no DR needed, it changed no decision). **This DR is
about the first**, which is structural: the work is in the wrong place, not merely on the wrong
thread.

## Problem

There is no reason for the operator to wait. The snapshot is **fail-open** by DR-0020's explicit
ratification — destroy proceeds whether or not it succeeded — so the synchronous wait cannot be
a result signal. And `api/routers/clusters.py:63` already records that **the SPA never reads the
destroy response body** (ui-contract row 34). The client blocks for up to five minutes per
service on a body it discards, to learn the outcome of an operation whose outcome is defined not
to matter.

Meanwhile both destroy workflows already have exactly the right step in exactly the right place.
`config/workflows/destroy-{cloud,shared}.yml` carry `autosnap` (`cluster.auto_snapshot`,
`on_failure: continue`, `timeout_seconds: 600`), positioned after `kubecfg` and before
`tailscale`/`destroy` — while the cluster is still whole. It exists for the TTL path (DR-0040)
and deliberately no-ops for operator destroys, for one reason, stated in its own docstring
(`engine/steps/cluster.py:607-612`): *"The operator path ALREADY snapshots… Firing here too would
write two snapshots for one destroy."*

So the operator path is inline **because** the step no-ops, and the step no-ops **because** the
operator path is inline. Invert both and the work lands where the machinery already is.

## Decision

**1. `snapshot` becomes an explicit flag on the event, alongside `trigger` — not folded into it.**
`DestroyRequested` and `DestroyDue` each gain `snapshot: bool = False`, defaulted exactly as
DR-0040 defaulted `DestroyDue.trigger` (`events.py:192`) so every existing construction, and any
timer row already armed in a live DB, still reads correctly.

DR-0040's `trigger` is **provenance** — which route reached `DestroyDue`. "Did the operator ask
for a snapshot" is not provenance, and overloading `trigger` with a third value to carry it
would make one field answer two unrelated questions. Two fields, two questions.

**2. `ClusterService.destroy` stops snapshotting and starts declaring.** It passes
`snapshot=snapshot_before_destroy` on the `DestroyRequested` event instead of awaiting
`attempt_pre_destroy_snapshot`. Both existing guards stay in front of it — the production-cluster
force guard (DR-0018) and the discovered-origin guard — preserving DR-0020's "a destroy that is
ultimately going to be rejected must never leave a stray `is_auto` snapshot behind".

**Kept deliberately:** the `self._snapshots is None → PermanentError(UNSUPPORTED) → 501` guard.
The work moved; DR-0020's "the flag is never a lie even in a degraded configuration" property
did not, and it should not be lost as a side effect of relocating a call.

**3. The machine threads it through, and `_destroy_timer` stamps it.** `_destroy_timer`
(`machine.py:248`) takes `snapshot` and stamps it onto the `DestroyDue` it injects; every
`DestroyRequested` transition that arms it passes `snapshot=event.snapshot`. The two TTL
transitions keep `trigger="ttl_expiry"` and leave `snapshot` at its default.
`_cluster_destroy_scheduled_destroy_due` (`machine.py:446`) adds `"snapshot"` to the `RunWorkflow`
args beside `"trigger"`. All of this is pure — no IO, no new `(state × event)` cells.

**4. `DESTROY_SCHEDULED × DestroyRequested` stops being a plain `_ignore`. This is the sharp edge
of the whole change.**

`_cluster_destroy_scheduled_destroy_requested_dup` (`machine.py:483`) currently returns
`_ignore` — correct today, because a re-request for an already-scheduled destroy genuinely is a
no-op, *and because `ClusterService.destroy` has already taken the snapshot before the machine
ever sees the event*.

Move the intent onto the event and that safety disappears. Sequence: a TTL expires and schedules
the destroy; before the timer fires, an operator calls `DELETE ?snapshot_before_destroy=true`.
The event is ignored, the snapshot never happens, and the API still returns
`200 {"message": "cluster destruction initiated"}`. That is **precisely** the failure DR-0020
exists to prevent, in DR-0020's own words via `clusters.py:63`: *"silently accepting the flag and
skipping the snapshot is indistinguishable from silent data loss."*

The handler must therefore **merge** rather than ignore: when `event.snapshot` is true and the
armed timer does not already carry it, re-arm the destroy timer with `snapshot=True` while
**keeping** `trigger="ttl_expiry"`. Not replace — timers upsert on `timer_key="destroy"`, so a
naive re-arm also flips `trigger` to `"operator"`, silently disabling the profile-gated
auto-snapshot DR-0040 exists to deliver. It has to carry both.

**5. Precedence inside the step is load-bearing.** `AutoSnapshotParams` gains
`snapshot: bool = False`, and `AutoSnapshot.execute` tests **`snapshot is True` FIRST** →
`attempt_pre_destroy_snapshot` (unconditional — what the operator explicitly asked for, DR-0020);
only then `trigger == "ttl_expiry"` → `attempt_auto_snapshot` (profile-gated, DR-0040); neither →
skip with a reason through `ctx.progress`, as today.

The order is not cosmetic. Once decision 4 lets both flags be true at once, testing `trigger`
first would route an operator's explicit request into the *profile-gated* path and silently skip
it whenever that profile has `auto_snapshot` disabled — the same silent-skip bug as decision 4,
one layer down. Exactly one snapshot in every combination, and the explicit request always wins.

**6. Both workflows gain a `snapshot: {type: bool}` input**, bound into `autosnap`'s `with:` as
`{from: run.snapshot}`. This is existing grammar — `{from: run.trigger}` is already there — so
the frozen workflow grammar is untouched: no `if`, no expression, no new verb.

## Consequences

- **`DELETE` returns promptly.** The snapshot becomes a visible workflow step with SSE progress,
  in the run the operator is already watching, instead of an opaque five-minute wait on a
  response body nobody reads.
- **The verb catalog is unchanged at 33.** This reuses `cluster.auto_snapshot` rather than adding
  anything, so `test_registry_verb_set_is_exactly_the_dr_0022_catalog` is untouched — unlike
  DR-0040, which had to widen it 32 → 33.
- **DR-0020 is amended, not withdrawn.** Its substance — a real, fail-open, best-effort
  pre-destroy snapshot, v1 parity with `_attempt_auto_snapshot` — is preserved exactly. Only its
  *location* changes: `ClusterService.destroy` → the destroy workflow's `autosnap` step.
- **DR-0040 is amended.** `cluster.auto_snapshot` is no longer "a no-op for anything but
  `ttl_expiry`". Its no-double-snapshot guarantee is preserved by decision 5's precedence rather
  than by the step declining to run.
- **One latent note.** With a future `due_at`, the snapshot would now happen at destroy time
  rather than request time. This is currently unreachable: `due_at` defaults to `None` on
  `ClusterService.destroy` and **no caller passes it** — the router exposes only
  `force`/`snapshot_before_destroy`, `seedpodctl` has no flag, and reconciliation passes `None`
  explicitly — so `fire_at` is always "now". Recorded as a property of dormant surface, not
  weighed as a risk.
- **A narrow behavioural loss, stated plainly.** Today a caller that *does* read the response
  body learns the destroy was accepted only after the snapshot finished. Afterwards it learns
  only that it was accepted. Since the snapshot is fail-open, the body never carried the
  snapshot's outcome anyway — but `seedpodctl clusters destroy --snapshot-before-destroy` now
  returns before the snapshot exists, and anything scripted against "the command returned, so the
  dump is on disk" would be wrong. No such script exists in this tree; flagged because it is the
  kind of assumption that lives outside it.

## What would pin it

1. **The regression this change risks, tested directly.** TTL schedules a destroy; an operator
   then calls `DELETE ?snapshot_before_destroy=true` before the timer fires. Assert a snapshot is
   actually taken **and** `trigger` is still `ttl_expiry`. Without decision 4 this silently
   produces no snapshot and a 200 — it is the whole reason decision 4 exists.
2. `AutoSnapshot.execute` across all four input combinations (`ttl_expiry` / operator-`snapshot` /
   both / neither), asserting **exactly one** snapshot each; and specifically that both-true with
   a profile whose `auto_snapshot` is disabled still takes the operator's snapshot (decision 5).
3. Both branches of the new `DESTROY_SCHEDULED × DestroyRequested` transition, under the existing
   exhaustive `(state × event)` totality suite.
4. `ClusterService.destroy` no longer touches `SnapshotService` on the happy path — the 501 guard
   is the only remaining reference.
5. Live smoke on both providers: `DELETE` returns promptly, `autosnap` appears in workflow
   progress, a row lands in `snapshots` with a `.dump.gz`, and `GET /api/clusters` polled in a
   loop throughout the destroy never stalls.

---

## Erratum E1 — decision 4's merge is WITHDRAWN; the transition refuses instead (2026-08-16, during implementation)

**Decision 4 asked for something this machine cannot do.** It specified merging an operator's
snapshot request into an already-armed destroy timer — "re-arm with `snapshot=True` while
*keeping* `trigger='ttl_expiry'`". That is not implementable where it was specified:

- `seedpod/core/machine.py` is pure and sees only `record` and `event`. It cannot read the armed
  timer.
- `ClusterRecord` carries no record of **why** a destroy was scheduled. `pre_destroy_state` records
  the state to cancel back to, not the route in.
- `TimerRepository.upsert` replaces the stored `event` **wholesale** on conflict; there is no
  merge.

So "keep the existing `trigger`" had no source for the existing trigger. Re-arming blind would
have overwritten a TTL's `trigger="ttl_expiry"` with `"operator"`, silently disabling the
profile-gated auto-snapshot DR-0040 delivers — trading the silent loss decision 4 exists to
prevent for a different one.

**What landed instead:** `DESTROY_SCHEDULED × DestroyRequested` stays `_ignore` for a plain
re-request, and **raises `InvalidTransition` when the request carries `snapshot=True`** (→ 409 at
the router, with the message naming `POST /api/snapshots` as the recourse). The operator learns
their snapshot did not happen, which was decision 4's actual goal; only the mechanism changed,
from silently-correct to loudly-refused.

**Why not carry the trigger on `ClusterRecord`?** It would work — a `destroy_trigger` column
alongside `pre_destroy_state`, set on entry to DESTROY_SCHEDULED — but it costs a migration and a
new source of truth for a window roughly **one timer poll wide**: both destroy routes arm
`fire_at` at "now", so DESTROY_SCHEDULED is transient. Disproportionate. This is also why the
refusal could not be pinned at the API level — the cluster has already reached DESTROYING by the
time a second HTTP request lands — so "What would pin it" item 1 now lives at the machine
(`tests/core/test_cluster_table.py::test_destroy_scheduled_destroy_requested_carrying_a_snapshot_is_refused`)
rather than end-to-end.

**How the error happened, because it is the interesting part.** This DR was drafted from
`core/events.py`, `core/machine.py`'s transitions, and the workflow YAML — every layer the flag
travels through — and not from `ClusterRecord` or `TimerRepository`, the two that would have had
to supply the fact decision 4 depends on. That is the same failure DR-0040's Erratum E1 records
("written from `core/machine.py` and the `clusters` schema… and not from the service that would
have to take the snapshot"), one DR later, in the same subsystem. The tell was identical too: the
decision read as obviously correct because every layer it *named* supported it.

## Erratum E2 — `attempt_pre_destroy_snapshot` loses its `status == "active"` gate

Not anticipated by the decisions above, and required by decision 5.

`SnapshotService.attempt_pre_destroy_snapshot` opened with `if cluster.status != "active": return
None`. That was correct while it ran from `ClusterService.destroy`, **before** anything was
dispatched. Decision 5 calls it from inside the destroy *workflow*, by which point the machine has
already moved the cluster to DESTROYING — so the gate would have skipped 100% of the time and the
operator's explicit snapshot would have shipped silently inert.

This is precisely the trap DR-0040 documents having avoided when it built `attempt_auto_snapshot`
with no such gate: *"the same guard would have skipped 100% of the time. That would have shipped
as 'still inert', the exact bug this DR exists to fix."* The gate belonged to a call site that no
longer exists. What still protects against snapshotting a cluster that should not be is the guard
chain in `ClusterService.destroy`, which runs before the event is dispatched at all.

## Erratum E3 — dispatch dropped `snapshot`; found on the appliance, not in the suite (2026-08-16)

`WorkflowDispatch.resolve` builds the destroy arm's inputs as an explicit **allowlist**, correctly
per DR-0022 ruling 2. This DR threaded `snapshot` through events → machine → `RunWorkflow.args` →
workflow YAML → step Params and **missed that one line**, so both destroy workflows declared an
input nothing supplied. Every destroy then died instantly on `KeyError('snapshot')`, stranding a
live DigitalOcean droplet mid-destroy.

**Why 2506 tests passed over it.** The machine test asserted the *effect's* args; the step tests
drove `AutoSnapshotParams` directly; the API test asserted the armed *timer's* `DestroyDue`. Both
ends of the seam were covered and the seam itself was not. "What would pin it" item 1 named
exactly this — `workflow_runs.args` carrying both keys — but it was written as a live-smoke check
rather than a test, and it is the item that failed.

The replacement asserts the *relationship*: whatever a shipped destroy workflow declares under
`inputs:`, `resolve` must supply — parametrised over every provider, read from the real YAML
(`tests/engine/test_dispatch_table.py`). Verified to bite: reverting the one-line fix fails five
of its cases.

**A second lesson, from the failed recovery.** Fixing dispatch did **not** rescue the stuck run.
`resume_inflight` replays a run's *persisted* args, frozen at admission — so the already-admitted
run resumed and died on the same `KeyError` against the fixed build. Recovery needed a hand-written
repair of `workflow_runs.args`. Generalised: **adding a required input to a workflow strands every
run admitted before the change**, with no migration path and no warning, and the frozen-grammar
rule makes adding inputs the sanctioned way to extend a workflow. That sharp edge sits on the
supported path.

Both of those are dwarfed by what made a one-line bug expensive: the crashed run sat at
`status=running` with an empty `error` forever, so the cluster never left `destroying` and real
infrastructure kept billing. See the DR raised separately for that.

**Verified live on the appliance, 2026-08-16** (release `2.0.0a0+dcbf40c1`, DigitalOcean): `DELETE`
returned in **0.15s**; the resumed run produced **exactly one** snapshot, `created_by:
system:pre-destroy` — proving decision 5's precedence routed the operator's explicit request to the
unconditional helper — with 3 dumps totalling 134,207 bytes; droplet destroyed. That run also
proves Erratum E2 was load-bearing: the cluster was already `DESTROYING` when `autosnap` ran, so
the old `status == "active"` gate would have skipped it silently.

## What actually landed

- `DestroyRequested.snapshot` and `DestroyDue.snapshot`, both defaulted `False`.
- `_destroy_timer(..., snapshot=)`; every `DestroyRequested` transition threads `event.snapshot`,
  the two TTL transitions leave it default.
- `RunWorkflow.args` for destroy carries `{"trigger": …, "snapshot": …}`.
- `DESTROY_SCHEDULED × DestroyRequested` refuses a snapshot-carrying re-request (Erratum E1).
- `snapshot: {type: bool}` input + `autosnap` binding in both destroy workflows.
- `AutoSnapshotParams.snapshot`; `AutoSnapshot.execute` tests it **first** (decision 5).
- `attempt_pre_destroy_snapshot`'s status gate removed (Erratum E2).
- `ClusterService.destroy` no longer awaits a snapshot; the 501 capability guard stays.
- **Verb catalog unchanged at 33** — this reused `cluster.auto_snapshot` rather than adding one.

Suite: 2501 passed, 44 skipped.
