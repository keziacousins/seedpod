---
title: Seam A — Core state machine (Pillar 1)
type: design
status: active
created: 2026-07-12
updated: 2026-07-16
amended-by: coherence-review.md   # Conflicts 1–3, 8, 11, 12, 15, 16 override where they touch this spec
# also amended-by: DR-0009 (§D Timer delivery — conditional consume)
---

All verification done — the plan, the disputed REJECTED write sites (they exist: `presets.py:798-805`, `cluster_manager.py:1196-1212`), and v1's actual zombie semantics (detection is "DESTROYED in DB but droplet exists", reconciliation.py:291-299; the ZombieIntent goes DESTROYED→DESTROY_SCHEDULED, never through a real ACTIVE→ZOMBIE write). Producing the final seam specification.

# Seam A — Core state machine (Pillar 1): FINAL SPECIFICATION

## Verdicts

**Proposal 1 (durability-first)** — Winner on the executor side. The tx-lane/drain-lane split, the deterministic `effect_id = {aggregate}@{version}#{ordinal}` idempotency key, the single `apply()` write path, and the in-transaction `AdvanceDeployments` cascade are the strongest, most complete answer to "H7 closed by persistence" and exactly-once effect commitment; its `ClusterReady` cascade (deployments wait in PENDING until the cluster machine says go) is the right coordination model and is grafted wholesale. Fatal flaw on the machine side: it drops the DESTROYED→ZOMBIE edge — but v1's zombie *is literally* "DESTROYED in DB, droplet exists" (reconciliation.py:299), so P1's ACTIVE-only `ZombieDetected` misses the one zombie path v1 actually exercises. Its ★-actor privilege check and decision-time `Notify.environment` resolution are kept.

**Proposal 2 (test-ergonomics-first)** — Winner on the machine side. The event-class totality law (Commands may be Invalid; Reports/Timers/Observations/Cascades are *never* invalid, defaulting to explicit Ignore) is the single best idea in any proposal: it closes every stale-event race, the force=True sprawl, and the "monitor job won race" special case as one structural rule, testable in one exhaustive loop. `pre_destroy_state`, the timer-driven DESTROY_SCHEDULED model, the correct zombie semantics, and the builder/property-test harness are all adopted. Flaws: its outbox is thinner than P1's (timers/workflows drained async rather than committed in-tx — a durability gap P1 closes); it enqueues the deploy workflow at deployment creation, forcing the engine to gate on cluster readiness (P1's cascade is cleaner); its TTL arming is inconsistent (FAILED×TtlExpired row exists but the timer is only armed on ProvisionSucceeded).

**Proposal 3 (fidelity-first)** — Its v1→v2 disposition table and LOUD-callout discipline are excellent and the format is adopted below. As a design it loses: keeping CREATING/PROVISIONING/DEPLOYING as cluster states directly contradicts the plan ("shrink what counts as a state"; deployment failure "not smeared onto cluster state") and preserves the `DEPLOYMENT_FAILED→ACTIVE` smear the plan orders resolved; `ReconcileSync(observed=<any state>)` is v1's `force=True` in event clothing — the generic "set state to X" both other proposals correctly make unrepresentable; it deliberately re-pins naive-UTC timestamps (gotcha 18) and the SSE-without-persist no-op quirk; and it is factually wrong that deployment REJECTED has no v1 write site (two exist).

---

## THE FINAL SPEC

Everything below is self-contained and binding. Two pure machines (cluster, deployment) share one effect union, one codec, one outbox, one executor. **No locks anywhere in v2.** All timestamps are **aware UTC**, serialized ISO-8601 with trailing `Z` (v1's naive-UTC convention is retired; the wire string shape is unchanged for consumers).

### A. Effect union

```python
# seedpod2/core/effects.py — inert, frozen, serializable data. Zero behavior.
from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any, ClassVar, Mapping, Union

class EffectKind(StrEnum):
    PERSIST         = "persist"          # tx lane
    SCHEDULE_TIMER  = "schedule_timer"   # tx lane
    CANCEL_TIMER    = "cancel_timer"     # tx lane
    RUN_WORKFLOW    = "run_workflow"     # tx lane (row insert; engine executes)
    CANCEL_WORKFLOW = "cancel_workflow"  # tx lane (cancel flag; engine token polls it)
    CASCADE         = "cascade"          # tx lane (in-tx pure transitions on sibling records)
    NOTIFY          = "notify"           # drain lane (leaves the DB post-commit)

@dataclass(frozen=True, slots=True, kw_only=True)
class Persist:
    kind: ClassVar[str] = EffectKind.PERSIST
    record: "ClusterRecord | DeploymentRecord"   # full post-transition image, version already +1
    expected_version: int | None                 # None ⇒ INSERT (birth); else CAS UPDATE … WHERE version=expected

@dataclass(frozen=True, slots=True, kw_only=True)
class Notify:
    kind: ClassVar[str] = EffectKind.NOTIFY
    topic: str                       # "cluster_state_changed" | "deployment_status_changed"  (v1 names, verbatim)
    payload: Mapping[str, Any]       # v1-shaped: {cluster_id, old_status, new_status, …}; JSON-safe scalars only
    environment: str | None          # SSE env filter, resolved AT DECISION TIME from the record

@dataclass(frozen=True, slots=True, kw_only=True)
class RunWorkflow:
    kind: ClassVar[str] = EffectKind.RUN_WORKFLOW
    workflow: str                    # "provision" | "deploy" | "destroy" — closed registry, grows as verbs do
    cluster_id: str
    deployment_id: str | None = None
    args: Mapping[str, Any] = ()     # typed per workflow in Pillar 2; refs only, never secrets

@dataclass(frozen=True, slots=True, kw_only=True)
class CancelWorkflow:
    kind: ClassVar[str] = EffectKind.CANCEL_WORKFLOW
    workflow: str
    cluster_id: str
    deployment_id: str | None = None
    reason: str = ""

@dataclass(frozen=True, slots=True, kw_only=True)
class ScheduleTimer:
    kind: ClassVar[str] = EffectKind.SCHEDULE_TIMER
    aggregate_type: str              # "cluster" | "deployment"
    aggregate_id: str
    timer_key: str                   # "ttl" | "destroy" — upsert key ⇒ re-arming idempotent by construction
    fire_at: datetime                # ABSOLUTE aware-UTC, computed from event.at / record fields — transition never calls now()
    event: "Event"                   # the fact injected through apply() when it fires

@dataclass(frozen=True, slots=True, kw_only=True)
class CancelTimer:
    kind: ClassVar[str] = EffectKind.CANCEL_TIMER
    aggregate_type: str
    aggregate_id: str
    timer_key: str | None            # None = all timers for the aggregate

@dataclass(frozen=True, slots=True, kw_only=True)
class Cascade:
    """In-tx fan-out: apply `event` through the pure deployment transition to every deployment
    of `cluster_id` whose state is in `where_state` (excluding except_id). Nested effects join
    the SAME transaction/outbox. Replaces v1 _mark_deployments_destroyed and the supersede ORM
    writes; the machines stay the single author of every status change. Depth is asserted ≤ 2."""
    kind: ClassVar[str] = EffectKind.CASCADE
    cluster_id: str
    where_state: frozenset["DeploymentState"]
    event: "DeploymentEvent"
    except_id: str | None = None

Effect = Union[Persist, Notify, RunWorkflow, CancelWorkflow, ScheduleTimer, CancelTimer, Cascade]
```

### B. Serialization codec

One registry-driven codec, property-tested for round-trip (`decode(encode(x)) == x`). No pickle, no `__dict__` magic.

```python
# seedpod2/core/codec.py
def encode(x: Event | Effect) -> dict:      # {"kind": …, <field>: _enc(value), …}
    ...
def decode_event(d: dict) -> Event: ...
def decode_effect(d: dict) -> Effect: ...
# _enc: aware datetime → ISO-8601 "…Z" (asserts tzinfo is not None — naive datetimes are banned in v2);
# StrEnum → value; nested Event/Effect/record dataclass → encode recursively; tuple/frozenset → sorted list;
# Mapping → dict. _dec reverses via dataclass field type hints. Canonical JSON: sorted keys, no NaN.
```

### C. Durable outbox + timers (fresh schema, Phase 0)

```sql
CREATE TABLE effects_outbox (
    seq            INTEGER PRIMARY KEY AUTOINCREMENT,      -- global drain order (BIGSERIAL on PG)
    effect_id      TEXT    NOT NULL UNIQUE,                -- "{aggregate_type}/{aggregate_id}@{to_version}#{ordinal}"
    aggregate_type TEXT    NOT NULL CHECK (aggregate_type IN ('cluster','deployment')),
    aggregate_id   TEXT    NOT NULL,
    to_version     INTEGER NOT NULL,                       -- aggregate version AFTER the emitting transition
    ordinal        INTEGER NOT NULL,                       -- position in the transition's effect tuple
    kind           TEXT    NOT NULL,
    payload        TEXT    NOT NULL,                       -- canonical JSON from encode()
    lane           TEXT    NOT NULL CHECK (lane IN ('tx','drain')),
    status         TEXT    NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','done','dead')),
    attempts       INTEGER NOT NULL DEFAULT 0,
    available_at   TEXT    NOT NULL,                       -- backoff gate (drain lane)
    created_at     TEXT    NOT NULL,
    done_at        TEXT,
    last_error     TEXT
);
CREATE INDEX idx_outbox_drain     ON effects_outbox (status, available_at, seq);
CREATE INDEX idx_outbox_aggregate ON effects_outbox (aggregate_type, aggregate_id, seq);

CREATE TABLE timers (
    aggregate_type    TEXT NOT NULL,
    aggregate_id      TEXT NOT NULL,
    timer_key         TEXT NOT NULL,
    fire_at           TEXT NOT NULL,
    event             TEXT NOT NULL,          -- encode(event); delivered verbatim through apply() on fire
    created_by_effect TEXT NOT NULL,          -- provenance: effect_id that (re)armed it
    PRIMARY KEY (aggregate_type, aggregate_id, timer_key)
);
CREATE INDEX idx_timers_fire ON timers (fire_at);

-- clusters and deployments both carry:  version INTEGER NOT NULL  (optimistic CAS)
-- cluster_state_audits / deployment_state_audits: (aggregate_id, from_state, to_state, event_kind,
--   actor, at TEXT aware-UTC 'Z', metadata JSON) — written by the executor, same tx as Persist.
-- workflow_runs (Pillar 2) carries: dedupe_key TEXT UNIQUE, cancel_requested INTEGER DEFAULT 0.
```

### D. Executor contract — the single write path

```python
# seedpod2/core/apply.py — the ONLY way state changes in v2. No locks. (H8/H9 retired wholesale.)
async def apply(uow: UnitOfWork, aggregate: str, aggregate_id: str, event: Event) -> TransitionResult:
    record = await uow.load(aggregate, aggregate_id)               # NEW records built in-memory by the service
    result = transition(record, event)                             # PURE — the whole of Pillar 1
    if not result.effects:                                         # Ignore: nothing written, nothing notified
        return result
    for ordinal, eff in enumerate(result.effects):
        row = outbox_row(eff, aggregate, result.record.id, result.record.version, ordinal)
        match eff:
            case Persist():        await uow.persist(eff)          # INSERT, or CAS UPDATE; rowcount==0 → StaleVersion
                                   await uow.insert_audit(aggregate, record, result, event)
            case ScheduleTimer():  await uow.upsert_timer(eff, row.effect_id)
            case CancelTimer():    await uow.delete_timers(eff)
            case RunWorkflow():    await uow.insert_run(eff, dedupe_key=row.effect_id)   # ON CONFLICT DO NOTHING
            case CancelWorkflow(): await uow.flag_cancel(eff)
            case Cascade():
                for dep in await uow.deployments_in(eff.cluster_id, eff.where_state, eff.except_id):
                    await apply(uow, "deployment", dep.id, eff.event)   # same tx; depth asserted ≤ 2
            case Notify():         row.status = "pending"          # all tx-lane rows above insert as status='done'
        await uow.insert_outbox(row)
    await uow.commit()                                             # ← the one commit
    drainer.nudge(); engine.nudge(); timer_service.nudge()         # in-process hints; polling is the backstop
    return result
```

Binding rules:

- **Lanes.** `Persist/ScheduleTimer/CancelTimer/RunWorkflow/CancelWorkflow/Cascade` execute *inside* the commit transaction (their outbox row is the audit of what was decided, inserted `done`). `Notify` (and future external kinds) insert `pending` and are drained post-commit. **H7 is structurally impossible:** a Notify exists iff the state change committed; broadcast failure can neither roll back nor fail a transition; a crash mid-broadcast replays from the outbox.
- **Idempotency.** `effect_id` is deterministic from the transition; `UNIQUE(effect_id)` + the Persist CAS means one transition's effects commit exactly once. `RunWorkflow` dedupes on `dedupe_key = effect_id`; `ScheduleTimer` is an upsert on `timer_key`.
- **Notify drain semantics:** at-least-once attempt, best-effort delivery. Broadcast, mark `done`; broadcast exceptions are logged and the row is marked `done` after 1 attempt (duplicate SSE on crash-replay is harmless; UI reconciles by re-fetch on reconnect). Future external kinds: backoff `[1s, 5s, 30s, 2m, 10m…]` via `available_at`; `attempts ≥ 8` ⇒ `dead`, surfaced by reconciliation.
- **`StaleVersion`:** caller re-reads and re-decides, bounded to 3 attempts. This closes gotcha 16: a health job's `HealthCheckFailed` built from a stale read loses the CAS and must re-decide against the fresh record. The v1 re-entrancy deadlock (gotcha 10) cannot recur: self-triggered events are in-tx `Cascade`s; there is no lock to re-enter.
- **Timer delivery (amended by DR-0009):** timer service polls `timers WHERE fire_at <= now`; per timer, one transaction: **conditional consume** — `DELETE … WHERE (pk) AND fire_at = :snapshot` (the fire_at this scan pass saw); rowcount 1 ⇒ `apply(decode_event(…))` in the same transaction + commit; rowcount 0 ⇒ a concurrent same-key re-arm/cancel won the scan-to-fire window: skip the apply entirely (the surviving row, if any, fires at its own deadline on a later pass). Atomic consume, at-least-once. Staleness coverage is two-part: machine Ignore rows absorb fires whose state moved on; conditional consume covers the same-state re-arm the machine cannot see (e.g. `ACTIVE × TtlExpired` after a TTL extend).
- **Ordering:** single process, one drainer, `ORDER BY seq`. Multi-process later ⇒ `FOR UPDATE SKIP LOCKED`; nothing else changes.

### E. Records and state sets

```python
# seedpod2/core/records.py — frozen dataclass DTOs; the machine never sees ORM objects or kubeconfig bytes.
class Origin(StrEnum):
    MANAGED = "managed"
    DISCOVERED = "discovered"

class ClusterState(StrEnum):          # 10 (v1 had 11); string values keep v1's hyphens verbatim
    NEW               = "new"               # pre-persistence; makes birth a real, audited transition (gotcha 15)
    PROVISIONING      = "provisioning"      # absorbs v1 CREATING; v1 DEPLOYING is deleted (see disposition table)
    ACTIVE            = "active"
    DESTROY_SCHEDULED = "destroy-scheduled"
    DESTROYING        = "destroying"
    DESTROYED         = "destroyed"
    DESTROY_FAILED    = "destroy-failed"
    FAILED            = "failed"
    ZOMBIE            = "zombie"            # crisp v1 semantics: records say destroyed, provider says running
    UNMANAGED         = "unmanaged"

@dataclass(frozen=True, slots=True, kw_only=True)
class ClusterRecord:
    id: str
    name: str
    state: ClusterState
    version: int
    provider: str                            # "digitalocean" | "kind" | "tart" | "orbstack"
    environment: str
    origin: Origin
    expires_at: datetime | None = None       # TTL, aware UTC
    public_ip: str | None = None
    kubeconfig_ref: str | None = None        # opaque secret ref — never kubeconfig bytes
    provider_resource_id: str | None = None
    pre_destroy_state: ClusterState | None = None   # set on entry to DESTROY_SCHEDULED; cancel returns here
    failure_reason: str | None = None

class DeploymentState(StrEnum):       # v1's raw-string column promoted to a real machine; all 8 statuses kept
    NEW        = "new"
    PENDING    = "pending"
    DEPLOYING  = "deploying"
    ACTIVE     = "active"
    FAILED     = "failed"
    SUPERSEDED = "superseded"
    CANCELLED  = "cancelled"
    REJECTED   = "rejected"           # real v1 write sites: presets.py:798, cluster_manager.py:1196
    DESTROYED  = "destroyed"

@dataclass(frozen=True, slots=True, kw_only=True)
class DeploymentRecord:
    id: str
    cluster_id: str
    state: DeploymentState
    version: int
    environment: str
    manifest_version: str
    failure_reason: str | None = None
    superseded_by: str | None = None
```

### F. Event taxonomy and union

Every event is a frozen, slotted, kind-registered dataclass with two mandatory base fields: `at: datetime` (aware UTC, **caller-supplied** — `transition()` never calls `now()`) and `actor: str` (`api:<user>` | `reconciler` | `health` | `engine:run:<run_id>` | `timer:<key>` | `cluster-machine`). Events serialize as tagged JSON via the codec (needed for `timers.event`).

**The five event classes drive the totality law:**

```python
class Command(Event): ...       # user/API/reconciler intent — MAY raise InvalidTransition (caller's 409)
class Report(Event): ...        # workflow-run outcome — NEVER invalid; stale/duplicate ⇒ Ignore
class TimerFired(Event): ...    # durable timer delivery — NEVER invalid; raced ⇒ Ignore
class Observation(Event): ...   # reconciler/health facts — NEVER invalid; THE replacement for force=True
class Cascaded(Event): ...      # delivered by the Cascade effect (or chained by the service layer)
```

```python
# ---- Cluster events (15) ----
class CreateRequested(Command):        pass                                  # applied to service-built NEW record
class Discovered(Command):             observed: DiscoveredInfo              # reconciler found foreign infra
class RetryRequested(Command):         pass
class AdoptRequested(Command):         pass                                  # takeover / rehabilitation
class DestroyRequested(Command):       due_at: datetime | None = None; force: bool = False
class DestroyCancelled(Command):       pass
class TtlExpired(TimerFired):          pass                                  # timer_key "ttl"
class DestroyDue(TimerFired):          pass                                  # timer_key "destroy"
class ProvisionSucceeded(Report):      public_ip: str; kubeconfig_ref: str; provider_resource_id: str | None = None
class ProvisionFailed(Report):         reason: str                           # fired after compensation ran
class DestroySucceeded(Report):        pass
class DestroyFailed(Report):           reason: str
class InfraRunningObserved(Observation): pass                                # provider confirms PRESENT
class InfraMissingObserved(Observation): pass    # provider confirms ABSENT — only after the salvaged
                                                 # InfrastructureUnreachableError distinction says "absent", never "unreachable"
class HealthCheckFailed(Observation):  reason: str

# ---- Deployment events (8) ----
class DeployRequested(Command):        spec_ref: str                         # applied to service-built NEW record
class DeployRejected(Command):         reason: str                           # rule engine said no; audited record
class CancelRequested(Command):        reason: str = ""                      # api | (via Cascade) cluster machine
class ClusterReady(Cascaded):          pass                                  # cluster-machine cascade | api redeploy chain
class DeploySucceeded(Report):         resolved_images: Mapping[str, str] = ()
class DeployFailed(Report):            reason: str
class SupersededBy(Cascaded):          new_deployment_id: str
class ClusterGone(Cascaded):           pass
```

**`force=True` is retired**, replaced by two principled mechanisms:

1. **Observations are privileged facts.** `transition()` raises `InvalidTransition` unless an `Observation`'s actor is `reconciler` or `health` (a pure check on the event field). Every v1 dynamic-target/force sync site reduces to `{InfraRunningObserved, InfraMissingObserved, HealthCheckFailed}` + the Reports; a generic "set state to X" event **does not exist and is unrepresentable**.
2. **The discovered-cluster guard is a pure event-field check:** on records with `origin == DISCOVERED`, `DestroyRequested` requires `force=True`, else `InvalidTransition` (exact port of v1's intersection semantics, gotcha 6 — v1 applied it at ACTIVE; UNMANAGED "manual cleanup" stays unguarded as in v1).

**Totality law (binding):** the transition tables are `dict[(state, event_type) → rule]`, made total by `_fill_defaults`: on `NEW`, only the birth Commands are valid and everything else is Invalid; on all other states, unlisted Commands → `InvalidTransition`, unlisted Report/TimerFired/Observation/Cascaded → **Ignore** (unchanged record, empty effects, no version bump, no SSE, no audit — unlike v1's no-op path, gotcha 7). `InvalidTransition` from `api:*` actors → HTTP 409; from `reconciler`/`engine:*`/`timer:*` → logged and dropped.

```python
# seedpod2/core/machine.py
@dataclass(frozen=True, slots=True)
class TransitionResult:
    record: ClusterRecord | DeploymentRecord   # version bumped iff a Persist is present
    effects: tuple[Effect, ...]                # () ⇒ Ignore

class InvalidTransition(Exception): ...
class StaleVersion(Exception): ...

def transition(record, event) -> TransitionResult:
    """Pure. No IO, no clock, no locks. Total over (state × event-type)."""
    table = CLUSTER_TABLE if isinstance(record, ClusterRecord) else DEPLOYMENT_TABLE
    return table[(record.state, type(event))](record, event)
```

Rules build the new record with `dataclasses.replace(record, state=…, version=record.version + 1, <explicit typed fields>)` and emit `Persist(record=new, expected_version=old.version)`. v1's `setattr`-if-`hasattr` kwarg smearing (gotcha 8) has no equivalent — unknown fields don't typecheck.

### G. Cluster transition table (COMPLETE — 10 × 15; unlisted cells follow the totality-law defaults)

Legend: **P** = Persist(+audit), **N** = Notify(`cluster_state_changed`, v1-shaped payload, env from record), **RW(w)** = RunWorkflow, **ST(k@t, ev)** = ScheduleTimer, **CT(k)** = CancelTimer, **Casc(states, ev)** = Cascade. "if ttl" = `record.expires_at is not None`. `Casc-gone` = `Casc(all states except DESTROYED, ClusterGone)`. **P and N are always emitted together on every state change.**

| State | Event | → State | Additional effects & record updates |
|---|---|---|---|
| NEW | CreateRequested | PROVISIONING | P(INSERT), N(`old_status:""` — preserves v1's UI-visible birth broadcast shape), RW(provision) |
| NEW | Discovered | UNMANAGED | P(INSERT, origin=DISCOVERED, +observed fields), N |
| PROVISIONING | ProvisionSucceeded | ACTIVE | sets `public_ip`, `kubeconfig_ref`, `provider_resource_id`; ST(ttl@expires_at, TtlExpired) if ttl; **Casc({PENDING}, ClusterReady)** — kicks the waiting deployment |
| PROVISIONING | ProvisionFailed | FAILED | sets `failure_reason`; ST(ttl@expires_at, TtlExpired) if ttl *(failed clusters get TTL auto-cleanup — closes a v1 leak)* |
| ACTIVE | DestroyRequested † | DESTROY_SCHEDULED | `pre_destroy_state=ACTIVE`; CT(ttl), ST(destroy@`due_at or event.at`, DestroyDue) |
| ACTIVE | TtlExpired | DESTROY_SCHEDULED | `pre_destroy_state=ACTIVE`; ST(destroy@event.at, DestroyDue) |
| ACTIVE | HealthCheckFailed | FAILED | sets `failure_reason`; ttl timer stays armed (FAILED×TtlExpired cleans up) |
| ACTIVE | InfraMissingObserved | DESTROYED | CT(all), Casc-gone *(v1's force=True orphan intent, now a legal edge)* |
| ACTIVE | ProvisionSucceeded | *Ignore* | duplicate report — gotcha 2's law, generalized |
| ACTIVE | DestroyCancelled | *Ignore* | idempotent double-cancel |
| DESTROY_SCHEDULED | DestroyDue | DESTROYING | RW(destroy) — run row committed atomically with the state, so no separate DestroyStarted event is needed |
| DESTROY_SCHEDULED | DestroyCancelled | *`pre_destroy_state`* | clears it; CT(destroy); ST(ttl@expires_at, TtlExpired) iff returning to ACTIVE with ttl |
| DESTROY_SCHEDULED | InfraMissingObserved | DESTROYED | CT(all), Casc-gone *(v1's "droplet already gone" direct edge, gotcha 4)* |
| DESTROY_SCHEDULED | DestroyRequested † | *Ignore* | idempotent re-request |
| DESTROYING | DestroySucceeded | DESTROYED | CT(all), Casc-gone |
| DESTROYING | DestroyFailed | DESTROY_FAILED | sets `failure_reason` |
| DESTROYING | InfraMissingObserved | DESTROYED | CT(all), Casc-gone *(reconciler won the race; the late DestroySucceeded then Ignores in DESTROYED)* |
| DESTROYING | *(all Commands)* | **Invalid** | mid-destroy is not cancellable (v1 parity) |
| FAILED | RetryRequested | PROVISIONING | clears `failure_reason`; RW(provision) *(v1 FAILED→CREATING retry)* |
| FAILED | DestroyRequested † | DESTROY_SCHEDULED | `pre_destroy_state=FAILED`; CT(ttl), ST(destroy@`due_at or event.at`, DestroyDue) |
| FAILED | TtlExpired | DESTROY_SCHEDULED | `pre_destroy_state=FAILED`; ST(destroy@event.at, DestroyDue) |
| FAILED | InfraMissingObserved | DESTROYED | CT(all), Casc-gone |
| DESTROY_FAILED | DestroyRequested † | DESTROY_SCHEDULED | `pre_destroy_state=DESTROY_FAILED`; ST(destroy@…, DestroyDue) *(retry destruction)* |
| DESTROY_FAILED | AdoptRequested | ACTIVE | clears `failure_reason`, `pre_destroy_state`; ST(ttl) if ttl *(v1 "resources still running")* |
| DESTROY_FAILED | InfraMissingObserved | DESTROYED | CT(all), Casc-gone *(destroy actually worked)* |
| DESTROYED | DestroyRequested † | DESTROY_SCHEDULED | `pre_destroy_state=DESTROYED`; ST(destroy@…, DestroyDue) *(v1 zombie-cleanup re-destroy)* |
| DESTROYED | AdoptRequested | ACTIVE | ST(ttl) if ttl *(rehabilitation, v1)* |
| DESTROYED | InfraRunningObserved | ZOMBIE | *(v1 reconciliation.py:299 semantics, exactly: DB destroyed, droplet exists)* |
| DESTROYED | DestroySucceeded / InfraMissingObserved / DestroyDue / TtlExpired | *Ignore* | late duplicates / stale timers |
| ZOMBIE | DestroyRequested † | DESTROY_SCHEDULED | `pre_destroy_state=ZOMBIE`; ST(destroy@…, DestroyDue) *(ZombieIntent fires this)* |
| ZOMBIE | AdoptRequested | ACTIVE | ST(ttl) if ttl |
| ZOMBIE | InfraMissingObserved | DESTROYED | *(zombie died on its own; no Casc — deployments were cascaded on first DESTROYED)* |
| UNMANAGED | AdoptRequested | ACTIVE | `origin` stays DISCOVERED, so the destroy guard keeps protecting it |
| UNMANAGED | DestroyRequested | DESTROY_SCHEDULED | `pre_destroy_state=UNMANAGED`; ST(destroy@…, DestroyDue) *(manual cleanup; unguarded, matching v1's ACTIVE-only restriction)* |
| UNMANAGED | InfraMissingObserved | DESTROYED | record hygiene; no Casc (discovered clusters have no managed deployments) |

† discovered-cluster guard: `origin == DISCOVERED and not event.force ⇒ InvalidTransition`.

Every one of v1's 27 `ALLOWED_TRANSITIONS` edges is either reachable above via a named event, has moved inside a workflow (CREATING→PROVISIONING→DEPLOYING chain), or is a LOUD callout in §J. The map stays deliberately cyclic — DESTROYED/ZOMBIE/UNMANAGED rehabilitation and zombie-cleanup paths (gotcha 3) are preserved.

### H. Deployment transition table (COMPLETE — 9 × 8)

**N** = Notify(`deployment_status_changed`, v1-shaped payload). No workflow, job, or API handler ever writes a deployment row directly — every v1 scattered ORM write site becomes an event through `apply()`.

| State | Event | → State | Additional effects & record updates |
|---|---|---|---|
| NEW | DeployRequested | PENDING | P(INSERT), N — waits for ClusterReady; no workflow enqueued yet |
| NEW | DeployRejected | REJECTED | P(INSERT, +reason), N *(v1's audited rejected records: presets.py:798, cluster_manager.py:1196)* |
| PENDING | ClusterReady | DEPLOYING | RW(deploy, deployment_id=self.id) |
| PENDING | CancelRequested | CANCELLED | — |
| PENDING | ClusterGone | DESTROYED | — |
| DEPLOYING | DeploySucceeded | ACTIVE | sets `resolved_images`; **Casc({ACTIVE}, SupersededBy(self.id), except_id=self.id)** — supersede is a machine decision, not an ORM write |
| DEPLOYING | DeployFailed | FAILED | sets `failure_reason` — **cluster record untouched: gotcha 1's UX (infra fine, redeployable), minus the state smear** |
| DEPLOYING | CancelRequested | CANCELLED | CW(deploy) *(v1 rule kept: only pending/deploying cancellable; cluster state untouched — dissolves gotcha 13's ACTIVE-vs-FAILED cancel guesswork)* |
| DEPLOYING | ClusterGone | DESTROYED | CW(deploy) |
| ACTIVE | SupersededBy | SUPERSEDED | sets `superseded_by` |
| ACTIVE | DeploySucceeded | *Ignore* | *(v1's "monitor job won race", gotcha 2)* |
| ACTIVE | DeployFailed | *Ignore* | stale failure after success |
| ACTIVE / FAILED / SUPERSEDED / CANCELLED / REJECTED | ClusterGone | DESTROYED | *(v1 bulk `_mark_deployments_destroyed`, per-record and audited, incl. per-deployment SSE via each N)* |
| DESTROYED | *(everything non-Command)* | *Ignore* | terminal; duplicate cascades harmless |

Wiring: (a) **initial deploy** — the service chains `apply(deployment-NEW, DeployRequested)` in the same transaction as `apply(cluster-NEW, CreateRequested)`; the deployment waits in PENDING until the cluster's `ProvisionSucceeded` cascades `ClusterReady`. (b) **redeploy onto an ACTIVE cluster** — the API chains `DeployRequested` then `ClusterReady` in one transaction; **the cluster never leaves ACTIVE** (v1's ACTIVE→DEPLOYING→ACTIVE round-trip is deleted; UI composes `cluster.state × latest deployment.state`).

### I. v1 → v2 disposition table (every state, sub-event, dispatch action, force site)

| v1 thing | v2 home |
|---|---|
| `CREATING` state; `DROPLET_READY` | `provision` workflow steps `provider.create` → `wait-for-readiness` (all four providers; the droplet-flavored "Reuse event" aliasing, gotcha 12, disappears) |
| v1 `PROVISIONING` phase; `K3S_INSTALLED`/`K3S_FAILED`; hidden kubeconfig rewrite+encrypt (gotcha 9) | `provision` steps `k3s.install` → `kubeconfig.capture` (localhost→public-IP rewrite, Fernet-encrypt, store secret ref) — an explicit, tested step whose failure fails/retries the run; machine receives only `ProvisionSucceeded(kubeconfig_ref=…)` |
| `DEPLOYING` cluster state; `MANIFESTS_DEPLOYED`/`DEPLOYMENT_FAILED` | deployment machine + `deploy` workflow (`images.resolve` → `manifests.render` → `kubectl-apply` → per-wave `wait-rollout`); terminal outcomes fire `DeploySucceeded`/`DeployFailed` |
| `HEALTH_CHECK_PASSED` | **not ported** — dead in v1 |
| Dispatch: Cloudflare DNS + Traefik HelmChartConfig on DEPLOYING (gotcha 17) | `provision`/`deploy` steps `resolve-dns`, `traefik.configure`, **best-effort** (step-level `on_error: continue`) — v1's deliberate non-fatal semantics preserved |
| Dispatch: schedule deployment/destruction jobs; destruction polling; `_schedule_follow_up_transition` fire-and-forget | `RunWorkflow` effects; destroy-poll = `infra.wait-gone` step; durable `ScheduleTimer` rows |
| Dispatch: `_mark_deployments_destroyed` (H10 site) | `Cascade(…, ClusterGone)` in-tx, per-record, audited, with per-deployment Notify |
| Initial creation bypass (repo insert + manual empty-old-status SSE, gotcha 15/7) | `NEW × CreateRequested` — real audited birth transition whose Notify keeps the `old_status:""` payload shape |
| `force=True`: provider/status syncs (job_manager:600, reconciliation:720, cluster_manager:493, status_sync_job:81, job_executor:151, generic_operation_job:552 — gotcha 11) | reconciler translates observations into `InfraRunningObserved` / `InfraMissingObserved` (+ Reports); "set state to X" unrepresentable |
| `force=True`: orphan → DESTROYED (reconciliation:459,662) | `InfraMissingObserved` |
| `force=True`: zombie resurrection / startup → ACTIVE (clusters.py:845, reconciliation:610) | `AdoptRequested` (actor `reconciler` or `api:*`) |
| `force=True`: creation-error → FAILED / destroy-error → DESTROY_FAILED (cluster_manager:380,581) | `ProvisionFailed` / `DestroyFailed` — legal Report edges, no force needed |
| Startup "mark stuck PROVISIONING/DEPLOYING FAILED" (reconciliation:623,636) | engine **resumes** from the persisted step cursor; only a genuinely dead run fires `ProvisionFailed`/`DeployFailed` |
| API destroy force passthrough (clusters.py:491, cluster_manager:562) | `DestroyRequested(force=…)` — the guard is a pure field check |
| Production-cluster destroy gate (clusters.py:472-477: `environment=="production" and not force` → 400) | **survives at the service edge** (DR-0018): `ClusterService.destroy` rejects a managed `environment=="production"` cluster without `force=True`; distinct from and additional to the machine's discovered-origin guard. Not a machine invariant — an edge policy, as in v1 |
| Deployment cancel return-state logic (deployments.py:1130-1150, gotcha 13) | dissolved — cluster never leaves ACTIVE for deploys, so there is nothing to return it to |
| Event kwargs → column `setattr` smear (gotcha 8) | explicit typed `dataclasses.replace` per rule; unknown fields don't typecheck |
| Per-cluster `asyncio.Lock` dict + 30s timeout (H8/H9) | deleted; optimistic CAS + `StaleVersion` re-decide |
| Naive-UTC audit timestamps + hand-appended "Z" (gotcha 18) | aware UTC everywhere; wire format identical |

### J. LOUD callouts — v1 behaviors deliberately NOT ported (each documented, none silent)

1. **ACTIVE→ZOMBIE and ACTIVE→UNMANAGED edges dropped.** No v1 call site fires either through the state machine except force-based dynamic syncs; v1's real zombie is detected on DESTROYED (reconciliation.py:291-299), which v2 ports exactly. Discovered infra is born UNMANAGED via `Discovered`. If a reconciler case surfaces that needs re-marking an ACTIVE cluster, add one Observation event — a reviewed verb, not a bypass.
2. **Stale `DEPLOYMENT_FAILED` resurrecting a dying cluster** (v1's any-state ladder could pull DESTROY_SCHEDULED/…→ACTIVE): deployment failures no longer touch cluster state at all.
3. **Health check FAILing a cluster mid-redeploy** (job_manager:496 stale reads): `HealthCheckFailed` is legal only at ACTIVE; the CAS additionally rejects stale writers.
4. **DESTROY_SCHEDULED → DESTROY_FAILED** ("destruction job failed to schedule"): structurally impossible — the run row commits atomically with DESTROYING.
5. **No-op transitions emitting SSE and applying column updates** (gotcha 7): v2 `Ignore` writes and notifies nothing. The one load-bearing v1 no-op broadcast (initial CREATING) is covered by the real birth transition's Notify.
6. **`HEALTH_CHECK_PASSED`**: dead enum member, not ported.
7. **Destroy-cancel always returning to ACTIVE**: replaced by `pre_destroy_state` — cancelling the destroy of a FAILED/ZOMBIE cluster no longer silently resurrects it.
8. **No timer-driven destroy-retry loop from DESTROY_FAILED**: retries are explicit `DestroyRequested` (reconciler or human), as in v1.

### K. Test ergonomics contract (binding on Phase 1)

- `tests/core/` imports **no** `unittest.mock` (the plan's design sensor). Harness = record builders (`a_cluster(state=…, **over)`, `a_deployment(…)`) + one canonical event instance per type (`AN_EVENT`, with a meta-test asserting it covers every registered kind).
- **Totality test**: iterate `product(States, Events)` per machine; every cell exists; `InvalidTransition` may only escape for `Command` subclasses or on `NEW`; any non-Ignore result contains exactly one `Persist` whose `record == result.record` and `record.version == old.version + 1`; Ignore results equal `TransitionResult(old_record, ())` exactly.
- **Exact-equality effect tests** per interesting row (frozen dataclasses make `assert result.effects == (…,)` free).
- **Property tests** (hypothesis): codec round-trip over generated events/effects; every entry into DESTROY_SCHEDULED emits exactly one `ScheduleTimer("destroy",…)` and sets `pre_destroy_state`; destroy-cancel returns to `pre_destroy_state` from every destroy-requestable state; `DeploySucceeded` from DEPLOYING always cascades `SupersededBy` with `where_state == {ACTIVE}` and `except_id == self.id`; Notify payload values are JSON scalars; no rule emits two Persists or a Persist with `expected_version != old.version`.

---

## Taste calls for the human (max 3)

1. **Chose deleting cluster `DEPLOYING` (P1/P2) over keeping all 11 v1 states for wire fidelity (P3)** because the plan explicitly orders deployment progress off the cluster state, and keeping DEPLOYING preserves the exact smear (`DEPLOYMENT_FAILED→ACTIVE`, cancel-return guesswork) the rebuild exists to kill; the UI composes `cluster.state × latest deployment.state` instead — flip if UI/SSE consumers can't absorb that query change at cutover.
2. **Chose `pre_destroy_state` (P2) over v1's unconditional cancel-to-ACTIVE** because v1's behavior silently resurrects FAILED/ZOMBIE/DESTROYED clusters on destroy-cancel; the cost is one data-dependent target in the table — flip if you'd rather pin v1's exact cancel semantics for zero drift.
3. **Chose forbidding `DestroyRequested` during PROVISIONING (v1 parity, and it removes `CancelWorkflow` from the cluster machine entirely) over P2's new abort-provision edge** because it's the one edge v1 never had and mid-provision compensation ordering (cancel token → undo steps → destroy) is Pillar-2 work; the cost is un-abortable provisions until the run terminates — flip if operators need kill-switch UX in v2.0.
