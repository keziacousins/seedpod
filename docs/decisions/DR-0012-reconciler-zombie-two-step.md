---
title: DR-0012 — Reconciler maps ZombieIntent two-step through the ZOMBIE state
type: decision
status: active
created: 2026-07-16
updated: 2026-07-16
---

# DR-0012: Reconciler maps `ZombieIntent` two-step through the ZOMBIE state

**Status: ACTIVE — ratified by Kezia, 2026-07-16. Pins the reconciler's `ZombieIntent →
event` mapping ahead of the Round-5 build; resolves the seam-a §H ambiguity between the three
live zombie edges.**
(Origin: Round-5 grounding pass — v1 reconciler/health archaeology, 2026-07-16.)

## Problem

The provider `Reconcile` command returns salvaged `ZombieIntent`s for the v1 condition "DB
says DESTROYED but the infrastructure is still alive"
(`reference-code/seedpod/seedpod/core/reconciliation.py:290-299`). In v1 that intent drove a
single transition, DESTROYED → DESTROY_SCHEDULED (`reconciliation.py:424-437`), so the ZOMBIE
state **was never actually written** — seam-a:11 records this as v1's real behavior.

v2 deliberately promoted ZOMBIE to a first-class, UI-rendered state (StatusBadge,
docs/design/ui-contract.md:99) and split detection from cleanup across **three live edges**
(`seedpod/core/machine.py`; seam-a §H):

- `DESTROYED × InfraRunningObserved → ZOMBIE` (`machine.py:538`; seam-a:361 — "v1
  reconciliation.py:299 semantics, exactly") — **detection**.
- `ZOMBIE × DestroyRequested → DESTROY_SCHEDULED`, `pre_destroy_state=ZOMBIE`
  (`machine.py:542`; seam-a:363 — "ZombieIntent fires this") — **cleanup**.
- `DESTROYED × DestroyRequested → DESTROY_SCHEDULED`, `pre_destroy_state=DESTROYED`
  (`machine.py:519`; seam-a:359 — "v1 zombie-cleanup re-destroy") — **direct re-destroy**.

The machine is therefore *over-specified* for the reconciler: nothing in the seam specs pins
which event the reconciler emits for a `ZombieIntent`. A direct `DestroyRequested` (edge 519)
is the closest literal port of v1, but it leaves the ZOMBIE state the v2 machine deliberately
added unreachable from the only code that would ever enter it — a silent regression of a
conscious v2 addition. Detect-only (`InfraRunningObserved` and stop) regresses v1's
auto-cleanup: a live zombie would sit untouched until a human issued a manual destroy.

## Decision

The reconciler maps each `ZombieIntent` **two-step, through the ZOMBIE state**, as two
separate `Dispatcher.apply()` calls (hence two DB transactions — DR-0008 forbids holding one
across the provider probe that produced the intent):

### Phase 1 — detect (per intent)
Emit `InfraRunningObserved` (an `Observation`; actor `reconciler`) against the DESTROYED
record → DESTROYED → ZOMBIE (`machine.py:538`). The zombie becomes a first-class, audited
state with a `reconciler`-attributed transition explaining *why* — an audit trail v1 lacked.
Applying `InfraRunningObserved` to a record already in ZOMBIE Ignores under the totality law
(seam-a:308), so a re-detected zombie is a no-op.

### Phase 2 — cleanup (once per pass, after all providers' intents are applied)
The reconciler sweeps every cluster **currently in ZOMBIE** and emits `DestroyRequested`
(actor `reconciler`, `due_at = None` ⇒ fire immediately) → ZOMBIE → DESTROY_SCHEDULED, arming
the destroy timer (`machine.py:542`). The sweep is a plain DB read; it covers both zombies
promoted this pass **and any left in ZOMBIE by a crashed prior pass** — this is what makes the
two-step crash-safe: no zombie can get stuck between the phases, because Phase 2 re-derives its
worklist from durable state every pass, not from this pass's Phase-1 output.

Idempotency holds end to end: a second `DestroyRequested` against an already-scheduled record
takes the dup-ignore path (`machine.py:412`, seam-a:362). Discovered-origin zombies
(`origin == DISCOVERED`) require `force=True` on the `DestroyRequested` per the discovered
guard (seam-a:306); the reconciler is a privileged actor and sets it.

Edge 519 (`DESTROYED × DestroyRequested` direct) is **not** used by the reconciler; it stays
reserved for manual/API re-destroy of an already-DESTROYED record.

## Consequences

- Preserves v1's auto-cleanup of zombies (the edge behavior CLAUDE.md's one-failure-mode rule
  protects) **and** exercises the ZOMBIE state v2 added on purpose — the only mapping that
  satisfies both. The operator-visible ZOMBIE window is brief but the detection is durably
  audited.
- The reconciler carries a small amount of state-aware logic: a `ZombieIntent` produces
  detection, and a separate ZOMBIE sweep produces cleanup. Both re-derive from durable state,
  so the reconciler stays a stateless-per-pass function of (DB, provider truth).
- Round-5 pinned tests: a `ZombieIntent` drives DESTROYED → ZOMBIE → DESTROY_SCHEDULED with two
  audited transitions both actored `reconciler`; a crash after Phase 1 leaves a ZOMBIE that the
  next pass's sweep cleans up (no stuck zombie); a discovered-origin zombie is destroyed only
  because the reconciler sets `force`; a ZOMBIE adopted by an operator
  (`ZOMBIE × AdoptRequested → ACTIVE`, `machine.py:554`) before the sweep is not re-destroyed
  (the sweep re-reads and finds it ACTIVE).
- No existing seam doc changes state (the reconciler is unbuilt); this DR is the normative
  source for its zombie mapping. seam-a §H's three zombie edges are unchanged — this pins which
  the reconciler uses.

## Alternatives considered

- **Direct `DestroyRequested` on DESTROYED (edge 519), one event, exact v1 single-step**
  (rejected: never enters ZOMBIE, so the state v2 deliberately added is unreachable from the
  reconciler — re-inherits v1's "never written" outcome that seam-a:11 flags, and loses the
  detection audit trail).
- **Detect-only: `InfraRunningObserved` → ZOMBIE, wait for manual/API destroy** (rejected:
  regresses v1's auto-cleanup — a live zombie lingers until a human acts; the failure mode the
  project charter names).
- **Single intent → two applies inline, no ZOMBIE sweep** (rejected: a crash between the two
  applies strands the cluster in ZOMBIE forever, because the provider's salvaged zombie
  detection keys on DESTROYED and never re-emits for a ZOMBIE-status cluster; the pass-scoped
  sweep over durable ZOMBIE state is what closes that hole).
- **Extend provider zombie detection to also fire for ZOMBIE-status clusters** (rejected:
  mutates salvaged, conformance-tested provider logic copied verbatim per DR-0004/seam-c; the
  crash-recovery duty belongs to the reconciler, not the stateless provider).
