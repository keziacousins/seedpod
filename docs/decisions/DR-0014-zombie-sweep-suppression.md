---
title: DR-0014 — The DR-0012 Phase-2 ZOMBIE sweep inherits Phase-C destructive-intent suppression
type: decision
status: active
created: 2026-07-16
updated: 2026-07-17
---

# DR-0014: The DR-0012 Phase-2 ZOMBIE sweep inherits Phase-C suppression

**Status: ACTIVE — ratified by Kezia, 2026-07-17. Gap 2 of the first Round-5 halt (reconciliation
judge, run `wf_2d729255-fa5`, 2026-07-16). Amends DR-0012.**

## Problem

DR-0012 splits zombie handling into Phase 1 (`InfraRunningObserved` → ZOMBIE, detection) and a
Phase-2 pass-scoped sweep (`DestroyRequested` on every current-ZOMBIE record → DESTROY_SCHEDULED,
the actual re-destruction). It is **silent on whether the Phase-2 sweep re-applies Phase-C
destructive-intent suppression** — coherence-review Conflict 5's rule that a destructive action is
suppressed for a cluster with a live run (`'blocked'` counts as live).

The Round-5 build resolved the silence the wrong way. It applied the suppression check at **Phase 1
(detection)** and argued Phase 2 needed none, because a cluster suppressed *this pass* never reaches
ZOMBIE so the sweep's `list_by_status(ZOMBIE)` read won't find it. That reasoning has two defects:

1. **It guards the wrong step.** Phase-1 detection (`InfraRunningObserved` → ZOMBIE) is
   *non-destructive* — it only makes the zombie visible. The Phase-2 `DestroyRequested` is the
   destructive act that suppression exists to gate. The build suppresses the harmless step and
   leaves the harmful one open.
2. **It misses the cross-pass case.** The sweep re-derives its worklist from *durable* ZOMBIE state
   (that is what makes DR-0012 crash-safe). A cluster promoted to ZOMBIE in a **prior** pass, when
   no run existed, that has since acquired a blocking run, is found by this pass's sweep and
   destroyed **unsuppressed** — because nothing re-checks at sweep time.

## Decision

**The Phase-2 sweep applies Phase-C suppression, checked fresh at sweep time.** For each cluster the
sweep finds in ZOMBIE, the reconciler re-reads run status (`repos.workflow_runs.active_for_cluster`,
a short DB-only read) and **skips emitting `DestroyRequested` if a live/`'blocked'` run exists**;
the cluster stays ZOMBIE and is re-swept on a later pass once the run clears. Because the check is at
the destructive step and re-reads durable state every pass, both defects close: the destroy is the
guarded action, and a run that appeared after promotion is honored.

Phase-1 detection (`InfraRunningObserved` → ZOMBIE) is **not** suppressed — ZOMBIE visibility is
desirable even for a cluster with a live run, and detection takes no destructive action. (This
inverts the build's placement: suppression moves from Phase 1 to Phase 2.)

## Consequences

- DR-0012's Phase-2 description gains one sentence: the sweep skips any ZOMBIE cluster with a
  live/`'blocked'` run, checked at sweep time; Phase-1 detection is unsuppressed.
- Pinned tests: a ZOMBIE cluster that acquires a `'blocked'` run between passes is **not**
  re-destroyed by the sweep (stays ZOMBIE); once the run reaches terminal, a later sweep emits
  `DestroyRequested`; detection still promotes a run-bearing cluster to ZOMBIE (visibility
  preserved).
- Consistent with the Orphan path, which already suppresses at its single (destructive) apply.

## Alternatives considered

- **Suppress at detection only (the build's choice)** — rejected: guards the non-destructive step,
  leaves the destructive sweep open, and misses the cross-pass run-appeared case (the defects
  above).
- **Never suppress the sweep — a `DestroyRequested` against a ZOMBIE is always safe** — rejected:
  a `'blocked'`-on-unreachable destroy or an in-flight adopt on the same cluster is exactly the
  race Phase-C suppression exists to prevent; re-destroying underneath it reintroduces the conflict.
