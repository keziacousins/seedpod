---
title: DR-0006 — Birth rows through the Dispatcher (`record=` takes the full row DTO)
type: decision
status: active
created: 2026-07-15
updated: 2026-07-15
---

# DR-0006: Birth rows through the Dispatcher (`record=` takes the full row DTO)

**Status: ACTIVE — ratified by Kezia, 2026-07-15. Coherence-review Conflict 3's signature and
§2 glossary amended accordingly; the Round-4 dispatcher rework may proceed.**
(Origin: Round-4 workflow run `wf_3c420b88-f97` halted on this gap via the adversarial judge,
2026-07-15 — the spec-gap protocol, same pattern as DR-0004.)

## Problem

Conflict 3 gives `Dispatcher.apply` a `record: ClusterRecord | DeploymentRecord | None`
parameter so NEW births flow through the only legal write path. But the `clusters` row carries
NOT NULL, semantically load-bearing columns that the pure `ClusterRecord` deliberately does
**not** (the Conflict 11 record/row split): `slug` (human-facing; DNS; the
`ux_clusters_slug_live` uniqueness surface), `provider_config` (provisioning INPUTS — Conflict
10's `cluster.load_spec` builds the `ClusterSpecification` from it), plus `name`,
`environment`, `repository`/`branch`, `node_count`.

Three specs triangulate to an unanswered question: **how does a birth get real values into
row-only columns through `apply()`?** The Round-4 build had to invent an answer and did —
`slug = record.id`, `provider_config = {}` — which means a Dispatcher-born cluster provisions
with empty inputs and leaks a uuid slug to UI/DNS. Its documented escape hatch (pre-insert the
row via `ClusterRepository.insert`, then `apply(record=)` for the birth) deterministically
violates the `clusters.id` PK, because the ratified birth model is
`Persist(expected_version=None)` ⇒ INSERT (Seam A §A) — the birth itself inserts. Caught by
the Round-4 adversarial judge; the workflow halted per the spec-gap protocol.

## Decision

1. **`Dispatcher.apply(..., record=)` accepts the full-row DTOs** — `ClusterRow |
   DeploymentRow` (owner: `seedpod/data/repositories.py`) — for NEW births. Conflict 3's
   signature is amended accordingly. The Dispatcher narrows the row to the pure record (the
   same row→record mapping `ClusterRepository.load` uses) before calling `transition()`; the
   machine still sees only `ClusterRecord`/`DeploymentRecord`, and Pillar 1 is untouched.
2. **The birth INSERT is the caller's row with the machine's fields overlaid.** The birth
   `Persist.record` (state, version, `pre_destroy_state`, …) wins for every machine-owned
   field; the row supplies what the machine doesn't own (`slug`, `provider_config`, `name`,
   billing/crypto columns, …) — never the reverse. Row INSERT + state audit + outbox effects
   stay atomic in the one birth transaction, exactly as for any other transition.
3. **The Dispatcher never synthesizes column values.** Row synthesis — slug minting (salvaged
   `generate_cluster_slug` / `naming_strategy.py`), `provider_config` from rules/presets,
   `node_count` — is the API-layer service's job (Round 6 `ClusterService` /
   `DeploymentService`), which constructs the `ClusterRow` and the birth Command together.
   Until Round 6, tests construct rows by hand (they already do — `tests/data/`).
4. **Uniform for both aggregates**: deployment births pass `DeploymentRow`, even though it
   currently has no row-only NOT NULLs beyond Clock-derived timestamps — one contract, no
   per-aggregate special case.

## Consequences

- `coherence-review.md` Conflict 3's signature line is amended (`record: ClusterRow |
  DeploymentRow | None`) and the §2 glossary gains `ClusterRow`/`DeploymentRow` rows (owner
  `data/repositories.py`), on ratification.
- The Round-4 dispatcher component is reworked: DDL-default synthesis and the pre-insert
  workaround docstring are **deleted**; birth path takes the row, narrows, overlays, INSERTs.
- Round 6's services own row synthesis; the acceptance flow (version-update → rules → birth)
  passes a fully populated `ClusterRow` through this contract.
- Nothing changes for non-birth transitions, the machine, the codec, or the effect model.

## Alternatives considered

- **`ClusterRecord` grows the birth-only columns** (rejected: bloats the pure machine DTO with
  fields `transition()` never reads, churns Pillar 1's exhaustive `(state × event)` suite, and
  re-smuggles the row grab-bag into the machine — the exact disease the Conflict 11 split
  exists to prevent).
- **Pre-insert the row, then birth on the existing row** (rejected: under the ratified
  `Persist(expected_version=None)` ⇒ INSERT model it double-inserts the PK; re-modeling birth
  as CAS-from-NEW instead would split row creation and the birth event across two
  transactions, so a crash between them strands an unborn row with no audit trail — births
  stop being atomic).
- **Keep DDL-default synthesis in the Dispatcher** (rejected: this is the paper-over the judge
  caught — unprovisionable clusters and uuid slugs on DNS/UI surfaces).
- **A separate `Dispatcher.birth()` method** (rejected: two write paths where the whole design
  demands one; `record=` already marks births at the call site).
