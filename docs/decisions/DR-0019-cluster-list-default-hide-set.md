---
title: DR-0019 — GET /api/clusters default hide-set is {destroyed, zombie, unmanaged}
type: decision
status: active
created: 2026-07-18
updated: 2026-07-18
---

# DR-0019: `GET /api/clusters` default hide-set — v1 parity, distinct from TERMINAL_STATES

**Status: ACTIVE — ratified by Kezia, 2026-07-18. Round-6 halt (api-clusters judge, run
`wf_3c2e5583-540`, 2026-07-18).**

## Problem

v1's cluster list hid terminal/anomalous clusters by default, revealed by `?show_destroyed=true`
(`reference-code/seedpod/seedpod/api/clusters.py:124,135,167` — *"By default, excludes
destroyed/zombie/unmanaged clusters unless show_destroyed=true"*). So v1's default hide-set is
**{destroyed, zombie, unmanaged}**.

The api-clusters build instead filtered on `core/records.py`'s
`TERMINAL_STATES = ("destroyed", "failed")` — but that constant is the **slug-release** set
(Conflict 11: the states that let a cluster's slug be reused), a wholly different concept from the
UI list hide-set. Conflating them produced two regressions at once: it **hides FAILED** (v1 showed
failed clusters — operators need failures visible for attention/rehabilitation) and **reveals
zombie + unmanaged** (v1 hid them). ui-contract's row 32 "Required change" only says "status gets
new value set (no creating/deploying)" and never redefines the hide-set, so it was unpinned.

## Decision (PROPOSED)

**`GET /api/clusters`' default hide-set is `{DESTROYED, ZOMBIE, UNMANAGED}`, ported verbatim from
v1; `?show_destroyed=true` reveals them.** Every other state —
`NEW / PROVISIONING / ACTIVE / DESTROY_SCHEDULED / DESTROYING / DESTROY_FAILED / FAILED` — shows by
default (v1 showed all of these; failed and destroy-failed especially need to stay visible for
attention and rehabilitation).

- This is a UI-list concern; define it as its own explicit constant (e.g. a
  `_LIST_HIDDEN_STATES`) in the service/router. It is **NOT** `core.records.TERMINAL_STATES`
  (the slug-release set `{destroyed, failed}`) — the two must not be conflated, and neither may
  be defined in terms of the other.
- `?show_destroyed` keeps v1's exact name and semantics (ui-contract obligation 6: the param
  survives at parity).
- Hiding ZOMBIE by default does not conflict with DR-0012: that DR made ZOMBIE a real, audited
  *state*; default *list visibility* is a separate concern, and a zombie is transient
  (auto-re-destroyed by the DR-0012 sweep) — v1 correctly kept it out of the default view and
  behind `show_destroyed`.

## Consequences

- `ClusterService.list` / the clusters router applies `{DESTROYED, ZOMBIE, UNMANAGED}` when
  `show_destroyed` is false (default) and no state filter when true; the build's
  `TERMINAL_STATES`-based filter is corrected.
- Test: the default list excludes destroyed/zombie/unmanaged and INCLUDES failed/destroy-failed/
  active; `?show_destroyed=true` includes all. A comment ties the constant to this DR and warns
  against reusing `TERMINAL_STATES`.
- No ui-contract shape change (the fields are unchanged); the hide-set semantics are now explicit.

## Alternatives considered

- **The build's `{destroyed, failed}`** — rejected: hides FAILED (regression — failures must stay
  visible; rehabilitation and retry are live features) and exposes zombie/unmanaged (changes v1).
  It is the slug-release set, not the UI hide-set.
- **Also hide `destroy-failed` / `destroy-scheduled` / `destroying`** — rejected: v1 showed all of
  these; they are in-progress or attention-worthy, not archive. Over-hiding loses operational
  visibility.
- **Show everything by default (no hide-set)** — rejected: floods the default view with terminal
  and anomalous entries; v1 deliberately kept them behind `show_destroyed`.
