---
title: DR-0018 — Restore the production-cluster destroy force-guard at the service edge
type: decision
status: active
created: 2026-07-17
updated: 2026-07-17
---

# DR-0018: Production-cluster destroy requires `force=True` (restored at ClusterService)

**Status: ACTIVE — ratified by Kezia, 2026-07-17. Round-6 halt (api-clusters judge, run
`wf_3c2e5583-540`, 2026-07-17). A silent regression of a v1 safety edge — the charter's named
failure mode; caught by the adversarial judge.**

## Problem

v1's `DELETE /clusters/{id}` refused to destroy a **production** cluster without `force=true`:

```python
# reference-code/seedpod/seedpod/api/clusters.py:472-477
if cluster.environment == "production" and not force:
    raise HTTPException(400, "Production cluster destruction requires force=true parameter")
```

v2 ported only the state machine's **discovered-origin** guard (`core/machine.py`:
`origin == DISCOVERED and not force`). A managed cluster with `environment == "production"`
(origin MANAGED) now destroys with **no force required** — the production safety edge is gone.

This was missed, not retired:

- seam-a-core.md's force-retirement table (line 414, "API destroy force passthrough … the guard
  is a pure field check") enumerates the surviving destroy guards but never mentions the
  production-environment gate; coherence-review.md (the override authority) is silent on it.
- DR-0013's environment/origin split kept `environment` as a real column (`production` is a
  valid value), and the force retirement (§F) explicitly preserved the API destroy `force`
  param. Neither removes the ability — or the reason — to gate production destroys.
- **The smoking gun:** `ClusterService.extend` *did* faithfully port v1's production guard
  (`cluster_service.py:145` — a production cluster cannot be extended), so production clusters
  are protected from casual TTL-extend but not from casual destroy. That asymmetry is an
  oversight, not a design.

## Decision (PROPOSED)

**Restore the guard at `ClusterService.destroy`, the service edge** — mirroring where v1
enforced it (the API handler) and where `ClusterService.extend` already guards:

- A managed cluster with `environment == "production"` requires `force=True` to destroy; without
  it, raise `PermanentError(code=INVALID_INPUT, command="destroy", …)` → HTTP 400, matching the
  extend guard's shape and v1's message intent.
- `force=True` **overrides** it and the destroy proceeds (v1 semantics — unlike `extend`, which
  is an absolute block with no force escape; the two methods differ exactly as v1 made them).
- This guard is **distinct from and additional to** the machine's discovered-origin guard, which
  stays untouched. They compose: a production cluster needs force; a discovered cluster needs
  force; a production-and-discovered cluster needs force.
- It lives at the **service edge, not `core/machine.py`** — `core` is frozen, the machine guard
  is a provenance invariant (discovered infra), and production-environment is an edge *policy*
  v1 enforced at the API layer, not a state-machine invariant. `ClusterService.destroy` threads
  `force` already; add the environment check before it emits `DestroyRequested`.

## Consequences

- `ClusterService.destroy` gains the `environment == "production" and not force → raise` check;
  a test pins production-requires-force (400 without force, proceeds with) beside the existing
  discovered-origin test.
- seam-a-core.md §F's force-retirement table gains a row: the production-destroy guard survives at
  the service edge (cite DR-0018) — closing the enumeration gap that caused this.
- ui-contract's `DELETE /api/clusters/{id}` row already documents `?force`; the production
  semantics are now explicit rather than implied.

## Alternatives considered

- **Leave it dropped** (production destroys without force) — rejected: a silent regression of a
  real v1 safety edge; the extend asymmetry proves it was unintentional. Accidentally destroying
  production is the catastrophe the guard exists to prevent.
- **Put the guard in `core/machine.py`** alongside the discovered check — rejected: `core` is
  frozen; the machine guard is about provenance, not environment policy; v1 enforced production
  at the API layer; `environment` is not a state-machine invariant.
- **Require force for ALL managed destroys** — rejected: over-broad; changes ephemeral/staging
  destroy ergonomics v1 never guarded, for no safety gain (those are disposable).
