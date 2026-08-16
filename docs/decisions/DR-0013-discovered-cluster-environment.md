---
title: DR-0013 — Environment for a discovered (CreateUnmanaged) cluster birth
type: decision
status: active
created: 2026-07-16
updated: 2026-07-17
---

# DR-0013: Environment for a discovered (CreateUnmanaged) cluster birth

**Status: ACTIVE — ratified by Kezia, 2026-07-17: `"production"` default accepted, flagged for
verification against real fleet behavior once discovery is exercised in testing. Gap 1 of the
first Round-5 halt (reconciliation judge, run `wf_2d729255-fa5`, 2026-07-16).**

## Problem

A `CreateUnmanagedIntent` is raised for a droplet/VM that carries Seedpod's UUID tag but has **no
DB record** — infrastructure that appeared outside Seedpod's control. The reconciler births it as
an UNMANAGED cluster via `Dispatcher.apply(record=)` (DR-0006). That birth row must set
`environment`, and there is no environment signal to read from: the intent carries only the
provider instance and a slug.

v1 rode a `'discovered'` **environment sentinel** for exactly this. Seam D Decision 6 deliberately
**retired** that sentinel — `'discovered'`/`'unmanaged'` were cluster *kind* smuggled into
`environment`; they moved to a real `kind` column, and `environment` is now pinned "real env only:
`local`/`development`/`ephemeral`/`staging`/`production`" (`seedpod/data/migrations/
0001_initial.sql:20,72`). Crucially `clusters.environment` is **`TEXT NOT NULL`** — so "unscoped /
NULL" is **not** available; the row must carry one of the five real envs.

This is consequential, not cosmetic. `environment` is an **authorization-visibility control**:

- It scopes SSE delivery — DR-0010's filter drops a discovered cluster's `cluster_state_changed`
  events for any connection whose key scope is a *different* real env (only the matching env and
  `'all'`-scoped keys ever see them).
- It scopes REST GET — an env-scoped key cannot fetch resources outside its env (DR-0010's stated
  no-asymmetry rationale).

So the value chosen sets *which operator population* is responsible for seeing and triaging foreign
infrastructure — a policy call the same shape as DR-0006/0010/0012, not a build-agent default. The
build unilaterally chose `"production"` and self-classified it "not a spec gap"; the judge
correctly escalated it.

## Decision

**`"production"`** (ratified). **Flagged for verification**: revisit whether this default fits real
fleet behavior — in particular a discovered *local* kind/tart/orbstack VM labelled `production` (the
known category-error wart) — once cluster discovery is actually exercised under test. A genuinely-foreign cluster is treated as sensitive-until-triaged:
its lifecycle events and record surface only to production-scoped and `'all'`-scoped keys (the
most-privileged operators), and a narrowly-scoped non-prod key can neither watch nor GET
infrastructure of unknown provenance. Admins (`'all'`) see it regardless. This is the
conservative-authorization reading of "unknown infra that appeared in the fleet."

The reconciler sets `environment = "production"` on the CreateUnmanaged birth row; everything else
in the row is the observed field set salvaged from v1 `_create_unmanaged_cluster` (`origin`/`kind`
= discovered, slug, provider, droplet_id/region/size).

## Alternatives considered

- **`"development"` (the system default — Seam D config `environment: str = "development"`,
  seam-d:416).** Principle: "unknown ⇒ least-privileged default." Cost: production-scoped operators
  do **not** see foreign infrastructure — arguably the wrong direction for a security-visibility
  default (the people who most need to notice an unexplained droplet are the least likely to be
  dev-scoped).
- **Provider-derived env** (local providers kind/tart/orbstack ⇒ `development`; cloud DO ⇒
  `production`). Fixes the category error where a discovered *local* VM gets labelled `production`
  (and vice-versa). Cost: adds a provider→env policy map the reconciler must own; discovered
  clusters are transient (auto-destroyed by the same sweep, or adopted), so the mislabel window is
  short — likely not worth the map.
- **A config setting `discovered_cluster_environment` (default `production`).** Lets operators tune
  it to their fleet. Cost: another config knob for a rare, transient case; defer unless a real
  deployment needs it.
- **Nullable `environment` / unscoped** — **not available**: `clusters.environment` is `NOT NULL`
  and Seam D pins it to real envs. Recorded here so it is not re-proposed.

## Consequences

- On ratification, DR-0006's CreateUnmanaged birth path pins `environment` to the chosen value; the
  reconciliation component brief cites this DR; a test asserts the discovered birth row carries it
  and that DR-0010 scoping follows (a non-matching-env scoped connection does not receive the
  discovered cluster's `cluster_state_changed`).
- If `"development"` or provider-derived is chosen instead, only the birth-row value and its test
  change — the mechanism is identical.
