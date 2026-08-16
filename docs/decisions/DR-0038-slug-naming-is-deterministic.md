---
title: DR-0038 — v2's deterministic slug stands; the inert naming_strategy surface is withdrawn rather than implemented
type: decision
status: active
created: 2026-08-11
updated: 2026-08-11
---

# DR-0038: cluster slugs are derived, not configured

**Status: ACTIVE — ratified by Kezia, 2026-08-11.** Closes backlog **P1 #5**.

## Context

Backlog #5 read "port v1's slug naming-strategy engine (`core/naming_strategy.py`); presets'
`naming_strategy` field is inert until ported". Checked against source, the framing was backwards:
v2 did not *fail* to port it, it **deliberately replaced** it, and said so —
`app/services/deployment_service.py:57`: *"v1's slug naming-strategy engine
(`core/naming_strategy.py`) is replaced with a minimal, deterministic slugifier (`_slugify`) —
stable names"*. So the item was not "finish the work" but "reverse a documented decision", which
needs a DR either way.

What is genuinely wrong is smaller and different: **`naming_strategy` is still accepted, stored and
returned by the preset surface, and does nothing.** It rides `PresetRow`, `PresetService.create`/
`update`, `POST`/`PUT /api/presets`, and two `seedpodctl` flags. Nothing reads it. A user can set it,
see it echoed back, and get a deterministic slug regardless.

The SPA does **not** render or edit it — `ui-contract.md`'s inventory records v1's SPA reading
`naming_strategy{type,name,pattern}`, but v2's `ui/src` has no reference to it at all. So the
misleading surface is API- and CLI-only.

## Decisions

### 1. The deterministic slug stands

`_slugify(repo, branch, suffix=cluster_id[:8])` remains how every cluster slug is derived. Not
ported, and not by omission.

Two reasons beyond "it was already decided". First, the slug is now **load-bearing beyond naming**:
DR-0034 makes it the DNS record name (`{cluster_slug}.cluster.example.com`). A `fixed`-name
preset — v1's own strategy set includes one — would give two clusters from the same preset the same
hostname, so the second provision would upsert the first's DNS record and point the name at the new
droplet while the old cluster still runs. v1 had no such coupling; v2 does, and it makes configurable
names materially more dangerous than they were.

Second, determinism is what makes a slug diagnosable: given a cluster id you can derive its name, and
given a name you can find the cluster. v1's `random`-suffixed strategies broke that both ways.

### 2. The inert surface is withdrawn, loudly

`naming_strategy` is **rejected** at the API boundary: `POST`/`PUT /api/presets` with a non-null
value returns 400 naming why, rather than storing a field nothing honours. The two `seedpodctl
--naming-strategy` flags are removed, since they could now only produce that error.

Rejecting rather than silently dropping is the same call DR-0037 just made for
`resolution_strategy`, for the same reason: a request that asks for behaviour the system will not
deliver should fail where the user can see it, not succeed and behave differently.

### 3. The column and the serializer stay

`presets.naming_strategy` is not dropped and the GET serializer still returns whatever a row holds.
No migration, and any preset created before this keeps reading back exactly as it did — the field
simply cannot be set or changed to a non-null value any more. Dropping the column would be a
destructive migration on a live database ([[operational-readiness]]: migrations are forward-only)
to remove a nullable field that is now unreachable. Not worth it.

## Consequences

- `seedpod/api/routers/presets.py` — reject non-null `naming_strategy` on create and update.
- `seedpod/ctl/cli.py`, `seedpod/ctl/client.py` — the flags and their plumbing go.
- `docs/design/ui-contract.md` — record that v2 does not carry this field, so the v1 inventory row
  is not mistaken for an obligation.
- No schema change, no SPA change.

**If configurable names are ever genuinely wanted**, this DR is what a future one supersedes — and
it must resolve the DNS collision in decision 1, which is the real design work, not the slug format.
