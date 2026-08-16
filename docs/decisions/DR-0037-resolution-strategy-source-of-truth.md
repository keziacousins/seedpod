---
title: DR-0037 — the profile owns fallback_branches, and a resolution_strategy v2 cannot honour is a loud config error
type: decision
status: active
created: 2026-08-11
updated: 2026-08-11
---

# DR-0037: which file owns `fallback_branches`

**Status: ACTIVE — ratified by Kezia, 2026-08-11.** Closes backlog **P1 #7**, whose own description
("the resolver never loads it; the endpoint has no real source") was half stale and half the wrong
problem.

## Context

Backlog #7 asked for `resolution-strategies.yml` to be loaded. Checked against source, the endpoint
half **already works**: `api/routers/config.py:83`'s `_load_strategies` reads the real file and
`GET /api/config/resolution-strategies` serves it. What is actually true is stranger, and it is a
divergence nobody recorded:

**v1 and v2 are exactly inverted about where fallback branches come from.**

| | v1 | v2 |
|---|---|---|
| resolution reads | `strategy.fallback_branches` from `resolution-strategies.yml` (`manifest_resolver.py:659-660`) | `profile.fallback_branches` from the profile YAML (`services/manifests.py:698`) |
| the other field | the profile's own `fallback_branches` is parsed and **only ever echoed in an API response** (`manifest_resolver.py:1037`) — dead config that looks live | `resolution_strategy` is recorded in the audit and served by the API, and **never consulted** |

For `exampleco-staging-stack` — which declares `resolution_strategy: branch_discovery_with_fallback` and
`fallback_branches: ["staging", "dev"]`, while that strategy declares `["dev", "main"]` — the two
systems resolve **different images** when a service has no build for the triggering branch: v1 falls
back `dev → main`, v2 falls back `staging → dev`.

Two consequences follow from v2 ignoring the strategy name entirely:

- A typo'd `resolution_strategy` is silently accepted. v1 raised `ValueError: Unknown resolution
  strategy` (`manifest_resolver.py:479-480`).
- `GET /api/config/resolution-strategies` advertises four strategies (`branch_discovery_with_fallback`,
  `latest_only`, `strict_branch`, `dev_priority`) and the resolver honours **none** of them. A profile
  set to `strict_branch` — which promises "no fallbacks, fail if not found" — silently gets full
  fallback behaviour. **This is the same shape as backlog #24**: a surface advertising a capability
  the engine does not implement.

`require_triggering_repo` is defined in v1 (`manifest_resolver.py:176`) and **never read anywhere**.
Porting it would pin a v1 non-feature; it is deliberately not carried.

## Decisions

### 1. The profile's `fallback_branches` stays authoritative

v2's behaviour is kept, and the divergence from v1 is now recorded rather than accidental. The
profile author wrote `["staging", "dev"]` on a profile whose branch is `staging`, and meant it; v1's
version of that field was dead config that read as live, which is a trap rather than a feature.

This is a **deliberate divergence from v1** under DR-0001, not a salvage. Anyone reading the two
codebases side by side will find v1 falling back to `dev → main` for the same file, and that is
expected, not a bug.

### 2. A `resolution_strategy` v2 cannot honour is a loud error at profile load

`load_deployment_profile` — the single choke point for every profile read — raises `PermanentError`
for any `resolution_strategy` other than `branch_discovery_with_fallback`.

This is the honest closure of the "advertises what it does not implement" gap: rather than silently
degrading `strict_branch` to full-fallback behaviour, a profile that asks for it now fails to load,
naming the supported set. It restores v1's raise (`Unknown resolution strategy`) while narrowing it —
v1 rejected names absent from the YAML, v2 rejects names it cannot *honour*, which is the stronger
and more useful check.

Every shipped profile declares `branch_discovery_with_fallback` or nothing (the default), so no
shipped profile is affected.

### 3. The API keeps serving the full strategy list, marked

`GET /api/config/resolution-strategies` continues to read the YAML and serve all four — the file is
real documentation of intent, and the SPA surfaces the descriptions. Each entry gains
`supported: bool`, true only for `branch_discovery_with_fallback`, so the API tells the truth about
what the engine will actually do rather than leaving the caller to discover it at deploy time.

Deleting the unsupported entries from the YAML was considered and rejected: they document a real
design intent, and a profile naming one now fails loudly at load (decision 2), so they cannot be
silently mis-honoured. Making them *work* is a larger feature nobody has asked for — if it is ever
wanted, this DR is what it supersedes.

## Consequences

- `seedpod/app/services/profiles.py` — the validation, at the one load choke point.
- `seedpod/api/routers/config.py` — `supported` on each strategy entry.
- Tests: the raise, the supported flag, and a pin that every **shipped** profile loads.

**No behaviour changes for any shipped profile**, which is why this ships without a smoke: the
resolution path is unchanged for every file in `config/deployment-profiles/`, and the new failure
mode is unreachable for all of them.
