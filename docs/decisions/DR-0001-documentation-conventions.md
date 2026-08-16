---
title: DR-0001 — Documentation conventions
type: decision
status: active
created: 2026-07-12
updated: 2026-07-12
---

# DR-0001 — Documentation conventions

## Context

The v2 build is multi-session and agent-heavy: work fans out to subagents that read docs cold, and design authority is spread across a plan, a design lock, four seam specs, and a coherence review with an explicit precedence chain. Without a settled scheme, docs drift (v1 accumulated eight PLAN-*/TODO-* files at root with no status or ordering) and agents can't tell what is binding from what is historical.

## Decision

**Layout.** Root holds only `CLAUDE.md` and (eventually) `README.md`. Everything else lives under `docs/`:

```
docs/
  PLAN-refactor.md      # the constitution (why v2, pillars, salvage-vs-rebuild)
  DESIGN.md             # the design lock (index of the 8 pinned decisions)
  design/               # normative specs — "what IS" (seam specs, coherence review)
  decisions/            # DR-NNNN decision records — "why it changed" (append-only)
  guides/               # how-to / onboarding, added as needed
```

**Frontmatter.** Every doc under `docs/` opens with:

```yaml
---
title: <short title>
type: plan | design | decision | guide | reference
status: proposal | active | superseded
created: YYYY-MM-DD
updated: YYYY-MM-DD
# optional:
supersedes: <doc or DR>
superseded-by: <doc or DR>
amended-by: <doc>          # a doc that overrides this one where they conflict
---
```

**Decision records.** Numbered `DR-NNNN-slug.md`, format: Context / Decision / Consequences, one page max. A DR is required for: changing any decision locked in `DESIGN.md` (including blessing or flipping its taste calls), growing the workflow grammar (should never happen — the DR is the speed bump), adding a step verb or provider, and any deliberate divergence from salvaged v1 behavior not already a LOUD callout in the seam specs. DRs are append-only: once `active`, never edited except to set `superseded-by`.

**Normative docs are edited in place.** When a DR changes a design doc, apply the change to the doc itself (bump `updated`, cite the DR inline where the change lands). Readers of `docs/design/` must never need to replay a DR log to know current truth — DRs record *why*, design docs record *what is*. Exception: the four seam specs and coherence review are generated artifacts of the 2026-07-12 design-lock workflow; they keep their `amended-by` pointers rather than being rewritten, and post-lock changes to their content land as DRs plus edits to `DESIGN.md`.

**Authority chain** (also in `CLAUDE.md`): `docs/design/coherence-review.md` > seam spec > `DESIGN.md` summary > this-or-any conversation. If two docs disagree and precedence doesn't resolve it, that's a bug — file a DR resolving it.

## Consequences

- Agents get a deterministic reading order and a machine-checkable status field; stale docs are marked, not deleted.
- Design changes acquire a paper trail at exactly the granularity the plan cares about (grammar freezes, salvage fidelity, locked interfaces).
- Cost: a little ceremony per change. Accepted — the failure mode it prevents (silently drifting from the design lock mid-fan-out) is the project's stated existential risk.
