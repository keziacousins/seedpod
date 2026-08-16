# Seedpod v2

Clean-room rebuild of Seedpod, Exampleco's K3s control-plane orchestrator. **Skeleton is greenfield; logic is salvaged** from `reference-code/` (the read-only v1 tree). The one failure mode that matters here: silently regressing edge behavior v1 already got right.

## Authority chain — read in this order

1. `docs/PLAN-refactor.md` — the constitution: why v2, the three pillars, salvage-vs-rebuild.
2. `docs/DESIGN.md` — the design lock: 8 pinned interface decisions, open taste calls, build plan.
3. `docs/design/` — normative specs. Precedence: `coherence-review.md` **overrides** the seam specs wherever they conflict; seam specs override the DESIGN.md summaries.
4. `docs/decisions/` — DR-NNNN records of why things changed. Changing anything locked in DESIGN.md requires a DR (see DR-0001).

## Hard rules

- **`reference-code/` is not in this repository, and does not need to be.** It was the v1 tree — Seedpod's initial version, and the parity reference v2 was rebuilt against. The docs cite `reference-code/seedpod/...` paths throughout, including in the rules below; **those citations are provenance for where logic was salvaged from, not links to a tree you can open.** Nothing here needs the v1 code to build, test, or run, so a dangling citation is expected and is not a defect to fix. If a copy is ever placed in this directory it stays gitignored and is never committed: it carries `.env`, `admin-api-key.txt`, `db/`, `logs/`, and an embedded `.git` with dirty history.
- **The workflow grammar is frozen**: no `if`/`when`/expressions/interpolation in workflow YAML, ever. A new need becomes a new step verb (a typed, tested `Step` — reviewed, with a DR), never grammar. Wanting an escape hatch is the stop signal, not a judgment call.
- **`seedpod/core/` is pure**: no IO, no `now()` (inject `Clock`), no locks, naive datetimes banned. If a core test needs `Mock`/`patch`, the seam has leaked — fix the seam, not the test. The rule is **no IO**, not "no third-party imports" — and a library can have both a pure and an impure surface. Worked example: `jinja2` is permitted in `core/` for in-memory `Template(string).render()` only (`core/environment_config.py`, salvaging v1's env-var substitution); its **loaders are IO** and belong in `services/` (`services/manifests.py`'s `Environment(loader=FileSystemLoader(...))`). Importing a library into `core/` licenses its pure surface, never its whole API.
- **Providers are stateless**: no DB access, no retry/poll/sleep loops (the engine's `Schedule` owns retry), kubeconfig always passed in. Every provider must pass the shared conformance suite.
- **One error-taxonomy home**: `seedpod/core/errors.py`. `InfrastructureUnreachableError` means "cannot determine state" — it never triggers compensation and is never conflated with absence.
- **State changes go through `Dispatcher.apply()` only.** No direct ORM/status writes anywhere; repositories never commit.
- Don't pin v1 bugs: before porting a v1 behavior verbatim, check the seam specs' LOUD-callout / not-ported lists and `reference-code/seedpod/review/SUMMARY.md`.

## Documentation

Root holds only `CLAUDE.md` + `README.md`; all other docs live under `docs/` with standard frontmatter (`title / type / status / created / updated`, optional `supersedes / superseded-by / amended-by`). `docs/design/` is normative "what is" and is edited in place; `docs/decisions/` is append-only "why it changed". Full conventions: `docs/decisions/DR-0001-documentation-conventions.md`.

## Testing posture

- `tests/acceptance/test_deployment_flow.py` (ported from v1) is the parity gate — green there = cutover-ready.
- Core: exhaustive `(state × event)` totality tests, zero mocks. Engine: fake verbs, crash/cancel matrices at each persistence point. Providers: conformance suite C-01…C-24 with fault injection at the transport seam — never `Mock`/`patch`.
