---
title: DR-0021 — Entry-point & CLI architecture: server runner, offline bootstrap CLI, HTTP user CLI
type: decision
status: active
created: 2026-07-19
updated: 2026-07-19
---

# DR-0021: Three entry points by trust model — serve, bootstrap (offline), ctl (HTTP)

**Status: ACTIVE — ratified by Kezia, 2026-07-19. Direction set by Kezia: "cli for users vs cli
for bootstrapping a new system safely are two different use cases — bootstrap cli does on-disk
setup; usual cli talks to the http api." Confirmed: the server runner is a distinct third surface;
three separate console scripts (not subcommands of one binary).**

## Problem

`seedpod/__main__.py` is a stub (`raise SystemExit("runtime spine not yet built")`); v2 cannot
run. seam-d Decision 8 already specs the **server runner** (`build_app(AppConfig.from_env())` +
`uvicorn.run(app.api)`, plus `start.py` for `load_dotenv`/PID-file/log-rotation) — Round 6 built
`app/` + `api/` but left the entry point unbuilt. Beyond that, the **CLI surface** (v1's
`generate-keys`, `bootstrap <admin>`, `list-keys`, `revoke-key`, `create-secret`, …) is undesigned
for v2.

v1 conflated all of it into one `seedpod` CLI that mixed **direct-DB, privileged** operations
(generate encryption keys, mint the first admin key, apply schema) with everyday operator
operations. Direct-DB writes bypass the state machine, the Dispatcher (`apply()` — the one write
path, CLAUDE.md), and auth entirely. And "mint the first admin key" is a chicken-and-egg: it must
work *before* any credential exists and *before* the server runs, so it cannot be an HTTP endpoint
without being an unauthenticated privileged hole.

## Decision (PROPOSED) — three distinct entry points, one per trust model

**1. Server runner — `python -m seedpod` (`__main__.py`), per seam-d Decision 8.**
`main()`: `setup_logging()`; `app = build_app(AppConfig.from_env())`; run `uvicorn.run(app.api,
…)`. `App.start()/stop()` (migrate, executor, reconciler, health, per DR-0008 ordering) wire into
the ASGI **lifespan** so the runtime starts/stops with the server. `start.py` keeps only
`load_dotenv` + PID-file singleton + log-rotation, then calls `main()`. This surface is already
specified; it just needs building.

**2. Bootstrap CLI — `seedpod-bootstrap` (separate console script). OFFLINE, on-disk only.**
The **only** tool with direct DB / filesystem write access. Runs with no server and no
credentials — *local filesystem access is its trust boundary*. Minimal command set for cold-start:

- `generate-keys` — Fernet dev/prod encryption keys → `.env` (or stdout).
- `migrate` — apply the numbered-migration schema (the `migrate()` runner) to a cold DB.
- `create-admin <username>` — mint the **first** API key directly via the repos + crypto
  (the same hashing `ApiKeyService` uses), INSERT it, print the plaintext once.

It exists solely to break the cold-start chicken-and-egg (no key exists ⇒ the user CLI can't run
yet). It is never exposed over HTTP and never talks to a running server.

**3. User CLI — `seedpodctl` (separate console script). HTTP client, authenticated.**
A thin client over the **same authenticated API the SPA uses** — **no** direct DB/filesystem
access. Bearer auth from `SEEDPOD_API_KEY` (or a config file) against the API base URL. Command
groups mirror the endpoints: `keys`, `secrets`, `clusters`, `deployments` + `deploy`
(version-update), `snapshots`, `presets`, `workflows`, `timers`, `health`, `config`. This is the
everyday operator + CI surface.

## Rationale — the safety property

Confining every direct-DB/privileged write to the tiny, offline bootstrap tool makes the trust
boundary **structural**: the everyday path (user CLI) *physically cannot* bypass the Dispatcher,
the state machine, or auth, because it only speaks HTTP — it inherits every server-side guard for
free, exactly like the SPA. The first-credential mint is local-only and never network-exposed.
Three tools = three trust models: a server process, a local-root cold-start tool, and an
authenticated remote client.

## Consequences

- Build all three. `pyproject.toml` `[project.scripts]`: `seedpod` → the module/serve entry,
  `seedpod-bootstrap` → the offline tool, `seedpodctl` → the HTTP client. (Server runner spec is
  seam-d Decision 8; the two CLIs are new, normatively pinned here.)
- Bootstrap salvages v1's `generate-keys` + first-admin logic (direct DB, via repos + crypto).
- User CLI salvages v1's `list-keys`/`create-secret`/etc. surface but **re-pointed at HTTP** — a
  client, not a direct-DB tool.
- the parity backlog (not published) tracks these as build items (§0).
- This is normative until (if) folded into a design doc; seam-d Decision 8's server-runner block
  stands and is cited by item 1.

## Alternatives considered

- **One unified `seedpod` CLI (v1's shape)** — rejected: mixes direct-DB privileged ops with
  client ops; the direct-DB path bypasses `Dispatcher.apply()`/auth, and a single binary invites
  running privileged bootstrap ops in the wrong context. The split makes the boundary structural,
  not conventional.
- **Bootstrap over HTTP** (an unauthenticated `/bootstrap` endpoint to mint the first key) —
  rejected: a standing unauthenticated-privileged hole; cold-start must be local-only.
- **User CLI with direct DB access** (no server needed) — rejected: bypasses the one-write-path
  invariant; the user CLI must be a pure API client so it inherits every server-side guard.
