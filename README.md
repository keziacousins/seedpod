# Seedpod

A control plane for **ephemeral Kubernetes environments**. A branch pushes, a cluster
appears, a stack deploys onto it, and a TTL destroys it again — on DigitalOcean droplets or
local VMs (tart, kind, OrbStack), all behind one provider contract.

It was built to give a team real, disposable, full-stack environments per branch, and it ran
that way against real infrastructure: multi-tier stacks on two providers, DNS records created
and reclaimed in both directions, Let's Encrypt certificates, snapshot-and-restore of live
data, and TTL destruction that cleans up after itself.

```bash
uv sync
uv run pytest    # ~2400 tests; tests/acceptance/test_deployment_flow.py is the gate
```

## What's actually interesting here

Not the feature list — the constraints, and what they cost.

- **`seedpod/core/` is pure.** No IO, no `now()`, no locks, no naive datetimes. State
  transitions are tested exhaustively over `(state × event)` with **zero mocks**; if a core
  test needs one, that is treated as a leaking seam, and the seam gets fixed instead.
- **The workflow grammar is frozen.** No `if`, no expressions, no interpolation in workflow
  YAML — ever. A new need becomes a new typed, tested step verb, never a new escape hatch.
  Wanting an escape hatch is the stop signal, not a judgement call.
- **Providers are stateless.** No database access, no retry or poll loops (the engine owns
  retry), kubeconfig always passed in. Every provider passes one shared conformance suite
  with fault injection at the transport seam rather than at `Mock`.
- **One error taxonomy.** `InfrastructureUnreachableError` means "cannot determine state" —
  it never triggers compensation and is never conflated with absence. That distinction is
  the difference between retrying and destroying something real.

## `docs/decisions/` is the point

Forty-six decision records. Most exist because something failed against real infrastructure
and the obvious fix was wrong. They record what broke, what was tried, and what the
second-order consequence turned out to be — including a ratified decision withdrawn because
the code had already solved the problem, a test that pinned a decision while missing its
consequence and so held a dead rule in place, and a recurring defect shape where the system
computes exactly why something failed and then discards the reason before anyone sees it.

`docs/design/` holds the normative specs; `docs/decisions/` records why things changed.
`CLAUDE.md` states the hard rules an agent working in this repo must not break.

## About this repository

Published as a **single commit**. Seedpod was built during a private client engagement; the
commit history is a diary of that work and is not what makes the design worth reading. The
reasoning lives in `docs/decisions/` instead.

Two consequences worth knowing:

- Docs cite `reference-code/…` paths where logic was salvaged from the predecessor system.
  **That tree is not published** — the citations are provenance, not links.
- `config/` here is a small generic example that exercises the same features. The real
  deployment configuration is private.

## Operating a release

Seedpod ships as a dev appliance (DR-0041). The layout says the whole thing: code is
disposable, state is not.

```
~/seedpod/
  releases/<release>/   the artifact — seedpod/ config/ ui/dist/ bin/ uv.lock MANIFEST.json .venv/
  current -> releases/<release>
  var/                  .env  db/  data/  log/  seedpod.pid   ← state; no upgrade writes here
```

**Build** — on a dev machine, from a clean checkout:

```bash
(cd ui && npm ci && npm run build)      # the bundle ships, so the appliance needs no node
uv run python scripts/build_release.py  # → dist/seedpod-<release>.tar.gz
```

**Install and upgrade are the same four commands** — an upgrade just swaps the symlink,
and a rollback swaps it back:

```bash
mkdir -p ~/seedpod/releases ~/seedpod/var
tar -xzf seedpod-<release>.tar.gz -C ~/seedpod/releases/
(cd ~/seedpod/releases/<release> && uv sync --locked --python 3.11)
ln -sfn ~/seedpod/releases/<release> ~/seedpod/current
```

`--python 3.11` is explicit rather than a `.python-version` file: pyenv reads that filename
too, and where `uv` is installed as a pyenv shim, the file makes `uv` itself unrunnable
(`pyenv: uv: command not found`).

**First run only** — put the two keys `generate-keys` prints into `var/.env` first:

```bash
~/seedpod/current/bin/seedpod-bootstrap generate-keys
~/seedpod/current/bin/seedpod-bootstrap migrate
~/seedpod/current/bin/seedpod-bootstrap create-admin <username>
~/seedpod/current/bin/seedpod-bootstrap seed-secrets ephemeral --profile <profile>
```

`seed-secrets` reports what is missing and changes nothing; add `--placeholder` to fill the
gaps. The environment is the secrets environment (`ephemeral`, `development`, …), not the
profile's name.

`create-admin` prints its key once and never again. Put it in `var/.env` as well as wherever
you keep it — `seedpodctl` reads it from there, and without it the first thing you try after
installing fails with "no API key configured":

```
SEEDPOD_API_URL=http://127.0.0.1:8000
SEEDPOD_API_KEY=seedpod_all_…
```

**Run** — foreground, in a session you keep open:

```bash
~/seedpod/current/bin/seedpod            # Ctrl-C stops it
~/seedpod/current/bin/seedpod --version  # which code is this?
```

There is no supervisor, deliberately: the process borrows its shell's macOS Local Network
grant, which is what removes the code-signing question entirely. **Stop it with Ctrl-C** —
not by killing a pattern. `pkill -f "python start.py"` matches nothing, because the real
argv is `.../Python.app/Contents/MacOS/Python start.py`, and a stale server that survives
your restart will answer `/health` and make the restart look successful.

A second start refuses and names the pid holding the lock or the port, exiting 75. If you
think the refusal is wrong, believe the refusal.

**State and logs.** Everything mutable lives under `var/`, and the launcher points the
server at it with absolute paths (the defaults are cwd-relative, and a release root is not
a directory anyone stands in). Override the location with `SEEDPOD_VAR`.

```bash
tail -f ~/seedpod/var/log/seedpod.log
```

JSON lines, rotated daily, 30 days retained. Each boot snapshots the outgoing file to
`seedpod.log.startup-<timestamp>`, keeping the last 10 — so "what did the run before this
one do?" is answerable.

`var/.env` holds secrets and tokens. It is also where `seedpodctl` reads `SEEDPOD_API_URL`
and `SEEDPOD_API_KEY`; the launcher owns the five path variables and overrides `.env` for
those alone.

```bash
~/seedpod/current/bin/seedpodctl clusters list
```
