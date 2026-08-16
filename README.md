# Seedpod

A control plane for **ephemeral Kubernetes environments**. A branch pushes, a cluster
appears, a stack deploys onto it, and a TTL destroys it again. Clusters run on DigitalOcean
droplets or local VMs (tart, kind, OrbStack), behind one provider contract.

It was built to give a team disposable full-stack environments, one per branch, and it ran
that way against real infrastructure: multi-tier stacks on two providers, DNS records created
and reclaimed in both directions, Let's Encrypt certificates, snapshot and restore of live
data, and TTL destruction that cleans up after itself.

```bash
uv sync
uv run pytest    # ~2400 tests; tests/acceptance/test_deployment_flow.py is the gate
```

## The constraints

The constraints are the interesting part, not the feature list.

- **`seedpod/core/` is pure.** No IO, no `now()`, no locks, no naive datetimes. State
  transitions are tested over every `(state × event)` pair, with zero mocks. A core test that
  needs a mock means a seam is leaking, so the seam gets fixed instead.
- **The workflow grammar is frozen.** Workflow YAML has no `if`, no expressions, and no
  interpolation. A new need becomes a new typed, tested step verb. Wanting an escape hatch is
  the stop signal, not a judgement call.
- **Providers are stateless.** No database access. No retry or poll loops — the engine owns
  retry. Kubeconfig is always passed in. Every provider passes one shared conformance suite,
  with faults injected at the transport seam rather than at `Mock`.
- **One error taxonomy.** `InfrastructureUnreachableError` means "cannot determine state". It
  never triggers compensation, and it is never treated as absence. That distinction decides
  whether the system retries or destroys something real.

## The decision records

Forty-six of them, in `docs/decisions/`. Most exist because something failed against real
infrastructure and the obvious fix was wrong. Each one records what broke, what was tried,
and what the second-order consequence turned out to be. They include a ratified decision
withdrawn because the code had already solved the problem; a test that pinned a decision but
missed its consequence, and so held a dead rule in place; and a recurring defect where the
system computes exactly why something failed, then discards the reason before anyone sees it.

`docs/design/` holds the normative specs. `docs/decisions/` records why things changed.
`CLAUDE.md` states the rules an agent working in this repo must not break.

## About this repository

Seedpod is published as a **single commit**. It was built during a private client engagement.
The commit history is a diary of that work, and it is not what makes the design worth
reading. The reasoning is in `docs/decisions/` instead.

Two consequences:

- Docs cite `reference-code/…` paths where logic was salvaged from the predecessor system.
  That tree is not published. The citations are provenance, not links.
- `config/` here is a small generic example that exercises the same features. The real
  deployment configuration is private and lives in its own repo (DR-0041, erratum E3).

## Operating a release

Seedpod ships as a dev appliance (DR-0041). Code is disposable. State is not.

```
~/seedpod/
  releases/<release>/   the artifact — seedpod/ config/ ui/dist/ bin/
                        uv.lock  pyproject.toml  MANIFEST.json  README.md
                        plus .venv/, built in place by uv sync at install time
  current -> releases/<release>
  var/                  .env  db/  data/  log/  seedpod.pid   ← state; no upgrade writes here
```

The artifact carries no virtualenv. A venv is not portable across platforms or interpreter
ABIs, so `uv.lock` ships instead and the appliance materialises the venv itself (erratum E1).
The `config/` it carries is the generic example, not any real deployment's configuration.
Point `SEEDPOD_CONFIG_DIR` at your own config, which lives in its own repo (erratum E3).

**Build** on a dev machine, from a clean checkout:

```bash
(cd ui && npm ci && npm run build)      # the bundle ships, so the appliance needs no node
uv run python scripts/build_release.py  # → dist/seedpod-<release>.tar.gz
```

**Install and upgrade are the same four commands.** An upgrade swaps the symlink. A rollback
swaps it back.

```bash
mkdir -p ~/seedpod/releases ~/seedpod/var
tar -xzf seedpod-<release>.tar.gz -C ~/seedpod/releases/
(cd ~/seedpod/releases/<release> && uv sync --locked --python 3.11)
ln -sfn ~/seedpod/releases/<release> ~/seedpod/current
```

`--python 3.11` is explicit rather than a `.python-version` file. pyenv reads that filename
too, and where `uv` is installed as a pyenv shim, the file makes `uv` itself unrunnable
(`pyenv: uv: command not found`).

**First run only.** Put the two keys `generate-keys` prints into `var/.env` before running
the rest.

```bash
~/seedpod/current/bin/seedpod-bootstrap generate-keys
~/seedpod/current/bin/seedpod-bootstrap migrate
~/seedpod/current/bin/seedpod-bootstrap create-admin <username>
~/seedpod/current/bin/seedpod-bootstrap seed-secrets ephemeral --profile <profile>
```

`seed-secrets` reports what is missing and changes nothing. Add `--placeholder` to fill the
gaps. Its first argument is the secrets environment (`ephemeral`, `development`, …), not the
profile name.

`create-admin` prints its key once and never again. Put it in `var/.env` as well as wherever
you keep it. `seedpodctl` reads it from there, and without it the first command you run after
installing fails with "no API key configured":

```
SEEDPOD_API_URL=http://127.0.0.1:8000
SEEDPOD_API_KEY=seedpod_all_…
```

**Run** it in the foreground, in a session you keep open:

```bash
~/seedpod/current/bin/seedpod            # Ctrl-C stops it
~/seedpod/current/bin/seedpod --version  # which code is this?
```

There is no supervisor, deliberately. The process borrows its shell's macOS Local Network
grant, which removes the code-signing question.

Stop it with **Ctrl-C**, not by killing a pattern. `pkill -f "python start.py"` matches
nothing, because the real argv is `.../Python.app/Contents/MacOS/Python start.py`. A stale
server that survives your restart still answers `/health`, and makes the restart look
successful.

A second start refuses, names the pid holding the lock or the port, and exits 75. If you
think the refusal is wrong, believe the refusal.

**State and logs.** Everything mutable lives under `var/`. The launcher points the server at
it with absolute paths, because the defaults are relative to the working directory and a
release root is not a directory anyone stands in. `SEEDPOD_VAR` overrides the location.

```bash
tail -f ~/seedpod/var/log/seedpod.log
```

Logs are JSON lines, rotated daily, kept for 30 days. Each boot snapshots the outgoing file
to `seedpod.log.startup-<timestamp>` and keeps the last 10, so what the previous run did is
still readable.

`var/.env` holds secrets and tokens. `seedpodctl` reads `SEEDPOD_API_URL` and
`SEEDPOD_API_KEY` from it. The launcher owns four path variables — the log directory, pid
file, snapshot path and database URL — and overrides `.env` for those alone.

**`SEEDPOD_CONFIG_DIR` is yours.** Set it in `var/.env` to an absolute path and it wins;
leave it unset and the release's own `config/` is used. That is how an appliance runs a real
deployment's configuration, which lives in its own repo rather than in the artifact (DR-0041,
erratum E3). A relative value is ignored, and a path that does not exist is refused rather
than quietly replaced with the example config.

```bash
~/seedpod/current/bin/seedpodctl clusters list
```

## How this was built

The code and the documentation here were written by Claude models, working in Claude Code.

A human directed that work and held design authority over it. The architecture, the eight
pinned interface decisions in `docs/DESIGN.md`, the rules in `CLAUDE.md`, and every
ratification in `docs/decisions/` are human calls, as is the review of what the models
produced. The decision records name who decided, and when. Several exist because a human
decision turned out to be wrong, and real infrastructure disagreed with it first.

This is stated rather than left out. Leaving it out would claim human authorship of prose the
models wrote.
