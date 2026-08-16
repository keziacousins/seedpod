---
title: DR-0041 — packaging seedpod as a dev appliance: a versioned release root, uv-managed runtime, and seedpod serves its own UI
type: decision
status: active
created: 2026-08-14
updated: 2026-08-15
---

# DR-0041: packaging the dev appliance

**Status: ACTIVE — ratified by Kezia, 2026-08-14**, with amendments A–D below, raised by her
in the same conversation. Target: minimax as a dev appliance; seedpod serves the SPA
same-origin; **no supervisor — the operator runs it and owns the lifecycle**.

## Context

Getting v2 onto minimax by hand, twice in three days, is what this DR is drawn from. The
install is currently six undocumented steps — venv, `bootstrap migrate`, `create-admin`, seed
~20 secrets, start the server in a session, `npm install` + vite in a second session — and each
one failed in an instructive way at least once.

**What already works and must not be broken.** Rsyncing the tree while excluding `.env`, `db/`,
`/data/`, `logs/` and `admin-api-key.txt` moved code repeatedly with zero state loss.
`SEEDPOD_CONFIG_DIR` and `core/paths.py` did exactly their job: `config/` is an editable
on-disk tree, located by env var, and nothing needed touching. **The code/config/state split is
evidenced, not theorised** — it is the shape this DR formalises rather than replaces.

**What is actually broken.**

1. **Nothing owns the process.** The 2026-08-13 server orphaned itself when its SSH session
   dropped and kept serving 18-hour-old code. Worse, the restart script's own health check went
   green *against the survivor*: `pkill -f "python start.py"` matched nothing, because the real
   argv is `.../Python.app/Contents/MacOS/Python start.py` — capital P. "Restart the server
   after a code change" (a handoff rule since smoke 12) is unenforceable by convention alone.
2. **The runtime is whatever the host happens to have.** minimax's venv is a hand-rolled one on
   Homebrew `python@3.11` — the exact binary `docs/guides/tart-local-dev.md` records as being
   in the *denied* state for Local Network. Meanwhile **the project already uses `uv`**:
   `uv.lock` is committed and CI runs `uv sync --locked`. minimax simply never got the memo.
3. **The SPA is a second origin and a second toolchain.** It needs `npm install` and a vite
   process on a second port, reaching the API cross-origin via `VITE_API_URL` with
   `allow_origins=["*"]`. `api/factory.py:29` declined a static mount because no built bundle
   existed; one exists now, from `npm run build`.
4. **Secret seeding is tribal knowledge.** ~20 keys, derived by grepping manifest templates, one
   of which must equal a literal (`s3_access_key` = `dev-minio-access-key`).

## Decision

**1. `uv` is the runtime, and it brings its own Python.** Install and upgrade are
`uv sync --locked` against the committed `uv.lock` — the same command CI runs, so the appliance
and CI resolve identically. `uv python install` supplies the interpreter, which removes the
Homebrew dependency entirely and makes the 3.14 move (agreed in principle on 2026-08-12) a
one-line `requires-python` bump rather than a host migration.

*Rejected: a PyInstaller single binary.* It was the right answer when supervision implied
launchd, because a reparented process is judged on its own binary for Local Network and a
stable signed path earns one durable grant. **Decision 4 removes that pressure**: a
session-bound process borrows the session's grant, so interpreter identity stops mattering and
the signing question disappears with it.

*Rejected: vendored wheels.* `uv.lock` already is the reproducibility mechanism.

**2. A versioned release root, with state outside it.**

```
~/seedpod/
  releases/<version>/     code + config/ + ui/dist/ + .venv
  current -> releases/<version>
  var/                    .env, db/, data/, logs/, admin-api-key.txt
```

Upgrade is: unpack a new release, `uv sync --locked`, swap the `current` symlink. Rollback is
swapping it back. **State lives in `var/` and no upgrade ever writes there** — the same
boundary the rsync exclude-list has been enforcing by hand. `SEEDPOD_CONFIG_DIR`,
`SEEDPOD_DATABASE_URL`, `SEEDPOD_SNAPSHOT_STORAGE_PATH` and `SEEDPOD_LOG_DIR` are pointed at
absolute paths under `var/` (note `AppConfig.config_dir` defaults to the cwd-relative
`Path("config")`, so the launcher must export absolutes).

**3. seedpod serves its own SPA, same-origin.**

- `ui/dist` ships **in the artifact**, built by `npm run build`. It is `.gitignore`d and must
  stay so; CI already runs that build with node 24, so the release job reuses it and **node is
  not needed on the appliance at all**.
- Located by `SEEDPOD_UI_DIR`, a sibling of `config/` — the same "editable on-disk tree found
  by env var" pattern `config_dir` already establishes.
- Mounted in **`seedpod/__main__.py`**, never `create_api`. That module's docstring already
  records the precedent and the reason: tests drive `app.api` over `httpx.ASGITransport`, which
  emits no lifespan and needs no bundle, so production-only wiring belongs at the entry point.
  With `SEEDPOD_UI_DIR` unset, nothing mounts and the vite workflow is untouched.
- **An SPA fallback is required, and is the easy thing to miss.** The UI uses `preact-router`
  with real paths (`/clusters`, `/deployments`, …), so a deep link or a refresh must return
  `index.html`. `/api/*` and `/health` take precedence; only unmatched GETs fall back.
- Consequence: no CORS, no second port, no `VITE_API_URL`. `SEEDPOD_CORS_ORIGINS` stops being
  load-bearing on the appliance.

**4. No supervisor. The operator starts it, in a session, deliberately.**

Kezia's call, 2026-08-14, and it is the load-bearing simplification in this DR. A
session-bound process inherits its shell's Local Network grant, which is the workaround
`docs/guides/tart-local-dev.md` documents as *borrowing* a grant rather than holding one — and
that is precisely why launchd would have forced the signing question.

What the launcher (`bin/seedpod`) must therefore do, since nothing else will:

- **Refuse to start if the port is already held**, naming the pid. The 2026-08-14 incident is
  the specification: a stale server that answers `/health` is worse than no server, because it
  makes a restart *look* successful.
- Match existing processes **case-insensitively** on `start.py`.
- Print the resolved release version and git sha at startup, so "which code is this?" is
  answerable from the terminal that started it.
- Run in the foreground. No `nohup`, no `&`. **("no pidfile games" stood here and is
  superseded by Amendment B — the singleton is wanted, it just has to be a real lock.)**

**5. `seedpod-bootstrap seed-secrets <environment> --profile <name>`.** Derives the required
keys by scanning the profile's manifest templates for `{{ secrets.* }}`, reports which are
missing, and (with `--placeholder`) fills them. Cold-start for a dev stack stops being an
oral tradition. It must know that `s3_access_key` is not free-form — it has to equal the
profile's own `MINIO_ROOT_USER` literal — and that Keycloak's realm policy rejects a
placeholder with no uppercase and no special character (both learned the expensive way on
2026-08-12/13).

## Consequences

- The appliance needs `uv` and nothing else — no Homebrew Python, no nvm, no npm at runtime.
- `python@3.11`'s denied Local Network grant stops mattering, since the interpreter becomes
  uv-managed and the grant is borrowed from the operator's session either way.
- A release job must produce the artifact (source + `uv.lock` + built `ui/dist`). CI already
  does every step of that; it does not yet assemble or publish one.
- **Not addressed here:** multi-host distribution, code signing, unattended restart, and
  anything that requires the process to outlive a terminal. All three follow from decision 4
  and should be reopened only if that changes.

## What would pin it

- A test that `SEEDPOD_UI_DIR` unset mounts nothing (the vite workflow and every existing test
  keep working), and that when set, `/` and a deep link like `/clusters` both return
  `index.html` while `/api/*` and `/health` are untouched.
- A launcher test that a second `bin/seedpod` against a held port exits non-zero and names the
  holding pid rather than reporting health.
- `seed-secrets --dry-run` against `exampleco-dev-stack-nodns` listing exactly the ~20 keys the
  2026-08-12 run needed.

---

## Amendments A–D (Kezia, 2026-08-14)

### A. The artifact carries no source tree

Only what running requires: the resolved `.venv`, `config/`, `ui/dist/`, `bin/`, and an ops
README. **Not** `tests/`, `docs/`, `ui/src/`, `ui/node_modules/`, `.git/`, and never
`reference-code/`. Copying a source checkout to the appliance stops being the mechanism —
that is what this whole DR replaces.

**This breaks one line of decision 4.** "Print the resolved release version and git sha at
startup" assumed a git tree to ask. With no `.git`, the build must **stamp** a manifest
(version, git sha, build timestamp, `uv.lock` hash) into the artifact, and the launcher reads
that. A `git log` in the launcher would simply be empty on the appliance — the failure mode
being avoided is a launcher that can't answer "which code is this?", which is exactly the
question Wednesday's stale server made unanswerable.

### B. A real singleton lock, in the packaged entry point

**The finding that motivates this, and it is a packaging finding, not a bug report.**
`start.py` — the dev convenience script — carries three operational behaviours:
`load_dotenv()`, `check_pid_file()`, and `rotate_logs_on_startup(retention=10)`.
`seedpod/__main__.py`, which IS the `seedpod` console script and therefore the entry point an
artifact ships, carries **none of the three** (grep count: 0). The packaged path is the *less
capable* one. Everything an operator relies on today only exists in the file the artifact
would not include.

So all three move into the packaged entry point. And the singleton becomes a real lock:

- **`fcntl.flock(LOCK_EX | LOCK_NB)`** on a lockfile held open for the process's lifetime, not
  today's read-pid → `os.kill(pid, 0)` → unlink → write sequence. That sequence is
  check-then-act: two starts racing can both observe "stale" and both proceed. A flock is
  released by the kernel when the holder dies, so there is no stale-file cleanup path, no
  reliance on `atexit`, and no window.
- **The lockfile lives in `var/`**, not the repo root (`PROJECT_ROOT / "seedpod.pid"` has no
  meaning once `current` is a symlink into `releases/`).
- **Check the port as well as the lock.** A pid-liveness test cannot see "something else is
  listening on 8000". Wednesday's stale server answered `/health` perfectly.

**Honest note on why Wednesday's incident happened.** The existing guard would have caught it:
`start.py` writes `seedpod.pid` and refuses to start when a live pid holds it. My own restart
script defeated it, with an `rm -f seedpod.pid` before launching.

**And a correction, found by the test written for this amendment.** A first draft of this
section argued the flock is "harder to defeat, because a held lock does not care what the
filesystem says". That is wrong, and the test proved it: `rm -f` then start still gets through,
because `os.open(..., O_CREAT)` after an unlink makes a *new inode* and an flock on that is
uncontended — the incumbent holds a lock on an inode with no name. There is no filesystem-only
fix for unlink-then-recreate.

So the lock is not the backstop; **the port check is**. Two servers cannot both bind 8000,
whatever either believes about a file. The flock's real value is the narrower one it genuinely
delivers: no check-then-act window, and no stale-file cleanup path to get wrong. The pair is
defence in depth, and it is worth being precise about which half stops what — a guard everyone
believes is stronger than it is, is how Wednesday happened in the first place.

### C. Logging is already right; packaging only has to point it somewhere

Deliberately **not** redesigned. `app/logging.py` already does what v1 did, salvaged:
`TimedRotatingFileHandler(when="midnight", backupCount=30)` with the JSON formatter and the
correlation filter, plus `rotate_logs_on_startup(log_dir=..., retention=10)` for the
per-boot snapshot. Daily rotation, 30 days retained, 10 startup files. There is nothing to
decide.

What packaging owes it:

- `SEEDPOD_LOG_DIR` must be an **absolute** path under `var/log/`. `AppConfig.log_dir` defaults
  to the cwd-relative `Path("logs")` — the same trap as `config_dir`, and it bites harder here
  because a release root is not a working directory anyone will `cd` into.
- `rotate_logs_on_startup` moves into the packaged entry point with the other two (Amendment B),
  or the appliance silently loses per-boot log separation.

### Erratum E1 — the artifact carries `uv.lock`, not a `.venv` (2026-08-15)

Amendment A lists "the resolved `.venv`" among the artifact's contents. **It cannot be, and
decision 1 already said the right thing.** A virtualenv is not portable: its console scripts
carry the absolute path they were created at, and its wheels are built for one platform and
one interpreter ABI — while the release job that produces this artifact runs on ubuntu and
the appliance is macOS/arm64. Shipping one would ship something that cannot run.

So the artifact carries `uv.lock`, and install materialises the venv with `uv sync --locked`
— decision 1's own command, resolving the two halves in decision 1's favour. **The lock IS
the resolved runtime**; a `.venv` is only its local instantiation, and `uv.lock`'s hash is
stamped into `MANIFEST.json` so a lock edited on the appliance is visible rather than silent.

The same mistake in miniature: `.python-version` was the obvious way to pin the interpreter,
and it broke `uv` on the first host it met. **pyenv reads that filename too**, so where `uv`
is installed as a pyenv shim, a version pyenv does not have yields `pyenv: uv: command not
found` — the file pins the interpreter by disabling the tool that installs it. The pin lives
in the install command (`uv sync --locked --python 3.11`), which no other tool reads.

### Erratum E2 — the two guards refuse in one line, not a traceback (2026-08-15)

Decision 4 requires the launcher to "refuse to start if the port is already held, naming the
pid". `AlreadyRunning` and `PortUnavailable` compose exactly the right sentence — the holding
pid and the command that stops it — and then `main()` let them propagate, wrapping it in a
stack trace whose top frame is `contextlib`. That is this repo's oldest recurring defect in
its milder form: the reason is buried rather than dropped. `main()` now catches both, prints
one line, and exits **75** (`EX_TEMPFAIL`), distinct from the shell launchers' **78**
(`EX_CONFIG`) — so a script can tell "something is already running" from "this is
misconfigured". Found by starting a second `bin/seedpod` against a real installed release,
which is the only way that path is ever reached.

### D. A succinct ops section in `README.md`

Not a new document: root holds only `CLAUDE.md` and `README.md` (DR-0001), and everything else
belongs under `docs/`. The convenience scripts (`bin/seedpod`, `bin/seedpod-bootstrap`,
`bin/seedpodctl`) are documented there in the smallest form that works — install, upgrade, run,
stop, where state lives, how to read the logs. Short enough that it stays true.
