---
name: run-seedpod
description: Build, launch, smoke and screenshot the seedpod control plane and its SPA. Use when asked to run, start, boot, drive, smoke-test or screenshot seedpod or its UI, to check the API by hand, or to confirm a change works in the real running app rather than only in pytest.
---

# Running seedpod

Seedpod is a FastAPI control plane that also serves its own preact SPA from the
same origin (DR-0041). Driving it means two surfaces: the HTTP API, and the
built bundle in a browser.

**Paths below are relative to the repo root.** The driver is
`.claude/skills/run-seedpod/driver.sh`.

`pytest` covers ~2440 cases and never boots a server. It cannot catch a broken
bundle, an unmounted SPA, or a routing regression — that is what this is for.

## Prerequisites

Already present in a normal checkout: `uv`, `node` (24), `npm`. No system
packages were needed on macOS 15. The driver needs nothing else.

## Run (agent path)

```bash
.claude/skills/run-seedpod/driver.sh up      # build SPA, init state, launch, wait for health
.claude/skills/run-seedpod/driver.sh smoke   # 12 assertions; exits non-zero on failure
.claude/skills/run-seedpod/driver.sh key     # the admin API key (for browser login)
.claude/skills/run-seedpod/driver.sh down    # stop
.claude/skills/run-seedpod/driver.sh reset   # stop and delete all state
```

`up` is idempotent — it reuses the existing database, keys and admin key, so
`down` then `up` returns to the same state. `reset` is the one that starts over.

All run state lives in `/tmp/seedpod-smoke` (override with `SEEDPOD_SMOKE_DIR`);
the server listens on `127.0.0.1:8099` (`SEEDPOD_SMOKE_PORT`). **Nothing is
written into the checkout** — see Gotchas for why that matters.

`smoke` asserts the invariants that are easy to break and invisible to pytest:
health, the SPA at `/`, deep links falling back to `index.html`, unknown
`/api/*` staying a JSON 404 rather than being swallowed by that fallback, 401
without a key, 200 with one, and every `/assets/*` referenced by `index.html`
resolving.

## Run (browser path)

There is **no `chromium-cli`, `chromium` or `playwright` binary on this
machine** — driving the UI uses the Playwright **MCP tools** instead. After
`driver.sh up`:

1. `mcp__playwright__browser_navigate` → `http://127.0.0.1:8099/`
2. `mcp__playwright__browser_snapshot` → the login form (`API Token` + `Login`)
3. `mcp__playwright__browser_type` the key from `driver.sh key` into the token box
4. `mcp__playwright__browser_click` the `Login` button
5. `mcp__playwright__browser_snapshot` → nav, `Connected` badge, cluster table
6. `mcp__playwright__browser_take_screenshot` with a **relative** `filename`

Take a **fresh snapshot after any navigation or click** before referencing an
element. Refs like `f1e46` are invalidated by a re-render, and reusing one — or
guessing a selector like `textbox` — fails with `does not match any elements`.
Logging out and back in is exactly such a re-render.

Screenshots must use a relative filename — an absolute `/tmp/...` path is
rejected ("outside allowed roots"). The file lands at the **repo root**, not in
`.playwright-mcp/` as the tool result implies. Both it and the `.playwright-mcp/`
directory are untracked files in the checkout, so delete them when done or the
next release build refuses to run.

A logged-in session should show the `Connected` badge (SSE is live) and
`No data available` in the cluster table on a fresh database.

## What this canNOT exercise

The driver starts with **no providers** and background tasks off, so
provisioning, deployment, DNS, reconciliation and TTL destruction are all out of
reach. It verifies the app boots, serves, authenticates and renders. Anything
touching real infrastructure still needs a real smoke that spends real money.

## Gotchas

- **`SEEDPOD_ENABLED_PROVIDERS=""` does not disable providers.** `_csv_env`
  ends `return values or default`, so empty falls back to all four, and startup
  dies in `digitalocean.check_ready()` with `Illegal header value b'Bearer '` —
  httpx rejecting a header built from an absent `DIGITALOCEAN_TOKEN`. It reads
  like a networking bug and is a config one. Pass a name no provider has
  (`none`); `load_enabled_providers` constructs nothing for it, which is the
  documented way to disable one.
- **`SEEDPOD_UI_DIR` unset mounts no SPA at all** — the server is healthy and
  `/` 404s. Set but without `index.html` inside raises `SpaNotBuilt` and the
  process exits. Build the UI first.
- **Never let run state land in the checkout.** `scripts/build_release.py`
  refuses to build from a dirty tree and counts *untracked* files as dirty, so a
  stray `seedpod.pid`, `db/` or `logs/` silently blocks the next release. The
  driver points `SEEDPOD_PID_FILE`, `SEEDPOD_LOG_DIR` and an absolute sqlite URL
  at its scratch dir for exactly this reason.
- **Backgrounding the server needs `nohup` + `</dev/null` + `disown`, all
  three.** With a plain `cmd &` the server keeps the caller's stdout pipe open,
  so `driver.sh up | tail` hangs forever even though the server started fine.
- **A stale key in `localStorage` does not bounce you to the login screen.**
  After `reset` the browser still holds the previous key; the app shows
  `Disconnected` plus `Error: Authentication failed. Please log in again.` and
  keeps the shell. Click `Logout`, then log in with the new key.
- **The SSE token travels in the query string** —
  `/api/events/stream?token=<key>` (`ui/src/lib/sse-client.js:30`), because
  `EventSource` cannot set headers. API keys therefore appear in URLs and server
  logs; do not paste a production key into a shared terminal.
- **The admin key is printed exactly once** by `create-admin`. The driver
  captures it to `$STATE/admin-key.txt`; by hand, save it immediately.
- **Docs are stale on `.env` loading.** `.env.example` and
  `docs/guides/operations.md` say only `start.py` loads `.env`, but
  `seedpod/__main__.py:162` calls `load_dotenv()` too (DR-0041 Amendment B moved
  it there). Both entry points load it.
- **Do not use `$TMPDIR` for state on macOS** — it is a per-user
  `/var/folders/…/T/` path *with a trailing slash*, which makes the state
  directory unpredictable and yields `//seedpod-smoke`.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `Illegal header value b'Bearer '` at startup, exit 3 | `SEEDPOD_ENABLED_PROVIDERS` fell back to the default four. Set it to `none`. |
| `SpaNotBuilt` at startup | `ui/dist/index.html` missing. `cd ui && npm ci --ignore-scripts && npm run build`. |
| `/` returns 404, health is 200 | `SEEDPOD_UI_DIR` unset, so nothing was mounted. |
| `driver.sh up` never returns when piped | The launch lost `</dev/null`/`disown`. The server is usually running — check `curl 127.0.0.1:8099/api/health`. |
| UI shows `Disconnected` + auth error | Stale `localStorage` key. Click `Logout` and log in with `driver.sh key`. |
| Startup exits complaining another instance holds the port | A previous run's server survived. `driver.sh down`, or `pkill -f 'python start.py'`. |
| Screenshot fails "outside allowed roots" | Use a relative `filename`; the file lands at the repo root. |

## Test suites (sanity check, not the main event)

```bash
uv run ruff check .        # clean
uv run pytest -q           # 2442 passed, 44 skipped
cd ui && npm test          # 12 passed
cd ui && npm run build     # vite 8 / rolldown
```
