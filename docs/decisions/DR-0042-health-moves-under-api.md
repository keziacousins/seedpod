---
title: DR-0042 — the health endpoints move under /api, because the SPA owns /health
type: decision
status: active
created: 2026-08-15
updated: 2026-08-15
---

# DR-0042: `/health` moves to `/api/health`

**Status: ACTIVE — ratified by Kezia, 2026-08-15.** Amends `docs/design/ui-contract.md`
obligation 7 and DR-0003's endpoint naming. A deliberate, recorded divergence from v1.

## Context

Every API router is mounted under `/api` except one: `health` is included at the root
(`api/factory.py:98`), serving `GET /health` and `GET /health/detailed`. That was deliberate —
`health.py`'s own docstring records why: *"Mounted at the ROOT path (no `/api` prefix — the
acceptance gate hits `GET /health` bare)"*, and v1 served it at the root too
(`reference-code/seedpod/seedpod/api/health.py:23`, plus a `/health` entry in v1's middleware
allowlist).

**DR-0041 decision 3 turned that into a live defect.** Now that seedpod serves the SPA from its
own origin, the two namespaces overlap — and they overlap on exactly this path. The SPA has a
Health page at `/health` (`ui/src/app.jsx:42` nav item, `:130`
`<Health path="/health" />`). API routes are matched before the static mount, by design and
necessarily. So:

- clicking **Health** in the nav works, because preact-router never asks the server;
- **refreshing that page, or pasting the link, returns raw JSON** instead of the app.

Verified against a real `npm run build`: `/health` answers `HTTP 200 application/json`. It is
the deep-link failure DR-0041 decision 3 exists to prevent, on the one path where the fix
cannot help, because the API legitimately owns it first.

Two smaller things point the same way:

- `api/spa.py`'s `_API_PREFIXES = ("/api", "/health")` — that tuple has two entries solely
  because of this split. The SPA fallback needs a special case for one endpoint family.
- `ui/src/pages/Health.jsx:35` calls `apiClient.get("/health/detailed")` — **the only call in
  the SPA that does not pass a `/api/...` path.** All ~19 others already do.

## Decision

**The health router is included with `prefix="/api"`, like every other router.** The endpoints
become `GET /api/health` and `GET /api/health/detailed`. Their response shapes, their
public-ness (no auth dependency), and DR-0003's `/health/detailed` block structure are all
unchanged — this moves a URL and nothing else.

**No root alias is kept.** Serving health at both paths would leave the SPA unable to own
`/health`, which is the entire point; and a permanent alias is a second surface to remember in
every future routing decision. If an external prober ever needs a root path, that is a new
requirement with a new decision, not a reason to keep this one.

`_API_PREFIXES` collapses to `("/api",)`.

## Divergence from v1, stated plainly

v1 served `/health` at the root and this is a deliberate departure, so CLAUDE.md's first-named
failure mode — "silently regressing edge behaviour v1 already got right" — deserves an explicit
answer rather than an omission.

v1 got it right *for v1*. v1's SPA (`reference-code/.../seedpod-ui/`) was never served from the
API's origin, so no collision could arise and a conventional root health path cost nothing. v2
serves both from one process on one port (DR-0041), which is a shape v1 never had. The v1
behaviour being preserved here is "health is public, unauthenticated, and returns
`{status: 'healthy', ...}`" — all intact. What changes is a URL prefix, in a system whose
routing constraints are genuinely different.

The parity gate (`tests/acceptance/test_deployment_flow.py:376`) is updated to
`GET /api/health`. Its docstring's promise — *"Assertions are v1's, verbatim; only the fixture
plumbing changed"* — is narrowed by this DR: the assertions (`200`, `status == "healthy"`)
remain verbatim; the URL they are made against does not. That is the one line of the parity
gate this DR knowingly edits, and it is recorded here so a future reader does not mistake it
for drift.

## Rejected alternatives

- **Keep root `/health`; rename the SPA's page to `/status`.** Preserves v1's URL exactly and
  is a smaller change. Rejected because it fixes the symptom by moving the *UI*, leaves two API
  namespaces, keeps the special case in `spa.py`, and leaves `Health.jsx` as the one UI call
  shaped differently from every other.
- **Serve health at both `/health` and `/api/health`.** Removes nothing: the SPA still cannot
  own `/health`, and the ambiguity has to be remembered forever.

## What changes

- `api/factory.py` — `include_router(health.router, prefix="/api")`.
- `api/spa.py` — `_API_PREFIXES = ("/api",)`.
- `ctl/client.py` — `seedpodctl health basic|detailed` targets the new paths.
- `ui/src/pages/Health.jsx` — `/api/health/detailed`, matching every other call in the SPA.
- `tests/api/test_health.py`, `tests/app/test_entrypoint.py`, and the parity gate.
- `docs/design/ui-contract.md` — obligation 7 and the endpoint table, edited in place (it is
  normative "what is"). DR-0003 is append-only history and is left alone; this DR supersedes
  its endpoint *naming* while leaving its `/health/detailed` block shape wholly intact.

## What pins it

A test that `GET /health` returns **the SPA shell** when a UI is mounted — the deep-link case
that motivated this — alongside the existing `test_an_unknown_api_path_stays_a_404`. Together
they assert the boundary from both sides: the API keeps its 404s, and the SPA gets its page
back.
