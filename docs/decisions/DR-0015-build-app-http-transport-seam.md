---
title: DR-0015 — Fourth build_app seam: http_transport for httpx-based supporting services
type: decision
status: active
created: 2026-07-17
updated: 2026-07-17
---

# DR-0015: Fourth `build_app` seam — `http_transport` for the httpx supporting services

**Status: ACTIVE — ratified by Kezia, 2026-07-17. First Round-6 halt (app-composition build, run
`wf_3c2e5583-540`, 2026-07-17). Predicted by the parity gate's own docstring
(`test_deployment_flow_with_github_token`) and by seam-d Decision 8's GHCR construction.**

## Problem

`GhcrService.__init__(config, transport: httpx.AsyncClient)` and
`DnsService.__init__(config, transport: httpx.AsyncClient)` take an **injected** httpx client
(§5.4's construction contract — "conformance fault injection happens at its
`httpx.AsyncBaseTransport` seam, never `unittest.mock`"). The four cloud providers do the same,
but providers are already swappable in tests through `build_app`'s `providers=` seam. The two
httpx-based *supporting services* (GHCR, Cloudflare DNS) are **not** providers, so nothing in
the current test surface can give them a fake transport:

- `tests/conftest.py` pins `build_app`'s test seams as exactly three — `providers`, `clock`,
  `id_gen` — and states they "are the entire test surface".
- seam-d Decision 8's `AppConfig` field list (which Round 6 is instructed to match exactly) has
  no transport field.

So `test_deployment_flow_with_github_token` — which sets `github_token` and must exercise the
GHCR path **without real network and without `patch()`** — has nowhere to inject a fake. The
acceptance suite's own docstring flagged this ("GhcrService must be fakeable at its transport
seam or injectable through build_app — return the problem to a DR if neither fits"). The
app-composition build agent raised it before the DeploymentService component (its first
consumer) would hit it.

## Decision

**Add a fourth keyword-only seam to `build_app`: `http_transport: httpx.AsyncClient | None =
None`**, alongside `providers` / `clock` / `id_gen`. It is the shared outbound-HTTP seam for the
httpx-based supporting services.

- **Default (`None`)**: `build_app` constructs one real `httpx.AsyncClient` (sane timeouts, no
  `base_url`, no default headers — the services already pass absolute URLs and per-request
  headers) and shares it across the httpx supporting services it wires.
- **Wiring is credential-gated** (matches v1 and the acceptance expectations): GHCR is wired
  only when `config.github_token` is set — otherwise `ManifestResolver` is constructed with
  `ghcr_service=None` and image resolution degrades gracefully (the "limited manifest
  resolution" the no-token acceptance test asserts). Cloudflare DNS likewise wires only when
  `config.cloudflare_api_token` is present. So the default hermetic acceptance fixtures
  (`github_token=None`) make **no** outbound HTTP at all; only credentialed runs use the
  transport.
- **Tests** inject a fake via `httpx.AsyncClient(transport=httpx.MockTransport(handler))` —
  httpx's own library transport (the same `httpx.AsyncBaseTransport` seam the conformance suite
  already uses for providers), **not** `unittest.mock`. `tests/conftest.py`'s `make_app`
  forwards `http_transport`, and its "three seams" note becomes four.
  `test_deployment_flow_with_github_token` supplies a handler returning a canned GHCR version
  listing so the credentialed path resolves images offline.
- **`AppConfig` is UNCHANGED** — Decision 8's field list stands. The transport is a `build_app`
  kwarg (a *test seam*), not a config *value*, exactly like `providers`/`clock`/`id_gen`. This
  is the distinction the exact-field-list instruction was protecting.

## Consequences

- seam-d Decision 8's `build_app` signature gains the fourth kwarg (amended in place, citing this
  DR); `tests/conftest.py`'s three-seams note becomes four and `make_app` forwards it.
- The parity gate goes green hermetically: default fixtures make no network; the one github-token
  test injects a `MockTransport` handler.
- Pinned tests: `build_app(http_transport=fake)` routes GHCR/DNS calls to the fake; with
  `github_token=None` no client is touched (ghcr_service is None); the github-token acceptance
  test resolves images from the injected handler with zero real network.
- One shared client serves all httpx supporting services; a test handler routes by host
  (`api.github.com` vs the Cloudflare host).

## Alternatives considered

- **An `AppConfig` transport-factory field** — rejected: contradicts Decision 8's exact field
  list, and conflates config *values* with injected *objects*; the existing three seams are
  `build_app` kwargs, not config fields, precisely for this reason.
- **No seam; let the github-token test hit real GHCR and rely on graceful degradation** —
  rejected: non-hermetic (network dependency, flakiness, latency), and a real 401 is not the
  "token works, images resolve" path the test asserts.
- **Separate per-service seams (`ghcr_transport`, `dns_transport`)** — rejected: one shared
  client suffices (absolute URLs; the `MockTransport` handler routes by host); minimize seam
  surface.
- **Fold GHCR/DNS into the `providers=` mapping** — rejected: they are not `Provider`s (no
  Provider protocol); it would muddy that seam's meaning.
