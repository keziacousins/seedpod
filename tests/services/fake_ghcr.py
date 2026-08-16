"""tests/services/fake_ghcr.py — a typed FAKE TRANSPORT simulating enough of the GitHub
packages/versions API for ``seedpod.services.ghcr.GhcrService`` testing (mirrors
``tests/conformance/fake_digitalocean.py``'s pattern for the Pillar-3 provider suite).

``FakeGhcrBackend`` is the in-memory "registry": a plain mutable store of package
"versions" (GHCR's odd unit: one version == one image digest, with a *list* of tags
attached) mined from real GHCR API response shapes. ``FakeGhcrTransport`` is an
``httpx.AsyncBaseTransport`` — installed into a real ``httpx.AsyncClient(transport=...)``,
so fault injection happens at the actual transport seam ``GhcrService`` talks to, never
``Mock``/``patch`` (CLAUDE.md).

Reuses ``tests.conformance.harness.Fault`` for the fault vocabulary shared with the
six-provider conformance suite (UNREACHABLE/AUTH/RATE_LIMIT/TRANSIENT_ONCE all have
direct GHCR analogues); ``malformed_body``/``not_found`` are separate constructor
toggles because GHCR's row-35 "JSON garbage" symptom and row-34 "repository not found"
have no member in that shared enum (they're not among the six injectable ``Fault``s
documented on ``tests/conformance/harness.py``).
"""

from __future__ import annotations

from dataclasses import dataclass, field

import httpx

from tests.conformance.harness import Fault

__all__ = ["FakeGhcrBackend", "FakeGhcrTransport"]


@dataclass
class FakeGhcrBackend:
    """repo name -> list of raw GitHub "package version" JSON dicts."""

    versions: dict[str, list[dict]] = field(default_factory=dict)
    call_count: int = 0

    def add_version(
        self,
        repo: str,
        *,
        digest: str,
        tags: list[str],
        created_at: str = "2026-01-01T00:00:00Z",
        updated_at: str = "2026-01-01T00:00:00Z",
        size: int = 1024,
    ) -> None:
        self.versions.setdefault(repo, []).append(
            {
                "name": digest,  # the version-name-is-the-sha256 quirk
                "created_at": created_at,
                "updated_at": updated_at,
                "metadata": {"container": {"tags": tags, "size": size}},
            }
        )


class FakeGhcrTransport(httpx.AsyncBaseTransport):
    """Routes ``httpx.Request``s against a ``FakeGhcrBackend`` and applies ``Fault``
    behavior. Installed via ``httpx.AsyncClient(transport=FakeGhcrTransport(...))``."""

    def __init__(
        self,
        backend: FakeGhcrBackend,
        faults: frozenset[Fault] = frozenset(),
        *,
        malformed_body: bool = False,
    ) -> None:
        self.backend = backend
        self.faults = faults
        self.malformed_body = malformed_body
        self._transient_once_consumed = False

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.backend.call_count += 1

        if Fault.UNREACHABLE in self.faults:
            raise httpx.ConnectTimeout("simulated GitHub API timeout", request=request)

        if Fault.AUTH in self.faults:
            return httpx.Response(401, json={"message": "Bad credentials"})

        if Fault.RATE_LIMIT in self.faults:
            return httpx.Response(403, json={"message": "API rate limit exceeded"}, headers={"Retry-After": "5"})

        if Fault.TRANSIENT_ONCE in self.faults and not self._transient_once_consumed:
            self._transient_once_consumed = True
            return httpx.Response(503, json={"message": "temporarily unavailable"})

        if self.malformed_body:
            return httpx.Response(200, content=b"not json {{{")

        path = request.url.path
        prefix = "/orgs/"
        assert path.startswith(prefix), f"fake GHCR transport only routes /orgs paths, got {path}"
        # /orgs/{org}/packages/container/{repo}/versions
        parts = path[len(prefix) :].split("/")
        repo = parts[3] if len(parts) >= 5 else None

        if repo is None or repo not in self.backend.versions:
            return httpx.Response(404, json={"message": "Not Found"})

        return httpx.Response(200, json=self.backend.versions[repo])
