"""seedpod/services/ghcr.py — ``GhcrService``, a supporting service (NOT a Provider —
docs/design/seam-c-provider.md §5.4 "Supporting services", coherence-review.md
§2 type glossary: ``services/ghcr.py``).

Talks to the GitHub REST API (``/orgs/{org}/packages/container/{repo}/versions``) over
an **injected** ``httpx.AsyncClient`` (§5.4's construction contract: ``__init__(config,
transport)`` is IO-free); fault injection for tests sits at that transport seam, never
``Mock``/``patch``. Same three-leaf error taxonomy as providers, but per decision-table
rows 32-35 (§5.1): **GHCR never raises ``InfrastructureUnreachableError``** — no
infra-state inference hangs on it, so every connectivity failure (timeout, conn error,
5xx, garbage body) classifies as ``TransientError``. One bounded attempt per call — no
internal retry/sleep, the engine's ``Schedule`` owns that (H4-H6).

Salvaged byte-for-byte from ``reference-code/seedpod/seedpod/providers/ghcr.py``
(``GHCRClient.find_image``/``find_latest_image``, lines 211-331, and
``GHCRService.resolve_images_for_branch``, lines 413-457) — the crown jewels:

- ``/`` -> ``-`` branch normalization (v1 line 233) before any tag matching.
- Pattern match ``^{branch}-[a-f0-9]+$`` (v1 line 242) preferred over an exact-name
  match; ties broken newest-first by ``updated_at`` (v1 line 247,
  ``sorted(..., key=lambda t: t.updated_at, reverse=True)``).
- Mutable -> immutable digest re-resolution (v1 lines 265-286): if only an exact
  (mutable) tag match exists, look for another tag with the *same digest* that also
  matches the commit pattern, and prefer that immutable tag over the mutable one.
  Depends on the GHCR API quirk (v1 line 194 comment) that a package "version"'s
  ``name`` field is actually the image's sha256 digest, not a human name — see
  ``_tags_from_version`` below, which carries that quirk forward verbatim.
- ``dev``/``main``/``master`` fallback chain (v1 lines 296-331,
  ``GHCRClient.find_latest_image``): try the requested branch first, then each
  fallback in order, skipping the branch itself if it's already one of the fallbacks.
- Per-repository failure -> ``None`` for that repo only, never aborting the batch (v1
  lines 443-452, ``GHCRService.resolve_images_for_branch``'s per-repo
  try/except-and-continue).
- 404 (repository doesn't exist / has no packages) -> ``()`` from ``list_tags``, which
  propagates to ``None`` from ``find_image`` — DATA, never an exception (row 34).

**No-pagination-past-100 limitation** (documented, not silently carried): GHCR's
packages/versions endpoint is paginated: v1 requested ``per_page=100`` and never
followed ``Link: rel="next"`` (reference-code .../ghcr.py:181), so any repository with
more than 100 versions silently loses its oldest tags to this service's view. Carried
forward unchanged — real pagination is future work, not this task's scope.

Dead code deliberately NOT copied (Seam C §5.4, "Dead code not copied"): ``GHCRService``
v1's ``_repository_cache``/``_cache_ttl_minutes`` (never actually consulted — set but
never read, reference-code .../ghcr.py:410-411) and ``GHCRClient._rate_limit`` (a
no-op: ``REQUEST_DELAY = 0.0``, reference-code .../ghcr.py:79-119).
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime

import httpx

from seedpod.core.errors import ErrorCode, ProviderError, TransientError
from seedpod.providers.classify import classify_http

__all__ = ["GhcrConfig", "ImageTag", "GhcrService"]

_HOST = "ghcr.io"  # the classify_http() host label; API calls actually hit api_url (api.github.com)


@dataclass(frozen=True)
class GhcrConfig:
    """IO-free construction data. Loaded by the composition root from
    ``config/providers/ghcr.yml`` (or equivalent) + a ``GHCR_TOKEN``/GitHub-PAT secret;
    this module never reads a file or an environment variable itself."""

    token: str
    organization: str
    registry_url: str = "https://ghcr.io"
    api_url: str = "https://api.github.com"
    timeout_s: float = 30.0


@dataclass(frozen=True)
class ImageTag:
    """Salvaged from v1's ``ImageTag`` dataclass (reference-code .../ghcr.py:22-29),
    now frozen. ``digest`` is the GHCR "version name" quirk documented on the module."""

    name: str
    digest: str
    created_at: datetime
    updated_at: datetime
    size_bytes: int


def _parse_iso(value: str) -> datetime:
    # v1's `.replace("Z", "+00:00")` normalization (reference-code .../ghcr.py:158-159,
    # 195-196), salvaged verbatim: GitHub returns `Z`-suffixed UTC timestamps, which
    # `datetime.fromisoformat` only accepts as an explicit offset pre-3.11-quirks.
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _tags_from_version(version: Mapping[str, object]) -> tuple[ImageTag, ...]:
    """One GHCR package "version" can carry multiple tags (v1 lines 189-198) — each
    becomes its own ``ImageTag``, all sharing ``version["name"]`` as ``digest`` (the
    version-name-is-the-sha256 quirk)."""
    metadata = version.get("metadata")
    container = metadata.get("container") if isinstance(metadata, Mapping) else None
    tag_names = container.get("tags") if isinstance(container, Mapping) else None
    if not isinstance(tag_names, Sequence) or isinstance(tag_names, (str, bytes)):
        return ()
    size = container.get("size", 0) if isinstance(container, Mapping) else 0
    created_at = _parse_iso(str(version["created_at"]))
    updated_at = _parse_iso(str(version["updated_at"]))
    digest = str(version["name"])
    return tuple(
        ImageTag(name=str(name), digest=digest, created_at=created_at, updated_at=updated_at, size_bytes=int(size))
        for name in tag_names
    )


class GhcrService:
    """Stateless (§5.4's construction contract, even though this is a service, not a
    ``Provider``): no cache, no DB, one bounded attempt per HTTP call."""

    def __init__(self, config: GhcrConfig, transport: httpx.AsyncClient) -> None:
        self.config = config
        self.transport = transport

    # ------------------------------------------------------------------
    # reads
    # ------------------------------------------------------------------

    async def list_tags(self, repository: str) -> tuple[ImageTag, ...]:
        """Row 34: repository not found (404) -> ``()``, DATA never an exception. No
        pagination past 100 results (module docstring)."""
        url = f"{self.config.api_url}/orgs/{self.config.organization}/packages/container/{repository}/versions"
        body = await self._get_json(url, params={"per_page": 100}, command="list_tags", not_found_ok=True)
        if body is None:
            return ()
        if not isinstance(body, Sequence) or isinstance(body, (str, bytes)):
            return ()
        tags: list[ImageTag] = []
        for version in body:
            if isinstance(version, Mapping):
                tags.extend(_tags_from_version(version))
        return tuple(tags)

    async def find_image(self, repository: str, branch_or_tag: str) -> str | None:
        """Salvaged verbatim from ``GHCRClient.find_image``
        (reference-code .../ghcr.py:211-294). Returns ``None`` (never raises) when no
        tag matches — row 34's "absence is data" applied to the derived lookup, not
        just the raw list."""
        tags = await self.list_tags(repository)
        if not tags:
            return None

        # v1 line 233: `/` -> `-` so branch names like "feature/payments" match GHCR's
        # tag-safe spelling "feature-payments".
        normalized_branch = branch_or_tag.replace("/", "-")

        # 1. Pattern match: branch-{commit-sha-prefix}, MOST SPECIFIC — prefer this,
        # newest-first by updated_at (v1 lines 242-255).
        pattern = re.compile(f"^{re.escape(normalized_branch)}-[a-f0-9]+$")
        pattern_matches = [tag for tag in tags if pattern.match(tag.name)]
        if pattern_matches:
            latest = sorted(pattern_matches, key=lambda t: t.updated_at, reverse=True)[0]
            return self._image_url(repository, latest.name)

        # 2. Fallback: exact match (v1 lines 257-286).
        exact_matches = [tag for tag in tags if tag.name == normalized_branch]
        if not exact_matches:
            return None
        latest_exact = max(exact_matches, key=lambda t: t.updated_at)

        # Mutable -> immutable re-resolution: same digest, commit-pattern name exists?
        same_digest_tags = [tag for tag in tags if tag.digest == latest_exact.digest]
        commit_tagged = [tag for tag in same_digest_tags if pattern.match(tag.name)]
        if commit_tagged:
            resolved = max(commit_tagged, key=lambda t: t.updated_at)
            return self._image_url(repository, resolved.name)

        # No commit-tagged version of this image exists yet — use the mutable tag.
        return self._image_url(repository, latest_exact.name)

    async def find_latest_image(self, repository: str, branch: str = "main") -> str | None:
        """Salvaged verbatim from ``GHCRClient.find_latest_image``
        (reference-code .../ghcr.py:296-331): try ``branch``, then the ``dev``/``main``/
        ``master`` fallback chain in order (skipping ``branch`` itself if already one
        of them)."""
        image_url = await self.find_image(repository, branch)
        if image_url:
            return image_url

        fallback_branches = ["dev", "main", "master"] if branch not in ("dev", "main", "master") else [branch]
        for fallback in fallback_branches:
            if fallback == branch:
                continue
            image_url = await self.find_image(repository, fallback)
            if image_url:
                return image_url
        return None

    async def resolve_images_for_branch(
        self,
        repositories: Sequence[str],
        branch: str,
        exclude_repos: frozenset[str] | None = None,
    ) -> dict[str, str | None]:
        """Salvaged from ``GHCRService.resolve_images_for_branch``
        (reference-code .../ghcr.py:413-457): per-repository failure maps to ``None``
        for that repo only — the batch never aborts on one bad repository (crown
        jewel #5). ``InfrastructureUnreachableError`` cannot occur here (GHCR never
        raises it — row 91's class-3 sentence); ``ProviderError`` is the catch-all for
        "this repo failed, keep going"."""
        exclude = exclude_repos or frozenset()
        results: dict[str, str | None] = {}
        for repo in repositories:
            if repo in exclude:
                results[repo] = None
                continue
            try:
                results[repo] = await self.find_image(repo, branch)
            except ProviderError:
                results[repo] = None
        return results

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------

    def _image_url(self, repository: str, tag: str) -> str:
        registry_host = self.config.registry_url.replace("https://", "").replace("http://", "")
        return f"{registry_host}/{self.config.organization}/{repository}:{tag}"

    async def _get_json(
        self, url: str, *, params: Mapping[str, object], command: str, not_found_ok: bool
    ) -> object | None:
        """One bounded attempt (no internal retry — H4-H6). Never raises
        ``InfrastructureUnreachableError`` (row 91): transport-level failures and
        malformed bodies classify as ``TransientError`` here, unlike the machine
        providers' ``observing_infra=True`` path."""
        headers = {
            "Authorization": f"Bearer {self.config.token}",
            "Accept": "application/vnd.github.v3+json",
        }
        try:
            response = await self.transport.get(url, params=params, headers=headers, timeout=self.config.timeout_s)
        except httpx.TimeoutException as e:
            raise TransientError(
                f"ghcr.{command}: timed out calling {url}",
                code=ErrorCode.API_TIMEOUT,
                provider="ghcr",
                command=command,
                detail={"url": url},
            ) from e
        except httpx.TransportError as e:
            raise TransientError(
                f"ghcr.{command}: could not reach {_HOST}: {e}",
                code=ErrorCode.ENDPOINT_UNREACHABLE,
                provider="ghcr",
                command=command,
                detail={"url": url},
            ) from e

        if response.status_code == 404 and not_found_ok:
            return None

        if response.status_code == 403:
            # Row 33: v1 treats every 403 as a rate-limit signal (GHCRRateLimitError,
            # reference-code .../ghcr.py:125-126) — GHCR's REST API has no separate
            # "forbidden" distinct from rate-limiting at this call site.
            retry_after = self._retry_after(response)
            raise classify_http(
                provider="ghcr",
                command=command,
                host=_HOST,
                status=403,
                rate_limited=True,
                retry_after=retry_after,
                observing_infra=False,
            )

        if not response.is_success:
            raise classify_http(
                provider="ghcr", command=command, host=_HOST, status=response.status_code, observing_infra=False
            )

        if not response.content:
            raise classify_http(
                provider="ghcr",
                command=command,
                host=_HOST,
                status=response.status_code,
                malformed_body=True,
                observing_infra=False,
            )
        try:
            return response.json()
        except ValueError as e:
            raise classify_http(
                provider="ghcr",
                command=command,
                host=_HOST,
                status=response.status_code,
                malformed_body=True,
                observing_infra=False,
            ) from e

    @staticmethod
    def _retry_after(response: httpx.Response) -> float | None:
        if "retry-after" in response.headers:
            try:
                return float(response.headers["retry-after"])
            except ValueError:
                return None
        return None
