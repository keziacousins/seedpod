"""tests/services/test_ghcr.py — golden tests for ``seedpod.services.ghcr.GhcrService``'s
salvaged crown jewels (docs/design/seam-c-provider.md §5.4 "Supporting services"), plus
the conformance-suite "shapes" the seam says apply to services (⊂ of C-05/C-10/C-15/C-17
— §5.6's table) even though GHCR/DNS don't implement the ``Provider`` protocol and so
can't literally join ``tests/conformance``'s ``Harness``-parametrized suite (whose shape
is built around ``ProviderCommand``/``execute()``, not a plain typed-method service).

No ``Mock``/``patch`` anywhere: fault injection sits at ``FakeGhcrTransport``, the actual
``httpx.AsyncBaseTransport`` seam ``GhcrService`` talks to (CLAUDE.md).
"""

from __future__ import annotations

import time

import httpx
import pytest

from seedpod.core.errors import (
    ErrorCode,
    InfrastructureUnreachableError,
    PermanentError,
    TransientError,
)
from seedpod.services.ghcr import GhcrConfig, GhcrService
from tests.conformance.harness import Fault
from tests.services.fake_ghcr import FakeGhcrBackend, FakeGhcrTransport

pytestmark = pytest.mark.asyncio

_ORG = "exampleco"
_REPO = "exampleco-core"


def _service(backend: FakeGhcrBackend, *faults: Fault, malformed_body: bool = False) -> GhcrService:
    transport = httpx.AsyncClient(transport=FakeGhcrTransport(backend, frozenset(faults), malformed_body=malformed_body))
    config = GhcrConfig(token="fake-token", organization=_ORG)  # pragma: allowlist secret
    return GhcrService(config, transport)


# ============================================================================
# crown jewels: find_image / find_latest_image
# ============================================================================


async def test_branch_slash_normalized_to_dash_before_matching():
    """v1 line 233: 'feature/payments' -> 'feature-payments' before any tag match."""
    backend = FakeGhcrBackend()
    backend.add_version(_REPO, digest="sha256:aaa", tags=["feature-payments-abc123f"])
    service = _service(backend)

    image = await service.find_image(_REPO, "feature/payments")

    assert image == f"ghcr.io/{_ORG}/{_REPO}:feature-payments-abc123f"


async def test_pattern_match_preferred_and_newest_first_by_updated_at():
    """Commit-tagged matches beat exact matches; among commit-tagged matches, the one
    with the newest ``updated_at`` wins — not creation order (v1 lines 242-255)."""
    backend = FakeGhcrBackend()
    backend.add_version(
        _REPO, digest="sha256:older", tags=["main-1111111"], created_at="2026-01-01T00:00:00Z", updated_at="2026-01-01T00:00:00Z"
    )
    backend.add_version(
        _REPO, digest="sha256:newer", tags=["main-2222222"], created_at="2026-01-02T00:00:00Z", updated_at="2026-01-03T00:00:00Z"
    )
    backend.add_version(_REPO, digest="sha256:exact", tags=["main"], created_at="2026-01-04T00:00:00Z", updated_at="2026-01-04T00:00:00Z")
    service = _service(backend)

    image = await service.find_image(_REPO, "main")

    assert image == f"ghcr.io/{_ORG}/{_REPO}:main-2222222"


async def test_mutable_tag_resolved_to_immutable_commit_tag_same_digest():
    """Only an exact ('main') match exists at first glance, but another tag on the SAME
    digest carries a commit-pattern name — v1 prefers that immutable tag (lines
    265-280)."""
    backend = FakeGhcrBackend()
    backend.add_version(
        _REPO,
        digest="sha256:shared",
        tags=["main", "main-deadbee1"],
        updated_at="2026-01-05T00:00:00Z",
    )
    service = _service(backend)

    image = await service.find_image(_REPO, "main")

    assert image == f"ghcr.io/{_ORG}/{_REPO}:main-deadbee1"


async def test_mutable_tag_used_when_no_commit_tagged_sibling_exists():
    """Exact match with no same-digest commit-tagged sibling ⇒ use the mutable tag
    itself (v1 lines 281-286)."""
    backend = FakeGhcrBackend()
    backend.add_version(_REPO, digest="sha256:onlymutable", tags=["main"])
    service = _service(backend)

    image = await service.find_image(_REPO, "main")

    assert image == f"ghcr.io/{_ORG}/{_REPO}:main"


async def test_dev_main_master_fallback_chain():
    """No image for the requested branch ⇒ try dev, then main, then master, in order
    (v1 lines 296-331)."""
    backend = FakeGhcrBackend()
    backend.add_version(_REPO, digest="sha256:m", tags=["main-c0ffee1"])
    service = _service(backend)

    image = await service.find_latest_image(_REPO, "feature-nonexistent")

    assert image == f"ghcr.io/{_ORG}/{_REPO}:main-c0ffee1"


async def test_fallback_chain_skips_branch_itself_when_already_a_fallback_name():
    """``branch='main'`` with no image ⇒ fallback list collapses to just ``['main']``,
    which is then skipped as 'already tried' — no image found, not an infinite loop or
    a spurious dev/master lookup (v1 line 314)."""
    backend = FakeGhcrBackend()  # no versions at all
    service = _service(backend)

    image = await service.find_latest_image(_REPO, "main")

    assert image is None


async def test_per_repository_failure_returns_none_for_that_repo_batch_continues():
    """Crown jewel #5: one bad repository (auth failure ⇒ PermanentError) doesn't abort
    resolution for the rest of the batch (v1 lines 443-452)."""
    healthy = FakeGhcrBackend()
    healthy.add_version("ok-repo", digest="sha256:ok", tags=["main-abc0001"])
    healthy.add_version("bad-repo", digest="sha256:bad", tags=["main-abc0002"])

    # A single shared transport with AUTH faulted would fail every repo; instead we
    # exercise the "per-repo failure" contract by raising directly from a stub
    # transport that fails only requests for "bad-repo".
    class _PerRepoFaultyTransport(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            if "/bad-repo/" in request.url.path:
                return httpx.Response(401, json={"message": "Bad credentials"})
            repo = request.url.path.split("/")[5]
            return httpx.Response(200, json=healthy.versions.get(repo, []))

    transport = httpx.AsyncClient(transport=_PerRepoFaultyTransport())
    service = GhcrService(GhcrConfig(token="t", organization=_ORG), transport)  # pragma: allowlist secret

    results = await service.resolve_images_for_branch(["ok-repo", "bad-repo"], "main")

    assert results["ok-repo"] == f"ghcr.io/{_ORG}/ok-repo:main-abc0001"
    assert results["bad-repo"] is None


async def test_exclude_repos_short_circuits_without_a_call():
    backend = FakeGhcrBackend()
    backend.add_version("skip-me", digest="sha256:x", tags=["main-abc0003"])
    service = _service(backend)

    results = await service.resolve_images_for_branch(["skip-me"], "main", exclude_repos=frozenset({"skip-me"}))

    assert results == {"skip-me": None}
    assert backend.call_count == 0


# ============================================================================
# C-05 shape — absence is DATA, never an exception (row 34)
# ============================================================================


async def test_list_tags_on_missing_repository_is_empty_tuple_not_exception():
    service = _service(FakeGhcrBackend())

    tags = await service.list_tags("does-not-exist")

    assert tags == ()


async def test_find_image_with_no_match_is_none_not_exception():
    backend = FakeGhcrBackend()
    backend.add_version(_REPO, digest="sha256:x", tags=["unrelated-branch"])
    service = _service(backend)

    image = await service.find_image(_REPO, "main")

    assert image is None


# ============================================================================
# C-15 shape — one bounded attempt, no internal retry/sleep (H4-H6)
# ============================================================================


async def test_single_attempt_no_internal_retry_then_succeeds_on_reinvocation():
    backend = FakeGhcrBackend()
    backend.add_version(_REPO, digest="sha256:x", tags=["main-c0ffee2"])
    service = _service(backend, Fault.TRANSIENT_ONCE)

    before = backend.call_count
    start = time.monotonic()
    with pytest.raises(TransientError):
        await service.list_tags(_REPO)
    elapsed = time.monotonic() - start

    assert backend.call_count - before == 1, "exactly one transport attempt, no internal retry loop"
    assert elapsed < 2.0, f"{elapsed:.2f}s suggests an internal retry/sleep loop"

    # Same service instance now succeeds — the fault was single-shot.
    tags = await service.list_tags(_REPO)
    assert len(tags) == 1


# ============================================================================
# C-17 shape — classification decision-table rows 32/33/35, GHCR never Unreachable
# ============================================================================

_CLASSIFICATION_CASES = [
    pytest.param((Fault.AUTH,), False, PermanentError, ErrorCode.AUTH, id="row32-401-permanent-auth"),
    pytest.param((Fault.RATE_LIMIT,), False, TransientError, ErrorCode.RATE_LIMITED, id="row33-403-transient-rate-limited"),
    pytest.param((Fault.TRANSIENT_ONCE,), False, TransientError, ErrorCode.API_5XX, id="row35-5xx-transient"),
    pytest.param((Fault.UNREACHABLE,), False, TransientError, ErrorCode.API_TIMEOUT, id="row35-timeout-transient-never-unreachable"),
    pytest.param((), True, TransientError, ErrorCode.MALFORMED_RESPONSE, id="row35-malformed-body-transient"),
]


@pytest.mark.parametrize("faults,malformed_body,expected_cls,expected_code", _CLASSIFICATION_CASES)
async def test_classification_table(faults, malformed_body, expected_cls, expected_code):
    backend = FakeGhcrBackend()
    backend.add_version(_REPO, digest="sha256:x", tags=["main-c0ffee3"])
    service = _service(backend, *faults, malformed_body=malformed_body)

    with pytest.raises(expected_cls) as excinfo:
        await service.list_tags(_REPO)

    err = excinfo.value
    assert err.code == expected_code
    assert not isinstance(err, InfrastructureUnreachableError), "GHCR must never raise Unreachable (row 91)"
    assert err.provider == "ghcr"
    assert err.command
    assert isinstance(err.detail, dict)


async def test_rate_limit_carries_retry_after():
    backend = FakeGhcrBackend()
    service = _service(backend, Fault.RATE_LIMIT)

    with pytest.raises(TransientError) as excinfo:
        await service.list_tags(_REPO)

    assert excinfo.value.retry_after == 5.0
