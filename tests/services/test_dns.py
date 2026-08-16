"""tests/services/test_dns.py — golden tests for ``seedpod.services.dns.DnsService``'s
salvaged crown jewels (docs/design/seam-c-provider.md §5.4 "Supporting services"), plus
the conformance-suite "shapes" the seam says apply to services (⊂ of C-05/C-10/C-15/C-17
— §5.6's table) even though DNS/GHCR don't implement the ``Provider`` protocol and so
can't literally join ``tests/conformance``'s ``Harness``-parametrized suite. See
``tests/services/test_ghcr.py``'s module docstring for the same note.

No ``Mock``/``patch`` anywhere: fault injection sits at ``FakeCloudflareTransport``, the
actual ``httpx.AsyncBaseTransport`` seam ``DnsService`` talks to (CLAUDE.md).
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
from seedpod.services.dns import DnsConfig, DnsDeleted, DnsRecordUpserted, DnsService
from tests.conformance.harness import Fault
from tests.services.fake_cloudflare import FakeCloudflareBackend, FakeCloudflareTransport

pytestmark = pytest.mark.asyncio

_ZONE = "example.com"


def _service(backend: FakeCloudflareBackend, *faults: Fault, success_false: bool = False) -> DnsService:
    transport = httpx.AsyncClient(
        transport=FakeCloudflareTransport(backend, frozenset(faults), success_false=success_false)
    )
    config = DnsConfig(api_token="fake-token")  # pragma: allowlist secret
    return DnsService(config, transport)


def _seeded_backend() -> FakeCloudflareBackend:
    backend = FakeCloudflareBackend()
    backend.add_zone(_ZONE)
    return backend


# ============================================================================
# crown jewels: upsert (GET -> PUT-or-POST), {name}.{zone} suffixing, subdomain_pattern
# ============================================================================


async def test_upsert_creates_when_no_existing_record():
    backend = _seeded_backend()
    service = _service(backend)

    result = await service.upsert_record(zone=_ZONE, cluster_slug="my-cluster", ip="203.0.113.5")

    assert isinstance(result, DnsRecordUpserted)
    assert result.created is True
    assert result.zone == _ZONE
    assert result.fqdn == f"my-cluster.{_ZONE}"
    assert backend.dns_records[result.record_id]["content"] == "203.0.113.5"


async def test_upsert_updates_existing_record_created_false():
    """GET finds an existing record -> PUT, not POST; ``created=False`` so a caller's
    undo-on-failure never deletes a record that pre-existed the run (module docstring's
    'P2 graft')."""
    backend = _seeded_backend()
    zone_id = backend.zones[_ZONE]
    existing_id = backend.add_record(zone_id=zone_id, name=f"my-cluster.{_ZONE}", content="198.51.100.1")
    service = _service(backend)

    result = await service.upsert_record(zone=_ZONE, cluster_slug="my-cluster", ip="203.0.113.5")

    assert result.created is False
    assert result.record_id == existing_id
    assert backend.dns_records[existing_id]["content"] == "203.0.113.5", "PUT must have updated the IP"


async def test_subdomain_pattern_default_is_cluster_slug():
    backend = _seeded_backend()
    service = _service(backend)

    result = await service.upsert_record(zone=_ZONE, cluster_slug="ephemeral-42", ip="203.0.113.5")

    assert result.fqdn == f"ephemeral-42.{_ZONE}"


async def test_subdomain_pattern_custom():
    backend = _seeded_backend()
    service = _service(backend)

    result = await service.upsert_record(
        zone=_ZONE, cluster_slug="ephemeral-42", ip="203.0.113.5", subdomain_pattern="{cluster_slug}.cluster"
    )

    assert result.fqdn == f"ephemeral-42.cluster.{_ZONE}"


async def test_name_already_ending_in_zone_is_not_double_suffixed():
    """v1 lines 118/169: a name already ending in ``.{zone}`` is used verbatim."""
    backend = _seeded_backend()
    service = _service(backend)

    result = await service.upsert_record(
        zone=_ZONE, cluster_slug="x", ip="203.0.113.5", subdomain_pattern=f"already-fqdn.{_ZONE}"
    )

    assert result.fqdn == f"already-fqdn.{_ZONE}"
    assert not result.fqdn.endswith(f".{_ZONE}.{_ZONE}")


async def test_zone_not_found_is_permanent_not_found():
    """Row 37: missing zone -> Permanent/NOT_FOUND (v1 line 96's ValueError, typed)."""
    backend = FakeCloudflareBackend()  # no zones seeded
    service = _service(backend)

    with pytest.raises(PermanentError) as excinfo:
        await service.upsert_record(zone="nonexistent.example", cluster_slug="x", ip="203.0.113.5")

    assert excinfo.value.code == ErrorCode.NOT_FOUND


# ============================================================================
# C-05 / C-10 shape — absence is DATA, idempotent delete (row 38)
# ============================================================================


async def test_delete_record_404_is_existed_false_not_exception():
    backend = _seeded_backend()
    service = _service(backend)

    result = await service.delete_record(zone=_ZONE, record_id="never-existed")

    assert result == DnsDeleted(existed=False)


async def test_delete_record_success_existed_true():
    backend = _seeded_backend()
    zone_id = backend.zones[_ZONE]
    record_id = backend.add_record(zone_id=zone_id, name=f"my-cluster.{_ZONE}", content="203.0.113.5")
    service = _service(backend)

    result = await service.delete_record(zone=_ZONE, record_id=record_id)

    assert result == DnsDeleted(existed=True)
    assert record_id not in backend.dns_records


async def test_delete_record_twice_succeeds_twice():
    """C-10 shape: destroying an already-absent resource is idempotent success, not a
    second failure."""
    backend = _seeded_backend()
    zone_id = backend.zones[_ZONE]
    record_id = backend.add_record(zone_id=zone_id, name=f"my-cluster.{_ZONE}", content="203.0.113.5")
    service = _service(backend)

    first = await service.delete_record(zone=_ZONE, record_id=record_id)
    second = await service.delete_record(zone=_ZONE, record_id=record_id)

    assert first == DnsDeleted(existed=True)
    assert second == DnsDeleted(existed=False)


# ============================================================================
# C-15 shape — one bounded attempt, no internal retry/sleep (H4-H6)
# ============================================================================


async def test_single_attempt_no_internal_retry_then_succeeds_on_reinvocation():
    backend = _seeded_backend()
    service = _service(backend, Fault.TRANSIENT_ONCE)

    before = backend.call_count
    start = time.monotonic()
    with pytest.raises(TransientError):
        await service.upsert_record(zone=_ZONE, cluster_slug="x", ip="203.0.113.5")
    elapsed = time.monotonic() - start

    assert backend.call_count - before == 1, "exactly one transport attempt, no internal retry loop"
    assert elapsed < 2.0, f"{elapsed:.2f}s suggests an internal retry/sleep loop"

    # Same service instance now succeeds — the fault was single-shot.
    result = await service.upsert_record(zone=_ZONE, cluster_slug="x", ip="203.0.113.5")
    assert result.created is True


# ============================================================================
# C-17 shape — classification decision-table rows 36-38, Cloudflare never Unreachable
# ============================================================================

_CLASSIFICATION_CASES = [
    pytest.param((Fault.UNREACHABLE,), False, TransientError, ErrorCode.API_TIMEOUT, id="row36-timeout-transient"),
    pytest.param((Fault.AUTH,), False, PermanentError, ErrorCode.AUTH, id="row37-401-permanent-auth"),
    pytest.param((), True, PermanentError, ErrorCode.INVALID_INPUT, id="row37-success-false-permanent-invalid-input"),
    pytest.param((Fault.RATE_LIMIT,), False, TransientError, ErrorCode.RATE_LIMITED, id="row37-429-transient-rate-limited"),
]


@pytest.mark.parametrize("faults,success_false,expected_cls,expected_code", _CLASSIFICATION_CASES)
async def test_classification_table(faults, success_false, expected_cls, expected_code):
    backend = _seeded_backend()
    service = _service(backend, *faults, success_false=success_false)

    with pytest.raises(expected_cls) as excinfo:
        await service.upsert_record(zone=_ZONE, cluster_slug="x", ip="203.0.113.5")

    err = excinfo.value
    assert err.code == expected_code
    assert not isinstance(err, InfrastructureUnreachableError), "Cloudflare must never raise Unreachable (row 91)"
    assert err.provider == "cloudflare"
    assert err.command
    assert isinstance(err.detail, dict)
