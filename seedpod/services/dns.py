"""seedpod/services/dns.py — ``DnsService``, a supporting service (NOT a Provider —
docs/design/seam-c-provider.md §5.4 "Supporting services", coherence-review.md §2 type
glossary: ``services/dns.py``).

Talks to the Cloudflare API v4 (``/zones``, ``/zones/{id}/dns_records``) over an
**injected** ``httpx.AsyncClient`` (§5.4's construction contract). Same three-leaf error
taxonomy as providers, but per decision-table rows 36-38 (§5.1): **Cloudflare never
raises ``InfrastructureUnreachableError``** — connectivity failures classify as
``TransientError``. One bounded attempt per call — no internal retry/sleep (H4-H6).

Salvaged from ``reference-code/seedpod/seedpod/providers/cloudflare_dns.py``:

- ``CloudflareDNSProvider._get_zone_id``/``get_record``/``create_record`` (lines
  68-235) -> ``upsert_record`` below: GET the zone id, GET the existing record by name,
  PUT if it exists else POST — same three-call shape, folded into one method.
- ``{name}.{zone}`` suffixing, skipped if ``name`` already ends with ``zone`` (v1 lines
  118, 169) -> ``_full_name`` below, verbatim.
- ``delete_record`` (lines 237-293): 404 -> ``existed=False`` (v1 line 275-280,
  ``return False`` — never an exception), any other non-2xx -> classified error.
- ``create_cluster_dns_record`` (lines 308-354): ``subdomain_pattern.format(
  cluster_slug=...)`` with default ``"{cluster_slug}"`` (v1 line 335) is folded
  directly into ``upsert_record`` here rather than kept as a separate wrapper function —
  a service, unlike a bare provider, is allowed to own this one call-site convenience.

Deliberate v2 change from v1 (a real correctness improvement, not a pinned bug):
``upsert_record`` returns ``created: bool`` (``True`` iff the POST branch ran) so a
caller — specifically ``providers/compensation.py``'s undo-on-create-failure path per
Seam C §5.5's table — can delete the record on rollback **iff it created it**, never
deleting (and thereby destroying) a record that merely got its IP updated. v1 had no
such distinction; this is Seam C's documented "P2 graft — improves on v1".

Deliberately NOT carried forward: v1's per-instance ``self._zone_id_cache`` (reference-
code .../cloudflare_dns.py:60, 78-79, 99). A supporting service still has to behave the
same on a fresh instance as one that has already served requests (Seam C §5.4's
statelessness discipline, C-03's spirit even though services aren't Providers) — a
cached zone id would let a stale/renamed zone silently keep resolving after the real
zone changed, and would make conformance fault injection (a zone lookup that should
fail on retry) order-dependent. Costs one extra GET per call; not worth the risk.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

import httpx

from seedpod.core.errors import ErrorCode, PermanentError, TransientError
from seedpod.providers.classify import classify_http

__all__ = ["DnsConfig", "DnsRecordUpserted", "DnsDeleted", "DnsService"]

_HOST = "api.cloudflare.com"


@dataclass(frozen=True)
class DnsConfig:
    """IO-free construction data. Loaded by the composition root from
    ``config/providers/cloudflare.yml`` (or equivalent) + a ``CLOUDFLARE_API_TOKEN``
    secret; this module never reads a file or an environment variable itself."""

    api_token: str
    base_url: str = "https://api.cloudflare.com/client/v4"
    timeout_s: float = 30.0


@dataclass(frozen=True)
class DnsRecordUpserted:
    """§5.4's normative return shape for ``upsert_record``. ``created`` distinguishes
    "we just made this record" (undo-on-create-failure should delete it) from "we only
    updated its IP" (undo-on-create-failure must NOT delete a record that pre-existed
    the run) — see the module docstring's "P2 graft"."""

    record_id: str
    fqdn: str
    zone: str
    created: bool


@dataclass(frozen=True)
class DnsDeleted:
    """Row 38: idempotent delete. ``existed=False`` is a typed Result, never an
    exception — mirrors ``DestroyOutcome``'s absence-is-data discipline for the
    machine-provider destroy vocabulary (§5.3)."""

    existed: bool


def _full_name(name: str, zone: str) -> str:
    # v1 lines 118, 169, verbatim: don't double-suffix a name that's already fully
    # qualified.
    return name if name.endswith(zone) else f"{name}.{zone}"


class DnsService:
    """Stateless (§5.4's construction contract): no cache (see module docstring), no
    DB, one bounded attempt per HTTP call."""

    def __init__(self, config: DnsConfig, transport: httpx.AsyncClient) -> None:
        self.config = config
        self.transport = transport

    # ------------------------------------------------------------------
    # writes
    # ------------------------------------------------------------------

    async def upsert_record(
        self,
        *,
        zone: str,
        cluster_slug: str,
        ip: str,
        subdomain_pattern: str = "{cluster_slug}",
        ttl: int = 300,
        proxied: bool = False,
    ) -> DnsRecordUpserted:
        """GET zone id -> GET existing record by name -> PUT (update) or POST
        (create). Salvaged from ``CloudflareDNSProvider.create_record``
        (reference-code .../cloudflare_dns.py:147-235)."""
        name = subdomain_pattern.format(cluster_slug=cluster_slug)
        zone_id = await self._zone_id(zone, command="upsert_record")
        full_name = _full_name(name, zone)

        existing = await self._find_record(zone_id, full_name, command="upsert_record")
        payload = {"type": "A", "name": full_name, "content": ip, "ttl": ttl, "proxied": proxied}

        if existing is not None:
            body = await self._request(
                "PUT",
                f"/zones/{zone_id}/dns_records/{existing['id']}",
                json_body=payload,
                command="upsert_record",
            )
            record = self._unwrap_result(body, command="upsert_record")
            return DnsRecordUpserted(record_id=str(record["id"]), fqdn=str(record["name"]), zone=zone, created=False)

        body = await self._request("POST", f"/zones/{zone_id}/dns_records", json_body=payload, command="upsert_record")
        record = self._unwrap_result(body, command="upsert_record")
        return DnsRecordUpserted(record_id=str(record["id"]), fqdn=str(record["name"]), zone=zone, created=True)

    async def delete_record(self, *, zone: str, record_id: str) -> DnsDeleted:
        """Row 38: 404 -> ``DnsDeleted(existed=False)`` (idempotent, never an
        exception). Salvaged from ``CloudflareDNSProvider.delete_record``
        (reference-code .../cloudflare_dns.py:237-293) — the record_id-known path only;
        the id is persisted by the caller at create time (module docstring), so the
        name-lookup fallback v1 offered for a missing id is not carried forward."""
        zone_id = await self._zone_id(zone, command="delete_record")
        response = await self._raw_request(
            "DELETE", f"/zones/{zone_id}/dns_records/{record_id}", json_body=None, command="delete_record"
        )
        if response.status_code == 404:
            return DnsDeleted(existed=False)
        if not response.is_success:
            raise classify_http(
                provider="cloudflare",
                command="delete_record",
                host=_HOST,
                status=response.status_code,
                observing_infra=False,
            )
        body = self._parse_json(response, command="delete_record")
        self._check_success(body, command="delete_record")
        return DnsDeleted(existed=True)

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------

    async def _zone_id(self, zone: str, *, command: str) -> str:
        body = await self._request("GET", "/zones", params={"name": zone}, command=command)
        zones = body.get("result") if isinstance(body, Mapping) else None
        if not zones:
            # v1 line 96, ValueError -> here, the typed row-37 NOT_FOUND classification.
            raise PermanentError(
                f"cloudflare.{command}: zone '{zone}' not found in Cloudflare account",
                code=ErrorCode.NOT_FOUND,
                provider="cloudflare",
                command=command,
                detail={"zone": zone},
            )
        return str(zones[0]["id"])

    async def _find_record(self, zone_id: str, full_name: str, *, command: str) -> Mapping[str, object] | None:
        body = await self._request(
            "GET", f"/zones/{zone_id}/dns_records", params={"name": full_name, "type": "A"}, command=command
        )
        records = body.get("result") if isinstance(body, Mapping) else None
        if not records:
            return None
        return records[0]

    def _unwrap_result(self, body: object, *, command: str) -> Mapping[str, object]:
        result = body.get("result") if isinstance(body, Mapping) else None
        if not isinstance(result, Mapping):
            raise classify_http(
                provider="cloudflare", command=command, host=_HOST, status=200, malformed_body=True, observing_infra=False
            )
        return result

    def _check_success(self, body: object, *, command: str) -> None:
        # Row 37: Cloudflare's own error-envelope quirk — a call can come back HTTP 200
        # with `"success": false` and a real error in `errors` (v1 lines 90-92,
        # 129-131, 218-220). No further string-sniffing of `errors[].message`: this is
        # deliberately routed through the generic INVALID_INPUT cell rather than
        # inventing per-error-code mapping the seam doesn't specify.
        if isinstance(body, Mapping) and body.get("success") is False:
            raise PermanentError(
                f"cloudflare.{command}: API reported failure: {body.get('errors')}",
                code=ErrorCode.INVALID_INPUT,
                provider="cloudflare",
                command=command,
                detail={"errors": str(body.get("errors"))},
            )

    async def _request(
        self, method: str, path: str, *, json_body: object = None, params: Mapping[str, object] | None = None, command: str
    ) -> Mapping[str, object]:
        response = await self._raw_request(method, path, json_body=json_body, params=params, command=command)
        if not response.is_success:
            raise classify_http(
                provider="cloudflare", command=command, host=_HOST, status=response.status_code, observing_infra=False
            )
        body = self._parse_json(response, command=command)
        self._check_success(body, command=command)
        return body if isinstance(body, Mapping) else {}

    async def _raw_request(
        self, method: str, path: str, *, json_body: object, params: Mapping[str, object] | None = None, command: str
    ) -> httpx.Response:
        url = f"{self.config.base_url}{path}"
        headers = {"Authorization": f"Bearer {self.config.api_token}", "Content-Type": "application/json"}
        try:
            return await self.transport.request(
                method, url, json=json_body, params=params, headers=headers, timeout=self.config.timeout_s
            )
        except httpx.TimeoutException as e:
            # Row 36: conn error/timeout -> Transient/ENDPOINT_UNREACHABLE, never
            # Unreachable (Cloudflare is a supporting service, not managed infra).
            raise TransientError(
                f"cloudflare.{command}: timed out calling {path}",
                code=ErrorCode.API_TIMEOUT,
                provider="cloudflare",
                command=command,
                detail={"path": path},
            ) from e
        except httpx.TransportError as e:
            raise TransientError(
                f"cloudflare.{command}: could not reach {_HOST}: {e}",
                code=ErrorCode.ENDPOINT_UNREACHABLE,
                provider="cloudflare",
                command=command,
                detail={"path": path},
            ) from e

    def _parse_json(self, response: httpx.Response, *, command: str) -> object:
        if not response.content:
            raise classify_http(
                provider="cloudflare",
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
                provider="cloudflare",
                command=command,
                host=_HOST,
                status=response.status_code,
                malformed_body=True,
                observing_infra=False,
            ) from e
