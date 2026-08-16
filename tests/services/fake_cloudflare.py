"""tests/services/fake_cloudflare.py — a typed FAKE TRANSPORT simulating enough of the
Cloudflare API v4 (zone lookup, DNS record CRUD) for
``seedpod.services.dns.DnsService`` testing (mirrors
``tests/conformance/fake_digitalocean.py``'s pattern for the Pillar-3 provider suite).

``FakeCloudflareBackend`` is the in-memory "account": zones and DNS records, wrapped in
Cloudflare's real ``{"success": bool, "result": ..., "errors": [...]}`` envelope shape.
``FakeCloudflareTransport`` is an ``httpx.AsyncBaseTransport`` — installed into a real
``httpx.AsyncClient(transport=...)``, so fault injection happens at the actual transport
seam ``DnsService`` talks to, never ``Mock``/``patch`` (CLAUDE.md).

Reuses ``tests.conformance.harness.Fault`` for the fault vocabulary shared with the
six-provider conformance suite; ``success_false`` is a separate constructor toggle for
Cloudflare's own HTTP-200-but-``"success": false`` error envelope quirk (row 37), which
has no member in that shared enum.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import httpx

from tests.conformance.harness import Fault

__all__ = ["FakeCloudflareBackend", "FakeCloudflareTransport"]


@dataclass
class FakeCloudflareBackend:
    zones: dict[str, str] = field(default_factory=dict)  # zone name -> zone id
    dns_records: dict[str, dict] = field(default_factory=dict)  # record id -> record
    call_count: int = 0
    _next_id: int = 1000

    def add_zone(self, name: str) -> str:
        self._next_id += 1
        zone_id = f"zone-{self._next_id}"
        self.zones[name] = zone_id
        return zone_id

    def add_record(self, *, zone_id: str, name: str, content: str, ttl: int = 300, proxied: bool = False) -> str:
        self._next_id += 1
        record_id = f"rec-{self._next_id}"
        self.dns_records[record_id] = {
            "id": record_id,
            "zone_id": zone_id,
            "type": "A",
            "name": name,
            "content": content,
            "ttl": ttl,
            "proxied": proxied,
        }
        return record_id


class FakeCloudflareTransport(httpx.AsyncBaseTransport):
    def __init__(
        self,
        backend: FakeCloudflareBackend,
        faults: frozenset[Fault] = frozenset(),
        *,
        success_false: bool = False,
    ) -> None:
        self.backend = backend
        self.faults = faults
        self.success_false = success_false
        self._transient_once_consumed = False

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.backend.call_count += 1

        if Fault.UNREACHABLE in self.faults:
            raise httpx.ConnectTimeout("simulated Cloudflare API timeout", request=request)

        if Fault.AUTH in self.faults:
            return self._envelope(401, success=False, errors=[{"code": 9109, "message": "Invalid API Token"}])

        if Fault.RATE_LIMIT in self.faults:
            return self._envelope(429, success=False, errors=[{"code": 10013, "message": "rate limited"}])

        if Fault.TRANSIENT_ONCE in self.faults and not self._transient_once_consumed:
            self._transient_once_consumed = True
            return self._envelope(500, success=False, errors=[{"code": 0, "message": "internal error"}])

        if self.success_false:
            return self._envelope(200, success=False, errors=[{"code": 1000, "message": "invalid record"}])

        path = request.url.path
        params = request.url.params
        prefix = "/client/v4"
        assert path.startswith(prefix), f"fake Cloudflare transport only routes {prefix} paths, got {path}"
        route = path[len(prefix) :]

        if request.method == "GET" and route == "/zones":
            name = params.get("name")
            zone_id = self.backend.zones.get(name)
            results = [{"id": zone_id, "name": name}] if zone_id else []
            return self._envelope(200, success=True, result=results)

        if request.method == "GET" and route.endswith("/dns_records"):
            zone_id = route.split("/")[2]
            name = params.get("name")
            results = [r for r in self.backend.dns_records.values() if r["zone_id"] == zone_id and r["name"] == name]
            return self._envelope(200, success=True, result=results)

        if request.method == "POST" and route.endswith("/dns_records"):
            zone_id = route.split("/")[2]
            payload = json.loads(request.content or b"{}")
            record_id = self.backend.add_record(
                zone_id=zone_id,
                name=payload["name"],
                content=payload["content"],
                ttl=payload.get("ttl", 300),
                proxied=payload.get("proxied", False),
            )
            return self._envelope(200, success=True, result=self.backend.dns_records[record_id])

        if request.method == "PUT" and "/dns_records/" in route:
            record_id = route.rsplit("/", 1)[-1]
            record = self.backend.dns_records.get(record_id)
            if record is None:
                return self._envelope(404, success=False, errors=[{"code": 81044, "message": "record not found"}])
            payload = json.loads(request.content or b"{}")
            record.update({"content": payload.get("content", record["content"]), "ttl": payload.get("ttl", record["ttl"])})
            return self._envelope(200, success=True, result=record)

        if request.method == "DELETE" and "/dns_records/" in route:
            record_id = route.rsplit("/", 1)[-1]
            existed = self.backend.dns_records.pop(record_id, None) is not None
            if not existed:
                return self._envelope(404, success=False, errors=[{"code": 81044, "message": "record not found"}])
            return self._envelope(200, success=True, result={"id": record_id})

        return self._envelope(404, success=False, errors=[{"code": 0, "message": f"fake transport: no route for {request.method} {route}"}])

    @staticmethod
    def _envelope(status: int, *, success: bool, result: object = None, errors: list | None = None) -> httpx.Response:
        body = {"success": success, "result": result, "errors": errors or []}
        return httpx.Response(status, json=body)
