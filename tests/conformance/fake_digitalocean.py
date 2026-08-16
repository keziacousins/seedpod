"""tests/conformance/fake_digitalocean.py — a typed FAKE TRANSPORT simulating enough of the
DigitalOcean REST API v2 (droplet lifecycle, SSH keys, project resource assignment) for
``seedpod.providers.digitalocean.DigitalOceanProvider`` conformance testing (Seam C §5.6).

``FakeDigitalOceanBackend`` is the in-memory "cloud": a plain mutable store of droplets/ssh-keys
mined from real DO API response shapes. ``FakeDigitalOceanTransport`` is an
``httpx.AsyncBaseTransport`` — installed into a real ``httpx.AsyncClient(transport=...)``, so
fault injection happens at the actual transport seam the provider talks to, never
``Mock``/``patch`` (CLAUDE.md).
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field

import httpx

from tests.conformance.harness import Fault

__all__ = ["FakeDigitalOceanBackend", "FakeDigitalOceanTransport"]


@dataclass
class FakeDigitalOceanBackend:
    """The in-memory DigitalOcean account. Droplet ids are strings (matching the provider's
    ``resource_ids["droplet_id"]`` convention) even though the real API returns integers —
    the fake stores them the same way the real JSON would round-trip.
    """

    droplets: dict[str, dict] = field(default_factory=dict)
    ssh_keys: list[dict] = field(default_factory=lambda: [{"id": 501, "name": "exampleco-testing"}])
    project_resources: dict[str, set[str]] = field(default_factory=dict)
    vpcs: dict[str, dict] = field(default_factory=dict)
    firewalls: dict[str, dict] = field(default_factory=dict)
    call_count: int = 0
    _next_id: int = 10_000
    _next_vpc_id: int = 20_000
    _next_firewall_id: int = 30_000

    def create_vpc(self, payload: dict) -> dict:
        self._next_vpc_id += 1
        vpc_id = str(self._next_vpc_id)
        vpc = {"id": vpc_id, "name": payload["name"], "region": payload["region"], "ip_range": payload.get("ip_range")}
        self.vpcs[vpc_id] = vpc
        return vpc

    def create_firewall(self, payload: dict) -> dict:
        self._next_firewall_id += 1
        firewall_id = str(self._next_firewall_id)
        firewall = {
            "id": firewall_id,
            "name": payload["name"],
            "inbound_rules": payload.get("inbound_rules", []),
            "outbound_rules": payload.get("outbound_rules", []),
            "droplet_ids": [],
        }
        self.firewalls[firewall_id] = firewall
        return firewall

    def create_droplet(self, payload: dict) -> dict:
        self._next_id += 1
        droplet_id = self._next_id
        droplet = {
            "id": droplet_id,
            "name": payload.get("name", f"droplet-{droplet_id}"),
            "status": "new",
            "region": {"slug": payload.get("region", "ams3")},
            "size_slug": payload.get("size", "s-1vcpu-1gb"),
            "image": {"slug": payload.get("image", "ubuntu-22-04-x64")},
            "tags": list(payload.get("tags", [])),
            "networks": {"v4": [], "v6": []},
            "created_at": "2026-07-14T00:00:00Z",
            "vpc_uuid": payload.get("vpc_uuid"),
        }
        self.droplets[str(droplet_id)] = droplet
        return droplet

    def seed_droplet(self, *, tags: list[str], status: str = "active", ip: str = "203.0.113.9") -> str:
        """Test setup helper (not part of the DO API surface): directly inserts a droplet,
        bypassing ``create_droplet``'s id sequencing quirks, for harness pre-seeding."""
        droplet = self.create_droplet({"name": "seed", "tags": tags})
        if status == "active":
            self.activate(str(droplet["id"]), ip=ip)
        else:
            droplet["status"] = status
        return str(droplet["id"])

    def activate(self, droplet_id: str, *, ip: str = "203.0.113.9") -> None:
        """Flips a droplet to DO's 'active' status with a public IPv4 — the real API takes
        time to reach this state; the fake lets tests reach it instantly. Models v1's 30s
        post-active warmup only as a *state transition*, never a sleep (Seam C §5.4)."""
        droplet = self.droplets[droplet_id]
        droplet["status"] = "active"
        droplet["networks"] = {"v4": [{"ip_address": ip, "type": "public"}], "v6": []}

    def mark_stuck_active(self, droplet_id: str) -> None:
        self.droplets[droplet_id]["status"] = "active"

    def mark_destroying(self, droplet_id: str) -> None:
        self.droplets[droplet_id]["status"] = "archive"

    def list_by_tag(self, tag: str) -> list[dict]:
        return [d for d in self.droplets.values() if tag in d.get("tags", [])]

    def destroy_droplet(self, droplet_id: str) -> bool:
        return self.droplets.pop(droplet_id, None) is not None


class FakeDigitalOceanTransport(httpx.AsyncBaseTransport):
    """Routes ``httpx.Request``s against a ``FakeDigitalOceanBackend`` and applies ``Fault``
    behavior. Installed via ``httpx.AsyncClient(transport=FakeDigitalOceanTransport(...))``.
    """

    def __init__(self, backend: FakeDigitalOceanBackend, faults: frozenset[Fault]) -> None:
        self.backend = backend
        self.faults = faults
        self._transient_once_consumed = False

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.backend.call_count += 1

        if Fault.UNREACHABLE in self.faults:
            # Row 9: a timed-out DO API call ⇒ Unreachable/API_TIMEOUT. ConnectTimeout is a
            # TimeoutException, matching v1's `_run_sync` -> asyncio.wait_for TimeoutError.
            raise httpx.ConnectTimeout("simulated DO API timeout", request=request)

        if Fault.AUTH in self.faults:
            return self._json(401, {"id": "unauthorized", "message": "Unable to authenticate you."})

        if Fault.TRANSIENT_ONCE in self.faults and not self._transient_once_consumed:
            self._transient_once_consumed = True
            return self._json(500, {"id": "server_error", "message": "temporary failure, please retry"})

        if Fault.RATE_LIMIT in self.faults:
            return self._json(
                429,
                {"id": "too_many_requests", "message": "API Rate limit exceeded"},
                headers={"Retry-After": "2"},
            )

        path = request.url.path
        assert path.startswith("/v2/"), f"fake DO transport only routes /v2 paths, got {path}"
        route = path[len("/v2") :]
        params = request.url.params

        if Fault.DIE_MID_CREATE in self.faults and route.startswith("/projects/") and route.endswith("/resources"):
            # Simulates connectivity lost *after* the droplet already exists server-side
            # (Progress(RESOURCE_ALLOCATED) has already been emitted by the time this call
            # happens) — the C1 mid-create-death window, conformance C-09.
            raise httpx.ConnectError("simulated connection reset mid-create", request=request)

        if request.method == "GET" and route == "/account":
            return self._json(200, {"account": {"status": "active"}})

        if request.method == "GET" and route == "/account/keys":
            if Fault.MISSING_SOURCE in self.faults:
                return self._json(200, {"ssh_keys": []})
            return self._json(200, {"ssh_keys": list(self.backend.ssh_keys)})

        if request.method == "POST" and route == "/droplets":
            payload = json.loads(request.content or b"{}")
            droplet = self.backend.create_droplet(payload)
            return self._json(202, {"droplet": droplet})

        if request.method == "GET" and route == "/droplets" and "tag_name" in params:
            return self._json(200, {"droplets": self.backend.list_by_tag(params["tag_name"])})

        if request.method == "GET" and route.startswith("/droplets/"):
            droplet_id = route.rsplit("/", 1)[-1]
            droplet = self.backend.droplets.get(droplet_id)
            if droplet is None:
                return self._json(404, {"id": "not_found", "message": "The resource you were accessing could not be found."})
            return self._json(200, {"droplet": droplet})

        if request.method == "DELETE" and route.startswith("/droplets/"):
            droplet_id = route.rsplit("/", 1)[-1]
            existed = self.backend.destroy_droplet(droplet_id)
            return httpx.Response(204 if existed else 404, request=request)

        if request.method == "POST" and route.startswith("/projects/") and route.endswith("/resources"):
            project_id = route.split("/")[2]
            payload = json.loads(request.content or b"{}")
            self.backend.project_resources.setdefault(project_id, set()).update(payload.get("resources", []))
            return self._json(200, {"resources": list(payload.get("resources", []))})

        if request.method == "GET" and route == "/vpcs":
            return self._json(200, {"vpcs": list(self.backend.vpcs.values())})

        if request.method == "POST" and route == "/vpcs":
            payload = json.loads(request.content or b"{}")
            vpc = self.backend.create_vpc(payload)
            return self._json(201, {"vpc": vpc})

        if request.method == "GET" and route == "/firewalls":
            return self._json(200, {"firewalls": list(self.backend.firewalls.values())})

        if request.method == "POST" and route == "/firewalls":
            payload = json.loads(request.content or b"{}")
            firewall = self.backend.create_firewall(payload)
            return self._json(202, {"firewall": firewall})

        if request.method == "POST" and route.startswith("/firewalls/") and route.endswith("/droplets"):
            firewall_id = route.split("/")[2]
            firewall = self.backend.firewalls.get(firewall_id)
            if firewall is None:
                return self._json(404, {"id": "not_found", "message": "The resource you were accessing could not be found."})
            payload = json.loads(request.content or b"{}")
            firewall["droplet_ids"] = sorted(set(firewall["droplet_ids"]) | set(payload.get("droplet_ids", [])))
            return httpx.Response(204, request=request)

        return self._json(404, {"id": "not_found", "message": f"fake transport: no route for {request.method} {route}"})

    @staticmethod
    def _json(status: int, body: dict, *, headers: dict[str, str] | None = None) -> httpx.Response:
        return httpx.Response(status, json=body, headers=headers)
