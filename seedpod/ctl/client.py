"""``SeedpodClient`` -- the pure HTTP client ``seedpodctl`` drives (DR-0021
§0c/point 3). Speaks ONLY ``httpx`` over the same authenticated
``/api/*``/``/health*`` surface the SPA and the acceptance gate already hit
(``seedpod/api/routers/*.py``) -- no direct DB/filesystem access, no import of
``seedpod.data``/``seedpod.app.services``/``seedpod.services.crypto``/
``sqlalchemy`` (this package's own ``__init__.py`` docstring; the trust
boundary DR-0021 makes structural).

Salvaged HTTP-client *shape* (base_url + ``Bearer`` header, one ``_request``
choke point, 401/403/404 error mapping) from
``reference-code/seedpod/seedpod/seedpodctl.archived/seedpodctl/client.py``
(the whole module -- its ``SeedpodClient.__init__``/``_request``), rewired onto
v2's actual response shapes: v1's bare-array collection responses become v2's
DR-0017 ``{<resource>: [...]}`` envelopes (``seedpod/api/routers/*.py``'s own
module docstrings), and every permission scope/route path below is read from
the REAL committed router, not guessed or carried over from v1's route table.

**Testability (this round's brief):** every constructor accepts an injectable
``httpx.AsyncClient`` (or a raw ``transport``) so tests drive this client over
an ``httpx.ASGITransport`` bound to a real ``build_app().api`` -- no live
server, no ``Mock``/``patch`` anywhere (``httpx.ASGITransport`` is an
``AsyncBaseTransport``-only implementation, hence this client is async
end-to-end; ``seedpod/ctl/cli.py``'s ``main()`` is the one place that drives
it via ``asyncio.run``).
"""

from __future__ import annotations

from typing import Any

import httpx

__all__ = [
    "SeedpodClient",
    "SeedpodApiError",
    "AuthenticationError",
    "PermissionDeniedError",
    "NotFoundError",
    "ApiError",
    "ConnectionFailedError",
]


class SeedpodApiError(Exception):
    """Base for every error this client raises -- ``seedpod/ctl/cli.py``'s
    ``main()`` catches this ONE type (plus its subclasses) at the top level
    and turns it into a clean stderr message, never a stack trace."""


class ConnectionFailedError(SeedpodApiError):
    """The API base URL could not be reached at all (DNS/connection-refused/
    timeout) -- distinct from a real HTTP error response, which the server
    never got a chance to send."""


class AuthenticationError(SeedpodApiError):
    """401 -- missing/invalid/expired API key (``seedpod/api/auth.py``'s
    ``get_current_api_key``)."""


class PermissionDeniedError(SeedpodApiError):
    """403 -- a valid key lacking the required scope
    (``seedpod/api/auth.py``'s ``require_permission``)."""


class NotFoundError(SeedpodApiError):
    """404 -- no such resource."""


class ApiError(SeedpodApiError):
    """Any other 4xx/5xx, carrying the server's own ``detail`` message and
    status code."""

    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(f"HTTP {status_code}: {detail}")
        self.status_code = status_code
        self.detail = detail


def _clean(params: dict[str, Any] | None) -> dict[str, Any] | None:
    """Drop ``None`` entries -- ``httpx`` otherwise serializes ``None`` as the
    literal empty string (``?environment=``), which is a PRESENT-but-empty
    query param to FastAPI, not the "omitted, use the server default" this
    client always means by ``None``."""
    if params is None:
        return None
    return {k: v for k, v in params.items() if v is not None}


def _detail(response: httpx.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return response.text or response.reason_phrase
    if isinstance(body, dict) and "detail" in body:
        return str(body["detail"])
    return str(body)


class SeedpodClient:
    """Thin async HTTP client. Construct with either ``api_key``/``base_url``
    (the everyday CLI path) or a pre-built ``client``/``transport`` (the test
    seam)."""

    def __init__(
        self,
        *,
        base_url: str = "",
        api_key: str | None = None,
        client: httpx.AsyncClient | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        timeout: float = 30.0,
    ) -> None:
        if client is not None:
            self._client = client
            self._owns_client = False
        else:
            headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
            self._client = httpx.AsyncClient(
                base_url=base_url, transport=transport, timeout=timeout, headers=headers
            )
            self._owns_client = True

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> SeedpodClient:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.aclose()

    # -- the one choke point every method below funnels through -------------

    async def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: dict[str, Any] | None = None,
    ) -> Any:
        try:
            response = await self._client.request(method, path, params=_clean(params), json=json)
        except httpx.HTTPError as exc:
            raise ConnectionFailedError(f"could not reach {path!r}: {exc}") from exc

        if response.status_code == 401:
            raise AuthenticationError(_detail(response))
        if response.status_code == 403:
            raise PermissionDeniedError(_detail(response))
        if response.status_code == 404:
            raise NotFoundError(_detail(response))
        if response.status_code >= 400:
            raise ApiError(response.status_code, _detail(response))
        if response.status_code == 204 or not response.content:
            return None
        return response.json()

    # -- health (seedpod/api/routers/health.py) ------------------------------

    async def health(self) -> Any:
        return await self._request("GET", "/api/health")

    async def health_detailed(self) -> Any:
        return await self._request("GET", "/api/health/detailed")

    # -- keys (seedpod/api/routers/keys.py) ----------------------------------

    async def list_keys(
        self, *, active_only: bool = False, username: str | None = None, environment: str | None = None
    ) -> Any:
        params = {"active_only": active_only, "username": username, "environment": environment}
        return await self._request("GET", "/api/keys", params=params)

    async def get_key(self, key_id: int) -> Any:
        return await self._request("GET", f"/api/keys/{key_id}")

    async def create_key(
        self,
        *,
        username: str,
        environment: str | None = None,
        permissions: list[str] | None = None,
        expires_hours: float | None = None,
        description: str | None = None,
    ) -> Any:
        body = {
            "username": username,
            "environment": environment,
            "permissions": permissions or [],
            "expires_hours": expires_hours,
            "description": description,
        }
        return await self._request("POST", "/api/keys", json=body)

    async def patch_key(
        self, key_id: int, *, description: str | None = None, expires_at: str | None = None
    ) -> Any:
        body = {"description": description, "expires_at": expires_at}
        return await self._request("PATCH", f"/api/keys/{key_id}", json=body)

    async def delete_key(self, key_id: int) -> Any:
        return await self._request("DELETE", f"/api/keys/{key_id}")

    # -- secrets (seedpod/api/routers/secrets.py) ----------------------------

    async def list_secrets(self, *, environment: str | None = None) -> Any:
        return await self._request("GET", "/api/secrets", params={"environment": environment})

    async def create_secret(self, *, environment: str, key_name: str, value: str) -> Any:
        body = {"environment": environment, "key_name": key_name, "value": value}
        return await self._request("POST", "/api/secrets", json=body)

    async def reveal_secret(self, environment: str, key_name: str) -> Any:
        return await self._request("GET", f"/api/secrets/{environment}/{key_name}/reveal")

    async def delete_secret(self, environment: str, key_name: str) -> Any:
        return await self._request("DELETE", f"/api/secrets/{environment}/{key_name}")

    # -- clusters (seedpod/api/routers/clusters.py) --------------------------

    async def list_clusters(self, *, show_destroyed: bool = False, status: str | None = None) -> Any:
        params = {"show_destroyed": show_destroyed, "status": status}
        return await self._request("GET", "/api/clusters", params=params)

    async def get_cluster(self, cluster_id: str) -> Any:
        return await self._request("GET", f"/api/clusters/{cluster_id}")

    async def extend_cluster(self, cluster_id: str, *, ttl_hours: float) -> Any:
        return await self._request(
            "POST", f"/api/clusters/{cluster_id}/extend", json={"ttl_hours": ttl_hours}
        )

    async def rehabilitate_cluster(self, cluster_id: str) -> Any:
        return await self._request("POST", f"/api/clusters/{cluster_id}/rehabilitate")

    async def destroy_cluster(
        self, cluster_id: str, *, force: bool = False, snapshot_before_destroy: bool = False
    ) -> Any:
        params = {"force": force, "snapshot_before_destroy": snapshot_before_destroy}
        return await self._request("DELETE", f"/api/clusters/{cluster_id}", params=params)

    async def cluster_pods(self, cluster_id: str, *, namespace: str | None = None) -> Any:
        return await self._request(
            "GET", f"/api/clusters/{cluster_id}/pods", params={"namespace": namespace}
        )

    async def cluster_pod_logs(
        self,
        cluster_id: str,
        namespace: str,
        pod_name: str,
        *,
        container: str | None = None,
        tail_lines: int = 100,
        previous: bool = False,
    ) -> Any:
        params = {"container": container, "tail_lines": tail_lines, "previous": previous}
        return await self._request(
            "GET", f"/api/clusters/{cluster_id}/pods/{namespace}/{pod_name}/logs", params=params
        )

    async def cluster_deployments(self, cluster_id: str) -> Any:
        return await self._request("GET", f"/api/clusters/{cluster_id}/deployments")

    async def cluster_audit(self, cluster_id: str, *, limit: int = 50) -> Any:
        return await self._request(
            "GET", f"/api/clusters/{cluster_id}/audit", params={"limit": limit}
        )

    # -- deployments (seedpod/api/routers/deployments.py) --------------------

    async def list_deployments(
        self, *, cluster_id: str | None = None, show_history: bool = False
    ) -> Any:
        params = {"cluster_id": cluster_id, "show_history": show_history}
        return await self._request("GET", "/api/deployments", params=params)

    async def get_deployment(self, deployment_id: str) -> Any:
        return await self._request("GET", f"/api/deployments/{deployment_id}")

    async def redeploy_deployment(self, deployment_id: str) -> Any:
        return await self._request("POST", f"/api/deployments/{deployment_id}/redeploy")

    async def retrigger_deployment(self, deployment_id: str) -> Any:
        return await self._request("POST", f"/api/deployments/{deployment_id}/retrigger")

    async def cancel_deployment(self, deployment_id: str, *, reason: str = "") -> Any:
        return await self._request(
            "POST", f"/api/deployments/{deployment_id}/cancel", json={"reason": reason}
        )

    async def deploy(self, *, repo: str, branch: str, image: str, commit: str, tag: str | None = None) -> Any:
        """``POST /api/version-update`` -- the webhook-shaped deploy trigger."""
        body = {"repo": repo, "branch": branch, "image": image, "commit": commit, "tag": tag}
        return await self._request("POST", "/api/version-update", json=body)

    # -- snapshots (seedpod/api/routers/snapshots.py) ------------------------

    async def list_snapshots(self, *, branch: str | None = None, profile: str | None = None) -> Any:
        return await self._request(
            "GET", "/api/snapshots", params={"branch": branch, "profile": profile}
        )

    async def get_snapshot(self, snapshot_id: str) -> Any:
        return await self._request("GET", f"/api/snapshots/{snapshot_id}")

    async def create_snapshot(self, *, cluster_id: str, name: str, description: str | None = None) -> Any:
        body = {"cluster_id": cluster_id, "name": name, "description": description}
        return await self._request("POST", "/api/snapshots", json=body)

    async def restore_snapshot(
        self,
        snapshot_id: str,
        *,
        cluster_id: str,
        services: list[str] | None = None,
        run_migrations: bool = True,
    ) -> Any:
        body = {"cluster_id": cluster_id, "services": services, "run_migrations": run_migrations}
        return await self._request("POST", f"/api/snapshots/{snapshot_id}/restore", json=body)

    async def delete_snapshot(self, snapshot_id: str) -> Any:
        return await self._request("DELETE", f"/api/snapshots/{snapshot_id}")

    async def restore_history(self, cluster_id: str) -> Any:
        return await self._request("GET", f"/api/snapshots/clusters/{cluster_id}/restore-history")

    # -- presets (seedpod/api/routers/presets.py) ----------------------------

    async def list_presets(self, *, profile: str | None = None) -> Any:
        return await self._request("GET", "/api/presets", params={"profile": profile})

    async def get_preset(self, preset_id: str) -> Any:
        return await self._request("GET", f"/api/presets/{preset_id}")

    async def create_preset(
        self,
        *,
        name: str,
        profile_name: str,
        description: str | None = None,
        service_overrides: dict[str, Any] | None = None,
        default_branch: str | None = None,
        default_ttl_hours: int | None = None,
        default_provider: str | None = None,  # DR-0046
    ) -> Any:
        body = {
            "name": name,
            "description": description,
            "profile_name": profile_name,
            "service_overrides": service_overrides,
            "default_branch": default_branch,
            "default_ttl_hours": default_ttl_hours,
            "default_provider": default_provider,
        }
        return await self._request("POST", "/api/presets", json=body)

    async def update_preset(
        self,
        preset_id: str,
        *,
        name: str | None = None,
        description: str | None = None,
        profile_name: str | None = None,
        service_overrides: dict[str, Any] | None = None,
        default_branch: str | None = None,
        default_ttl_hours: int | None = None,
        default_provider: str | None = None,  # DR-0046
    ) -> Any:
        body = {
            "name": name,
            "description": description,
            "profile_name": profile_name,
            "service_overrides": service_overrides,
            "default_branch": default_branch,
            "default_ttl_hours": default_ttl_hours,
            "default_provider": default_provider,
        }
        return await self._request("PUT", f"/api/presets/{preset_id}", json=body)

    async def delete_preset(self, preset_id: str) -> Any:
        return await self._request("DELETE", f"/api/presets/{preset_id}")

    async def deploy_preset(
        self,
        preset_id: str,
        *,
        branch: str | None = None,
        service_overrides: dict[str, Any] | None = None,
        provider_override: str | None = None,
        ttl_hours: float | None = None,
        cluster_name: str | None = None,
        data_initialization: dict[str, Any] | None = None,
    ) -> Any:
        body = {
            "branch": branch,
            "service_overrides": service_overrides,
            "provider_override": provider_override,
            "ttl_hours": ttl_hours,
            "cluster_name": cluster_name,
            "data_initialization": data_initialization,
        }
        return await self._request("POST", f"/api/presets/{preset_id}/deploy", json=body)

    # -- workflows / timers (seedpod/api/routers/{workflows,timers}.py) ------

    async def list_workflows(self) -> Any:
        return await self._request("GET", "/api/workflows")

    async def list_timers(self) -> Any:
        return await self._request("GET", "/api/timers")

    # -- config (seedpod/api/routers/config.py) ------------------------------

    async def config_overview(self) -> Any:
        return await self._request("GET", "/api/config/overview")

    async def config_rules(self) -> Any:
        return await self._request("GET", "/api/config/rules")

    async def config_deployment_profiles(self, profile_name: str | None = None) -> Any:
        if profile_name is not None:
            return await self._request("GET", f"/api/config/deployment-profiles/{profile_name}")
        return await self._request("GET", "/api/config/deployment-profiles")

    async def config_resolution_strategies(self, strategy_name: str | None = None) -> Any:
        if strategy_name is not None:
            return await self._request("GET", f"/api/config/resolution-strategies/{strategy_name}")
        return await self._request("GET", "/api/config/resolution-strategies")

    async def config_providers(self) -> Any:
        return await self._request("GET", "/api/config/providers")
