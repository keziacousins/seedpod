"""``seedpodctl`` -- the ``argparse``-based command-line front end for
``seedpod.ctl.client.SeedpodClient`` (docs/decisions/DR-0021 §0c/point 3).
Command groups mirror the API surface one-for-one (``keys``, ``secrets``,
``clusters``, ``deployments``, ``deploy``, ``snapshots``, ``presets``,
``workflows``, ``timers``, ``health``, ``config``) -- each subcommand is one
client call, printed as JSON.

Argparse (stdlib), NOT ``click`` (v1's archived ``seedpodctl`` used click --
this round's brief re-does the CLI layer in argparse rather than adding a new
dependency). Salvaged surface/behavior, re-pointed at HTTP:
``reference-code/seedpod/seedpod/seedpodctl.archived/seedpodctl/cli.py``'s
command groups (v1 spoke directly to a click-friendly SDK; this module speaks
only through ``SeedpodClient``) and ``reference-code/seedpod/seedpod_cli.py``'s
``list-keys``/``create-secret``/``list-secrets`` surface (v1's was direct-DB;
here it is ``client.list_keys()``/``client.create_secret()``/
``client.list_secrets()`` -- HTTP calls, DR-0021 §0c: "re-point v1's direct-DB
user commands at HTTP").

Credential/URL resolution order (DR-0021: "Bearer from ``SEEDPOD_API_KEY`` or
a config file, against the API base URL"): ``--api-key``/``--api-url`` flags,
then ``SEEDPOD_API_KEY``/``SEEDPOD_API_URL`` env vars, then an optional JSON
config file (``--config``, default ``~/.seedpodctl.json``, keys ``api_key``/
``api_url``) -- the exact file location/shape isn't pinned anywhere upstream,
so this module picks the simplest single-dotfile convention rather than
inventing a directory hierarchy. Missing token/url fails with a clear one-line
stderr message, never a stack trace.

Zero import-time side effects (CLAUDE.md / DR-0021): every environment read,
file read, and network call happens inside ``main()``/the ``_cmd_*``
functions, never at module scope.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import Any

from seedpod.ctl.client import (
    ApiError,
    AuthenticationError,
    ConnectionFailedError,
    NotFoundError,
    PermissionDeniedError,
    SeedpodApiError,
    SeedpodClient,
)

__all__ = ["main"]

_DEFAULT_API_URL = "http://localhost:8000"
_DEFAULT_CONFIG_PATH = Path.home() / ".seedpodctl.json"

CommandFunc = Callable[[SeedpodClient, argparse.Namespace], Awaitable[Any]]


# ---------------------------------------------------------------------------
# Credential / base-URL resolution
# ---------------------------------------------------------------------------


def _read_config_file(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _resolve_api_url(args: argparse.Namespace, config: dict[str, Any]) -> str:
    return args.api_url or os.environ.get("SEEDPOD_API_URL") or config.get("api_url") or _DEFAULT_API_URL


def _resolve_api_key(args: argparse.Namespace, config: dict[str, Any]) -> str | None:
    return args.api_key or os.environ.get("SEEDPOD_API_KEY") or config.get("api_key")


# ---------------------------------------------------------------------------
# keys
# ---------------------------------------------------------------------------


async def _keys_list(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.list_keys(
        active_only=args.active_only, username=args.username, environment=args.environment
    )


async def _keys_get(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.get_key(args.key_id)


async def _keys_create(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.create_key(
        username=args.username,
        environment=args.environment,
        permissions=args.permission,
        expires_hours=args.expires_hours,
        description=args.description,
    )


async def _keys_patch(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.patch_key(args.key_id, description=args.description, expires_at=args.expires_at)


async def _keys_delete(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.delete_key(args.key_id)


def _add_keys(subparsers: argparse._SubParsersAction) -> None:
    group = subparsers.add_parser("keys", help="manage API keys")
    actions = group.add_subparsers(dest="action", required=True)

    p = actions.add_parser("list", help="list API keys")
    p.add_argument("--active-only", action="store_true")
    p.add_argument("--username")
    p.add_argument("--environment")
    p.set_defaults(func=_keys_list)

    p = actions.add_parser("get", help="get one API key")
    p.add_argument("key_id", type=int)
    p.set_defaults(func=_keys_get)

    p = actions.add_parser("create", help="mint a new API key")
    p.add_argument("username")
    p.add_argument("--environment")
    p.add_argument(
        "--permission", action="append", default=[], help="grant a permission scope (repeatable)"
    )
    p.add_argument("--expires-hours", type=float)
    p.add_argument("--description")
    p.set_defaults(func=_keys_create)

    p = actions.add_parser("patch", help="update an API key's description/expiry")
    p.add_argument("key_id", type=int)
    p.add_argument("--description")
    p.add_argument("--expires-at", help="ISO-8601 timestamp")
    p.set_defaults(func=_keys_patch)

    p = actions.add_parser("delete", help="revoke an API key")
    p.add_argument("key_id", type=int)
    p.set_defaults(func=_keys_delete)


# ---------------------------------------------------------------------------
# secrets
# ---------------------------------------------------------------------------


async def _secrets_list(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.list_secrets(environment=args.environment)


async def _secrets_create(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.create_secret(environment=args.environment, key_name=args.key_name, value=args.value)


async def _secrets_reveal(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.reveal_secret(args.environment, args.key_name)


async def _secrets_delete(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.delete_secret(args.environment, args.key_name)


def _add_secrets(subparsers: argparse._SubParsersAction) -> None:
    group = subparsers.add_parser("secrets", help="manage per-environment secrets")
    actions = group.add_subparsers(dest="action", required=True)

    p = actions.add_parser("list", help="list secret metadata")
    p.add_argument("--environment")
    p.set_defaults(func=_secrets_list)

    p = actions.add_parser("create", help="create or update a secret")
    p.add_argument("environment")
    p.add_argument("key_name")
    p.add_argument("value")
    p.set_defaults(func=_secrets_create)

    p = actions.add_parser("reveal", help="reveal a secret's plaintext value")
    p.add_argument("environment")
    p.add_argument("key_name")
    p.set_defaults(func=_secrets_reveal)

    p = actions.add_parser("delete", help="delete a secret")
    p.add_argument("environment")
    p.add_argument("key_name")
    p.set_defaults(func=_secrets_delete)


# ---------------------------------------------------------------------------
# clusters
# ---------------------------------------------------------------------------


async def _clusters_list(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.list_clusters(show_destroyed=args.show_destroyed, status=args.status)


async def _clusters_get(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.get_cluster(args.cluster_id)


async def _clusters_extend(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.extend_cluster(args.cluster_id, ttl_hours=args.ttl_hours)


async def _clusters_rehabilitate(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.rehabilitate_cluster(args.cluster_id)


async def _clusters_destroy(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.destroy_cluster(
        args.cluster_id, force=args.force, snapshot_before_destroy=args.snapshot_before_destroy
    )


async def _clusters_pods(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.cluster_pods(args.cluster_id, namespace=args.namespace)


async def _clusters_logs(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.cluster_pod_logs(
        args.cluster_id,
        args.namespace,
        args.pod_name,
        container=args.container,
        tail_lines=args.tail_lines,
        previous=args.previous,
    )


async def _clusters_deployments(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.cluster_deployments(args.cluster_id)


async def _clusters_audit(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.cluster_audit(args.cluster_id, limit=args.limit)


def _add_clusters(subparsers: argparse._SubParsersAction) -> None:
    group = subparsers.add_parser("clusters", help="manage clusters")
    actions = group.add_subparsers(dest="action", required=True)

    p = actions.add_parser("list", help="list clusters")
    p.add_argument("--show-destroyed", action="store_true")
    p.add_argument("--status")
    p.set_defaults(func=_clusters_list)

    p = actions.add_parser("get", help="get one cluster")
    p.add_argument("cluster_id")
    p.set_defaults(func=_clusters_get)

    p = actions.add_parser("extend", help="extend a cluster's TTL")
    p.add_argument("cluster_id")
    p.add_argument("--ttl-hours", type=float, required=True)
    p.set_defaults(func=_clusters_extend)

    p = actions.add_parser("rehabilitate", help="rehabilitate a failed cluster")
    p.add_argument("cluster_id")
    p.set_defaults(func=_clusters_rehabilitate)

    p = actions.add_parser("destroy", help="destroy a cluster")
    p.add_argument("cluster_id")
    p.add_argument("--force", action="store_true")
    p.add_argument("--snapshot-before-destroy", action="store_true")
    p.set_defaults(func=_clusters_destroy)

    p = actions.add_parser("pods", help="list a cluster's pods")
    p.add_argument("cluster_id")
    p.add_argument("--namespace")
    p.set_defaults(func=_clusters_pods)

    p = actions.add_parser("logs", help="fetch a pod's logs")
    p.add_argument("cluster_id")
    p.add_argument("namespace")
    p.add_argument("pod_name")
    p.add_argument("--container")
    p.add_argument("--tail-lines", type=int, default=100)
    p.add_argument("--previous", action="store_true")
    p.set_defaults(func=_clusters_logs)

    p = actions.add_parser("deployments", help="list a cluster's deployments")
    p.add_argument("cluster_id")
    p.set_defaults(func=_clusters_deployments)

    p = actions.add_parser("audit", help="show a cluster's state-audit history")
    p.add_argument("cluster_id")
    p.add_argument("--limit", type=int, default=50)
    p.set_defaults(func=_clusters_audit)


# ---------------------------------------------------------------------------
# deployments (+ top-level `deploy`)
# ---------------------------------------------------------------------------


async def _deployments_list(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.list_deployments(cluster_id=args.cluster_id, show_history=args.show_history)


async def _deployments_get(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.get_deployment(args.deployment_id)


async def _deployments_redeploy(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.redeploy_deployment(args.deployment_id)


async def _deployments_retrigger(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.retrigger_deployment(args.deployment_id)


async def _deployments_cancel(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.cancel_deployment(args.deployment_id, reason=args.reason)


async def _deploy(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.deploy(repo=args.repo, branch=args.branch, image=args.image, commit=args.commit, tag=args.tag)


def _add_deployments(subparsers: argparse._SubParsersAction) -> None:
    group = subparsers.add_parser("deployments", help="manage deployments")
    actions = group.add_subparsers(dest="action", required=True)

    p = actions.add_parser("list", help="list deployments")
    p.add_argument("--cluster-id")
    p.add_argument("--show-history", action="store_true")
    p.set_defaults(func=_deployments_list)

    p = actions.add_parser("get", help="get one deployment")
    p.add_argument("deployment_id")
    p.set_defaults(func=_deployments_get)

    p = actions.add_parser("redeploy", help="redeploy the current spec")
    p.add_argument("deployment_id")
    p.set_defaults(func=_deployments_redeploy)

    p = actions.add_parser("retrigger", help="retrigger a deployment from scratch")
    p.add_argument("deployment_id")
    p.set_defaults(func=_deployments_retrigger)

    p = actions.add_parser("cancel", help="cancel a deployment")
    p.add_argument("deployment_id")
    p.add_argument("--reason", default="")
    p.set_defaults(func=_deployments_cancel)


def _add_deploy(subparsers: argparse._SubParsersAction) -> None:
    p = subparsers.add_parser("deploy", help="trigger a version-update deploy (POST /api/version-update)")
    p.add_argument("--repo", required=True)
    p.add_argument("--branch", required=True)
    p.add_argument("--image", required=True)
    p.add_argument("--commit", required=True)
    p.add_argument("--tag")
    p.set_defaults(func=_deploy)


# ---------------------------------------------------------------------------
# snapshots
# ---------------------------------------------------------------------------


async def _snapshots_list(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.list_snapshots(branch=args.branch, profile=args.profile)


async def _snapshots_get(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.get_snapshot(args.snapshot_id)


async def _snapshots_create(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.create_snapshot(cluster_id=args.cluster_id, name=args.name, description=args.description)


async def _snapshots_restore(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.restore_snapshot(
        args.snapshot_id,
        cluster_id=args.cluster_id,
        services=args.service or None,
        run_migrations=not args.no_run_migrations,
    )


async def _snapshots_delete(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.delete_snapshot(args.snapshot_id)


async def _snapshots_restore_history(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.restore_history(args.cluster_id)


def _add_snapshots(subparsers: argparse._SubParsersAction) -> None:
    group = subparsers.add_parser("snapshots", help="manage database snapshots")
    actions = group.add_subparsers(dest="action", required=True)

    p = actions.add_parser("list", help="list snapshots")
    p.add_argument("--branch")
    p.add_argument("--profile")
    p.set_defaults(func=_snapshots_list)

    p = actions.add_parser("get", help="get one snapshot")
    p.add_argument("snapshot_id")
    p.set_defaults(func=_snapshots_get)

    p = actions.add_parser("create", help="create a snapshot of a cluster")
    p.add_argument("cluster_id")
    p.add_argument("name")
    p.add_argument("--description")
    p.set_defaults(func=_snapshots_create)

    p = actions.add_parser("restore", help="restore a snapshot onto a cluster")
    p.add_argument("snapshot_id")
    p.add_argument("cluster_id")
    p.add_argument("--service", action="append", default=[], help="limit restore to this service (repeatable)")
    p.add_argument("--no-run-migrations", action="store_true")
    p.set_defaults(func=_snapshots_restore)

    p = actions.add_parser("delete", help="delete a snapshot")
    p.add_argument("snapshot_id")
    p.set_defaults(func=_snapshots_delete)

    p = actions.add_parser("restore-history", help="show a cluster's restore history")
    p.add_argument("cluster_id")
    p.set_defaults(func=_snapshots_restore_history)


# ---------------------------------------------------------------------------
# presets
# ---------------------------------------------------------------------------


def _parse_json_arg(raw: str | None) -> dict[str, Any] | None:
    if raw is None:
        return None
    return json.loads(raw)


async def _presets_list(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.list_presets(profile=args.profile)


async def _presets_get(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.get_preset(args.preset_id)


async def _presets_create(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.create_preset(
        name=args.name,
        profile_name=args.profile_name,
        description=args.description,
        service_overrides=_parse_json_arg(args.service_overrides),
        default_branch=args.default_branch,
        default_ttl_hours=args.default_ttl_hours,
        default_provider=args.default_provider,
    )


async def _presets_update(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.update_preset(
        args.preset_id,
        name=args.name,
        description=args.description,
        profile_name=args.profile_name,
        service_overrides=_parse_json_arg(args.service_overrides),
        default_branch=args.default_branch,
        default_ttl_hours=args.default_ttl_hours,
        default_provider=args.default_provider,
    )


async def _presets_delete(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.delete_preset(args.preset_id)


async def _presets_deploy(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.deploy_preset(
        args.preset_id,
        branch=args.branch,
        service_overrides=_parse_json_arg(args.service_overrides),
        provider_override=args.provider_override,
        ttl_hours=args.ttl_hours,
        cluster_name=args.cluster_name,
        data_initialization=_parse_json_arg(args.data_initialization),
    )


def _add_presets(subparsers: argparse._SubParsersAction) -> None:
    group = subparsers.add_parser("presets", help="manage deployment presets")
    actions = group.add_subparsers(dest="action", required=True)

    p = actions.add_parser("list", help="list presets")
    p.add_argument("--profile")
    p.set_defaults(func=_presets_list)

    p = actions.add_parser("get", help="get one preset")
    p.add_argument("preset_id")
    p.set_defaults(func=_presets_get)

    p = actions.add_parser("create", help="create a preset")
    p.add_argument("name")
    p.add_argument("profile_name")
    p.add_argument("--description")
    p.add_argument("--service-overrides", help="JSON object")
    p.add_argument("--default-branch")
    p.add_argument("--default-ttl-hours", type=int)
    p.add_argument("--default-provider", help="pin this preset's provider (DR-0046), e.g. tart")
    p.set_defaults(func=_presets_create)

    p = actions.add_parser("update", help="update a preset")
    p.add_argument("preset_id")
    p.add_argument("--name")
    p.add_argument("--description")
    p.add_argument("--profile-name")
    p.add_argument("--service-overrides", help="JSON object")
    p.add_argument("--default-branch")
    p.add_argument("--default-ttl-hours", type=int)
    p.add_argument("--default-provider", help="pin this preset's provider (DR-0046), e.g. tart")
    p.set_defaults(func=_presets_update)

    p = actions.add_parser("delete", help="delete a preset")
    p.add_argument("preset_id")
    p.set_defaults(func=_presets_delete)

    p = actions.add_parser("deploy", help="deploy from a preset")
    p.add_argument("preset_id")
    p.add_argument("--branch")
    p.add_argument("--service-overrides", help="JSON object")
    p.add_argument("--provider-override")
    p.add_argument("--ttl-hours", type=float)
    p.add_argument("--cluster-name")
    p.add_argument("--data-initialization", help="JSON object")
    p.set_defaults(func=_presets_deploy)


# ---------------------------------------------------------------------------
# workflows / timers
# ---------------------------------------------------------------------------


async def _workflows_list(client: SeedpodClient, _args: argparse.Namespace) -> Any:
    return await client.list_workflows()


async def _timers_list(client: SeedpodClient, _args: argparse.Namespace) -> Any:
    return await client.list_timers()


def _add_workflows(subparsers: argparse._SubParsersAction) -> None:
    group = subparsers.add_parser("workflows", help="view workflow runs")
    actions = group.add_subparsers(dest="action", required=True)
    p = actions.add_parser("list", help="list workflow runs")
    p.set_defaults(func=_workflows_list)


def _add_timers(subparsers: argparse._SubParsersAction) -> None:
    group = subparsers.add_parser("timers", help="view armed timers")
    actions = group.add_subparsers(dest="action", required=True)
    p = actions.add_parser("list", help="list armed timers")
    p.set_defaults(func=_timers_list)


# ---------------------------------------------------------------------------
# health
# ---------------------------------------------------------------------------


async def _health_basic(client: SeedpodClient, _args: argparse.Namespace) -> Any:
    return await client.health()


async def _health_detailed(client: SeedpodClient, _args: argparse.Namespace) -> Any:
    return await client.health_detailed()


def _add_health(subparsers: argparse._SubParsersAction) -> None:
    group = subparsers.add_parser("health", help="server health")
    actions = group.add_subparsers(dest="action", required=True)
    actions.add_parser("basic", help="GET /api/health").set_defaults(func=_health_basic)
    actions.add_parser("detailed", help="GET /api/health/detailed").set_defaults(func=_health_detailed)


# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------


async def _config_overview(client: SeedpodClient, _args: argparse.Namespace) -> Any:
    return await client.config_overview()


async def _config_rules(client: SeedpodClient, _args: argparse.Namespace) -> Any:
    return await client.config_rules()


async def _config_deployment_profiles(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.config_deployment_profiles(args.name)


async def _config_resolution_strategies(client: SeedpodClient, args: argparse.Namespace) -> Any:
    return await client.config_resolution_strategies(args.name)


async def _config_providers(client: SeedpodClient, _args: argparse.Namespace) -> Any:
    return await client.config_providers()


def _add_config(subparsers: argparse._SubParsersAction) -> None:
    group = subparsers.add_parser("config", help="browse server configuration")
    actions = group.add_subparsers(dest="action", required=True)

    actions.add_parser("overview", help="GET /api/config/overview").set_defaults(func=_config_overview)
    actions.add_parser("rules", help="GET /api/config/rules").set_defaults(func=_config_rules)

    p = actions.add_parser("deployment-profiles", help="GET /api/config/deployment-profiles[/{name}]")
    p.add_argument("name", nargs="?", default=None)
    p.set_defaults(func=_config_deployment_profiles)

    p = actions.add_parser("resolution-strategies", help="GET /api/config/resolution-strategies[/{name}]")
    p.add_argument("name", nargs="?", default=None)
    p.set_defaults(func=_config_resolution_strategies)

    actions.add_parser("providers", help="GET /api/config/providers").set_defaults(func=_config_providers)


# ---------------------------------------------------------------------------
# top-level parser + entry point
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="seedpodctl",
        description=(
            "Authenticated HTTP client for seedpod v2 (DR-0021 §0c). Speaks only "
            "the /api/* HTTP surface -- no direct database access."
        ),
    )
    parser.add_argument("--api-url", help=f"API base URL (default: $SEEDPOD_API_URL or {_DEFAULT_API_URL})")
    parser.add_argument("--api-key", help="Bearer token (default: $SEEDPOD_API_KEY)")
    parser.add_argument(
        "--config",
        type=Path,
        default=_DEFAULT_CONFIG_PATH,
        help=f"JSON config file with api_url/api_key fallbacks (default: {_DEFAULT_CONFIG_PATH})",
    )

    subparsers = parser.add_subparsers(dest="group", required=True)
    _add_keys(subparsers)
    _add_secrets(subparsers)
    _add_clusters(subparsers)
    _add_deployments(subparsers)
    _add_deploy(subparsers)
    _add_snapshots(subparsers)
    _add_presets(subparsers)
    _add_workflows(subparsers)
    _add_timers(subparsers)
    _add_health(subparsers)
    _add_config(subparsers)
    return parser


async def _run(args: argparse.Namespace) -> int:
    config = _read_config_file(args.config)
    api_url = _resolve_api_url(args, config)
    api_key = _resolve_api_key(args, config)

    if not api_key:
        print(
            "error: no API key configured -- set SEEDPOD_API_KEY, pass --api-key, "
            f"or add \"api_key\" to {args.config}",
            file=sys.stderr,
        )
        return 1

    client = SeedpodClient(base_url=api_url, api_key=api_key)
    try:
        result = await args.func(client, args)
    except AuthenticationError as exc:
        print(f"error: authentication failed: {exc}", file=sys.stderr)
        return 1
    except PermissionDeniedError as exc:
        print(f"error: permission denied: {exc}", file=sys.stderr)
        return 1
    except NotFoundError as exc:
        print(f"error: not found: {exc}", file=sys.stderr)
        return 1
    except ConnectionFailedError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except ApiError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    except SeedpodApiError as exc:  # pragma: no cover -- defensive catch-all
        print(f"error: {exc}", file=sys.stderr)
        return 1
    finally:
        await client.aclose()

    if result is not None:
        print(json.dumps(result, indent=2))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
