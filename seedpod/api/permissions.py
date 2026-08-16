"""The v2 permission registry + scope check (``GET /api/permissions``, ui-contract
§1: ``permissions{}, categories{}``).

Salvaged from ``reference-code/seedpod/seedpod/core/permissions.py``'s
``AVAILABLE_PERMISSIONS``/``PERMISSION_CATEGORIES``/``has_permission`` (the whole
module), with ONE deliberate v2 shape change (not a bug pin -- CLAUDE.md's "don't
pin v1 bugs"): v1's permission values are stored as a ``dict[str, bool]`` (an
"enabled" flag per key -- most entries always ``True``, unused expressiveness) and
its super-wildcard is the string ``"admin:*"``. v2's ``api_keys.permissions`` is a
JSON **list** of granted permission strings (``ApiKeyRepository``'s own docstring;
the pinned ``create_api_key(..., permissions=[...])`` conftest contract), and this
round's brief names the super-wildcard literally: bare ``"*"`` (the conftest
fixture's own ``permissions=["*"]``) -- so v1's ``expand_permissions``/wildcard-
dict machinery collapses to the three-rule check in ``has_permission`` below; there
is no per-key "enabled" flag to expand.

``AVAILABLE_PERMISSIONS``/``PERMISSION_CATEGORIES`` keep v1's category shape
verbatim except the glossary rename (coherence-review Conflict 16: "'run' never
'job'... Actor grammar"): v1's ``jobs:read``/``jobs:*`` (APScheduler tracking)
becomes ``workflows:read``/``workflows:*`` (the ``workflow_runs`` table this round's
``GET /api/workflows``/``GET /api/timers`` expose). v1's ``deployments:version-update``/
``deployments:trigger`` (webhook-trigger scopes for endpoints this round's brief does
not build) are kept in the catalog anyway -- ``GET /api/permissions`` is a forward-
looking registry for API-key creation (CreateApiKey.jsx, ui-contract), the same
discipline v1's own registry used (it listed ``config:read``/``config:reload``
scopes with no endpoint enforcing them at the time either).

**Round 6, api-features addition:** ``snapshots:read``/``snapshots:create``/
``snapshots:delete``/``snapshots:*`` -- genuinely new-in-v2 scopes (v1's
``reference-code/seedpod/seedpod/api/snapshots.py`` gated its routes behind these
exact same names, but no v2 permission catalog had them yet since the snapshot
subsystem didn't exist before this component). Everything else this round's new
routers need already exists in the catalog: presets reuse ``deployments:read``/
``deployments:create`` (v1's own ``api/presets.py`` gates preset CRUD + deploy
behind those same two scopes, never a dedicated ``presets:*``); the registry
browse routes reuse ``deployments:read`` (v1 ``api/registry.py`` parity); secrets
reuse the existing ``secrets:read``/``secrets:create``/``secrets:delete`` (finer-
grained than v1's single ``secrets:write``, no catalog gap to fill); config reuses
``config:read``; and ``/api/keys``/``/api/permissions`` are both gated behind the
bare ``"*"`` super-wildcard (v1's ``admin:*`` on every ``/keys`` route,
``reference-code/seedpod/seedpod/api/auth.py``, translated to v2's convention --
this module's own docstring)."""

from __future__ import annotations

from collections.abc import Sequence

__all__ = ["AVAILABLE_PERMISSIONS", "PERMISSION_CATEGORIES", "has_permission"]

AVAILABLE_PERMISSIONS: dict[str, str] = {
    "*": "Full administrative access to all resources",
    "deployments:create": "Create new deployments",
    "deployments:read": "View deployment information",
    "deployments:update": "Update existing deployments",
    "deployments:delete": "Delete deployments",
    "deployments:trigger": "Trigger manual deployments",
    "deployments:*": "All deployment permissions",
    "clusters:create": "Create new clusters",
    "clusters:read": "View cluster information",
    "clusters:update": "Update cluster configuration",
    "clusters:delete": "Destroy clusters",
    "clusters:extend": "Extend cluster TTL",
    "clusters:*": "All cluster permissions",
    "secrets:read": "View secrets (metadata only, not values)",
    "secrets:create": "Create new secrets",
    "secrets:delete": "Delete secrets",
    "secrets:*": "All secret permissions",
    "config:read": "View system configuration",
    "config:reload": "Reload configuration files",
    "events:stream": "Subscribe to real-time event stream (SSE)",
    "workflows:read": "View workflow run history and armed timers",
    "workflows:*": "All workflow permissions",
    "snapshots:read": "View snapshot metadata and restore history",
    "snapshots:create": "Create and restore snapshots",
    "snapshots:delete": "Delete snapshots",
    "snapshots:*": "All snapshot permissions",
}

PERMISSION_CATEGORIES: dict[str, list[str]] = {
    "Administrative": ["*"],
    "Deployments": [
        "deployments:*",
        "deployments:create",
        "deployments:read",
        "deployments:update",
        "deployments:delete",
        "deployments:trigger",
    ],
    "Clusters": [
        "clusters:*",
        "clusters:create",
        "clusters:read",
        "clusters:update",
        "clusters:delete",
        "clusters:extend",
    ],
    "Secrets": ["secrets:*", "secrets:read", "secrets:create", "secrets:delete"],
    "Configuration": ["config:read", "config:reload"],
    "Events": ["events:stream"],
    "Workflows": ["workflows:*", "workflows:read"],
    "Snapshots": ["snapshots:*", "snapshots:read", "snapshots:create", "snapshots:delete"],
}


def has_permission(granted: Sequence[str], required: str) -> bool:
    """``True`` iff ``granted`` (an ``ApiKeyRow.permissions`` list) admits
    ``required``. Three rules, checked in order:

    1. ``"*"`` in ``granted`` -- the super-wildcard, admits everything.
    2. ``required`` itself is in ``granted`` -- an exact grant.
    3. ``"<category>:*"`` is in ``granted``, where ``category`` is ``required``'s
       segment before its first ``":"`` -- a category wildcard (e.g.
       ``"clusters:*"`` admits ``"clusters:read"``).

    No dict/bool expansion (module docstring) -- ``granted`` is already the flat
    set of what this key holds."""
    if "*" in granted:
        return True
    if required in granted:
        return True
    category = required.split(":", 1)[0]
    return f"{category}:*" in granted
