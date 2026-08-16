"""``required_secrets`` — which secrets a deployment profile actually needs
(DR-0041 decision 5).

**Why this exists.** Cold-starting a dev stack meant knowing ~20 secret names
that were written down nowhere. The list was recovered on 2026-08-12 by grepping
``config/manifest-templates/`` for ``{{ secrets.* }}``, and two of its rules were
learned by watching real deployments fail. That is oral tradition, and it is
exactly the kind that goes wrong quietly: a missing secret does not fail at
deploy time in an obvious place -- ``services/manifests.py`` raises, deliberately,
where v1 rendered an empty string.

The templates are the only honest source. A profile does not list its secrets, and
``ManifestProfile`` deliberately carries no secrets field, so this reads the
rendered-template text the same way the resolver will.

**The two rules that cost real runs**, both encoded below rather than remembered:

1. ``s3_access_key`` is NOT free-form. It has to equal the ``MINIO_ROOT_USER``
   literal the profile itself declares, because minio is configured from one and
   the app authenticates with the other. v1's real value turned out to be exactly
   that literal, which is where the rule came from.
2. A placeholder has to satisfy Keycloak's realm password policy. The 2026-08-12
   run used ``dev-placeholder-secret`` and ``exampleco-api`` died on
   ``invalidPasswordMinUpperCaseCharsMessage``; the retry used
   ``DevPlaceholder123`` and it died on ``invalidPasswordMinSpecialCharsMessage``.
   Two runs, ~20 minutes each, to discover a password policy.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from seedpod.app.services.profiles import load_deployment_profile
from seedpod.core.paths import resolve_under_config_dir

__all__ = ["DEFAULT_PLACEHOLDER", "GENERATED_SECRETS", "SecretRequirement", "required_secrets"]

# Upper + lower + digit + symbol. Not decoration: see rule 2 in the module docstring.
DEFAULT_PLACEHOLDER = "DevPlaceholder1!"

# Secrets seedpod produces itself, which an operator must NOT be told to invent.
# `ghcr_dockerconfig_json` is built by ManifestResolver._add_ghcr_auth_if_needed
# from the org + GITHUB_TOKEN (services/manifests.py:74) and only ever ADDED to the
# resolved set.
GENERATED_SECRETS = frozenset({"ghcr_dockerconfig_json"})

_SECRET_REF = re.compile(r"secrets\.([A-Za-z_][A-Za-z0-9_]*)")
_TEMPLATE_SUFFIXES = (".yaml", ".yml")


@dataclass(frozen=True, slots=True)
class SecretRequirement:
    key_name: str
    pinned_value: str | None = None  # must equal this exactly; None = any value will do
    reason: str | None = None  # why it is pinned, for the operator reading the output


def required_secrets(config_dir: Path, profile_name: str) -> tuple[SecretRequirement, ...]:
    """Every ``{{ secrets.X }}`` the profile's manifest templates reference, minus
    the ones seedpod generates, sorted by name.

    Raises ``PermanentError(NOT_FOUND)`` (via ``load_deployment_profile``) for an
    unknown profile -- the same failure every other profile reader gives.
    """
    _profile, raw = load_deployment_profile(config_dir, profile_name)
    templates_dir = resolve_under_config_dir(config_dir, str(raw.get("manifests_dir", "")))

    names: set[str] = set()
    for path in sorted(templates_dir.rglob("*")):
        if path.suffix.lower() in _TEMPLATE_SUFFIXES and path.is_file():
            names.update(_SECRET_REF.findall(path.read_text()))

    minio_user = _declared_minio_root_user(raw)
    return tuple(
        SecretRequirement(
            key_name=name,
            pinned_value=minio_user if name == "s3_access_key" else None,
            reason=(
                "must equal the profile's own MINIO_ROOT_USER, or the app cannot "
                "authenticate against the minio it just configured"
                if name == "s3_access_key" and minio_user
                else None
            ),
        )
        for name in sorted(names - GENERATED_SECRETS)
    )


def _declared_minio_root_user(raw: dict[str, Any]) -> str | None:
    """The ``MINIO_ROOT_USER`` literal from the profile's own environment_variables.

    Searched rather than read from a fixed path: the block is nested per service
    (``environment_variables.services.minio.MINIO_ROOT_USER`` in the shipped
    profiles) and a profile is free to name its minio service something else. The
    value is what matters, not where it sits.
    """
    def walk(node: Any) -> str | None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key == "MINIO_ROOT_USER" and isinstance(value, str) and value:
                    return value
                found = walk(value)
                if found:
                    return found
        elif isinstance(node, list):
            for item in node:
                found = walk(item)
                if found:
                    return found
        return None

    return walk(raw.get("environment_variables"))
