"""``load_deployment_profile`` -- disk-loading for ``config/deployment-profiles/
*.yml`` into ``seedpod.services.manifests.ManifestProfile``, PLUS the raw parsed
mapping (for the fields ``ManifestProfile`` deliberately does not carry: `provider`,
`resolution_strategy`, `cluster_spec` -- see ``seedpod/services/manifests.py``'s
own module docstring, "Deliberate, LOUD-called-out scope narrowing... the caller
now builds and passes in a ManifestProfile directly").

**Round 9, env-vars component:** ``environment_variables:`` graduates OUT of
"raw-only" -- it is now also parsed into a real ``core.environment_config.
EnvironmentVariables`` and carried on ``ManifestProfile.environment_variables``
(``seedpod/services/manifests.py``'s natural home for it, per that dataclass's own
docstring), via ``create_environment_variables_from_dict``. Presence-checked the
same way v1's caller did (``reference-code/seedpod/seedpod/orchestrator/
manifest_resolver.py:269``, ``if 'environment_variables' in template_data:``): a
profile with no ``environment_variables:`` key at all yields an empty
``EnvironmentVariables()``, never a crash; a profile that DOES have the key gets
full validation (a malformed block -- e.g. ``environment_variables:`` written with
no children, which YAML parses as ``None`` -- is a loud ``PermanentError``, not
silently tolerated as "absent").

New-in-v2 plumbing (no v1 salvage source -- v1's equivalent,
``ManifestResolver._load_manifests_config``/``get_manifest_config``, is explicitly
OUT of ``seedpod/services/manifests.py``'s scope per that module's own docstring,
"Deployment-profile ... YAML loading from disk ... the caller now builds and
passes in a ManifestProfile directly"). ``DeploymentService`` is that caller; this
module is its loader, kept separate so the loader's one genuinely-new design
decision -- how a profile's ``manifests_dir`` string combines with the injected
``config_dir`` -- has one place to live and be tested.

**Design decision (this module's own, not spec-pinned):** every shipped profile's
``manifests_dir`` is written as ``"config/manifest-templates/<profile>"`` (a path
that reads naturally relative to the repo root). ``config_dir`` itself is *also*
conventionally named ``config`` (``AppConfig.config_dir``'s default), and the test
fixture (``tests/conftest.py``'s ``test_config_dir``) copies the whole ``config/``
tree into a tmp dir whose ROOT already plays the ``config`` role (i.e. the tmp dir
IS ``config``, not ``config``'s parent) -- so resolving ``manifests_dir`` against
``config_dir``'s *parent* would work in production (if the process cwd happens to
be the repo root) but silently miss in tests, and resolving against ``config_dir``
directly would double the ``config/`` segment. This loader splits the difference
the only way that is correct under BOTH layouts without depending on cwd at all:
strip one leading ``config/`` (or ``config\\``) path segment from the YAML value,
if present, then resolve what remains against the injected ``config_dir`` --
``config_dir`` is thereby the single source of truth for where ``config/`` lives,
exactly as ``RuleEngine.load(config.config_dir / "deployment-rules.yml")`` already
treats it at the composition root.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import yaml

from seedpod.core.environment_config import (
    EnvironmentVariables,
    create_environment_variables_from_dict,
)
from seedpod.core.errors import ErrorCode, PermanentError
from seedpod.core.paths import resolve_under_config_dir
from seedpod.services.manifests import ManifestProfile, ServiceSpec

__all__ = ["load_deployment_profile"]


def _resolve_manifests_dir(config_dir: Path, raw_value: str) -> Path:
    """Delegates to ``core/paths.py``'s ``resolve_under_config_dir`` -- the rule
    this module's docstring reasons out, hoisted to ONE home (Round-8a gate
    finding M-2) once ``engine/steps/kube.py`` turned out to need the identical
    join and had reimplemented it cwd-dependently."""
    return resolve_under_config_dir(config_dir, raw_value)


# DR-0037 decision 2. v2 implements exactly one resolution strategy; the others in
# `config/resolution-strategies.yml` are documented intent, not behaviour.
SUPPORTED_RESOLUTION_STRATEGIES = frozenset({"branch_discovery_with_fallback"})


def _check_resolution_strategy(name: str, raw: Mapping[str, Any]) -> None:
    """Reject a ``resolution_strategy`` v2 cannot honour, at the one place every
    profile is read.

    v1 raised ``ValueError: Unknown resolution strategy`` for a name absent from
    `resolution-strategies.yml` (``manifest_resolver.py``:479-480). v2 ignored the
    field entirely, so a profile declaring ``strict_branch`` -- which promises "no
    fallbacks, fail if not found" -- silently got FULL fallback behaviour. That is
    backlog #24's shape (a surface advertising what the engine does not implement),
    and it is closed by refusing to load rather than by quietly degrading.

    Narrower and stronger than v1's check: v1 rejected names missing from the YAML,
    this rejects names v2 cannot HONOUR -- a strategy can be perfectly well-defined
    in the file and still be a lie if the resolver ignores it."""
    declared = raw.get("resolution_strategy")
    if declared is None or declared in SUPPORTED_RESOLUTION_STRATEGIES:
        return
    raise PermanentError(
        f"deployment profile {name!r} declares resolution_strategy={declared!r}, which this version "
        f"does not implement (supported: {', '.join(sorted(SUPPORTED_RESOLUTION_STRATEGIES))}). "
        "config/resolution-strategies.yml documents the others as intent; the resolver honours only "
        "the one.",
        code=ErrorCode.INVALID_INPUT,
        provider="deployment-profile",
        command="load_deployment_profile",
        detail={"profile": name, "resolution_strategy": str(declared)},
    )


def load_deployment_profile(config_dir: Path, name: str) -> tuple[ManifestProfile, dict[str, Any]]:
    """Read ``config_dir / "deployment-profiles" / f"{name}.yml"``. Raises
    ``PermanentError(NOT_FOUND)`` -- THE error-taxonomy home (CLAUDE.md) -- for a
    missing/unparsable file, matching every other fail-fast config load in this
    tree (``RuleEngine.load``). Returns ``(profile, raw)`` -- ``raw`` is the
    untouched parsed mapping, for the fields ``ManifestProfile`` does not carry."""
    path = config_dir / "deployment-profiles" / f"{name}.yml"
    if not path.exists():
        raise PermanentError(
            f"deployment profile {name!r} not found ({path})",
            code=ErrorCode.NOT_FOUND,
            provider="deployment-profiles",
            command="load",
            detail={"profile": name, "path": str(path)},
        )
    try:
        raw = yaml.safe_load(path.read_text()) or {}
    except yaml.YAMLError as exc:
        raise PermanentError(
            f"deployment profile {name!r} failed to parse: {exc}",
            code=ErrorCode.INVALID_INPUT,
            provider="deployment-profiles",
            command="load",
            detail={"profile": name, "path": str(path)},
        ) from exc

    services = {
        service_name: ServiceSpec(
            repository=service_raw.get("repository", service_name),
            image_override=service_raw.get("image_override"),
            branch_override=service_raw.get("branch_override"),
            external=bool(service_raw.get("external", False)),
            required=bool(service_raw.get("required", True)),
        )
        for service_name, service_raw in (raw.get("services") or {}).items()
    }
    manifests_dir = _resolve_manifests_dir(config_dir, raw.get("manifests_dir", f"config/manifest-templates/{name}"))
    # DR-0037 decision 1: the PROFILE owns fallback branches. A deliberate
    # divergence from v1, which read them off the named strategy in
    # `resolution-strategies.yml` and left this field dead-but-echoed
    # (`manifest_resolver.py:1037`) -- so for `exampleco-staging-stack` v1 falls back
    # `dev -> main` where v2 falls back `staging -> dev`. Recorded, not accidental.
    fallback_branches = tuple(raw.get("fallback_branches") or ("dev", "main"))
    _check_resolution_strategy(name, raw)

    # Presence-checked, not `raw.get(...) is not None` -- a profile that omits
    # `environment_variables:` entirely gets an empty EnvironmentVariables (no
    # crash), but one that writes the key with a malformed value still fails
    # loudly through create_environment_variables_from_dict's own validation
    # (module docstring).
    if "environment_variables" in raw:
        environment_variables = create_environment_variables_from_dict(raw["environment_variables"])
    else:
        environment_variables = EnvironmentVariables()

    profile = ManifestProfile(
        name=name,
        manifests_dir=manifests_dir,
        services=services,
        fallback_branches=fallback_branches,
        environment_variables=environment_variables,
    )
    return profile, raw
