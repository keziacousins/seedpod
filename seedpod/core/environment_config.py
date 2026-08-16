"""core/environment_config.py — ``EnvironmentVariables``: shared + per-service
environment-variable resolution for deployment profiles, with Jinja2 template
substitution against a caller-supplied context.

Salvaged from ``reference-code/seedpod/seedpod/core/environment_config.py`` (304
lines):

- ``EnvironmentVariables`` (v1 lines 17-26) — verbatim shape: ``shared: dict[str,
  str]``, ``services: dict[str, dict[str, str]]``.
- ``resolve_for_service`` (v1 lines 28-76) — verbatim resolution order: start
  from ``shared``, then override with ``services[service_name]`` (service wins),
  then render EACH value through Jinja2 with ``StrictUndefined`` IF AND ONLY IF
  the value contains BOTH ``'{{'`` and ``'}}'`` (v1 line 62's guard, ported
  verbatim) — a literal ``$`` or an unmatched ``{`` in a plain string (e.g. a
  password) must never be treated as a template.
- ``resolve_all_services`` (v1 lines 78-105) — resolve every named service;
  fail-fast on the first failure (no partial resolution — see "Error taxonomy"
  below for how the fail-fast survives the logging removal).
- ``create_environment_variables_from_dict`` (v1 lines 228-273) — verbatim
  validation: ``shared``/``services`` must be dicts, every value must be a
  ``str``, and a YAML entry written with no value at all (``"service-name:"``,
  which PyYAML parses as ``None``) is tolerated as "no per-service overrides"
  (v1 lines 261-264) rather than a validation error — a REAL edge v1 got right;
  ``config/deployment-profiles/exampleco-dev-stack-nodns.yml``'s ``postgres:``,
  ``mailpit:``, ``tigerbeetle:`` etc. entries depend on it. Unlike v1, this port
  does NOT mutate the caller's ``config_dict["services"]`` in place (v1 line
  263's ``services[service_name] = {}``, mutating the very dict it was handed)
  — every sub-dict is copied into a fresh structure instead, because
  ``config_dict`` here is a slice of a larger parsed-YAML mapping
  (``app/services/profiles.py``'s ``raw``) a caller may still hold and read
  from; core code silently rewriting a caller's own dict out from under it is
  exactly the action-at-a-distance CLAUDE.md's purity rule exists to rule out.

**What did NOT come across, and why (deliberate, LOUD-called-out scope
narrowing, in ``seedpod/services/manifests.py``'s own style):**

- The module-level ``logger`` and every ``logger.error(...)`` call (v1 lines 8,
  14, 70, 73, 101). ``seedpod/core/`` is pure — no logging singleton, no IO
  (CLAUDE.md). Every place v1 logged-then-raised now just raises; observability
  belongs at the seam that calls this module, not inside it.
- ``get_service_variable_count``, ``get_all_variable_keys``,
  ``validate_service_references`` (v1 lines 107-159) — dead weight with no v2
  caller. Nothing in this pillar cross-validates env-var service references
  against a profile's declared services, and nothing counts variables for
  display. Left out rather than ported as dead code.
- ``ResolvedEnvironmentVariables`` and its self-summarising ``__post_init__``
  (v1 lines 162-226) — a v1 API-response convenience (a resolution "summary"
  object) with no v2 caller. ``resolve_all_services``'s plain ``dict[str,
  dict[str, str]]`` return is exactly the shape the shipped templates read
  (``environment_variables.get(service, {}).items()`` — ``config/manifest-
  templates/exampleco-misc/exampleco-web-2.yaml``, ``config/manifest-templates/
  exampleco-stack/*.yaml``); nothing downstream wants a wrapper object.
- ``validate_template_syntax`` (v1 lines 276-304) — a pre-flight syntax checker
  with no PRODUCTION caller in v1 (only v1's own unit tests call it —
  ``reference-code/seedpod/tests/test_environment_variables.py:444,458,470`` —
  which exercise the checker itself, not any caller that acts on its result).
  ``resolve_for_service`` already raises a precise, typed error the moment a
  bad template is actually rendered, which is the only time correctness
  matters.

**Error taxonomy (a judgment call, not v1's).** v1's ``resolve_for_service``
catches ``TemplateError`` and re-raises a NEW ``TemplateError`` (plus a second,
broader ``except Exception`` that re-raises as ``ValueError``); v1's
``resolve_all_services`` wraps every per-service call in ITS OWN try/except
that logs and then re-raises the SAME exception unchanged. Once the logging
side effect is gone (pure core, above), that outer wrapper does nothing a bare
loop wouldn't already do — a raise from ``resolve_for_service`` propagates out
of ``resolve_all_services`` on its own, which IS v1's fail-fast behaviour
("we don't want partial resolution", v1 lines 100-103) — so it is kept, but
without a redundant no-op try/except. What DOES change is which exception
type: a template referencing an undefined variable is bad deployment-profile
CONFIG, not a transient condition, so it now raises ``seedpod.core.errors.
PermanentError`` (``ErrorCode.INVALID_INPUT``) — THE one error-taxonomy home
(CLAUDE.md) — naming the offending service AND key in the message, same as
v1's log line did. The same taxonomy (``PermanentError``/``INVALID_INPUT``)
replaces v1's raw ``ValueError`` in ``create_environment_variables_from_dict``
for the same reason: a malformed ``environment_variables:`` block is bad
config, not a transient failure.

v1's second, broader ``except Exception -> ValueError`` handler (v1 lines
72-74) DOES have a live behaviour to account for: its ``try`` wrapped the
``'{{' in value`` guard itself, not just the render call, so a non-``str``
value reaching ``resolve_for_service`` (possible today: ``EnvironmentVariables``
is directly constructible with no ``__post_init__`` validation, so only the
``create_environment_variables_from_dict`` path is type-checked) tripped that
broader handler and still came out the other side as a typed error naming
``service.key``. This port keeps that coverage rather than dropping it: the
``try`` below wraps the guard AND the render (restoring v1's actual structure,
not just its ``except TemplateError`` line), and its ``except`` clause is
``(TemplateError, TypeError)`` — ``TypeError`` being the ONE exception a
non-``str`` value can raise at ``'{{' in value`` or at ``Template(value, ...)``
(every render-time failure Jinja itself raises, e.g. ``UndefinedError``, is
already a ``TemplateError`` subclass). This is deliberately narrower than v1's
bare ``except Exception``, which would also swallow an ``AttributeError`` or
``KeyError`` raised by an actual bug elsewhere in this function and misreport
it as bad deployment-profile config — a genuine correctness fix, not a v1 bug
pin, and the reason it is ``(TemplateError, TypeError)`` rather than
``Exception`` verbatim.

**Purity note, recorded rather than left for a grep to find.** ``jinja2`` is
NOT the first third-party import under ``seedpod/core/`` -- ``cluster_spec.py``
and ``dns_record.py`` already import ``pydantic`` for their validation
models, with no IO behind it either. It IS the first import of a *templating*
library, which is worth naming explicitly because rendering could easily have
smuggled IO in (a loader reading files, a filesystem-backed ``Environment``)
the way a "pure" ORM import sometimes smuggles a DB handle. It doesn't:
``jinja2.Template(value, ...)`` (above) is constructed on a plain in-memory
``str`` with no ``Environment``/loader/filesystem access anywhere in this
module, so rendering stays pure string-in, string-out computation -- no IO,
no ``now()``, no locks, no naive datetimes. Failures still funnel through the
one error-taxonomy home (``seedpod.core.errors``, above), same as every other
core module.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from jinja2 import StrictUndefined, Template, TemplateError

from seedpod.core.errors import ErrorCode, PermanentError

__all__ = ["EnvironmentVariables", "create_environment_variables_from_dict"]


@dataclass(frozen=True)
class EnvironmentVariables:
    """Shared + per-service environment variables for one deployment profile.
    Verbatim shape from v1 (module docstring); frozen for consistency with
    every other salvaged profile/resolver dataclass in this tree (``services/
    manifests.py``'s ``ServiceSpec``/``ManifestProfile``/etc.) — freezing the
    container doesn't stop in-place dict mutation, but nothing here needs that
    either, and it rules out a caller accidentally reassigning ``.shared``."""

    shared: dict[str, str] = field(default_factory=dict)
    services: dict[str, dict[str, str]] = field(default_factory=dict)

    def resolve_for_service(
        self, service_name: str, template_context: Mapping[str, Any]
    ) -> dict[str, str]:
        """Resolve one service's environment variables: ``shared`` first, then
        ``services[service_name]`` overrides matching keys (service wins) and
        adds any that are unique to it, then every resulting VALUE is rendered
        through Jinja2 with ``StrictUndefined`` -- but ONLY if it contains both
        ``'{{'`` and ``'}}'`` (v1 line 62's guard, ported verbatim: a literal
        ``$`` or ``{`` in a plain value, e.g. a password, is never template
        syntax and must pass through untouched).

        Raises ``PermanentError(ErrorCode.INVALID_INPUT)`` naming the service
        and key the moment a value fails to resolve -- most commonly a template
        fails to render (``template_context`` is missing a key the value
        references), but also a value that is not a ``str`` in the first place
        (module docstring's "Error taxonomy" note: the guard and the render are
        both inside the same ``try``, matching v1's actual structure, so this
        case is covered too)."""
        resolved = dict(self.shared)
        if service_name in self.services:
            resolved.update(self.services[service_name])

        final_resolved: dict[str, str] = {}
        for key, value in resolved.items():
            try:
                if "{{" in value and "}}" in value:
                    rendered = Template(value, undefined=StrictUndefined).render(**template_context)
                else:
                    rendered = value
            except (TemplateError, TypeError) as exc:
                raise PermanentError(
                    f"environment-config.resolve_for_service: failed to resolve "
                    f"{service_name}.{key} = {value!r}: {exc}",
                    code=ErrorCode.INVALID_INPUT,
                    provider="environment-config",
                    command="resolve_for_service",
                    detail={"service": service_name, "key": key},
                ) from exc
            final_resolved[key] = rendered

        return final_resolved

    def resolve_all_services(
        self, service_names: Iterable[str], template_context: Mapping[str, Any]
    ) -> dict[str, dict[str, str]]:
        """Resolve every named service. Fail-fast (v1 lines 100-103): the first
        ``resolve_for_service`` failure propagates immediately -- there is no
        partial-resolution result."""
        return {name: self.resolve_for_service(name, template_context) for name in service_names}


def create_environment_variables_from_dict(config_dict: dict[str, Any]) -> EnvironmentVariables:
    """The validation constructor for a profile's parsed ``environment_variables:``
    block. Verbatim validation from v1 (module docstring): ``shared``/``services``
    must be dicts, every value must be a ``str``, and a ``services`` entry parsed
    as ``None`` (an empty YAML value, e.g. ``postgres:`` with nothing under it)
    tolerates to ``{}`` rather than failing validation. Builds an entirely new
    structure -- never mutates ``config_dict`` (module docstring's purity note)."""
    if not isinstance(config_dict, dict):
        raise PermanentError(
            "environment-config.create_environment_variables_from_dict: "
            f"configuration must be a dictionary, got {type(config_dict).__name__}",
            code=ErrorCode.INVALID_INPUT,
            provider="environment-config",
            command="create_environment_variables_from_dict",
            detail={"type": type(config_dict).__name__},
        )

    shared = config_dict.get("shared", {})
    services = config_dict.get("services", {})

    if not isinstance(shared, dict):
        raise PermanentError(
            "environment-config.create_environment_variables_from_dict: "
            f"'shared' section must be a dictionary, got {type(shared).__name__}",
            code=ErrorCode.INVALID_INPUT,
            provider="environment-config",
            command="create_environment_variables_from_dict",
            detail={"type": type(shared).__name__},
        )
    if not isinstance(services, dict):
        raise PermanentError(
            "environment-config.create_environment_variables_from_dict: "
            f"'services' section must be a dictionary, got {type(services).__name__}",
            code=ErrorCode.INVALID_INPUT,
            provider="environment-config",
            command="create_environment_variables_from_dict",
            detail={"type": type(services).__name__},
        )

    for key, value in shared.items():
        if not isinstance(value, str):
            raise PermanentError(
                "environment-config.create_environment_variables_from_dict: "
                f"shared environment variable {key!r} must be a string, got {type(value).__name__}",
                code=ErrorCode.INVALID_INPUT,
                provider="environment-config",
                command="create_environment_variables_from_dict",
                detail={"key": key, "type": type(value).__name__},
            )

    resolved_services: dict[str, dict[str, str]] = {}
    for service_name, service_vars in services.items():
        # Handle None from empty YAML entries (e.g. "service-name:" with no
        # values) -- a real edge v1 got right, v1 lines 261-264.
        if service_vars is None:
            resolved_services[service_name] = {}
            continue

        if not isinstance(service_vars, dict):
            raise PermanentError(
                "environment-config.create_environment_variables_from_dict: "
                f"service {service_name!r} environment variables must be a dictionary, "
                f"got {type(service_vars).__name__}",
                code=ErrorCode.INVALID_INPUT,
                provider="environment-config",
                command="create_environment_variables_from_dict",
                detail={"service": service_name, "type": type(service_vars).__name__},
            )

        for key, value in service_vars.items():
            if not isinstance(value, str):
                raise PermanentError(
                    "environment-config.create_environment_variables_from_dict: "
                    f"environment variable {service_name}.{key} must be a string, "
                    f"got {type(value).__name__}",
                    code=ErrorCode.INVALID_INPUT,
                    provider="environment-config",
                    command="create_environment_variables_from_dict",
                    detail={"service": service_name, "key": key, "type": type(value).__name__},
                )
        resolved_services[service_name] = dict(service_vars)

    return EnvironmentVariables(shared=dict(shared), services=resolved_services)
