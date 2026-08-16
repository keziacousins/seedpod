"""tests/core/test_environment_config.py — ``EnvironmentVariables``/
``create_environment_variables_from_dict`` (``seedpod/core/environment_config.py``):
shared/service precedence, the ``'{{'``-guard, Jinja substitution + its
``StrictUndefined`` failure mode, and ``create_environment_variables_from_dict``'s
validation (including the empty-YAML-service-entry tolerance and the
no-caller-mutation fix). Pure dicts + Jinja rendering -- no fixtures, no IO, zero
mocks.

The profile-shaped, end-to-end version of this ("exampleco-web-2 gets all four keys,
tailscale gets only the two shared ones") lives in ``tests/app/test_profiles.py``,
against the REAL shipped ``config/deployment-profiles/exampleco-web-2.yml``.
"""

from __future__ import annotations

import pytest

from seedpod.core.environment_config import (
    EnvironmentVariables,
    create_environment_variables_from_dict,
)
from seedpod.core.errors import ErrorCode, PermanentError

# ============================================================================
# resolve_for_service — shared/service precedence
# ============================================================================


def test_shared_only_resolves_the_same_for_any_service():
    env = EnvironmentVariables(shared={"ENVIRONMENT_NAME": "ephemeral"})
    assert env.resolve_for_service("whichever-service", {}) == {"ENVIRONMENT_NAME": "ephemeral"}


def test_service_only_environment_variables_do_not_leak_to_other_services():
    env = EnvironmentVariables(services={"exampleco-web-2": {"APP_NAME": "exampleco-web-2"}})
    assert env.resolve_for_service("exampleco-web-2", {}) == {"APP_NAME": "exampleco-web-2"}
    assert env.resolve_for_service("tailscale", {}) == {}


def test_service_specific_value_overrides_shared_key_of_the_same_name():
    env = EnvironmentVariables(
        shared={"LOG_LEVEL": "info", "REGION": "eu"},
        services={"noisy-service": {"LOG_LEVEL": "debug"}},
    )
    resolved = env.resolve_for_service("noisy-service", {})
    assert resolved == {"LOG_LEVEL": "debug", "REGION": "eu"}  # service wins; shared keys not lost


# ============================================================================
# Jinja substitution + the '{{'-guard
# ============================================================================


def test_jinja_substitution_renders_against_the_supplied_context():
    env = EnvironmentVariables(shared={"URL": "https://{{ host }}:{{ port }}/path"})
    resolved = env.resolve_for_service("svc", {"host": "example.test", "port": 8443})
    assert resolved == {"URL": "https://example.test:8443/path"}


def test_value_with_no_braces_and_a_literal_dollar_passes_through_untouched():
    """v1's guard is `'{{' in value and '}}' in value` -- a plain string (e.g. a
    password) containing '$' must never be treated as template syntax."""
    env = EnvironmentVariables(shared={"SECRET": "p@ss$word"})
    assert env.resolve_for_service("svc", {}) == {"SECRET": "p@ss$word"}


@pytest.mark.parametrize(
    "value",
    ["{not a template", "{{ unclosed", "unopened }}"],
    ids=["neither-marker", "open-marker-only", "close-marker-only"],
)
def test_value_with_at_most_one_half_of_the_brace_pair_passes_through_untouched(value):
    """v1's guard is `'{{' in value and '}}' in value` -- BOTH markers, not just
    one, must be present before Jinja is even invoked. This pins the `and`:
    "{{ unclosed" contains the open marker but no matching close marker, and is
    NOT valid Jinja on its own -- `Template("{{ unclosed",
    undefined=StrictUndefined)` raises a `TemplateSyntaxError` at parse time
    (verified directly against jinja2). So if the guard were ever loosened from
    `and` to `or` (or narrowed to check only `'{{' in value`), this exact value
    would flip from v1's silent pass-through into a PermanentError, silently
    rejecting a whole profile over what v1 treats as an ordinary literal string
    -- precisely the "silently regressing edge behavior v1 already got right"
    failure mode (CLAUDE.md). The neither-marker and close-marker-only cases
    happen to render unchanged under Jinja too (a stray '}}' alone is not an
    error), so they don't independently pin the `and`, but they are kept
    because they are exactly the values a reader would reach for to double
    check the guard's boundary, and they must never regress either."""
    env = EnvironmentVariables(shared={"FRAGMENT": value})
    assert env.resolve_for_service("svc", {}) == {"FRAGMENT": value}


def test_non_string_value_raises_permanent_error_naming_service_and_key():
    """`EnvironmentVariables` is directly constructible with no runtime type
    validation (only `create_environment_variables_from_dict` validates --
    module docstring's 'Error taxonomy' paragraph on v1's second, broader
    handler). A non-str value must still fail as a typed, contextful
    PermanentError naming service+key, not a bare TypeError escaping core
    untyped."""
    env = EnvironmentVariables(shared={"PORT": 8080})  # type: ignore[dict-item]

    with pytest.raises(PermanentError) as excinfo:
        env.resolve_for_service("svc", {})

    err = excinfo.value
    assert err.code == ErrorCode.INVALID_INPUT
    assert "svc" in str(err)
    assert "PORT" in str(err)
    assert err.detail == {"service": "svc", "key": "PORT"}


def test_strict_undefined_raises_permanent_error_naming_service_and_key():
    env = EnvironmentVariables(services={"exampleco-web-2": {"CLUSTER_ID": "{{ cluster_id }}"}})

    with pytest.raises(PermanentError) as excinfo:
        env.resolve_for_service("exampleco-web-2", {})

    err = excinfo.value
    assert err.code == ErrorCode.INVALID_INPUT
    assert "exampleco-web-2" in str(err)
    assert "CLUSTER_ID" in str(err)
    assert err.detail == {"service": "exampleco-web-2", "key": "CLUSTER_ID"}


# ============================================================================
# resolve_all_services — per-service resolution + fail-fast
# ============================================================================


def test_resolve_all_services_resolves_each_service_independently():
    env = EnvironmentVariables(
        shared={"SHARED_KEY": "shared-value"},
        services={
            "svc-a": {"A_ONLY": "a-value"},
            "svc-b": {"SHARED_KEY": "b-override"},
        },
    )
    result = env.resolve_all_services(["svc-a", "svc-b"], {})
    assert result == {
        "svc-a": {"SHARED_KEY": "shared-value", "A_ONLY": "a-value"},
        "svc-b": {"SHARED_KEY": "b-override"},
    }


def test_resolve_all_services_fails_fast_naming_the_offending_service():
    """v1 lines 100-103: fail fast, no partial resolution."""
    env = EnvironmentVariables(services={"bad-service": {"MISSING": "{{ not_supplied }}"}})

    with pytest.raises(PermanentError) as excinfo:
        env.resolve_all_services(["bad-service"], {})

    assert "bad-service" in str(excinfo.value)
    assert "MISSING" in str(excinfo.value)


# ============================================================================
# create_environment_variables_from_dict — the validation constructor
# ============================================================================


def test_create_environment_variables_from_dict_missing_sections_default_to_empty():
    assert create_environment_variables_from_dict({}) == EnvironmentVariables(shared={}, services={})


def test_create_environment_variables_from_dict_builds_shared_and_services():
    env = create_environment_variables_from_dict(
        {"shared": {"A": "1"}, "services": {"web": {"B": "2"}}}
    )
    assert env == EnvironmentVariables(shared={"A": "1"}, services={"web": {"B": "2"}})


def test_empty_yaml_service_entry_tolerates_to_an_empty_dict():
    """PyYAML parses `postgres:` with no children as None --
    config/deployment-profiles/exampleco-dev-stack-nodns.yml's `postgres:`/`mailpit:`/
    `tigerbeetle:` entries depend on this NOT being a validation error (v1 lines
    261-264)."""
    env = create_environment_variables_from_dict(
        {"shared": {"A": "1"}, "services": {"postgres": None, "web": {"B": "2"}}}
    )
    assert env.services == {"postgres": {}, "web": {"B": "2"}}


def test_create_environment_variables_from_dict_does_not_mutate_caller_input():
    """Genuine correctness fix over v1 (module docstring): v1's
    `services[service_name] = {}` rewrote the CALLER's own dict in place; this
    port builds an entirely new structure instead."""
    raw_services = {"postgres": None}
    config_dict = {"shared": {}, "services": raw_services}

    create_environment_variables_from_dict(config_dict)

    assert raw_services == {"postgres": None}
    assert config_dict["services"] is raw_services


@pytest.mark.parametrize("bad_config", ["not-a-dict", 123, None, ["a", "list"]])
def test_create_environment_variables_from_dict_rejects_non_dict_input(bad_config):
    with pytest.raises(PermanentError) as excinfo:
        create_environment_variables_from_dict(bad_config)
    assert excinfo.value.code == ErrorCode.INVALID_INPUT


def test_create_environment_variables_from_dict_rejects_non_dict_shared_section():
    with pytest.raises(PermanentError):
        create_environment_variables_from_dict({"shared": ["not", "a", "dict"]})


def test_create_environment_variables_from_dict_rejects_non_dict_services_section():
    with pytest.raises(PermanentError):
        create_environment_variables_from_dict({"services": "not-a-dict"})


def test_create_environment_variables_from_dict_rejects_non_string_shared_value():
    with pytest.raises(PermanentError) as excinfo:
        create_environment_variables_from_dict({"shared": {"PORT": 8080}})
    assert "PORT" in str(excinfo.value)


def test_create_environment_variables_from_dict_rejects_non_dict_service_vars():
    with pytest.raises(PermanentError):
        create_environment_variables_from_dict({"services": {"web": "not-a-dict"}})


def test_create_environment_variables_from_dict_rejects_non_string_service_value():
    with pytest.raises(PermanentError) as excinfo:
        create_environment_variables_from_dict({"services": {"web": {"REPLICAS": 3}}})
    assert "web" in str(excinfo.value)
    assert "REPLICAS" in str(excinfo.value)
