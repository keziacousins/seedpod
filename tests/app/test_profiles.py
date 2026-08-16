"""tests/app/test_profiles.py — ``load_deployment_profile``'s Round 9 env-vars
extension: the ``environment_variables:`` block parses into a real ``core.
environment_config.EnvironmentVariables`` and lands on ``ManifestProfile.
environment_variables``.

Uses ``test_config_dir`` (``tests/conftest.py`` -- the REAL shipped ``config/``
tree, not a hand-rolled fixture) so the load-then-resolve path is proven against
the actual shipped ``config/deployment-profiles/exampleco-web-2.yml``, not a stand-in
that could drift from it. No Mock/patch anywhere.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from seedpod.app.services.profiles import load_deployment_profile
from seedpod.core.environment_config import EnvironmentVariables
from seedpod.core.errors import PermanentError


def test_load_deployment_profile_parses_the_environment_variables_block(test_config_dir: Path):
    profile, _raw = load_deployment_profile(test_config_dir, "exampleco-web-2")

    assert profile.environment_variables == EnvironmentVariables(
        shared={"ENVIRONMENT_NAME": "ephemeral", "CLUSTER_ID": "{{ cluster_id }}"},
        services={"exampleco-web-2": {"APP_NAME": "exampleco-web-2", "SERVICE_PORT": "8000"}},
    )


def test_exampleco_web_2_resolves_all_four_expected_keys_and_cluster_id_renders(test_config_dir: Path):
    """The round's premise: with a real `cluster_id` in the template context,
    CLUSTER_ID: "{{ cluster_id }}" (config/deployment-profiles/exampleco-web-2.yml)
    renders to the supplied id, merged with exampleco-web-2's own three keys."""
    profile, _raw = load_deployment_profile(test_config_dir, "exampleco-web-2")

    resolved = profile.environment_variables.resolve_for_service(
        "exampleco-web-2", {"cluster_id": "c-abc123"}
    )

    assert resolved == {
        "ENVIRONMENT_NAME": "ephemeral",
        "CLUSTER_ID": "c-abc123",
        "APP_NAME": "exampleco-web-2",
        "SERVICE_PORT": "8000",
    }


def test_tailscale_gets_only_the_two_shared_keys(test_config_dir: Path):
    """tailscale is a declared SERVICE in exampleco-web-2.yml's `services:` block but
    has no entry under `environment_variables.services` -- it must resolve to
    exactly the shared keys, nothing more."""
    profile, _raw = load_deployment_profile(test_config_dir, "exampleco-web-2")

    resolved = profile.environment_variables.resolve_for_service(
        "tailscale", {"cluster_id": "c-abc123"}
    )

    assert resolved == {"ENVIRONMENT_NAME": "ephemeral", "CLUSTER_ID": "c-abc123"}


def test_profile_with_no_environment_variables_block_yields_empty(test_config_dir: Path):
    """tests/fixtures/deployment-profiles/ephemeral-stack.yml (overlaid onto
    test_config_dir) has no `environment_variables:` key at all -- must not crash,
    must yield an empty EnvironmentVariables."""
    profile, raw = load_deployment_profile(test_config_dir, "ephemeral-stack")

    assert "environment_variables" not in raw  # the fixture genuinely omits it
    assert profile.environment_variables == EnvironmentVariables()
    assert profile.environment_variables.resolve_for_service("anything", {}) == {}


# ---------------------------------------------------------------------------
# DR-0037 — resolution_strategy validation at the one load choke point.
# ---------------------------------------------------------------------------


def _write_profile(tmp_path, name: str, body: str) -> Path:
    d = tmp_path / "deployment-profiles"
    d.mkdir(parents=True, exist_ok=True)
    (d / f"{name}.yml").write_text(body)
    return tmp_path


def test_a_strategy_v2_cannot_honour_fails_to_load(tmp_path):
    """DR-0037 decision 2. Before this, a profile declaring `strict_branch` -- which
    promises "no fallbacks, fail if not found" -- loaded fine and silently got FULL
    fallback behaviour, because v2 never reads the field. Backlog #24's shape:
    a surface advertising what the engine does not implement."""
    config_dir = _write_profile(
        tmp_path,
        "strict",
        "environment_type: ephemeral\nresolution_strategy: strict_branch\nservices: {}\n",
    )
    with pytest.raises(PermanentError) as excinfo:
        load_deployment_profile(config_dir, "strict")
    assert "strict_branch" in str(excinfo.value)
    assert "branch_discovery_with_fallback" in str(excinfo.value)  # names the supported set


def test_the_supported_strategy_and_an_absent_one_both_load(tmp_path):
    """Absent means "the default", which is the supported one -- most profiles."""
    config_dir = _write_profile(
        tmp_path,
        "explicit",
        "environment_type: ephemeral\nresolution_strategy: branch_discovery_with_fallback\nservices: {}\n",
    )
    _write_profile(tmp_path, "implicit", "environment_type: ephemeral\nservices: {}\n")
    assert load_deployment_profile(config_dir, "explicit")[0].name == "explicit"
    assert load_deployment_profile(config_dir, "implicit")[0].name == "implicit"


def test_every_shipped_profile_loads():
    """The guard rail for the guard rail: if a shipped profile ever declares an
    unsupported strategy, that is now a hard failure at deploy time, so it must be
    a hard failure here first."""
    config_dir = Path(__file__).resolve().parents[2] / "config"
    names = sorted(p.stem for p in (config_dir / "deployment-profiles").glob("*.yml"))
    assert names, "no shipped profiles found -- this test would pass vacuously"
    for name in names:
        load_deployment_profile(config_dir, name)


def test_every_shipped_rule_names_a_profile_that_loads():
    """The sibling of ``test_every_shipped_profile_loads``, one layer out: a rule may
    only name a profile that exists. ``load_deployment_profile`` raises
    ``PermanentError(NOT_FOUND)`` for a missing file, so a rule pointing at a name with
    no file is a webhook that fails at deploy time and nowhere earlier.

    Found by the 2026-08-12 tart run: ``main_stack_feature_branches`` -- the PRIMARY
    rule, covering five core repos on ``dev``/``feature/*`` -- named ``exampleco-stack``,
    which is a manifests DIRECTORY name, not a profile. No profile file has ever
    existed for it. Nothing caught that, because every rules test to date drives
    ``tests/fixtures/deployment-rules.yml`` rather than the shipped file."""
    config_dir = Path(__file__).resolve().parents[2] / "config"
    raw = yaml.safe_load((config_dir / "deployment-rules.yml").read_text()) or {}
    rules = raw.get("rules") or []
    assert rules, "no shipped rules found -- this test would pass vacuously"

    named = [
        (rule.get("name"), (rule.get("config") or {}).get("deployment_profile"))
        for rule in rules
    ]
    named = [(rule_name, profile) for rule_name, profile in named if profile]
    assert named, "no shipped rule names a profile -- this test would pass vacuously"

    for rule_name, profile in named:
        try:
            load_deployment_profile(config_dir, profile)
        except PermanentError as exc:
            pytest.fail(f"rule {rule_name!r} names profile {profile!r}, which does not load: {exc}")


def test_every_shipped_profile_pins_a_provider_that_has_config():
    """A profile's ``provider:`` key is live -- ``deployment_service.py``'s
    ``provider_override or raw_profile.get("provider", default)`` -- so a value with no
    ``config/providers/<name>.yml`` behind it is a profile that cannot deploy anywhere.
    Absent is fine and means "use the composition root's default"."""
    config_dir = Path(__file__).resolve().parents[2] / "config"
    known = {p.stem for p in (config_dir / "providers").glob("*.yml")}
    assert known, "no shipped provider configs found -- this test would pass vacuously"

    for path in sorted((config_dir / "deployment-profiles").glob("*.yml")):
        _profile, raw = load_deployment_profile(config_dir, path.stem)
        provider = raw.get("provider")
        if provider is not None:
            assert provider in known, (
                f"profile {path.stem!r} pins provider {provider!r}, "
                f"which has no config/providers/{provider}.yml (known: {sorted(known)})"
            )


def test_the_profile_owns_fallback_branches_not_the_strategy_file():
    """DR-0037 decision 1, and the concrete divergence from v1 it records: v1 read
    fallbacks off the NAMED STRATEGY (`dev -> main` for this profile) and left the
    profile's own field dead-but-echoed; v2 reads the profile (`staging -> dev`).
    Same file, different images when a service has no build for the branch."""
    config_dir = Path(__file__).resolve().parents[2] / "config"
    profile, raw = load_deployment_profile(config_dir, "exampleco-staging-stack")
    assert raw["resolution_strategy"] == "branch_discovery_with_fallback"
    assert profile.fallback_branches == ("staging", "dev")  # the PROFILE's list
