"""tests/services/test_rules.py — table-driven tests for
``seedpod.services.rules.RuleEngine`` over ``tests/fixtures/deployment-rules.yml`` and
the real ``config/deployment-rules.yml``, plus fail-fast ``load()`` construction.

Pure in-process logic (no transport, no IO beyond reading a YAML file from disk) — no
Mock/patch needed or used.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from seedpod.services.rules import RuleEngine, RuleValidationError

_FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures"
_CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"


@pytest.fixture
def engine() -> RuleEngine:
    return RuleEngine.load(_FIXTURES_DIR / "deployment-rules.yml")


# ============================================================================
# core v1 semantics from the task description: feature/* -> create_ephemeral,
# main -> update_environment staging, no-match -> no_action, disabled skipped.
# ============================================================================


def test_feature_branch_creates_ephemeral(engine: RuleEngine):
    decision = engine.evaluate("any-repo", "feature/FIN-123")

    assert decision.action == "create_ephemeral"
    assert decision.matched_rule == "feature_branches"
    assert decision.environment == "ephemeral"
    assert decision.config["ttl_hours"] == 2  # rule-specific overrides the default


def test_main_branch_updates_staging(engine: RuleEngine):
    decision = engine.evaluate("any-repo", "main")

    assert decision.action == "update_environment"
    assert decision.matched_rule == "main_staging"
    assert decision.environment == "staging"
    assert decision.config["environment"] == "staging"


def test_no_match_is_no_action(engine: RuleEngine):
    decision = engine.evaluate("any-repo", "some-random-branch")

    assert decision.action == "no_action"
    assert decision.matched_rule is None
    assert decision.config == {}
    assert "some-random-branch" in decision.reason


def test_disabled_rule_is_skipped(tmp_path: Path):
    raw = """
version: "1.0"
global_ephemeral_enabled: true
default_ttl_hours: 8
defaults: {}
rules:
  - name: disabled_feature
    enabled: false
    branch_pattern: "feature/*"
    action: create_ephemeral
  - name: fallback_main
    enabled: true
    branch_pattern: "*"
    action: update_environment
    config:
      environment: staging
valid_actions: [create_ephemeral, update_environment, no_action]
valid_environments: [staging]
"""
    config_path = tmp_path / "disabled.yml"
    config_path.write_text(raw)
    engine = RuleEngine.load(config_path)

    decision = engine.evaluate("any-repo", "feature/whatever")

    # disabled_feature is skipped even though its pattern matches; fallback_main
    # (a "*" branch pattern) is the actual match.
    assert decision.matched_rule == "fallback_main"
    assert decision.action == "update_environment"


# ============================================================================
# global_ephemeral_enabled short-circuit
# ============================================================================


def test_global_ephemeral_disabled_short_circuits_to_no_action(tmp_path: Path):
    raw = """
version: "1.0"
global_ephemeral_enabled: false
default_ttl_hours: 8
defaults: {}
rules:
  - name: feature_branches
    enabled: true
    branch_pattern: "feature/*"
    action: create_ephemeral
valid_actions: [create_ephemeral, no_action]
valid_environments: []
"""
    config_path = tmp_path / "global-disabled.yml"
    config_path.write_text(raw)
    engine = RuleEngine.load(config_path)

    decision = engine.evaluate("any-repo", "feature/x")

    assert decision.action == "no_action"
    assert decision.matched_rule == "feature_branches"
    assert "globally disabled" in decision.reason.lower()


# ============================================================================
# repo pattern matching — case-insensitive glob
# ============================================================================


def test_repo_pattern_matching_is_case_insensitive(tmp_path: Path):
    raw = """
version: "1.0"
global_ephemeral_enabled: true
default_ttl_hours: 8
defaults: {}
rules:
  - name: core_only
    enabled: true
    repo_patterns: ["Exampleco-Core"]
    branch_pattern: "*"
    action: create_ephemeral
valid_actions: [create_ephemeral, no_action]
valid_environments: []
"""
    config_path = tmp_path / "repo-case.yml"
    config_path.write_text(raw)
    engine = RuleEngine.load(config_path)

    matched = engine.evaluate("EXAMPLECO-CORE", "main")
    unmatched = engine.evaluate("exampleco-other", "main")

    assert matched.matched_rule == "core_only"
    assert unmatched.action == "no_action"


# ============================================================================
# branch pattern matching — case-sensitive glob
# ============================================================================


def test_branch_pattern_matching_is_case_sensitive(tmp_path: Path):
    raw = """
version: "1.0"
global_ephemeral_enabled: true
default_ttl_hours: 8
defaults: {}
rules:
  - name: main_only
    enabled: true
    branch_pattern: "Main"
    action: create_ephemeral
valid_actions: [create_ephemeral, no_action]
valid_environments: []
"""
    config_path = tmp_path / "branch-case.yml"
    config_path.write_text(raw)
    engine = RuleEngine.load(config_path)

    matched = engine.evaluate("any-repo", "Main")
    unmatched = engine.evaluate("any-repo", "main")

    assert matched.matched_rule == "main_only"
    assert unmatched.action == "no_action"


# ============================================================================
# tag_pattern semantics: tag present + tag_pattern set -> branch NOT tried as
# fallback for that rule
# ============================================================================


def test_tag_present_and_matches_tag_pattern_wins(tmp_path: Path):
    raw = """
version: "1.0"
global_ephemeral_enabled: true
default_ttl_hours: 8
defaults: {}
rules:
  - name: release_tags
    enabled: true
    branch_pattern: "main"
    tag_pattern: "v*.*.*"
    action: staging_then_manual
    config:
      require_approval: true
valid_actions: [staging_then_manual, no_action]
valid_environments: []
"""
    config_path = tmp_path / "tag-match.yml"
    config_path.write_text(raw)
    engine = RuleEngine.load(config_path)

    decision = engine.evaluate("any-repo", "main", tag="v1.2.3")

    assert decision.action == "staging_then_manual"
    assert decision.matched_rule == "release_tags"
    assert decision.environment == "staging"


def test_tag_present_but_does_not_match_pattern_does_not_fall_back_to_branch(tmp_path: Path):
    """v1 semantics: when a tag is supplied AND the rule has a tag_pattern, branch is
    NOT tried as a fallback for that rule even though the branch pattern would
    otherwise match — the rule as a whole is skipped, falling through to the next
    rule (or no_action)."""
    raw = """
version: "1.0"
global_ephemeral_enabled: true
default_ttl_hours: 8
defaults: {}
rules:
  - name: release_tags
    enabled: true
    branch_pattern: "main"
    tag_pattern: "v*.*.*"
    action: staging_then_manual
valid_actions: [staging_then_manual, no_action]
valid_environments: []
"""
    config_path = tmp_path / "tag-mismatch.yml"
    config_path.write_text(raw)
    engine = RuleEngine.load(config_path)

    decision = engine.evaluate("any-repo", "main", tag="not-a-version-tag")

    assert decision.action == "no_action"
    assert decision.matched_rule is None


def test_no_tag_supplied_falls_through_to_branch_match_even_with_tag_pattern_set(tmp_path: Path):
    raw = """
version: "1.0"
global_ephemeral_enabled: true
default_ttl_hours: 8
defaults: {}
rules:
  - name: release_tags
    enabled: true
    branch_pattern: "main"
    tag_pattern: "v*.*.*"
    action: staging_then_manual
valid_actions: [staging_then_manual, no_action]
valid_environments: []
"""
    config_path = tmp_path / "no-tag.yml"
    config_path.write_text(raw)
    engine = RuleEngine.load(config_path)

    decision = engine.evaluate("any-repo", "main", tag=None)

    assert decision.action == "staging_then_manual"
    assert decision.matched_rule == "release_tags"


# ============================================================================
# first-match-wins rule order
# ============================================================================


def test_first_match_wins_over_later_broader_match(tmp_path: Path):
    raw = """
version: "1.0"
global_ephemeral_enabled: true
default_ttl_hours: 8
defaults: {}
rules:
  - name: specific_first
    enabled: true
    branch_pattern: "feature/*"
    action: create_ephemeral
    config:
      ttl_hours: 99
  - name: catch_all
    enabled: true
    branch_pattern: "*"
    action: update_environment
    config:
      environment: staging
valid_actions: [create_ephemeral, update_environment, no_action]
valid_environments: [staging]
"""
    config_path = tmp_path / "order.yml"
    config_path.write_text(raw)
    engine = RuleEngine.load(config_path)

    decision = engine.evaluate("any-repo", "feature/x")

    assert decision.matched_rule == "specific_first"
    assert decision.action == "create_ephemeral"


# ============================================================================
# defaults merging, keyed by action family — v1's _build_config
# ============================================================================


def test_defaults_merged_under_ephemeral_and_overridden_by_rule_config(tmp_path: Path):
    raw = """
version: "1.0"
global_ephemeral_enabled: true
default_ttl_hours: 8
defaults:
  ephemeral:
    ttl_hours: 4
    other_default: yes
rules:
  - name: feature_branches
    enabled: true
    branch_pattern: "feature/*"
    action: create_ephemeral
    config:
      ttl_hours: 1
valid_actions: [create_ephemeral, no_action]
valid_environments: []
"""
    config_path = tmp_path / "defaults-ephemeral.yml"
    config_path.write_text(raw)
    engine = RuleEngine.load(config_path)

    decision = engine.evaluate("any-repo", "feature/x")

    # rule-specific ttl_hours (1) wins over the ephemeral default (4); the
    # unrelated default key still comes through.
    assert decision.config["ttl_hours"] == 1
    assert decision.config["other_default"] is True


def test_defaults_merged_under_persistent_for_update_environment_and_staging_then_manual(
    tmp_path: Path,
):
    raw = """
version: "1.0"
global_ephemeral_enabled: true
default_ttl_hours: 8
defaults:
  persistent:
    replicas: 3
rules:
  - name: main_staging
    enabled: true
    branch_pattern: "main"
    action: update_environment
    config:
      environment: staging
valid_actions: [update_environment, no_action]
valid_environments: [staging]
"""
    config_path = tmp_path / "defaults-persistent.yml"
    config_path.write_text(raw)
    engine = RuleEngine.load(config_path)

    decision = engine.evaluate("any-repo", "main")

    assert decision.config["replicas"] == 3
    assert decision.config["environment"] == "staging"


# ============================================================================
# fail-fast load() — the one deliberate v2 deviation: no swallow, no ruleless run
# ============================================================================


def test_load_missing_file_raises():
    with pytest.raises(RuleValidationError):
        RuleEngine.load(_FIXTURES_DIR / "does-not-exist.yml")


def test_load_malformed_yaml_raises(tmp_path: Path):
    bad = tmp_path / "bad.yml"
    bad.write_text("rules: [this is not: valid: yaml: at all")

    with pytest.raises(RuleValidationError):
        RuleEngine.load(bad)


def test_load_non_mapping_yaml_raises(tmp_path: Path):
    bad = tmp_path / "list.yml"
    bad.write_text("- just\n- a\n- list\n")

    with pytest.raises(RuleValidationError):
        RuleEngine.load(bad)


def test_load_rule_missing_branch_pattern_raises(tmp_path: Path):
    bad = tmp_path / "missing-branch-pattern.yml"
    bad.write_text(
        """
version: "1.0"
global_ephemeral_enabled: true
default_ttl_hours: 8
defaults: {}
rules:
  - name: broken_rule
    enabled: true
    action: create_ephemeral
valid_actions: [create_ephemeral]
valid_environments: []
"""
    )

    with pytest.raises(RuleValidationError, match="branch_pattern"):
        RuleEngine.load(bad)


def test_load_rule_with_invalid_action_raises(tmp_path: Path):
    bad = tmp_path / "bad-action.yml"
    bad.write_text(
        """
version: "1.0"
global_ephemeral_enabled: true
default_ttl_hours: 8
defaults: {}
rules:
  - name: broken_rule
    enabled: true
    branch_pattern: "main"
    action: launch_the_missiles
valid_actions: [create_ephemeral, no_action]
valid_environments: []
"""
    )

    with pytest.raises(RuleValidationError, match="invalid action"):
        RuleEngine.load(bad)


def test_load_update_environment_missing_environment_config_raises(tmp_path: Path):
    bad = tmp_path / "missing-env.yml"
    bad.write_text(
        """
version: "1.0"
global_ephemeral_enabled: true
default_ttl_hours: 8
defaults: {}
rules:
  - name: broken_rule
    enabled: true
    branch_pattern: "main"
    action: update_environment
valid_actions: [update_environment]
valid_environments: [staging]
"""
    )

    with pytest.raises(RuleValidationError, match="missing environment config"):
        RuleEngine.load(bad)


def test_load_update_environment_invalid_environment_value_raises(tmp_path: Path):
    bad = tmp_path / "bad-env.yml"
    bad.write_text(
        """
version: "1.0"
global_ephemeral_enabled: true
default_ttl_hours: 8
defaults: {}
rules:
  - name: broken_rule
    enabled: true
    branch_pattern: "main"
    action: update_environment
    config:
      environment: nonexistent_env
valid_actions: [update_environment]
valid_environments: [staging, production]
"""
    )

    with pytest.raises(RuleValidationError, match="invalid environment"):
        RuleEngine.load(bad)


def test_a_live_engine_never_has_broken_rules():
    """There is no 'constructed but invalid' state — load() either raises or hands
    back a fully valid engine (module docstring: no swallow, ever)."""
    engine = RuleEngine.load(_FIXTURES_DIR / "deployment-rules.yml")

    assert len(engine.config.rules) > 0
    for rule in engine.config.rules:
        assert rule.action in engine.config.valid_actions


# ============================================================================
# real production config parses and behaves as expected (repo_patterns as a
# real list, branch_pattern as both a list and a bare string, tag_pattern)
# ============================================================================


def test_real_config_dir_deployment_rules_loads_and_matches():
    real_engine = RuleEngine.load(_CONFIG_DIR / "deployment-rules.yml")

    ephemeral = real_engine.evaluate("exampleco-core", "feature/FIN-1")
    assert ephemeral.action == "create_ephemeral"
    assert ephemeral.matched_rule == "main_stack_feature_branches"
    # Was `exampleco-stack` -- a name with no profile file behind it, so this rule raised
    # NOT_FOUND for every repo/branch it matched. This assertion pinned the routing
    # decision's VALUE and never that the value RESOLVES, so it locked the broken
    # reference in rather than catching it (the same shape as #13's
    # test_reuses_existing_active_cluster_..., which pinned the decision and never
    # asked whether the deployment it routed ever deployed). The consequence is now
    # covered by tests/app/test_profiles.py::
    # test_every_shipped_rule_names_a_profile_that_loads.
    assert ephemeral.config["deployment_profile"] == "exampleco-dev-stack-nodns"

    tagged = real_engine.evaluate("exampleco-core", "main", tag="v1.0.0")
    assert tagged.action == "staging_then_manual"
    assert tagged.matched_rule == "main_stack_production_tags"

    standalone = real_engine.evaluate("exampleco-web-2", "dev")
    assert standalone.action == "create_ephemeral"
    assert standalone.matched_rule == "exampleco_web_2"

    no_match = real_engine.evaluate("exampleco-web-2", "main")
    assert no_match.action == "no_action"
