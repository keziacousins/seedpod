"""seedpod/services/rules.py — ``RuleEngine``: salvaged deployment-rule evaluation
behind the ``load()``/``evaluate()`` surface coherence-review §2's type glossary names
("salvaged rules, fail-fast load") and seam-d-foundation.md's Decision 8 composition
root wants (``rules = RuleEngine.load(config.config_dir / "deployment-rules.yml")``).

Salvaged from ``reference-code/seedpod/seedpod/orchestrator/rule_engine.py``
(``RuleEngine``/``Rule``/``RuleConfig``/``DeploymentDecision``/``RuleValidationError``,
the whole 459-line module) — the matching semantics below are copied byte-for-byte
because the acceptance suite (``tests/acceptance/test_deployment_flow.py``) depends on
them verbatim: first-match-wins rule order, case-insensitive glob ``repo_patterns``
(``fnmatch`` on lower-cased strings, v1 lines 285-291), case-sensitive glob
``branch_patterns``/``tag_pattern`` (v1 lines 306-311, 236-237), disabled rules skipped
(v1 lines 217-219), ``global_ephemeral_enabled`` short-circuiting ``create_ephemeral``
rules to a ``no_action`` decision (v1 lines 226-233), and defaults merged under
rule-specific config keyed by action family (v1's ``_build_config``, lines 313-334).

FAIL-FAST construction — the one deliberate, DOCUMENTED deviation from v1, and the
whole reason this module exists as `services/rules.py` rather than staying inline in an
orchestrator: v1's ``_load_rules`` (reference-code .../rule_engine.py:91-108) wrapped
its own ``raise RuleValidationError(...)`` in a bare ``except Exception:`` that just
LOGGED the failure and re-raised the SAME exception — which reads as fail-fast in
isolation, but every real caller of ``RuleEngine()`` at startup (v1's
``core/globals.py``-style singleton wiring) is documented by seam-d-foundation.md as
having swallowed that exception one level up and run the rest of the app RULELESS
(seam-d-foundation.md: "fail-fast `RuleEngine` construction fixes a real v1 swallow").
That swallow is explicitly NOT ported (CLAUDE.md's "don't pin v1 bugs" + this task's own
instruction): ``RuleEngine.load()`` raises ``RuleValidationError`` straight out of a
missing/malformed config file or a failed validation pass, and there is no "constructed
but broken" state — a live ``RuleEngine`` instance always has valid, ready rules.

Deliberately not ported (v1 API-layer conveniences with no v2 caller yet — this module
stays a pure ``load()``/``evaluate()`` surface, not a stateful admin-facing service):
``reload_rules``/``get_config_summary``/``validate_rules``/``is_rule_enabled``/
``get_matching_rules``/``get_all_rules`` (reference-code .../rule_engine.py:355-459).
A future caller that needs rule introspection can read the frozen ``RuleEngine.rules``
tuple directly; nothing here needs mutation or a config-file path held past construction.
"""

from __future__ import annotations

import fnmatch
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

__all__ = [
    "RuleValidationError",
    "Rule",
    "RuleConfig",
    "DeploymentDecision",
    "RuleEngine",
]


class RuleValidationError(Exception):
    """Raised by ``RuleEngine.load()`` when the rules file is missing, unparsable, or
    fails validation. Salvaged from reference-code .../rule_engine.py:66-68 — same
    name, same meaning, now actually fatal (see module docstring)."""


@dataclass(frozen=True)
class Rule:
    """Individual deployment rule. Salvaged from reference-code .../rule_engine.py:
    41-51, frozen (this module has no mutation surface)."""

    name: str
    description: str
    enabled: bool
    branch_patterns: tuple[str, ...]
    action: str
    config: Mapping[str, Any] = field(default_factory=dict)
    repo_patterns: tuple[str, ...] = ("*",)
    tag_pattern: str | None = None


@dataclass(frozen=True)
class RuleConfig:
    """Complete rule configuration loaded from YAML. Salvaged from reference-code
    .../rule_engine.py:54-63, frozen."""

    version: str
    global_ephemeral_enabled: bool
    default_ttl_hours: int
    defaults: Mapping[str, Mapping[str, Any]]
    rules: tuple[Rule, ...]
    valid_actions: tuple[str, ...]
    valid_environments: tuple[str, ...]


@dataclass(frozen=True)
class DeploymentDecision:
    """Result of rule evaluation. Salvaged from reference-code .../rule_engine.py:
    30-38, minus the ``cluster_id`` field — v1 declared it on the dataclass but
    ``evaluate_deployment`` never populated it (dead weight; a caller that wants to
    thread a cluster id through a decision can do so itself)."""

    action: str
    config: Mapping[str, Any]
    matched_rule: str | None = None
    reason: str = ""
    environment: str | None = None


def _as_tuple(value: str | list[str]) -> tuple[str, ...]:
    # v1 lines 119-131: repo_patterns/branch_pattern may be a bare string or a list in
    # the YAML; normalize to a tuple either way.
    return (value,) if isinstance(value, str) else tuple(value)


def _parse_config(raw: Mapping[str, Any]) -> RuleConfig:
    """Salvaged from reference-code .../rule_engine.py:110-153 (``_parse_config``)."""
    rules: list[Rule] = []
    for rule_data in raw.get("rules", []):
        if "branch_pattern" not in rule_data:
            name = rule_data.get("name", "unknown")
            raise RuleValidationError(f"Rule '{name}' missing required field 'branch_pattern'")

        rules.append(
            Rule(
                name=rule_data["name"],
                description=rule_data.get("description", ""),
                enabled=rule_data.get("enabled", True),
                branch_patterns=_as_tuple(rule_data["branch_pattern"]),
                action=rule_data["action"],
                config=rule_data.get("config", {}),
                repo_patterns=_as_tuple(rule_data.get("repo_patterns", ["*"])),
                tag_pattern=rule_data.get("tag_pattern"),
            )
        )

    return RuleConfig(
        version=str(raw.get("version", "1.0")),
        global_ephemeral_enabled=raw.get("global_ephemeral_enabled", True),
        default_ttl_hours=raw.get("default_ttl_hours", 8),
        defaults=raw.get("defaults", {}),
        rules=tuple(rules),
        valid_actions=tuple(raw.get("valid_actions", [])),
        valid_environments=tuple(raw.get("valid_environments", [])),
    )


def _validate(config: RuleConfig) -> list[str]:
    """Salvaged from reference-code .../rule_engine.py:155-190 (``_validate_config``),
    returning the error list instead of raising directly — ``load()`` is the single
    raise point."""
    errors: list[str] = []

    for rule in config.rules:
        if not rule.name:
            errors.append("Rule missing name")
        if not rule.branch_patterns:
            errors.append(f"Rule '{rule.name}' missing branch_patterns")
        if not rule.action:
            errors.append(f"Rule '{rule.name}' missing action")
        if not rule.repo_patterns:
            errors.append(f"Rule '{rule.name}' has empty repo_patterns list")

        if rule.action not in config.valid_actions:
            errors.append(f"Rule '{rule.name}' has invalid action '{rule.action}'")

        if rule.action == "update_environment":
            env = rule.config.get("environment")
            if not env:
                errors.append(f"Rule '{rule.name}' with update_environment action missing environment config")
            elif env not in config.valid_environments:
                errors.append(f"Rule '{rule.name}' has invalid environment '{env}'")

    return errors


class RuleEngine:
    """Deployment rule engine: branch/tag pattern matching -> deployment action.
    Construction is pure (no IO) — use ``RuleEngine.load()`` to read a config file."""

    def __init__(self, config: RuleConfig) -> None:
        self.config = config

    @classmethod
    def load(cls, path: Path) -> RuleEngine:
        """Read, parse, and validate ``path``. Raises ``RuleValidationError`` on ANY
        failure (missing file, bad YAML, failed validation) — fail-fast, see module
        docstring. Salvaged from reference-code .../rule_engine.py:91-108
        (``_load_rules``), minus the swallow."""
        try:
            raw = yaml.safe_load(path.read_text())
        except FileNotFoundError as e:
            raise RuleValidationError(f"Rules configuration file not found: {path}") from e
        except yaml.YAMLError as e:
            raise RuleValidationError(f"Failed to parse rules configuration {path}: {e}") from e

        if not isinstance(raw, Mapping):
            raise RuleValidationError(f"Rules configuration {path} did not parse to a mapping")

        try:
            config = _parse_config(raw)
        except (KeyError, TypeError) as e:
            raise RuleValidationError(f"Failed to parse rules configuration {path}: {e}") from e

        errors = _validate(config)
        if errors:
            joined = "\n".join(f"  - {e}" for e in errors)
            raise RuleValidationError(f"Rule validation errors:\n{joined}")

        return cls(config)

    def evaluate(self, repo: str, branch: str, tag: str | None = None) -> DeploymentDecision:
        """Evaluate rules in order (first match wins) and return a decision. Salvaged
        from reference-code .../rule_engine.py:192-272 (``evaluate_deployment``),
        matching logic verbatim:

        1. repo must match one of the rule's ``repo_patterns`` (case-insensitive).
        2. if ``global_ephemeral_enabled`` is False and the rule's action is
           ``create_ephemeral``, short-circuit to ``no_action`` (the rule "matched"
           but is globally suppressed).
        3. if a tag was supplied and the rule has a ``tag_pattern``, match against
           the tag and STOP considering branch for this rule either way (tag present +
           tag_pattern set but no match -> next rule, branch is not tried as a
           fallback for THIS rule).
        4. otherwise match against ``branch_patterns``.
        """
        for rule in self.config.rules:
            if not rule.enabled:
                continue

            if not self._matches_repo(repo, rule):
                continue

            if not self.config.global_ephemeral_enabled and rule.action == "create_ephemeral":
                return DeploymentDecision(
                    action="no_action",
                    config={},
                    reason="Ephemeral environments are globally disabled",
                    matched_rule=rule.name,
                )

            if tag and rule.tag_pattern:
                if fnmatch.fnmatch(tag, rule.tag_pattern):
                    return DeploymentDecision(
                        action=rule.action,
                        config=self._build_config(rule),
                        matched_rule=rule.name,
                        reason=(
                            f"Matched rule '{rule.name}' with repo pattern and tag pattern "
                            f"'{rule.tag_pattern}' (tag: {tag})"
                        ),
                        environment=self._resolve_environment(rule),
                    )
                continue

            if self._matches_branch(branch, rule):
                return DeploymentDecision(
                    action=rule.action,
                    config=self._build_config(rule),
                    matched_rule=rule.name,
                    reason=f"Matched rule '{rule.name}' with repo pattern and branch pattern (branch: {branch})",
                    environment=self._resolve_environment(rule),
                )

        reason = f"No deployment rule matches repo '{repo}', branch '{branch}'"
        if tag:
            reason += f", tag '{tag}'"
        return DeploymentDecision(action="no_action", config={}, reason=reason)

    def _matches_repo(self, repo: str, rule: Rule) -> bool:
        repo_lower = repo.lower()
        return any(fnmatch.fnmatch(repo_lower, pattern.lower()) for pattern in rule.repo_patterns)

    def _matches_branch(self, branch: str, rule: Rule) -> bool:
        return any(fnmatch.fnmatch(branch, pattern) for pattern in rule.branch_patterns)

    def _build_config(self, rule: Rule) -> dict[str, Any]:
        # v1 lines 313-334: defaults keyed by action family, then rule-specific config
        # on top.
        config: dict[str, Any] = {}
        if rule.action == "create_ephemeral" and "ephemeral" in self.config.defaults:
            config.update(self.config.defaults["ephemeral"])
        elif rule.action in ("update_environment", "staging_then_manual") and "persistent" in self.config.defaults:
            config.update(self.config.defaults["persistent"])
        config.update(rule.config)
        return config

    def _resolve_environment(self, rule: Rule) -> str | None:
        if rule.action == "create_ephemeral":
            return "ephemeral"
        if rule.action == "update_environment":
            return rule.config.get("environment")
        if rule.action == "staging_then_manual":
            return "staging"
        return None
