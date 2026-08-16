"""``seedpod/app/services/rules_admin.py`` -- runtime enable/disable + reload for the
shared ``RuleEngine`` instance ``DeploymentService`` was constructed with (Round-6,
api-deployments component). Backs ``POST /api/rules/{name}/disable|enable`` and
``POST /api/rules/reload`` (``seedpod/api/routers/deployments.py``).

**Salvage note (LOUD, per CLAUDE.md "don't silently regress"):** grepping the whole
``reference-code/seedpod/seedpod/api/`` tree turns up ZERO ``disable``/``enable``
routes anywhere -- v1 only ever shipped a read-only ``GET /rules/{name}/status``
(``RuleEngine.is_rule_enabled``) and ``POST /rules/reload`` (``reload_rules``, a disk
re-read). The parity gate's own ``test_rule_disabled_flow`` (ported byte-for-byte from
v1's e2e suite) guards its disable-then-assert block behind ``if
rules_response.status_code == 200`` -- exactly the shape a client written against a
server that never implemented this endpoint would produce (silently skips its
assertions on a 404). This round's brief asks for a WORKING pair regardless
("Salvage the rule enable/disable from v1 (config/rules mutation)") -- read as
"build the admin capability v1's own rule-engine vocabulary (``enabled``,
``reload_rules``) implies but never exposed over HTTP," not as porting a literal
endpoint that never existed. This module is therefore genuinely new-in-v2 plumbing.

``seedpod/services/rules.py`` is deliberately NOT edited -- that module's own
docstring is explicit: "this module stays a pure ``load()``/``evaluate()`` surface,
not a stateful admin-facing service... A future caller that needs rule introspection
can read the frozen ``RuleEngine.rules`` tuple directly; nothing here needs mutation."
This module takes that invitation one step further, from the OUTSIDE: ``RuleEngine.__init__``
assigns a plain, public ``self.config`` attribute (not a dataclass field, not
underscored, never itself reassigned again by the class) -- reassigning it here via
``dataclasses.replace`` on the (frozen) ``RuleConfig``/``Rule`` values ``RuleEngine``
already hands back is using that public surface, not editing the committed module.
Mutating happens on the SAME ``RuleEngine`` object ``DeploymentService`` (and
``App.rules``, ``seedpod/app/app.py``) already hold a reference to, so the very next
``RuleEngine.evaluate()`` call (which reads ``self.config.rules`` fresh every time)
observes the change immediately -- no new seam, no second source of truth, no cache to
invalidate.
"""

from __future__ import annotations

import dataclasses
from pathlib import Path

from seedpod.services.rules import RuleEngine

__all__ = ["set_rule_enabled", "reload_rules", "rules_summary"]


def set_rule_enabled(engine: RuleEngine, name: str, *, enabled: bool) -> bool:
    """Flip the named rule's ``enabled`` flag in place. Returns ``False`` (a no-op --
    the router turns that into a 404) if no rule by that name exists."""
    found = False
    new_rules = []
    for rule in engine.config.rules:
        if rule.name == name:
            found = True
            rule = dataclasses.replace(rule, enabled=enabled)
        new_rules.append(rule)
    if not found:
        return False
    engine.config = dataclasses.replace(engine.config, rules=tuple(new_rules))
    return True


def reload_rules(engine: RuleEngine, path: Path) -> None:
    """Re-read ``path`` from disk (``RuleEngine.load``'s own fail-fast parse +
    validate) and replace ``engine``'s live config in place -- ANY manual
    disable/enable done since the last load is discarded, matching v1's
    ``reload_rules`` semantics (a full reload from the file, not a merge).
    Raises ``RuleValidationError`` straight through on a missing/invalid file
    (the router maps that to 400); ``engine`` is left untouched on failure
    (the new ``RuleEngine`` is fully constructed before anything is assigned)."""
    engine.config = RuleEngine.load(path).config


def rules_summary(engine: RuleEngine) -> dict[str, object]:
    """The ``GET /api/config/overview`` ``rules{...}`` shape (ui-contract), reused
    verbatim as the ``POST /api/rules/reload`` response's ``summary`` field --
    same concept, same caller-facing shape, one formatter."""
    cfg = engine.config
    enabled = [r.name for r in cfg.rules if r.enabled]
    disabled = [r.name for r in cfg.rules if not r.enabled]
    return {
        "version": cfg.version,
        "total": len(cfg.rules),
        "enabled": len(enabled),
        "disabled": len(disabled),
        "global_ephemeral_enabled": cfg.global_ephemeral_enabled,
        "enabled_rules": enabled,
        "disabled_rules": disabled,
    }
