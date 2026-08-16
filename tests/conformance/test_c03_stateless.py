"""tests/conformance/test_c03_stateless.py — C-03 (Seam C §5.6 table).

    C-03 | test_stateless_no_upward_imports | all + services (⊂) | static: provider/service
    modules import nothing from seedpod.data, core.database, session providers, scheduler, or
    state manager (H18 by construction); same command on two fresh instances behaves
    identically (host keys travel in commands, not caches)

Two halves, per the table cell: a static import-graph check (no fake, no harness — a pure
AST scan of the source tree) and a runtime statelessness check parametrized over all six
providers.
"""

from __future__ import annotations

import ast
from pathlib import Path

import pytest

from tests.conformance._support import drain

# No blanket `pytestmark = pytest.mark.asyncio`: this module mixes sync (AST scan) and async
# (statelessness) tests; `asyncio_mode = "auto"` (pyproject.toml) already async-wraps the
# latter without it.

REPO_ROOT = Path(__file__).resolve().parents[2]
SCANNED_DIRS = (REPO_ROOT / "seedpod" / "providers", REPO_ROOT / "seedpod" / "services")

# Substrings of a dotted module path that make an import "upward" (CLAUDE.md: "Providers are
# stateless: no DB access, no retry/poll/sleep loops ... kubeconfig always passed in"; Seam C
# §5.6's C-03 cell names these five families verbatim).
FORBIDDEN_IMPORT_SUBSTRINGS = (
    "seedpod.data",
    "core.database",
    "session_provider",
    "sessionprovider",
    "scheduler",
    "state_manager",
    "statemanager",
)


def _module_names(node: ast.stmt) -> list[str]:
    if isinstance(node, ast.Import):
        return [alias.name for alias in node.names]
    if isinstance(node, ast.ImportFrom) and node.module:
        return [node.module]
    return []


def _provider_and_service_source_files() -> list[Path]:
    files: list[Path] = []
    for directory in SCANNED_DIRS:
        if directory.exists():
            files.extend(sorted(directory.glob("*.py")))
    return files


@pytest.mark.parametrize("path", _provider_and_service_source_files(), ids=lambda p: p.name)
def test_no_forbidden_upward_imports(path: Path):
    tree = ast.parse(path.read_text(), filename=str(path))
    offenders: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            for module in _module_names(node):
                lowered = module.lower()
                if any(forbidden in lowered for forbidden in FORBIDDEN_IMPORT_SUBSTRINGS):
                    offenders.append(module)
    assert not offenders, f"{path} imports upward from {offenders} — providers/services must stay DB/scheduler-free"


async def test_same_command_two_fresh_instances_behaves_identically(harness):
    """Statelessness at the instance level: two freshly-constructed providers, sharing only
    the harness's fake backend (never provider-internal state), must answer the same command
    identically — host keys, kubeconfigs, and every other per-call fact travel IN the command,
    never cached on ``self`` between ``execute()`` calls."""
    first = await drain(harness.provider(), harness.observe_command())
    second = await drain(harness.provider(), harness.observe_command())
    assert first == second
