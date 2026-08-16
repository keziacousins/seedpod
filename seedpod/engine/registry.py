"""engine/registry.py — StepRegistry: DI-built verb -> Step instance registry
(coherence-review glossary; docs/design/seam-b-engine.md §2.0: "Steps are constructed
with explicit DI at registry build time; no globals").

COORDINATION NOTE: this task's brief says "exposing the RegistryView the validator
needs (coordinate: config.py declares that protocol — if not present yet, define it
compatibly and note it)". ``engine/config.py`` landed in this tree concurrently (built
in parallel, uncommitted, discovered mid-task) and already declares its OWN
``RegistryView``/``VerbSpec`` Protocols there:

    class VerbSpec(Protocol):
        Params: type[BaseModel]; Output: type[BaseModel]; gateable: bool; undoable: bool
    class RegistryView(Protocol):
        def verb(self, name: str) -> VerbSpec | None: ...
        def resolve_type(self, type_expr: str) -> type | None: ...

This module does NOT redefine ``RegistryView`` (a second, differently-shaped protocol
of the same name in a sibling module would itself be a coherence bug). Instead
``StepRegistry`` below is shaped to satisfy config.py's protocols structurally:
``verb()`` returns the raw ``Step`` instance (a ``Step`` already carries ``Params``/
``Output``/``gateable``/``undoable`` as instance-readable ClassVars — no wrapper
needed), and ``resolve_type()`` maps a workflow ``inputs:`` type-name to a Python
type (``"str"``, ``"ClusterSpecification"``, ``"Optional[str]"``,
``"list[ManifestDoc]"``).

**Why ``resolve_type`` lives here.** An earlier revision of this module argued it is
"not verb-registry data" and left it out, deferring the pairing of a registry with a
separate type-name resolver to the composition root. The Round-8a gate (finding M-1)
established that this made ``config.validate_workflow(wf, real_registry)`` IMPOSSIBLE
to call in production at all — nothing anywhere implemented the second half of
``RegistryView``, so the only registry the shipped workflows were ever validated
against was the TEST fixture (``tests/engine/declared_verbs.py``). Nothing in CI
proved the shipped YAML matched the REAL verb catalog. Making ``StepRegistry`` a
complete ``RegistryView`` is what closes that: ``tests/engine/test_verb_conventions.py``
now validates every fully-registered shipped workflow against this object directly.

``resolve_type_expr`` is exported so the test fixtures resolve type names through the
SAME parser production uses — three independent copies of this ten-line grammar
(``declared_verbs.py``, ``fakes.py``, and the gate's own throwaway wrapper) was the
shape that let fixture and production "agree by luck".
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping

from pydantic import BaseModel

from seedpod.core.cluster_spec import ClusterSpecification
from seedpod.core.deploy_wave import (
    ApplyChangeSummary,
    DeploymentProfile,
    ManifestDoc,
    SnapshotRestoreSpec,
    Wave,
)
from seedpod.core.dns_record import DnsRecordRef
from seedpod.engine.schedule import Schedule
from seedpod.engine.step import Step

__all__ = ["UnknownVerbError", "StepRegistry", "NAMED_TYPES", "resolve_type_expr"]


class UnknownVerbError(KeyError):
    """Raised by StepRegistry.get for a verb with no registered Step."""


# Workflow ``inputs:`` type names -> Python types. Scalars plus every domain model a
# shipped workflow may name. Deliberately NOT auto-derived from the registered Steps'
# Params/Output models: a workflow input's type vocabulary is a declared, reviewable
# surface, and inferring it would let a new Step silently widen what YAML can declare.
#
# The five DR-0028 deploy-path DTOs (docs/decisions/DR-0028-deploy-path-dtos.md,
# seedpod/core/deploy_wave.py) join ClusterSpecification/DnsRecordRef here: no shipped
# workflow currently types an `inputs:` block with any of the five (grep-verified
# against config/workflows/*.yml — every `inputs:` entry today is `{type: str}`), but
# NAMED_TYPES is the declared, reviewable surface regardless of current use, and this
# round's own brief requires registering all five here, matching
# tests/engine/declared_verbs.py's fixture-local NAMED_TYPES (which must resolve every
# name identically — see resolve_type_expr's own docstring).
NAMED_TYPES: Mapping[str, type] = {
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "ClusterSpecification": ClusterSpecification,
    "DnsRecordRef": DnsRecordRef,
    "ManifestDoc": ManifestDoc,
    "DeploymentProfile": DeploymentProfile,
    "SnapshotRestoreSpec": SnapshotRestoreSpec,
    "Wave": Wave,
    "ApplyChangeSummary": ApplyChangeSummary,
}

_OPTIONAL_RE = re.compile(r"Optional\[(.+)\]")
_LIST_RE = re.compile(r"list\[(.+)\]")


def resolve_type_expr(expr: str, named: Mapping[str, type]) -> type | None:
    """A workflow ``inputs:`` ``type:`` string -> Python type, or None if the name is
    unrecognized (which ``config.validate_workflow`` reports as a V-rule violation
    rather than crashing). Grammar: a named type, ``Optional[<expr>]``, or
    ``list[<expr>]``, nestable. Shared verbatim with the test fixtures so production
    and fixture registries cannot drift apart on type identity."""
    expr = expr.strip()
    if expr in named:
        return named[expr]
    if (m := _OPTIONAL_RE.fullmatch(expr)) is not None:
        inner = resolve_type_expr(m.group(1), named)
        return (inner | None) if inner is not None else None
    if (m := _LIST_RE.fullmatch(expr)) is not None:
        inner = resolve_type_expr(m.group(1), named)
        return list[inner] if inner is not None else None
    return None


class StepRegistry:
    """Maps verb -> a single constructed Step instance. Construction (DI: providers,
    repositories, SecretManager, SubprocessManager) happens once, at composition-root
    build time — never inside a Step, never via a global lookup."""

    def __init__(self, steps: Mapping[str, Step], *, named_types: Mapping[str, type] | None = None) -> None:
        self._steps: dict[str, Step] = dict(steps)
        self._named_types: dict[str, type] = dict(NAMED_TYPES if named_types is None else named_types)

    @classmethod
    def for_tests(cls, *fake_steps: Step) -> StepRegistry:
        """Build a registry from already-constructed fake Step instances; each fake's
        ``verb`` ClassVar supplies its registry key, e.g.
        ``StepRegistry.for_tests(FakeApply(), FakeGate())``."""
        return cls({step.verb: step for step in fake_steps})

    def get(self, verb: str) -> Step:
        try:
            return self._steps[verb]
        except KeyError:
            raise UnknownVerbError(verb) from None

    def verb(self, name: str) -> Step | None:
        """Structurally satisfies ``engine/config.py``'s ``RegistryView.verb`` /
        ``VerbSpec`` protocols: returns the Step instance itself (None on miss,
        matching config.py's None-on-unregistered contract — unlike ``get()``, which
        raises)."""
        return self._steps.get(name)

    def resolve_type(self, type_expr: str) -> type | None:
        """The other half of ``engine/config.py``'s ``RegistryView`` — see this
        module's docstring for why it belongs on the registry (gate finding M-1)."""
        return resolve_type_expr(type_expr, self._named_types)

    def __contains__(self, verb: str) -> bool:
        return verb in self._steps

    def verbs(self) -> Iterable[str]:
        return self._steps.keys()

    def params_type(self, verb: str) -> type[BaseModel]:
        return self.get(verb).Params

    def output_type(self, verb: str) -> type[BaseModel]:
        return self.get(verb).Output

    def is_gateable(self, verb: str) -> bool:
        return self.get(verb).gateable

    def is_undoable(self, verb: str) -> bool:
        return self.get(verb).undoable

    def default_retry(self, verb: str) -> Schedule:
        return self.get(verb).default_retry

    def default_timeout_seconds(self, verb: str) -> int:
        return self.get(verb).default_timeout_seconds
