"""The frozen-grammar YAML parser + validator (Pillar 2).

Owns docs/design/seam-b-engine.md section 2.2 in full (the grammar block and
validator rules V1-V10), amended by docs/design/coherence-review.md Conflicts
9/10/12/14 for what well-formed provision/deploy-rollback files look like (this
module does not ship those YAML files -- that is a later task -- but its grammar
must parse them unmodified).

**The grammar is frozen** (CLAUDE.md hard rule, restated by Seam B 2.2): no
``if``/``when``/``for``/``env``/``run:``/templating, ever. A scalar containing
``${`` anywhere is a hard load error (V10) -- wanting interpolation is the stop
signal, not a feature request.

Two-phase pipeline, both pure (no IO, no ``now()``, no DB):

1. ``parse_workflow(text)`` -- YAML text -> a frozen typed AST (``WorkflowDefinition``),
   enforcing the purely structural rules that need no verb knowledge: unknown-key
   rejection anywhere (V10), the ``${`` scalar ban (V10), the forbidden
   compensation-as-YAML keys (V7), and id/retry/timeout bounds (V9).
2. ``validate_workflow(ast, registry)`` -- the semantic rules that need a verb
   registry: verb existence (V1), ``with:`` key/type-checking against the verb's
   ``Params`` (V2, V4), ``Ref`` lexical scoping (V3), ``foreach.items`` list typing
   (V5), ``gate:`` only on gateable verbs (V6), and event-name/outcome-payload
   checking against the Pillar-1 event union (V8).

``RegistryView``/``VerbSpec`` are minimal read-only protocols so this module never
imports the real ``engine/registry.py`` (a later task) -- tests supply fakes.

``load_workflow(text, registry)`` runs both phases and returns the AST, or raises
``ConfigError`` on the *first* violation encountered (fail-fast: "unknown keys
anywhere = load error" reads naturally as "the file doesn't load", not "collect
every problem").
"""

from __future__ import annotations

import dataclasses
import typing
from collections.abc import Mapping
from dataclasses import dataclass, field
from types import UnionType
from typing import Any, Protocol, Union, get_args, get_origin

import yaml
from pydantic import BaseModel

from seedpod.core.events import EVENT_REGISTRY
from seedpod.engine.schedule import NAMED_POLICIES

__all__ = [
    "ConfigError",
    "Ref",
    "InputDef",
    "OutcomeDef",
    "Outcome",
    "GateDef",
    "EmitDef",
    "ExplicitRetry",
    "StepDef",
    "ForeachDef",
    "Entry",
    "WorkflowDefinition",
    "VerbSpec",
    "RegistryView",
    "parse_workflow",
    "validate_workflow",
    "load_workflow",
]

_DOLLAR_BRACE = "${"

# Bounds for V9 ("retry values positive and bounded"); the grammar text names no
# numbers, so these are this module's chosen sane caps -- generous enough that no
# real verb hits them, tight enough to catch a typo'd extra zero.
_MAX_RETRY_ATTEMPTS = 20
_MAX_DELAY_SECONDS = 3600.0
_MAX_TIMEOUT_SECONDS = 24 * 3600

_WORKFLOW_KEYS = {"workflow", "version", "inputs", "on_failure", "outcome", "steps"}
_WORKFLOW_REQUIRED = {"workflow", "version", "on_failure", "outcome", "steps"}
_INPUT_KEYS = {"type", "secret"}
_OUTCOME_BLOCK_KEYS = {"event", "payload"}
_STEP_KEYS = {"id", "uses", "with", "retry", "timeout_seconds", "gate", "on_failure", "emit"}
_FOREACH_KEYS = {"id", "foreach", "body"}
_FOREACH_SPEC_KEYS = {"items", "as"}
_GATE_KEYS = {"timeout_seconds", "interval_seconds", "max_consecutive_poll_failures", "settle_seconds"}
_EMIT_KEYS = {"event", "payload"}
_RETRY_KEYS = {"max_attempts", "base_delay_seconds", "factor", "max_delay_seconds"}
_ON_FAILURE_WORKFLOW = {"compensate", "report"}
_ON_FAILURE_STEP = {"abort", "continue"}

# V7: "undoable is a verb property, not YAML" -- these keys don't exist in the
# grammar at all, but calling them out by name (instead of falling through to the
# generic V10 unknown-key error) makes the violation's *meaning* legible.
_FORBIDDEN_COMPENSATION_KEYS = {"undo", "undoable", "compensate", "compensator"}

# A sentinel scope entry for ids that occupy the namespace (V9 uniqueness) but are
# not themselves referenceable as a Ref source -- e.g. a ForeachDef's own id has no
# Output (only its body steps do).
_UNREFERENCEABLE = object()


class ConfigError(Exception):
    """One grammar or validation violation.

    ``rule`` is the Seam B 2.2 V-rule ("V1".."V10") the violation maps to, or
    "grammar" for purely structural violations the numbered rules don't name
    directly (e.g. a missing required top-level key).
    """

    def __init__(self, rule: str, message: str) -> None:
        super().__init__(f"[{rule}] {message}")
        self.rule = rule
        self.message = message


# ---------------------------------------------------------------------------
# The frozen typed AST
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Ref:
    """``{from: <path>}`` -- the only place data flows between steps."""

    path: str


# A "Value" is a literal (str/int/float/bool/list/dict, recursively containing no
# Ref -- the grammar is explicit that a Ref is only ever the *entire* value of a
# param) or a Ref. Python's type system can't express that constraint on a type
# alias, so this is `Any`; `_parse_value` is where the constraint is enforced.
Value = Any


@dataclass(frozen=True, slots=True)
class InputDef:
    name: str
    type: str
    secret: bool = False


@dataclass(frozen=True, slots=True)
class OutcomeDef:
    event: str
    payload: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class Outcome:
    succeeded: OutcomeDef
    failed: OutcomeDef
    cancelled: OutcomeDef


@dataclass(frozen=True, slots=True)
class GateDef:
    timeout_seconds: Any  # int | Ref -- Ref must type-check to int (V4)
    interval_seconds: int = 5
    max_consecutive_poll_failures: int = 3
    # DR-0022 Erratum E2: seam-c-provider.md:445 already specifies settle_seconds
    # as a gate parameter ("preserved as data, deleted as sleeps") -- GateDef
    # never implemented it until now. A post-Ready grace (NOT a poll interval --
    # folding it into interval_seconds does not reproduce it, since the gate
    # polls at t=0), inspired by v1's destruction_job.py:164-181 (a few extra
    # seconds so Tailscale can send its disconnect before the step completes).
    # Erratum E8 (accepted divergence from v1): the engine honors this on ANY
    # gate Ready, not only "the delete actually removed something" -- v1 slept
    # only when returncode == 0, explicitly not on the NotFound branch; this
    # gate has no way to see *why* a probe reported Ready (nor should it --
    # that would push step semantics into the engine), so it settles uniformly.
    settle_seconds: int = 0


@dataclass(frozen=True, slots=True)
class EmitDef:
    event: str
    payload: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ExplicitRetry:
    max_attempts: int
    base_delay_seconds: float = 5.0
    factor: float = 2.0
    max_delay_seconds: float = 60.0


@dataclass(frozen=True, slots=True)
class StepDef:
    id: str
    uses: str
    with_: Mapping[str, Any] = field(default_factory=dict)
    retry: str | ExplicitRetry | None = None
    timeout_seconds: int | None = None
    gate: GateDef | None = None
    on_failure: str = "abort"  # 'abort' | 'continue'
    emit: EmitDef | None = None


@dataclass(frozen=True, slots=True)
class ForeachDef:
    id: str
    items: Ref
    as_: str
    body: tuple[StepDef, ...]


Entry = StepDef | ForeachDef


@dataclass(frozen=True, slots=True)
class WorkflowDefinition:
    workflow: str
    version: int
    inputs: Mapping[str, InputDef]
    on_failure: str  # 'compensate' | 'report'
    outcome: Outcome
    steps: tuple[Entry, ...]


# ---------------------------------------------------------------------------
# RegistryView -- the seam that keeps the validator pure
# ---------------------------------------------------------------------------


class VerbSpec(Protocol):
    """What the validator needs to know about one registered verb."""

    Params: type[BaseModel]
    Output: type[BaseModel]
    gateable: bool
    undoable: bool


class RegistryView(Protocol):
    """Read-only view onto the verb registry (``engine/registry.py``, a later
    task) so this module never imports real steps -- tests supply fakes."""

    def verb(self, name: str) -> VerbSpec | None:
        """The VerbSpec for a registered verb, or None if unregistered (V1)."""
        ...

    def resolve_type(self, type_expr: str) -> type | None:
        """Resolve a YAML ``type:`` string (workflow ``inputs``) to a Python
        type, or None if unrecognized. Scalars, ``Optional[...]``/``list[...]``
        wrappers, and named registered models are all valid forms -- interpreting
        the string is entirely the registry's responsibility."""
        ...


# ---------------------------------------------------------------------------
# Phase 1: structural parse (no registry needed)
# ---------------------------------------------------------------------------


def _scan_interpolation(node: Any) -> None:
    """V10: any scalar containing '${' anywhere in the file is a hard error."""
    if isinstance(node, str):
        if _DOLLAR_BRACE in node:
            raise ConfigError("V10", f"interpolation marker '${{' found in scalar: {node!r}")
    elif isinstance(node, dict):
        for k, v in node.items():
            if isinstance(k, str) and _DOLLAR_BRACE in k:
                raise ConfigError("V10", f"interpolation marker '${{' found in key: {k!r}")
            _scan_interpolation(v)
    elif isinstance(node, list):
        for item in node:
            _scan_interpolation(item)


def _check_keys(d: Mapping[str, Any], allowed: set[str], required: set[str], ctx: str) -> None:
    for k in d:
        if k in _FORBIDDEN_COMPENSATION_KEYS:
            raise ConfigError(
                "V7", f"{ctx}: '{k}' is not a grammar key -- undoable is a verb property, not YAML"
            )
        if k not in allowed:
            raise ConfigError("V10", f"{ctx}: unknown key '{k}'")
    missing = required - set(d)
    if missing:
        raise ConfigError("grammar", f"{ctx}: missing required key(s) {sorted(missing)}")


def _is_ref(value: Any) -> bool:
    return isinstance(value, dict) and set(value) == {"from"} and isinstance(value["from"], str)


def _assert_literal(value: Any) -> Any:
    """A Ref is only ever the entire value of a param -- reject nested Refs."""
    if _is_ref(value):
        raise ConfigError(
            "grammar", "a Ref ({from: ...}) must be the entire value of a param -- nested Refs are not allowed"
        )
    if isinstance(value, dict):
        for v in value.values():
            _assert_literal(v)
    elif isinstance(value, list):
        for v in value:
            _assert_literal(v)
    return value


def _parse_value(value: Any) -> Any:
    if _is_ref(value):
        return Ref(path=value["from"])
    return _assert_literal(value)


def _is_pos_int(v: Any, bound: int) -> bool:
    return isinstance(v, int) and not isinstance(v, bool) and 0 < v <= bound


def _is_nonneg_int(v: Any, bound: int) -> bool:
    return isinstance(v, int) and not isinstance(v, bool) and 0 <= v <= bound


def _is_pos_number(v: Any, bound: float) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool) and 0 < v <= bound


def _parse_gate(raw: Any, ctx: str) -> GateDef:
    if not isinstance(raw, dict):
        raise ConfigError("grammar", f"{ctx}: must be a mapping")
    _check_keys(raw, _GATE_KEYS, {"timeout_seconds"}, ctx)
    timeout = _parse_value(raw["timeout_seconds"])
    interval = raw.get("interval_seconds", 5)
    max_fail = raw.get("max_consecutive_poll_failures", 3)
    settle = raw.get("settle_seconds", 0)
    if not _is_pos_int(interval, _MAX_TIMEOUT_SECONDS):
        raise ConfigError("V9", f"{ctx}.interval_seconds: must be a positive bounded int")
    if not _is_pos_int(max_fail, 1000):
        raise ConfigError("V9", f"{ctx}.max_consecutive_poll_failures: must be a positive bounded int")
    if not _is_nonneg_int(settle, _MAX_TIMEOUT_SECONDS):
        raise ConfigError("V9", f"{ctx}.settle_seconds: must be a non-negative bounded int")
    if isinstance(timeout, Ref):
        pass  # type-checked against int in the semantic phase (V4)
    elif not _is_pos_int(timeout, _MAX_TIMEOUT_SECONDS):
        raise ConfigError("V9", f"{ctx}.timeout_seconds: must be a positive bounded int or a Ref")
    return GateDef(
        timeout_seconds=timeout, interval_seconds=interval, max_consecutive_poll_failures=max_fail, settle_seconds=settle
    )


def _parse_retry(raw: Any, ctx: str) -> str | ExplicitRetry | None:
    if raw is None:
        return None
    if isinstance(raw, str):
        if not raw:
            raise ConfigError("grammar", f"{ctx}.retry: policy name must be non-empty")
        if raw not in NAMED_POLICIES:
            raise ConfigError(
                "V9", f"{ctx}.retry: '{raw}' is not a registered named policy (NAMED_POLICIES: {sorted(NAMED_POLICIES)})"
            )
        return raw
    if isinstance(raw, dict):
        _check_keys(raw, _RETRY_KEYS, {"max_attempts"}, f"{ctx}.retry")
        max_attempts = raw["max_attempts"]
        base = raw.get("base_delay_seconds", 5.0)
        factor = raw.get("factor", 2.0)
        max_delay = raw.get("max_delay_seconds", 60.0)
        if not _is_pos_int(max_attempts, _MAX_RETRY_ATTEMPTS):
            raise ConfigError("V9", f"{ctx}.retry.max_attempts: must be an int in [1, {_MAX_RETRY_ATTEMPTS}]")
        for name, val in (("base_delay_seconds", base), ("factor", factor), ("max_delay_seconds", max_delay)):
            if not _is_pos_number(val, _MAX_DELAY_SECONDS):
                raise ConfigError("V9", f"{ctx}.retry.{name}: must be a positive bounded number")
        return ExplicitRetry(
            max_attempts=max_attempts,
            base_delay_seconds=float(base),
            factor=float(factor),
            max_delay_seconds=float(max_delay),
        )
    raise ConfigError("grammar", f"{ctx}.retry: must be a policy name (str) or a mapping")


def _parse_event_block(raw: Any, allowed: set[str], ctx: str) -> tuple[str, dict[str, Any]]:
    if not isinstance(raw, dict):
        raise ConfigError("grammar", f"{ctx}: must be a mapping")
    _check_keys(raw, allowed, {"event"}, ctx)
    event = raw["event"]
    if not isinstance(event, str) or not event:
        raise ConfigError("grammar", f"{ctx}.event: must be a non-empty string")
    payload_raw = raw.get("payload", {})
    if not isinstance(payload_raw, dict):
        raise ConfigError("grammar", f"{ctx}.payload: must be a mapping")
    payload = {k: _parse_value(v) for k, v in payload_raw.items()}
    return event, payload


def _parse_emit(raw: Any, ctx: str) -> EmitDef:
    event, payload = _parse_event_block(raw, _EMIT_KEYS, ctx)
    return EmitDef(event=event, payload=payload)


def _parse_outcome_block(raw: Any, ctx: str) -> OutcomeDef:
    event, payload = _parse_event_block(raw, _OUTCOME_BLOCK_KEYS, ctx)
    return OutcomeDef(event=event, payload=payload)


def _parse_step(raw: Any, ctx: str) -> StepDef:
    if not isinstance(raw, dict):
        raise ConfigError("grammar", f"{ctx}: step must be a mapping")
    _check_keys(raw, _STEP_KEYS, {"id", "uses"}, ctx)
    step_id = raw["id"]
    uses = raw["uses"]
    if not isinstance(step_id, str) or not step_id:
        raise ConfigError("grammar", f"{ctx}.id: must be a non-empty string")
    if not isinstance(uses, str) or not uses:
        raise ConfigError("grammar", f"{ctx}.uses: must be a non-empty string")
    with_raw = raw.get("with", {})
    if not isinstance(with_raw, dict):
        raise ConfigError("grammar", f"{ctx}.with: must be a mapping")
    with_ = {k: _parse_value(v) for k, v in with_raw.items()}
    retry = _parse_retry(raw.get("retry"), ctx)
    timeout_seconds = raw.get("timeout_seconds")
    if timeout_seconds is not None and not _is_pos_int(timeout_seconds, _MAX_TIMEOUT_SECONDS):
        raise ConfigError("V9", f"{ctx}.timeout_seconds: must be a positive bounded int")
    gate = _parse_gate(raw["gate"], f"{ctx}.gate") if "gate" in raw else None
    on_failure = raw.get("on_failure", "abort")
    if on_failure not in _ON_FAILURE_STEP:
        raise ConfigError("grammar", f"{ctx}.on_failure: must be one of {sorted(_ON_FAILURE_STEP)}")
    emit = _parse_emit(raw["emit"], f"{ctx}.emit") if "emit" in raw else None
    return StepDef(
        id=step_id,
        uses=uses,
        with_=with_,
        retry=retry,
        timeout_seconds=timeout_seconds,
        gate=gate,
        on_failure=on_failure,
        emit=emit,
    )


def _parse_foreach(raw: Any, ctx: str) -> ForeachDef:
    _check_keys(raw, _FOREACH_KEYS, {"id", "foreach", "body"}, ctx)
    foreach_id = raw["id"]
    if not isinstance(foreach_id, str) or not foreach_id:
        raise ConfigError("grammar", f"{ctx}.id: must be a non-empty string")
    spec = raw["foreach"]
    if not isinstance(spec, dict):
        raise ConfigError("grammar", f"{ctx}.foreach: must be a mapping")
    _check_keys(spec, _FOREACH_SPEC_KEYS, _FOREACH_SPEC_KEYS, f"{ctx}.foreach")
    items_raw = spec["items"]
    if not _is_ref(items_raw):
        raise ConfigError("grammar", f"{ctx}.foreach.items: must be a Ref ({{from: ...}})")
    items = Ref(path=items_raw["from"])
    as_ = spec["as"]
    if not isinstance(as_, str) or not as_:
        raise ConfigError("grammar", f"{ctx}.foreach.as: must be a non-empty string")
    body_raw = raw["body"]
    if not isinstance(body_raw, list) or not body_raw:
        raise ConfigError("grammar", f"{ctx}.body: must be a non-empty list")
    body = tuple(_parse_step(item, f"{ctx}.body[{i}]") for i, item in enumerate(body_raw))
    return ForeachDef(id=foreach_id, items=items, as_=as_, body=body)


def _parse_entry(raw: Any, ctx: str) -> Entry:
    if not isinstance(raw, dict):
        raise ConfigError("grammar", f"{ctx}: entry must be a mapping")
    if "foreach" in raw:
        return _parse_foreach(raw, ctx)
    return _parse_step(raw, ctx)


def _parse_inputs(raw: Any) -> dict[str, InputDef]:
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError("grammar", "inputs: must be a mapping")
    out: dict[str, InputDef] = {}
    for name, spec in raw.items():
        if not isinstance(spec, dict):
            raise ConfigError("grammar", f"inputs.{name}: must be a mapping")
        _check_keys(spec, _INPUT_KEYS, {"type"}, f"inputs.{name}")
        type_str = spec["type"]
        secret = spec.get("secret", False)
        if not isinstance(type_str, str) or not type_str:
            raise ConfigError("grammar", f"inputs.{name}.type: must be a non-empty string")
        if not isinstance(secret, bool):
            raise ConfigError("grammar", f"inputs.{name}.secret: must be a bool")
        out[name] = InputDef(name=name, type=type_str, secret=secret)
    return out


def _parse_outcome(raw: Any) -> Outcome:
    if not isinstance(raw, dict):
        raise ConfigError("grammar", "outcome: must be a mapping")
    required = {"succeeded", "failed", "cancelled"}
    _check_keys(raw, required, required, "outcome")
    return Outcome(
        succeeded=_parse_outcome_block(raw["succeeded"], "outcome.succeeded"),
        failed=_parse_outcome_block(raw["failed"], "outcome.failed"),
        cancelled=_parse_outcome_block(raw["cancelled"], "outcome.cancelled"),
    )


def parse_workflow(text: str) -> WorkflowDefinition:
    """YAML text -> frozen typed AST. Pure structural parse; no registry needed.

    Enforces V7, V9 (ids' uniqueness is a semantic-phase concern; retry/timeout
    bounds are structural), V10, and grammar shape. Raises ``ConfigError`` on the
    first violation.
    """
    try:
        raw = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise ConfigError("grammar", f"invalid YAML: {exc}") from exc
    if not isinstance(raw, dict):
        raise ConfigError("grammar", "workflow document must be a mapping")
    _scan_interpolation(raw)
    _check_keys(raw, _WORKFLOW_KEYS, _WORKFLOW_REQUIRED, "<root>")
    workflow = raw["workflow"]
    if not isinstance(workflow, str) or not workflow:
        raise ConfigError("grammar", "workflow: must be a non-empty string")
    version = raw["version"]
    if not _is_pos_int(version, 1_000_000):
        raise ConfigError("grammar", "version: must be a positive int")
    inputs = _parse_inputs(raw.get("inputs"))
    on_failure = raw["on_failure"]
    if on_failure not in _ON_FAILURE_WORKFLOW:
        raise ConfigError("grammar", f"on_failure: must be one of {sorted(_ON_FAILURE_WORKFLOW)}")
    outcome = _parse_outcome(raw["outcome"])
    steps_raw = raw["steps"]
    if not isinstance(steps_raw, list) or not steps_raw:
        raise ConfigError("grammar", "steps: must be a non-empty list")
    steps = tuple(_parse_entry(item, f"steps[{i}]") for i, item in enumerate(steps_raw))
    return WorkflowDefinition(
        workflow=workflow, version=version, inputs=inputs, on_failure=on_failure, outcome=outcome, steps=steps
    )


# ---------------------------------------------------------------------------
# Phase 2: semantic validation (needs a RegistryView)
# ---------------------------------------------------------------------------


def _strip_optional(t: Any) -> tuple[Any, bool]:
    origin = get_origin(t)
    if origin in (Union, UnionType):
        args = get_args(t)
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1 and type(None) in args:
            return non_none[0], True
    return t, False


def _types_equal(a: Any, b: Any) -> bool:
    if a is b or a == b:
        return True
    oa, ob = get_origin(a), get_origin(b)
    if oa is not None and ob is not None and oa == ob:
        aa, ab = get_args(a), get_args(b)
        return len(aa) == len(ab) and all(_types_equal(x, y) for x, y in zip(aa, ab, strict=True))
    return False


def _is_assignable(source: Any, target: Any) -> bool:
    """V4's type-compatibility rule, including the Optional binding rule:
    ``Optional[T]`` sources bind only ``Optional[T]`` params (a required ``T``
    may still widen into an ``Optional[T]`` param)."""
    s_inner, s_opt = _strip_optional(source)
    t_inner, t_opt = _strip_optional(target)
    if s_opt and not t_opt:
        return False
    return _types_equal(s_inner, t_inner)


def _fields_of(t: Any) -> Mapping[str, Any] | None:
    """Field-name -> annotation for a BaseModel or dataclass type; None if `t`
    isn't a structured type a Ref path can drill into."""
    if isinstance(t, type) and issubclass(t, BaseModel):
        return {name: f.annotation for name, f in t.model_fields.items()}
    if dataclasses.is_dataclass(t):
        hints = typing.get_type_hints(t)
        return {f.name: hints[f.name] for f in dataclasses.fields(t)}
    return None


def _resolve_ref_type(
    ref: Ref, scope: Mapping[str, Any], wf: WorkflowDefinition, registry: RegistryView, ctx: str
) -> Any:
    """V3 (lexical scoping) + the type-walk V4 needs. Raises ConfigError."""
    segments = ref.path.split(".")
    if not segments or not all(segments):
        raise ConfigError("V3", f"{ctx}: malformed ref path '{ref.path}'")
    head, *rest = segments
    if head == "run":
        if not rest:
            raise ConfigError("V3", f"{ctx}: 'run' refs must name an input, got '{ref.path}'")
        input_name, *tail = rest
        input_def = wf.inputs.get(input_name)
        if input_def is None:
            raise ConfigError("V3", f"{ctx}: unknown workflow input 'run.{input_name}' (ref '{ref.path}')")
        cur = registry.resolve_type(input_def.type)
        if cur is None:
            raise ConfigError("V4", f"{ctx}: cannot resolve type '{input_def.type}' for input '{input_name}'")
        remaining = tail
    else:
        if head not in scope:
            raise ConfigError(
                "V3",
                f"{ctx}: '{head}' does not resolve to a lexically earlier step, alias, or run input "
                f"(ref '{ref.path}')",
            )
        cur = scope[head]
        if cur is _UNREFERENCEABLE:
            raise ConfigError("V4", f"{ctx}: '{head}' has no Output to read fields from (ref '{ref.path}')")
        remaining = rest
    for seg in remaining:
        fields = _fields_of(cur)
        if fields is None or seg not in fields:
            raise ConfigError("V4", f"{ctx}: '{seg}' is not a field on the type resolved from '{ref.path}'")
        cur = fields[seg]
    return cur


def _validate_event_block(
    event: str,
    payload: Mapping[str, Any],
    scope: Mapping[str, Any],
    wf: WorkflowDefinition,
    registry: RegistryView,
    ctx: str,
) -> None:
    """V8: event names against the Pillar-1 event union; payload Ref types."""
    if event not in EVENT_REGISTRY:
        raise ConfigError("V8", f"{ctx}: '{event}' is not a registered Pillar-1 event")
    event_fields = _fields_of(EVENT_REGISTRY[event]) or {}
    payload_fields = {k: v for k, v in event_fields.items() if k not in ("at", "actor")}
    extras = set(payload) - set(payload_fields)
    if extras:
        raise ConfigError("V8", f"{ctx}: payload has keys not on event {event}: {sorted(extras)}")
    for k, value in payload.items():
        if isinstance(value, Ref):
            source_type = _resolve_ref_type(value, scope, wf, registry, f"{ctx}.payload.{k}")
            target_type = payload_fields[k]
            if not _is_assignable(source_type, target_type):
                raise ConfigError(
                    "V8",
                    f"{ctx}.payload.{k}: '{value.path}' ({source_type!r}) is not assignable "
                    f"to event field type {target_type!r}",
                )


def validate_workflow(wf: WorkflowDefinition, registry: RegistryView) -> None:
    """Semantic validation (V1, V2, V3, V4, V5, V6, V8, and the V9 id-uniqueness
    half). Raises ``ConfigError`` on the first violation."""

    for name, inp in wf.inputs.items():
        if registry.resolve_type(inp.type) is None:
            raise ConfigError("V4", f"inputs.{name}: cannot resolve type '{inp.type}'")

    def register(name: str, typ: Any, s: dict[str, Any], ctx: str) -> None:
        if name in s:
            raise ConfigError("V9", f"{ctx}: duplicate id '{name}' in scope")
        s[name] = typ

    def validate_step(step: StepDef, s: dict[str, Any], ctx: str) -> None:
        verb = registry.verb(step.uses)
        if verb is None:
            raise ConfigError("V1", f"{ctx}: unregistered verb '{step.uses}'")
        if step.gate is not None and not verb.gateable:
            raise ConfigError("V6", f"{ctx}: verb '{step.uses}' is not gateable but step declares a gate")
        params_fields = {name: f.annotation for name, f in verb.Params.model_fields.items()}
        required = {name for name, f in verb.Params.model_fields.items() if f.is_required()}
        extras = set(step.with_) - set(params_fields)
        if extras:
            raise ConfigError("V2", f"{ctx}.with: keys not on {step.uses}'s Params: {sorted(extras)}")
        missing = required - set(step.with_)
        if missing:
            raise ConfigError("V2", f"{ctx}.with: missing required Params keys: {sorted(missing)}")
        for pname, value in step.with_.items():
            if isinstance(value, Ref):
                target_type = params_fields[pname]
                source_type = _resolve_ref_type(value, s, wf, registry, f"{ctx}.with.{pname}")
                if not _is_assignable(source_type, target_type):
                    raise ConfigError(
                        "V4",
                        f"{ctx}.with.{pname}: '{value.path}' ({source_type!r}) is not assignable "
                        f"to Params field type {target_type!r}",
                    )
        if step.gate is not None and isinstance(step.gate.timeout_seconds, Ref):
            gate_type = _resolve_ref_type(step.gate.timeout_seconds, s, wf, registry, f"{ctx}.gate.timeout_seconds")
            if not _is_assignable(gate_type, int):
                raise ConfigError(
                    "V4", f"{ctx}.gate.timeout_seconds: ref '{step.gate.timeout_seconds.path}' does not type-check to int"
                )
        register(step.id, verb.Output, s, ctx)
        if step.emit is not None:
            # emit fires "on step success" (persistence point 5) -- its payload may
            # reference the step's OWN just-computed output (e.g. Proof 2's
            # `emit: {payload: {droplet_id: {from: create.droplet_id}}}` on `create`
            # itself), so this validates against scope AFTER self-registration,
            # unlike `with:` above which must not see the step's own output.
            _validate_event_block(step.emit.event, step.emit.payload, s, wf, registry, f"{ctx}.emit")

    def validate_foreach(entry: ForeachDef, s: dict[str, Any], ctx: str) -> None:
        register(entry.id, _UNREFERENCEABLE, s, ctx)
        items_type = _resolve_ref_type(entry.items, s, wf, registry, f"{ctx}.foreach.items")
        if get_origin(items_type) is not list:
            raise ConfigError(
                "V5", f"{ctx}.foreach.items: ref '{entry.items.path}' does not type-check to list[T] (got {items_type!r})"
            )
        args = get_args(items_type)
        elem_type = args[0] if args else Any
        child_scope = dict(s)
        register(entry.as_, elem_type, child_scope, f"{ctx}.foreach.as")
        for i, body_step in enumerate(entry.body):
            validate_step(body_step, child_scope, f"{ctx}.body[{i}]:{body_step.id}")

    scope: dict[str, Any] = {}
    top_level_scope: dict[str, Any] = {}
    for i, entry in enumerate(wf.steps):
        ctx = f"steps[{i}]:{entry.id}"
        if isinstance(entry, StepDef):
            validate_step(entry, scope, ctx)
            top_level_scope[entry.id] = scope[entry.id]
        else:
            validate_foreach(entry, scope, ctx)

    # V8's "outcome payload Refs may reference top-level scope only".
    for name in ("succeeded", "failed", "cancelled"):
        block: OutcomeDef = getattr(wf.outcome, name)
        _validate_event_block(block.event, block.payload, top_level_scope, wf, registry, f"outcome.{name}")


def load_workflow(text: str, registry: RegistryView) -> WorkflowDefinition:
    """Parse + validate in one call -- the entry point tests and the engine use."""
    wf = parse_workflow(text)
    validate_workflow(wf, registry)
    return wf
