"""One registry-driven codec for events and effects. No pickle, no ``__dict__`` magic.

Salvaged from docs/design/seam-a-core.md §B (``seedpod2/core/codec.py``; the
``seedpod2`` name is dead per coherence review Conflict 16.1). Canonical JSON
semantics: sorted keys, no NaN, aware-UTC datetimes serialize with a trailing
``'Z'`` (naive datetimes are asserted against and rejected -- banned in v2),
``StrEnum`` members serialize to their ``.value``, nested ``Event``/``Effect``/
record dataclasses encode recursively, ``tuple``/``frozenset`` become sorted
lists, and any ``Mapping`` becomes a plain ``dict``.

**Registry design.** Every dataclass this codec touches (``Event`` leaves,
``Effect`` leaves, ``ClusterRecord``/``DeploymentRecord``, ``DiscoveredInfo``)
is encoded with a ``"kind"`` tag: effects use their own ``EffectKind`` value
(matching the ``effects_outbox.kind`` CHECK column, coherence review Conflict 1);
everything else uses its class name (matching ``core/events.py``'s
``EVENT_REGISTRY``, keyed the same way). Decoding a *nested* nested dataclass
field (``Persist.record: ClusterRecord | DeploymentRecord``,
``ScheduleTimer.event: Event``, ``Cascade.event: DeploymentEvent``) dispatches
on that ``"kind"`` tag rather than the field's static type -- this sidesteps
needing to disambiguate a `Union` of concrete dataclasses from a type hint
alone, and is exactly what makes `decode_event`/`decode_effect` themselves
trivial (look the top-level `"kind"` up in the matching registry).

Type hints for every field are resolved via ``typing.get_type_hints`` with an
explicit merged namespace covering ``core/events.py``, ``core/effects.py``, and
``core/records.py`` -- required because those modules use
``from __future__ import annotations`` (postponed evaluation) and
``effects.py`` only imports the ``Event``/``DeploymentEvent``/record names
under ``TYPE_CHECKING`` (so they are not bound in its own module globals at
runtime; passing an explicit ``globalns`` overrides `get_type_hints`'s
per-base-class module lookup for every class in the MRO, per the ``typing``
module's own resolution rules).
"""

from __future__ import annotations

import dataclasses
import json
import typing
from collections.abc import Mapping as MappingABC
from datetime import UTC, datetime
from enum import Enum
from types import UnionType

from seedpod.core import effects as _effects_mod
from seedpod.core import events as _events_mod
from seedpod.core import records as _records_mod
from seedpod.core.effects import (
    CancelTimer,
    CancelWorkflow,
    Cascade,
    Effect,
    Notify,
    Persist,
    RunWorkflow,
    ScheduleTimer,
)
from seedpod.core.events import EVENT_REGISTRY, DiscoveredInfo, Event
from seedpod.core.records import ClusterRecord, DeploymentRecord

__all__ = ["encode", "decode_event", "decode_effect", "canonical_json"]

# ---------------------------------------------------------------------------
# Registries
# ---------------------------------------------------------------------------

_EFFECT_CLASSES: tuple[type, ...] = (
    Persist,
    Notify,
    RunWorkflow,
    CancelWorkflow,
    ScheduleTimer,
    CancelTimer,
    Cascade,
)
EFFECT_REGISTRY: dict[str, type] = {str(cls.kind): cls for cls in _EFFECT_CLASSES}

_RECORD_CLASSES: tuple[type, ...] = (ClusterRecord, DeploymentRecord, DiscoveredInfo)
_RECORD_REGISTRY: dict[str, type] = {cls.__name__: cls for cls in _RECORD_CLASSES}

# every kind tag this codec can decode a nested dataclass field from, event or not
_SUPER_REGISTRY: dict[str, type] = {**EVENT_REGISTRY, **EFFECT_REGISTRY, **_RECORD_REGISTRY}

# merged namespace so `get_type_hints` can resolve forward refs living in any of the
# three core modules regardless of which module originally declared the annotation
_HINT_NAMESPACE: dict[str, object] = {
    **vars(_events_mod),
    **vars(_effects_mod),
    **vars(_records_mod),
}


def _type_hints(cls: type) -> dict[str, object]:
    return typing.get_type_hints(cls, globalns=_HINT_NAMESPACE)


# ---------------------------------------------------------------------------
# datetime <-> ISO-8601 'Z'
# ---------------------------------------------------------------------------


def _format_dt(dt: datetime) -> str:
    if dt.tzinfo is None:
        raise ValueError("codec cannot encode a naive datetime -- naive datetimes are banned in v2")
    return dt.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_dt(value: str) -> datetime:
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    dt = datetime.fromisoformat(text)
    if dt.tzinfo is None:
        raise ValueError(f"decoded a naive datetime from {value!r} -- naive datetimes are banned in v2")
    return dt


# ---------------------------------------------------------------------------
# Encode
# ---------------------------------------------------------------------------


def _kind_tag(instance: object) -> str:
    kind_attr = getattr(type(instance), "kind", None)  # Effect leaves only (ClassVar)
    return str(kind_attr) if kind_attr is not None else type(instance).__name__


def _strip_none(ftype: object) -> object:
    origin = typing.get_origin(ftype)
    if origin is typing.Union or origin is UnionType:
        args = [a for a in typing.get_args(ftype) if a is not type(None)]
        if len(args) == 1:
            return args[0]
        return typing.Union[tuple(args)]  # noqa: UP007  -- reconstruct a >1-arm union, None dropped
    return ftype


def _enc(value: object) -> object:
    """Value-driven encode: used wherever the concrete runtime type is unambiguous."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return _format_dt(value)
    if isinstance(value, Enum):
        return value.value
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        return _enc_dataclass(value)
    if isinstance(value, MappingABC):
        return {str(k): v for k, v in value.items()}
    if isinstance(value, (frozenset, set)):
        return sorted(_enc(v) for v in value)
    if isinstance(value, (tuple, list)):
        return [_enc(v) for v in value]
    if isinstance(value, (str, int, float, bool)):
        return value
    raise TypeError(f"codec cannot encode value of type {type(value)!r}: {value!r}")


def _is_mapping_type(ftype: object, origin: object) -> bool:
    if origin is dict or origin is MappingABC:
        return True
    return isinstance(ftype, type) and issubclass(ftype, MappingABC)


def _enc_typed(value: object, ftype: object) -> object:
    """Field-type-aware encode: only needed to normalize the `Mapping[...] = ()`
    empty-default sentinel (an empty tuple standing in for an empty mapping,
    docs/design/coherence-review.md Conflict 11) into a real `{}`."""
    ftype = _strip_none(ftype)
    if value is None:
        return None
    origin = typing.get_origin(ftype)
    if _is_mapping_type(ftype, origin):
        return {str(k): v for k, v in dict(value).items()}
    return _enc(value)


def _enc_dataclass(instance: object) -> dict:
    cls = type(instance)
    hints = _type_hints(cls)
    out: dict[str, object] = {"kind": _kind_tag(instance)}
    for f in dataclasses.fields(cls):
        raw = getattr(instance, f.name)
        out[f.name] = _enc_typed(raw, hints.get(f.name, type(raw)))
    return out


def encode(x: Event | Effect) -> dict:
    """`{"kind": ..., <field>: _enc(value), ...}` -- the canonical dict form of
    any registered Event or Effect (and, recursively, any nested dataclass)."""
    return _enc_dataclass(x)


def canonical_json(x: Event | Effect) -> str:
    """Canonical JSON: sorted keys, no NaN. Used for outbox `payload` / `timers.event`."""
    return json.dumps(encode(x), sort_keys=True, allow_nan=False)


# ---------------------------------------------------------------------------
# Decode
# ---------------------------------------------------------------------------


def _dec_typed(value: object, ftype: object) -> object:
    ftype = _strip_none(ftype)
    if value is None:
        return None
    if ftype is datetime:
        return _parse_dt(value)  # type: ignore[arg-type]
    if isinstance(ftype, type) and issubclass(ftype, Enum):
        return ftype(value)
    origin = typing.get_origin(ftype)
    if origin in (frozenset, set):
        (elem_t,) = typing.get_args(ftype) or (object,)
        return frozenset(_dec_typed(v, elem_t) for v in value)  # type: ignore[union-attr]
    if origin is tuple:
        args = typing.get_args(ftype)
        elem_t = args[0] if args else object
        return tuple(_dec_typed(v, elem_t) for v in value)  # type: ignore[union-attr]
    if _is_mapping_type(ftype, origin):
        # Empty decodes to `()`, not `{}`: `Mapping[str, str] = ()` is the pinned empty-mapping
        # sentinel (docs/design/coherence-review.md Conflict 11) that avoids a mutable dataclass
        # default; normalizing decode toward it (rather than a fresh `{}`) is what makes
        # `decode(encode(x)) == x` hold for the common unset case (Seam A §B's round-trip law) --
        # a real non-empty mapping always round-trips as a `dict` either way.
        decoded = dict(value)  # type: ignore[arg-type]  -- Mapping values are JSON scalars (refs only)
        return decoded if decoded else ()
    if dataclasses.is_dataclass(ftype) or origin is typing.Union or origin is UnionType:
        # dataclass target, possibly an abstract base (`Event`) or a Union of concretes
        # (`ClusterRecord | DeploymentRecord`, `DeploymentEvent`) -- resolved from the
        # payload's own "kind" tag, never guessed from the annotation.
        return _dec_dataclass_by_kind(value)  # type: ignore[arg-type]
    return value  # already-native JSON scalar


def _dec_dataclass(value: dict, cls: type) -> object:
    hints = _type_hints(cls)
    kwargs = {}
    for f in dataclasses.fields(cls):
        if f.name not in value:
            continue  # rely on the field's own default
        kwargs[f.name] = _dec_typed(value[f.name], hints[f.name])
    return cls(**kwargs)


def _dec_dataclass_by_kind(value: dict) -> object:
    if not isinstance(value, dict) or "kind" not in value:
        raise ValueError(f"expected a tagged dataclass dict with a 'kind' key, got {value!r}")
    cls = _SUPER_REGISTRY.get(value["kind"])
    if cls is None:
        raise ValueError(f"unknown kind {value['kind']!r}")
    return _dec_dataclass(value, cls)


def decode_event(d: dict) -> Event:
    cls = EVENT_REGISTRY.get(d.get("kind"))
    if cls is None:
        raise ValueError(f"unknown event kind {d.get('kind')!r}")
    return _dec_dataclass(d, cls)


def decode_effect(d: dict) -> Effect:
    cls = EFFECT_REGISTRY.get(d.get("kind"))
    if cls is None:
        raise ValueError(f"unknown effect kind {d.get('kind')!r}")
    return _dec_dataclass(d, cls)
