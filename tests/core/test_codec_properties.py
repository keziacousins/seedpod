"""Codec round-trip property tests -- Seam A §K's binding test contract:

    "codec round-trip decode(encode(x)) == x over generated events and effects
    (build composite strategies from the registered unions; aware-UTC datetimes only)"

No ``unittest.mock`` (CLAUDE.md's core testing posture) -- the codec is pure, so no
double is needed anywhere here. Strategies live in ``tests/core/strategies.py``,
built directly from the registered ``EVENT_REGISTRY`` / ``EFFECT_REGISTRY`` /
``ClusterEvent`` / ``DeploymentEvent`` unions in ``seedpod/core/{events,effects}.py``.
"""

from __future__ import annotations

from hypothesis import given, settings

from seedpod.core.codec import decode_effect, decode_event, encode
from seedpod.core.effects import Effect
from seedpod.core.events import Event
from tests.core.strategies import effects, events


@settings(deadline=None)
@given(event=events)
def test_event_round_trips_through_encode_decode(event: Event) -> None:
    assert decode_event(encode(event)) == event


@settings(deadline=None)
@given(effect=effects)
def test_effect_round_trips_through_encode_decode(effect: Effect) -> None:
    assert decode_effect(encode(effect)) == effect
