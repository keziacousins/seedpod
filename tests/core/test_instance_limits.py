"""Admission limits (DR-0055 decision 6): the pure function, no mocks."""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from pydantic import ValidationError

from seedpod.core.instance_limits import InstanceLimits, refusal


@dataclass(frozen=True)
class Live:
    owner_key_id: int = 1
    environment: str = "ephemeral"
    provider: str = "tart"
    vcpu: int = 2
    memory_mb: int = 2048
    hourly_price: float = 0.5


NEW = {"key_id": 1, "environment": "ephemeral", "provider": "tart", "vcpu": 2, "memory_mb": 2048, "hourly_price": 0.5}


@pytest.mark.parametrize(
    ("limits", "expected"),
    [
        ({}, None),
        ({"per_key": {"count": 2}}, None),  # the new instance makes two, and two is allowed
        ({"per_key": {"count": 1}}, "per_key.count"),
        ({"per_key": {"vcpu": 3}}, "per_key.vcpu"),
        ({"per_key": {"memory_mb": 4095}}, "per_key.memory_mb"),
        ({"per_key": {"hourly_spend": 0.9}}, "per_key.hourly_spend"),
        ({"per_environment": {"count": 1}}, "per_environment.count"),
        ({"per_provider": {"tart": {"vcpu": 3}}}, "per_provider.vcpu"),
        ({"per_key": {"count": 1}, "per_provider": {"tart": {"count": 1}}}, "per_key.count"),  # the first one named
        ({"per_key": {"count": 5, "vcpu": 3}, "per_provider": {"tart": {"count": 1}}}, "per_key.vcpu"),
    ],
)
def test_the_first_exceeded_limit_is_named(limits, expected):
    assert refusal(InstanceLimits.model_validate(limits), [Live()], **NEW) == expected


@pytest.mark.parametrize(
    ("block", "other"),
    [
        ("per_key", Live(owner_key_id=2)),
        ("per_environment", Live(environment="staging")),
        ("per_provider", Live(provider="digitalocean")),
    ],
)
def test_a_block_counts_only_what_shares_its_key_environment_or_provider(block, other):
    caps = {"count": 1}
    limits = InstanceLimits.model_validate({block: {"tart": caps} if block == "per_provider" else caps})
    assert refusal(limits, [other], **NEW) is None
    assert refusal(limits, [other, Live()], **NEW) == f"{block}.count"


def test_a_provider_not_in_the_map_has_no_cap():
    limits = InstanceLimits.model_validate({"per_provider": {"digitalocean": {"count": 1}}})
    assert refusal(limits, [Live(), Live()], **NEW) is None
    assert refusal(limits, [Live(provider="digitalocean")], **{**NEW, "provider": "digitalocean"}) == "per_provider.count"


def test_an_unknown_field_in_the_limits_file_is_refused():
    with pytest.raises(ValidationError):
        InstanceLimits.model_validate({"per_key": {"cpus": 4}})


def test_a_spend_cap_that_is_an_exact_sum_of_prices_admits_the_last_instance():
    live = [Live(hourly_price=0.1), Live(hourly_price=0.1)]
    limits = InstanceLimits.model_validate({"per_key": {"hourly_spend": 0.3}})
    assert refusal(limits, live, **{**NEW, "hourly_price": 0.1}) is None
