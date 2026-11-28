"""Admission limits for instances (DR-0055 decision 6), as a pure function. No IO.

``app/factory.py``'s ``load_instance_limits`` reads ``config/instance-limits.yml``. The router calls
``refusal`` inside the transaction that writes the birth row.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Protocol

from pydantic import BaseModel, ConfigDict, Field

__all__ = ["Caps", "InstanceLimits", "LiveInstance", "refusal"]


class Caps(BaseModel):
    """Ceilings on the TOTAL, new instance included. ``None`` is no ceiling."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    count: int | None = Field(default=None, ge=0)
    vcpu: int | None = Field(default=None, ge=0)
    memory_mb: int | None = Field(default=None, ge=0)
    hourly_spend: float | None = Field(default=None, ge=0)


class InstanceLimits(BaseModel):
    """``per_key`` caps each owner key and ``per_environment`` each environment, the same ``Caps``
    for every one. ``per_provider`` maps a provider name to its own ``Caps`` (a local host's
    capacity); a provider not in the map has no cap. A missing block is no cap."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    per_key: Caps | None = None
    per_environment: Caps | None = None
    per_provider: dict[str, Caps] = Field(default_factory=dict)


class LiveInstance(Protocol):
    owner_key_id: int
    environment: str
    provider: str
    vcpu: int
    memory_mb: int
    hourly_price: float


def refusal(
    limits: InstanceLimits,
    live: Iterable[LiveInstance],
    *,
    key_id: int,
    environment: str,
    provider: str,
    vcpu: int,
    memory_mb: int,
    hourly_price: float,
) -> str | None:
    """The name of the first limit the new instance would exceed, such as ``"per_key.vcpu"``, or
    ``None``. ``live`` is every instance that is not ``destroyed``. Blocks are checked in the order
    per_key, per_environment, per_provider, and fields in the order count, vcpu, memory_mb,
    hourly_spend."""
    live = list(live)
    blocks = (
        ("per_key", limits.per_key, lambda r: r.owner_key_id == key_id),
        ("per_environment", limits.per_environment, lambda r: r.environment == environment),
        ("per_provider", limits.per_provider.get(provider), lambda r: r.provider == provider),
    )
    for name, caps, same in blocks:
        if caps is None:
            continue
        held = [r for r in live if same(r)]
        totals = {
            "count": len(held) + 1,
            "vcpu": sum(r.vcpu for r in held) + vcpu,
            "memory_mb": sum(r.memory_mb for r in held) + memory_mb,
            "hourly_spend": sum(r.hourly_price for r in held) + hourly_price,
        }
        for field, total in totals.items():
            cap = getattr(caps, field)
            if cap is not None and round(total, 6) > cap:  # 0.1 * 3 is 0.30000000000000004
                return f"{name}.{field}"
    return None
