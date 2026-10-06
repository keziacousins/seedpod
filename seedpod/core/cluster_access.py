"""The hostname an operator uses to reach a cluster (DR-0047).

Two different facts can supply that hostname, and the cluster API reports which one
it used:

- ``dns_record``: seedpod created a DNS record for the cluster. The name is on the
  cluster row (``clusters.dns_hostname``, written by ``cluster.store_dns_record``).
- ``profile``: the deployment profile's ``hostname.strategy`` resolved a name, and
  seedpod created no DNS record for it. The name is ``cluster_hostname`` in the
  deployment audit's ``resolved_config``. Whether the name resolves depends on the
  operator's network (a Tailscale MagicDNS name, mDNS, a hosts file), which seedpod
  cannot know.

``dns_record`` wins when both exist: it is the name seedpod owns and deletes.

An IP address is not a hostname. ``hostname.strategy: provider_host`` resolves to
the provider's address, which on DigitalOcean and tart is an IP; the API already
reports that as ``public_ip``. The test here is the same narrow one as the ``is
dns_name`` template test in ``services/manifests.py``: reject IP literals and
nothing else.

Pure: no IO, no clock.
"""

from __future__ import annotations

import ipaddress
from dataclasses import dataclass
from typing import Literal

__all__ = ["AccessHostname", "HostnameSource", "access_hostname"]

HostnameSource = Literal["dns_record", "profile"]


@dataclass(frozen=True, slots=True)
class AccessHostname:
    hostname: str
    source: HostnameSource


def _is_ip_literal(value: str) -> bool:
    try:
        ipaddress.ip_address(value)
    except ValueError:
        return False
    return True


def access_hostname(*, dns_hostname: str | None, resolved_hostname: object) -> AccessHostname | None:
    """``resolved_hostname`` is typed ``object`` because it comes out of a JSON column:
    a value that is not a non-empty string gives ``None``, it does not raise."""
    if dns_hostname:
        return AccessHostname(hostname=dns_hostname, source="dns_record")
    if not isinstance(resolved_hostname, str):
        return None
    name = resolved_hostname.strip()
    if not name or _is_ip_literal(name):
        return None
    return AccessHostname(hostname=name, source="profile")
