"""``core/cluster_access.py`` (DR-0047): which hostname the cluster API reports, and
where it came from. Pure, so no fixtures and no mocks."""

from __future__ import annotations

import pytest

from seedpod.core.cluster_access import AccessHostname, access_hostname


def test_dns_record_is_the_hostname_when_seedpod_made_one():
    assert access_hostname(dns_hostname="web.example.com", resolved_hostname=None) == AccessHostname(
        hostname="web.example.com", source="dns_record"
    )


def test_dns_record_wins_over_a_profile_resolved_name():
    result = access_hostname(dns_hostname="web.example.com", resolved_hostname="web.tailnet.ts.net")
    assert result == AccessHostname(hostname="web.example.com", source="dns_record")


def test_profile_resolved_name_is_used_when_there_is_no_dns_record():
    result = access_hostname(dns_hostname=None, resolved_hostname="preset-web-1a2b3c4d.tailnet.ts.net")
    assert result == AccessHostname(hostname="preset-web-1a2b3c4d.tailnet.ts.net", source="profile")


@pytest.mark.parametrize("address", ["192.168.65.49", "203.0.113.40", "::1", "2001:db8::1", " 192.168.65.49 "])
def test_an_ip_address_is_not_a_hostname(address):
    # `hostname.strategy: provider_host` resolves to the provider's address. The API
    # reports that as `public_ip`; it must not also become a URL.
    assert access_hostname(dns_hostname=None, resolved_hostname=address) is None


@pytest.mark.parametrize("value", [None, "", "   ", 42, ["web.example.com"], {"host": "web.example.com"}])
def test_a_missing_or_malformed_resolved_value_gives_no_hostname(value):
    # The value comes out of a JSON column, so any shape can arrive.
    assert access_hostname(dns_hostname=None, resolved_hostname=value) is None


def test_an_empty_dns_hostname_falls_through_to_the_profile_name():
    result = access_hostname(dns_hostname="", resolved_hostname="kind-host.local")
    assert result == AccessHostname(hostname="kind-host.local", source="profile")
