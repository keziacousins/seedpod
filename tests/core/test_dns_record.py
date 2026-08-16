"""tests/core/test_dns_record.py — the two pure DNS facts: ``DnsRecordRef.from_columns``
(the v1 parity guard behind ``cluster.load_infra`` -> ``dns.delete_record``) and
``DnsIntent.from_provider_config`` (DR-0034's ``cluster.load_spec`` -> ``dns.create_record``).

Salvage sources:

- ``reference-code/seedpod/seedpod/jobs/state/destruction_job.py``'s ``_cleanup_dns_record``:
  read the record id / zone / hostname, and return early (debug-level "no DNS record to
  cleanup", NOT a warning or an error) unless BOTH the id and the zone are present.
- ``reference-code/seedpod/seedpod/core/state_manager.py``:955-958 + ``providers/
  cloudflare_dns.py``:331-339 for the intent: read ``provider_config["dns_config"]``,
  skip unless ``enabled``, and default ``subdomain_pattern``/``ttl``/``proxied`` to
  ``"{cluster_slug}"``/300/False.

**Where the record lives changed in DR-0034** (decision 4): v1 kept all three fields in
its one ``provider_config`` blob and this type read them from there — which was
unreachable, because v2 never wrote them (backlog #22/#6). They are columns now.
"""

from __future__ import annotations

import pytest

from seedpod.core.dns_record import DnsIntent, DnsRecordRef
from seedpod.core.errors import ErrorCode, PermanentError


def test_full_row_yields_a_complete_ref():
    ref = DnsRecordRef.from_columns(record_id="rec-123", zone="example.com", hostname="c-1.example.com")
    assert ref == DnsRecordRef(record_id="rec-123", zone="example.com", hostname="c-1.example.com")


def test_hostname_is_optional_because_the_delete_is_keyed_on_the_id():
    ref = DnsRecordRef.from_columns(record_id="rec-123", zone="example.com")
    assert ref is not None
    assert ref.hostname is None
    assert (ref.record_id, ref.zone) == ("rec-123", "example.com")


def test_no_dns_record_at_all_is_none_not_an_error():
    """Most clusters never get a DNS record; both shipped destroy workflows bind this
    as Optional with 'None => no-op'."""
    assert DnsRecordRef.from_columns(record_id=None, zone=None) is None
    assert DnsRecordRef.from_columns(record_id=None, zone=None, hostname=None) is None


def test_a_partial_record_is_none_never_a_doomed_delete():
    """v1's guard is `if not dns_record_id or not dns_zone: return` -- either half
    missing means no delete is attempted. Returning a partial ref would make
    `dns.delete_record` issue an API call that cannot succeed."""
    assert DnsRecordRef.from_columns(record_id="rec-123", zone=None) is None
    assert DnsRecordRef.from_columns(record_id=None, zone="example.com") is None
    # Empty strings are falsy in v1's guard too -- not just NULL columns.
    assert DnsRecordRef.from_columns(record_id="", zone="example.com") is None
    assert DnsRecordRef.from_columns(record_id="rec-123", zone="") is None


def test_the_ref_carries_what_the_dns_service_actually_needs():
    """`services/dns.py`'s committed surface is `delete_record(*, zone, record_id)` --
    this DTO must supply exactly those two, which is why `record_id` is required
    (the declared_verbs.py stand-in's {zone, name} shape could not have)."""
    ref = DnsRecordRef.from_columns(record_id="rec-123", zone="example.com")
    assert ref is not None
    assert {"zone": ref.zone, "record_id": ref.record_id} == {"zone": "example.com", "record_id": "rec-123"}


# ---------------------------------------------------------------------------
# DnsIntent — DR-0034's create side.
# ---------------------------------------------------------------------------


def test_a_disabled_or_absent_dns_block_is_no_intent():
    """v1's own first guard (state_manager.py:958): not enabled => skip, at debug
    level. `_provider_config_from` only writes the block when it IS enabled, so
    absence and disabled have to agree."""
    assert DnsIntent.from_provider_config(None) is None
    assert DnsIntent.from_provider_config({}) is None
    assert DnsIntent.from_provider_config({"dns_config": {"enabled": False, "zone": "example.com"}}) is None
    assert DnsIntent.from_provider_config({"node_specification": {}}) is None


def test_an_enabled_block_carries_v1s_defaults():
    """v1's defaults, verbatim from cloudflare_dns.py:335-339."""
    intent = DnsIntent.from_provider_config({"dns_config": {"enabled": True, "zone": "example.com"}})
    assert intent == DnsIntent(zone="example.com", subdomain_pattern="{cluster_slug}", ttl=300, proxied=False)


def test_a_profile_may_override_every_default():
    intent = DnsIntent.from_provider_config(
        {
            "dns_config": {
                "enabled": True,
                "zone": "example.com",
                "subdomain_pattern": "{cluster_slug}.cluster",
                "ttl": 60,
                "proxied": True,
            }
        }
    )
    assert intent == DnsIntent(
        zone="example.com", subdomain_pattern="{cluster_slug}.cluster", ttl=60, proxied=True
    )


def test_enabled_with_no_zone_raises_rather_than_silently_skipping():
    """The one deliberate divergence from v1 (DR-0034 decision 2). v1 raised
    ValueError here (cloudflare_dns.py:333) inside `_create_dns_record_if_configured`'s
    blanket `except Exception`, so it degraded to a debug-level skip and the cluster
    provisioned advertising a hostname ending in a bare dot."""
    with pytest.raises(PermanentError) as excinfo:
        DnsIntent.from_provider_config({"dns_config": {"enabled": True}})
    assert excinfo.value.code is ErrorCode.INVALID_INPUT
    assert "dns.zone" in str(excinfo.value)


def test_fqdn_for_is_the_one_home_for_the_concatenation():
    """DR-0034 decision 8: this must equal what `_resolve_hostname` renders into the
    manifests. Pinned end-to-end against a shipped profile in
    tests/app/test_deployment_service_resolved_config.py."""
    intent = DnsIntent(zone="example.com", subdomain_pattern="{cluster_slug}.cluster")
    assert intent.fqdn_for("preset-abc-e35dbd4d") == "preset-abc-e35dbd4d.cluster.example.com"
    assert DnsIntent(zone="example.com").fqdn_for("c-1") == "c-1.example.com"
