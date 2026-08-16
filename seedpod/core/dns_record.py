"""core/dns_record.py — the two pure DNS facts a workflow carries: ``DnsIntent``
(what this cluster's profile ASKED for, ``cluster.load_spec`` -> ``dns.create_record``)
and ``DnsRecordRef`` (what was actually created, ``cluster.load_infra`` ->
``dns.delete_record``). DR-0034 pairs them; before it, only the second existed and
nothing wrote the fields it read.

Pure data (Pillar 1: no IO, no ``now()``), the same shape and home discipline as
``core/cluster_spec.py``'s ``ClusterSpecification``.

**Why ``record_id`` is the load-bearing field.** v1 deleted a cluster's DNS record
by ID, not by name (``reference-code/seedpod/seedpod/jobs/state/destruction_job.py``
``_cleanup_dns_record``): it read ``dns_record_id``/``dns_zone``/``dns_hostname``
off the cluster's ``provider_config`` blob and called
``delete_cluster_dns_record(dns_zone=..., dns_record_id=...)``. That maps 1:1 onto
v2's already-committed ``services/dns.py`` surface,
``DnsService.delete_record(zone=..., record_id=...)`` — so this DTO needs no new
service method and no lookup-by-name round trip.

The test fixture's stand-in (``tests/engine/declared_verbs.py``'s ``DnsRecordRef``,
``{zone, name}``) predates this and is missing ``record_id`` entirely — a verb
built to that shape could not have called the DNS service at all. The fixture is
reconciled to this type as the real one lands (Round 8b), which is exactly what
``tests/engine/test_verb_conventions.py``'s real-registry validation exists to
force (Round-8a finding M-1).

**Absence is normal, not an error.** v1's cleanup returned early — logging at
debug, not warning — when ``dns_record_id`` or ``dns_zone`` was missing, because
most clusters never get a DNS record at all. Both shipped destroy workflows
therefore bind ``record: {from: infra.dns_record}`` as ``Optional`` with the
comment "None => no-op", and ``cluster.load_infra`` yields ``None`` unless BOTH
fields are present (see ``from_columns``).

**Where the three fields live: columns, not the blob (DR-0034 decision 4).** v1
kept ``dns_record_id``/``dns_zone``/``dns_hostname`` in its one
``provider_config`` blob, and this type's original ``from_provider_config``
read them from there — which was unreachable, because v2 never wrote them. v2's
schema splits v1's blob into provisioning INPUTS (``provider_config``), provider
OUTPUTS (``provider_resources``) and first-class columns; two of the three fields
were already columns, so migration 0002 adds the third and this type reads all
three from the row.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel

from seedpod.core.errors import ErrorCode, PermanentError

__all__ = ["DnsIntent", "DnsRecordRef"]


class DnsRecordRef(BaseModel):
    """A DNS record this cluster owns, identified the way its provider's API
    addresses it. ``hostname`` is carried for logging/identity parity with v1's
    log payload; it is never what the delete is keyed on."""

    record_id: str
    zone: str
    hostname: str | None = None

    @classmethod
    def from_columns(
        cls, *, record_id: str | None, zone: str | None, hostname: str | None = None
    ) -> DnsRecordRef | None:
        """v1's ``_cleanup_dns_record`` guard, verbatim in effect: a record exists
        only when BOTH ``dns_record_id`` and ``dns_zone`` are present; anything else
        is "no DNS record to clean up" and yields ``None``, never a partial ref that
        would make ``dns.delete_record`` issue a doomed API call.

        Takes the three values rather than a row object so ``core/`` keeps knowing
        nothing about ``data/`` (Pillar 1) — ``cluster.load_infra`` passes the
        columns in."""
        if not record_id or not zone:
            return None
        return cls(
            record_id=str(record_id),
            zone=str(zone),
            hostname=str(hostname) if hostname else None,
        )


class DnsIntent(BaseModel):
    """What a cluster's deployment profile ASKED for, read off the cluster row by
    ``cluster.load_spec`` and consumed by ``dns.create_record`` (DR-0034
    decisions 2 and 3).

    The field set and every default is v1's, verbatim from
    ``reference-code/seedpod/seedpod/providers/cloudflare_dns.py``
    ``create_cluster_dns_record`` (lines 331-339): ``zone`` required,
    ``subdomain_pattern`` defaulting to ``"{cluster_slug}"``, ``ttl`` 300,
    ``proxied`` False.

    **The FQDN this produces must equal the one the manifests render** —
    ``app/services/deployment_service.py``'s ``_resolve_hostname`` builds
    ``f"{subdomain_pattern.format(cluster_slug=slug)}.{zone}"`` from the same
    profile block, and ``DnsService._full_name`` folds identically. That is why
    ``fqdn_for`` lives here rather than being spelled out a third time at a call
    site (DR-0034 decision 8; pinned by a test that runs both paths off one
    shipped profile)."""

    zone: str
    subdomain_pattern: str = "{cluster_slug}"
    ttl: int = 300
    proxied: bool = False

    def fqdn_for(self, cluster_slug: str) -> str:
        return f"{self.subdomain_pattern.format(cluster_slug=cluster_slug)}.{self.zone}"

    @classmethod
    def from_provider_config(cls, provider_config: Mapping[str, Any] | None) -> DnsIntent | None:
        """v1's own two guards, in v1's own order
        (``reference-code/seedpod/seedpod/core/state_manager.py``:955-958): read
        ``provider_config["dns_config"]``, and yield nothing unless it is
        ``enabled``. The block is only ever written when enabled in the first
        place (v1's ``cluster_manager.py``:318-321, mirrored by v2's
        ``_provider_config_from``), so absence and disabled agree.

        **One deliberate divergence from v1**: ``enabled: true`` with no ``zone``
        raises instead of silently skipping. v1 raised ``ValueError`` here
        (``cloudflare_dns.py``:333) but inside ``_create_dns_record_if_configured``'s
        blanket ``except Exception``, so it degraded to a debug-level skip. A
        profile that asks for DNS and names no zone is malformed — the hostname it
        makes the manifests render is ``"<slug>.cluster."``, equally broken — and
        this repo's standing lesson is that a computed reason must not be thrown
        away (DR-0033). No shipped profile does this."""
        config = (provider_config or {}).get("dns_config") or {}
        if not config.get("enabled", False):
            return None
        zone = config.get("zone")
        if not zone:
            raise PermanentError(
                "dns-intent: profile sets dns.enabled but names no dns.zone",
                code=ErrorCode.INVALID_INPUT,
                provider="dns",
                command="load_intent",
                detail={"dns_config": str(dict(config))},
            )
        # Only pass through keys the profile actually sets, so this type's own
        # defaults (v1's) stay the single home for them.
        declared = {
            field: config[field]
            for field in ("subdomain_pattern", "ttl", "proxied")
            if config.get(field) is not None
        }
        return cls(zone=str(zone), **declared)
