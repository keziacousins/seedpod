"""tests/engine/steps/test_dns_steps.py — ``seedpod/engine/steps/dns.py``'s two verbs:
``dns.delete_record`` (Round 8b, "destroy path" — the tree's first ``plane="service"``
step) and ``dns.create_record`` (DR-0034, the half the vocabulary never had).

Against the REAL ``DnsService`` over ``tests/services/fake_cloudflare.py``'s fake
TRANSPORT (a real ``httpx.AsyncClient`` with an ``AsyncBaseTransport`` fake) — faults
are injected at the actual seam the service talks to, never ``Mock``/``patch``
(CLAUDE.md testing posture), exactly as ``tests/services/test_dns.py`` does.

Covers:
- absence is a no-op, and does NOT require a configured DNS service (the common case:
  most clusters never get a record);
- a REAL record with no service configured fails LOUDLY rather than leaking it;
- the delete is keyed on ``record_id``, which is why ``DnsRecordRef`` carries one;
- an already-deleted record is success, not an error (404 -> ``existed=False``);
- a genuine API failure PROPAGATES — v1's blanket ``try/except`` is deliberately not
  ported, because both destroy workflows express that policy as ``retry: api_default``
  + ``on_failure: continue`` instead.

And, for the create half (DR-0034):

- no intent is a no-op, so all four provision workflows can bind it unconditionally;
- the created FQDN equals ``DnsIntent.fqdn_for``, which is what the manifests render;
- a re-run UPSERTS (``created=False``), which is what makes ``idempotent=True`` honest;
- the undo deletes iff THIS run created the record, never one it merely re-pointed;
- an API failure propagates and therefore FAILS the run — decision 7's deliberate
  divergence from v1's "not critical", because in v2 the hostname is load-bearing.
"""

from __future__ import annotations

import httpx
import pytest

from seedpod.core.dns_record import DnsIntent, DnsRecordRef
from seedpod.core.errors import PermanentError, ProviderError
from seedpod.engine.step import EmptyOutput, StepServices
from seedpod.engine.steps.dns import (
    CreateRecordParams,
    DeleteRecordParams,
    DnsCreateRecord,
    DnsDeleteRecord,
)
from seedpod.services.dns import DnsConfig, DnsService
from tests.conformance.harness import Fault
from tests.engine.fakes import FakeSubprocessManager, make_step_context
from tests.services.fake_cloudflare import FakeCloudflareBackend, FakeCloudflareTransport

_ZONE = "example.com"


def _ctx():
    return make_step_context(services=StepServices(subprocess_manager=FakeSubprocessManager(), providers={}))


def _service(backend: FakeCloudflareBackend, *faults: Fault) -> DnsService:
    transport = httpx.AsyncClient(transport=FakeCloudflareTransport(backend, frozenset(faults)))
    return DnsService(DnsConfig(api_token="fake-token"), transport)  # pragma: allowlist secret


def _seeded() -> tuple[FakeCloudflareBackend, str, str]:
    backend = FakeCloudflareBackend()
    zone_id = backend.add_zone(_ZONE)
    record_id = backend.add_record(zone_id=zone_id, name=f"c-1.{_ZONE}", content="1.2.3.4")
    return backend, zone_id, record_id


def test_declares_the_dr_0022_contract():
    step = DnsDeleteRecord(dns=None)
    assert step.verb == "dns.delete_record"
    assert step.plane == "service"  # NOT "provider": DnsService is not a Provider (DR-0015)
    assert step.thin is False
    assert step.gateable is False
    assert step.undoable is False
    assert step.idempotent is True


# ---------------------------------------------------------------------------
# Absence: the common case.
# ---------------------------------------------------------------------------


async def test_no_record_is_a_no_op_and_needs_no_dns_service():
    """v1's `_cleanup_dns_record` returned early at debug level when the cluster had
    no dns_record_id/dns_zone. Both destroy workflows bind `record` as Optional with
    'None => no-op'. A deployment with no Cloudflare token at all must still destroy."""
    step = DnsDeleteRecord(dns=None)

    output = await step.execute(DeleteRecordParams(record=None), _ctx())

    assert isinstance(output, EmptyOutput)


async def test_no_record_is_a_no_op_even_when_a_service_is_configured():
    backend, _zone_id, _record_id = _seeded()
    step = DnsDeleteRecord(dns=_service(backend))

    await step.execute(DeleteRecordParams(record=None), _ctx())

    assert backend.call_count == 0, "a None record must not touch the DNS API at all"
    assert len(backend.dns_records) == 1  # the unrelated record is untouched


# ---------------------------------------------------------------------------
# A real record with no service: loud, never a silent leak.
# ---------------------------------------------------------------------------


async def test_a_real_record_with_no_dns_service_fails_loudly():
    """Returning success here would leak the record forever: the cluster row naming
    it is about to be destroyed, so nothing would ever point at it again."""
    step = DnsDeleteRecord(dns=None)
    record = DnsRecordRef(record_id="rec-1", zone=_ZONE, hostname=f"c-1.{_ZONE}")

    with pytest.raises(PermanentError) as exc_info:
        await step.execute(DeleteRecordParams(record=record), _ctx())

    assert "rec-1" in str(exc_info.value)


# ---------------------------------------------------------------------------
# The happy path: delete by ID.
# ---------------------------------------------------------------------------


async def test_deletes_the_record_by_id():
    backend, _zone_id, record_id = _seeded()
    step = DnsDeleteRecord(dns=_service(backend))
    record = DnsRecordRef(record_id=record_id, zone=_ZONE, hostname=f"c-1.{_ZONE}")

    output = await step.execute(DeleteRecordParams(record=record), _ctx())

    assert isinstance(output, EmptyOutput)
    assert record_id not in backend.dns_records


async def test_deleting_an_already_deleted_record_is_success_not_an_error():
    """`DnsService.delete_record` maps 404 to DnsDeleted(existed=False) as a typed
    Result -- the same absence-is-data discipline as the machine plane's
    DestroyOutcome. It is what makes this verb safely idempotent under the destroy
    path's retries."""
    backend, _zone_id, record_id = _seeded()
    step = DnsDeleteRecord(dns=_service(backend))
    record = DnsRecordRef(record_id=record_id, zone=_ZONE)

    await step.execute(DeleteRecordParams(record=record), _ctx())
    await step.execute(DeleteRecordParams(record=record), _ctx())  # second delete: no raise

    assert record_id not in backend.dns_records


async def test_a_record_id_that_never_existed_is_also_success():
    backend, _zone_id, _record_id = _seeded()
    step = DnsDeleteRecord(dns=_service(backend))
    record = DnsRecordRef(record_id="rec-never", zone=_ZONE)

    await step.execute(DeleteRecordParams(record=record), _ctx())


# ---------------------------------------------------------------------------
# Failures propagate -- v1's blanket try/except is NOT ported.
# ---------------------------------------------------------------------------


async def test_api_failure_propagates_rather_than_being_swallowed():
    """v1 wrapped the whole cleanup in `except Exception` and logged a warning. That
    policy MOVED into the workflow (`retry: api_default` + `on_failure: continue` on
    this step in both destroy files); re-implementing the swallow here would double it
    and deny the engine the retry it now owns (Seam C taste call 2)."""
    backend, _zone_id, record_id = _seeded()
    step = DnsDeleteRecord(dns=_service(backend, Fault.AUTH))
    record = DnsRecordRef(record_id=record_id, zone=_ZONE)

    with pytest.raises(ProviderError):
        await step.execute(DeleteRecordParams(record=record), _ctx())


# ===========================================================================
# dns.create_record — DR-0034. The half the vocabulary never had (backlog #22).
# ===========================================================================


def test_create_declares_the_dr_0034_contract():
    step = DnsCreateRecord(dns=None)
    assert step.verb == "dns.create_record"
    assert step.plane == "service"
    assert step.thin is False
    assert step.gateable is False
    assert step.idempotent is True  # the service call is an upsert keyed on the name
    # undoable=True, unlike its delete sibling: §5.5's "destruction IS compensation,
    # never auto-undone" is about DELETES. A create has a real inverse.
    assert step.undoable is True


async def test_create_with_no_intent_is_a_no_op_and_needs_no_dns_service():
    """The common case: most profiles never enable DNS, so `cluster.load_spec` yields
    `dns_intent=None` and all four provision workflows bind it unconditionally. v1's
    own first guard (state_manager.py:958) skipped at debug level, not as an error."""
    step = DnsCreateRecord(dns=None)

    output = await step.execute(CreateRecordParams(intent=None, slug="c-1", address="203.0.113.5"), _ctx())

    assert output.record is None
    assert output.created is False


async def test_create_makes_the_record_and_returns_a_deletable_ref():
    backend = FakeCloudflareBackend()
    backend.add_zone(_ZONE)
    step = DnsCreateRecord(dns=_service(backend))
    intent = DnsIntent(zone=_ZONE, subdomain_pattern="{cluster_slug}.cluster")

    output = await step.execute(
        CreateRecordParams(intent=intent, slug="preset-abc", address="203.0.113.5"), _ctx()
    )

    assert output.created is True
    assert output.record is not None
    assert output.record.hostname == f"preset-abc.cluster.{_ZONE}"
    assert output.record.zone == _ZONE
    # The ref must be exactly what `dns.delete_record` needs -- this is the whole
    # point of #6: destroy could never delete a record nothing recorded.
    assert backend.dns_records[output.record.record_id]["content"] == "203.0.113.5"


async def test_created_fqdn_equals_the_intents_own_fqdn_for():
    """DR-0034 decision 8: `DnsIntent.fqdn_for` is the single home for the
    concatenation the manifests also render, and the record must match it."""
    backend = FakeCloudflareBackend()
    backend.add_zone(_ZONE)
    step = DnsCreateRecord(dns=_service(backend))
    intent = DnsIntent(zone=_ZONE, subdomain_pattern="{cluster_slug}.cluster")

    output = await step.execute(CreateRecordParams(intent=intent, slug="c-9", address="1.2.3.4"), _ctx())

    assert output.record is not None
    assert output.record.hostname == intent.fqdn_for("c-9")


async def test_create_honours_the_profiles_ttl_and_proxied():
    backend = FakeCloudflareBackend()
    backend.add_zone(_ZONE)
    step = DnsCreateRecord(dns=_service(backend))
    intent = DnsIntent(zone=_ZONE, ttl=60, proxied=True)

    output = await step.execute(CreateRecordParams(intent=intent, slug="c-1", address="1.2.3.4"), _ctx())

    assert output.record is not None
    record = backend.dns_records[output.record.record_id]
    assert (record["ttl"], record["proxied"]) == (60, True)


async def test_a_rerun_updates_rather_than_duplicating_and_reports_created_false():
    """`idempotent=True` is only truthful because the service call is an UPSERT: a
    crash-resumed run finds its own record by name and re-points it. `created=False`
    is then what stops the undo deleting a record it did not make."""
    backend = FakeCloudflareBackend()
    backend.add_zone(_ZONE)
    step = DnsCreateRecord(dns=_service(backend))
    intent = DnsIntent(zone=_ZONE)

    first = await step.execute(CreateRecordParams(intent=intent, slug="c-1", address="1.1.1.1"), _ctx())
    second = await step.execute(CreateRecordParams(intent=intent, slug="c-1", address="2.2.2.2"), _ctx())

    assert first.created is True
    assert second.created is False
    assert first.record is not None and second.record is not None
    assert second.record.record_id == first.record.record_id
    assert len(backend.dns_records) == 1
    assert backend.dns_records[second.record.record_id]["content"] == "2.2.2.2"


async def test_an_intent_with_no_dns_service_configured_fails_loudly():
    """The mirror of the delete side's own rule. Reporting success would provision a
    cluster that advertises a hostname nothing answers -- exactly backlog #22."""
    step = DnsCreateRecord(dns=None)

    with pytest.raises(PermanentError) as excinfo:
        await step.execute(
            CreateRecordParams(intent=DnsIntent(zone=_ZONE), slug="c-1", address="1.2.3.4"), _ctx()
        )
    assert _ZONE in str(excinfo.value)


async def test_api_failure_propagates_so_the_run_fails_and_compensates():
    """DR-0034 decision 7, the deliberate divergence from v1's "not critical"
    (state_manager.py:1009): no swallow here and no `on_failure: continue` in the
    workflows, so a permanent Cloudflare failure fails the provision instead of
    leaving a cluster whose advertised name does not resolve."""
    backend = FakeCloudflareBackend()
    backend.add_zone(_ZONE)
    step = DnsCreateRecord(dns=_service(backend, Fault.AUTH))

    with pytest.raises(ProviderError):
        await step.execute(
            CreateRecordParams(intent=DnsIntent(zone=_ZONE), slug="c-1", address="1.2.3.4"), _ctx()
        )


# ---------------------------------------------------------------------------
# undo: delete iff THIS run created it.
# ---------------------------------------------------------------------------


async def test_undo_deletes_a_record_this_run_created():
    backend = FakeCloudflareBackend()
    backend.add_zone(_ZONE)
    step = DnsCreateRecord(dns=_service(backend))
    params = CreateRecordParams(intent=DnsIntent(zone=_ZONE), slug="c-1", address="1.2.3.4")

    output = await step.execute(params, _ctx())
    assert output.created is True
    await step.undo(params, output, {}, _ctx())

    assert backend.dns_records == {}


async def test_undo_never_deletes_a_record_the_run_only_repointed():
    """`DnsRecordUpserted.created` exists for exactly this (services/dns.py's "P2
    graft" over v1): a rollback must not destroy a record that pre-existed the run."""
    backend = FakeCloudflareBackend()
    zone_id = backend.add_zone(_ZONE)
    pre_existing = backend.add_record(zone_id=zone_id, name=f"c-1.{_ZONE}", content="9.9.9.9")
    step = DnsCreateRecord(dns=_service(backend))
    params = CreateRecordParams(intent=DnsIntent(zone=_ZONE), slug="c-1", address="1.2.3.4")

    output = await step.execute(params, _ctx())
    assert output.created is False
    await step.undo(params, output, {}, _ctx())

    assert pre_existing in backend.dns_records


async def test_undo_with_no_output_does_nothing():
    """execute never returned, so v2 cannot know whether the POST landed -- and
    guessing is what `created` exists to avoid (DR-0034 decision 6)."""
    backend = FakeCloudflareBackend()
    zone_id = backend.add_zone(_ZONE)
    record_id = backend.add_record(zone_id=zone_id, name=f"c-1.{_ZONE}", content="9.9.9.9")
    step = DnsCreateRecord(dns=_service(backend))

    await step.undo(
        CreateRecordParams(intent=DnsIntent(zone=_ZONE), slug="c-1", address="1.2.3.4"), None, {}, _ctx()
    )

    assert record_id in backend.dns_records
