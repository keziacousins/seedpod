"""engine/steps/dns.py — the ``dns.*`` verb family (DR-0022's re-normalized
vocabulary, Round 8b "destroy path"; ``dns.create_record`` added by DR-0034):
``dns.create_record`` and ``dns.delete_record``.

**The first ``plane="service"`` step in the tree.** DR-0022 P1 classifies a DNS
record as a supporting *service*, not a provider: ``DnsService``
(``seedpod/services/dns.py``) is one of DR-0015's two httpx-backed supporting
services and is deliberately NOT a ``Provider`` — it has no Seam C command, no
conformance suite, and is unreachable through ``ctx.services.providers``. So this
step is a plain ``Step`` with the service constructor-injected at registry build
time (``engine/registry.py``: "Construction ... happens once, at composition-root
build time"), never a ``ProviderStep``. Hence ``thin=False``: there is no single
Seam C command being wrapped.

**Absence is the common case, and it is a no-op — not an error.** Most clusters
never get a DNS record at all. v1's ``_cleanup_dns_record``
(``reference-code/seedpod/seedpod/jobs/state/destruction_job.py``) returned early
at DEBUG level when ``dns_record_id``/``dns_zone`` were missing from
``provider_config``; that guard now lives in
``core/dns_record.py``'s ``DnsRecordRef.from_provider_config``, which yields
``None``, and both shipped destroy workflows bind ``record: {from:
infra.dns_record}`` with the comment "None => no-op". This step honours that
directly.

**What is NOT ported: v1's blanket ``try/except``.** v1 wrapped the whole cleanup
in ``except Exception`` and logged a warning ("DNS cleanup failed ... proceeding
with destruction"). That policy is not lost — it MOVED into the workflow, where it
is legible and typed: both destroy files declare ``retry: api_default`` plus
``on_failure: continue`` on this step. Re-implementing the swallow here would
double it, and would deny the engine the retry it now owns (Seam C taste call 2:
steps never retry). A genuine API failure therefore propagates as the taxonomy
``DnsService`` already raises (``classify_http``/``TransientError``/
``PermanentError``) and the workflow decides.

Deleting an already-deleted record is not a failure either: ``DnsService.delete_record``
maps 404 to ``DnsDeleted(existed=False)`` as a typed Result, never an exception
(its row-38 salvage note) — the same absence-is-data discipline as the machine
plane's ``DestroyOutcome``. That is what makes this verb safely ``idempotent`` for
the destroy path's retries.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import ClassVar, Literal

from pydantic import BaseModel

from seedpod.core.dns_record import DnsIntent, DnsRecordRef
from seedpod.core.errors import ErrorCode, PermanentError
from seedpod.engine.step import EmptyOutput, Step, StepContext
from seedpod.services.dns import DnsService

__all__ = [
    "CreateRecordOutput",
    "CreateRecordParams",
    "DeleteRecordParams",
    "DnsCreateRecord",
    "DnsDeleteRecord",
]


class CreateRecordParams(BaseModel):
    """``intent`` is ``Optional`` for the same reason ``DeleteRecordParams.record``
    is: ``cluster.load_spec`` yields ``None`` for every profile that did not enable
    DNS -- the majority -- and V4's Optional-binds-Optional rule is what lets all
    four provision workflows bind it unconditionally."""

    intent: DnsIntent | None = None
    slug: str
    address: str


class CreateRecordOutput(BaseModel):
    """``record`` is ``None`` exactly when there was no intent, so
    ``cluster.store_dns_record`` can bind it straight through and no-op too.

    ``created`` is what makes ``undo`` safe (DR-0034 decision 6): it is ``True``
    only when the POST branch ran, so a rollback never deletes a record that
    pre-existed this run and merely got its IP updated."""

    record: DnsRecordRef | None = None
    created: bool = False


class DnsCreateRecord(Step[CreateRecordParams, CreateRecordOutput]):
    """DR-0034: the create half, which the vocabulary simply never had.

    **Placed right after the address gate** in all four ``provision-*.yml``, the
    earliest point the IP exists -- v1's own precondition
    (``reference-code/seedpod/seedpod/core/state_manager.py``:962-965 returned
    ``False`` with no ``public_ip``), reached structurally here rather than by a
    runtime check, since the ``address`` binding comes from a gate that has already
    succeeded.

    **Not best-effort, unlike v1** (DR-0034 decision 7, ratified 2026-08-10). v1
    swallowed every DNS failure -- *"Don't fail the deployment if DNS creation
    fails - it's not critical"* -- which was true in v1's world. In v2 a
    DNS-profile cluster renders its hostname into every Ingress ``host`` and every
    app URL, so a cluster that reaches ACTIVE with a name that does not resolve is
    silently broken (backlog #22, found by smoke 10). So this step carries
    ``retry: api_default`` and NO ``on_failure: continue``: transient Cloudflare
    errors are absorbed by the engine's Schedule, and a permanent one -- bad token,
    zone not in the account, a ``success: false`` envelope -- fails the run and
    compensates.
    """

    verb = "dns.create_record"
    Params = CreateRecordParams
    Output = CreateRecordOutput
    plane: ClassVar[Literal["provider", "service", "domain"]] = "service"
    thin = False
    undoable = True
    # idempotent stays True (Step's default): the service call is an UPSERT keyed on
    # the record's name, so a re-run after a crash finds its own record and returns
    # the same id with created=False.

    def __init__(self, *, dns: DnsService | None) -> None:
        """``dns`` is REQUIRED as a keyword but MAY be ``None`` -- the identical
        contract to ``DnsDeleteRecord.__init__``, for the identical reason."""
        self._dns = dns

    def _service(self, intent: DnsIntent) -> DnsService:
        if self._dns is None:
            # The profile asked for a DNS record and this process cannot make one.
            # Same call as the delete side's: failing loudly is the only honest
            # option, because the alternative is a cluster that provisions green
            # and advertises a hostname nothing answers.
            raise PermanentError(
                f"{self.verb}: profile requires a DNS record in zone {intent.zone!r} "
                "but no DNS service is configured (no Cloudflare API token)",
                code=ErrorCode.INVALID_INPUT,
                provider="dns",
                command="create_record",
                detail={"zone": intent.zone},
            )
        return self._dns

    async def execute(self, params: CreateRecordParams, ctx: StepContext) -> CreateRecordOutput:
        intent = params.intent
        if intent is None:
            # v1's own early return: DNS is not configured for this cluster
            # (state_manager.py:958, debug, not a warning).
            return CreateRecordOutput()
        dns = self._service(intent)
        result = await dns.upsert_record(
            zone=intent.zone,
            cluster_slug=params.slug,
            ip=params.address,
            subdomain_pattern=intent.subdomain_pattern,
            ttl=intent.ttl,
            proxied=intent.proxied,
        )
        await ctx.progress(
            "dns record created" if result.created else "dns record updated",
            zone=result.zone,
            fqdn=result.fqdn,
            record_id=result.record_id,
            address=params.address,
        )
        return CreateRecordOutput(
            record=DnsRecordRef(record_id=result.record_id, zone=result.zone, hostname=result.fqdn),
            created=result.created,
        )

    async def undo(
        self, params: CreateRecordParams, output: CreateRecordOutput | None, notes: Mapping[str, str], ctx: StepContext
    ) -> None:
        """Delete iff THIS run created the record. ``DnsRecordUpserted.created`` is
        the flag v2 added over v1 for exactly this (``services/dns.py``'s "P2 graft"
        note, Seam C §5.5): a run that only re-pointed an existing record must not
        destroy it on rollback.

        ``output is None`` (execute never returned) deliberately does NOTHING. v2
        cannot know whether the POST landed, and guessing is what ``created``
        exists to avoid -- the residual risk (a create that succeeded with a lost
        response leaves an unrecorded record) is recorded in DR-0034 decision 6 and
        is self-limiting: the next provision for the same slug upserts the same
        name rather than adding a second record."""
        if output is None or not output.created or output.record is None:
            return
        if self._dns is None:  # unreachable: execute would have raised before creating
            return
        await self._dns.delete_record(zone=output.record.zone, record_id=output.record.record_id)


class DeleteRecordParams(BaseModel):
    """``record`` is ``Optional`` because ``cluster.load_infra`` yields ``None`` for
    every cluster that never had a DNS record -- the majority. V4's
    Optional-binds-Optional rule is what lets the shipped workflows bind it."""

    record: DnsRecordRef | None = None


class DnsDeleteRecord(Step[DeleteRecordParams, EmptyOutput]):
    verb = "dns.delete_record"
    Params = DeleteRecordParams
    Output = EmptyOutput
    plane: ClassVar[Literal["provider", "service", "domain"]] = "service"
    thin = False
    undoable = False  # deleting a DNS record has no inverse this system may take
    # idempotent stays True (Step's default): a repeat delete is DnsDeleted(existed=False).

    def __init__(self, *, dns: DnsService | None) -> None:
        """``dns`` is REQUIRED as a keyword but MAY be ``None``: ``build_app()``
        constructs ``DnsService`` only when a Cloudflare API token is configured, and
        a deployment with no DNS at all is entirely normal. ``None`` is accepted so
        the verb still registers (and still no-ops) in that deployment -- the
        alternative, omitting the verb from the registry, is the
        "silently register fewer verbs, find out at ``UnknownVerbError`` mid-destroy"
        failure mode ``_build_step_registry``'s own docstring rules out. What it must
        never do is silently SKIP a record that really exists: see ``execute``."""
        self._dns = dns

    async def execute(self, params: DeleteRecordParams, ctx: StepContext) -> EmptyOutput:
        record = params.record
        if record is None:
            # v1's early return: "No DNS record to cleanup" (debug, not a warning).
            return EmptyOutput()
        if self._dns is None:
            # A real record exists but this process has no DNS service configured.
            # Failing loudly is the only honest option -- returning success would
            # leak the record forever, and the cluster row that names it is about to
            # be destroyed, so nothing would ever point at it again.
            raise PermanentError(
                f"{self.verb}: cluster has DNS record {record.record_id!r} in zone {record.zone!r} "
                "but no DNS service is configured (no Cloudflare API token)",
                code=ErrorCode.INVALID_INPUT,
                provider="dns",
                command="delete_record",
                detail={"zone": record.zone, "record_id": record.record_id},
            )
        result = await self._dns.delete_record(zone=record.zone, record_id=record.record_id)
        await ctx.progress(
            "dns record deleted" if result.existed else "dns record already absent",
            zone=record.zone,
            record_id=record.record_id,
            existed=result.existed,
        )
        return EmptyOutput()
