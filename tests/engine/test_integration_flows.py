"""tests/engine/test_integration_flows.py — whole-workflow integration tests against
``tests/engine/fakes.py``'s harness, using inline YAML fixtures modeled on
docs/design/seam-b-engine.md's proofs (amended by docs/design/coherence-review.md
Conflicts 9/10/12/14). Every other engine test module drives ONE persistence
point/gate/cancel/park scenario in isolation; this module instead drives a handful of
COMPLETE workflows end to end and checks cross-cutting properties that only show up
across a whole run:

  (a) a deploy-waves-shaped flow, foreach over a planned wave list, materialized step
      paths (``wave[1].ready``), ``on_failure: report`` leaves infra untouched on a
      mid-loop permanent failure and records ``failed_step`` correctly.
  (b) a provision-shaped flow, ``on_failure: compensate``, mid-flow permanent failure
      -> exact LIFO undo order across four distinct step instances, the failed step
      undone FIRST with ``output=None`` (execute never returned) but its persistence-
      point-4 note still reaches ``undo()``, and ``undo_incomplete`` populated by a
      permanently-failing undo while the remaining (earlier-declared) undos still run.
  (c) ``emit`` fires in the SAME transaction as step success -- proven via row
      visibility through the exact ``tx`` session ``FakeDispatcher.apply`` received.
  (d) typed named bindings resolved from PERSISTED rows, not live Python state: mutate
      a producer fake's in-memory value AFTER it has succeeded, and prove a later
      step's Ref-bound param is unaffected.
  (e) foreach iterations run strictly sequentially, in list order.
  (f) ``SecretStr`` outputs are never persisted/emitted in plaintext -- and (flagged,
      left failing on purpose, matching this module's own documented TODO(spine) in
      seedpod/engine/engine.py) a downstream step Ref-bound to a ``SecretStr`` output
      does NOT currently receive the original secret back, because pydantic's default
      JSON-mode masking is irreversible, not the Fernet round-trip the schema comment
      ("SecretStr fields encrypted") implies -- Pillar 4 (services/crypto.py) is not
      built yet, exactly as engine.py's module docstring already flags.

Run admission (coherence-review Conflict 2) is out of this engine's job -- every test
inserts the ``workflow_runs`` (and, for deploy-shaped flows, ``deployments``) row(s)
directly, exactly like test_engine_smoke.py / test_crash_matrix.py / test_cancel_matrix.py,
then hands the id to ``WorkflowEngine.start()``.
"""

from __future__ import annotations

import asyncio
import dataclasses
import json
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime

from pydantic import BaseModel, SecretStr
from sqlalchemy import text

from seedpod.core.clock import FrozenClock
from seedpod.core.errors import ErrorCode, PermanentError
from seedpod.data.repositories import WorkflowRunRow, WorkflowStepRepository
from seedpod.engine.schedule import NAMED_POLICIES
from seedpod.engine.step import Step, StepContext
from tests.engine.fakes import (
    FailOnValueStep,
    MintOnceStep,
    NoteOutput,
    NoteParams,
    PauseGate,
    RecordedApply,
    RecordingStep,
    build_engine,
)

NOW = datetime(2026, 7, 13, 12, 0, 0, tzinfo=UTC)


# --------------------------------------------------------------------------------------
# Shared insert helpers (same convention as test_engine_smoke.py / test_crash_matrix.py /
# test_cancel_matrix.py: run admission is out of scope, tests craft the rows directly)
# --------------------------------------------------------------------------------------


def _insert_cluster(session, cluster_id: str) -> None:
    session.execute(
        text(
            """
            INSERT INTO clusters (id, name, slug, environment, status, provider, created_at, updated_at)
            VALUES (:id, :id, :id, 'ephemeral', 'provisioning', 'fake', :now, :now)
            """
        ),
        {"id": cluster_id, "now": NOW.isoformat()},
    )


def _insert_deployment(session, deployment_id: str, cluster_id: str) -> None:
    session.execute(
        text(
            """
            INSERT INTO deployments
                (id, cluster_id, environment, status, version, manifest_version, created_at, updated_at)
            VALUES
                (:id, :cluster_id, 'ephemeral', 'deploying', 0, '1', :now, :now)
            """
        ),
        {"id": deployment_id, "cluster_id": cluster_id, "now": NOW.isoformat()},
    )


async def _insert_run(
    harness,
    run_id: str,
    cluster_id: str,
    workflow: str,
    *,
    deployment_id: str | None = None,
    args: dict | None = None,
) -> None:
    async with harness.uow() as tx:
        _insert_cluster(tx, cluster_id)
        if deployment_id is not None:
            _insert_deployment(tx, deployment_id, cluster_id)
        harness.run_repo.insert(
            tx,
            WorkflowRunRow(
                id=run_id,
                workflow=workflow,
                workflow_version=1,
                cluster_id=cluster_id,
                deployment_id=deployment_id,
                dedupe_key=f"dedupe:{run_id}",
                args=args if args is not None else {"cluster_id": cluster_id},
                status="pending",
                cancel_requested=False,
                failed_step=None,
                error=None,
                undo_incomplete=None,
                initiated_by="test",
                created_at=NOW,
                started_at=None,
                finished_at=None,
            ),
        )


def _outcome_block(workflow: str, on_failure: str, event_prefix: str) -> str:
    """``event_prefix`` is "Provision" (cluster-targeted) or "Deploy" (deployment-
    targeted, Conflict 8's ``workflow.startswith("deploy")`` targeting rule).
    ``ProvisionSucceeded`` has no field defaults (public_ip/kubeconfig_ref both
    required); ``DeploySucceeded`` does (``resolved_images`` defaults to ``()``)."""
    succeeded_payload = "{public_ip: \"1.2.3.4\", kubeconfig_ref: \"kc-1\"}" if event_prefix == "Provision" else "{}"
    return f"""
workflow: {workflow}
version: 1
inputs:
  cluster_id: {{type: str}}
on_failure: {on_failure}
outcome:
  succeeded: {{event: {event_prefix}Succeeded, payload: {succeeded_payload}}}
  failed:    {{event: {event_prefix}Failed, payload: {{reason: "n/a"}}}}
  cancelled: {{event: {event_prefix}Failed, payload: {{reason: "cancelled"}}}}
"""


# ========================================================================================
# (a) deploy-waves-shaped: foreach over a planned wave list, materialized step paths,
# on_failure: report leaves infra untouched, failed_step recorded exactly.
# ========================================================================================


@dataclass(frozen=True)
class WaveSpec:
    name: str


async def test_deploy_waves_shaped_foreach_stops_at_failed_wave_and_leaves_infra_untouched(tmp_path):
    clock = FrozenClock(NOW)
    ready = FailOnValueStep(fail_on="wave-b")
    apply = RecordingStep()
    wf = """
workflow: deploy-waves-fake
version: 1
inputs:
  cluster_id: {type: str}
  waves: {type: "list[WaveSpec]"}
on_failure: report
outcome:
  succeeded: {event: DeploySucceeded, payload: {}}
  failed:    {event: DeployFailed, payload: {reason: "n/a"}}
  cancelled: {event: DeployFailed, payload: {reason: "cancelled"}}
steps:
  - id: wave
    foreach: {items: {from: run.waves}, as: w}
    body:
      - id: ready
        uses: fake.fail_on_value
        with: {message: {from: w.name}}
      - id: apply
        uses: fake.recording
        with: {message: {from: w.name}}
"""
    harness = build_engine(
        tmp_path, {"deploy-waves-fake": wf}, [ready, apply], clock, named_types={"WaveSpec": WaveSpec}
    )
    run_id, cluster_id, deployment_id = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
    waves = [{"name": "wave-a"}, {"name": "wave-b"}, {"name": "wave-c"}]
    await _insert_run(
        harness,
        run_id,
        cluster_id,
        "deploy-waves-fake",
        deployment_id=deployment_id,
        args={"cluster_id": cluster_id, "waves": waves},
    )

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        rows = {r.step_path: r for r in harness.step_repo.list_for_run(tx, run_id)}

    # ---- materialized step paths: wave[0] fully ran, wave[1].ready failed and
    # stopped the loop cold -- wave[1].apply and every wave[2].* row NEVER inserted
    assert set(rows) == {"wave[0].ready", "wave[0].apply", "wave[1].ready"}
    assert rows["wave[0].ready"].status == "succeeded"
    assert rows["wave[0].apply"].status == "succeeded"
    assert rows["wave[1].ready"].status == "failed"
    assert rows["wave[1].ready"].output is None  # execute raised -- never returned a value

    # ---- sequential in list order, stopped after the failure (wave-c never attempted)
    assert ready.calls == ["wave-a", "wave-b"]
    assert apply.received == ["wave-a"]

    assert run_row.status == "failed"
    assert run_row.failed_step == "wave[1].ready"
    assert run_row.error == {"kind": "permanent", "step": "wave[1].ready", "message": "forced failure on 'wave-b'"}
    assert run_row.undo_incomplete is None

    # ---- report mode: infra untouched -- compensation never ran, no undo_status anywhere
    for row in rows.values():
        assert row.undo_status is None

    # ---- Conflict 8 targeting: deploy-shaped -> ('deployment', run.deployment_id)
    outcome_call = harness.dispatcher.calls[-1]
    assert outcome_call.aggregate == "deployment"
    assert outcome_call.aggregate_id == deployment_id
    assert type(outcome_call.event).__name__ == "DeployFailed"
    assert outcome_call.event.reason == "n/a"
    harness.db.dispose()


# ========================================================================================
# (e) foreach iterations run strictly sequentially, in list order (no early failure)
# ========================================================================================


async def test_foreach_iterations_run_sequentially_in_list_order(tmp_path):
    clock = FrozenClock(NOW)
    work = RecordingStep()
    wf = """
workflow: foreach-sequential-fake
version: 1
inputs:
  cluster_id: {type: str}
  items: {type: "list[str]"}
on_failure: report
outcome:
  succeeded: {event: ProvisionSucceeded, payload: {public_ip: "1.2.3.4", kubeconfig_ref: "kc-1"}}
  failed:    {event: ProvisionFailed, payload: {reason: "n/a"}}
  cancelled: {event: ProvisionFailed, payload: {reason: "cancelled"}}
steps:
  - id: loop
    foreach: {items: {from: run.items}, as: item}
    body:
      - id: work
        uses: fake.recording
        with: {message: {from: item}}
"""
    harness = build_engine(tmp_path, {"foreach-sequential-fake": wf}, [work], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(
        harness, run_id, cluster_id, "foreach-sequential-fake",
        args={"cluster_id": cluster_id, "items": ["a", "b", "c"]},
    )

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    assert work.received == ["a", "b", "c"]  # exact list order, never reordered/concurrent

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        rows = {r.step_path: r for r in harness.step_repo.list_for_run(tx, run_id)}
    assert run_row.status == "succeeded"
    assert set(rows) == {"loop[0].work", "loop[1].work", "loop[2].work"}
    for i, item in enumerate(("a", "b", "c")):
        assert rows[f"loop[{i}].work"].output == {"echoed": item}
    harness.db.dispose()


# ========================================================================================
# (b) provision-shaped, on_failure: compensate: mid-flow permanent failure -> exact LIFO
# undo order, failed step undone FIRST with output=None but its note reaches undo(),
# undo_incomplete populated by a permanently-failing undo while later undos still run.
# ========================================================================================


class _LifoStep(Step[NoteParams, NoteOutput]):
    """Test-local (not tests/engine/fakes.py -- this scenario alone needs a SHARED
    ``order_log`` across several distinct step instances, a pattern no existing fake
    primitive offers): succeeds on ``execute`` unless ``fail_execute``, always writing
    a ``ctx.note()`` first (persistence point 4) -- so even the step that fails still
    has a note on record for undo to read. ``undo`` always appends ``self.name`` to
    the shared ``order_log`` (proving exact LIFO order across instances) then
    optionally raises ``PermanentError`` (the ``undo_incomplete`` branch)."""

    Params = NoteParams
    Output = NoteOutput
    idempotent = True
    undoable = True
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 30

    def __init__(
        self, name: str, order_log: list[str], *, fail_execute: bool = False, fail_undo: bool = False
    ) -> None:
        self.verb = f"fake.lifo.{name}"
        self.name = name
        self.order_log = order_log
        self.fail_execute = fail_execute
        self.fail_undo = fail_undo
        self.undo_calls: list[tuple[NoteParams, NoteOutput | None, dict[str, str]]] = []

    async def execute(self, params: NoteParams, ctx: StepContext) -> NoteOutput:
        await ctx.note(resource_id=params.resource_id)
        if self.fail_execute:
            raise PermanentError(f"{self.name} failed", code=ErrorCode.INVALID_INPUT)
        return NoteOutput(resource_id=params.resource_id)

    async def undo(
        self, params: NoteParams, output: NoteOutput | None, notes: dict, ctx: StepContext
    ) -> None:
        self.order_log.append(self.name)
        self.undo_calls.append((params, output, dict(notes)))
        if self.fail_undo:
            raise PermanentError(f"{self.name} undo failed", code=ErrorCode.INVALID_INPUT)


async def test_provision_shaped_compensate_exact_lifo_undo_order_and_undo_incomplete(tmp_path):
    clock = FrozenClock(NOW)
    order_log: list[str] = []
    a = _LifoStep("a", order_log)
    b = _LifoStep("b", order_log, fail_undo=True)
    c = _LifoStep("c", order_log)
    failer = _LifoStep("failer", order_log, fail_execute=True)
    wf = _outcome_block("provision-lifo-fake", "compensate", "Provision") + """steps:
  - id: a
    uses: fake.lifo.a
    with: {resource_id: "r-a"}
  - id: b
    uses: fake.lifo.b
    with: {resource_id: "r-b"}
  - id: c
    uses: fake.lifo.c
    with: {resource_id: "r-c"}
  - id: failer
    uses: fake.lifo.failer
    with: {resource_id: "r-failer"}
"""
    harness = build_engine(tmp_path, {"provision-lifo-fake": wf}, [a, b, c, failer], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "provision-lifo-fake")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
        rows = {r.step_path: r for r in harness.step_repo.list_for_run(tx, run_id)}

    # ---- exact LIFO order: the FAILED step undone first, then the rest in reverse
    # declaration order (c, b, a) -- across four DISTINCT step instances
    assert order_log == ["failer", "c", "b", "a"]

    # ---- the failed step: output=None (execute never returned), but its
    # persistence-point-4 note survived and reached undo()
    assert rows["failer"].status == "failed"
    assert rows["failer"].output is None
    assert rows["failer"].notes == {"resource_id": "r-failer"}
    assert len(failer.undo_calls) == 1
    failer_params, failer_output, failer_notes = failer.undo_calls[0]
    assert failer_params == NoteParams(resource_id="r-failer")
    assert failer_output is None
    assert failer_notes == {"resource_id": "r-failer"}
    assert rows["failer"].undo_status == "done"

    # ---- undo_incomplete: "b"'s undo permanently failed; "a" (later in LIFO order)
    # still ran to completion regardless
    assert run_row.undo_incomplete == ["b"]
    assert rows["b"].undo_status == "failed"
    assert rows["a"].undo_status == "done"
    assert rows["c"].undo_status == "done"
    assert len(a.undo_calls) == 1
    assert len(b.undo_calls) == 1
    assert len(c.undo_calls) == 1

    assert run_row.status == "failed"
    assert run_row.failed_step == "failer"
    assert run_row.error == {"kind": "permanent", "step": "failer", "message": "failer failed"}

    outcome_call = harness.dispatcher.calls[-1]
    assert outcome_call.aggregate == "cluster"
    assert outcome_call.aggregate_id == cluster_id
    assert type(outcome_call.event).__name__ == "ProvisionFailed"
    assert outcome_call.event.reason == "n/a"
    harness.db.dispose()


# ========================================================================================
# (c) emit fires in the SAME transaction as step success -- proven via row visibility
# through the exact ``tx`` session ``FakeDispatcher.apply`` received.
# ========================================================================================


@dataclass
class TxVisibilityDispatcher:
    """Records, for each ``apply()`` call, whether the emitting step's row is
    ALREADY visible as 'succeeded' with its final output when queried through the
    EXACT SAME (still-open, not-yet-committed) session the engine handed to
    ``apply()`` -- the only way to prove ``dispatcher.apply(..., tx=step_tx)`` runs
    inside ``_persist_step_succeeded``'s own transaction rather than after it
    commits (Conflict 3)."""

    run_id: str
    step_path: str
    step_repo: WorkflowStepRepository = field(default_factory=WorkflowStepRepository)
    calls: list[RecordedApply] = field(default_factory=list)
    visible_at_apply: list[bool] = field(default_factory=list)

    async def apply(self, aggregate: str, aggregate_id: str, event, *, tx) -> None:
        row = self.step_repo.get(tx, self.run_id, self.step_path)
        self.visible_at_apply.append(
            row is not None and row.status == "succeeded" and row.output == {"echoed": "hi"}
        )
        self.calls.append(RecordedApply(aggregate=aggregate, aggregate_id=aggregate_id, event=event, tx=tx))


async def test_emit_fires_in_the_same_transaction_as_step_success(tmp_path):
    clock = FrozenClock(NOW)
    from tests.engine.fakes import InstantStep

    solo = InstantStep()
    wf = _outcome_block("emit-tx-fake", "report", "Provision") + """steps:
  - id: solo
    uses: fake.instant
    with: {message: "hi"}
    emit: {event: EndpointReady, payload: {public_ip: {from: solo.echoed}}}
"""
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    dispatcher = TxVisibilityDispatcher(run_id=run_id, step_path="solo")
    harness = build_engine(tmp_path, {"emit-tx-fake": wf}, [solo], clock, dispatcher=dispatcher)
    await _insert_run(harness, run_id, cluster_id, "emit-tx-fake")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    assert len(dispatcher.calls) == 2  # the mid-run emit, then the terminal outcome
    emit_call = dispatcher.calls[0]
    assert type(emit_call.event).__name__ == "EndpointReady"
    assert emit_call.event.public_ip == "hi"
    # the step row was ALREADY 'succeeded' with its final output when apply() ran,
    # observed through the exact tx `apply()` was handed -- same transaction
    assert dispatcher.visible_at_apply[0] is True

    async with harness.uow() as tx:
        run_row = harness.run_repo.get(tx, run_id)
    assert run_row.status == "succeeded"
    harness.db.dispose()


# ========================================================================================
# (d) typed named bindings resolved from PERSISTED rows, never live Python state:
# mutate the producer fake's in-memory value AFTER it succeeds; the consumer's bound
# param -- both what got persisted mid-run and what the step actually finishes with --
# stays pinned to the value that was true when the producer's row was written.
# ========================================================================================


async def test_named_bindings_resolved_from_persisted_rows_not_live_fake_state(tmp_path):
    clock = FrozenClock(NOW)
    gate = PauseGate()
    mint = MintOnceStep(value="v1")
    consumer = RecordingStep(pause=gate)
    wf = _outcome_block("bind-persisted-fake", "report", "Provision") + """steps:
  - id: mint
    uses: fake.mint_once
    with: {message: "ignored"}
  - id: consumer
    uses: fake.recording
    with: {message: {from: mint.echoed}}
"""
    harness = build_engine(tmp_path, {"bind-persisted-fake": wf}, [mint, consumer], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "bind-persisted-fake")

    await harness.engine.start(run_id)
    await asyncio.wait_for(gate.entered.wait(), timeout=5.0)  # consumer is mid-execute, paused

    async with harness.uow() as tx:
        mint_row = harness.step_repo.get(tx, run_id, "mint")
        consumer_row = harness.step_repo.get(tx, run_id, "consumer")
    assert mint_row.status == "succeeded"
    assert mint_row.output == {"echoed": "v1"}
    assert consumer_row.status == "running"
    assert consumer_row.params == {"message": "v1"}  # bound+persisted BEFORE we mutate mint below

    mint.value = "MUTATED-AFTER-SUCCESS"  # mutate the live fake's in-memory state

    async with harness.uow() as tx:
        consumer_row_again = harness.step_repo.get(tx, run_id, "consumer")
    assert consumer_row_again.params == {"message": "v1"}  # unaffected by the mutation

    gate.release()
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        consumer_row_final = harness.step_repo.get(tx, run_id, "consumer")
    assert consumer_row_final.status == "succeeded"
    assert consumer_row_final.output == {"echoed": "v1"}  # NOT "MUTATED-AFTER-SUCCESS"
    assert consumer.received == ["v1"]
    assert mint.calls == 1  # mint was never re-executed
    harness.db.dispose()


# ========================================================================================
# (f) SecretStr outputs: never plaintext at rest / in progress payloads or events.
# ========================================================================================


class SecretParams(BaseModel):
    seed: str = "seed"


class SecretOutput(BaseModel):
    token: SecretStr


class MintSecretStep(Step[SecretParams, SecretOutput]):
    verb = "fake.mint_secret"
    Params = SecretParams
    Output = SecretOutput
    idempotent = True
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 30

    def __init__(self, value: str) -> None:
        self.value = value
        self.calls = 0

    async def execute(self, params: SecretParams, ctx: StepContext) -> SecretOutput:
        self.calls += 1
        await ctx.progress("token minted")  # deliberately never passes the raw value
        return SecretOutput(token=SecretStr(self.value))


class SecretConsumerParams(BaseModel):
    token: SecretStr


class SecretConsumerOutput(BaseModel):
    received: str  # plaintext -- what a REAL downstream provider step needs to use the secret


class SecretConsumerStep(Step[SecretConsumerParams, SecretConsumerOutput]):
    verb = "fake.secret_consumer"
    Params = SecretConsumerParams
    Output = SecretConsumerOutput
    idempotent = True
    default_retry = NAMED_POLICIES["none"]
    default_timeout_seconds = 30

    def __init__(self) -> None:
        self.received_values: list[str] = []

    async def execute(self, params: SecretConsumerParams, ctx: StepContext) -> SecretConsumerOutput:
        value = params.token.get_secret_value()
        self.received_values.append(value)
        return SecretConsumerOutput(received=value)


async def test_secretstr_outputs_never_plaintext_at_rest_or_in_progress_payloads(tmp_path):
    """Two-part assertion, deliberately split:

    PART 1 (passes today): the raw secret never appears verbatim in
    ``workflow_steps.output`` at rest, nor in any ``effects_outbox`` row (progress /
    job_* Notify payloads). This holds even without a real crypto service, because
    pydantic masks ``SecretStr`` fields to a fixed sentinel on ``model_dump(mode="json")``.

    PART 2 (LEFT FAILING ON PURPOSE -- a flagged deviation, not a workaround): per
    the schema comment ("output ... SecretStr fields encrypted") and Seam B §2.1
    ("SecretStr fields encrypted"), a downstream step Ref-bound to a SecretStr output
    should receive the ORIGINAL secret back (encrypt-then-decrypt is reversible;
    masking is not). seedpod/engine/engine.py's own module docstring already flags
    this exact gap ("this module does not Fernet-encrypt SecretStr Output fields...
    NOT implemented here; it needs seedpod/services/crypto.py, Pillar 4"). Today the
    consumer step actually receives the literal masked sentinel string, not the
    secret -- this is real data loss, not just a security nicety deferred. Left
    failing per this task's brief rather than worked around; seedpod/engine/engine.py
    is out of this task's edit scope.
    """
    clock = FrozenClock(NOW)
    RAW_SECRET = "sk-super-duper-secret-value-1234"  # noqa: N806
    mint = MintSecretStep(RAW_SECRET)
    consumer = SecretConsumerStep()
    wf = _outcome_block("secret-flow-fake", "report", "Provision") + """steps:
  - id: mint
    uses: fake.mint_secret
    with: {}
  - id: consumer
    uses: fake.secret_consumer
    with: {token: {from: mint.token}}
"""
    harness = build_engine(tmp_path, {"secret-flow-fake": wf}, [mint, consumer], clock)
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _insert_run(harness, run_id, cluster_id, "secret-flow-fake")

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        mint_row = harness.step_repo.get(tx, run_id, "mint")
        outbox_rows = harness.outbox_repo.list_for_aggregate(tx, "run", run_id)

    # ---- PART 1: never plaintext at rest, never in progress/job Notify payloads ----
    assert mint_row.output["token"] != RAW_SECRET
    for row in outbox_rows:
        assert RAW_SECRET not in row.payload
    for call in harness.dispatcher.calls:
        event_fields = {f.name: str(getattr(call.event, f.name)) for f in dataclasses.fields(call.event)}
        assert RAW_SECRET not in json.dumps(event_fields)

    # ---- PART 2 (FLAGGED, LEFT FAILING): downstream Ref-bound consumer should get
    # the real secret back via decrypt, not the masked sentinel -- see docstring.
    assert consumer.received_values == [RAW_SECRET]
    harness.db.dispose()
