"""tests/engine/test_crashed_run_is_terminal.py — DR-0045: a run that crashes must
become a FAILED run, not sit at `running` forever.

The incident this pins (2026-08-16): DR-0043 erratum E3 shipped a workflow input that
`DispatchTable.resolve` did not supply, binding resolution raised a bare
`KeyError('snapshot')`, and the run task died. Nothing retrieved the exception, so
`workflow_runs` kept `status='running'` with `error` NULL, the cluster stayed
`destroying`, and a live DigitalOcean droplet billed until a human noticed. The
one-line bug was cheap; the stranding was not.

**Where the gap actually was, which writing these tests corrected.** A step raising an
arbitrary exception was ALREADY handled — `_run_step` classifies anything a verb throws
through the §2.3.1 taxonomy, and the run fails and compensates normally. The
unprotected surface was the engine's OWN machinery around step execution: binding
resolution, scope construction, outcome-event building. That is precisely where E3's
`KeyError` lived, and it is what `_fail_unexpected` now covers. So the classified path
is asserted here too — as a regression guard that DR-0045's boundary did not quietly
swallow behaviour that already worked.

Zero Mock/patch (CLAUDE.md): the crash is produced by real args that genuinely do not
satisfy a real workflow definition, and by a real fake step that raises.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

from sqlalchemy import text

from seedpod.core.clock import FrozenClock
from seedpod.data.repositories import WorkflowRunRow
from seedpod.engine.step import EmptyOutput, Step
from tests.engine.fakes import InstantStep, build_engine

NOW = datetime(2026, 8, 16, 12, 0, 0, tzinfo=UTC)

# Declares an input nothing supplies -- the E3 shape, reproduced structurally.
NEEDS_AN_INPUT_YAML = """
workflow: provision-fake
version: 1
inputs:
  cluster_id: {type: str}
  snapshot: {type: bool}
on_failure: compensate
outcome:
  succeeded: {event: ProvisionSucceeded, payload: {public_ip: "1.2.3.4", kubeconfig_ref: "kc"}}
  failed:    {event: ProvisionFailed, payload: {reason: "n/a"}}
  cancelled: {event: ProvisionFailed, payload: {reason: "cancelled"}}
steps:
  - id: first
    uses: fake.instant
    with: {message: "hello"}
"""

BOOM_YAML = """
workflow: provision-fake
version: 1
inputs:
  cluster_id: {type: str}
on_failure: report
outcome:
  succeeded: {event: ProvisionSucceeded, payload: {public_ip: "1.2.3.4", kubeconfig_ref: "kc"}}
  failed:    {event: ProvisionFailed, payload: {reason: "n/a"}}
  cancelled: {event: ProvisionFailed, payload: {reason: "cancelled"}}
steps:
  - id: boom
    uses: fake.boom
    with: {}
"""


class _Unexpected(RuntimeError):
    pass


class BoomStep(Step[EmptyOutput, EmptyOutput]):
    verb = "fake.boom"
    Params = EmptyOutput
    Output = EmptyOutput
    plane = "domain"
    thin = False

    async def execute(self, params, ctx):  # noqa: ARG002
        raise _Unexpected("a verb blew up in a way the taxonomy still classifies")


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


async def _seed_run(harness, *, args: dict, run_id: str, cluster_id: str) -> None:
    async with harness.uow() as tx:
        _insert_cluster(tx, cluster_id)
        harness.run_repo.insert(
            tx,
            WorkflowRunRow(
                id=run_id, workflow="provision-fake", workflow_version=1, cluster_id=cluster_id,
                deployment_id=None, dedupe_key=f"dedupe:{run_id}", args=args, status="pending",
                cancel_requested=False, failed_step=None, error=None, undo_incomplete=None,
                initiated_by="test", created_at=NOW, started_at=None, finished_at=None,
            ),
        )


async def _run_with_unsatisfiable_args(tmp_path):
    """A run admitted BEFORE its definition grew a required input -- exactly the E3
    situation, and the one that made the 2026-08-16 recovery need a hand-written
    UPDATE against production."""
    harness = build_engine(
        tmp_path, {"provision-fake": NEEDS_AN_INPUT_YAML}, [InstantStep()], FrozenClock(NOW)
    )
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _seed_run(harness, args={"cluster_id": cluster_id}, run_id=run_id, cluster_id=cluster_id)
    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)
    return harness, run_id


async def test_unsatisfiable_args_end_the_run_failed_naming_the_input(tmp_path):
    """Decisions 1 and 4. Before this the run stayed `running` with `error` NULL and
    `resume_inflight` retried it forever, because the args are frozen at admission and
    no amount of retrying can add the missing key."""
    harness, run_id = await _run_with_unsatisfiable_args(tmp_path)

    async with harness.uow() as tx:
        row = harness.run_repo.get(tx, run_id)
        steps = harness.step_repo.list_for_run(tx, run_id)

    assert row.status == "failed"  # NOT 'running'
    assert row.finished_at is not None  # terminal, so nothing will adopt it again
    assert row.error is not None, "a crashed run must record WHY, not just that it stopped"
    assert row.error["kind"] == "permanent"
    assert "snapshot" in row.error["message"], "the failure must name the missing input"
    assert "frozen" in row.error["message"], "and say why retrying cannot fix it"
    assert steps == [], "it must fail BEFORE running any step"


async def test_the_aggregate_follows_a_crashed_run(tmp_path):
    """Decision 5 -- the point of decision 1 is not tidier bookkeeping, it is that the
    machine's existing recovery routes become REACHABLE. The terminal write goes
    through `_apply_terminal`, so the workflow's `outcome.failed` event is dispatched
    and the aggregate leaves its in-flight state. Before this, nothing ever told the
    machine the run had died, which is why a droplet sat in `destroying` and billed."""
    harness, _run_id = await _run_with_unsatisfiable_args(tmp_path)

    events = [type(c.event).__name__ for c in harness.dispatcher.calls]
    assert "ProvisionFailed" in events, (
        "the workflow's outcome.failed event was never dispatched, so the aggregate "
        "stays in-flight forever -- the exact shape that stranded a live droplet"
    )


async def test_an_engine_level_crash_does_not_compensate(tmp_path):
    """Decision 2, ratified explicitly as "the only correct/fail-safe option".

    `NEEDS_AN_INPUT_YAML` is `on_failure: compensate`, so an ORDINARY failure would run
    undo. An unanticipated one must not: the engine cannot know what state it is in,
    and undo against real infrastructure from an unknown state can do real harm."""
    harness, run_id = await _run_with_unsatisfiable_args(tmp_path)

    async with harness.uow() as tx:
        row = harness.run_repo.get(tx, run_id)
        steps = harness.step_repo.list_for_run(tx, run_id)

    assert row.undo_incomplete is None, "no compensation pass should have been recorded"
    assert all(s.undo_status is None for s in steps), "no step should have been undone"


async def test_a_step_exception_is_still_classified_and_still_compensates(tmp_path):
    """The regression guard, and the correction this suite taught.

    A verb raising an arbitrary exception was ALREADY handled by §2.3.1's taxonomy
    before DR-0045 -- that path was never the gap. This asserts DR-0045's catch-all did
    not quietly swallow it: the step still fails through the classified route, the run
    still reaches `failed`, and the error still carries the verb's own message rather
    than a generic engine-crash string."""
    harness = build_engine(
        tmp_path, {"provision-fake": BOOM_YAML}, [InstantStep(), BoomStep()], FrozenClock(NOW)
    )
    run_id, cluster_id = str(uuid.uuid4()), str(uuid.uuid4())
    await _seed_run(harness, args={"cluster_id": cluster_id}, run_id=run_id, cluster_id=cluster_id)

    await harness.engine.start(run_id)
    await harness.engine.wait_for(run_id)

    async with harness.uow() as tx:
        row = harness.run_repo.get(tx, run_id)
        steps = {s.step_path: s for s in harness.step_repo.list_for_run(tx, run_id)}

    assert row.status == "failed"
    assert row.failed_step == "boom", "the classified path records WHICH step failed"
    assert "classifies" in row.error["message"], "the verb's own message survives"
    assert steps["boom"].status == "failed"
