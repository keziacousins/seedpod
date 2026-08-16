"""tests/engine/test_step.py — StepContext.note()/progress() persistence seam and the
Step ABC contract (non-gateable/non-undoable verbs raise NotImplementedError on
poll_ready/undo). Cancellation/subprocess behavior lives in test_cancel.py.
"""

from __future__ import annotations

import pytest

from seedpod.engine.step import EmptyOutput, StepServices
from tests.engine.fakes import (
    FakeEchoStep,
    FakeSubprocessManager,
    RecordingNoteSink,
    RecordingProgressSink,
    make_step_context,
)


async def test_note_persists_via_injected_sink_before_returning():
    sink = RecordingNoteSink()
    ctx = make_step_context(note_sink=sink, step_path="wave[1].apply")
    await ctx.note(droplet_id="abc123")
    assert sink.calls == [(ctx.run_id, "wave[1].apply", {"droplet_id": "abc123"})]


async def test_note_merges_multiple_calls_as_separate_sink_invocations():
    sink = RecordingNoteSink()
    ctx = make_step_context(note_sink=sink)
    await ctx.note(a="1")
    await ctx.note(b="2")
    assert len(sink.calls) == 2


async def test_progress_forwards_message_and_fields_to_sink():
    sink = RecordingProgressSink()
    ctx = make_step_context(progress_sink=sink, cluster_id="c1", workflow="deploy-waves", step_path="wave[0].ready")
    await ctx.progress("waiting for rollout", replicas_ready=2, replicas_total=3)
    assert len(sink.calls) == 1
    run_id, cluster_id, workflow, step_path, attempt, message, fields = sink.calls[0]
    assert cluster_id == "c1"
    assert workflow == "deploy-waves"
    assert step_path == "wave[0].ready"
    assert message == "waiting for rollout"
    assert fields == {"replicas_ready": 2, "replicas_total": 3}


async def test_progress_never_raises_to_the_step_even_if_sink_fails():
    sink = RecordingProgressSink(raise_on_call=True)
    ctx = make_step_context(progress_sink=sink)
    await ctx.progress("this will explode internally")  # must not raise


async def test_progress_never_touches_the_cursor():
    """progress() has no access to anything that could mutate step status; this is
    structural (no repositories/cursor object reachable from ctx.progress at all)."""
    sink = RecordingProgressSink()
    ctx = make_step_context(progress_sink=sink)
    before_attempt = ctx.attempt
    await ctx.progress("tick")
    assert ctx.attempt == before_attempt


# ----------------------------------------------------------------------------
# Step ABC contract: non-gateable/non-undoable verbs
# ----------------------------------------------------------------------------


async def test_execute_runs_on_a_plain_step():
    from tests.engine.fakes import EchoParams

    step = FakeEchoStep()
    ctx = make_step_context()
    output = await step.execute(EchoParams(message="hello"), ctx)
    assert output.echoed == "hello"


async def test_poll_ready_not_implemented_for_non_gateable_step():
    from tests.engine.fakes import EchoParams

    step = FakeEchoStep()
    ctx = make_step_context()
    with pytest.raises(NotImplementedError):
        await step.poll_ready(EchoParams(), EmptyOutput(), ctx)


async def test_undo_not_implemented_for_non_undoable_step():
    from tests.engine.fakes import EchoParams

    step = FakeEchoStep()
    ctx = make_step_context()
    with pytest.raises(NotImplementedError):
        await step.undo(EchoParams(), None, {}, ctx)


def test_step_services_defaults_are_safe_placeholders():
    services = StepServices(subprocess_manager=FakeSubprocessManager())
    assert services.providers == {}
    assert services.repositories is None
    assert services.secret_manager is None
