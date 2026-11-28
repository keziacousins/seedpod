"""``StepContext`` for an instance run (DR-0055): it carries ``instance_id`` under its own name,
never presents it as a cluster id, and reports its progress under the instance.
"""

from __future__ import annotations

import uuid

import pytest

from seedpod.engine.cancel import CancelToken
from seedpod.engine.step import StepContext, StepServices
from tests.engine.fakes import (
    FakeSubprocessManager,
    RecordingNoteSink,
    RecordingProgressSink,
    make_step_context,
)


def test_an_instance_context_has_an_instance_id_and_no_cluster_id():
    ctx = make_step_context(instance_id="i1")
    assert ctx.instance_id == "i1"
    with pytest.raises(ValueError, match="instance 'i1', not to a cluster"):
        ctx.cluster_id  # noqa: B018 -- the access is the test


def test_a_cluster_context_is_unchanged_and_has_no_instance_id():
    ctx = make_step_context(cluster_id="c1")
    assert ctx.cluster_id == "c1"
    with pytest.raises(ValueError, match="cluster 'c1', not to an instance"):
        ctx.instance_id  # noqa: B018


@pytest.mark.parametrize("ids", [{}, {"cluster_id": "c", "instance_id": "i"}])
def test_a_context_needs_exactly_one_aggregate(ids):
    with pytest.raises(ValueError, match="exactly one"):
        StepContext(
            run_id=uuid.uuid4(), workflow="w", step_path="s", attempt=1, cancel=CancelToken(),
            services=StepServices(subprocess_manager=FakeSubprocessManager()),
            note_sink=RecordingNoteSink(), progress_sink=RecordingProgressSink(), **ids,
        )


async def test_progress_on_an_instance_run_names_the_instance_and_no_cluster():
    sink = RecordingProgressSink()
    ctx = make_step_context(instance_id="i1", progress_sink=sink)
    await ctx.progress("booting", pct=10)
    (call,) = sink.calls
    assert call[1] is None  # no cluster id
    assert sink.instance_ids == ["i1"]


async def test_progress_on_a_cluster_run_is_exactly_what_it_was():
    sink = RecordingProgressSink()
    ctx = make_step_context(cluster_id="c1", progress_sink=sink)
    await ctx.progress("booting")
    assert sink.calls[0][1] == "c1" and sink.instance_ids == [None]
