"""tests/engine/steps/test_deploy_restore_steps.py — Round 10's "restore-and-
rehydrate" component: ``deploy.restore_snapshot``
(``seedpod/engine/steps/deploy_restore.py``).

``SnapshotService`` (``seedpod/app/services/snapshot_service.py``) is the
real, frozen-for-this-round collaborator this Step delegates to -- rather
than a real DB-backed instance (which would need a live kubectl transport to
exercise ``restore``'s own pod-exec calls, not this Step's concern), a small
hand-written FAKE stands in (CLAUDE.md testing posture: no Mock/patch
anywhere), structurally satisfying the two methods this Step actually calls
(``list``/``restore``) and recording every call it received so each test can
assert exactly what this Step decided to do with it.

Both restore modes (explicit id; ``restore_from_latest`` criteria resolved at
execute time), the ``spec=None``/no-snapshot-matched no-ops, the
``SnapshotNotFound`` -> ``PermanentError`` split, and the
``RestoreResult.success=False`` -> ``TransientError`` split (this verb's own
retry-on-ambiguous-failure posture, module docstring)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import SecretStr

from seedpod.app.services.snapshot_service import (
    RestoreResult,
    SnapshotIncompatible,
    SnapshotNotFound,
)
from seedpod.core.clock import FrozenClock
from seedpod.core.deploy_wave import RestoreFromLatest, SnapshotRestoreSpec
from seedpod.core.errors import (
    ErrorCode,
    InfrastructureUnreachableError,
    PermanentError,
    TransientError,
)
from seedpod.data.repositories import SnapshotRow
from seedpod.engine.steps.deploy_restore import DeployRestoreSnapshot, RestoreSnapshotParams
from tests.engine.fakes import RecordingProgressSink, make_step_context

NOW = datetime(2026, 8, 8, 12, 0, 0, tzinfo=UTC)
_KUBECONFIG = SecretStr("fake-kubeconfig")


def _snapshot_row(
    snapshot_id: str, *, branch: str | None = None, profile: str = "exampleco-dev-stack-nodns", created_at: datetime = NOW
) -> SnapshotRow:
    return SnapshotRow(
        id=snapshot_id, name=snapshot_id, description=None, source_cluster_id="c1", source_cluster_slug="c1",
        branch=branch, deployment_profile=profile, services=[{"service_name": "postgres"}],
        storage_path=f"/tmp/{snapshot_id}", total_size_bytes=100, is_auto=False, created_by="api:test",
        created_at=created_at,
    )


class _FakeSnapshots:
    """Structurally satisfies the two ``SnapshotService`` methods
    ``DeployRestoreSnapshot`` actually calls -- ``list``/``restore`` -- and
    records every call so each test can assert this Step's own decisions
    (which snapshot id it resolved, what it passed through) rather than
    ``SnapshotService``'s real internals (frozen for this round, already
    tested on its own)."""

    def __init__(self, *, snapshots: tuple[SnapshotRow, ...] = (), restore_result=None, restore_error=None):
        self._snapshots = snapshots
        self._restore_result = restore_result
        self._restore_error = restore_error
        self.list_calls: list[tuple[str | None, str | None]] = []
        self.restore_calls: list[dict] = []

    async def list(self, *, branch: str | None = None, profile: str | None = None) -> list[SnapshotRow]:
        self.list_calls.append((branch, profile))
        matched = [
            s for s in self._snapshots
            if (branch is None or s.branch == branch) and (profile is None or s.deployment_profile == profile)
        ]
        return sorted(matched, key=lambda s: s.created_at, reverse=True)

    async def restore(self, snapshot_id: str, *, cluster_id: str, services, run_migrations: bool, actor: str):
        self.restore_calls.append(
            {"snapshot_id": snapshot_id, "cluster_id": cluster_id, "services": services,
             "run_migrations": run_migrations, "actor": actor}
        )
        if self._restore_error is not None:
            raise self._restore_error
        assert self._restore_result is not None
        return self._restore_result


def _success(services_restored: tuple[str, ...] = ("postgres",)) -> RestoreResult:
    return RestoreResult(success=True, services_restored=list(services_restored), services_failed=[], error=None)


def _failure(*, services_failed: tuple[str, ...] = ("postgres",), error: str | None = "connection refused") -> RestoreResult:
    return RestoreResult(success=False, services_restored=[], services_failed=list(services_failed), error=error)


def test_declares_the_dr_0022_contract():
    step = DeployRestoreSnapshot(snapshots=object(), clock=object())  # construction only
    assert step.verb == "deploy.restore_snapshot"
    assert step.plane == "domain"
    assert step.thin is False
    assert step.gateable is False
    assert step.undoable is False
    assert step.idempotent is True


async def test_spec_none_is_a_no_op_and_never_calls_the_snapshot_service():
    snapshots = _FakeSnapshots()
    step = DeployRestoreSnapshot(snapshots=snapshots, clock=FrozenClock(NOW))

    output = await step.execute(RestoreSnapshotParams(kubeconfig=_KUBECONFIG, spec=None), make_step_context())

    assert output.model_dump() == {}
    assert snapshots.list_calls == []
    assert snapshots.restore_calls == []


async def test_explicit_restore_from_snapshot_calls_restore_with_that_id():
    snapshots = _FakeSnapshots(restore_result=_success())
    step = DeployRestoreSnapshot(snapshots=snapshots, clock=FrozenClock(NOW))
    spec = SnapshotRestoreSpec(restore_from_snapshot="snap-explicit-1")

    await step.execute(
        RestoreSnapshotParams(kubeconfig=_KUBECONFIG, spec=spec), make_step_context(cluster_id="c-explicit")
    )

    assert snapshots.list_calls == []  # explicit id needs no lookup (DR-0028 decision 3)
    assert snapshots.restore_calls == [
        {"snapshot_id": "snap-explicit-1", "cluster_id": "c-explicit", "services": None,
         "run_migrations": False, "actor": "system:deploy"}
    ]


async def test_explicit_restore_wins_over_latest_when_both_are_set():
    """v1's own precedence (deployment_job.py:246-248, cited by
    ``SnapshotRestoreSpec``'s own docstring): ``restore_from_snapshot`` wins
    outright when both happen to be present."""
    snapshots = _FakeSnapshots(
        snapshots=(_snapshot_row("snap-latest", branch="main"),), restore_result=_success()
    )
    step = DeployRestoreSnapshot(snapshots=snapshots, clock=FrozenClock(NOW))
    spec = SnapshotRestoreSpec(
        restore_from_snapshot="snap-explicit-wins", restore_from_latest=RestoreFromLatest(branch="main")
    )

    await step.execute(RestoreSnapshotParams(kubeconfig=_KUBECONFIG, spec=spec), make_step_context())

    assert snapshots.list_calls == []
    assert snapshots.restore_calls[0]["snapshot_id"] == "snap-explicit-wins"


async def test_restore_from_latest_resolves_the_most_recent_matching_snapshot():
    older = _snapshot_row("snap-older", branch="main", created_at=NOW - timedelta(days=1))
    newer = _snapshot_row("snap-newer", branch="main", created_at=NOW)
    other_branch = _snapshot_row("snap-other-branch", branch="staging", created_at=NOW + timedelta(days=1))
    snapshots = _FakeSnapshots(snapshots=(older, newer, other_branch), restore_result=_success())
    step = DeployRestoreSnapshot(snapshots=snapshots, clock=FrozenClock(NOW))
    spec = SnapshotRestoreSpec(restore_from_latest=RestoreFromLatest(branch="main"))

    await step.execute(RestoreSnapshotParams(kubeconfig=_KUBECONFIG, spec=spec), make_step_context())

    assert snapshots.list_calls == [("main", None)]
    assert snapshots.restore_calls[0]["snapshot_id"] == "snap-newer"


async def test_restore_from_latest_filters_by_max_age_days_via_the_injected_clock():
    """CLAUDE.md's hard rule: no ambient now() -- the cutoff is computed from
    the injected Clock, never a bare datetime.now()."""
    too_old = _snapshot_row("snap-too-old", created_at=NOW - timedelta(days=10))
    fresh = _snapshot_row("snap-fresh", created_at=NOW - timedelta(days=1))
    snapshots = _FakeSnapshots(snapshots=(too_old, fresh), restore_result=_success())
    step = DeployRestoreSnapshot(snapshots=snapshots, clock=FrozenClock(NOW))
    spec = SnapshotRestoreSpec(restore_from_latest=RestoreFromLatest(max_age_days=7))

    await step.execute(RestoreSnapshotParams(kubeconfig=_KUBECONFIG, spec=spec), make_step_context())

    assert snapshots.restore_calls[0]["snapshot_id"] == "snap-fresh"


async def test_restore_from_latest_no_match_is_a_no_op_not_an_error():
    """v1's own outcome, verbatim (deployment_job.py:266-269): "Not an error -
    just no data to restore"."""
    snapshots = _FakeSnapshots(snapshots=())
    step = DeployRestoreSnapshot(snapshots=snapshots, clock=FrozenClock(NOW))
    spec = SnapshotRestoreSpec(restore_from_latest=RestoreFromLatest(branch="main"))

    output = await step.execute(RestoreSnapshotParams(kubeconfig=_KUBECONFIG, spec=spec), make_step_context())

    assert output.model_dump() == {}
    assert snapshots.restore_calls == []


async def test_spec_with_neither_mode_set_is_a_no_op():
    snapshots = _FakeSnapshots()
    step = DeployRestoreSnapshot(snapshots=snapshots, clock=FrozenClock(NOW))

    output = await step.execute(
        RestoreSnapshotParams(kubeconfig=_KUBECONFIG, spec=SnapshotRestoreSpec()), make_step_context()
    )

    assert output.model_dump() == {}
    assert snapshots.restore_calls == []


async def test_services_allow_list_is_threaded_through():
    snapshots = _FakeSnapshots(restore_result=_success())
    step = DeployRestoreSnapshot(snapshots=snapshots, clock=FrozenClock(NOW))
    spec = SnapshotRestoreSpec(restore_from_snapshot="snap-1", services=["postgres"])

    await step.execute(RestoreSnapshotParams(kubeconfig=_KUBECONFIG, spec=spec), make_step_context())

    assert snapshots.restore_calls[0]["services"] == ["postgres"]


async def test_successful_restore_returns_empty_output():
    snapshots = _FakeSnapshots(restore_result=_success())
    step = DeployRestoreSnapshot(snapshots=snapshots, clock=FrozenClock(NOW))
    spec = SnapshotRestoreSpec(restore_from_snapshot="snap-1")

    output = await step.execute(RestoreSnapshotParams(kubeconfig=_KUBECONFIG, spec=spec), make_step_context())

    assert output.model_dump() == {}


async def test_snapshot_not_found_raises_permanent_error_not_retried():
    """A stale/typo'd explicit id -- provably useless to retry."""
    snapshots = _FakeSnapshots(restore_error=SnapshotNotFound("snap-gone"))
    step = DeployRestoreSnapshot(snapshots=snapshots, clock=FrozenClock(NOW))
    spec = SnapshotRestoreSpec(restore_from_snapshot="snap-gone")

    with pytest.raises(PermanentError):
        await step.execute(RestoreSnapshotParams(kubeconfig=_KUBECONFIG, spec=spec), make_step_context())


async def test_failed_restore_result_raises_transient_error_so_the_engine_retries():
    """Restoring into a not-yet-ready database is a REAL, expected failure
    mode on this wave (module docstring) -- a failed ``RestoreResult`` must
    raise ``TransientError``, not ``PermanentError``, so the workflow's own
    explicit retry (``config/workflows/deploy-waves.yml``'s ``restore`` step,
    ~180s -- replacing the earlier, far-too-short ``kubectl_default``) gives a
    cold-started database a real chance."""
    snapshots = _FakeSnapshots(restore_result=_failure())
    step = DeployRestoreSnapshot(snapshots=snapshots, clock=FrozenClock(NOW))
    spec = SnapshotRestoreSpec(restore_from_snapshot="snap-1")

    with pytest.raises(TransientError) as exc_info:
        await step.execute(RestoreSnapshotParams(kubeconfig=_KUBECONFIG, spec=spec), make_step_context())

    assert exc_info.value.detail["snapshot_id"] == "snap-1"
    assert "postgres" in exc_info.value.detail["services_failed"]


async def test_snapshot_incompatible_raises_permanent_error_not_retried():
    """DR-0030 fix 2: a pre-flight incompatibility (``SnapshotIncompatible``,
    already a ``PermanentError``) must reach the engine un-retried -- never
    folded into the ``TransientError`` every ``RestoreResult.success=False``
    below gets. This Step's own ``except SnapshotIncompatible: raise`` is
    what this test pins (removing that clause would still pass today, since
    the exception IS a ``PermanentError`` already and would propagate
    unhandled -- but the explicit clause documents the deliberate choice, and
    this test guards against a future refactor accidentally re-wrapping it
    into something the ``except SnapshotNotFound``/blanket-retry logic could
    swallow)."""
    snapshots = _FakeSnapshots(
        restore_error=SnapshotIncompatible(
            target_profile="infrastructure-only", snapshot_profile="snapshot-stack", missing_services=["web"]
        )
    )
    step = DeployRestoreSnapshot(snapshots=snapshots, clock=FrozenClock(NOW))
    spec = SnapshotRestoreSpec(restore_from_snapshot="snap-1")

    with pytest.raises(PermanentError) as exc_info:
        await step.execute(RestoreSnapshotParams(kubeconfig=_KUBECONFIG, spec=spec), make_step_context())

    assert not isinstance(exc_info.value, TransientError)
    assert "web" in str(exc_info.value)


async def test_infrastructure_unreachable_error_propagates_unwrapped():
    """CLAUDE.md's hard rule: ``InfrastructureUnreachableError`` "never
    triggers compensation and is never conflated with absence" -- it must
    pass through this Step's own ``execute`` completely UN-wrapped (not
    turned into ``TransientError``/``PermanentError``, and not swallowed),
    so the engine's blocked-park law -- not ``Schedule`` -- is what handles
    it (``seedpod/engine/schedule.py``: ``Outcome.UNREACHABLE`` "NEVER
    consumes Schedule budget"). This Step catches only ``SnapshotNotFound``/
    ``SnapshotIncompatible`` and lets everything else (including this)
    propagate -- a future refactor widening either ``except`` clause to a
    bare ``Exception`` would fold this into a retried/failed outcome, and
    this test would catch that."""
    snapshots = _FakeSnapshots(
        restore_error=InfrastructureUnreachableError(
            "api server unreachable", code=ErrorCode.ENDPOINT_UNREACHABLE, provider="kubectl", command="get pods",
        )
    )
    step = DeployRestoreSnapshot(snapshots=snapshots, clock=FrozenClock(NOW))
    spec = SnapshotRestoreSpec(restore_from_snapshot="snap-1")

    with pytest.raises(InfrastructureUnreachableError):
        await step.execute(RestoreSnapshotParams(kubeconfig=_KUBECONFIG, spec=spec), make_step_context())


async def test_restore_from_latest_max_age_days_zero_means_no_filter():
    """v1's own guard is truthy (``deployment_job.py:254``:
    ``if criteria.get("max_age_days"):``), so ``max_age_days: 0`` means "do
    not filter", not "cutoff = now" -- which would filter out every snapshot
    and silently drop an explicitly requested restore. A snapshot ten days
    old must still be selected when ``max_age_days=0``."""
    old = _snapshot_row("snap-old", created_at=NOW - timedelta(days=10))
    snapshots = _FakeSnapshots(snapshots=(old,), restore_result=_success())
    step = DeployRestoreSnapshot(snapshots=snapshots, clock=FrozenClock(NOW))
    spec = SnapshotRestoreSpec(restore_from_latest=RestoreFromLatest(max_age_days=0))

    await step.execute(RestoreSnapshotParams(kubeconfig=_KUBECONFIG, spec=spec), make_step_context())

    assert snapshots.restore_calls[0]["snapshot_id"] == "snap-old"


async def test_explicit_empty_id_alongside_latest_falls_through_a_documented_divergence():
    """A narrow, DOCUMENTED divergence from v1 (see ``_resolve_snapshot_id``'s
    own docstring, ``seedpod/engine/steps/deploy_restore.py``): v1 decides
    mode precedence by dict KEY PRESENCE (an explicitly-set-but-EMPTY
    ``restore_from_snapshot`` wins outright and never falls through), while
    this Step tests truthiness (a plain pydantic model has no wire-preserved
    "was this key present" fact once it has crossed the workflow's own
    scope-binding boundary). The ONLY combination where this is observable:
    ``restore_from_snapshot=""`` set alongside a working ``restore_from_latest``
    -- this Step evaluates ``restore_from_latest`` instead of short-circuiting
    to a no-op the way v1 would. Pinned here so a future change cannot make
    this WORSE (e.g. silently dropping the restore entirely) without a test
    failing."""
    newer = _snapshot_row("snap-newer", branch="main", created_at=NOW)
    snapshots = _FakeSnapshots(snapshots=(newer,), restore_result=_success())
    step = DeployRestoreSnapshot(snapshots=snapshots, clock=FrozenClock(NOW))
    spec = SnapshotRestoreSpec(restore_from_snapshot="", restore_from_latest=RestoreFromLatest(branch="main"))

    await step.execute(RestoreSnapshotParams(kubeconfig=_KUBECONFIG, spec=spec), make_step_context())

    assert snapshots.restore_calls[0]["snapshot_id"] == "snap-newer"


# ---------------------------------------------------------------------------
# DR-0035 — the SPA's live signal for an IN-WORKFLOW restore.
# ---------------------------------------------------------------------------


async def test_a_successful_restore_emits_progress_the_spa_can_refetch_on():
    """DR-0035 decision 3. Deliberately `workflow_progress`, not a second
    `snapshot_restore_completed` broadcast -- the bespoke topic stays with the
    single-attempt REST path (`api/routers/snapshots.py`), because this step retries
    up to 19 times by design and a terminal-sounding per-attempt topic would fill the
    HUD with failures for a run that then succeeds."""
    snapshots = _FakeSnapshots(restore_result=_success())
    step = DeployRestoreSnapshot(snapshots=snapshots, clock=FrozenClock(NOW))
    spec = SnapshotRestoreSpec(restore_from_snapshot="snap-1")
    progress = RecordingProgressSink()

    await step.execute(
        RestoreSnapshotParams(kubeconfig=_KUBECONFIG, spec=spec),
        make_step_context(cluster_id="c-1", progress_sink=progress),
    )

    messages = [call[5] for call in progress.calls]
    assert any("restore completed" in m for m in messages)
    fields = next(call[6] for call in progress.calls if "restore completed" in call[5])
    assert fields["snapshot_id"] == "snap-1"
    # cluster_id rides the envelope, which is what every SPA handler filters on.
    assert all(call[1] == "c-1" for call in progress.calls)


async def test_a_failed_attempt_says_why_before_raising():
    """DR-0033's lesson applied here: without this a 19-attempt restore is silent
    until the whole budget is exhausted. Smoke 10's restore took 39s and emitted
    nothing at all."""
    snapshots = _FakeSnapshots(restore_result=_failure())
    step = DeployRestoreSnapshot(snapshots=snapshots, clock=FrozenClock(NOW))
    spec = SnapshotRestoreSpec(restore_from_snapshot="snap-1")
    progress = RecordingProgressSink()

    with pytest.raises(TransientError):
        await step.execute(
            RestoreSnapshotParams(kubeconfig=_KUBECONFIG, spec=spec),
            make_step_context(cluster_id="c-1", progress_sink=progress),
        )

    fields = next(call[6] for call in progress.calls if "did not complete" in call[5])
    assert fields["error"]


async def test_a_no_op_restore_emits_no_progress_at_all():
    """No spec / no matching snapshot is not a restore, so it must not look like one
    in the HUD."""
    snapshots = _FakeSnapshots()
    step = DeployRestoreSnapshot(snapshots=snapshots, clock=FrozenClock(NOW))
    progress = RecordingProgressSink()

    await step.execute(
        RestoreSnapshotParams(kubeconfig=_KUBECONFIG, spec=None),
        make_step_context(progress_sink=progress),
    )

    assert progress.calls == []
