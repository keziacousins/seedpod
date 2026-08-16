"""``App.start()``/``App.stop()`` -- the amended lifecycle (docs/design/seam-d-
foundation.md Decision 8, AS AMENDED by docs/design/coherence-review.md Conflict
15: check_ready-once, TimerService, the reordered start/stop sequence). Real
sqlite tmp db, ``FrozenClock``, hand-built fakes -- zero Mock/patch (CLAUDE.md).
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from cryptography.fernet import Fernet

from seedpod.app.config import AppConfig
from seedpod.app.factory import build_app
from seedpod.core.clock import FrozenClock
from seedpod.data.repositories import ApiKeyRepository
from tests.fakes import FakeProvider, sequential_ids

_NOW = datetime(2026, 1, 1, tzinfo=UTC)


class _CheckReadyRecordingProvider(FakeProvider):
    """Hand-built fake (CLAUDE.md: no Mock/patch): records whether/how-many-times
    ``check_ready()`` was awaited, proving App.start()'s "fail at startup, not
    mid-provision" preflight (coherence-review Conflict 15) actually runs."""

    def __init__(self) -> None:
        self.check_ready_calls = 0

    async def check_ready(self) -> None:
        self.check_ready_calls += 1


def _config(tmp_path: Path, test_config_dir: Path, **overrides) -> AppConfig:
    overrides.setdefault("background_tasks", False)
    return AppConfig(
        database_url=f"sqlite:///{tmp_path}/t.db",
        secret_key_dev=Fernet.generate_key().decode(),
        config_dir=test_config_dir,
        **overrides,
    )


async def test_start_calls_check_ready_once_per_provider(tmp_path, test_config_dir):
    fake = _CheckReadyRecordingProvider()
    config = _config(tmp_path, test_config_dir)
    app = build_app(config, providers={"fake": fake}, clock=FrozenClock(_NOW), id_gen=sequential_ids())
    await app.start()
    try:
        assert fake.check_ready_calls == 1
    finally:
        await app.stop()


async def test_start_always_runs_executor_and_timers_regardless_of_background_tasks(tmp_path, test_config_dir):
    """"the outbox executor ALWAYS runs (it is correctness)" (Decision 8);
    "timers are correctness, like the executor" (coherence-review Conflict 15) --
    both must be running even with background_tasks=False."""
    config = _config(tmp_path, test_config_dir, background_tasks=False)
    app = build_app(config, providers={"fake": FakeProvider()}, clock=FrozenClock(_NOW), id_gen=sequential_ids())
    await app.start()
    try:
        assert app.executor.running
        assert app.timers.running
        # background_tasks=False: reconciliation/health must NOT have started.
        assert not app.services.reconciliation.running
        assert not app.services.health.running
    finally:
        await app.stop()


async def test_start_runs_reconciliation_and_health_iff_background_tasks(tmp_path, test_config_dir):
    config = _config(tmp_path, test_config_dir, background_tasks=True)
    app = build_app(config, providers={"fake": FakeProvider()}, clock=FrozenClock(_NOW), id_gen=sequential_ids())
    await app.start()
    try:
        assert app.services.reconciliation.running
        assert app.services.health.running
    finally:
        await app.stop()
        assert not app.services.reconciliation.running
        assert not app.services.health.running


async def test_stop_reverses_cleanly_and_is_idempotent(tmp_path, test_config_dir):
    config = _config(tmp_path, test_config_dir, background_tasks=True)
    app = build_app(config, providers={"fake": FakeProvider()}, clock=FrozenClock(_NOW), id_gen=sequential_ids())
    await app.start()
    await app.stop()
    assert not app.executor.running
    assert not app.timers.running
    assert not app.services.reconciliation.running
    assert not app.services.health.running
    # idempotent: a second stop() must not raise (every collaborator's own
    # stop() is idempotent; App.stop() adds no state of its own).
    await app.stop()


async def test_stop_flushes_buffered_last_used_touches(tmp_path, test_config_dir):
    """DR-0044: `validate()` buffers the `last_used_at` touch in memory, so shutdown
    must write it out or it is simply lost.

    Deliberately runs with `background_tasks=False`, i.e. the periodic flush loop was
    never started -- that is the asymmetry `App.stop()` documents (the loop is gated,
    the final flush is not), and it is the case where forgetting to flush would go
    unnoticed, since an unflushed touch and a never-used key look identical.

    The write must also land BEFORE `db.dispose()`, which is why `api_keys.stop()`
    sits where it does in the teardown order; if it were moved after, this fails."""
    config = _config(tmp_path, test_config_dir, background_tasks=False)
    app = build_app(config, providers={"fake": FakeProvider()}, clock=FrozenClock(_NOW), id_gen=sequential_ids())
    await app.start()
    record, plaintext = await app.services.api_keys.create_api_key(
        username="u", environment="all", permissions=[]
    )
    assert await app.services.api_keys.validate(plaintext) is not None

    async with app.uow() as tx:
        assert ApiKeyRepository().get_by_id(tx, record.id).last_used_at is None

    await app.stop()

    # A fresh App over the same database file -- the touch has to be ON DISK, not
    # merely still sitting in the stopped instance's buffer.
    reopened = build_app(config, providers={"fake": FakeProvider()}, clock=FrozenClock(_NOW), id_gen=sequential_ids())
    await reopened.start()
    try:
        async with reopened.uow() as tx:
            assert ApiKeyRepository().get_by_id(tx, record.id).last_used_at == _NOW
    finally:
        await reopened.stop()


async def test_running_context_manager_starts_and_stops(tmp_path, test_config_dir):
    config = _config(tmp_path, test_config_dir)
    app = build_app(config, providers={"fake": FakeProvider()}, clock=FrozenClock(_NOW), id_gen=sequential_ids())
    async with app.running() as running_app:
        assert running_app is app
        assert app.executor.running
    assert not app.executor.running


async def test_restart_after_stop_is_clean(tmp_path, test_config_dir):
    """migrate() is a no-op past the pinned user_version; a second start() after
    a stop() must not raise or re-apply migrations destructively."""
    config = _config(tmp_path, test_config_dir)
    app = build_app(config, providers={"fake": FakeProvider()}, clock=FrozenClock(_NOW), id_gen=sequential_ids())
    await app.start()
    await app.stop()
    await app.start()
    try:
        assert app.executor.running
    finally:
        await app.stop()


async def test_services_container_carries_all_six_services(tmp_path, test_config_dir):
    """Round 6 (app-services component): the four thin application services are
    now fully wired alongside the two already-committed Round-5 runtime
    services -- nothing in ``app.services`` is ``None`` post-``build_app()``."""
    from seedpod.app.services import ApiKeyService, ClusterService, DeploymentService, SecretService

    config = _config(tmp_path, test_config_dir)
    app = build_app(config, providers={"fake": FakeProvider()}, clock=FrozenClock(_NOW), id_gen=sequential_ids())
    assert isinstance(app.services.clusters, ClusterService)
    assert isinstance(app.services.deployments, DeploymentService)
    assert isinstance(app.services.secrets, SecretService)
    assert isinstance(app.services.api_keys, ApiKeyService)
    assert app.services.reconciliation is not None
    assert app.services.health is not None
