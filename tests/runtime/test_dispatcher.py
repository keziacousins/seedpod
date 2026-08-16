"""``seedpod/runtime/dispatcher.py`` -- the ONLY write path for cluster/deployment
state (docs/design/coherence-review.md Conflict 3). Real tmp SQLite, no mocks.

Covers: atomicity (a later-effect failure rolls back everything the same
transaction already wrote), Ignore writes nothing, Cascade writes per-deployment
audits+effects in the SAME transaction, ``tx=`` chaining commits exactly once,
birth via ``record=``, ``effect_id`` determinism, lane/status assignment per
``EffectKind``, unretried ``StaleVersion`` propagation, and audit ``reason``/
``context`` derivation from the event (DR-0007).
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError

from seedpod.core.codec import encode
from seedpod.core.effects import (
    CancelTimer,
    CancelWorkflow,
    Cascade,
    Notify,
    Persist,
    RunWorkflow,
    ScheduleTimer,
)
from seedpod.core.events import (
    ClusterGone,
    CreateRequested,
    DeployRequested,
    DestroyRequested,
    Discovered,
    DiscoveredInfo,
    HealthCheckFailed,
    InfraMissingObserved,
    ProvisionFailed,
    ProvisionSucceeded,
)
from seedpod.core.machine import StaleVersion
from seedpod.core.records import (
    ClusterRecord,
    ClusterState,
    DeploymentState,
    Origin,
)
from seedpod.data.database import Database
from seedpod.data.repositories import OutboxRow
from seedpod.data.uow import UnitOfWork
from seedpod.runtime.dispatcher import outbox_row
from tests.runtime.conftest import NOW, FakePokeable, make_cluster_row, make_deployment_row

LATER = NOW + timedelta(minutes=5)


def _insert_deployment_audit(session, audit_id: str, cluster_id: str) -> None:
    """Minimal raw INSERT satisfying ``deployment_audits``' NOT NULL columns --
    only needed so ``DeploymentRecord.spec_ref``'s FK (``deployments.spec_ref
    REFERENCES deployment_audits(id)``) is satisfiable in a birth test; the
    encrypted columns' content is never read back here (``DeploymentAuditRepository``
    -- Fernet round-trip -- is exercised by tests/data/test_machine_repos.py, not
    this module)."""
    session.execute(
        text(
            """
            INSERT INTO deployment_audits
                (id, cluster_id, environment, triggering_repo, triggering_branch,
                 triggering_image, deployment_profile_name, resolution_strategy,
                 encrypted_resolved_manifests, encrypted_resolved_secrets, key_class,
                 created_at)
            VALUES
                (:id, :cluster_id, 'ephemeral', 'org/repo', 'main', 'ghcr.io/org/web:sha',
                 'default', 'latest', 'x', 'x', 'DEV', :created_at)
            """
        ),
        {"id": audit_id, "cluster_id": cluster_id, "created_at": NOW.isoformat()},
    )


# ---------------------------------------------------------------------------
# outbox_row(): effect_id determinism + lane/status per EffectKind
# ---------------------------------------------------------------------------


def test_effect_id_deterministic_across_identical_calls():
    eff = Notify(topic="cluster_state_changed", payload={"a": 1}, environment="ephemeral")
    row1 = outbox_row(eff, "cluster", "c1", 3, 1, now=NOW)
    row2 = outbox_row(eff, "cluster", "c1", 3, 1, now=LATER)  # different `now` -- effect_id unaffected
    assert row1.effect_id == row2.effect_id == "cluster/c1@3#1"


_A_CLUSTER_RECORD = ClusterRecord(
    id="c1", name="c1", state=ClusterState.ACTIVE, version=1,
    provider="fake", environment="ephemeral", origin=Origin.MANAGED,
)


@pytest.mark.parametrize(
    ("eff", "expected_lane", "expected_status"),
    [
        (Persist(record=_A_CLUSTER_RECORD, expected_version=0), "tx", "done"),
        (
            ScheduleTimer(
                aggregate_type="cluster", aggregate_id="c1", timer_key="ttl", fire_at=NOW,
                event=CreateRequested(at=NOW, actor="api:test"),
            ),
            "tx",
            "done",
        ),
        (CancelTimer(aggregate_type="cluster", aggregate_id="c1", timer_key="ttl"), "tx", "done"),
        (
            Cascade(cluster_id="c1", where_state=frozenset({DeploymentState.PENDING}),
                     event=ClusterGone(at=NOW, actor="cluster-machine")),
            "tx",
            "done",
        ),
        (Notify(topic="t", payload={}, environment=None), "drain", "pending"),
        (RunWorkflow(workflow="provision", cluster_id="c1"), "drain", "pending"),
        (CancelWorkflow(workflow="deploy", cluster_id="c1"), "drain", "pending"),
    ],
)
def test_lane_and_status_per_effect_kind(eff, expected_lane, expected_status):
    row = outbox_row(eff, "cluster", "c1", 1, 0, now=NOW)
    assert row.lane == expected_lane
    assert row.status == expected_status
    assert (row.done_at is not None) == (expected_status == "done")


# ---------------------------------------------------------------------------
# Ignore: writes NOTHING
# ---------------------------------------------------------------------------


async def test_ignore_writes_nothing_and_does_not_poke(uow, repos, dispatcher):
    async with uow() as tx:
        repos.clusters.insert(tx, make_cluster_row("c1", "demo", status="active"))

    fake = FakePokeable()
    dispatcher.attach_executor(fake)

    # ACTIVE x ProvisionSucceeded -> Ignore (duplicate report, machine.py's
    # `(ClusterState.ACTIVE, ProvisionSucceeded): _ignore`).
    event = ProvisionSucceeded(at=NOW, actor="engine:run:r1", public_ip="1.2.3.4", kubeconfig_ref="ref")
    result = await dispatcher.apply("cluster", "c1", event)

    assert result.effects == ()
    assert result.record.version == 0

    async with uow() as tx:
        row = repos.clusters.get(tx, "c1")
        assert row.version == 0
        assert row.status == "active"
        audits = repos.cluster_state_audits.list_for_cluster(tx, "c1")
        assert audits == []
        outbox = repos.outbox.list_for_aggregate(tx, "cluster", "c1")
        assert outbox == []

    assert fake.count == 0  # Ignore never pokes


# ---------------------------------------------------------------------------
# poke(): a non-Ignore apply() pokes both attached collaborators exactly once
# after the commit (latency hint only -- Conflict 3/15). Regressing this can't
# corrupt state, but would silently add poll-interval latency to every
# transition, so it gets its own positive-path coverage alongside the
# Ignore-never-pokes test above.
# ---------------------------------------------------------------------------


async def test_non_ignore_apply_pokes_executor_and_timers_once(uow, repos, dispatcher):
    async with uow() as tx:
        repos.clusters.insert(tx, make_cluster_row("c1", "demo", status="active"))

    executor = FakePokeable()
    timers = FakePokeable()
    dispatcher.attach_executor(executor)
    dispatcher.attach_timers(timers)

    event = InfraMissingObserved(at=NOW, actor="reconciler")
    result = await dispatcher.apply("cluster", "c1", event)

    assert result.effects != ()
    assert executor.count == 1
    assert timers.count == 1


# ---------------------------------------------------------------------------
# Audit reason/context derivation (DR-0007): end-to-end through Dispatcher.apply,
# proving the audit repos' add() derives both mechanically from the event it is
# handed -- the Dispatcher itself never inspects `reason` or builds `context`.
# ---------------------------------------------------------------------------


async def test_audit_reason_null_for_reasonless_event_full_event_in_context(uow, repos, dispatcher):
    birth_row = make_cluster_row("c1", "demo", status="new")
    event = CreateRequested(at=NOW, actor="api:test")  # declares no `reason` field
    await dispatcher.apply("cluster", "c1", event, record=birth_row)

    async with uow() as tx:
        audits = repos.cluster_state_audits.list_for_cluster(tx, "c1")
    assert len(audits) == 1
    assert audits[0].reason is None
    assert audits[0].context == encode(event)  # full tagged event, verbatim


async def test_audit_reason_derives_from_event_field_full_event_in_context(uow, repos, dispatcher):
    async with uow() as tx:
        repos.clusters.insert(tx, make_cluster_row("c1", "demo", status="provisioning"))

    event = ProvisionFailed(at=NOW, actor="engine:run:r1", reason="quota exceeded")
    await dispatcher.apply("cluster", "c1", event)

    async with uow() as tx:
        audits = repos.cluster_state_audits.list_for_cluster(tx, "c1")
    assert len(audits) == 1
    assert audits[0].reason == "quota exceeded"  # the event's OWN field -- never invented
    assert audits[0].context == encode(event)


# ---------------------------------------------------------------------------
# StaleVersion: propagates unretried, on a losing CAS
# ---------------------------------------------------------------------------


async def test_stale_cas_raises_stale_version_and_leaves_db_untouched(uow, repos, dispatcher):
    async with uow() as tx:
        repos.clusters.insert(tx, make_cluster_row("c1", "demo", status="active", version=0))

    # A caller-supplied row standing in for a stale in-memory read (version=5, while
    # the real row is still at version=0) -- record= is not birth-only mechanically
    # (only the resulting Persist's expected_version decides INSERT vs CAS UPDATE),
    # so this is still the right way to force a deterministic, unmocked CAS loss.
    stale_row = make_cluster_row("c1", "demo", status="active", version=5)
    event = HealthCheckFailed(at=NOW, actor="health", reason="oom")

    with pytest.raises(StaleVersion):
        await dispatcher.apply("cluster", "c1", event, record=stale_row)

    async with uow() as tx:
        row = repos.clusters.get(tx, "c1")
        assert row.version == 0
        assert row.status == "active"
        assert repos.cluster_state_audits.list_for_cluster(tx, "c1") == []
        assert repos.outbox.list_for_aggregate(tx, "cluster", "c1") == []


# ---------------------------------------------------------------------------
# Atomicity: a later-effect failure rolls back everything the same
# transaction already wrote (state, audit, outbox all-or-nothing).
# ---------------------------------------------------------------------------


async def test_mid_apply_failure_rolls_back_everything(uow, repos, dispatcher):
    async with uow() as tx:
        repos.clusters.insert(tx, make_cluster_row("c1", "demo", status="active", version=0))

    # DestroyRequested at ACTIVE -> DESTROY_SCHEDULED emits 4 effects, ordinals 0..3:
    # Persist, Notify, CancelTimer(ttl), ScheduleTimer(destroy). Pre-plant a colliding
    # effects_outbox row at exactly the effect_id ordinal 1 (Notify) would use, so the
    # SECOND effect's outbox insert hits a real UNIQUE constraint violation -- a
    # genuine, unmocked mid-transaction failure after ordinal 0 already "wrote"
    # (uncommitted) inside the same transaction.
    colliding_effect_id = "cluster/c1@1#1"
    async with uow() as tx:
        repos.outbox.insert(
            tx,
            OutboxRow(
                seq=None, effect_id=colliding_effect_id, aggregate_type="cluster", aggregate_id="decoy",
                to_version=0, ordinal=0, kind="notify", payload="{}", lane="drain", status="pending",
                attempts=0, available_at=NOW, created_at=NOW, done_at=None, last_error=None,
            ),
        )

    event = DestroyRequested(at=NOW, actor="api:test")
    with pytest.raises(IntegrityError):
        await dispatcher.apply("cluster", "c1", event)

    async with uow() as tx:
        row = repos.clusters.get(tx, "c1")
        assert row.version == 0  # the Persist that "succeeded" mid-transaction rolled back
        assert row.status == "active"
        assert repos.cluster_state_audits.list_for_cluster(tx, "c1") == []
        # only the pre-planted decoy row survives -- nothing from the failed apply() landed
        remaining = repos.outbox.list_for_aggregate(tx, "cluster", "c1")
        assert remaining == []
        decoy = repos.outbox.get(tx, colliding_effect_id)
        assert decoy is not None and decoy.aggregate_id == "decoy"


# ---------------------------------------------------------------------------
# Timers: ScheduleTimer/CancelTimer arm/disarm the `timers` table itself (not
# just the outbox row) -- and `created_by_effect` provenance traces back to
# the SAME outbox row's own `effect_id` (Conflict 1's DDL comment).
# ---------------------------------------------------------------------------


async def test_schedule_timer_arms_ttl_row_with_outbox_provenance(uow, repos, dispatcher):
    async with uow() as tx:
        repos.clusters.insert(tx, make_cluster_row("c1", "demo", status="provisioning", expires_at=LATER))

    # ProvisionSucceeded on a cluster with expires_at set emits ScheduleTimer(ttl).
    event = ProvisionSucceeded(at=NOW, actor="engine:run:r1", public_ip="1.2.3.4", kubeconfig_ref="ref")
    await dispatcher.apply("cluster", "c1", event)

    async with uow() as tx:
        timer = repos.timers.get(tx, "cluster", "c1", "ttl")
        assert timer is not None  # ScheduleTimer actually upserted a row, not just an outbox entry
        assert timer.fire_at == LATER

        outbox = repos.outbox.list_for_aggregate(tx, "cluster", "c1")
        schedule_row = next(r for r in outbox if r.kind == "schedule_timer")
        assert timer.created_by_effect == schedule_row.effect_id  # provenance == this same effect


async def test_destroy_requested_cancels_ttl_and_arms_destroy_timer(uow, repos, dispatcher):
    async with uow() as tx:
        repos.clusters.insert(tx, make_cluster_row("c1", "demo", status="active", version=0))
        # Pre-arm a "ttl" timer directly through the repo, standing in for the one a
        # prior ProvisionSucceeded would have armed -- proves CancelTimer actually
        # DELETEs the row, not merely emits an effect that never lands.
        repos.timers.upsert(
            tx,
            ScheduleTimer(
                aggregate_type="cluster", aggregate_id="c1", timer_key="ttl", fire_at=LATER,
                event=CreateRequested(at=NOW, actor="api:test"),
            ),
            "cluster/c1@0#0",
        )

    # ACTIVE x DestroyRequested -> DESTROY_SCHEDULED emits CancelTimer(ttl) + ScheduleTimer(destroy).
    event = DestroyRequested(at=NOW, actor="api:test")
    await dispatcher.apply("cluster", "c1", event)

    async with uow() as tx:
        assert repos.timers.get(tx, "cluster", "c1", "ttl") is None  # CancelTimer(ttl) deleted it

        destroy_timer = repos.timers.get(tx, "cluster", "c1", "destroy")
        assert destroy_timer is not None
        assert destroy_timer.fire_at == NOW  # no due_at on the event -> fire_at = event.at

        outbox = repos.outbox.list_for_aggregate(tx, "cluster", "c1")
        schedule_row = next(r for r in outbox if r.kind == "schedule_timer")
        assert destroy_timer.created_by_effect == schedule_row.effect_id


# ---------------------------------------------------------------------------
# Cascade: writes per-deployment audits + effects in the SAME tx
# ---------------------------------------------------------------------------


async def test_cascade_writes_per_deployment_audit_and_effects_same_tx(uow, repos, dispatcher):
    async with uow() as tx:
        repos.clusters.insert(
            tx,
            make_cluster_row("c1", "demo", status="provisioning", expires_at=LATER),
        )
        repos.deployments.insert(tx, make_deployment_row("d1", "c1", status="pending"))
        repos.deployments.insert(tx, make_deployment_row("d2", "c1", status="active"))

    event = ProvisionSucceeded(at=NOW, actor="engine:run:r1", public_ip="1.2.3.4", kubeconfig_ref="ref")
    result = await dispatcher.apply("cluster", "c1", event)

    assert result.record.state.value == "active"

    async with uow() as tx:
        cluster_row = repos.clusters.get(tx, "c1")
        assert cluster_row.status == "active"
        assert cluster_row.public_ip == "1.2.3.4"

        d1 = repos.deployments.get(tx, "d1")
        assert d1.status == "deploying"
        assert d1.version == 1
        d2 = repos.deployments.get(tx, "d2")
        assert d2.status == "active"  # untouched -- not in Cascade's where_state={PENDING}

        d1_audits = repos.deployment_state_audits.list_for_deployment(tx, "d1")
        assert len(d1_audits) == 1
        assert d1_audits[0].from_state == "pending"
        assert d1_audits[0].to_state == "deploying"

        d2_audits = repos.deployment_state_audits.list_for_deployment(tx, "d2")
        assert d2_audits == []

        cluster_outbox = repos.outbox.list_for_aggregate(tx, "cluster", "c1")
        # Persist, Notify, ScheduleTimer(ttl), Cascade
        assert {r.kind for r in cluster_outbox} == {"persist", "notify", "schedule_timer", "cascade"}

        d1_outbox = repos.outbox.list_for_aggregate(tx, "deployment", "d1")
        # Persist, Notify, RunWorkflow(deploy) -- committed in the SAME transaction as
        # the cluster's own effects (both visible together, one commit).
        assert {r.kind for r in d1_outbox} == {"persist", "notify", "run_workflow"}
        run_row = next(r for r in d1_outbox if r.kind == "run_workflow")
        assert run_row.lane == "drain"
        assert run_row.status == "pending"


# ---------------------------------------------------------------------------
# tx=: chaining onto a caller-owned transaction commits exactly once
# ---------------------------------------------------------------------------


async def test_tx_chaining_commits_once(db, uow, repos, dispatcher):
    """Mirrors tests/data/test_workflow_repos.py's
    test_repos_never_commit_visible_only_after_uow_exit pattern: a chained
    ``dispatcher.apply(..., tx=t)`` call must not commit anything on its own --
    only the OUTER ``async with uow() as t:`` block's exit commits, exactly once,
    for everything (including the nested Cascade) written inside it."""
    async with uow() as tx:
        repos.clusters.insert(tx, make_cluster_row("c1", "demo", status="active"))
        repos.deployments.insert(tx, make_deployment_row("d1", "c1", status="pending"))

    reader_db = Database(db.database_url)
    reader_uow = UnitOfWork(reader_db)
    try:
        cluster_event = InfraMissingObserved(at=NOW, actor="reconciler")
        async with uow() as tx:
            await dispatcher.apply("cluster", "c1", cluster_event, tx=tx)

            # Still inside the caller's open transaction -- a second, independent
            # connection to the same file must NOT see the change yet (no mid-block
            # commit from the chained apply() call).
            async with reader_uow() as reader_tx:
                assert repos.clusters.get(reader_tx, "c1").status == "active"

        # Now the outer `async with uow() as tx:` block has exited -> ONE commit.
        async with reader_uow() as reader_tx:
            assert repos.clusters.get(reader_tx, "c1").status == "destroyed"
            # the Cascade to d1 (ClusterGone, since c1's Casc-gone fans out to every
            # non-DESTROYED deployment state) committed in the SAME transaction.
            assert repos.deployments.get(reader_tx, "d1").status == "destroyed"
    finally:
        reader_db.dispose()


# ---------------------------------------------------------------------------
# Birth via record=: DR-0006 -- the FULL row DTO, not the bare pure record.
# INSERT instead of load, for both aggregates; row-only columns survive
# verbatim; machine-owned columns always come from the post-transition
# record, even when the caller's row disagrees.
# ---------------------------------------------------------------------------


async def test_cluster_birth_full_row_fidelity(uow, repos, dispatcher, clock):
    """slug/provider_config/node_count -- row-only columns the pure
    ClusterRecord never carries -- land in the DB exactly as the caller's row
    supplied them; machine-owned columns (status, version) come from the
    post-transition record."""
    birth_row = make_cluster_row(
        "c-new", "brand-new-slug", status="new", version=0,
        provider_config={"region": "nyc3", "size": "s-2vcpu-4gb"},
        node_count=3,
    )
    event = CreateRequested(at=NOW, actor="api:test")

    result = await dispatcher.apply("cluster", "c-new", event, record=birth_row)

    assert result.record.state.value == "provisioning"
    assert result.record.version == 1

    async with uow() as tx:
        row = repos.clusters.get(tx, "c-new")
        assert row is not None
        assert row.status == "provisioning"  # machine-owned
        assert row.version == 1  # machine-owned
        assert row.slug == "brand-new-slug"  # row-only -- survives verbatim
        assert row.provider_config == {"region": "nyc3", "size": "s-2vcpu-4gb"}  # row-only
        assert row.node_count == 3  # row-only
        assert row.created_at == clock.now()  # row-only, caller-supplied

        audits = repos.cluster_state_audits.list_for_cluster(tx, "c-new")
        assert len(audits) == 1
        assert audits[0].from_state == "new"
        assert audits[0].to_state == "provisioning"

        outbox = repos.outbox.list_for_aggregate(tx, "cluster", "c-new")
        assert {r.kind for r in outbox} == {"persist", "notify", "run_workflow"}
        assert all(r.effect_id.startswith("cluster/c-new@1#") for r in outbox)


async def test_cluster_birth_machine_overlay_wins_over_stale_row_fields(uow, repos, dispatcher):
    """DR-0006: 'machine wins on shared fields, never the reverse.' The caller's
    row carries deliberately WRONG values for machine-owned columns
    (origin/provider/public_ip); `Discovered`'s transition rule recomputes all
    three from `event.observed` -- the DB must reflect the machine's values,
    never the row's stale ones. A row-only column planted alongside them
    (`provider_config`) survives untouched, proving the overlay is scoped to
    exactly the machine-owned fields."""
    birth_row = make_cluster_row(
        "c-disc", "discovered-slug", status="new", version=0,
        origin=Origin.MANAGED,  # wrong -- Discovered always births DISCOVERED
        provider="fake",  # wrong -- overridden by observed.provider
        public_ip="1.1.1.1",  # wrong -- overridden by observed.public_ip
        provider_config={"note": "row-only, survives untouched"},
    )
    event = Discovered(
        at=NOW,
        actor="reconciler",
        observed=DiscoveredInfo(
            provider="digitalocean", public_ip="9.9.9.9", provider_resources={"droplet_id": "42"}
        ),
    )

    await dispatcher.apply("cluster", "c-disc", event, record=birth_row)

    async with uow() as tx:
        row = repos.clusters.get(tx, "c-disc")
        assert row.status == "unmanaged"
        assert row.origin == Origin.DISCOVERED  # machine wins, not the row's MANAGED
        assert row.provider == "digitalocean"  # machine wins, not "fake"
        assert row.public_ip == "9.9.9.9"  # machine wins, not "1.1.1.1"
        assert row.provider_resources == {"droplet_id": "42"}  # machine wins
        assert row.provider_config == {"note": "row-only, survives untouched"}  # row-only
        assert row.slug == "discovered-slug"  # row-only


async def test_deployment_birth_via_row_uniform(uow, repos, dispatcher):
    """Deployment births take the same DeploymentRow contract as cluster births
    (DR-0006 point 4: 'uniform for both aggregates') -- a row-only column
    (`deployed_by`) survives while the machine-owned `status`/`version`/
    `spec_ref` come from the post-transition record.

    The cluster is `provisioning`, not `active`, on purpose: this test is about the
    birth-row contract, and DR-0031's escalation would otherwise carry the deployment
    straight on to `deploying` (with a second audit row), obscuring the one thing being
    asserted here. The escalation has its own tests below."""
    async with uow() as tx:
        repos.clusters.insert(tx, make_cluster_row("c1", "demo", status="provisioning"))
        _insert_deployment_audit(tx, "audit-1", "c1")

    birth_row = make_deployment_row(
        "d-new", "c1", status="new", version=0, spec_ref=None, deployed_by="api:test-user"
    )
    event = DeployRequested(at=NOW, actor="api:test", spec_ref="audit-1")
    result = await dispatcher.apply("deployment", "d-new", event, record=birth_row)

    assert result.record.state.value == "pending"

    async with uow() as tx:
        row = repos.deployments.get(tx, "d-new")
        assert row is not None
        assert row.status == "pending"  # machine-owned
        assert row.version == 1  # machine-owned
        assert row.spec_ref == "audit-1"  # machine-owned, from DeployRequested.spec_ref
        assert row.deployed_by == "api:test-user"  # row-only -- survives verbatim

        audits = repos.deployment_state_audits.list_for_deployment(tx, "d-new")
        assert len(audits) == 1
        assert audits[0].from_state == "new"
        assert audits[0].to_state == "pending"


async def test_apply_on_nonexistent_aggregate_without_record_raises_lookup_error(uow, repos, dispatcher):
    """No row exists for "c-missing" and no ``record=`` was given, so ``_load`` --
    not the birth-row guard in ``_persist`` -- is what raises here: ``apply()`` always
    reads-before-transitioning unless ``record=`` is supplied."""
    with pytest.raises(LookupError):
        await dispatcher.apply("cluster", "c-missing", CreateRequested(at=NOW, actor="api:test"))


async def test_birth_persist_without_record_row_raises(uow, repos, dispatcher):
    """DR-0006: 'The Dispatcher never synthesizes column values' -- the birth guard
    IS reachable via ``apply()``: a pre-existing row with ``status='new'`` loads fine
    (``_load`` succeeds), ``NEW x CreateRequested`` yields a birth ``Persist``
    (``expected_version=None``), and ``_persist`` is reached with ``birth_row=None``
    because the caller never supplied ``record=`` -- a caller bug, not something to
    paper over with a synthesized row."""
    async with uow() as tx:
        repos.clusters.insert(tx, make_cluster_row("c1", "demo", status="new", version=0))

    with pytest.raises(ValueError, match="no record= row"):
        await dispatcher.apply("cluster", "c1", CreateRequested(at=NOW, actor="api:test"))
