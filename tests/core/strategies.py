"""Hypothesis strategies for the codec's registered event/effect/record unions.

Shared by ``tests/core/test_codec_properties.py`` (and reusable by any other
property test that needs a generated ``Event``/``Effect``/record). Aware-UTC
datetimes only (Seam A §K's binding test contract; naive datetimes are banned
core-wide, see ``seedpod/core/codec.py``).

``Mapping[str, str] = ()`` / ``Mapping[str, Any]`` fields are generated as either the
empty-sentinel ``()`` or a genuinely non-empty mapping -- never an explicit ``{}`` --
because the codec's documented round-trip law only holds for those two shapes
(``seedpod/core/codec.py``'s ``_dec_typed``: "Empty decodes to ``()``, not ``{}``
... a real non-empty mapping always round-trips as a dict either way"). Generating a
bare ``{}`` would be a true codec property gap, not a strategy bug -- it is
deliberately excluded here rather than smuggled in as a false failure.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from hypothesis import strategies as st

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
    AdoptRequested,
    CancelRequested,
    ClusterGone,
    ClusterReady,
    CreateRequested,
    DeployFailed,
    DeployRejected,
    DeployRequested,
    DeploySucceeded,
    DestroyCancelled,
    DestroyDue,
    DestroyFailed,
    DestroyRequested,
    DestroySucceeded,
    Discovered,
    DiscoveredInfo,
    EndpointReady,
    HealthCheckFailed,
    InfraAllocated,
    InfraMissingObserved,
    InfraRunningObserved,
    ProvisionFailed,
    ProvisionSucceeded,
    RetryRequested,
    RollbackFinished,
    SupersededBy,
    TtlExpired,
)
from seedpod.core.records import (
    ClusterRecord,
    ClusterState,
    DeploymentRecord,
    DeploymentState,
    Origin,
)

__all__ = ["events", "cluster_events", "deployment_events", "effects", "records"]

# ---------------------------------------------------------------------------
# Primitive building blocks
# ---------------------------------------------------------------------------

_EPOCH = datetime(2020, 1, 1, tzinfo=UTC)

aware_utc_datetimes = st.integers(min_value=0, max_value=60 * 60 * 24 * 365 * 20).map(
    lambda secs: _EPOCH + timedelta(seconds=secs)
)

actors = st.sampled_from(
    ["api:alice", "api:bob", "reconciler", "health", "engine:run:r1", "timer:ttl", "timer:destroy", "cluster-machine"]
)

# printable ASCII only -- avoids surrogate/exotic-unicode edge cases unrelated to what
# this suite is testing (the codec's shape-level round-trip, not full unicode fidelity)
_printable = st.characters(min_codepoint=32, max_codepoint=126)
safe_text = st.text(alphabet=_printable, min_size=0, max_size=16)
nonempty_safe_text = st.text(alphabet=_printable, min_size=1, max_size=16)
ids = st.text(alphabet=st.characters(min_codepoint=97, max_codepoint=122), min_size=1, max_size=10)

json_scalars = st.one_of(
    st.none(),
    st.booleans(),
    safe_text,
    st.integers(min_value=-1_000_000, max_value=1_000_000),
    st.floats(allow_nan=False, allow_infinity=False, width=32),
)


def string_mappings():
    """Mapping[str, str] = () -- empty sentinel or a genuinely non-empty dict."""
    return st.one_of(
        st.just(()),
        st.dictionaries(keys=nonempty_safe_text, values=safe_text, min_size=1, max_size=3),
    )


def json_mappings():
    """Mapping[str, Any] -- same empty-sentinel-or-nonempty shape, JSON-scalar values."""
    return st.one_of(
        st.just(()),
        st.dictionaries(keys=nonempty_safe_text, values=json_scalars, min_size=1, max_size=3),
    )


cluster_states = st.sampled_from(list(ClusterState))
deployment_states = st.sampled_from(list(DeploymentState))
origins = st.sampled_from(list(Origin))

# ---------------------------------------------------------------------------
# Events -- one st.builds() per registered leaf, unioned per Seam A's ClusterEvent /
# DeploymentEvent (docs/design/coherence-review.md Conflict 8's amended set).
# ---------------------------------------------------------------------------

_discovered_info = st.builds(
    DiscoveredInfo,
    provider=nonempty_safe_text,
    public_ip=st.one_of(st.none(), safe_text),
    provider_resources=string_mappings(),
)

cluster_events = st.one_of(
    st.builds(CreateRequested, at=aware_utc_datetimes, actor=actors),
    st.builds(Discovered, at=aware_utc_datetimes, actor=actors, observed=_discovered_info),
    st.builds(RetryRequested, at=aware_utc_datetimes, actor=actors),
    st.builds(AdoptRequested, at=aware_utc_datetimes, actor=actors),
    st.builds(
        DestroyRequested,
        at=aware_utc_datetimes,
        actor=actors,
        due_at=st.one_of(st.none(), aware_utc_datetimes),
        force=st.booleans(),
    ),
    st.builds(DestroyCancelled, at=aware_utc_datetimes, actor=actors),
    st.builds(TtlExpired, at=aware_utc_datetimes, actor=actors),
    st.builds(DestroyDue, at=aware_utc_datetimes, actor=actors),
    st.builds(
        ProvisionSucceeded,
        at=aware_utc_datetimes,
        actor=actors,
        public_ip=nonempty_safe_text,
        kubeconfig_ref=nonempty_safe_text,
    ),
    st.builds(ProvisionFailed, at=aware_utc_datetimes, actor=actors, reason=safe_text),
    st.builds(DestroySucceeded, at=aware_utc_datetimes, actor=actors),
    st.builds(DestroyFailed, at=aware_utc_datetimes, actor=actors, reason=safe_text),
    st.builds(InfraRunningObserved, at=aware_utc_datetimes, actor=actors),
    st.builds(InfraMissingObserved, at=aware_utc_datetimes, actor=actors),
    st.builds(HealthCheckFailed, at=aware_utc_datetimes, actor=actors, reason=safe_text),
    st.builds(InfraAllocated, at=aware_utc_datetimes, actor=actors, resource_ids=string_mappings()),
    st.builds(EndpointReady, at=aware_utc_datetimes, actor=actors, public_ip=nonempty_safe_text),
)

deployment_events = st.one_of(
    st.builds(DeployRequested, at=aware_utc_datetimes, actor=actors, spec_ref=nonempty_safe_text),
    st.builds(DeployRejected, at=aware_utc_datetimes, actor=actors, reason=safe_text),
    st.builds(CancelRequested, at=aware_utc_datetimes, actor=actors, reason=safe_text),
    st.builds(ClusterReady, at=aware_utc_datetimes, actor=actors),
    st.builds(DeploySucceeded, at=aware_utc_datetimes, actor=actors, resolved_images=string_mappings()),
    st.builds(DeployFailed, at=aware_utc_datetimes, actor=actors, reason=safe_text),
    st.builds(SupersededBy, at=aware_utc_datetimes, actor=actors, new_deployment_id=ids),
    st.builds(ClusterGone, at=aware_utc_datetimes, actor=actors),
    st.builds(RollbackFinished, at=aware_utc_datetimes, actor=actors, ok=st.booleans()),
)

events = st.one_of(cluster_events, deployment_events)

# ---------------------------------------------------------------------------
# Records
# ---------------------------------------------------------------------------

cluster_records = st.builds(
    ClusterRecord,
    id=ids,
    name=nonempty_safe_text,
    state=cluster_states,
    version=st.integers(min_value=0, max_value=1000),
    provider=st.sampled_from(["digitalocean", "kind", "tart", "orbstack"]),
    environment=nonempty_safe_text,
    origin=origins,
    expires_at=st.one_of(st.none(), aware_utc_datetimes),
    public_ip=st.one_of(st.none(), safe_text),
    kubeconfig_ref=st.one_of(st.none(), safe_text),
    provider_resources=string_mappings(),
    pre_destroy_state=st.one_of(st.none(), cluster_states),
    failure_reason=st.one_of(st.none(), safe_text),
)

deployment_records = st.builds(
    DeploymentRecord,
    id=ids,
    cluster_id=ids,
    state=deployment_states,
    version=st.integers(min_value=0, max_value=1000),
    environment=nonempty_safe_text,
    manifest_version=nonempty_safe_text,
    spec_ref=st.one_of(st.none(), safe_text),
    resolved_images=string_mappings(),
    superseded_by=st.one_of(st.none(), ids),
    failure_reason=st.one_of(st.none(), safe_text),
)

records = st.one_of(cluster_records, deployment_records)

# ---------------------------------------------------------------------------
# Effects
# ---------------------------------------------------------------------------

_persists = st.builds(
    Persist,
    record=records,
    expected_version=st.one_of(st.none(), st.integers(min_value=0, max_value=1000)),
)

_notifies = st.builds(
    Notify,
    topic=st.sampled_from(["cluster_state_changed", "deployment_status_changed"]),
    payload=json_mappings(),
    environment=st.one_of(st.none(), safe_text),
)

_run_workflows = st.builds(
    RunWorkflow,
    workflow=st.sampled_from(["provision", "deploy", "rollback", "destroy"]),
    cluster_id=ids,
    deployment_id=st.one_of(st.none(), ids),
    args=json_mappings(),
)

_cancel_workflows = st.builds(
    CancelWorkflow,
    workflow=st.sampled_from(["provision", "deploy", "rollback", "destroy"]),
    cluster_id=ids,
    deployment_id=st.one_of(st.none(), ids),
    reason=safe_text,
)

_schedule_timers = st.builds(
    ScheduleTimer,
    aggregate_type=st.sampled_from(["cluster", "deployment"]),
    aggregate_id=ids,
    timer_key=st.sampled_from(["ttl", "destroy"]),
    fire_at=aware_utc_datetimes,
    event=events,
)

_cancel_timers = st.builds(
    CancelTimer,
    aggregate_type=st.sampled_from(["cluster", "deployment"]),
    aggregate_id=ids,
    timer_key=st.one_of(st.none(), st.sampled_from(["ttl", "destroy"])),
)

_cascades = st.builds(
    Cascade,
    cluster_id=ids,
    where_state=st.frozensets(deployment_states, min_size=1, max_size=3),
    event=deployment_events,
    except_id=st.one_of(st.none(), ids),
)

effects = st.one_of(_persists, _notifies, _run_workflows, _cancel_workflows, _schedule_timers, _cancel_timers, _cascades)
