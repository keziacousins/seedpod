"""tests/providers/test_compensation.py — ``undo_for`` total over the whole
``ProviderCommand`` union (docs/design/seam-c-provider.md §5.5's table), amended by
coherence-review.md Conflict 7 (``Observed.data`` is flattened notes, no wrapper key)
and Conflict 12 (workflow-declared-compensator phrasing void; tested for absence here,
not for the workflow machinery itself). C-23 shape: total (never raises for an in-union
command) and idempotent-by-construction (a pure function returns an equal value for
equal input; the *executed* idempotence half of C-23 is the conformance suite's job,
against a real transport).

Pure function under a pure function — no ``Mock``/``patch`` needed or used.
"""

from __future__ import annotations

from typing import get_args

import pytest

from seedpod.core.cluster_spec import ClusterConfiguration, ClusterSpecification, NodeSpecification
from seedpod.providers.compensation import undo_for
from seedpod.providers.contract import (
    ApplyFirewalls,
    AssignToProject,
    CaptureHostKeys,
    CreateInstance,
    DestroyInstance,
    FetchKubeconfig,
    HostKeys,
    IngressConfig,
    InstallK3s,
    InstanceCreated,
    K3sInstalled,
    KubeApplyManifest,
    KubeDeleteManifest,
    KubeGetClusterInfo,
    KubeGetDeployments,
    KubeGetEvents,
    KubeGetNodes,
    KubeGetPodDetails,
    KubeGetPodLogs,
    KubeGetPods,
    KubeProbeRollout,
    KubeRestartDeployment,
    KubeRolloutUndo,
    KubeRun,
    KubeWatchPods,
    ListInstances,
    Observed,
    ProbeDestruction,
    ProbeInstance,
    ProbeK3s,
    ProbeSshPort,
    ProviderCommand,
    Reconcile,
    SSHTarget,
)

# ============================================================================
# Totality scaffolding: every leaf type in the ProviderCommand union, and a
# hand-built minimal-valid instance of each, kept 1:1 so a union that grows
# without a matching entry here fails loudly rather than being silently skipped.
# ============================================================================


def _flatten_union(tp) -> frozenset[type]:
    args = get_args(tp)
    if not args:
        return frozenset({tp})
    out: set[type] = set()
    for a in args:
        out |= _flatten_union(a)
    return frozenset(out)


ALL_COMMAND_TYPES = _flatten_union(ProviderCommand)

_SPEC = ClusterSpecification(
    node_specification=NodeSpecification(cpu_cores=2, memory_gb=4, disk_gb=50, region_hint="us-east"),
    cluster_config=ClusterConfiguration(),
)
_SSH = SSHTarget(host="10.0.0.5", user="root", private_key_path="/tmp/key")
_INGRESS = IngressConfig(ingress_type="traefik")

COMMAND_INSTANCES: dict[type, ProviderCommand] = {
    CreateInstance: CreateInstance(
        cluster_uuid="c1",
        slug="s1",
        spec=_SPEC,
        pod_cidr="10.42.0.0/24",
        service_cidr="10.43.0.0/24",
        tags=("cluster-uuid:c1",),
    ),
    ProbeInstance: ProbeInstance(resource_ids={"droplet_id": "123"}),
    DestroyInstance: DestroyInstance(slug="s1", resource_ids={"droplet_id": "123"}),
    ProbeDestruction: ProbeDestruction(resource_ids={"droplet_id": "123"}),
    ListInstances: ListInstances(),
    Reconcile: Reconcile(clusters=()),
    ApplyFirewalls: ApplyFirewalls(resource_ids={"droplet_id": "123"}, spec=_SPEC),
    AssignToProject: AssignToProject(resource_ids={"droplet_id": "123"}),
    ProbeSshPort: ProbeSshPort(host="10.0.0.5"),
    CaptureHostKeys: CaptureHostKeys(ssh=_SSH),
    InstallK3s: InstallK3s(
        ssh=_SSH,
        known_hosts="10.0.0.5 ssh-ed25519 AAAA...",
        pod_cidr="10.42.0.0/24",
        service_cidr="10.43.0.0/24",
        tls_sans=(),
        ingress=_INGRESS,
    ),
    ProbeK3s: ProbeK3s(ssh=_SSH, known_hosts="10.0.0.5 ssh-ed25519 AAAA..."),
    FetchKubeconfig: FetchKubeconfig(rewrite_server_to="https://cluster.example:6443"),
    KubeGetClusterInfo: KubeGetClusterInfo(kubeconfig="apiVersion: v1"),
    KubeGetNodes: KubeGetNodes(kubeconfig="apiVersion: v1"),
    KubeGetPods: KubeGetPods(kubeconfig="apiVersion: v1"),
    KubeGetPodDetails: KubeGetPodDetails(kubeconfig="apiVersion: v1", pod_name="p"),
    KubeGetPodLogs: KubeGetPodLogs(kubeconfig="apiVersion: v1", pod_name="p"),
    KubeApplyManifest: KubeApplyManifest(kubeconfig="apiVersion: v1", manifest_yaml="kind: ConfigMap"),
    KubeDeleteManifest: KubeDeleteManifest(kubeconfig="apiVersion: v1", manifest_yaml="kind: ConfigMap"),
    KubeGetDeployments: KubeGetDeployments(kubeconfig="apiVersion: v1"),
    KubeRestartDeployment: KubeRestartDeployment(kubeconfig="apiVersion: v1", deployment="d"),
    KubeProbeRollout: KubeProbeRollout(kubeconfig="apiVersion: v1", deployment="d"),
    KubeGetEvents: KubeGetEvents(kubeconfig="apiVersion: v1"),
    KubeRolloutUndo: KubeRolloutUndo(kubeconfig="apiVersion: v1"),
    KubeRun: KubeRun(kubeconfig="apiVersion: v1", args=("get", "pods")),
    KubeWatchPods: KubeWatchPods(kubeconfig="apiVersion: v1"),
}

EMPTY_OBSERVED = Observed(data={}, value=None)


def test_command_instance_table_matches_the_full_union():
    """Guards the totality test below against silent drift: if contract.py's
    ProviderCommand union grows, this fails until COMMAND_INSTANCES grows to match."""
    assert set(COMMAND_INSTANCES) == set(ALL_COMMAND_TYPES)


@pytest.mark.parametrize("command_type", sorted(COMMAND_INSTANCES, key=lambda t: t.__name__))
def test_undo_for_total_never_raises_and_result_is_none_or_in_union(command_type):
    cmd = COMMAND_INSTANCES[command_type]
    result = undo_for(cmd, EMPTY_OBSERVED)
    assert result is None or isinstance(result, tuple(ALL_COMMAND_TYPES))


@pytest.mark.parametrize("command_type", sorted(COMMAND_INSTANCES, key=lambda t: t.__name__))
def test_undo_for_is_deterministic(command_type):
    """Pure function: same (command, observed) -> equal result every time (C-23's
    'total' half doesn't just mean 'doesn't crash' — repeatable undo mapping is what
    makes an undo scope replay-safe across a crash-and-resume)."""
    cmd = COMMAND_INSTANCES[command_type]
    assert undo_for(cmd, EMPTY_OBSERVED) == undo_for(cmd, EMPTY_OBSERVED)


# ============================================================================
# The two commands with a real inverse (§5.5's table)
# ============================================================================


def test_create_instance_undo_from_terminal_result():
    cmd = COMMAND_INSTANCES[CreateInstance]
    result_value = InstanceCreated(
        resource_ids={"droplet_id": "123", "vpc_id": "456"},
        address="1.2.3.4",
        effective_pod_cidr="10.42.0.0/24",
        effective_service_cidr="10.43.0.0/24",
    )
    observed = Observed(data={}, value=result_value)
    inverse = undo_for(cmd, observed)
    assert inverse == DestroyInstance(slug=cmd.slug, resource_ids={"droplet_id": "123", "vpc_id": "456"})


def test_create_instance_undo_from_truncated_stream_notes_only():
    """The C1 close: a CreateInstance whose stream died right after RESOURCE_ALLOCATED
    still yields a real DestroyInstance, reading ids straight off Observed.data — per
    Conflict 7's amendment, ProviderStep.execute flattens resource_ids directly into
    ctx.note()'s kwargs, so Observed.data itself (not a nested "resource_ids" key) IS
    the id mapping when there is no terminal Result."""
    cmd = COMMAND_INSTANCES[CreateInstance]
    observed = Observed(data={"droplet_id": "123", "vpc_id": "456"}, value=None)
    inverse = undo_for(cmd, observed)
    assert inverse == DestroyInstance(slug=cmd.slug, resource_ids={"droplet_id": "123", "vpc_id": "456"})


def test_create_instance_undo_none_when_nothing_ever_allocated():
    """Stream died before RESOURCE_ALLOCATED ever fired: no notes, no result ⇒ no
    undo command — nothing was allocated, or tag-before-boot + reconciliation is the
    backstop (never a bare DestroyInstance({}))."""
    cmd = COMMAND_INSTANCES[CreateInstance]
    assert undo_for(cmd, EMPTY_OBSERVED) is None


def test_kube_apply_manifest_undo_is_literal_delete_inverse():
    cmd = COMMAND_INSTANCES[KubeApplyManifest]
    inverse = undo_for(cmd, EMPTY_OBSERVED)
    assert inverse == KubeDeleteManifest(
        kubeconfig=cmd.kubeconfig, manifest_yaml=cmd.manifest_yaml, ignore_not_found=True
    )


# ============================================================================
# Everything else has no inverse (§5.5's table: reads / already-compensating /
# escape hatch / the deploy-rollback machine decision Conflict 12 moves out of
# this module entirely)
# ============================================================================

NO_INVERSE_TYPES = [t for t in COMMAND_INSTANCES if t not in (CreateInstance, KubeApplyManifest)]


@pytest.mark.parametrize("command_type", sorted(NO_INVERSE_TYPES, key=lambda t: t.__name__))
def test_no_inverse_for_reads_and_already_compensating_commands(command_type):
    cmd = COMMAND_INSTANCES[command_type]
    assert undo_for(cmd, EMPTY_OBSERVED) is None


def test_kube_rollout_undo_has_no_command_level_inverse():
    """Conflict 12: deploy-cancel rollback (>=1-success KubeRolloutUndo semantics) is a
    machine-decided workflow (deploy-rollback.yml), never this module's inverse of
    KubeApplyManifest — asserted structurally: KubeRolloutUndo itself has no inverse,
    and KubeApplyManifest's inverse is always the literal KubeDeleteManifest, never a
    KubeRolloutUndo, regardless of what's Observed."""
    cmd = COMMAND_INSTANCES[KubeRolloutUndo]
    assert undo_for(cmd, EMPTY_OBSERVED) is None
    apply_cmd = COMMAND_INSTANCES[KubeApplyManifest]
    inverse = undo_for(apply_cmd, EMPTY_OBSERVED)
    assert isinstance(inverse, KubeDeleteManifest)


def test_destroy_instance_and_kube_delete_manifest_never_auto_undone():
    """Destruction IS compensation; it is never itself auto-undone (§5.5's table)."""
    assert undo_for(COMMAND_INSTANCES[DestroyInstance], EMPTY_OBSERVED) is None
    assert undo_for(COMMAND_INSTANCES[KubeDeleteManifest], EMPTY_OBSERVED) is None


def test_install_k3s_and_capture_host_keys_and_fetch_kubeconfig_subsumed_by_instance_undo():
    for command_type in (InstallK3s, CaptureHostKeys, FetchKubeconfig):
        cmd = COMMAND_INSTANCES[command_type]
        assert undo_for(cmd, EMPTY_OBSERVED) is None


def test_undo_of_k3s_installed_and_host_keys_results_still_yields_no_inverse():
    """Sanity: even with a populated terminal Result, these commands still have no
    inverse (they are not CreateInstance/KubeApplyManifest, so undo_for's catch-all
    applies regardless of what Observed carries)."""
    install_cmd = COMMAND_INSTANCES[InstallK3s]
    observed = Observed(data={}, value=K3sInstalled())
    assert undo_for(install_cmd, observed) is None

    capture_cmd = COMMAND_INSTANCES[CaptureHostKeys]
    observed = Observed(data={}, value=HostKeys(known_hosts="10.0.0.5 ssh-ed25519 AAAA..."))
    assert undo_for(capture_cmd, observed) is None
