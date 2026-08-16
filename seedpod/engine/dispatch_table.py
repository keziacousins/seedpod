"""engine/dispatch_table.py — WorkflowDispatch (docs/design/coherence-review.md
Conflict 13, AS AMENDED by DR-0022 ruling 2).

Effects carry abstract verbs (the machine stays provider-ignorant); the run-admitter
(runtime spine, NOT this pillar — Conflict 2) resolves verb x provider -> concrete
definition + inputs via this table, built in the composition root.
``workflow_runs.workflow`` stores the CONCRETE name (pinned with ``workflow_version``).

DR-0022 ruling 2: the destroy arm now returns ``{"cluster_id": eff.cluster_id}``,
matching provision — the ``DnsRecordRefResolver`` Protocol (a dispatch-time snapshot
of one field, stale on any retry or crash-resumed run) is DELETED. Both
``destroy-cloud.yml`` and ``destroy-shared.yml`` gain a ``cluster.load_infra`` head
step that reads ``{provider, slug, resource_ids, dns_record}`` FRESH at run time
instead — one mechanism instead of two for "get cluster facts into a workflow".
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping

    from seedpod.core.effects import RunWorkflow
    from seedpod.core.records import ClusterRecord

__all__ = ["WorkflowDispatch"]


@dataclass(frozen=True)
class WorkflowDispatch:
    """RunWorkflow.workflow ∈ {'provision','deploy','rollback','destroy'} — the closed verb set;
    grows only with new machine effects."""

    destroy_by_provider: Mapping[str, str]  # {'digitalocean':'destroy-cloud', 'tart':'destroy-cloud',
    #  'kind':'destroy-shared', 'orbstack':'destroy-shared'}

    def resolve(self, eff: RunWorkflow, cluster: ClusterRecord) -> tuple[str, dict]:
        match eff.workflow:
            case "provision":
                return (f"provision-{cluster.provider}", {"cluster_id": eff.cluster_id})
            case "deploy":
                return ("deploy-waves", {"deployment_id": eff.deployment_id})
            case "rollback":
                return ("deploy-rollback", {"deployment_id": eff.deployment_id})
            case "destroy":
                # DR-0040: `args` finally has a consumer. The destroy workflows declare a
                # `trigger` input so `cluster.auto_snapshot` can fire on an unattended
                # deletion only. DR-0043 adds `snapshot` -- the operator's explicit
                # `snapshot_before_destroy`, which the same step honours unconditionally.
                #
                # **This mapping is an ALLOWLIST, not a pass-through, and that is a trap
                # worth naming.** Every key a destroy workflow declares as an input has to
                # be named HERE as well; `RunWorkflow.args` carrying it is not enough. When
                # DR-0043 threaded `snapshot` through events -> machine -> args -> workflow
                # YAML -> step Params and missed this line, the workflow declared an input
                # nothing supplied and every destroy died on `KeyError('snapshot')` --
                # stranding a live droplet mid-destroy, because the crashed run task left
                # the run `running` forever. The allowlist itself is right (DR-0022 ruling
                # 2: dispatch must never smuggle resolvable state into run args); it just
                # has to be kept in step with the YAML, which
                # `tests/engine/test_dispatch_table.py` now enforces against the shipped
                # workflow files rather than against a hand-written expectation.
                args = dict(eff.args)
                return (
                    self.destroy_by_provider[cluster.provider],
                    {
                        "cluster_id": eff.cluster_id,
                        "trigger": args.get("trigger", "operator"),
                        "snapshot": args.get("snapshot", False),
                    },
                )
            case _:
                raise ValueError(f"unknown abstract workflow verb: {eff.workflow!r}")
