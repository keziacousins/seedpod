"""tests/engine/declared_verbs.py — the declared-verbs fixture registry for the
shipped workflow definitions under config/workflows/.

LOUD: this fixture IS the interface contract every real Pillar-3/domain-step
implementation must satisfy. Rekeyed to the DR-0022 vocabulary
(docs/decisions/DR-0022-step-verb-vocabulary.md), which supersedes DR-0004's
per-provider verb families. It is transcribed, verb by verb, from:

  - docs/decisions/DR-0022-step-verb-vocabulary.md's 30-verb table (the
    authoritative old->new name map and the merges it prescribes -- the DR's
    own prose "31" was an arithmetic slip corrected by its Erratum E1; the
    table itself, and this fixture, both have exactly 30 entries),
  - docs/design/seam-b-engine.md §2.2 Proofs 1-3 (the `# Output: ...` / inline
    comments on each step, now amended in place per DR-0022 ruling 8),
  - docs/design/coherence-review.md Conflicts 8 (resolved_images), 9 (kubeconfig
    tail), 10 (provision head), 12 (deploy-rollback, verbatim), 14 (known_hosts),
  - docs/design/seam-c-provider.md §5.3's `CreateInstance`/`ProbeInstance`/
    `FetchKubeconfig`/`DestroyInstance`/`KubeApplyManifest`/`KubeProbeRollout`
    dataclasses, narrowed to what the ProviderStep leaves workflow-visible.

Nothing here is invented capability -- every verb name is spec-given. Where the
*shape* of a Params/Output model is not fully spelled out by prose, this module
supplies the smallest reasonable pydantic model that makes the comment true,
flagged below with TODO(Pillar-3)/TODO(spine) markers matching the convention
already used in engine/provider_step.py and engine/step.py. `kube.wipe_namespace`'s
Params are this task's own minimal inference (Seam B names the verb and its
position in destroy-shared.yml but not its shape); the `AddressOutput`/
`InstanceCreatedOutput`/`FetchKubeconfigByResourceIdsParams`/`ApplyManifestParams`/
`ProbeRolloutParams`/`LoadInfraOutput` shapes below are this task's own honest
inference from the Seam C dataclasses (DR-0022 names the verbs and their command
mapping but not a full YAML field list) -- everything else's *field set* is
copied from a comment.

**Round 10 "dtos" component: the five DR-0028 stand-ins below are now real
imports, not local declarations.** `ManifestDoc`/`DeploymentProfile`/
`SnapshotRestoreSpec`/`Wave`/`ApplyChangeSummary` used to be declared LOCALLY in
this module, each carrying a TODO admitting the shape was inferred, never
audited, against v1 or any real consumer. `docs/decisions/DR-0028-deploy-path-dtos.md`
did that audit before Round 10's build and found four of five WRONG;
`seedpod/core/deploy_wave.py` now carries the ratified shapes, imported here
rather than re-declared -- "so the fixture and production registries cannot
drift on type identity" (this docstring's own long-standing claim, upheld here
for these five names for the first time). Where the real shape differs from what
this fixture used to declare (DR-0028's own audit table):

  - `DeploymentProfile.data_initialization: bool` is GONE, not narrowed --
    DR-0028 decision 2: that fact is not a profile field at all (a per-deployment
    choice, sourced from the deploy request, not profile YAML; no shipped profile
    ever declared it). The real `DeploymentProfile` carries
    `persistence_services: list[str]` instead -- see its own docstring for why
    that genuinely IS a profile fact where `data_initialization` was not. It ALSO
    carries `deploy_wave: Mapping[str, int]` (DR-0029, ratified after DR-0028 and
    superseding DR-0028 decision 5/Erratum E1's wave-model framing only --
    decisions 1-4 stand): the per-service wave-rank mapping (default 3) v2
    *builds*, realising a v1 plan (`PLAN-wave-orchestration.md`) v1 itself never
    shipped. `persistence_services`-alone structurally could not answer "which
    wave does this document belong to", which is why the field joined rather than
    the stand-in's original two-field guess being left as-is.
  - `SnapshotRestoreSpec.snapshot_id: str` is GONE -- v1 has two restore modes,
    not one bare id; the real type carries `restore_from_snapshot`/
    `restore_from_latest`/`services`, matching the ALREADY-COMMITTED
    `seedpod/api/routers/presets.py` `DataInitialization`/`RestoreFromLatest`
    (Round 6) field for field (a separate class -- `core/` cannot import `api/` --
    but the same shape, deliberately).
  - `ApplyChangeSummary` keeps its three-list shape exactly (DR-0028 never
    faulted the shape), and gains a tested `all_unchanged` property carrying the
    restart semantic DR-0028's audit found "undocumented anywhere".
  - `ManifestDoc`/`Wave` keep their exact declared field sets -- DR-0028 did not
    fault either's shape, only that neither was real; `ManifestDoc` gains
    `seedpod.core.deploy_wave.serialize_manifest_documents`/
    `parse_manifest_documents` as its (de)serialization boundary to/from
    `KubeApplyManifest.manifest_yaml: str`.

  Because `data_initialization` no longer nests inside `profile`, and Seam B's
  own Proof 1 comment ("restore attached to the persistence wave only when the
  profile declares data_initialization") predates DR-0028's finding that it
  isn't a profile field, `DeployLoadAuditOutput` and `PlanWavesParams` below each
  gain a NEW top-level `data_initialization: SnapshotRestoreSpec | None = None`
  field -- structurally required for `Wave.restore` to be reachable from
  anywhere at all now that `data_initialization` has moved off `profile`
  (DR-0028 decision 2's own words: "`deploy.load_audit` reads it back off the
  audit like every other resolved fact" -- top-level, exactly like the
  pre-existing `rollout_timeout_seconds`/`resolved_images`, never nested). Both
  fields are Optional (DR-0022 P4's conditional-as-data pattern, matching
  `Wave.restore`'s own optionality), so nothing in V2 (missing-required-keys)
  would ever have FAILED if the `plan` step's `with:` block never bound the new
  field -- `Wave.restore` would simply have stayed permanently unreachable, a
  silent-skip of the only route a preset deploy's requested restore could ever
  take. A fix-pass adversarial-judge finding on this exact rekeying caught that
  the binding was zero-risk to add NOW (pure workflow data-flow, no verb code,
  validates against this very fixture today): `config/workflows/deploy-waves.yml`'s
  `plan` step now binds `data_initialization: {from: audit.data_initialization}`,
  pinned by `test_shipped_workflows.py`'s own
  `test_deploy_waves_plan_step_binds_data_initialization_per_dr_0028_decision_2`
  so the wiring cannot be silently dropped again. The `deploy_direct`/
  `PresetService.deploy`/`_build_resolved_config` APPLICATION plumbing that
  actually POPULATES `resolved_config["data_initialization"]` from the deploy
  request remains Round 10 verb-building work this component does not do
  (`seedpod/app` is outside this component's authorized scope) -- wiring the
  YAML's data-flow does not, by itself, make a restore happen end to end.

**`DnsRecordRef` was ALSO still a stale local stand-in here, found while
rekeying, and fixed for the same reason.** Round 8b (commit `952b317`) made
`DnsRecordRef` real (`seedpod/core/dns_record.py`: `record_id`/`zone`/
`hostname`, keyed the way `DnsService.delete_record` actually takes it), and
every real Step already imports THAT type (`engine/steps/{dns,cluster}.py`) --
but this fixture file was never updated to match: it kept a LOCAL `{zone, name}`
class, missing `record_id` entirely, exactly the defect DR-0028's own body cites
as its motivating precedent ("DnsRecordRef's stand-in was {zone, name}... a verb
built to the declared shape could not have called its own service").
`test_registry_params_and_output_field_sets_match_declared_verbs_fixture`
(`tests/engine/test_verb_conventions.py`) only compares OUTER Params/Output
field NAMES, never a nested type's own fields, which is why this went uncaught
this long; nothing constructs a `DnsRecordRef` via this fixture's old stand-in
shape anywhere (grep-verified across `tests/`), so fixing it here -- while every
other stand-in in this same module is being reconciled for the identical
reason -- is zero-risk, not a scope expansion: DR-0028's own text names this
exact defect as the reason this whole rekeying discipline exists.

DR-0022's merges applied here (see the DR's table + rulings):
  - `do.create_droplet`/`kind.create_cluster`/`tart.create_vm`/
    `orbstack.adopt_cluster` -> ONE `infra.create_instance` (late-bound: its
    Params gains `provider: str`, the ruling-1 dict-lookup key). Its Output
    also merges to one shape -- `resource_ids`, `address: str | None` (None
    until a gate enriches it; already set for orbstack, which has none), and
    `adopted_existing: bool` (ruling 1: "honest for every provider, not just
    orbstack").
  - `do.await_droplet`/`kind.await_ready`/`tart.await_vm` -> ONE
    `infra.await_instance` (also late-bound), Output field `address: str`
    (P6: the glossary noun, never `ip`).
  - `kind.fetch_kubeconfig`/`orbstack.fetch_kubeconfig` -> ONE
    `infra.fetch_kubeconfig` (late-bound); the ssh variant is the DISTINCT
    `k3s.fetch_kubeconfig` (fixed `ssh-k3s` provider_name, no late binding).
  - `provider.destroy_server` -> `infra.destroy_instance`, gaining typed Params
    (P8: no `EmptyParams` provider verb) -- `provider`/`slug`/`resource_ids`,
    fed by the new `cluster.load_infra` head (ruling 2).
  - `kubectl.apply` -> `kube.apply_docs`, **`undoable=False`** (D1's fix: a
    failed deploy must never auto-delete the application's own manifests).
    `kubectl.apply_manifest` -> `kube.apply_file`, `undoable=True` (the infra
    shim verb, where the literal KubeDeleteManifest inverse is correct).
  - `kubectl.delete_daemonset` -> `kube.delete_daemonset`: **now gateable**;
    `wait`/`wait_timeout_seconds`/`settle_seconds` LEAVE Params (D2's fix -- no
    command waits, all waiting is an engine gate) for the workflow's own
    `gate:` block (see destroy-cloud.yml/destroy-shared.yml).

Pillar 3's real registry (`engine/registry.py` + concrete `ProviderStep`/domain-
step subclasses) must reconcile against this file: same verb names, same Params/
Output field sets. `ClusterSpecification`, `DnsRecordRef`, and (as of this
rekeying) `ManifestDoc`/`DeploymentProfile`/`SnapshotRestoreSpec`/`Wave`/
`ApplyChangeSummary` are all real, already-built `seedpod/core/` types, imported
below, never stood in for.

Zero Mock/patch (CLAUDE.md testing posture): this is a typed, static fixture --
no test constructs it dynamically or patches it.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field

from pydantic import BaseModel, SecretStr

from seedpod.core.acme import AcmeConfig
from seedpod.core.cluster_spec import ClusterSpecification
from seedpod.core.deploy_wave import (
    ApplyChangeSummary,
    DeploymentProfile,
    ManifestDoc,
    SnapshotRestoreSpec,
    Wave,
)
from seedpod.core.dns_record import DnsIntent, DnsRecordRef
from seedpod.engine.config import VerbSpec
from seedpod.engine.registry import resolve_type_expr

__all__ = [
    "EmptyParams",
    "EmptyOutput",
    "ManifestDoc",
    "DeploymentProfile",
    "SnapshotRestoreSpec",
    "Wave",
    "ApplyChangeSummary",
    "DnsRecordRef",
    "VerbFixture",
    "DECLARED_VERBS",
    "NAMED_TYPES",
    "ShippedWorkflowRegistry",
]


# ---------------------------------------------------------------------------
# EmptyParams/EmptyOutput stay fixture-local (so this module has no import-time
# dependency on production code beyond config.py's protocols); the other six
# names above -- ManifestDoc/DeploymentProfile/SnapshotRestoreSpec/Wave/
# ApplyChangeSummary (Round 10, this rekeying) and DnsRecordRef (Round 8b) --
# are no longer declared here at all. See the module docstring's "Round 10 dtos
# component" and "DnsRecordRef" sections for what changed and why.
# ---------------------------------------------------------------------------


class EmptyParams(BaseModel):
    """The canonical Params for verbs whose YAML `with:` block is empty/absent
    (e.g. `cluster.load_kubeconfig`) -- the step reads `ctx.cluster_id`/
    `ctx.run_id` implicitly rather than via a binding."""


class EmptyOutput(BaseModel):
    """Mirrors engine/step.py's EmptyOutput; declared locally so this module has
    no import-time dependency on production code beyond config.py's protocols."""


# ---------------------------------------------------------------------------
# VerbFixture -- structurally satisfies engine/config.py's VerbSpec Protocol.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class VerbFixture:
    Params: type[BaseModel]
    Output: type[BaseModel]
    gateable: bool = False
    undoable: bool = False
    # DR-0022 P2 -- "Step gains plane ... and thin: bool ..., enforced by a
    # registry test." Populated per-verb from the DR-0022 table below (see
    # DECLARED_VERBS' per-row comments for the table's own wording that each
    # value is transcribed from), so the Erratum-E4c reconciliation test can
    # check plane/thin the same way it already checks gateable/undoable --
    # otherwise a Round-8b verb declaring the wrong plane/thin passes silently
    # (the registry-only checks in test_verb_conventions.py can only assert
    # internal consistency, e.g. thin => ProviderStep, never the EXPECTED
    # value per verb).
    plane: str = "domain"
    thin: bool = False
    # `idempotent` (Round-8a "infra-and-do" review finding): Step's own default
    # is True (engine/step.py) and governs crash-mid-step resume
    # (engine/engine.py: idempotent => re-enter execute() up to
    # resume_replay_limit; not idempotent => mark_failed("interrupted;
    # non-idempotent") -> compensate). seam-b-engine.md:524 pins "exactly one
    # verb is non-idempotent (do.create_droplet)" -- DR-0022 renamed it
    # `infra.create_instance`, so this fixture (and the reconciliation test
    # below) is the one place that flip stays load-bearing rather than a
    # silent `ProviderStep` default drift.
    idempotent: bool = True


# ---------------------------------------------------------------------------
# Per-verb Params models (only where the verb needs one; EmptyParams otherwise).
# ---------------------------------------------------------------------------


class DeployLoadAuditParams(BaseModel):
    deployment_id: str


class DeployLoadAuditOutput(BaseModel):
    # Conflict 8: "deploy.load_audit's Output gains resolved_images: Mapping[str, str]"
    manifests: list[ManifestDoc]
    profile: DeploymentProfile
    rollout_timeout_seconds: int
    resolved_images: Mapping[str, str] = {}
    # DR-0028 decision 2 (Round 10 "dtos" rekeying, this fixture): data_initialization
    # is not a DeploymentProfile field (see that type's own docstring), so this Output
    # gains its own top-level fact, read off the audit's resolved_config "like every
    # other resolved fact" -- exactly parallel to resolved_images/rollout_timeout_seconds
    # above. None (the default) is the common case: no restore-from-snapshot requested.
    data_initialization: SnapshotRestoreSpec | None = None


class KubeconfigOutput(BaseModel):
    kubeconfig: SecretStr


class OptionalKubeconfigOutput(BaseModel):
    kubeconfig: SecretStr | None = None


class KubeconfigParams(BaseModel):
    kubeconfig: SecretStr


class PlanWavesParams(BaseModel):
    manifests: list[ManifestDoc]
    profile: DeploymentProfile
    rollout_timeout_seconds: int
    # DR-0028 decision 2 (Round 10 "dtos" rekeying, this fixture): mirrors
    # DeployLoadAuditOutput's own new field, now bound in config/workflows/
    # deploy-waves.yml's plan step ({from: audit.data_initialization}; see the
    # module docstring's fix-pass note). This is the ONLY channel through
    # which `deploy.plan_waves` can ever attach a resolved SnapshotRestoreSpec
    # to some Wave.restore in its output, now that data_initialization no
    # longer rides inside `profile` -- see Wave's own docstring
    # (seedpod/core/deploy_wave.py) for why that must not be the SAME Wave as
    # the one carrying the persistence-service docs.
    data_initialization: SnapshotRestoreSpec | None = None


class PlanWavesOutput(BaseModel):
    waves: list[Wave]


class DeleteJobsParams(BaseModel):
    kubeconfig: SecretStr
    jobs: list[str]


class ApplyParams(BaseModel):
    kubeconfig: SecretStr
    docs: list[ManifestDoc]


class ApplyOutput(BaseModel):
    changes: ApplyChangeSummary


class RestoreSnapshotParams(BaseModel):
    kubeconfig: SecretStr
    spec: SnapshotRestoreSpec | None = None


class RolloutRestartParams(BaseModel):
    kubeconfig: SecretStr
    deployments: list[str]
    changes: ApplyChangeSummary


class WaveReadyParams(BaseModel):
    kubeconfig: SecretStr
    deployments: list[str]
    jobs: list[str]


class LoadSpecParams(BaseModel):
    cluster_id: str


class LoadSpecOutput(BaseModel):
    # DR-0022 table: "output gains provider: str" (Conflict-10's head reused by
    # infra.create_instance's late-binding Params, ruling 1). `slug` added by
    # this round's own "infra-and-do" component (see CreateInstanceParams'
    # docstring immediately below for why) -- mirrors `cluster.load_infra`'s
    # `LoadInfraOutput`, which already carries `slug` for the destroy side.
    # `ssh_user`/`ssh_private_key_path` added by DR-0023: every k3s-plane Seam C
    # command but ProbeSshPort needs an SSHTarget, whose user/private_key_path
    # are required with no defaults; both are `None` for kind/orbstack (no SSH
    # plane). Per DR-0023 Erratum E1, that optionality does NOT type-enforce the
    # plane matrix (this is one global `str | None`, and kind/orbstack workflows
    # carry no k3s.* step anyway) -- `k3s.py`'s `_target()` raises on a None.
    spec: ClusterSpecification
    provider: str
    slug: str
    # `dns_intent` added by DR-0034 decision 3: what the profile asked for in DNS
    # terms, read off `provider_config["dns_config"]`, `None` for every profile
    # that did not enable DNS. Mirrors `LoadInfraOutput.dns_record` on the destroy
    # side -- one load head per direction, each carrying that direction's DNS fact.
    ssh_user: str | None
    ssh_private_key_path: str | None
    dns_intent: DnsIntent | None = None
    # `acme` added by DR-0036: the Let's Encrypt certresolver, gated on ssl.enabled AND
    # dns.enabled (v1's `use_acme_certs` -- the same condition under which the Ingress
    # templates render the annotation naming it). Bound into `k3s.install`.
    acme: AcmeConfig | None = None


class CreateInstanceParams(BaseModel):
    """Seam C `CreateInstance`'s Params, as exposed to workflow YAML -- `spec`
    plus `provider` (DR-0022 ruling 1's late-binding key) plus `cluster_id`/
    `slug` (Round-8a "infra-and-do" component finding, P8-shaped: `tags`/
    `pod_cidr`/`service_cidr`/`tls_sans`/`api_host`/`api_port` genuinely ARE
    derived by the ProviderStep from `spec` alone with no further binding
    needed, but `cluster_uuid` (conformance C-07's idempotency key) and `slug`
    (the DO/kind/tart naming convention, e.g. DO's `k3s-{slug}` droplet name
    and its `cluster-{slug}` legacy tag fallback) are per-cluster identity
    `command(self, params)` cannot conjure from `spec` alone, and `command()`
    is pure -- no `ctx` (`engine/provider_step.py`/`engine/steps/late_bound.py`:
    "MUST stay pure... no ctx"). P8 itself is the mechanism: `cluster_id` binds
    straight from the already-available `run.cluster_id` workflow input; `slug`
    is produced by extending `cluster.load_spec`'s own Output (mirroring
    `cluster.load_infra`'s `LoadInfraOutput.slug` on the destroy side) and
    bound from there. Replaces DR-0004's four identical-shaped per-provider
    Params (do.create_droplet/kind.create_cluster/tart.create_vm/
    orbstack.adopt_cluster) -- the shape never changed just because the
    provider name did; DR-0022 also collapses the name."""

    provider: str
    spec: ClusterSpecification
    cluster_id: str
    slug: str


class InstanceCreatedOutput(BaseModel):
    """Seam C `InstanceCreated`, narrowed to what `infra.create_instance`
    surfaces to YAML. `resource_ids` was `droplet_id: str` pre-Conflict-10.
    `address` is `None` until a separate `infra.await_instance` gate enriches
    it (DO/kind/tart); already set at create time for orbstack, which has no
    separate gate. `adopted_existing` is DR-0022 ruling 1's honesty move --
    "already `InstanceCreated`'s field... honest for every provider, not just
    orbstack"."""

    resource_ids: Mapping[str, str]
    address: str | None = None
    adopted_existing: bool = False


class AwaitInstanceParams(BaseModel):
    provider: str
    resource_ids: Mapping[str, str]


class AddressOutput(BaseModel):
    """`infra.await_instance`'s Output -- Seam C `InstanceState.address`, once
    ready. DR-0022 P6 (glossary nouns): one name, `address`, replacing the
    pre-DR-0022 `ip`/`address` split (do.await_droplet/tart.await_vm used `ip`;
    kind.await_ready used `address` -- now merged, 3->1, on one field name)."""

    address: str


class HostParams(BaseModel):
    """`k3s.await_ssh` only -- `ProbeSshPort` needs just `host`/`port`, no
    `SSHTarget` (DR-0023), so this verb never gains ssh_user/ssh_private_key_path."""

    host: str


class TrustHostKeysParams(BaseModel):
    """DR-0023: `CaptureHostKeys` needs a full `SSHTarget`, so -- unlike
    `k3s.await_ssh` -- this verb can no longer share `HostParams`. ssh_user/
    ssh_private_key_path are `str | None` (not required `str`) because
    `cluster.load_spec`'s own Output is `str | None` -- V4's Optional-binds-
    Optional rule means a required-`str` Params field here would make
    provision-digitalocean.yml/provision-tart.yml's own legitimate bindings
    fail to validate; the real Step's `command()` (`engine/steps/k3s.py`)
    raises loudly if either arrives `None` instead."""

    host: str
    ssh_user: str | None
    ssh_private_key_path: str | None


class KnownHostsOutput(BaseModel):
    known_hosts: str


class InstallK3sParams(BaseModel):
    host: str
    spec: ClusterSpecification
    extra_tls_san: str
    known_hosts: str
    ssh_user: str | None
    ssh_private_key_path: str | None
    # DR-0036: bound from `cluster.load_spec`'s `acme`. The certresolver goes into the
    # SAME HelmChartConfig the hostport strategy already writes BEFORE k3s starts --
    # v1 wrote a second, competing one at DEPLOYING instead, and v2 keeps one writer.
    acme: AcmeConfig | None = None


class K3sAwaitReadyParams(BaseModel):
    host: str
    known_hosts: str
    ssh_user: str | None
    ssh_private_key_path: str | None


class FetchKubeconfigParams(BaseModel):
    """`k3s.fetch_kubeconfig` (the ssh variant) -- fixed `ssh-k3s` provider_name,
    never late-bound (only the `infra.*` family late-binds)."""

    host: str
    rewrite_server_to: str
    known_hosts: str
    ssh_user: str | None
    ssh_private_key_path: str | None


class KubeconfigStoreParams(BaseModel):
    cluster_id: str
    kubeconfig: SecretStr


class KubeconfigRefOutput(BaseModel):
    kubeconfig_ref: str


class ApplyFirewallsParams(BaseModel):
    resource_ids: Mapping[str, str]
    spec: ClusterSpecification


class AssignToProjectParams(BaseModel):
    resource_ids: Mapping[str, str]


class DeleteDaemonsetParams(BaseModel):
    """DR-0022 ruling 4 (D2's fix): `wait`/`wait_timeout_seconds`/`settle_seconds`
    are DELETED from Params -- `kube.delete_daemonset` is now `gateable`, and the
    workflow YAML carries the same values as a `gate: {timeout_seconds,
    interval_seconds}` block instead (no command waits; all waiting is an
    engine gate). The v1 edge behaviour they protect (gotcha 10 -- the 48-hour
    lingering Tailscale node) is preserved as gate data, not deleted."""

    kubeconfig: SecretStr | None = None
    name: str
    namespace: str
    grace_period_seconds: int


class DeleteRecordParams(BaseModel):
    record: DnsRecordRef | None = None


class CreateRecordParams(BaseModel):
    """`dns.create_record` (DR-0034). `intent` is Optional for the same reason
    `DeleteRecordParams.record` is: most profiles never enable DNS, and V4's
    Optional-binds-Optional rule is what lets all four provision workflows bind
    `spec.dns_intent` unconditionally."""

    intent: DnsIntent | None = None
    slug: str
    address: str


class CreateRecordOutput(BaseModel):
    """`created` is what makes the undo safe: True only when the POST branch ran,
    so a rollback never deletes a record that pre-existed the run and merely got
    its IP updated (`services/dns.py`'s "P2 graft", Seam C §5.5)."""

    record: DnsRecordRef | None = None
    created: bool = False


class StoreDnsRecordParams(BaseModel):
    cluster_id: str
    record: DnsRecordRef | None = None


class AutoSnapshotParams(BaseModel):
    """DR-0040. `trigger` is defaulted so the operator case is what you get by
    omission -- the unattended case must be stamped deliberately.

    DR-0043 adds `snapshot`: the operator's explicit `snapshot_before_destroy`,
    which is a SEPARATE question from `trigger`'s provenance and can be set
    alongside it. Also defaulted, so omission means "nobody asked"."""

    cluster_id: str
    trigger: str = "operator"
    snapshot: bool = False


class AutoSnapshotOutput(BaseModel):
    snapshot_id: str | None = None
    skipped_reason: str | None = None


class WipeNamespaceParams(BaseModel):
    """TODO(Pillar-3): this task's own minimal inference -- see module docstring."""

    kubeconfig: SecretStr | None = None
    namespace: str


class RolloutUndoParams(BaseModel):
    kubeconfig: SecretStr
    namespace: str = "default"


class FetchKubeconfigByResourceIdsParams(BaseModel):
    """`infra.fetch_kubeconfig` -- the kind/orbstack variant of Seam C
    `FetchKubeconfig`: identifies the local cluster via `resource_ids` instead
    of an ssh `SSHTarget`/`known_hosts` pair (the ssh-k3s variant is the
    distinct, fixed-provider `k3s.fetch_kubeconfig`/`FetchKubeconfigParams`
    above). `provider` is DR-0022 ruling 1's late-binding key -- 2->1 merge of
    DR-0004's `kind.fetch_kubeconfig`/`orbstack.fetch_kubeconfig`."""

    provider: str
    resource_ids: Mapping[str, str]
    rewrite_server_to: str


class ApplyManifestParams(BaseModel):
    """Seam C `KubeApplyManifest`, transcribed for the Traefik infra-shim steps
    (`kube.apply_file`): `manifest_path` is a literal workflow constant
    (config/manifest-templates/infrastructure/traefik-{kind,orbstack}.yaml),
    never a Ref -- there is no upstream step that produces a manifest body for
    this single-file shim, unlike `kube.apply_docs`'s `docs: list[ManifestDoc]`
    (which stays deploy-waves-only; this is a deliberately distinct, narrower
    verb so the two Params shapes never collide -- Seam C's
    `KubeApplyManifest.manifest_yaml: str` is the literal file's text, read by
    the step implementation, not passed through YAML)."""

    kubeconfig: SecretStr
    manifest_path: str


class ProbeRolloutParams(BaseModel):
    """Seam C `KubeProbeRollout` -- one iteration of `kubectl rollout status
    --watch=false`, gate-polled; non-fatal for the Traefik shim (seam-c-provider.md
    fault table row 26: "rollout slow after apply -- not an error"). `deployment`/
    `namespace` are literal ("traefik"/"traefik"), matching the shipped manifests'
    own metadata.name/metadata.namespace."""

    kubeconfig: SecretStr
    deployment: str
    namespace: str = "default"


class DestroyInstanceParams(BaseModel):
    """`infra.destroy_instance` -- Seam C `DestroyInstance`'s Params
    (`slug`/`resource_ids`) plus `provider` (DR-0022 ruling 1's late-binding
    key). DR-0022 P8 fix: the pre-DR-0022 `provider.destroy_server` verb
    declared `EmptyParams`, which cannot type-check `command(self, params)` as
    a pure mapping to `DestroyInstance(slug, resource_ids)` -- this typed
    Params, fed by the new `cluster.load_infra` head step (ruling 2), closes
    that gap."""

    provider: str
    slug: str
    resource_ids: Mapping[str, str]


class LoadInfraParams(BaseModel):
    cluster_id: str


class LoadInfraOutput(BaseModel):
    """`cluster.load_infra` -- DR-0022 ruling 2's destroy head, replacing the
    dispatch table's `dns_record_ref(cluster)` snapshot-at-dispatch-time hook.
    Read FRESH at run time (stale-on-retry-proof), feeding
    `infra.destroy_instance`'s typed Params and `dns.delete_record`'s
    `record` binding in one load."""

    provider: str
    slug: str
    resource_ids: Mapping[str, str]
    dns_record: DnsRecordRef | None = None


# ---------------------------------------------------------------------------
# The declared-verbs table -- rekeyed to DR-0022's 30-verb vocabulary.
# ---------------------------------------------------------------------------

# plane/thin, transcribed from DR-0022's verb table + P2/E12:
#   - every `infra.*`/`k3s.*`/`kube.*`/`do.*` verb is plane="provider" (it
#     wraps a Seam C ProviderCommand, conformance-covered -- ProviderStep/
#     LateBoundProviderStep both default `plane="provider"` per Erratum E12).
#   - `thin=True` iff the table's "Seam C command" column names exactly ONE
#     command with no composite/probe conjunction. The table's own words mark
#     the exceptions: `infra.destroy_instance` ("DestroyInstance +
#     ProbeDestruction"), `kube.delete_daemonset` ("delete + absence probe"),
#     `kube.wipe_namespace` ("composite (KubeRun)"), `deploy.prepare_wave`/
#     `deploy.ensure_rollouts` ("composite"), `deploy.await_wave` ("composite
#     gate") are all thin=False -- the rest of the provider-plane rows are
#     thin=True. `deploy.*`'s three composites stay plane="provider" (they are
#     ProviderSteps issuing N Seam C commands per Erratum E12's own example:
#     "composites like kube.wipe_namespace and deploy.await_wave are
#     ProviderSteps issuing N commands"), NOT plane="domain" -- only
#     `deploy.load_audit`/`plan_waves`/`restore_snapshot` (no Seam C command at
#     all) and the `cluster.*` family are domain.
#   - `dns.delete_record` is plane="service" (P1: "dns. -- a DNS record via
#     DnsService", the ONE service-plane verb) -- and therefore thin=False,
#     since thin implies ProviderStep (plane="provider"), which DnsService's
#     verb is not.
DECLARED_VERBS: dict[str, VerbFixture] = {
    # -- deploy-waves.yml / deploy-rollback.yml -----------------------------
    "deploy.load_audit": VerbFixture(DeployLoadAuditParams, DeployLoadAuditOutput, plane="domain"),
    "cluster.load_kubeconfig": VerbFixture(EmptyParams, KubeconfigOutput, plane="domain"),
    "kube.cluster_info": VerbFixture(KubeconfigParams, EmptyOutput, plane="provider", thin=True),
    "deploy.plan_waves": VerbFixture(PlanWavesParams, PlanWavesOutput, plane="domain"),
    "deploy.prepare_wave": VerbFixture(DeleteJobsParams, EmptyOutput, plane="provider", thin=False),
    "kube.apply_docs": VerbFixture(
        ApplyParams, ApplyOutput, undoable=False, plane="provider", thin=True
    ),  # D1 fix (ruling 3)
    "deploy.restore_snapshot": VerbFixture(RestoreSnapshotParams, EmptyOutput, plane="domain"),
    "deploy.ensure_rollouts": VerbFixture(RolloutRestartParams, EmptyOutput, plane="provider", thin=False),
    "deploy.await_wave": VerbFixture(WaveReadyParams, EmptyOutput, gateable=True, plane="provider", thin=False),
    "kube.rollout_undo": VerbFixture(RolloutUndoParams, EmptyOutput, plane="provider", thin=True),
    # -- provision-*.yml: cluster head + late-bound infra.* family ----------
    "cluster.load_spec": VerbFixture(LoadSpecParams, LoadSpecOutput, plane="domain"),
    "infra.create_instance": VerbFixture(
        CreateInstanceParams, InstanceCreatedOutput, undoable=True, plane="provider", thin=True, idempotent=False
    ),  # seam-b-engine.md:524's one pinned non-idempotent verb (see VerbFixture.idempotent's docstring)
    "infra.await_instance": VerbFixture(
        AwaitInstanceParams, AddressOutput, gateable=True, plane="provider", thin=True
    ),
    "infra.fetch_kubeconfig": VerbFixture(
        FetchKubeconfigByResourceIdsParams, KubeconfigOutput, plane="provider", thin=True
    ),
    # -- provision-digitalocean.yml / provision-tart.yml: ssh-k3s plane -----
    "k3s.await_ssh": VerbFixture(HostParams, EmptyOutput, gateable=True, plane="provider", thin=True),
    "k3s.trust_host_keys": VerbFixture(TrustHostKeysParams, KnownHostsOutput, plane="provider", thin=True),
    # undoable=False (NOT True): seam-c-provider.md:474 gives InstallK3s no
    # inverse ("CaptureHostKeys / InstallK3s / FetchKubeconfig -> none --
    # subsumed by the instance undo"), and providers/compensation.py's
    # `undo_for` has no InstallK3s arm -- it is unconditionally None. A
    # provision workflow's `on_failure: compensate` pushes an undo scope for
    # every undoable step in it; declaring this one undoable=True would be
    # the same "fixture says undoable but the compensation table gives it no
    # inverse" contradiction Erratum E7 corrected for `dns.delete_record`.
    "k3s.install": VerbFixture(InstallK3sParams, EmptyOutput, undoable=False, plane="provider", thin=True),
    "k3s.await_api": VerbFixture(K3sAwaitReadyParams, EmptyOutput, gateable=True, plane="provider", thin=True),
    "k3s.fetch_kubeconfig": VerbFixture(FetchKubeconfigParams, KubeconfigOutput, plane="provider", thin=True),
    "cluster.store_kubeconfig": VerbFixture(KubeconfigStoreParams, KubeconfigRefOutput, plane="domain"),
    # -- provision-*.yml: DNS creation (DR-0034) ---------------------------
    # undoable=True, unlike its `dns.delete_record` sibling: §5.5's "destruction IS
    # compensation; never auto-undone" is about DELETES. A CREATE has a real
    # inverse, and `DnsRecordUpserted.created` is what makes taking it safe -- undo
    # deletes iff this run's POST branch ran, never a record it merely re-pointed.
    "dns.create_record": VerbFixture(
        CreateRecordParams, CreateRecordOutput, undoable=True, plane="service", thin=False
    ),
    "cluster.store_dns_record": VerbFixture(StoreDnsRecordParams, EmptyOutput, plane="domain"),
    "cluster.auto_snapshot": VerbFixture(AutoSnapshotParams, AutoSnapshotOutput, plane="domain"),
    "do.apply_firewalls": VerbFixture(ApplyFirewallsParams, EmptyOutput, plane="provider", thin=True),
    "do.assign_project": VerbFixture(AssignToProjectParams, EmptyOutput, plane="provider", thin=True),
    # -- provision-kind.yml / provision-orbstack.yml: Traefik infra shim ----
    "kube.apply_file": VerbFixture(ApplyManifestParams, EmptyOutput, undoable=True, plane="provider", thin=True),
    "kube.await_rollout": VerbFixture(ProbeRolloutParams, EmptyOutput, gateable=True, plane="provider", thin=True),
    # -- destroy-cloud.yml / destroy-shared.yml -----------------------------
    "cluster.load_infra": VerbFixture(LoadInfraParams, LoadInfraOutput, plane="domain"),
    "cluster.load_kubeconfig_optional": VerbFixture(EmptyParams, OptionalKubeconfigOutput, plane="domain"),
    "kube.delete_daemonset": VerbFixture(
        DeleteDaemonsetParams, EmptyOutput, gateable=True, plane="provider", thin=False
    ),  # D2 fix (ruling 4)
    # undoable=False: seam-c-provider.md §5.5's compensation table is explicit --
    # "DestroyInstance / KubeDeleteManifest / DNS delete -> none -- destruction IS
    # compensation; never auto-undone". Marking a destroy-workflow DNS deletion
    # undoable would be the exact D1-shaped landmine DR-0022 already fixed for
    # kube.apply_docs (destroy-cloud.yml/destroy-shared.yml both run this with
    # on_failure: report today, so no undo scope is ever pushed -- but the fixture
    # is the contract Round 8b's real Step must satisfy, and "never auto-undone"
    # is unconditional, not conditioned on today's YAML).
    "dns.delete_record": VerbFixture(DeleteRecordParams, EmptyOutput, undoable=False, plane="service", thin=False),
    "infra.destroy_instance": VerbFixture(
        DestroyInstanceParams, EmptyOutput, gateable=True, plane="provider", thin=False
    ),
    "kube.wipe_namespace": VerbFixture(WipeNamespaceParams, EmptyOutput, plane="provider", thin=False),
}


# ---------------------------------------------------------------------------
# Named types for workflow `inputs:` type strings, and the RegistryView itself.
# ---------------------------------------------------------------------------

NAMED_TYPES: dict[str, type] = {
    "str": str,
    "int": int,
    "float": float,
    "bool": bool,
    "ClusterSpecification": ClusterSpecification,
    "DnsRecordRef": DnsRecordRef,
    "ManifestDoc": ManifestDoc,
    "DeploymentProfile": DeploymentProfile,
    "SnapshotRestoreSpec": SnapshotRestoreSpec,
    "Wave": Wave,
    "ApplyChangeSummary": ApplyChangeSummary,
}


def _parse_type_expr(expr: str, named: Mapping[str, type]) -> type | None:
    """Delegates the grammar to production's own ``resolve_type_expr``
    (``engine/registry.py``), so this fixture registry and the REAL
    ``StepRegistry`` can never resolve the same ``type:`` string differently --
    gate finding M-1. The NAME table below stays a SEPARATE dict from
    ``engine/registry.py``'s own ``NAMED_TYPES`` (fixture files never import
    production's mutable module state), but as of the Round 10 "dtos" rekeying
    every entry in it is the same real, imported ``seedpod/core/`` type
    production registers too -- no stand-in DTOs remain in this table."""
    return resolve_type_expr(expr, named)


@dataclass
class ShippedWorkflowRegistry:
    """The concrete RegistryView (engine/config.py's Protocol) tests/engine/
    test_shipped_workflows.py validates every shipped YAML against."""

    verbs: Mapping[str, VerbFixture] = field(default_factory=lambda: DECLARED_VERBS)
    named_types: Mapping[str, type] = field(default_factory=lambda: NAMED_TYPES)

    def verb(self, name: str) -> VerbSpec | None:
        return self.verbs.get(name)

    def resolve_type(self, type_expr: str) -> type | None:
        return _parse_type_expr(type_expr, self.named_types)
