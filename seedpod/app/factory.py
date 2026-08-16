"""``build_app()`` -- the composition root (docs/design/seam-d-foundation.md
Decision 8), AS AMENDED by docs/design/coherence-review.md Conflict 15 (the
``TimerService``/``check_ready``/``poke``-naming resolution, which OVERRIDES Seam
D's own factory excerpt per CLAUDE.md's precedence rule: "coherence-review.md
overrides the seam specs") AND by DR-0015 (ratified 2026-07-17: a fourth keyword
seam, ``http_transport``, for the httpx-based supporting services GHCR/DNS --
neither is a ``Provider``, so neither is reachable through ``providers=``).
Construction follows the dependency DAG -- leaves -> persistence -> rules (FAIL
FAST) -> providers -> httpx supporting services (DR-0015) -> dispatcher ->
timers -> engine -> executor -> services -> api -- with no post-hoc setters
beyond ``dispatcher.attach_executor``/``attach_timers`` (both correctness-inert
latency hints; Decision 8's own acyclicity exception).

Pure construction: no IO, no threads, no DB connection, no env reads, no schema
apply. Every constructor call below is matched against the REAL committed
``__init__``/classmethod signature (read, not guessed -- several have drifted from
Decision 8's own illustrative code block, e.g. ``EffectExecutor`` now takes
``dispatch: WorkflowDispatch`` instead of ``repos=``, and ``WorkflowEngine`` takes
``run_repo``/``step_repo``/``outbox_repo`` instead of a single ``repos=``).

One prerequisite component this task's brief explicitly permits to be partial
(documented again at its construction site below, not silently swallowed):

* The concrete verb catalog -- ``ProviderStep``/domain-``Step`` subclasses for
  every verb ``config/workflows/*.yml`` names (DR-0022's re-normalized 30-verb
  vocabulary: ``infra.*``/``k3s.*``/``kube.*``/``cluster.*``/``deploy.*``/
  ``dns.*``/``do.*`` -- supersedes DR-0004's ``do.*``/``kind.*``/``tart.*``/
  ``orbstack.*``/``ssh.*``/``k3s.*``/``kubectl.*``/``kubeconfig.*``/``provider.*``).
  Round 8a's "domain-steps" component landed the first two -- ``cluster.load_spec``/
  ``cluster.store_kubeconfig`` (``seedpod/engine/steps/cluster.py``), the
  provision workflows' head/tail. The "infra-and-do" component
  lands five more -- ``infra.create_instance``/``infra.await_instance``/
  ``infra.fetch_kubeconfig`` (``LateBoundProviderStep`` bindings, DR-0022
  ruling 1) plus ``do.apply_firewalls``/``do.assign_project`` (DO-only, P7)
  (``seedpod/engine/steps/infra.py``). This round's "k3s-family" component lands
  five more -- ``k3s.await_ssh``/``k3s.trust_host_keys``/``k3s.install``/
  ``k3s.await_api``/``k3s.fetch_kubeconfig`` (fixed ``provider_name="ssh-k3s"``
  ``ProviderStep`` bindings, ``seedpod/engine/steps/k3s.py`` -- that module's own
  docstring records the SSH-identity question DR-0023 settled:
  ``cluster.load_spec``'s Output threads ``ssh_user``/``ssh_private_key_path``
  (this module's own ``_ssh_identities()`` reads them from each provider's
  ``config/providers/*.yml``), and every ``k3s.py`` verb that builds an
  ``SSHTarget`` now takes both as ``Params``, bound from that head step in
  ``provision-{digitalocean,tart}.yml`` -- no module-level placeholder remains).
  The "kube-shim" component lands the last two provision-path verbs --
  ``kube.apply_file``/``kube.await_rollout``, the Traefik infra-shim apply +
  non-fatal rollout gate ``provision-{kind,orbstack}.yml`` both need
  (``seedpod/engine/steps/kube.py``) -- 14 provision-path verbs total, enough
  for real end-to-end provisioning on all four
  ``provision-{digitalocean,tart,kind,orbstack}.yml`` workflows start to finish
  (proven end-to-end against real DigitalOcean infrastructure on 2026-08-02:
  all 11 steps, first attempt, cluster ACTIVE in 185s). Round 8b's destroy-path
  components then land the remaining ``infra.destroy_instance``/
  ``cluster.load_infra``/``dns.delete_record``/``kube.cluster_info``/
  ``kube.rollout_undo``/``kube.delete_daemonset``/``kube.wipe_namespace`` --
  23 verbs total, both destroy workflows (``destroy-cloud.yml``/
  ``destroy-shared.yml``) now fully registered and proven end to end on real
  DigitalOcean infrastructure too (the full cluster lifecycle, provision
  through destroy). This round's ("Round 10") "load-and-plan" component adds
  three more -- ``deploy.load_audit``/``deploy.plan_waves``/
  ``deploy.prepare_wave`` (``seedpod/engine/steps/deploy.py``) -- 26 verbs
  total. This round's "apply-and-wait" component then lands three more --
  ``kube.apply_docs``/``deploy.ensure_rollouts``/``deploy.await_wave``
  (``seedpod/engine/steps/deploy_apply.py``) -- 29 verbs total. This round's
  "restore-and-rehydrate" component lands the LAST verb --
  ``deploy.restore_snapshot`` (``seedpod/engine/steps/deploy_restore.py``) --
  **30 of 30 DR-0022 verbs registered, the full catalog complete.** The
  formerly-PARTIAL registry note below is now historical: every verb every
  shipped workflow names resolves against the real registry (``tests/engine/
  test_verb_conventions.py``'s own ``FULLY_REGISTERED_WORKFLOWS`` -- widened
  this round to every ``config/workflows/*.yml`` file, not a subset). The ONLY
  place the FULL verb->shape contract is declared IN A TEST FIXTURE (as
  opposed to the real registry built here) remains
  ``tests/engine/declared_verbs.py``. ``load_workflow_definitions()`` matches
  Decision 8's own signature exactly -- a directory path, no registry argument
  -- which is consistent with the committed design, not a workaround: grepping
  ``seedpod/engine/engine.py`` shows ``WorkflowEngine.__init__`` never calls
  ``validate_workflow`` either, so boot-time verb validation was never wired in
  the first place.

  A workflow whose every verb is NOT registered hits ``UnknownVerbError`` at
  the first unregistered verb, unhandled -- ``WorkflowEngine._run()``'s task
  dies ("Task exception was never retrieved"), no ``workflow_steps`` row is
  ever written, the run row is stranded in ``running`` forever, and whatever
  cluster/deployment it targeted is stranded with no API path out (recorded in
  full, including the DigitalOcean-droplet manual-cleanup consequence it had
  on the destroy path before Round 8b closed it, at docs/PARITY-BACKLOG.md
  P0 #0's sub-finding). That failure mode is now structurally unreachable for
  every shipped workflow -- kept as a recorded lesson, not a live risk.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import yaml

from seedpod.api.factory import create_api
from seedpod.app.app import App, Services
from seedpod.app.config import AppConfig
from seedpod.app.services import (
    ApiKeyService,
    ClusterService,
    DeploymentService,
    PresetService,
    SecretService,
    SnapshotService,
)
from seedpod.core.clock import Clock, SystemClock
from seedpod.data.database import Database
from seedpod.data.repositories import (
    ApiKeyRepository,
    ClusterRepository,
    ClusterStateAuditRepository,
    DeploymentAuditRepository,
    DeploymentRepository,
    DeploymentStateAuditRepository,
    OutboxRepository,
    PresetRepository,
    Repositories,
    SecretAuditRepository,
    SecretRepository,
    SnapshotRepository,
    TimerRepository,
    WorkflowRunRepository,
    WorkflowStepRepository,
)
from seedpod.data.uow import UnitOfWork
from seedpod.engine.config import WorkflowDefinition, parse_workflow
from seedpod.engine.dispatch_table import WorkflowDispatch
from seedpod.engine.engine import WorkflowEngine
from seedpod.engine.registry import StepRegistry
from seedpod.engine.step import StepServices
from seedpod.engine.steps.cluster import (
    AutoSnapshot,
    LoadInfra,
    LoadKubeconfig,
    LoadKubeconfigOptional,
    LoadSpec,
    SshIdentity,
    StoreDnsRecord,
    StoreKubeconfig,
)
from seedpod.engine.steps.deploy import DeployLoadAudit, DeployPrepareWave, PlanWaves
from seedpod.engine.steps.deploy_apply import DeployAwaitWave, DeployEnsureRollouts, KubeApplyDocs
from seedpod.engine.steps.deploy_restore import DeployRestoreSnapshot
from seedpod.engine.steps.dns import DnsCreateRecord, DnsDeleteRecord
from seedpod.engine.steps.infra import (
    DoApplyFirewalls,
    DoAssignToProject,
    InfraAwaitInstance,
    InfraCreateInstance,
    InfraDestroyInstance,
    InfraFetchKubeconfig,
)
from seedpod.engine.steps.k3s import (
    K3sAwaitApi,
    K3sAwaitSsh,
    K3sFetchKubeconfig,
    K3sInstall,
    K3sTrustHostKeys,
)
from seedpod.engine.steps.kube import (
    KubeApplyFile,
    KubeAwaitRollout,
    KubeClusterInfo,
    KubeDeleteDaemonset,
    KubeRolloutUndoStep,
    KubeWipeNamespace,
)
from seedpod.providers.contract import Provider, SubprocessRunner
from seedpod.providers.digitalocean import DigitalOceanConfig, DigitalOceanProvider
from seedpod.providers.kind import KindConfig, KindProvider
from seedpod.providers.kubectl import KubectlConfig, KubectlProvider
from seedpod.providers.orbstack import OrbstackConfig, OrbstackProvider
from seedpod.providers.ssh_k3s import SshK3sConfig, SshK3sProvider
from seedpod.providers.tart import TartConfig, TartProvider
from seedpod.runtime.dispatcher import Dispatcher
from seedpod.runtime.effect_executor import EffectExecutor
from seedpod.runtime.health import HealthMonitor
from seedpod.runtime.reconciliation import ReconciliationService
from seedpod.runtime.sse import SSEHub
from seedpod.runtime.subprocess_manager import (
    DetachedLaunchRunner,
    SubprocessManager,
    TrackedSubprocessRunner,
)
from seedpod.runtime.timers import TimerService
from seedpod.services.crypto import CryptoService, SecretManager
from seedpod.services.dns import DnsConfig, DnsService
from seedpod.services.ghcr import GhcrConfig, GhcrService
from seedpod.services.manifests import ManifestResolver
from seedpod.services.rules import RuleEngine

__all__ = [
    "build_app",
    "load_enabled_providers",
    "load_workflow_definitions",
    "MissingGithubOrganization",
]

# DR-0005: `tart run` outlives seedpod (blast-radius law) -- every other provider's
# transport is the plain tracked runner.
_TART_LAUNCH_PREFIXES: tuple[tuple[str, ...], ...] = (("tart", "run"),)

# Conflict 13's WorkflowDispatch, resolved in the composition root (its own
# docstring): abstract 'destroy' -> the concrete definition per provider family
# (DR-0004's kind/tart/orbstack verb families share destroy-shared.yml; the two
# cloud/VM-hypervisor providers with real infrastructure to tear down share
# destroy-cloud.yml).
_DESTROY_BY_PROVIDER: Mapping[str, str] = {
    "digitalocean": "destroy-cloud",
    "tart": "destroy-cloud",
    "kind": "destroy-shared",
    "orbstack": "destroy-shared",
}


def build_app(
    config: AppConfig,
    *,
    providers: Mapping[str, Provider] | None = None,
    clock: Clock | None = None,
    id_gen: Callable[[], str] | None = None,
    http_transport: httpx.AsyncClient | None = None,
) -> App:
    """Pure construction. No IO, no threads, no DB connection, no env reads, no
    schema apply -- see module docstring for the two prerequisite gaps this
    build honestly leaves open (both flagged again inline, below).

    ``http_transport`` is DR-0015's fourth test seam (ratified 2026-07-17,
    amending Decision 8's three-seam signature): the shared outbound-HTTP
    transport for the httpx-based supporting services (GHCR, Cloudflare DNS --
    neither is a ``Provider``, so neither is reachable through the
    ``providers=`` seam). Constructing an ``httpx.AsyncClient`` is IO-free
    (lazy connection pool), so accepting/building one here does not violate
    "no IO, no threads" -- no request is made until a caller awaits one."""
    clock = clock or SystemClock()
    id_gen = id_gen or (lambda: str(uuid4()))

    # 1 -- leaves (no dependencies)
    db = Database(config.database_url)
    crypto = CryptoService(dev_key=config.secret_key_dev, prod_key=config.secret_key_prod)
    hub = SSEHub(clock)
    subprocesses = SubprocessManager()
    owns_http_transport = http_transport is None
    # DR-0015: sane timeouts, no base_url/default headers -- GhcrService/DnsService
    # already pass absolute URLs and per-request headers.
    transport = http_transport if http_transport is not None else httpx.AsyncClient(timeout=30.0)

    # 2 -- persistence (Repositories is deliberately the minimal Dispatcher-facing
    # bundle -- seedpod/data/repositories.py's own docstring: "grows additively" --
    # api_keys/secrets/deployment_audits/presets/snapshots repos exist as standalone
    # classes, not in this bundle, because the Dispatcher never touches them; this
    # component (the four app-services) wires the three of those five it needs
    # directly into the services that need them, below.)
    repos = Repositories(
        clusters=ClusterRepository(),
        deployments=DeploymentRepository(),
        cluster_state_audits=ClusterStateAuditRepository(),
        deployment_state_audits=DeploymentStateAuditRepository(),
        timers=TimerRepository(),
        outbox=OutboxRepository(),
        workflow_runs=WorkflowRunRepository(),
    )
    uow = UnitOfWork(db)
    deployment_audits_repo = DeploymentAuditRepository(crypto)
    secrets_repo = SecretRepository(crypto)
    secret_audits_repo = SecretAuditRepository()
    api_keys_repo = ApiKeyRepository()
    presets_repo = PresetRepository()
    snapshots_repo = SnapshotRepository()

    # 3 -- pure domain: nothing to construct; transition() is a function, imported
    # where used (runtime/dispatcher.py), not here.

    # 4 -- rule engine: FAIL FAST (v1 swallowed RuleValidationError and ran ruleless),
    # injected into DeploymentService below (step 9).
    rules = RuleEngine.load(config.config_dir / "deployment-rules.yml")

    # 5 -- providers: stateless, no DB, all context in the command. Absence from
    # the mapping IS "disabled" (no ProviderDisabledError type exists in v2).
    providers = dict(providers) if providers is not None else load_enabled_providers(config, subprocesses)

    # 5.5 -- httpx supporting services (DR-0015): credential-gated, sharing the
    # one `transport` above. github_token unset -> ghcr_service stays None and
    # ManifestResolver degrades gracefully (the no-token acceptance test's
    # "limited manifest resolution"); cloudflare_api_token unset -> dns stays
    # None. Default test fixtures (both unset) touch `transport` zero times.
    #
    # `organization=_resolve_github_organization(config)` (PARITY-BACKLOG #0b),
    # replacing the former `config.github_organization or ""` -- that silently
    # built a `GhcrConfig` whose every non-triggering-repo image URL came out
    # `ghcr.io//<repo>:<tag>` (a real double slash) whenever the env var was
    # unset, which v1 never hit because v1 always had `config/org.yml` to fall
    # back to. `_resolve_github_organization` (below) restores that fallback
    # AND fails loudly, right here, if the org is STILL empty afterwards --
    # see that function's own docstring for the full precedence/failure-point
    # reasoning. Deliberately INSIDE `if config.github_token:`: no token means
    # `ghcr_service` is never constructed and an org is never even resolved,
    # let alone validated, so the no-token degradation path above is entirely
    # untouched by this.
    ghcr_service: GhcrService | None = None
    if config.github_token:
        ghcr_service = GhcrService(
            GhcrConfig(token=config.github_token, organization=_resolve_github_organization(config)), transport
        )
    manifest_resolver = ManifestResolver(ghcr_service=ghcr_service)

    dns: DnsService | None = None
    if config.cloudflare_api_token:
        dns = DnsService(DnsConfig(api_token=config.cloudflare_api_token), transport)

    # 6 -- dispatcher: the ONLY writer of cluster/deployment transitions.
    dispatcher = Dispatcher(uow=uow, repos=repos, clock=clock)

    # 6.5 -- timers (coherence-review Conflict 15: Seam D's factory never wired a
    # poller for the `timers` table Conflict 1 introduced; this closes that gap).
    timers = TimerService(uow=uow, repos=repos, dispatcher=dispatcher, clock=clock)

    # 7 -- workflow engine: pinned definitions + closed verb registry. See module
    # docstring -- the registry is only PARTIALLY populated until the rest of the
    # verb catalog lands (Round 8a's "domain-steps" component: cluster.load_spec/
    # cluster.store_kubeconfig only, so far); `load_workflow_definitions` is
    # parse-only, matching both Decision 8's own signature (directory, no
    # registry arg) and WorkflowEngine's actual constructor (which never
    # validates against a registry either).
    kubectl_provider = KubectlProvider(KubectlConfig(), TrackedSubprocessRunner(subprocesses), clock=clock)
    # ssh-k3s: the shared k3s-plane provider both provision-digitalocean.yml and
    # provision-tart.yml drive (seedpod/engine/steps/k3s.py). `SshK3sConfig()` needs
    # no ssh identity -- DR-0023 threads that per-provider through `cluster.load_spec`
    # / `_ssh_identities()` instead -- only its two unrelated timeouts, both defaulted.
    ssh_k3s_provider = SshK3sProvider(SshK3sConfig(), TrackedSubprocessRunner(subprocesses))
    secret_manager = SecretManager(key=config.secret_key_dev)
    # DR-0020 (Round 6, api-features): the real fail-open pre-destroy snapshot
    # collaborator. Built HERE -- earlier than Decision 8's own step-9
    # illustrative ordering -- because Round 10's `deploy.restore_snapshot`
    # verb (`seedpod/engine/steps/deploy_restore.py`, the restore-and-rehydrate
    # component) is ALSO one of its callers now, and `_build_step_registry`
    # (just below) needs it constructor-injected the identical way every other
    # domain step's dependencies already are. `ClusterService` (step 9, below)
    # still receives this SAME instance -- one `SnapshotService`, two
    # consumers, never two independently constructed copies that could drift.
    snapshot_service = SnapshotService(
        snapshots_repo, repos, repos.deployments, crypto, kubectl_provider, uow, clock, id_gen,
        config.config_dir, config.snapshot_storage_path,
    )
    ssh_identities = _ssh_identities(config)
    steps = _build_step_registry(
        uow=uow,
        repos=repos,
        crypto=crypto,
        clock=clock,
        ssh_identities=ssh_identities,
        config_dir=config.config_dir,
        dns=dns,
        deployment_audits=deployment_audits_repo,
        manifest_resolver=manifest_resolver,
        snapshots=snapshot_service,
    )
    definitions = load_workflow_definitions(config.config_dir / "workflows")
    step_services = StepServices(
        subprocess_manager=subprocesses,
        providers={**providers, "kubectl": kubectl_provider, "ssh-k3s": ssh_k3s_provider},
        repositories=repos,
        secret_manager=secret_manager,
    )
    engine = WorkflowEngine(
        definitions=definitions,
        steps=steps,
        uow=uow,
        run_repo=repos.workflow_runs,
        step_repo=WorkflowStepRepository(),
        outbox_repo=repos.outbox,
        dispatcher=dispatcher,
        clock=clock,
        step_services=step_services,
        secret_manager=secret_manager,
    )

    # 8 -- effect executor: drains outbox -> hub | engine (run admission).
    dispatch = WorkflowDispatch(destroy_by_provider=_DESTROY_BY_PROVIDER)
    executor = EffectExecutor(
        uow=uow,
        repos=repos,
        hub=hub,
        engine=engine,
        dispatch=dispatch,
        clock=clock,
        poll_interval=config.outbox_poll_interval,
    )
    dispatcher.attach_executor(executor)  # sole late wire: .poke() latency hint only
    dispatcher.attach_timers(timers)  # ditto

    # 9 -- thin application services (seedpod/app/services/) + the Round-5 runtime
    # services this component must wire (seam-d Decision 8 predates
    # ReconciliationService/HealthMonitor; they mirror each other exactly per this
    # round's brief). Each app-service's real constructor is documented at its own
    # class docstring -- every kwarg beyond Decision 8's own illustrative sketch
    # (id_gen/config_dir/deployment_audits on DeploymentService; crypto/
    # kubectl_provider on ClusterService; the standalone secrets/api_keys repos)
    # is explained there, not repeated here.
    reconciliation = ReconciliationService(providers, repos, dispatcher, engine, uow, hub, clock)
    health = HealthMonitor(kubectl_provider, crypto, repos, dispatcher, uow, clock)
    deployment_service = DeploymentService(
        dispatcher, repos, uow, rules=rules, crypto=crypto, clock=clock,
        manifest_resolver=manifest_resolver, dns=dns, id_gen=id_gen, config_dir=config.config_dir,
        deployment_audits=deployment_audits_repo, secrets=secrets_repo,
    )
    # `snapshot_service` -- DR-0020's real fail-open pre-destroy collaborator,
    # now ALSO `deploy.restore_snapshot`'s own collaborator -- was constructed
    # earlier (step 7, above `_build_step_registry`'s own call) so the SAME
    # instance could be constructor-injected into the verb registry; reused
    # here for `ClusterService` rather than built a second time.
    services = Services(
        clusters=ClusterService(
            dispatcher, repos, uow, id_gen, clock, crypto=crypto, kubectl_provider=kubectl_provider,
            snapshots=snapshot_service,
        ),
        deployments=deployment_service,
        secrets=SecretService(crypto, secrets_repo, secret_audits_repo, uow, clock),
        api_keys=ApiKeyService(api_keys_repo, uow, clock),
        presets=PresetService(presets_repo, deployment_service, uow, clock, id_gen, config.config_dir),
        snapshots=snapshot_service,
        reconciliation=reconciliation,
        health=health,
    )

    # 10 -- HTTP edge, constructed last; consumes services + hub, owns nothing
    # (seedpod/api/factory.py's own docstring for why its signature accepts all
    # three of services/hub/config even though every handler reaches them fresh
    # through `api.state.app`, stamped immediately below).
    api = create_api(services=services, hub=hub, config=config)

    app = App(
        config=config,
        db=db,
        crypto=crypto,
        hub=hub,
        subprocesses=subprocesses,
        repos=repos,
        uow=uow,
        providers=providers,
        ssh_identities=ssh_identities,
        rules=rules,
        dispatcher=dispatcher,
        timers=timers,
        engine=engine,
        executor=executor,
        services=services,
        api=api,
        http_transport=transport,
        owns_http_transport=owns_http_transport,
        ghcr=ghcr_service,
        dns=dns,
        manifest_resolver=manifest_resolver,
        clock=clock,
    )
    api.state.app = app  # lifespan + routes reach App through api.state -- no globals
    return app


# ---------------------------------------------------------------------------
# Providers -- config/providers/*.yml + AppConfig secrets -> constructed Provider
# ---------------------------------------------------------------------------


def load_enabled_providers(config: AppConfig, subprocesses: SubprocessManager) -> dict[str, Provider]:
    """Construct each enabled provider with its transport. DR-0005: every
    provider's transport is a plain ``TrackedSubprocessRunner(subprocesses)``
    EXCEPT tart's, additionally wrapped for its detached ``tart run`` launch.
    Absence from the returned mapping (either ``name not in
    config.enabled_providers``, or the provider's own ``config/providers/*.yml``
    says ``enabled: false``) is the whole disabled-provider story -- no
    ``ProviderDisabledError`` type exists in v2 (Decision 8 step 5)."""
    tracked: SubprocessRunner = TrackedSubprocessRunner(subprocesses)
    tart_transport: SubprocessRunner = DetachedLaunchRunner(tracked, launch_prefixes=_TART_LAUNCH_PREFIXES)

    built: dict[str, Provider] = {}
    for name in config.enabled_providers:
        path = config.config_dir / "providers" / f"{name}.yml"
        raw = _load_yaml(path)
        if raw is None or not raw.get("enabled", True):
            continue
        if name == "digitalocean":
            built["digitalocean"] = DigitalOceanProvider(_digitalocean_config(config, raw), httpx.AsyncClient())
        elif name == "kind":
            built["kind"] = KindProvider(_kind_config(raw), tracked)
        elif name == "tart":
            built["tart"] = TartProvider(_tart_config(raw), tart_transport)
        elif name == "orbstack":
            built["orbstack"] = OrbstackProvider(_orbstack_config(raw), tracked)
        # An unrecognized name in `enabled_providers` constructs nothing -- it
        # fails loud later, at WorkflowDispatch.resolve() time (an unresolvable
        # `provision-<name>` definition), never here (no IO/validation at
        # construction, per this module's docstring).
    return built


def _load_yaml(path: Path) -> Mapping[str, Any] | None:
    if not path.exists():
        return None
    with path.open() as f:
        return yaml.safe_load(f) or {}


def _digitalocean_config(config: AppConfig, raw: Mapping[str, Any]) -> DigitalOceanConfig:
    defaults = raw.get("defaults", {}) or {}
    vpc = raw.get("vpc_config", {}) or {}
    firewall = raw.get("firewall_config", {}) or {}
    droplet = raw.get("droplet_config", {}) or {}
    timeouts = raw.get("api_timeouts", {}) or {}
    ssh_access = raw.get("ssh_access", {}) or {}
    k8s_access = raw.get("k8s_api_access", {}) or {}
    return DigitalOceanConfig(
        api_token=config.digitalocean_token or "",
        project_id=raw.get("project_id"),
        region_mapping=raw.get("region_mapping") or {},
        node_size_mapping=raw.get("node_size_mapping") or {},
        default_region=defaults.get("region", "ams3"),
        default_droplet_size=defaults.get("droplet_size", "s-2vcpu-4gb"),
        default_image=defaults.get("image", "ubuntu-22-04-x64"),
        ssh_key_name=defaults.get("ssh_key_name", "exampleco-testing"),
        default_tags=tuple(droplet.get("default_tags") or ("seedpod-managed", "k3s-cluster")),
        enable_ipv6=bool(droplet.get("enable_ipv6", True)),
        enable_private_networking=bool(droplet.get("enable_private_networking", True)),
        enable_monitoring=bool(droplet.get("enable_monitoring", True)),
        enable_backups=bool(droplet.get("enable_backups", False)),
        request_timeout_s=float(timeouts.get("default", 30)),
        list_timeout_s=float(timeouts.get("list_droplets", 45)),
        create_timeout_s=float(timeouts.get("create_droplet", 60)),
        delete_timeout_s=float(timeouts.get("delete_droplet", 45)),
        vpc_name_prefix=vpc.get("name_prefix", "seedpod-vpc"),
        vpc_ip_range=vpc.get("ip_range", "10.0.0.0/16"),
        vpc_description=vpc.get("description", "Seedpod Infrastructure Manager VPC"),
        vpc_create_if_missing=bool(vpc.get("create_if_missing", True)),
        management_firewall_name_prefix=(firewall.get("management") or {}).get("name_prefix", "seedpod-mgmt"),
        application_firewall_name_prefix=(firewall.get("application") or {}).get("name_prefix", "seedpod-apps"),
        ssh_allowed_sources=tuple(ssh_access.get("allowed_sources") or ("0.0.0.0/0",)),
        k8s_api_allowed_sources=tuple(k8s_access.get("allowed_sources") or ("0.0.0.0/0",)),
    )


def _kind_config(raw: Mapping[str, Any]) -> KindConfig:
    api_server = raw.get("api_server", {}) or {}
    defaults = raw.get("defaults", {}) or {}
    cluster_defaults = raw.get("cluster_defaults", {}) or {}
    networking = cluster_defaults.get("networking", {}) or {}
    health = raw.get("health_checks", {}) or {}
    extra_ports = cluster_defaults.get("extra_port_mappings") or ()
    node_sizes = {
        key: int(value["workers"])
        for key, value in (raw.get("node_size_mapping") or {}).items()
        if isinstance(value, Mapping) and "workers" in value
    }
    return KindConfig(
        api_server_host=api_server.get("host", "localhost"),
        port_range_start=int(api_server.get("port_range_start", 6443)),
        port_range_end=int(api_server.get("port_range_end", 6543)),
        node_image=defaults.get("node_image", "kindest/node:v1.29.2"),
        wait_timeout=defaults.get("wait_timeout", "5m"),
        pod_subnet=networking.get("pod_subnet", "10.244.0.0/16"),
        service_subnet=networking.get("service_subnet", "10.96.0.0/12"),
        extra_port_mappings=tuple(
            (int(m["container_port"]), int(m["host_port"]), m.get("protocol", "TCP")) for m in extra_ports
        ),
        node_size_mapping=node_sizes,
        check_ready_timeout_s=float(health.get("kubectl_timeout", 5.0)),
    )


def _tart_config(raw: Mapping[str, Any]) -> TartConfig:
    base_image = raw.get("base_image", {}) or {}
    defaults = raw.get("defaults", {}) or {}
    rosetta = raw.get("rosetta", {}) or {}
    cleanup = raw.get("cleanup", {}) or {}
    return TartConfig(
        base_image_name=base_image.get("name", "local-dev-base-rosetta"),
        defaults={
            "memory_mb": int(defaults.get("memory_mb", 4096)),
            "cpu_cores": int(defaults.get("cpu_cores", 4)),
            "disk_gb": int(defaults.get("disk_gb", 50)),
        },
        node_size_mapping=raw.get("node_size_mapping") or {},
        rosetta_enabled=bool(rosetta.get("enabled", True)),
        delete_on_destroy=bool(cleanup.get("delete_on_destroy", True)),
    )


def _orbstack_config(raw: Mapping[str, Any]) -> OrbstackConfig:
    api_server = raw.get("api_server", {}) or {}
    health = raw.get("health_checks", {}) or {}
    kwargs: dict[str, Any] = {
        "host": api_server.get("host", "localhost"),
        "public_hostname": raw.get("public_hostname"),
    }
    if "kubectl_context" in raw:
        kwargs["context"] = raw["kubectl_context"]
    if "kubectl_timeout" in health:
        kwargs["cluster_info_timeout_s"] = float(health["kubectl_timeout"])
        kwargs["fetch_kubeconfig_timeout_s"] = float(health["kubectl_timeout"])
    return OrbstackConfig(**kwargs)


def _ssh_identities(config: AppConfig) -> Mapping[str, SshIdentity]:
    """DR-0023: ``cluster.load_spec``'s ``ssh_user``/``ssh_private_key_path``
    are provider configuration, read from each provider's OWN
    ``config/providers/<provider>.yml`` section -- the same file v1 read
    (``digitalocean.yml``'s ``defaults.ssh_user``/``ssh_private_key_path``;
    ``tart.yml``'s ``ssh_config.ssh_user``/``ssh_private_key_path``) -- no
    second source of truth, no duplication into workflow YAML (DR-0023
    decision 2). ``kind``/``orbstack`` have no SSH plane and are deliberately
    absent -- ``LoadSpec`` resolves an absent provider to ``SshIdentity()``
    (``(None, None)``), never a fallback identity (DR-0023 point 5).

    ``~`` is expanded HERE -- this is "the config loader" DR-0023 point 4
    names as responsible for that expansion, matching ``SSHTarget.
    private_key_path``'s own documented contract ("~ pre-expanded by the
    config loader"). Reads raw YAML independently of ``load_enabled_providers``
    (which discards its own ``raw`` dict after building each ``*Config``
    dataclass, neither of which carries an ``ssh_user``/``ssh_private_key_path``
    field -- SSH identity is the ``ssh-k3s`` plane's concern, not the
    compute-provider adapter's own config shape) -- reading independently
    means a provider missing from ``config.enabled_providers`` still resolves
    correctly rather than silently degrading to ``(None, None)``."""
    do_raw = _load_yaml(config.config_dir / "providers" / "digitalocean.yml") or {}
    do_defaults = do_raw.get("defaults", {}) or {}
    tart_raw = _load_yaml(config.config_dir / "providers" / "tart.yml") or {}
    tart_ssh = tart_raw.get("ssh_config", {}) or {}
    identities = {
        "digitalocean": SshIdentity(
            user=do_defaults.get("ssh_user"),
            private_key_path=_expand_user(do_defaults.get("ssh_private_key_path")),
        ),
        "tart": SshIdentity(
            user=tart_ssh.get("ssh_user"),
            private_key_path=_expand_user(tart_ssh.get("ssh_private_key_path")),
        ),
        # kind/orbstack: no SSH plane -- deliberately absent, not `SshIdentity()`
        # explicitly, so a lookup miss (the same code path a genuinely
        # unconfigured provider would hit) is what produces `(None, None)`,
        # not a special-cased entry.
    }
    return identities


def _expand_user(path: str | None) -> str | None:
    return os.path.expanduser(path) if path else None


# ---------------------------------------------------------------------------
# GitHub organization (PARITY-BACKLOG #0b) -- config/org.yml + GITHUB_ORGANIZATION
# ---------------------------------------------------------------------------


class MissingGithubOrganization(RuntimeError):
    """Raised by ``_resolve_github_organization`` -- ``github_token`` is
    configured (a real ``GhcrService`` is about to be constructed) but no
    ``github_organization`` could be resolved from either the
    ``GITHUB_ORGANIZATION`` env var or ``config/org.yml``. Mirrors
    ``seedpod/app/config.py``'s ``MissingEnvironmentVariable`` -- a plain,
    locally-scoped ``RuntimeError`` for a composition-root boot failure, not a
    ``core/errors.py`` ``ProviderError`` (CLAUDE.md's "one error-taxonomy home"
    governs PROVIDER/STEP-runtime failures the engine retries/undoes around;
    this is a boot-time configuration defect the process never gets far enough
    to run a workflow step for -- the same reasoning that already keeps
    ``RuleValidationError``/``MissingEnvironmentVariable`` out of that
    taxonomy)."""


def _resolve_github_organization(config: AppConfig) -> str:
    """PARITY-BACKLOG #0b. v1 sourced ``github_organization`` from
    ``config/org.yml`` (``reference-code/seedpod/seedpod/core/config.py``'s
    ``Settings`` read it as a fallback default -- the shipped
    ``config/org.yml``'s own header: "This file defines infrastructure-level
    branding, naming, and conventions"); v2 read ONLY the ``GITHUB_ORGANIZATION``
    env var (``AppConfig.github_organization``) and silently built
    ``GhcrConfig(organization="")`` when it was unset, which is where the
    module-level comment's double-slash image URL came from.

    **Precedence: the env var wins; the file is the default.** 12-factor, and
    operators already rely on overriding config-file values with env vars
    elsewhere in this tree (``AppConfig.from_env()``'s own docstring: "the
    ONLY place ``os.environ`` is read"). ``config.github_organization`` is
    already that env var by the time it reaches here (or an explicit test
    override of the same field -- indistinguishable from this function's own
    point of view, by design), so a truthy value there short-circuits before
    ``org.yml`` is even read. A blank string counts as "not set", matching
    ``AppConfig._require``'s own ``if not value`` treatment of every other
    required env var in this module -- an operator who exports
    ``GITHUB_ORGANIZATION=""`` almost certainly meant "unset", not "the empty
    organization".

    **Reads ``config_dir / "org.yml"`` directly** (matching
    ``RuleEngine.load(config.config_dir / "deployment-rules.yml")``'s and this
    module's own ``_load_yaml(config.config_dir / "providers" / f"{name}.yml")``
    calls, immediately above) -- NOT ``core/paths.py``'s
    ``resolve_under_config_dir``, which resolves a config-relative path VALUE
    read out of some OTHER shipped YAML file (one that might carry a leading
    ``config/`` segment, e.g. a profile's own ``manifests_dir:`` string).
    ``org.yml`` has no such indirection: its location is a fixed, well-known
    filename directly under the config root, exactly like
    ``deployment-rules.yml``'s -- so a plain ``config_dir / "org.yml"`` join
    already satisfies the one rule that actually matters here (CLAUDE.md /
    the Round-8a ``kube.apply_file`` gate finding): resolve against the
    INJECTED ``config_dir``, never ``Path()`` against the process cwd.

    ``organization.github_organization`` is the ONE key this function reads
    off ``org.yml``. The file also carries ``organization.name``/
    ``.display_name``/``.version``, ``infrastructure.managed_tag``/
    ``.cluster_tag_prefix``, ``naming.{vpc,mgmt,apps}_prefix``,
    ``api.key_prefix``/``.user_agent``/``.title``/``.description``, and
    ``kubernetes.app_secrets_name`` -- NONE of which has a v2 reader yet.
    Deliberately NOT modeled as a dataclass/full config object (YAGNI,
    CLAUDE.md's DR-gated-invention discipline: a model for keys nothing reads
    is speculative structure, not salvage) -- this function reads exactly the
    one nested key it needs via plain ``.get()`` chaining, structured so a
    second key is an obvious one-line addition (another ``.get()`` off the
    same ``_load_yaml`` result) rather than a reason to invent a schema.

    **Failure point: composition-root (``build_app()``) construction time,
    not first use.** The PARITY-BACKLOG's own phrasing ("either port the
    org.yml read OR make the empty org a loud startup failure") reads as
    either/or; this function does both, and chooses the STARTUP half
    deliberately -- it is the earliest point that does not disturb the
    existing no-token degradation path (``build_app()``'s own 5.5-step
    comment: "github_token unset -> ghcr_service stays None and
    ManifestResolver degrades gracefully" -- an acceptance test,
    ``test_deployment_flow_without_github_token``, depends on exactly that).
    This function is called ONLY from inside that same ``if config.
    github_token:`` guard, so "no token" and "token but no resolvable org"
    stay two structurally distinct, independently testable states: the
    former never reaches this function at all (org is never even resolved,
    let alone validated -- ``GhcrService``/``ManifestResolver`` both already
    tolerate a wholly absent GHCR collaborator by design, per that same
    comment), while the latter fails LOUD, before a single HTTP request is
    ever served, rather than lazily on the first deployment that happens to
    resolve a non-triggering-repo image through GHCR -- a much later, far
    more confusing point to discover a boot-time misconfiguration."""
    if config.github_organization:
        return config.github_organization
    raw = _load_yaml(config.config_dir / "org.yml") or {}
    organization = (raw.get("organization") or {}).get("github_organization")
    if organization:
        return str(organization)
    raise MissingGithubOrganization(
        "github_token is set but no github_organization is configured -- set the "
        "GITHUB_ORGANIZATION environment variable, or organization.github_organization "
        f"in {config.config_dir / 'org.yml'}"
    )


# ---------------------------------------------------------------------------
# Workflow engine construction helpers
# ---------------------------------------------------------------------------


def _build_step_registry(
    *,
    uow: UnitOfWork,
    repos: Repositories,
    crypto: CryptoService,
    clock: Clock,
    ssh_identities: Mapping[str, SshIdentity],
    config_dir: Path,
    dns: DnsService | None,
    deployment_audits: DeploymentAuditRepository,
    manifest_resolver: ManifestResolver,
    snapshots: SnapshotService,
) -> StepRegistry:
    """The composition-root verb registry -- the FULL 30-verb DR-0022 catalog,
    complete as of this round's "restore-and-rehydrate" component. Built up
    incrementally, one component at a time: Round 8a's "domain-steps" component
    landed ``cluster.load_spec``/``cluster.store_kubeconfig``
    (``seedpod/engine/steps/cluster.py``); the "infra-and-do" component
    adds ``infra.create_instance``/``infra.await_instance``/
    ``infra.fetch_kubeconfig``/``do.apply_firewalls``/``do.assign_project``
    (``seedpod/engine/steps/infra.py``); the "k3s-family" component adds
    ``k3s.await_ssh``/``k3s.trust_host_keys``/``k3s.install``/``k3s.await_api``/
    ``k3s.fetch_kubeconfig`` (``seedpod/engine/steps/k3s.py``); the "kube-shim"
    component adds ``kube.apply_file``/``kube.await_rollout``
    (``seedpod/engine/steps/kube.py``) -- those fourteen provision-path verbs.
    Round 8b's "destroy" components then add ``infra.destroy_instance``/
    ``cluster.load_infra``/``dns.delete_record``/``kube.cluster_info``/
    ``kube.rollout_undo``/``kube.delete_daemonset``/``kube.wipe_namespace`` --
    the destroy path complete, 23 verbs total. This round's ("Round 10")
    "load-and-plan" component adds three more -- ``deploy.load_audit``/
    ``deploy.plan_waves``/``deploy.prepare_wave`` (``seedpod/engine/steps/
    deploy.py``), the load-and-plan third of the deploy path. Eleven of the
    fourteen provision-path ``infra.*``/``do.*``/``k3s.*``/``kube.*`` verbs take
    no constructor dependencies (stateless ``LateBoundProviderStep``/
    ``ProviderStep`` bindings: every fact they need arrives via ``params``/
    ``ctx.services.providers``, never DI -- see ``k3s.py``'s own module
    docstring: the one fact that isn't provider-local, an SSH identity, now
    arrives as typed ``Params`` threaded from ``cluster.load_spec`` per
    DR-0023, which this function's ``ssh_identities`` keyword supplies). The
    twelfth, ``kube.apply_file``, takes ``config_dir``: it reads a shipped
    template file off disk, and the Round-8a gate's M-2 finding was that
    reading it relative to the process cwd silently regressed v1's
    cwd-independence (see ``engine/steps/kube.py``'s own module docstring).
    ``deploy.load_audit`` is the first verb to need ``deployment_audits``
    (``DeploymentAuditRepository`` -- not part of the Dispatcher-facing
    ``Repositories`` bundle, matching ``DeploymentService``'s own established
    standalone-repository idiom, ``seedpod/app/services/deployment_service.py``'s
    own module docstring) -- a NEW required keyword, per this function's own
    "all keywords are required" discipline below. This round's "apply-and-wait"
    component then adds three more -- ``kube.apply_docs``/
    ``deploy.ensure_rollouts``/``deploy.await_wave`` (``seedpod/engine/steps/
    deploy_apply.py``) -- 29 verbs total; none take constructor dependencies
    (every fact they need arrives via ``params``/``ctx.services.providers``,
    matching the eleven dependency-free provision-path verbs above). This
    round's "restore-and-rehydrate" component adds the LAST verb --
    ``deploy.restore_snapshot`` (``seedpod/engine/steps/deploy_restore.py``)
    -- **30 verbs, the full catalog**. ``deploy.restore_snapshot`` needs two
    NEW dependencies neither prior component's keyword list carried:
    ``manifest_resolver`` (also newly threaded onto ``DeployLoadAudit``, for
    DR-0025 Erratum E2's deploy-time rehydration -- see that Step's own
    docstring) and ``snapshots`` (the ALREADY-BUILT, Round-6 ``SnapshotService``
    -- ``deploy.restore_snapshot`` is its second consumer, ``ClusterService``
    being the first). Not a ``StepRegistry.default()``
    classmethod (as docs/design/seam-d-
    foundation.md Decision 8's illustrative snippet names it) because adding a
    classmethod to the committed ``engine/registry.py`` would itself be an edit
    to committed engine code; this composition-root-local helper achieves the
    same construction-time role without touching that file.

    All keywords are REQUIRED (Round-8a review finding): a ``None``-defaulted
    signature bought "omit one dependency, silently register fewer verbs, and
    find out at ``UnknownVerbError`` mid-provision after a real droplet already
    exists" -- the worst-shaped failure mode available. ``ssh_identities`` gets
    the identical treatment for the identical reason (DR-0023 point 5: "no
    fallback identity may survive"; a silently-wrong SSH identity fails at
    k3s-install time with a confusing error, so a wiring omission belongs at
    boot-time ``TypeError``, not runtime), ``config_dir`` for the third
    instance of it (gate finding M-2: a defaulted ``Path("config")`` is exactly
    the cwd dependence that made ``provision-{kind,orbstack}`` "succeed" with no
    ingress controller), and ``deployment_audits``/``manifest_resolver``/
    ``snapshots`` for the identical reason again -- a defaulted ``None`` would
    let ``deploy.load_audit``/``deploy.restore_snapshot`` register against a
    collaborator that can never actually do anything, discovered only at first
    real deploy. ``build_app()`` (below) is the one
    production call site and already has all these in scope. Test call sites
    (``tests/engine/test_verb_conventions.py``) build a real tmp-sqlite
    ``uow``/``repos``/``CryptoService``/``FrozenClock`` (and a real/fake
    ``ssh_identities`` mapping, and a real ``DeploymentAuditRepository``)
    rather than relying on a zero-arg default.

    Per ``engine/registry.py``'s own docstring ("Construction ... happens once,
    at composition-root build time"), each domain ``Step`` gets its
    repository/crypto/clock/ssh-identity dependencies constructor-injected
    HERE, not via ``ctx.services`` at call time."""
    return StepRegistry(
        {
            LoadSpec.verb: LoadSpec(uow=uow, clusters=repos.clusters, ssh_identities=ssh_identities),
            StoreKubeconfig.verb: StoreKubeconfig(uow=uow, clusters=repos.clusters, crypto=crypto, clock=clock),
            StoreDnsRecord.verb: StoreDnsRecord(uow=uow, clusters=repos.clusters, clock=clock),
            # DR-0040: the 33rd verb. Fires only on an unattended (TTL) destroy --
            # the operator path already snapshots in ClusterService.destroy (DR-0020).
            AutoSnapshot.verb: AutoSnapshot(
                uow=uow, clusters=repos.clusters, snapshots=snapshots, clock=clock
            ),
            LoadInfra.verb: LoadInfra(uow=uow, clusters=repos.clusters),
            LoadKubeconfig.verb: LoadKubeconfig(uow=uow, clusters=repos.clusters, crypto=crypto),
            LoadKubeconfigOptional.verb: LoadKubeconfigOptional(
                uow=uow, clusters=repos.clusters, crypto=crypto
            ),
            InfraCreateInstance.verb: InfraCreateInstance(),
            InfraAwaitInstance.verb: InfraAwaitInstance(),
            InfraFetchKubeconfig.verb: InfraFetchKubeconfig(),
            InfraDestroyInstance.verb: InfraDestroyInstance(),
            DoApplyFirewalls.verb: DoApplyFirewalls(),
            DoAssignToProject.verb: DoAssignToProject(),
            K3sAwaitSsh.verb: K3sAwaitSsh(),
            K3sTrustHostKeys.verb: K3sTrustHostKeys(),
            K3sInstall.verb: K3sInstall(),
            K3sAwaitApi.verb: K3sAwaitApi(),
            K3sFetchKubeconfig.verb: K3sFetchKubeconfig(),
            KubeApplyFile.verb: KubeApplyFile(config_dir=config_dir),
            DnsCreateRecord.verb: DnsCreateRecord(dns=dns),
            DnsDeleteRecord.verb: DnsDeleteRecord(dns=dns),
            KubeClusterInfo.verb: KubeClusterInfo(),
            KubeRolloutUndoStep.verb: KubeRolloutUndoStep(),
            KubeDeleteDaemonset.verb: KubeDeleteDaemonset(),
            KubeWipeNamespace.verb: KubeWipeNamespace(),
            KubeAwaitRollout.verb: KubeAwaitRollout(),
            DeployLoadAudit.verb: DeployLoadAudit(
                uow=uow, deployments=repos.deployments, deployment_audits=deployment_audits,
                clusters=repos.clusters, manifest_resolver=manifest_resolver, config_dir=config_dir,
            ),
            PlanWaves.verb: PlanWaves(),
            DeployPrepareWave.verb: DeployPrepareWave(),
            KubeApplyDocs.verb: KubeApplyDocs(),
            DeployRestoreSnapshot.verb: DeployRestoreSnapshot(snapshots=snapshots, clock=clock),
            DeployEnsureRollouts.verb: DeployEnsureRollouts(),
            DeployAwaitWave.verb: DeployAwaitWave(),
        }
    )


def load_workflow_definitions(directory: Path) -> dict[str, WorkflowDefinition]:
    """Parse (NOT validate -- see module docstring) every ``*.yml`` file under
    ``directory`` into its typed AST, keyed by the definition's own ``workflow``
    name. Wraps ``engine/config.py``'s ``parse_workflow`` -- this module never
    reimplements grammar parsing."""
    definitions: dict[str, WorkflowDefinition] = {}
    for path in sorted(directory.glob("*.yml")):
        wf = parse_workflow(path.read_text())
        definitions[wf.workflow] = wf
    return definitions
