"""tests/engine/test_verb_conventions.py — DR-0022 P2 + P3 enforcement.

DR-0022 (docs/decisions/DR-0022-step-verb-vocabulary.md) pins two
machine-checkable naming/typing laws over the step-verb catalog:

  P2 — "Layer is a typed registry property, not a prefix." Every ``Step``
       declares ``plane: Literal['provider','service','domain']`` and
       ``thin: bool`` truthfully (``thin`` ⇒ the step wraps exactly one Seam C
       command, i.e. is a ``ProviderStep``/``LateBoundProviderStep`` instance).
  P3 — "A verb is named ``<ns>.await_x`` IFF it is ``gateable`` and its
       ``execute()`` is a no-op." Verbs that both actuate and gate keep the
       actuator name; DR-0022 names exactly two such verbs itself:
       ``infra.destroy_instance``, ``kube.delete_daemonset``.

This module runs both laws two ways, per this task's brief ("must not pass
vacuously on an empty registry"):

  1. Over ``tests/engine/declared_verbs.py``'s ``DECLARED_VERBS`` -- THE
     contract every real Step this catalog adds must satisfy. This fixture is
     fully populated today (30 verbs), so these assertions are non-vacuous
     right now, independent of whether any real Step has landed yet.
  2. Over the REAL registry the composition root builds
     (``seedpod.app.factory._build_step_registry()``, exercised here via this
     module's own ``registry`` fixture -- a real tmp-sqlite ``UnitOfWork``/
     ``Repositories``, ``CryptoService``, ``FrozenClock``, matching what
     ``build_app()`` wires). That registry carries the 14 provision-path verbs
     Round 8a landed; the remaining 16 destroy/deploy verbs are later
     components. These assertions are wired so that every verb landed so far,
     and every verb landed later, is checked here for free, with no further
     edits needed to this module. (The empty-registry vacuity this module's own
     docstring used to warn about is exactly the failure mode
     ``_build_step_registry``'s now-required, no-longer-``None``-defaultable
     keywords rule out at the call site -- see its own docstring.)

It also carries THE central proof of Round 8a (gate finding M-1): the shipped
``config/workflows/*.yml`` validate against that same REAL registry, via
``engine/config.py``'s ``validate_workflow`` -- see the last section. Every
other module validates shipped YAML against the ``declared_verbs.py`` FIXTURE
registry, which cannot prove the real catalog matches the files it ships with.

Zero Mock/patch (CLAUDE.md testing posture): the real registry is built via
the real composition-root helper, not a double.
"""

from __future__ import annotations

import contextlib
import re
import types
import typing
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

import pytest
from cryptography.fernet import Fernet
from pydantic import BaseModel, SecretStr

from seedpod.app.factory import _build_step_registry
from seedpod.app.services.snapshot_service import SnapshotService
from seedpod.core.clock import FrozenClock
from seedpod.data.database import Database
from seedpod.data.migrate import migrate
from seedpod.data.repositories import (
    ClusterRepository,
    ClusterStateAuditRepository,
    DeploymentAuditRepository,
    DeploymentRepository,
    DeploymentStateAuditRepository,
    OutboxRepository,
    Repositories,
    SnapshotRepository,
    TimerRepository,
    WorkflowRunRepository,
)
from seedpod.data.uow import UnitOfWork
from seedpod.engine.config import ConfigError, parse_workflow, validate_workflow
from seedpod.engine.provider_step import ProviderStep
from seedpod.engine.registry import StepRegistry
from seedpod.engine.step import StepServices
from seedpod.engine.steps.cluster import SshIdentity
from seedpod.providers.kubectl import KubectlConfig, KubectlProvider
from seedpod.runtime.subprocess_manager import SubprocessManager, TrackedSubprocessRunner
from seedpod.services.crypto import CryptoService
from seedpod.services.manifests import ManifestResolver
from tests.engine.declared_verbs import DECLARED_VERBS

_NOW = datetime(2026, 7, 20, 12, 0, 0, tzinfo=UTC)

REPO_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = REPO_ROOT / "config"
WORKFLOWS_DIR = CONFIG_DIR / "workflows"
SHIPPED_WORKFLOWS = sorted(WORKFLOWS_DIR.glob("*.yml"))

# The workflows whose every verb is registered TODAY. This is the honest
# coverage boundary -- asserted, not assumed, by
# `test_workflows_fully_covered_by_the_real_registry_are_exactly_these`, which
# fails both if coverage silently shrinks and when it grows (a prompt to widen
# this set). Round 10's "restore-and-rehydrate" component lands the LAST of
# the 30 DR-0022 verbs (`deploy.restore_snapshot`), so this set now covers
# EVERY shipped `config/workflows/*.yml` file -- the catalog is complete, not
# merely widened again.
FULLY_REGISTERED_WORKFLOWS = frozenset(
    {
        "provision-digitalocean.yml",
        "provision-kind.yml",
        "provision-orbstack.yml",
        "provision-tart.yml",
        # Round 8b: the first NON-provision workflow to become fully covered --
        # cluster.load_kubeconfig + kube.rollout_undo are both registered now.
        "deploy-rollback.yml",
        # Round 8b, destroy half complete: both destroy paths now validate against
        # the REAL registry, which is what makes a destroy able to finish instead of
        # stranding the cluster in 'destroying' (the 2026-08-02 smoke's worst finding).
        "destroy-cloud.yml",
        "destroy-shared.yml",
        # Round 10, "restore-and-rehydrate" component: deploy.restore_snapshot is
        # the 30th and last verb -- deploy-waves.yml is now fully covered too,
        # closing PARTIY-BACKLOG P0 #0 (a real deployment can reach a cluster).
        "deploy-waves.yml",
    }
)


@pytest.fixture
def registry(tmp_path) -> StepRegistry:
    """The REAL composition-root registry (CLAUDE.md testing posture: no Mock/
    patch anywhere) -- a real tmp-sqlite ``UnitOfWork``/``Repositories``, a real
    ``CryptoService`` (Fernet), and ``FrozenClock``, exactly the shape
    ``build_app()`` wires. Fixes a Round-8a review finding: every law in this
    module used to call ``_build_step_registry()`` with NO arguments, which
    (now that the function's dependencies are required, not ``None``-defaulted)
    would be a ``TypeError`` -- and previously silently returned an EMPTY
    registry, so every registry-side law here checked nothing. Building a real
    registry means the two landed verbs (``cluster.load_spec``/
    ``cluster.store_kubeconfig``) are actually exercised by every law below."""
    database = Database(f"sqlite:///{tmp_path / 'verb_conventions.db'}")
    migrate(database.engine)
    uow = UnitOfWork(database)
    repos = Repositories(
        clusters=ClusterRepository(),
        deployments=DeploymentRepository(),
        cluster_state_audits=ClusterStateAuditRepository(),
        deployment_state_audits=DeploymentStateAuditRepository(),
        timers=TimerRepository(),
        outbox=OutboxRepository(),
        workflow_runs=WorkflowRunRepository(),
    )
    crypto = CryptoService(dev_key=Fernet.generate_key(), prod_key=Fernet.generate_key())
    clock = FrozenClock(_NOW)
    ssh_identities = {
        "digitalocean": SshIdentity(user="root", private_key_path="/home/test/.ssh/id_exampleco_testing"),
        "tart": SshIdentity(user="admin", private_key_path="/home/test/.ssh/id_ed25519"),
    }
    # `deploy.restore_snapshot`'s two new dependencies (restore-and-rehydrate
    # component): a real `ManifestResolver` (no GHCR token -- construction is
    # IO-free either way) and a real `SnapshotService`, built the same shape
    # `app/factory.py::build_app` does (a real `KubectlProvider` over a real,
    # never-actually-dialled `SubprocessManager` -- no test here issues a live
    # kubectl call through it).
    manifest_resolver = ManifestResolver(ghcr_service=None)
    kubectl_provider = KubectlProvider(KubectlConfig(), TrackedSubprocessRunner(SubprocessManager()), clock=clock)
    snapshots = SnapshotService(
        SnapshotRepository(), repos, repos.deployments, crypto, kubectl_provider, uow, clock,
        lambda: "snapshot-id", tmp_path, tmp_path / "snapshots",
    )
    return _build_step_registry(
        uow=uow,
        repos=repos,
        crypto=crypto,
        clock=clock,
        ssh_identities=ssh_identities,
        config_dir=CONFIG_DIR,
        dns=None,  # no Cloudflare token in tests; dns.delete_record still registers and no-ops
        deployment_audits=DeploymentAuditRepository(crypto),
        manifest_resolver=manifest_resolver,
        snapshots=snapshots,
    )

# The DR-0022 table, transcribed verbatim (32 verbs, DR-0034 included -- the
# authoritative name list; see the DR's "verb table" section). Any name here
# that later drops out of DECLARED_VERBS, or any registry key that isn't a
# member of this set, is a coherence bug this test must catch.
DR_0022_VERBS = frozenset(
    {
        "infra.create_instance",
        "infra.await_instance",
        "infra.fetch_kubeconfig",
        "infra.destroy_instance",
        "k3s.await_ssh",
        "k3s.trust_host_keys",
        "k3s.install",
        "k3s.await_api",
        "k3s.fetch_kubeconfig",
        "kube.cluster_info",
        "kube.apply_docs",
        "kube.apply_file",
        "kube.await_rollout",
        "kube.rollout_undo",
        "kube.delete_daemonset",
        "kube.wipe_namespace",
        "deploy.load_audit",
        "deploy.plan_waves",
        "deploy.prepare_wave",
        "deploy.restore_snapshot",
        "deploy.ensure_rollouts",
        "deploy.await_wave",
        "cluster.load_spec",
        "cluster.load_infra",
        "cluster.load_kubeconfig",
        "cluster.load_kubeconfig_optional",
        "cluster.store_kubeconfig",
        # DR-0034 amends this catalog 30 -> 32: `dns.create_record` (the half the
        # vocabulary never had, backlog #22) and `cluster.store_dns_record` (which
        # writes the three columns that make `cluster.load_infra` -> the already
        # -shipped `dns.delete_record` delete a REAL record, backlog #6).
        "dns.create_record",
        "cluster.store_dns_record",
        # DR-0040 amends this catalog 32 -> 33: `cluster.auto_snapshot`, which honours
        # the profile's own auto_snapshot block on an UNATTENDED destroy. Three shipped
        # profiles had declared that block since Phase 0 and nothing had ever read it.
        "cluster.auto_snapshot",
        "dns.delete_record",
        "do.apply_firewalls",
        "do.assign_project",
    }
)

# P3's own named exceptions: verbs that both actuate and gate, so they keep
# the actuator name rather than an `await_` name.
ACTUATE_AND_GATE_VERBS = frozenset({"infra.destroy_instance", "kube.delete_daemonset"})

AWAIT_NAME_PATTERN = re.compile(r"^[a-z0-9_]+\.await_[a-z0-9_]+$")


# ---------------------------------------------------------------------------
# Name-set check against the DR table (non-vacuous today: DECLARED_VERBS is a
# populated, real fixture; also runs over the real registry for free later).
# ---------------------------------------------------------------------------


def test_dr_0022_catalog_is_exactly_33_verbs():
    """Erratum E1: "the catalog is 30 verbs, not 31" -- the DR's table is
    correct and complete; the prose count was an arithmetic slip. **DR-0034
    amends it to 32**, adding `dns.create_record` and
    `cluster.store_dns_record`; **DR-0040 amends it to 33**, adding
    `cluster.auto_snapshot`. Asserted
    directly (not just via the set-equality check below) so a future edit
    that silently drops or duplicates an entry in either set is caught even
    if it happens to preserve set equality by coincidence."""
    assert len(DR_0022_VERBS) == 33
    assert len(DECLARED_VERBS) == 33


def test_declared_verbs_are_exactly_the_dr_0022_table():
    assert set(DECLARED_VERBS) == DR_0022_VERBS


def test_registry_verbs_are_all_dr_0022_names(registry):
    for verb in registry.verbs():
        assert verb in DR_0022_VERBS, f"{verb!r} is not a DR-0022 verb name"


def test_registry_verb_set_is_exactly_the_dr_0022_catalog(registry):
    """DR-0022 Erratum E11's non-vacuous completeness gate, FLIPPED from
    ``xfail`` to a hard assertion: Round 10's "restore-and-rehydrate"
    component lands the 30th and last verb (``deploy.restore_snapshot``), so
    the real composition-root registry now carries the FULL catalog, not a
    partial one. If this ever regresses (a verb silently drops out of
    ``_build_step_registry``), this is the test that notices -- the subset
    check above (``test_registry_verbs_are_all_dr_0022_names``) would not,
    since it passes on any partial catalog too."""
    assert set(registry.verbs()) == DR_0022_VERBS


# ---------------------------------------------------------------------------
# P3 — await_ prefix <=> gateable pure gate (with the two named exceptions).
# ---------------------------------------------------------------------------


def test_declared_verbs_await_prefix_implies_gateable():
    """The checkable half from the fixture alone: every `<ns>.await_x` name
    must be `gateable=True` (VerbFixture carries no `execute()` to inspect for
    the no-op half; that half is enforced below, over real Step instances)."""
    for name, fixture in DECLARED_VERBS.items():
        if AWAIT_NAME_PATTERN.match(name):
            assert fixture.gateable, f"{name}: await_-named verb must be gateable (DR-0022 P3)"


def test_declared_verbs_gateable_non_await_are_the_named_actuate_and_gate_exceptions():
    """The converse: every gateable verb that is NOT await_-named must be one
    of DR-0022's own two named actuate-and-gate exceptions -- no silent third
    exception, and no vocabulary drift back toward the pre-DR-0022 mixing of
    conditionals/heuristics into names."""
    for name, fixture in DECLARED_VERBS.items():
        if fixture.gateable and not AWAIT_NAME_PATTERN.match(name):
            assert name in ACTUATE_AND_GATE_VERBS, (
                f"{name}: gateable, not await_-named, and not a documented "
                f"actuate-and-gate exception (DR-0022 P3) -- expected one of "
                f"{sorted(ACTUATE_AND_GATE_VERBS)}"
            )


class _RecordingProviders(Mapping):
    """Hand-written fake provider registry (CLAUDE.md testing posture: no
    Mock/patch anywhere -- matching the conformance suite's own fake-transport
    style), standing in for ``ctx.services.providers``. Records every key
    looked up and then raises ``KeyError`` immediately -- a true no-op
    ``execute()`` never reaches this far at all, so what matters is whether
    ``__getitem__`` was called, not what it would have returned."""

    def __init__(self) -> None:
        self.accessed: list[str] = []

    def __getitem__(self, key: str) -> object:
        self.accessed.append(key)
        raise KeyError(key)

    def __iter__(self):
        return iter(())

    def __len__(self) -> int:
        return 0


def _dummy_value_for(annotation: object) -> object:
    """Best-effort type-plausible dummy value for an arbitrary pydantic field
    annotation -- just enough that a real (actuating) ``execute()`` reading
    ``params.<field>`` never raises ``AttributeError`` before it reaches
    ``ctx.services.providers[...]`` (see ``_dummy_params`` below). Values are
    NOT constraint-valid (a ``ge=1`` int field gets ``0``) -- that's fine,
    ``model_construct`` never validates, and this helper only needs attribute
    *presence*, never semantic correctness."""
    origin = typing.get_origin(annotation)
    args = typing.get_args(annotation)
    if origin is typing.Union or origin is types.UnionType:
        non_none = [a for a in args if a is not type(None)]
        return _dummy_value_for(non_none[0]) if non_none else None
    if annotation is str:
        return "x"
    if annotation is int:
        return 0
    if annotation is float:
        return 0.0
    if annotation is bool:
        return False
    if isinstance(annotation, type) and issubclass(annotation, SecretStr):
        return SecretStr("x")
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return _dummy_params(annotation)
    if origin is list or (isinstance(annotation, type) and issubclass(annotation, list)):
        return []
    if origin is Mapping or origin is dict or (isinstance(annotation, type) and issubclass(annotation, Mapping)):
        return {}
    return None


def _dummy_params(model_cls: type[BaseModel]) -> BaseModel:
    """Build a ``Params`` (or nested ``BaseModel``) instance with every
    REQUIRED field set to a type-plausible dummy value via ``model_construct``
    (skips validation -- only attribute *presence* matters here).

    Fixes a Round-8a review finding: ``step.Params.model_construct()`` with no
    kwargs left every required field genuinely UNSET, so an actuating
    ``execute()`` reading e.g. ``params.provider`` (the exact shape of every
    late-bound ``infra.*`` verb, DR-0022 ruling 1) raised ``AttributeError``
    *before* ever reaching the provider dict lookup -- and
    ``contextlib.suppress(Exception)`` swallowed that, so
    ``providers.accessed`` stayed empty and the verb false-passed as a no-op.
    Populating required fields (however implausible their values) lets an
    actuating ``execute()`` run far enough to actually touch
    ``ctx.services.providers``, which is the only thing this check cares
    about."""
    kwargs = {}
    for name, field_info in model_cls.model_fields.items():
        if field_info.is_required():
            kwargs[name] = _dummy_value_for(field_info.annotation)
    return model_cls.model_construct(**kwargs)


async def _execute_emits_no_command(step) -> bool:
    """DR-0022 P3 / Erratum E4b's machine-checkable definition of "``execute()``
    is a no-op": **"execute emits no Seam C command"** -- it returns a
    provisional Output and invokes no provider. Verified by actually DRIVING
    the real ``execute()`` (never a source-text grep, which "passes for any
    execute touching repos, the clock, or the DB" per the Erratum) against a
    real ``StepContext`` whose provider registry is ``_RecordingProviders``
    above: params are built with every required field populated
    (``_dummy_params``) so an actuating ``execute()`` runs far enough to reach
    the provider lookup rather than raising ``AttributeError`` on an unset
    required field first, and any exception raised along the way that ISN'T
    the recorder's own probe is irrelevant to this question -- only whether
    ``providers.accessed`` ended up non-empty."""
    from tests.engine.fakes import FakeSubprocessManager, make_step_context

    providers = _RecordingProviders()
    ctx = make_step_context(services=StepServices(subprocess_manager=FakeSubprocessManager(), providers=providers))
    params = _dummy_params(step.Params)
    with contextlib.suppress(Exception):
        await step.execute(params, ctx)
    return not providers.accessed


# Self-check: since the real registry is honestly empty until Round 8b lands
# real Steps, `test_registry_await_named_steps_are_gateable_with_noop_execute`
# below currently iterates zero times -- so THIS test proves the mechanism
# itself is correct (catches both directions) independent of the registry,
# using two minimal local fakes rather than a real verb.
async def test_execute_emits_no_command_detects_both_directions():
    from seedpod.engine.step import EmptyOutput, Step

    class _Params(BaseModel):
        # REQUIRED, no default -- the exact shape of AwaitInstanceParams/
        # DestroyInstanceParams (DR-0022 ruling 1's late-bound family). A
        # defaulted field here would never exercise the required-field path
        # this self-check exists to prove is handled (see _dummy_params).
        provider: str

    class _NoopGate(Step[_Params, EmptyOutput]):
        verb = "test.noop_gate"
        Params = _Params
        Output = EmptyOutput
        gateable = True

        async def execute(self, params: _Params, ctx) -> EmptyOutput:
            return EmptyOutput()  # true no-op: never touches ctx.services.providers

    class _ActuatingGate(Step[_Params, EmptyOutput]):
        verb = "test.actuating_gate"
        Params = _Params
        Output = EmptyOutput
        gateable = True

        async def execute(self, params: _Params, ctx) -> EmptyOutput:
            ctx.services.providers[params.provider]  # emits a (fake) Seam C command
            return EmptyOutput()

    assert await _execute_emits_no_command(_NoopGate()) is True
    assert await _execute_emits_no_command(_ActuatingGate()) is False


async def test_registry_await_named_steps_are_gateable_with_noop_execute(registry):
    for verb in registry.verbs():
        if not AWAIT_NAME_PATTERN.match(verb):
            continue
        step = registry.get(verb)
        assert step.gateable, f"{verb}: await_-named step must be gateable (DR-0022 P3)"
        assert await _execute_emits_no_command(step), (
            f"{verb}: await_-named step's execute() must emit no Seam C command (DR-0022 P3/E4b)"
        )


def test_registry_gateable_non_await_steps_are_named_exceptions(registry):
    for verb in registry.verbs():
        step = registry.get(verb)
        if step.gateable and not AWAIT_NAME_PATTERN.match(verb):
            assert verb in ACTUATE_AND_GATE_VERBS, (
                f"{verb}: gateable, not await_-named, and not a documented actuate-and-gate exception"
            )


# ---------------------------------------------------------------------------
# P2 — plane/thin truthfulness (real registry only: VerbFixture carries
# neither ClassVar, since they are Step-instance properties, not part of the
# YAML-facing Params/Output/gateable/undoable contract config.py validates).
# ---------------------------------------------------------------------------


def test_registry_steps_declare_a_valid_plane(registry):
    for verb in registry.verbs():
        step = registry.get(verb)
        assert step.plane in {"provider", "service", "domain"}, f"{verb}: invalid plane {step.plane!r}"


def test_registry_thin_steps_wrap_exactly_one_seam_c_command(registry):
    """thin ⇒ exactly one Seam C command ⇒ the step IS a ProviderStep (the
    one mechanism in this codebase that maps one Params to one
    ProviderCommand via `command()`); thin steps must also declare
    plane='provider' (P2: layer is typed, and thin only ever applies to the
    provider plane -- domain/service steps never wrap a Seam C command)."""
    for verb in registry.verbs():
        step = registry.get(verb)
        if step.thin:
            assert isinstance(step, ProviderStep), f"{verb}: thin=True but not a ProviderStep"
            assert step.plane == "provider", f"{verb}: thin=True but plane={step.plane!r}, not 'provider'"


def test_declared_verbs_plane_and_thin_match_the_dr_0022_table():
    """The value-level half of P2, over the fixture (non-vacuous today, unlike
    the registry-only checks above which can only ever assert internal
    consistency -- e.g. thin => ProviderStep -- never the EXPECTED plane/thin
    per verb, and iterate zero times on a partial/empty registry). Every
    fixture entry's plane/thin is transcribed straight from DR-0022's verb
    table (see declared_verbs.py's own per-row comment); this test exists so
    a future edit to that transcription (e.g. Round 8b silently declaring
    `kube.wipe_namespace` thin=True, or `dns.delete_record` plane='domain')
    is caught even though it can't touch the registry-side checks."""
    for name, fixture in DECLARED_VERBS.items():
        assert fixture.plane in {"provider", "service", "domain"}, f"{name}: invalid plane {fixture.plane!r}"
        if fixture.thin:
            assert fixture.plane == "provider", f"{name}: thin=True but plane={fixture.plane!r}, not 'provider'"
    assert DECLARED_VERBS["dns.delete_record"].plane == "service", "P1: dns. is the one service-plane verb"
    for name in ("cluster.load_spec", "deploy.load_audit", "deploy.plan_waves", "deploy.restore_snapshot"):
        assert DECLARED_VERBS[name].plane == "domain", f"{name}: expected plane='domain'"
        assert DECLARED_VERBS[name].thin is False, f"{name}: domain-plane verbs are never thin"
    # The table's own "composite"/"composite gate"/"delete + absence probe"/
    # "DestroyInstance + ProbeDestruction" verbs stay plane='provider' (they
    # are ProviderSteps issuing N Seam C commands, Erratum E12) but are not
    # thin (not exactly one command).
    for name in (
        "infra.destroy_instance",
        "kube.delete_daemonset",
        "kube.wipe_namespace",
        "deploy.prepare_wave",
        "deploy.ensure_rollouts",
        "deploy.await_wave",
    ):
        assert DECLARED_VERBS[name].plane == "provider", f"{name}: expected plane='provider'"
        assert DECLARED_VERBS[name].thin is False, f"{name}: composite provider verb must not be thin"


# ---------------------------------------------------------------------------
# Erratum E4c — registry <-> declared_verbs reconciliation. Because
# ``ProviderStep`` hard-defaults ``undoable = True``, ruling 3's D1 fix (making
# a landmine unrepresentable for ``kube.apply_docs``) depends on Round 8b's
# concrete Steps actually opting OUT where the fixture says ``undoable=False``
# -- this test makes ``DECLARED_VERBS`` literal and load-bearing rather than
# aspirational documentation, for every flag on every verb the registry has
# actually implemented so far. Extended (Round-8a review finding) to
# reconcile `plane`/`thin` too, closing the identical landmine shape this
# erratum already closed for `undoable`: a Round-8b verb declaring
# `thin=True` on a composite, or the wrong `plane`, would otherwise pass
# silently forever (P2's registry-only checks above can't catch it, since
# they only assert internal consistency, never the table's EXPECTED value).
# Further extended (Round-8a "infra-and-do" review finding) to reconcile
# `idempotent` -- `ProviderStep` never overrides `Step`'s `idempotent = True`
# default, so a Round-8b verb landing at the wrong value (most acutely, this
# round's own `infra.create_instance` -- seam-b-engine.md's one pinned
# non-idempotent verb) would otherwise drift silently too.
# ---------------------------------------------------------------------------


def test_registry_gateable_and_undoable_flags_match_declared_verbs_fixture(registry):
    for verb in registry.verbs():
        assert verb in DECLARED_VERBS, f"{verb}: registered but not in DECLARED_VERBS (DR-0022 fixture)"
        step = registry.get(verb)
        fixture = DECLARED_VERBS[verb]
        assert step.gateable == fixture.gateable, (
            f"{verb}: Step.gateable={step.gateable!r} but DECLARED_VERBS says {fixture.gateable!r}"
        )
        assert step.undoable == fixture.undoable, (
            f"{verb}: Step.undoable={step.undoable!r} but DECLARED_VERBS says {fixture.undoable!r}"
        )
        assert step.plane == fixture.plane, (
            f"{verb}: Step.plane={step.plane!r} but DECLARED_VERBS says {fixture.plane!r}"
        )
        assert step.thin == fixture.thin, f"{verb}: Step.thin={step.thin!r} but DECLARED_VERBS says {fixture.thin!r}"
        assert step.idempotent == fixture.idempotent, (
            f"{verb}: Step.idempotent={step.idempotent!r} but DECLARED_VERBS says {fixture.idempotent!r}"
        )


def test_registry_params_and_output_field_sets_match_declared_verbs_fixture(registry):
    """E4c, extended (Round-8a review finding): nothing previously reconciled a
    real Step's ``Params``/``Output`` field SET against ``DECLARED_VERBS``' own
    (possibly-inferred-stand-in) model for the same verb -- a real ``Step``
    silently dropping or renaming a YAML-bindable field relative to the fixture
    contract would pass every other check in this module. Field NAMES only
    (never types): several fixture models are this task's own honest inference
    (module docstring's TODO markers) and are expected to narrow types once a
    real salvaged DTO lands, but the set of names workflow YAML can bind
    to/from must not silently drift."""
    for verb in registry.verbs():
        step = registry.get(verb)
        fixture = DECLARED_VERBS[verb]
        assert set(step.Params.model_fields) == set(fixture.Params.model_fields), (
            f"{verb}: Step.Params fields {sorted(step.Params.model_fields)} != "
            f"DECLARED_VERBS' {sorted(fixture.Params.model_fields)}"
        )
        assert set(step.Output.model_fields) == set(fixture.Output.model_fields), (
            f"{verb}: Step.Output fields {sorted(step.Output.model_fields)} != "
            f"DECLARED_VERBS' {sorted(fixture.Output.model_fields)}"
        )


# ---------------------------------------------------------------------------
# THE CENTRAL PROOF (Round-8a gate finding M-1): the shipped workflow YAML
# validates against the REAL composition-root registry -- not against the
# `tests/engine/declared_verbs.py` fixture.
#
# Before this, `StepRegistry` implemented only half of engine/config.py's
# `RegistryView` (no `resolve_type`), so `validate_workflow(wf, real_registry)`
# raised AttributeError before reaching V1 and was IMPOSSIBLE to call in
# production; `tests/engine/test_shipped_workflows.py` validated the same files
# against the fixture registry instead. The two agreed -- but nothing enforced
# that, so nothing in CI proved the shipped YAML matched the real verb catalog,
# which is precisely what Round 8a delivers. These tests close that gap.
# ---------------------------------------------------------------------------


def _verbs_used(wf) -> set[str]:
    """Every verb a workflow names, flattening `foreach` bodies (mirrors
    test_shipped_workflows.py's own `_all_steps`)."""
    verbs: set[str] = set()
    for entry in wf.steps:
        body = entry.body if hasattr(entry, "body") else (entry,)
        verbs.update(step.uses for step in body)
    return verbs


def test_workflows_fully_covered_by_the_real_registry_are_exactly_these(registry):
    """Pins the coverage boundary itself, so the parametrized validation below can
    never quietly become vacuous. Fails if a landed verb regresses out of the
    registry (set shrinks) and fires as a reminder to widen the set when the
    destroy/deploy components land (set grows)."""
    registered = set(registry.verbs())
    covered = {p.name for p in SHIPPED_WORKFLOWS if _verbs_used(parse_workflow(p.read_text())) <= registered}
    assert covered == set(FULLY_REGISTERED_WORKFLOWS)


@pytest.mark.parametrize(
    "path",
    [p for p in SHIPPED_WORKFLOWS if p.name in FULLY_REGISTERED_WORKFLOWS],
    ids=lambda p: p.name,
)
def test_shipped_workflow_validates_against_the_real_registry(path, registry):
    """V1-V6 against the real `StepRegistry`: every verb registered, every `with:`
    key on the real Params, every binding's real type assignable, gates only on
    really-gateable verbs. This is the load-bearing assertion that the real verb
    catalog and the shipped YAML actually match."""
    validate_workflow(parse_workflow(path.read_text()), registry)  # raises on first violation


def test_real_registry_resolves_the_shipped_workflows_input_types(registry):
    """`resolve_type` is the half of RegistryView that was missing. Every `inputs:`
    type name in every shipped file must resolve against the REAL registry --
    an unresolvable one is a V-rule failure at load, not an AttributeError."""
    for path in SHIPPED_WORKFLOWS:
        wf = parse_workflow(path.read_text())
        for name, input_def in wf.inputs.items():
            assert registry.resolve_type(input_def.type) is not None, (
                f"{path.name}: input {name!r} declares type {input_def.type!r}, "
                f"which the real StepRegistry cannot resolve"
            )


# --- non-vacuity: the real-registry validation must reject real mutations -----


def _digitalocean_yaml() -> str:
    return (WORKFLOWS_DIR / "provision-digitalocean.yml").read_text()


@pytest.mark.parametrize(
    ("mutation", "expected_rule"),
    [
        # V1 -- verb not in the real catalog.
        (lambda t: t.replace("uses: cluster.load_spec", "uses: cluster.load_spex"), "V1"),
        # V2 -- a `with:` key that is not a field on the real Params.
        (lambda t: t.replace("with: {cluster_id: {from: run.cluster_id}}", "with: {bogus: 1}"), "V2"),
        # V4 -- a binding whose real upstream type is not assignable to the real Params field.
        (lambda t: t.replace("with: {host: {from: droplet.address}}", "with: {host: {from: spec.spec}}"), "V4"),
        # V6 -- a gate on a verb the real registry says is not gateable.
        (
            lambda t: t.replace(
                "    timeout_seconds: 300              # install_timeout_seconds",
                "    gate: {timeout_seconds: 300, interval_seconds: 10}",
            ),
            "V6",
        ),
    ],
    ids=["V1-unknown-verb", "V2-unknown-with-key", "V4-type-mismatch", "V6-gate-on-non-gateable"],
)
def test_real_registry_validation_rejects_mutations(registry, mutation, expected_rule):
    """Proves the assertions above are load-bearing rather than a rubber stamp:
    each mutation of a file that passes today must fail against the REAL registry,
    on the expected rule."""
    mutated = mutation(_digitalocean_yaml())
    assert mutated != _digitalocean_yaml(), "mutation did not apply -- the anchor text has drifted"

    with pytest.raises(ConfigError) as exc_info:
        validate_workflow(parse_workflow(mutated), registry)
    assert exc_info.value.rule == expected_rule
