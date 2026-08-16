"""``seedpod/engine/steps/cluster.py`` -- the ``cluster.*`` domain steps
(``load_spec``, ``store_kubeconfig``, ``load_infra``, both kubeconfig loaders; DR-0022,
plus ``store_dns_record`` and ``load_spec``'s ``dns_intent`` per DR-0034), against a real
tmp SQLite DB (``migrate()``), a real ``UnitOfWork``, a real ``CryptoService``
(Fernet), and ``FrozenClock``. No Mock/patch anywhere (CLAUDE.md testing
posture).

Covers:
- ``cluster.load_spec`` produces exactly the fields the provision YAMLs bind
  (``spec: ClusterSpecification``, ``provider: str``), and its pod/service
  CIDRs are BIT-IDENTICAL to a direct call of the committed
  ``allocate_cluster_cidrs`` for the same cluster uuid (Tailscale-critical).
- ``cluster.store_kubeconfig`` round-trips a kubeconfig through Fernet (the
  row's ``encrypted_kubeconfig``/``kubeconfig_key_class`` decrypt back to the
  original plaintext) and its returned ``kubeconfig_ref`` is the documented
  opaque handle shape.
- The kubeconfig plaintext never appears anywhere in cleartext: not in the
  stored row's ciphertext column, and not in any ``ctx.note``/``ctx.progress``
  call either step makes (both record sinks are inspected directly).
- Both steps raise ``PermanentError`` (never a bare exception a step contract
  wouldn't recognize, never a silent no-op) for an unknown ``cluster_id``.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from cryptography.fernet import Fernet
from sqlalchemy import text

from seedpod.core.clock import FrozenClock
from seedpod.core.cluster_spec import allocate_cluster_cidrs
from seedpod.core.dns_record import DnsIntent, DnsRecordRef
from seedpod.core.errors import PermanentError
from seedpod.core.records import Origin
from seedpod.data.database import Database
from seedpod.data.migrate import migrate
from seedpod.data.repositories import ClusterRepository, ClusterRow
from seedpod.data.uow import UnitOfWork
from seedpod.engine.steps.cluster import (
    AutoSnapshot,
    AutoSnapshotParams,
    LoadInfra,
    LoadInfraParams,
    LoadKubeconfig,
    LoadKubeconfigOptional,
    LoadSpec,
    LoadSpecParams,
    SshIdentity,
    StoreDnsRecord,
    StoreDnsRecordParams,
    StoreKubeconfig,
    StoreKubeconfigParams,
)
from seedpod.services.crypto import CryptoService
from tests.engine.fakes import RecordingNoteSink, RecordingProgressSink, make_step_context

NOW = datetime(2026, 8, 1, 12, 0, 0, tzinfo=UTC)

PROVIDER_CONFIG = {
    "node_specification": {"cpu_cores": 2, "memory_gb": 4, "region_hint": "europe-west"},
    "cluster_config": {
        "node_count": 1,
        "ttl_hours": 4,
        "tags": ["ephemeral", "exampleco"],
        "kubernetes_version": "stable",
    },
}


@pytest.fixture
def clock() -> FrozenClock:
    return FrozenClock(NOW)


@pytest.fixture
def db(tmp_path):
    database = Database(f"sqlite:///{tmp_path / 'domain_steps.db'}")
    migrate(database.engine)
    return database


@pytest.fixture
def uow(db) -> UnitOfWork:
    return UnitOfWork(db)


@pytest.fixture
def clusters() -> ClusterRepository:
    return ClusterRepository()


@pytest.fixture
def crypto() -> CryptoService:
    return CryptoService(dev_key=Fernet.generate_key(), prod_key=Fernet.generate_key())


# DR-0023: v1's own per-provider values (digitalocean = root/~/.ssh/id_exampleco_testing;
# tart = admin/~/.ssh/id_ed25519), pre-expanded exactly as `_ssh_identities()` would.
SSH_IDENTITIES = {
    "digitalocean": SshIdentity(user="root", private_key_path="/home/test/.ssh/id_exampleco_testing"),
    "tart": SshIdentity(user="admin", private_key_path="/home/test/.ssh/id_ed25519"),
}


def _birth_row(
    cluster_id: str,
    *,
    provider: str = "digitalocean",
    environment: str = "ephemeral",
    provider_config: dict | None = None,
    provider_resources: dict | None = None,
    encrypted_kubeconfig: str | None = None,
    kubeconfig_key_class: str | None = None,
    dns_hostname: str | None = None,
    dns_zone: str | None = None,
    dns_record_id: str | None = None,
) -> ClusterRow:
    return ClusterRow(
        id=cluster_id, name=cluster_id, slug=cluster_id, origin=Origin.MANAGED, environment=environment,
        repository="exampleco-core", branch="feature/x", status="provisioning", pre_destroy_state=None, version=0,
        provider=provider, provider_config=provider_config if provider_config is not None else dict(PROVIDER_CONFIG),
        provider_resources=provider_resources or {}, dns_hostname=dns_hostname, dns_zone=dns_zone,
        dns_record_id=dns_record_id, public_ip=None, node_count=1,
        encrypted_kubeconfig=encrypted_kubeconfig, kubeconfig_key_class=kubeconfig_key_class,
        kubeconfig_ref=None, cost_per_hour=0.0,
        total_cost=0.0, consecutive_health_failures=0, failure_reason=None, last_reconciled_at=None,
        created_at=NOW, updated_at=NOW, expires_at=None,
    )


async def _insert(uow: UnitOfWork, clusters: ClusterRepository, row: ClusterRow) -> None:
    async with uow() as tx:
        clusters.insert(tx, row)


# ---------------------------------------------------------------------------
# cluster.load_spec
# ---------------------------------------------------------------------------


def test_load_spec_declares_the_dr_0022_contract():
    step = LoadSpec(uow=object(), clusters=object(), ssh_identities={})  # construction only -- no IO
    assert step.verb == "cluster.load_spec"
    assert step.plane == "domain"
    assert step.thin is False
    assert step.gateable is False
    assert step.undoable is False


async def test_load_spec_produces_spec_and_provider(uow, clusters):
    await _insert(uow, clusters, _birth_row("c1", provider="digitalocean"))
    step = LoadSpec(uow=uow, clusters=clusters, ssh_identities=SSH_IDENTITIES)
    ctx = make_step_context(cluster_id="c1")

    output = await step.execute(LoadSpecParams(cluster_id="c1"), ctx)

    assert output.provider == "digitalocean"
    assert output.spec.node_specification.cpu_cores == 2
    assert output.spec.node_specification.memory_gb == 4
    assert output.spec.node_specification.region_hint == "europe-west"
    assert output.spec.cluster_config.node_count == 1
    assert output.spec.cluster_config.ttl_hours == 4
    assert output.spec.cluster_config.tags == ["ephemeral", "exampleco"]
    # DR-0023: v1's digitalocean identity, threaded through unchanged.
    assert output.ssh_user == "root"
    assert output.ssh_private_key_path == "/home/test/.ssh/id_exampleco_testing"


async def test_load_spec_threads_the_tart_ssh_identity(uow, clusters):
    """DR-0023: v1 used a genuinely DIFFERENT identity per provider -- tart's
    must not be digitalocean's, and vice versa."""
    await _insert(uow, clusters, _birth_row("c-tart", provider="tart"))
    step = LoadSpec(uow=uow, clusters=clusters, ssh_identities=SSH_IDENTITIES)

    output = await step.execute(LoadSpecParams(cluster_id="c-tart"), make_step_context(cluster_id="c-tart"))

    assert output.ssh_user == "admin"
    assert output.ssh_private_key_path == "/home/test/.ssh/id_ed25519"


@pytest.mark.parametrize("provider", ["kind", "orbstack"])
async def test_load_spec_ssh_identity_is_none_for_providers_with_no_ssh_plane(uow, clusters, provider):
    """DR-0023 point 5: kind/orbstack have no SSH plane -- both fields are
    `None`, never a fallback identity. (Erratum E1 retracts decision 1's claim
    that this makes binding them into a k3s.* verb a type error; what actually
    prevents a silently-wrong credential is `k3s.py`'s `_target()` raising a
    loud PermanentError on a None.)"""
    await _insert(uow, clusters, _birth_row(f"c-{provider}", provider=provider))
    step = LoadSpec(uow=uow, clusters=clusters, ssh_identities=SSH_IDENTITIES)

    output = await step.execute(
        LoadSpecParams(cluster_id=f"c-{provider}"), make_step_context(cluster_id=f"c-{provider}")
    )

    assert output.ssh_user is None
    assert output.ssh_private_key_path is None


async def test_load_spec_ssh_identity_is_none_for_a_provider_absent_from_the_mapping(uow, clusters):
    """A provider the composition root never populated (not just kind/orbstack
    explicitly) must still resolve to (None, None), not raise -- `LoadSpec`
    never invents a fallback identity (DR-0023 point 5)."""
    await _insert(uow, clusters, _birth_row("c-fake", provider="fake"))
    step = LoadSpec(uow=uow, clusters=clusters, ssh_identities=SSH_IDENTITIES)

    output = await step.execute(LoadSpecParams(cluster_id="c-fake"), make_step_context(cluster_id="c-fake"))

    assert output.ssh_user is None
    assert output.ssh_private_key_path is None


async def test_load_spec_cidrs_are_bit_identical_to_the_committed_allocator(uow, clusters):
    """Tailscale-critical: the step must CALL allocate_cluster_cidrs, never
    reimplement or drift from it."""
    cluster_id = "3c8cf9ed-8229-45b1-a188-7cdcd726fe02"
    await _insert(uow, clusters, _birth_row(cluster_id))
    step = LoadSpec(uow=uow, clusters=clusters, ssh_identities=SSH_IDENTITIES)
    ctx = make_step_context(cluster_id=cluster_id)

    output = await step.execute(LoadSpecParams(cluster_id=cluster_id), ctx)

    expected_pod_cidr, expected_service_cidr = allocate_cluster_cidrs(cluster_id)
    assert output.spec.cluster_config.pod_cidr == expected_pod_cidr
    assert output.spec.cluster_config.service_cidr == expected_service_cidr


async def test_load_spec_cidrs_vary_deterministically_with_cluster_id(uow, clusters):
    ids = ["aaaaaaaa-0000-0000-0000-000000000000", "bbbbbbbb-0000-0000-0000-000000000000", "cccccccc-0000-0000-0000-000000000000"]
    for cid in ids:
        await _insert(uow, clusters, _birth_row(cid))
    step = LoadSpec(uow=uow, clusters=clusters, ssh_identities=SSH_IDENTITIES)

    seen = set()
    for cid in ids:
        output = await step.execute(LoadSpecParams(cluster_id=cid), make_step_context(cluster_id=cid))
        expected = allocate_cluster_cidrs(cid)
        assert (output.spec.cluster_config.pod_cidr, output.spec.cluster_config.service_cidr) == expected
        seen.add(expected)
    assert len(seen) == len(ids)  # distinct cluster ids -> distinct (deterministic) CIDRs here


async def test_load_spec_overlays_sibling_shaped_ingress_strategy(uow, clusters):
    """v1 parity trap (reference-code/seedpod/seedpod/core/cluster_spec.py
    :396-399): `ingress_strategy` is a SIBLING of `cluster_config` in 3 of the
    5 shipped config/deployment-profiles/*.yml. Must not be dropped."""
    provider_config = {
        "node_specification": {"cpu_cores": 2, "memory_gb": 4, "region_hint": "europe-west"},
        "cluster_config": {"node_count": 1},
        "ingress_strategy": {"type": "traefik", "traefik": {"enabled": True}},
    }
    await _insert(uow, clusters, _birth_row("c-sibling", provider_config=provider_config))
    step = LoadSpec(uow=uow, clusters=clusters, ssh_identities=SSH_IDENTITIES)

    output = await step.execute(LoadSpecParams(cluster_id="c-sibling"), make_step_context(cluster_id="c-sibling"))

    assert output.spec.cluster_config.ingress_strategy == {"type": "traefik", "traefik": {"enabled": True}}


async def test_load_spec_preserves_nested_ingress_strategy_and_does_not_overwrite_it(uow, clusters):
    """The other shape (config/deployment-profiles/exampleco-web-2.yml et al.):
    `ingress_strategy` nested directly inside `cluster_config`. A sibling key
    (absent here) must never overwrite an already-nested value either."""
    provider_config = {
        "node_specification": {"cpu_cores": 2, "memory_gb": 4, "region_hint": "europe-west"},
        "cluster_config": {"node_count": 1, "ingress_strategy": {"type": "nodeport"}},
    }
    await _insert(uow, clusters, _birth_row("c-nested", provider_config=provider_config))
    step = LoadSpec(uow=uow, clusters=clusters, ssh_identities=SSH_IDENTITIES)

    output = await step.execute(LoadSpecParams(cluster_id="c-nested"), make_step_context(cluster_id="c-nested"))

    assert output.spec.cluster_config.ingress_strategy == {"type": "nodeport"}


async def test_load_spec_unknown_cluster_raises_permanent_error(uow, clusters):
    step = LoadSpec(uow=uow, clusters=clusters, ssh_identities=SSH_IDENTITIES)
    ctx = make_step_context(cluster_id="does-not-exist")
    with pytest.raises(PermanentError):
        await step.execute(LoadSpecParams(cluster_id="does-not-exist"), ctx)


# ---------------------------------------------------------------------------
# cluster.store_kubeconfig
# ---------------------------------------------------------------------------


def test_store_kubeconfig_declares_the_dr_0022_contract():
    step = StoreKubeconfig(uow=object(), clusters=object(), crypto=object(), clock=object())
    assert step.verb == "cluster.store_kubeconfig"
    assert step.plane == "domain"
    assert step.thin is False
    assert step.gateable is False
    assert step.undoable is False


KUBECONFIG_PLAINTEXT = (
    "apiVersion: v1\nkind: Config\nclusters:\n- cluster:\n    server: https://198.51.100.7:6443\n"
    "    certificate-authority-data: VkVSWSBTRUNSRVQ=\n  name: c1\n"
)


async def test_store_kubeconfig_round_trips_through_fernet_and_mints_a_ref(uow, clusters, crypto, clock):
    await _insert(uow, clusters, _birth_row("c1", environment="ephemeral"))
    step = StoreKubeconfig(uow=uow, clusters=clusters, crypto=crypto, clock=clock)
    ctx = make_step_context(cluster_id="c1")

    from pydantic import SecretStr

    output = await step.execute(
        StoreKubeconfigParams(cluster_id="c1", kubeconfig=SecretStr(KUBECONFIG_PLAINTEXT)), ctx
    )

    assert output.kubeconfig_ref == "cluster-kubeconfig:c1"

    async with uow() as tx:
        row = clusters.get(tx, "c1")
    assert row is not None
    assert row.encrypted_kubeconfig is not None
    assert row.kubeconfig_key_class == "DEV"  # 'ephemeral' -> DEV per CryptoService's mapping
    decrypted = crypto.decrypt(row.encrypted_kubeconfig, row.kubeconfig_key_class)
    assert decrypted == KUBECONFIG_PLAINTEXT
    # never plaintext at rest
    assert row.encrypted_kubeconfig != KUBECONFIG_PLAINTEXT
    assert KUBECONFIG_PLAINTEXT not in row.encrypted_kubeconfig
    # kubeconfig_ref is machine-owned (set later by the Dispatcher via
    # ProvisionSucceeded) -- this step must never have written it itself.
    assert row.kubeconfig_ref is None


async def test_store_kubeconfig_production_environment_uses_prod_key_class(uow, clusters, crypto, clock):
    await _insert(uow, clusters, _birth_row("c1", environment="production"))
    step = StoreKubeconfig(uow=uow, clusters=clusters, crypto=crypto, clock=clock)
    ctx = make_step_context(cluster_id="c1")

    from pydantic import SecretStr

    await step.execute(StoreKubeconfigParams(cluster_id="c1", kubeconfig=SecretStr(KUBECONFIG_PLAINTEXT)), ctx)

    async with uow() as tx:
        row = clusters.get(tx, "c1")
    assert row.kubeconfig_key_class == "PROD"
    assert crypto.decrypt(row.encrypted_kubeconfig, "PROD") == KUBECONFIG_PLAINTEXT


async def test_store_kubeconfig_never_appears_in_plaintext_in_notes_or_progress(uow, clusters, crypto, clock):
    await _insert(uow, clusters, _birth_row("c1"))
    step = StoreKubeconfig(uow=uow, clusters=clusters, crypto=crypto, clock=clock)
    note_sink = RecordingNoteSink()
    progress_sink = RecordingProgressSink()
    ctx = make_step_context(cluster_id="c1", note_sink=note_sink, progress_sink=progress_sink)

    from pydantic import SecretStr

    await step.execute(StoreKubeconfigParams(cluster_id="c1", kubeconfig=SecretStr(KUBECONFIG_PLAINTEXT)), ctx)

    for _run_id, _step_path, facts in note_sink.calls:
        assert KUBECONFIG_PLAINTEXT not in repr(facts)
    for call in progress_sink.calls:
        assert KUBECONFIG_PLAINTEXT not in repr(call)


async def test_store_kubeconfig_unknown_cluster_raises_permanent_error(uow, clusters, crypto, clock):
    step = StoreKubeconfig(uow=uow, clusters=clusters, crypto=crypto, clock=clock)
    ctx = make_step_context(cluster_id="does-not-exist")

    from pydantic import SecretStr

    with pytest.raises(PermanentError):
        await step.execute(
            StoreKubeconfigParams(cluster_id="does-not-exist", kubeconfig=SecretStr(KUBECONFIG_PLAINTEXT)), ctx
        )


async def test_store_kubeconfig_unknown_environment_raises_permanent_error(uow, clusters, crypto, clock):
    await _insert(uow, clusters, _birth_row("c1", environment="not-a-real-environment"))
    step = StoreKubeconfig(uow=uow, clusters=clusters, crypto=crypto, clock=clock)
    ctx = make_step_context(cluster_id="c1")

    from pydantic import SecretStr

    with pytest.raises(PermanentError):
        await step.execute(StoreKubeconfigParams(cluster_id="c1", kubeconfig=SecretStr(KUBECONFIG_PLAINTEXT)), ctx)


# ---------------------------------------------------------------------------
# cluster.load_infra -- the DESTROY head (Round 8b, DR-0022 ruling 2)
# ---------------------------------------------------------------------------


def test_load_infra_declares_the_dr_0022_contract():
    step = LoadInfra(uow=object(), clusters=object())  # construction only -- no IO
    assert step.verb == "cluster.load_infra"
    assert step.plane == "domain"
    assert step.thin is False
    assert step.gateable is False
    assert step.undoable is False


async def test_load_infra_reads_resource_ids_from_provider_resources_not_provider_config(uow, clusters):
    """v2 splits what v1 kept in one blob: `provider_config` holds provisioning
    INPUTS, `provider_resources` holds the OUTPUTS (Seam C `InstanceCreated.
    resource_ids`, persisted via InfraAllocated). The destroy path wants the
    outputs -- reading the inputs would destroy nothing."""
    row = _birth_row("c-destroy", provider="digitalocean", provider_resources={"droplet_id": "589450319"})
    await _insert(uow, clusters, row)
    step = LoadInfra(uow=uow, clusters=clusters)

    output = await step.execute(LoadInfraParams(cluster_id="c-destroy"), make_step_context(cluster_id="c-destroy"))

    assert output.resource_ids == {"droplet_id": "589450319"}
    assert output.provider == "digitalocean"
    assert output.slug == "c-destroy"


async def test_load_infra_yields_no_dns_record_when_the_cluster_never_had_one(uow, clusters):
    """The common case -- most clusters have no DNS record. Both destroy workflows
    bind this Optional with 'None => no-op'."""
    await _insert(uow, clusters, _birth_row("c-nodns"))
    step = LoadInfra(uow=uow, clusters=clusters)

    output = await step.execute(LoadInfraParams(cluster_id="c-nodns"), make_step_context(cluster_id="c-nodns"))

    assert output.dns_record is None


async def test_load_infra_builds_the_dns_record_ref_from_the_columns(uow, clusters):
    """v1's `_cleanup_dns_record` read dns_record_id/dns_zone/dns_hostname off its one
    `provider_config` blob; this is the same read, now from the three columns
    `cluster.store_dns_record` writes (DR-0034 decision 4), typed and delivered to
    `dns.delete_record` through the grammar instead of a dispatch-time snapshot."""
    await _insert(
        uow,
        clusters,
        _birth_row(
            "c-dns",
            dns_record_id="rec-42",
            dns_zone="example.com",
            dns_hostname="c-dns.example.com",
        ),
    )
    step = LoadInfra(uow=uow, clusters=clusters)

    output = await step.execute(LoadInfraParams(cluster_id="c-dns"), make_step_context(cluster_id="c-dns"))

    assert output.dns_record is not None
    assert output.dns_record.record_id == "rec-42"
    assert output.dns_record.zone == "example.com"
    assert output.dns_record.hostname == "c-dns.example.com"


async def test_load_infra_is_read_fresh_not_a_dispatch_time_snapshot(uow, clusters):
    """DR-0022 ruling 2's whole point: the dispatch table's
    `dns_record_ref(cluster)` hook snapshotted at dispatch time and went stale on
    retry. Reading in `execute()` means a second run sees the CURRENT row."""
    await _insert(uow, clusters, _birth_row("c-fresh", provider_resources={"droplet_id": "old"}))
    step = LoadInfra(uow=uow, clusters=clusters)
    ctx = make_step_context(cluster_id="c-fresh")

    first = await step.execute(LoadInfraParams(cluster_id="c-fresh"), ctx)
    # Mutate the row underneath the step (raw SQL: the WRITE path is not what this
    # test is about -- the dispatcher owns it via InfraAllocated).
    async with uow() as tx:
        tx.execute(
            text("UPDATE clusters SET provider_resources = :r WHERE id = :id"),
            {"r": '{"droplet_id": "new"}', "id": "c-fresh"},
        )
    second = await step.execute(LoadInfraParams(cluster_id="c-fresh"), ctx)

    assert first.resource_ids == {"droplet_id": "old"}
    assert second.resource_ids == {"droplet_id": "new"}


async def test_load_infra_raises_for_an_unknown_cluster(uow, clusters):
    step = LoadInfra(uow=uow, clusters=clusters)
    with pytest.raises(PermanentError):
        await step.execute(LoadInfraParams(cluster_id="nope"), make_step_context(cluster_id="nope"))


# ---------------------------------------------------------------------------
# cluster.load_kubeconfig{,_optional} -- the decrypt inverse of store_kubeconfig
# ---------------------------------------------------------------------------


async def _store(uow, clusters, crypto, clock, cluster_id: str, kubeconfig: str) -> None:
    from pydantic import SecretStr

    step = StoreKubeconfig(uow=uow, clusters=clusters, crypto=crypto, clock=clock)
    await step.execute(
        StoreKubeconfigParams(cluster_id=cluster_id, kubeconfig=SecretStr(kubeconfig)),
        make_step_context(cluster_id=cluster_id),
    )


def test_load_kubeconfig_variants_declare_the_dr_0022_contract():
    for cls, verb in ((LoadKubeconfig, "cluster.load_kubeconfig"), (LoadKubeconfigOptional, "cluster.load_kubeconfig_optional")):
        step = cls(uow=object(), clusters=object(), crypto=object())
        assert step.verb == verb
        assert step.plane == "domain"
        assert step.thin is False
        assert step.gateable is False
        assert step.undoable is False
        assert set(step.Params.model_fields) == set()  # EmptyParams: reads ctx.cluster_id


async def test_load_kubeconfig_round_trips_what_store_kubeconfig_wrote(uow, clusters, crypto, clock):
    """The exact inverse: store encrypts, load decrypts, and the plaintext matches."""
    await _insert(uow, clusters, _birth_row("c-kc"))
    await _store(uow, clusters, crypto, clock, "c-kc", "apiVersion: v1\nkind: Config\n")
    step = LoadKubeconfig(uow=uow, clusters=clusters, crypto=crypto)

    output = await step.execute(step.Params(), make_step_context(cluster_id="c-kc"))

    assert output.kubeconfig.get_secret_value() == "apiVersion: v1\nkind: Config\n"


async def test_load_kubeconfig_raises_when_the_cluster_has_none(uow, clusters, crypto):
    """deploy-waves/deploy-rollback exist to act ON a provisioned cluster, so a
    missing kubeconfig is a genuine error there -- not a silent empty string."""
    await _insert(uow, clusters, _birth_row("c-nokc"))
    step = LoadKubeconfig(uow=uow, clusters=clusters, crypto=crypto)

    with pytest.raises(PermanentError):
        await step.execute(step.Params(), make_step_context(cluster_id="c-nokc"))


async def test_load_kubeconfig_optional_returns_none_when_the_cluster_has_none(uow, clusters, crypto):
    """Absence is NORMAL on the destroy path: a cluster whose provisioning died
    before `cluster.store_kubeconfig` still has real infrastructure to tear down.
    Failing here would strand it -- exactly the outcome the destroy workflows'
    `on_failure: continue` and Optional bindings exist to avoid."""
    await _insert(uow, clusters, _birth_row("c-halfprov"))
    step = LoadKubeconfigOptional(uow=uow, clusters=clusters, crypto=crypto)

    output = await step.execute(step.Params(), make_step_context(cluster_id="c-halfprov"))

    assert output.kubeconfig is None


async def test_load_kubeconfig_optional_returns_the_kubeconfig_when_present(uow, clusters, crypto, clock):
    await _insert(uow, clusters, _birth_row("c-kc2"))
    await _store(uow, clusters, crypto, clock, "c-kc2", "kubeconfig-body")
    step = LoadKubeconfigOptional(uow=uow, clusters=clusters, crypto=crypto)

    output = await step.execute(step.Params(), make_step_context(cluster_id="c-kc2"))

    assert output.kubeconfig is not None
    assert output.kubeconfig.get_secret_value() == "kubeconfig-body"


@pytest.mark.parametrize("cls", [LoadKubeconfig, LoadKubeconfigOptional])
async def test_both_loaders_raise_for_an_unknown_cluster(uow, clusters, crypto, cls):
    """A MISSING ROW is different from a missing kubeconfig: the optional variant
    tolerates the latter, but neither may invent a cluster that does not exist."""
    step = cls(uow=uow, clusters=clusters, crypto=crypto)
    with pytest.raises(PermanentError):
        await step.execute(step.Params(), make_step_context(cluster_id="ghost"))


# ---------------------------------------------------------------------------
# cluster.load_spec — the DNS intent (DR-0034 decision 3)
# ---------------------------------------------------------------------------


async def test_load_spec_yields_no_dns_intent_for_a_profile_that_never_enabled_dns(uow, clusters):
    """The majority. `_provider_config_from` only writes the block when the profile
    enabled DNS, so absence in the blob IS absence of intent -- no second opinion."""
    await _insert(uow, clusters, _birth_row("c-nodns"))
    step = LoadSpec(uow=uow, clusters=clusters, ssh_identities={})

    output = await step.execute(LoadSpecParams(cluster_id="c-nodns"), make_step_context(cluster_id="c-nodns"))

    assert output.dns_intent is None


async def test_load_spec_reads_the_dns_intent_off_provider_config(uow, clusters):
    """v1 kept the profile's whole `dns:` block in `provider_config["dns_config"]`
    (cluster_manager.py:318-321) and read it back at DNS-creation time
    (state_manager.py:955-956). Same two ends, now typed and delivered through the
    grammar to `dns.create_record`."""
    config = {
        **PROVIDER_CONFIG,
        "dns_config": {
            "enabled": True,
            "provider": "cloudflare",
            "zone": "example.com",
            "subdomain_pattern": "{cluster_slug}.cluster",
        },
    }
    await _insert(uow, clusters, _birth_row("c-dnsintent", provider_config=config))
    step = LoadSpec(uow=uow, clusters=clusters, ssh_identities={})

    output = await step.execute(
        LoadSpecParams(cluster_id="c-dnsintent"), make_step_context(cluster_id="c-dnsintent")
    )

    assert output.dns_intent == DnsIntent(zone="example.com", subdomain_pattern="{cluster_slug}.cluster")


async def test_load_spec_dns_config_does_not_disturb_the_cluster_specification(uow, clusters):
    """`_cluster_specification_from` reads only named keys, so the extra blob key is
    inert -- worth pinning, because provider_config is what builds the spec that
    provisions the machine."""
    config = {**PROVIDER_CONFIG, "dns_config": {"enabled": True, "zone": "example.com"}}
    await _insert(uow, clusters, _birth_row("c-both", provider_config=config))
    step = LoadSpec(uow=uow, clusters=clusters, ssh_identities={})

    output = await step.execute(LoadSpecParams(cluster_id="c-both"), make_step_context(cluster_id="c-both"))

    assert output.spec.node_specification.cpu_cores == 2
    assert output.spec.cluster_config.node_count == 1


# ---------------------------------------------------------------------------
# cluster.store_dns_record — DR-0034 decisions 1 and 4
# ---------------------------------------------------------------------------


def test_store_dns_record_declares_its_contract():
    step = StoreDnsRecord(uow=None, clusters=None, clock=None)
    assert step.verb == "cluster.store_dns_record"
    assert step.plane == "domain"
    assert step.thin is False
    assert step.gateable is False
    # The inverse of this write is deleting the record, which is dns.create_record's
    # undo -- it owns the `created` flag that makes taking the inverse safe.
    assert step.undoable is False
    assert step.idempotent is True


async def test_store_dns_record_writes_the_three_columns(uow, clusters, clock):
    """`clusters.dns_hostname` had NO writer at all before this (backlog #22) -- the
    column, `api/routers/clusters.py`'s `cluster_url`, and the SPA were all ready for
    a value nobody set."""
    await _insert(uow, clusters, _birth_row("c-store"))
    step = StoreDnsRecord(uow=uow, clusters=clusters, clock=clock)
    record = DnsRecordRef(record_id="rec-42", zone="example.com", hostname="c-store.example.com")

    await step.execute(
        StoreDnsRecordParams(cluster_id="c-store", record=record), make_step_context(cluster_id="c-store")
    )

    async with uow() as tx:
        row = clusters.get(tx, "c-store")
    assert (row.dns_hostname, row.dns_zone, row.dns_record_id) == (
        "c-store.example.com",
        "example.com",
        "rec-42",
    )


async def test_what_store_writes_is_exactly_what_load_infra_reads_back(uow, clusters, clock):
    """The join that closes backlog #6: destroy has always called `dns.delete_record`
    and always reported success, because `cluster.load_infra` had nothing to load."""
    await _insert(uow, clusters, _birth_row("c-round"))
    record = DnsRecordRef(record_id="rec-7", zone="example.com", hostname="c-round.example.com")
    await StoreDnsRecord(uow=uow, clusters=clusters, clock=clock).execute(
        StoreDnsRecordParams(cluster_id="c-round", record=record), make_step_context(cluster_id="c-round")
    )

    output = await LoadInfra(uow=uow, clusters=clusters).execute(
        LoadInfraParams(cluster_id="c-round"), make_step_context(cluster_id="c-round")
    )

    assert output.dns_record == record


async def test_store_dns_record_with_no_record_is_a_no_op(uow, clusters, clock):
    """Bound straight through from `dns.create_record`'s own Optional Output, so the
    provision workflows need no conditional -- which the frozen grammar has none of."""
    await _insert(uow, clusters, _birth_row("c-nostore"))
    step = StoreDnsRecord(uow=uow, clusters=clusters, clock=clock)

    await step.execute(
        StoreDnsRecordParams(cluster_id="c-nostore", record=None), make_step_context(cluster_id="c-nostore")
    )

    async with uow() as tx:
        row = clusters.get(tx, "c-nostore")
    assert (row.dns_hostname, row.dns_zone, row.dns_record_id) == (None, None, None)


async def test_store_dns_record_raises_when_the_row_vanished(uow, clusters, clock):
    """A silently-lost write here leaks the record forever: it exists at Cloudflare and
    this triple is the only thing that would ever name it again. Raising is what gets
    the run compensated, and `dns.create_record`'s undo then deletes it."""
    step = StoreDnsRecord(uow=uow, clusters=clusters, clock=clock)
    record = DnsRecordRef(record_id="rec-42", zone="example.com", hostname="gone.example.com")

    with pytest.raises(PermanentError):
        await step.execute(
            StoreDnsRecordParams(cluster_id="c-vanished", record=record),
            make_step_context(cluster_id="c-vanished"),
        )


# ---------------------------------------------------------------------------
# DR-0040: cluster.auto_snapshot
# ---------------------------------------------------------------------------


_UNSET = object()


class _RecordingSnapshots:
    """Stands in for SnapshotService. Not a Mock -- the testing posture bans those;
    this records the calls the step makes and returns whatever it is told to."""

    def __init__(self, result=None, *, operator_result=_UNSET):
        self.calls: list[tuple[str, str]] = []
        # Which METHOD was called matters as much as whether one was (DR-0043): the
        # profile-gated and unconditional paths are different promises to the operator.
        self.methods: list[str] = []
        self._result = result
        self._operator_result = result if operator_result is _UNSET else operator_result

    async def attempt_auto_snapshot(self, cluster, *, actor):
        """DR-0040's profile-gated path -- returns None when the profile opted out."""
        self.calls.append((cluster.id, actor))
        self.methods.append("attempt_auto_snapshot")
        return self._result

    async def attempt_pre_destroy_snapshot(self, cluster, *, actor):
        """DR-0043's unconditional path -- what an operator explicitly asked for."""
        self.calls.append((cluster.id, actor))
        self.methods.append("attempt_pre_destroy_snapshot")
        return self._operator_result


class _FakeSnapshotRow:
    id = "snap-1"
    name = "auto-c-auto-2026-08-14"


async def test_auto_snapshot_is_a_no_op_when_nobody_asked(uow, clusters, clock):
    """An operator destroy that did NOT tick snapshot_before_destroy takes nothing.
    Before DR-0043 this was the ONLY operator behaviour, because `ClusterService.
    destroy` snapshotted inline; now the step serves both routes and this is the
    genuinely-nobody-asked case."""
    await _insert(uow, clusters, _birth_row("c-auto-op"))
    snapshots = _RecordingSnapshots(result=_FakeSnapshotRow())
    step = AutoSnapshot(uow=uow, clusters=clusters, snapshots=snapshots, clock=clock)

    out = await step.execute(
        AutoSnapshotParams(cluster_id="c-auto-op", trigger="operator", snapshot=False),
        make_step_context(cluster_id="c-auto-op"),
    )

    assert snapshots.calls == []  # the service was never even consulted
    assert out.snapshot_id is None
    assert "no snapshot requested" in out.skipped_reason


async def test_auto_snapshot_fires_unconditionally_when_the_operator_asked(uow, clusters, clock):
    """DR-0043: `snapshot_before_destroy=true` reaches the step as `snapshot=True` and
    routes to the UNCONDITIONAL helper -- not the profile-gated one, which could
    silently decline what the operator explicitly asked for."""
    await _insert(uow, clusters, _birth_row("c-auto-req"))
    snapshots = _RecordingSnapshots(result=_FakeSnapshotRow())
    step = AutoSnapshot(uow=uow, clusters=clusters, snapshots=snapshots, clock=clock)

    out = await step.execute(
        AutoSnapshotParams(cluster_id="c-auto-req", trigger="operator", snapshot=True),
        make_step_context(cluster_id="c-auto-req"),
    )

    assert snapshots.methods == ["attempt_pre_destroy_snapshot"]
    assert snapshots.calls == [("c-auto-req", "operator")]
    assert out.snapshot_id == "snap-1"


async def test_auto_snapshot_operator_request_wins_over_a_ttl_trigger(uow, clusters, clock):
    """DR-0043 decision 5, the load-bearing precedence. Both inputs set at once: the
    explicit request must win, and must take EXACTLY ONE snapshot.

    The failure this pins is silent. Checking `trigger` first would route an operator's
    explicit request into the profile-gated path, so a profile with `auto_snapshot`
    disabled would skip it -- the operator asked, the profile said no, and nobody would
    be told. `operator_result=None` models exactly that profile."""
    await _insert(uow, clusters, _birth_row("c-auto-both"))
    snapshots = _RecordingSnapshots(result=None, operator_result=_FakeSnapshotRow())
    step = AutoSnapshot(uow=uow, clusters=clusters, snapshots=snapshots, clock=clock)

    out = await step.execute(
        AutoSnapshotParams(cluster_id="c-auto-both", trigger="ttl_expiry", snapshot=True),
        make_step_context(cluster_id="c-auto-both"),
    )

    assert snapshots.methods == ["attempt_pre_destroy_snapshot"]  # exactly one, the right one
    assert out.snapshot_id == "snap-1"


async def test_auto_snapshot_fires_for_an_unattended_ttl_destroy(uow, clusters, clock):
    """The gap DR-0040 exists to close: on 2026-08-13 a real 8-hour TTL expired, the
    destroy was flawless, and the snapshots table was empty."""
    await _insert(uow, clusters, _birth_row("c-auto-ttl"))
    snapshots = _RecordingSnapshots(result=_FakeSnapshotRow())
    step = AutoSnapshot(uow=uow, clusters=clusters, snapshots=snapshots, clock=clock)

    out = await step.execute(
        AutoSnapshotParams(cluster_id="c-auto-ttl", trigger="ttl_expiry"),
        make_step_context(cluster_id="c-auto-ttl"),
    )

    assert snapshots.calls == [("c-auto-ttl", "timer:ttl")]
    assert out.snapshot_id == "snap-1"
    assert out.skipped_reason is None


async def test_auto_snapshot_says_why_when_the_service_declines(uow, clusters, clock):
    """Fail-open, but never silent: "no snapshot exists" is otherwise indistinguishable
    from "the feature is inert again", which is exactly the state DR-0040 found."""
    await _insert(uow, clusters, _birth_row("c-auto-none"))
    snapshots = _RecordingSnapshots(result=None)  # disabled / nothing persistable / failed
    step = AutoSnapshot(uow=uow, clusters=clusters, snapshots=snapshots, clock=clock)

    out = await step.execute(
        AutoSnapshotParams(cluster_id="c-auto-none", trigger="ttl_expiry"),
        make_step_context(cluster_id="c-auto-none"),
    )

    assert out.snapshot_id is None
    assert "no snapshot taken" in out.skipped_reason


async def test_auto_snapshot_survives_a_cluster_row_that_is_already_gone(uow, clusters, clock):
    """A TTL destroy is a deadline; nothing here may raise and strand the teardown."""
    snapshots = _RecordingSnapshots(result=_FakeSnapshotRow())
    step = AutoSnapshot(uow=uow, clusters=clusters, snapshots=snapshots, clock=clock)

    out = await step.execute(
        AutoSnapshotParams(cluster_id="never-existed", trigger="ttl_expiry"),
        make_step_context(cluster_id="never-existed"),
    )

    assert snapshots.calls == []
    assert out.skipped_reason == "cluster row is gone"
