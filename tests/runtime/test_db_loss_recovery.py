"""What a REBUILT DATABASE actually recovers — the scenario behind verify flag 10
and the "Operational readiness" section of the (unpublished) parity backlog.

v2 is the only thing that knows a cluster exists. If `db/seedpod.db` is lost and
recreated from schema, reconciliation rediscovers the running infrastructure by the
`seedpod-managed` tag and births an UNMANAGED row. What nobody had measured is the
*consequence*: is that row enough to act on, or merely enough to look at?

`tests/runtime/test_reconciliation.py` already pins the discovery half thoroughly.
It stops at the birth row, which is the "pins the decision, misses the consequence"
shape this repo keeps meeting (backlog #13, and the DR-0036 seam bug smoke 12 found).
These tests carry the scenario one step further, into the real `cluster.load_infra`
step the destroy workflow's head actually runs.

**The claim they establish**, so an operator can rely on it without a live rehearsal:

- a rebuilt DB recovers **inventory and the ability to destroy**;
- it does **not** recover the ability to deploy, redeploy, or run anything through
  kubectl — `encrypted_kubeconfig` dies with the database and nothing re-fetches it;
- and it will **not** destroy anything on its own: a rediscovered cluster is
  UNMANAGED, while the zombie sweep only ever touches DESTROYED rows.

That last one is the reassuring half, and it is the one worth being certain of.
"""

from __future__ import annotations

import pytest

from seedpod.core.errors import PermanentError
from seedpod.core.reconciliation_intents import CreateUnmanagedIntent
from seedpod.core.records import Origin
from seedpod.engine.step import EmptyParams
from seedpod.engine.steps.cluster import LoadInfra, LoadInfraParams, LoadKubeconfig
from tests.engine.fakes import make_step_context
from tests.runtime.test_reconciliation import FakeProvider, _service

pytestmark = pytest.mark.asyncio

_DROPLET = {"id": 4242, "region": {"slug": "ams3"}, "size_slug": "s-4vcpu-8gb"}


async def _rediscover(uow, repos, dispatcher, hub, clock):
    """The rebuilt-DB scenario: an EMPTY database, and a provider still running a
    droplet seedpod created before the loss."""
    provider = FakeProvider(
        "digitalocean",
        intents=(CreateUnmanagedIntent(cluster_id="recovered-1", droplet=_DROPLET, slug="preset-lost-abc12345"),),
    )
    await _service({"digitalocean": provider}, repos, dispatcher, uow, hub, clock).tick()
    async with uow() as tx:
        return repos.clusters.get(tx, "recovered-1")


async def test_a_rebuilt_db_recovers_enough_to_destroy(uow, repos, dispatcher, hub, clock):
    """The load-bearing claim. `cluster.load_infra` is the destroy workflow's head,
    and everything `infra.destroy_instance` needs comes from it — so if this passes,
    a destroy issued after a DB loss reaches the real droplet."""
    row = await _rediscover(uow, repos, dispatcher, hub, clock)
    assert row.status == "unmanaged"
    assert row.origin == Origin.DISCOVERED

    output = await LoadInfra(uow=uow, clusters=repos.clusters).execute(
        LoadInfraParams(cluster_id="recovered-1"), make_step_context(cluster_id="recovered-1")
    )

    assert output.provider == "digitalocean"
    assert output.slug == "preset-lost-abc12345"
    # The droplet id is what DigitalOcean is addressed by; without it the destroy
    # would fall back to the cluster-uuid tag lookup for a uuid that no longer
    # matches anything, and leak the droplet.
    assert output.resource_ids == {"droplet_id": "4242"}


async def test_a_rebuilt_db_does_NOT_recover_the_kubeconfig(uow, repos, dispatcher, hub, clock):
    """The other half of the honest answer, and the reason "recovered" needs
    qualifying: the kubeconfig was Fernet-encrypted IN the database, so it is gone
    with it. Nothing re-fetches one for a discovered cluster, so the adopted row is
    inventory you can destroy — not a cluster you can deploy to."""
    await _rediscover(uow, repos, dispatcher, hub, clock)

    # `crypto` is never reached: the step short-circuits on the absent ciphertext
    # before decrypting, which is itself the point being made.
    step = LoadKubeconfig(uow=uow, clusters=repos.clusters, crypto=None)
    with pytest.raises(PermanentError) as excinfo:
        await step.execute(EmptyParams(), make_step_context(cluster_id="recovered-1"))
    assert "no stored kubeconfig" in str(excinfo.value)


async def test_rediscovery_never_destroys_anything_by_itself(uow, repos, dispatcher, hub, clock):
    """The reassuring half, pinned because the cost of being wrong is destroyed
    production infrastructure.

    The zombie sweep issues `DestroyRequested(force=True)` for discovered-origin
    rows, which sounds alarming next to "a DB loss rediscovers everything". It is
    not reachable here: a `ZombieIntent` fires only for a cluster the DB believes is
    DESTROYED while infra says it is running, and a rebuilt DB believes nothing.
    Rediscovery lands in UNMANAGED, which the sweep never reads."""
    row = await _rediscover(uow, repos, dispatcher, hub, clock)
    assert row.status == "unmanaged"

    async with uow() as tx:
        # Non-vacuity first: prove this query and these status literals actually
        # find the row, so the two emptiness assertions below mean something. A
        # typo'd status string would otherwise make them pass for free -- the
        # exact shape of vacuous test this repo has been bitten by.
        assert [r.id for r in repos.clusters.list_by_status(tx, ("unmanaged",))] == ["recovered-1"]
        zombies = repos.clusters.list_by_status(tx, ("zombie",))
        scheduled = repos.clusters.list_by_status(tx, ("destroy-scheduled", "destroying", "destroyed"))
    assert zombies == []
    assert scheduled == []


async def test_the_adopted_row_carries_no_dns_record_so_destroy_does_not_delete_a_stranger(
    uow, repos, dispatcher, hub, clock
):
    """A DNS record created before the loss is NOT recovered — `dns_record_id` was a
    column in the lost database and discovery cannot re-derive it. So destroying an
    adopted cluster leaves its DNS record behind (a leak, and the honest thing to
    document), rather than deleting a record it cannot identify. Pinned because the
    alternative — guessing a record from the slug — would risk deleting a name that
    now points somewhere else entirely."""
    await _rediscover(uow, repos, dispatcher, hub, clock)

    output = await LoadInfra(uow=uow, clusters=repos.clusters).execute(
        LoadInfraParams(cluster_id="recovered-1"), make_step_context(cluster_id="recovered-1")
    )

    assert output.dns_record is None
