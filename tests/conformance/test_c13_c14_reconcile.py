"""tests/conformance/test_c13_c14_reconcile.py — C-13/C-14 (Seam C §5.6 table).

    C-13 | test_reconcile_intent_matrix | machine | parametrized (db_status x backend
    reality): active+missing=>Orphan; active+stopped=>Orphan (kind/tart); DESTROYED+present
    =>Zombie; DESTROYING+missing=>Orphan(completion); no-uuid-tag=>skipped;
    uuid-tag-no-DB-row=>CreateUnmanaged (DO)
    C-14 | test_reconcile_unreachable_touches_nothing | machine | injected outage => raises
    Unreachable, zero intents produced, zero backend mutations

``_seed_reconcile_case`` drives every harness's own ``reconcile_truth_table()`` (Seam C §5.6's
``Harness.reconcile_truth_table()`` hook) through each provider's actual fake-backend seeding
API, mined verbatim from ``test_{provider}_smoke.py``'s reconcile tests — the intent vocabulary
itself (``OrphanIntent``/``ZombieIntent``/``CreateUnmanagedIntent``) is asserted generically via
``ReconciliationIntent.type`` (an ``IntentType`` ``StrEnum``, comparable to the truth table's
plain ``expected_intent`` string).
"""

from __future__ import annotations

import pytest

from seedpod.core.errors import InfrastructureUnreachableError
from seedpod.providers.contract import ClusterSnapshot, Reconcile
from tests.conformance._support import drain
from tests.conformance.harness import Fault, ReconcileCase

pytestmark = pytest.mark.asyncio


def _seed_reconcile_case(harness, case: ReconcileCase, uuid: str) -> ClusterSnapshot | None:
    """Seeds this harness's fake backend to match ``case``, returning the ``ClusterSnapshot``
    to include in ``Reconcile.clusters`` — or ``None`` when the case represents a bare backend
    resource with no corresponding DB row at all (``db_status is None``: it must never appear
    in ``clusters``, only in the backend)."""
    name = harness.name

    if name == "digitalocean":
        if case.db_status is None:
            tags = ["seedpod-managed"] + ([f"cluster-uuid:{uuid}"] if case.backend_tagged else [])
            harness.backend.seed_droplet(tags=tags)
            return None
        if case.backend_present:
            harness.backend.seed_droplet(tags=["seedpod-managed", f"cluster-uuid:{uuid}"])
        return ClusterSnapshot(cluster_uuid=uuid, slug=case.name, status=case.db_status, resource_ids={})

    if name in ("kind", "tart"):
        rid_key = "kind_cluster_name" if name == "kind" else "tart_vm_name"
        backend_name = f"seedpod-{case.name}"
        resource_ids: dict[str, str] = {}
        if case.backend_present:
            # Zombie needs presence only; the Orphan-on-present case is specifically the
            # "container/VM present but stopped" scenario (crown jewel: v1's
            # container-stopped=>Orphan) — the only "present" row for a non-destroyed
            # db_status in either truth table.
            running = case.db_status == "destroyed"
            if name == "kind":
                harness.backend.seed_cluster(backend_name, running=running)
            else:
                harness.backend.seed_vm(backend_name, running=running, ip=("192.168.64.10" if running else None))
            resource_ids = {rid_key: backend_name}
        if case.db_status is None:
            return None
        return ClusterSnapshot(cluster_uuid=uuid, slug=case.name, status=case.db_status, resource_ids=resource_ids)

    if name == "orbstack":
        # orbstack "never orphans" (module docstring): every row is reachable-backend, no
        # seeding needed, and every expected_intent is None by construction of its own
        # reconcile_truth_table().
        return ClusterSnapshot(cluster_uuid=uuid, slug=case.name, status=case.db_status, resource_ids={})

    raise AssertionError(f"no reconcile seeding for {name!r}")


async def test_reconcile_intent_matrix(machine_harness):
    cases = machine_harness.reconcile_truth_table()
    if not cases:
        pytest.skip(f"{machine_harness.name} declares no reconcile_truth_table()")

    provider = machine_harness.provider()
    clusters: list[ClusterSnapshot] = []
    uuid_for: dict[str, str] = {}
    for case in cases:
        uuid = f"{case.name}-uuid"
        uuid_for[case.name] = uuid
        snapshot = _seed_reconcile_case(machine_harness, case, uuid)
        if snapshot is not None:
            clusters.append(snapshot)

    (result,) = await drain(provider, Reconcile(clusters=tuple(clusters)))
    intents_by_uuid = {i.cluster_id: i for i in result.value}

    for case in cases:
        uuid = uuid_for[case.name]
        if case.expected_intent is None:
            assert uuid not in intents_by_uuid, f"{machine_harness.name}/{case.name}: expected no intent"
            continue
        intent = intents_by_uuid.get(uuid)
        assert intent is not None, f"{machine_harness.name}/{case.name}: expected {case.expected_intent!r}, got none"
        assert intent.type == case.expected_intent, (
            f"{machine_harness.name}/{case.name}: expected {case.expected_intent!r}, got {intent.type!r}"
        )


async def test_reconcile_unreachable_touches_nothing(machine_harness):
    provider = machine_harness.provider(Fault.UNREACHABLE)
    before = await machine_harness.backend_resources()
    with pytest.raises(InfrastructureUnreachableError):
        await drain(provider, Reconcile(clusters=()))
    assert await machine_harness.backend_resources() == before, "an unreachable Reconcile must touch nothing"
