"""``SnapshotService.restore`` -- the DATA-TRANSFER half.

This file exists because that half was missing entirely and nothing noticed.

``restore`` checked ``dump_path.exists()`` and then exec'd
``pg_restore -U … -d … --clean --if-exists`` with **no file argument and no
stdin**: the dump's bytes were never read, and ``KubeRun`` had no way to carry
them even if they had been. ``pg_restore`` against an empty stdin exits 1 with
``input file is too short (read 0, expected 5)`` -- so the failure was at least
loud, but it read as a corrupt dump rather than as v2 never sending one.

v1 did the transfer with ``kubectl cp`` into the pod then
``pg_restore … <remote_path>``
(``reference-code/seedpod/seedpod/services/snapshot_service.py:222-270``).
v2 diverges deliberately (Kezia, 2026-08-10): ``kubectl exec -i`` with the bytes
on stdin. The transport already accepted ``stdin``; there is no remote temp file
to create, name uniquely, or leak when a restore dies midway.

**Why 2349 green tests missed it.** The only restore coverage,
``tests/engine/steps/test_deploy_restore_steps.py``, injects a *fake*
``SnapshotService`` -- its own docstring says the real internals are "frozen for
this round". It pins the verb's handling of a ``RestoreResult``; nothing
asserted a ``RestoreResult`` was ever earned. The same shape as backlog #13: the
decision was pinned, the consequence never was.

No Mock/patch: a real ``SnapshotService`` over a real ``KubectlProvider`` over a
recording transport that wraps the shared conformance fake.
"""

from __future__ import annotations

import gzip
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path

import pytest

from seedpod.app.services.snapshot_service import SnapshotService
from seedpod.data.repositories import ClusterRow, SnapshotRepository, SnapshotRow
from seedpod.providers.contract import SubprocessResult
from seedpod.providers.kubectl import KubectlConfig, KubectlProvider
from tests.conformance.fake_kubectl import FakeKubectlBackend, FakeKubectlTransport

NOW = datetime(2026, 8, 10, 12, 0, 0, tzinfo=UTC)
CLUSTER_ID = "cl-restore-1"
SOURCE_CLUSTER_ID = "cl-source-1"
SNAPSHOT_ID = "snap-restore-1"

# A `pg_dump -Fc` archive starts with the magic "PGDMP"; the bytes after it are
# opaque to everything under test. Using a recognisable payload rather than
# b"x" makes a truncation or an encoding round-trip visible in the assertion.
DUMP_BYTES = b"PGDMP\x00\x01binary-archive-body\xff\xfe"


class RecordingTransport:
    """Delegates to the conformance fake, recording ``(argv, stdin)`` per call.

    ``FakeKubectlBackend.call_log`` records argv only, which is precisely the
    blind spot that let this bug live: every assertion anyone could write about
    a restore was about the command, never about its input.
    """

    def __init__(self, backend: FakeKubectlBackend) -> None:
        self._inner = FakeKubectlTransport(backend, frozenset())
        self.calls: list[tuple[tuple[str, ...], bytes | None]] = []

    async def run(
        self,
        argv: Sequence[str],
        *,
        stdin: bytes | None = None,
        env: Mapping[str, str] | None = None,
        timeout: float | None = None,
        cluster_id: str | None = None,
    ) -> SubprocessResult:
        self.calls.append((tuple(argv), stdin))
        return await self._inner.run(argv, stdin=stdin, env=env, timeout=timeout, cluster_id=cluster_id)

    def stream(self, *a, **kw):  # pragma: no cover -- restore never streams
        return self._inner.stream(*a, **kw)

    def exec_calls(self) -> list[tuple[tuple[str, ...], bytes | None]]:
        return [c for c in self.calls if len(c[0]) > 1 and c[0][1] == "exec"]


def _pods_with(service_name: str) -> dict[tuple[str, str], dict]:
    """One Running pod named ``<service>-<hash>`` -- what ``_find_pod`` matches on."""
    name = f"{service_name}-abc123"
    return {
        ("default", name): {
            "metadata": {"name": name, "namespace": "default", "creationTimestamp": "2026-01-01T00:00:00Z",
                         "labels": {"app": service_name}, "annotations": {}},
            "spec": {"nodeName": "node-1", "containers": [{"name": service_name, "image": "postgres:16", "ports": [], "env": []}]},
            "status": {"phase": "Running", "podIP": "10.42.0.7", "hostIP": "10.0.0.1",
                       "containerStatuses": [{"name": service_name, "ready": True, "restartCount": 0, "state": {"running": {}}}],
                       "conditions": [{"type": "Ready", "status": "True"}]},
        }
    }


@pytest.fixture
def snapshot_repo() -> SnapshotRepository:
    return SnapshotRepository()


async def _build(tmp_path, uow, repos, crypto, clock, snapshot_repo, *, dump_name: str, dump_payload: bytes):
    """A cluster with a kubeconfig, a snapshot row, and its dump file on disk."""
    from tests.conformance.kubectl_harness import FAKE_KUBECONFIG

    key_class = crypto.key_class_for_environment("ephemeral")

    def _cluster(cluster_id: str, slug: str, *, with_kubeconfig: bool) -> ClusterRow:
        return ClusterRow(
            id=cluster_id, name=slug, slug=slug, origin="managed",
            environment="ephemeral", repository="exampleco-core", branch="staging", status="active",
            pre_destroy_state=None, version=1, provider="digitalocean", provider_config={},
            provider_resources={}, dns_hostname=None, dns_zone=None, dns_record_id=None, public_ip="203.0.113.9",
            node_count=1,
            encrypted_kubeconfig=crypto.encrypt(FAKE_KUBECONFIG, key_class) if with_kubeconfig else None,
            kubeconfig_key_class=key_class if with_kubeconfig else None,
            kubeconfig_ref=None, cost_per_hour=0.0, total_cost=0.0,
            consecutive_health_failures=0, failure_reason=None, last_reconciled_at=None,
            created_at=NOW, updated_at=NOW, expires_at=None,
        )

    async with uow() as tx:
        # The snapshot's SOURCE cluster is a different, older one -- the realistic
        # shape (and what `snapshots.source_cluster_id`'s FK requires exist).
        repos.clusters.insert(tx, _cluster(SOURCE_CLUSTER_ID, "staging", with_kubeconfig=False))
        repos.clusters.insert(tx, _cluster(CLUSTER_ID, "restore-target-1", with_kubeconfig=True))

    storage = tmp_path / "snapshots" / SNAPSHOT_ID
    storage.mkdir(parents=True)
    (storage / dump_name).write_bytes(dump_payload)

    async with uow() as tx:
        snapshot_repo.insert(tx, SnapshotRow(
            id=SNAPSHOT_ID, name="staging snapshot", description=None,
            source_cluster_id=SOURCE_CLUSTER_ID, source_cluster_slug="staging", branch="staging",
            # v1's own profile NAME, deliberately not v2's: the compatibility
            # pre-flight compares SERVICE names, never this string.
            deployment_profile="exampleco-stack",
            services=[{"service_name": "postgres", "persistence_type": "postgres",
                       "file": dump_name, "size_bytes": len(dump_payload),
                       "database": "exampleco", "username": "postgres"}],
            storage_path=str(storage), total_size_bytes=len(dump_payload), is_auto=False,
            created_by="admin", created_at=NOW,
        ))

    backend = FakeKubectlBackend(pods=_pods_with("postgres"))
    transport = RecordingTransport(backend)
    service = SnapshotService(
        snapshots=snapshot_repo, repos=repos, deployments=repos.deployments, crypto=crypto,
        kubectl_provider=KubectlProvider(KubectlConfig(), transport), uow=uow, clock=clock,
        id_gen=lambda: SNAPSHOT_ID, config_dir=tmp_path, storage_dir=tmp_path / "snapshots",
    )
    return service, transport


async def test_restore_sends_the_dump_bytes_to_the_pod(tmp_path, uow, repos, crypto, clock, snapshot_repo):
    """THE regression pin: the dump reaches the pod's stdin, and `-i` is set.

    Both halves matter and they fail independently. Without ``stdin`` the bytes
    never leave this process; without ``-i`` kubectl never attaches the pod's
    stdin and discards them at the remote end -- a failure that looks identical
    from here and would be invisible to a test that only checked ``stdin=``.
    """
    service, transport = await _build(
        tmp_path, uow, repos, crypto, clock, snapshot_repo,
        dump_name="postgres.dump", dump_payload=DUMP_BYTES,
    )

    result = await service.restore(SNAPSHOT_ID, cluster_id=CLUSTER_ID, actor="api:kezia")

    assert result.success is True
    assert result.services_restored == ["postgres"]

    execs = transport.exec_calls()
    assert len(execs) == 1
    argv, stdin = execs[0]
    assert stdin == DUMP_BYTES  # the whole point
    assert "-i" in argv
    assert argv.index("-i") < argv.index("postgres-abc123")  # a flag, not a positional
    assert "pg_restore -U postgres -d exampleco --clean --if-exists" in argv[-1]


async def test_restore_transparently_decompresses_a_v1_gzipped_dump(
    tmp_path, uow, repos, crypto, clock, snapshot_repo
):
    """v1 wrote ``<service>.dump.gz`` and gunzipped on the way back in; v2 writes
    raw (backlog P2 #8). Sniffing the gzip magic on READ is what makes v1's six
    existing snapshots restorable by v2 at all -- otherwise ``pg_restore`` gets
    gzip bytes and fails on a file it would call corrupt."""
    service, transport = await _build(
        tmp_path, uow, repos, crypto, clock, snapshot_repo,
        dump_name="postgres.dump.gz", dump_payload=gzip.compress(DUMP_BYTES),
    )

    result = await service.restore(SNAPSHOT_ID, cluster_id=CLUSTER_ID, actor="api:kezia")

    assert result.success is True
    _, stdin = transport.exec_calls()[0]
    assert stdin == DUMP_BYTES  # decompressed, not the gzip container


async def test_restore_sniffs_content_not_the_filename(tmp_path, uow, repos, crypto, clock, snapshot_repo):
    """A row imported from v1 can name a ``.gz`` file whose bytes are already raw
    (or the reverse). ``services[].file`` is metadata; the bytes are the
    authority, so the suffix must not decide."""
    service, transport = await _build(
        tmp_path, uow, repos, crypto, clock, snapshot_repo,
        dump_name="postgres.dump.gz", dump_payload=DUMP_BYTES,  # .gz name, raw content
    )

    result = await service.restore(SNAPSHOT_ID, cluster_id=CLUSTER_ID, actor="api:kezia")

    assert result.success is True
    _, stdin = transport.exec_calls()[0]
    assert stdin == DUMP_BYTES  # passed through untouched, not gunzip-attempted


async def test_restore_reports_failure_when_the_dump_file_is_missing(
    tmp_path, uow, repos, crypto, clock, snapshot_repo
):
    """The pre-existing ``dump_path.exists()`` guard still short-circuits, and now
    genuinely means something: before, a present file was equally unread."""
    service, transport = await _build(
        tmp_path, uow, repos, crypto, clock, snapshot_repo,
        dump_name="postgres.dump", dump_payload=DUMP_BYTES,
    )
    (Path(tmp_path) / "snapshots" / SNAPSHOT_ID / "postgres.dump").unlink()

    result = await service.restore(SNAPSHOT_ID, cluster_id=CLUSTER_ID, actor="api:kezia")

    assert result.success is False
    assert result.services_failed == ["postgres"]
    assert transport.exec_calls() == []  # never exec'd pg_restore with nothing to give it


def test_dump_bytes_reads_both_compressed_and_raw_dumps(tmp_path):
    """Backlog P2 #8: v2 now gzips on write, as v1 did. The read side must keep
    handling BOTH -- v1's snapshots, and every snapshot v2 took while it still
    wrote raw `.dump` files. `_dump_bytes` sniffs the two-byte magic rather than
    trusting the suffix, because `services[].file` and the bytes on disk can
    disagree for a row imported from v1."""
    import gzip as _gzip

    from seedpod.app.services.snapshot_service import _dump_bytes

    payload = b"PGDMP\x00fake pg_dump payload"
    raw = tmp_path / "postgres.dump"
    raw.write_bytes(payload)
    compressed = tmp_path / "postgres.dump.gz"
    compressed.write_bytes(_gzip.compress(payload, mtime=0))
    # the adversarial case: gzip content under a name that says otherwise
    mislabelled = tmp_path / "mislabelled.dump"
    mislabelled.write_bytes(_gzip.compress(payload, mtime=0))

    assert _dump_bytes(raw) == payload
    assert _dump_bytes(compressed) == payload
    assert _dump_bytes(mislabelled) == payload


def test_gzip_write_is_deterministic_for_identical_input(tmp_path):
    """`mtime=0` keeps the bytes stable for the same input -- gzip otherwise
    stamps the current time into the header, which would make `total_size_bytes`
    and any byte-comparison flap between runs."""
    import gzip as _gzip

    payload = b"PGDMP\x00fake" * 100
    assert _gzip.compress(payload, mtime=0) == _gzip.compress(payload, mtime=0)
    assert len(_gzip.compress(payload, mtime=0)) < len(payload)


# ---------------------------------------------------------------------------
# DR-0040: name_pattern finally has a reader
# ---------------------------------------------------------------------------


def test_format_snapshot_name_substitutes_v1s_five_placeholders():
    """`name_pattern` had ZERO references anywhere in v2 before DR-0040 -- three shipped
    profiles declared it and nothing had ever read one. Behaviour salvaged from v1's
    `ClusterManager._format_snapshot_name` (cluster_manager.py:797-819)."""
    from datetime import UTC, datetime

    from seedpod.app.services.snapshot_service import _format_snapshot_name

    class _Cluster:
        id = "2273a5e1-590c-4090-93c2-a9af38893b77"
        slug = "preset-exampleco-dev-tart-dev-2273a5e1"
        branch = "feature/FIN-1"

    now = datetime(2026, 8, 14, 9, 30, 15, tzinfo=UTC)

    # the shipped default, which is what all three profiles actually declare
    assert _format_snapshot_name("auto-{cluster_slug}-{date}", _Cluster(), now) == (
        "auto-preset-exampleco-dev-tart-dev-2273a5e1-2026-08-14"
    )
    # every placeholder v1 supported, including the 8-char id and HHMMSS
    assert _format_snapshot_name(
        "{branch}-{cluster_id}-{date}-{time}", _Cluster(), now
    ) == "feature-FIN-1-2273a5e1-2026-08-14-093015"


def test_format_snapshot_name_sanitises_a_slash_bearing_branch():
    """v1 replaced `/` and `\\` and truncated to 50 -- a branch name is not a safe
    identifier, and snapshot names end up in filesystem paths."""
    from datetime import UTC, datetime

    from seedpod.app.services.snapshot_service import _format_snapshot_name

    class _Cluster:
        id = "abc12345-0000"
        slug = "s"
        branch = "feature/" + "x" * 80

    out = _format_snapshot_name("{branch}", _Cluster(), datetime(2026, 8, 14, tzinfo=UTC))
    assert "/" not in out
    assert len(out) == 50


def test_format_snapshot_name_tolerates_a_branchless_cluster():
    from datetime import UTC, datetime

    from seedpod.app.services.snapshot_service import _format_snapshot_name

    class _Cluster:
        id = "abc12345-0000"
        slug = None
        branch = None

    assert _format_snapshot_name(
        "{cluster_slug}-{branch}", _Cluster(), datetime(2026, 8, 14, tzinfo=UTC)
    ) == "unknown-unknown"
