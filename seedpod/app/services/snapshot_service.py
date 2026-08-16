"""``SnapshotService`` -- the real, fail-open database-snapshot capability
DR-0020 assigns to api-features: ``ClusterService.destroy``'s
``snapshot_before_destroy=true`` collaborator, PLUS the standalone
``GET/POST/DELETE /api/snapshots``, ``POST /api/snapshots/{id}/restore`` surface
(``seedpod/api/routers/snapshots.py``).

Salvaged from ``reference-code/seedpod/seedpod/services/snapshot_service.py``
(753 lines) and ``orchestrator/cluster_manager.py``'s ``_attempt_auto_snapshot``
(:681, cited verbatim by DR-0020) -- narrowed to what a stateless, provider-
plane-only (no ``SessionProvider`` singleton, no ``KubernetesProvider`` global)
v2 service can do:

- **Dump mechanism**: ``kubectl exec <pod> -- sh -c "<dump_command>"`` via the
  SAME ``KubeRun`` primitive ``ClusterService`` already uses for provider-plane
  reads (``seedpod/providers/contract.py``'s own ``KubeRun`` docstring: "required
  for ``pg_dump -Fc`` snapshot streaming" -- crown jewel #14, ``binary=True``
  returns undecoded ``stdout``). One bounded attempt, no internal retry (the
  engine's ``Schedule`` owns that elsewhere; this call site is synchronous, not
  engine-driven -- see "no workflow-run admission" note below).
- **Pod discovery**: v1 used a ``-l app={service}`` label selector
  (``_find_pod_for_service``, reference-code :148-166); v2's ``KubeGetPods`` (the
  provider-plane read primitive ``ClusterService.pods`` already exposes) carries
  no label-selector parameter, so this lists every pod in the namespace and picks
  the first ``Running`` one whose name starts with ``f"{service_name}-"`` (the
  standard Deployment-pod naming convention: ``<name>-<replicaset-hash>-
  <random>``). A pragmatic, documented narrowing -- not label-exact, but
  sufficient for the one-pod-per-service shape every shipped profile uses.
- **Persistable services**: read directly off the RAW profile YAML's
  ``services.<name>.persistence`` block (``type``/``database``/``username``/
  ``dump_command``/``restore_command``) -- ``ManifestProfile``/``ServiceSpec``
  (``seedpod/services/manifests.py``) deliberately carry no ``persistence`` field
  (that module's own scope-narrowing: image resolution + template rendering
  only), so this reads the raw mapping ``load_deployment_profile`` already
  returns alongside the typed profile, exactly as ``DeploymentService`` reads
  ``raw_profile.get("provider", ...)`` for the same reason.
- **A cluster's current profile**: v1 read ``cluster_record.provider_config
  ["deployment_profile"]`` (a field the actual v2 birth path never populates --
  ``DeploymentService._birth_cluster_row`` always starts ``provider_config={}``,
  that method's own module docstring). This service instead resolves the
  cluster's ACTIVE deployment (``DeploymentRepository.active_for_cluster``,
  falling back to the newest deployment row if none is yet ACTIVE) and reads
  ITS ``manifest_version`` -- the field v2 actually, always populates at deploy
  time. Same intent (which profile is this cluster running), a real v2 source.
- **Storage**: local filesystem under ``AppConfig.snapshot_storage_path``
  (Round-6-added; that field's own docstring), one subdirectory per snapshot id,
  one ``<service>.dump.gz`` file per persistable service, gzip-compressed as v1
  did (backlog P2 #8, closed 2026-08-11 -- it had been deferred as "an orthogonal
  storage-format choice", which was true until snapshots started accumulating for
  real). ``total_size_bytes`` is computed from the real bytes written either way,
  so it now records the COMPRESSED size. Snapshots taken before this stay raw on
  disk and keep restoring -- ``_dump_bytes`` sniffs the content, not the suffix.
- **Restore + restore-history**: v1's async ``SnapshotOperation``-table-backed
  background-task flow (``api/snapshots.py``'s ``_restore_snapshot_background``)
  has no v2 background-task infra to run on (this round's brief builds no
  ``BackgroundTasks``/polling surface) and ``snapshot_operations`` is
  deliberately NOT recreated (``SnapshotRow``'s own module docstring, citing
  Decision 6's v1->v2 delta table). ``restore()`` here runs SYNCHRONOUSLY
  (kubectl exec the dump back into the target pod, in-request) and records the
  outcome as a ``workflow_runs`` row (``workflow="snapshot-restore"``,
  ``cluster_id``, ``args`` carrying the ui-contract restore-history fields) --
  the exact table ``GET /api/snapshots/clusters/{id}/restore-history`` is
  specified to read FROM (this round's brief, citing the standing decision).
  This is a plain ``WorkflowRunRepository.insert()`` of an already-decided,
  already-terminal row (status ``succeeded``/``failed``, both OUTSIDE
  ``ACTIVE_RUN_STATUSES``) -- never a real engine-admitted run (no
  ``RunWorkflow`` effect, no ``dedupe_key``/``ux_wr_one_active`` contention),
  matching how ``DeploymentService``/``ClusterService`` already write
  cluster/deployment state directly through their own repositories' plain
  ``insert``/``update`` primitives rather than every write needing to be an
  engine-driven step; CLAUDE.md's "state changes go through ``Dispatcher.
  apply()`` only" binds cluster/deployment aggregate state, which this row is
  not. Building a real ``snapshot-restore`` workflow verb is exactly the kind of
  net-new engine/domain authorship this round's brief forbids ("do NOT edit
  seedpod/engine") -- flagged as a DR-worthy follow-up, not invented here.
- **Every compress/decompress and file touch runs off the event loop.** A dump is
  the one payload in this tree whose size is set by the USER's database rather
  than by anything v2 controls, and ``gzip.compress`` on a few hundred megabytes
  is seconds of solid CPU. Run inline it froze the whole process -- no requests
  served, no SSE keepalives, no timer polls, no health checks -- for the duration
  of every snapshot, on a path that also runs inside cluster destroy. So the
  compress/write/size and the read/decompress each cross ``asyncio.to_thread``
  ONCE (``_write_dump``/``_read_dump_bytes`` below), not once per operation:
  folding them into a single hop keeps the syscall count where it was. Both zlib
  and file IO release the GIL, so a thread is genuinely enough here and no
  process boundary is warranted. ``_dump_bytes`` itself stays sync -- it is the
  interesting logic (magic-byte sniffing) and is unit-tested directly.
"""

from __future__ import annotations

import asyncio
import contextlib
import gzip
import shutil
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from seedpod.app.services.profiles import load_deployment_profile
from seedpod.core.clock import Clock
from seedpod.core.errors import ErrorCode, InfrastructureUnreachableError, PermanentError
from seedpod.data.repositories import (
    ClusterRow,
    DeploymentRepository,
    Repositories,
    SnapshotRepository,
    SnapshotRow,
    WorkflowRunRow,
)
from seedpod.data.uow import UnitOfWork
from seedpod.providers.contract import KubeGetPods, KubeRun, Result
from seedpod.providers.kubectl import KubectlProvider
from seedpod.services.crypto import CryptoService

__all__ = ["SnapshotService", "SnapshotNotFound", "SnapshotCreationFailed", "SnapshotIncompatible", "RestoreResult"]

_DEFAULT_COMMANDS: Mapping[str, Mapping[str, str]] = {
    "postgres": {
        "dump": "pg_dump -U {username} -d {database} -Fc",
        "restore": "pg_restore -U {username} -d {database} --clean --if-exists",
    },
    "mysql": {
        "dump": "mysqldump -u {username} {database}",
        "restore": "mysql -u {username} {database}",
    },
}
_DUMP_TIMEOUT_S = 300.0
_NAMESPACE = "default"


class SnapshotNotFound(LookupError):
    pass


class SnapshotCreationFailed(PermanentError):
    def __init__(self, cluster_id: str, reason: str) -> None:
        super().__init__(
            f"snapshot of cluster {cluster_id} failed: {reason}",
            code=ErrorCode.INVALID_INPUT,
            provider="snapshot-service",
            command="create",
            detail={"cluster_id": cluster_id},
        )


class SnapshotIncompatible(PermanentError):
    """DR-0030 fix 2 -- ``restore``'s pre-flight compatibility check, ported from
    v1's ``_perform_snapshot_restore`` (``reference-code/seedpod/seedpod/jobs/
    state/deployment_job.py:296-313``): before attempting anything, the
    snapshot's own service names are compared against the TARGET's services
    that declare persistence. v1's message, salvaged close to verbatim
    (:305-309) -- naming the missing services AND both profile names, rather
    than failing late via "pod_name is None" (indistinguishable from "the pod
    isn't up yet")."""

    def __init__(self, *, target_profile: str, snapshot_profile: str, missing_services: Sequence[str]) -> None:
        super().__init__(
            f"Target profile {target_profile!r} missing persistence config for: "
            f"{', '.join(missing_services)}. Snapshot from profile {snapshot_profile!r}.",
            code=ErrorCode.INVALID_INPUT,
            provider="snapshot-service",
            command="restore",
            detail={
                "target_profile": target_profile,
                "snapshot_profile": snapshot_profile,
                "missing_services": list(missing_services),
            },
        )


class RestoreResult:
    """Plain DTO -- ``POST /api/snapshots/{id}/restore``'s response shape."""

    def __init__(self, *, success: bool, services_restored: list[str], services_failed: list[str], error: str | None):
        self.success = success
        self.services_restored = services_restored
        self.services_failed = services_failed
        self.error = error


def _persistable_services(raw_profile: Mapping[str, Any]) -> list[tuple[str, Mapping[str, Any]]]:
    services = raw_profile.get("services") or {}
    return [
        (name, cfg["persistence"])
        for name, cfg in services.items()
        if isinstance(cfg, Mapping) and cfg.get("persistence")
    ]


_GZIP_MAGIC = b"\x1f\x8b"


def _dump_bytes(dump_path: Path) -> bytes:
    """The dump's bytes, transparently decompressed when it is gzipped.

v2 now writes ``<service>.dump.gz``, matching v1
    (``reference-code/seedpod/seedpod/services/snapshot_service.py:222-224``) --
    backlog P2 #8. This sniffing predates that and is deliberately KEPT: it is what
    lets v1's existing snapshots restore, and what keeps every snapshot v2 took
    while it still wrote raw ``.dump`` files working unchanged.

    Sniffs the two-byte gzip magic rather than trusting the ``.gz`` suffix: the
    filename is whatever ``services[].file`` recorded, and a snapshot row imported
    from v1 can disagree with the bytes on disk. The content is the authority.
    """
    raw = dump_path.read_bytes()
    if raw[:2] == _GZIP_MAGIC:
        return gzip.decompress(raw)
    return raw


async def _read_dump_bytes(dump_path: Path) -> bytes:
    """``_dump_bytes`` off the event loop, in ONE thread hop (module docstring).
    The read and the decompress travel together deliberately -- splitting them
    would hand the loop a whole dump's worth of bytes between two hops for no
    gain, since nothing can act on the compressed half."""
    return await asyncio.to_thread(_dump_bytes, dump_path)


def _write_dump(dump_path: Path, payload: bytes) -> int:
    """Compress, write, and measure -- returning the size so the caller needs no
    second ``stat()`` round trip. Sync by design: ``create()`` calls it through a
    single ``asyncio.to_thread`` (module docstring).

    ``mtime=0`` keeps the bytes deterministic for identical input (gzip otherwise
    stamps the current time into the header), which matters because
    ``total_size_bytes`` is recorded in the row and compared in tests."""
    dump_path.write_bytes(gzip.compress(payload, mtime=0))
    return dump_path.stat().st_size


def _command(persistence: Mapping[str, Any], kind: str) -> str:
    explicit = persistence.get(f"{kind}_command")
    if explicit:
        return str(explicit)
    template = _DEFAULT_COMMANDS.get(persistence.get("type", ""), {}).get(kind, "")
    return template.format(
        username=persistence.get("username", ""), database=persistence.get("database", ""),
    )



def _format_snapshot_name(pattern: str, cluster: ClusterRow, now: datetime) -> str:
    """Salvaged verbatim in behaviour from v1's ``ClusterManager._format_snapshot_name``
    (reference-code/seedpod/seedpod/orchestrator/cluster_manager.py:797-819), including
    its branch sanitisation (``/`` and ``\\`` -> ``-``, truncated to 50) and its two
    time formats. ``name_pattern`` had zero references anywhere in v2 before DR-0040 --
    three shipped profiles declared it and nothing had ever read one."""
    branch = (cluster.branch or "unknown").replace("/", "-").replace("\\", "-")[:50]
    substitutions = {
        "cluster_slug": cluster.slug or "unknown",
        "cluster_id": cluster.id[:8],
        "branch": branch,
        "date": now.strftime("%Y-%m-%d"),
        "time": now.strftime("%H%M%S"),
    }
    result = pattern
    for key, value in substitutions.items():
        result = result.replace(f"{{{key}}}", value)
    return result


class SnapshotService:
    def __init__(
        self,
        snapshots: SnapshotRepository,
        repos: Repositories,
        deployments: DeploymentRepository,
        crypto: CryptoService,
        kubectl_provider: KubectlProvider,
        uow: UnitOfWork,
        clock: Clock,
        id_gen: Callable[[], str],
        config_dir: Path,
        storage_dir: Path,
    ) -> None:
        self._snapshots = snapshots
        self._repos = repos
        self._deployments = deployments
        self._crypto = crypto
        self._kubectl = kubectl_provider
        self._uow = uow
        self._clock = clock
        self._id_gen = id_gen
        self._config_dir = config_dir
        self._storage_dir = storage_dir

    # -------------------------------------------------------------------
    # CRUD reads
    # -------------------------------------------------------------------

    async def get(self, snapshot_id: str) -> SnapshotRow:
        async with self._uow() as tx:
            row = self._snapshots.get(tx, snapshot_id)
        if row is None:
            raise SnapshotNotFound(snapshot_id)
        return row

    async def list(self, *, branch: str | None = None, profile: str | None = None) -> list[SnapshotRow]:
        async with self._uow() as tx:
            return self._snapshots.list(tx, branch=branch, profile=profile)

    async def delete(self, snapshot_id: str) -> None:
        async with self._uow() as tx:
            row = self._snapshots.get(tx, snapshot_id)
            if row is None:
                raise SnapshotNotFound(snapshot_id)
            self._snapshots.delete(tx, snapshot_id)
        with contextlib.suppress(OSError):  # best-effort -- the DB row is authoritative
            # Off-loop like every other file touch here (module docstring): a
            # snapshot directory holds whole database dumps, so the unlink walk
            # is not the constant-time operation the one-line call looks like.
            await asyncio.to_thread(shutil.rmtree, Path(row.storage_path), ignore_errors=True)

    # -------------------------------------------------------------------
    # Create
    # -------------------------------------------------------------------

    async def _kubeconfig_and_profile(self, cluster: ClusterRow) -> tuple[str, str, dict[str, Any]]:
        if cluster.encrypted_kubeconfig is None or cluster.kubeconfig_key_class is None:
            raise SnapshotCreationFailed(cluster.id, "cluster has no kubeconfig yet")
        kubeconfig = self._crypto.decrypt(cluster.encrypted_kubeconfig, cluster.kubeconfig_key_class)
        async with self._uow() as tx:
            deployment = self._deployments.active_for_cluster(tx, cluster.id)
            if deployment is None:
                rows = self._deployments.list_for_cluster(tx, cluster.id)
                deployment = rows[0] if rows else None
        if deployment is None:
            raise SnapshotCreationFailed(cluster.id, "cluster has no deployment to derive a profile from")
        _, raw_profile = load_deployment_profile(self._config_dir, deployment.manifest_version)
        return kubeconfig, deployment.manifest_version, raw_profile

    async def _find_pod(self, kubeconfig: str, service_name: str) -> str | None:
        pods = await self._kube(KubeGetPods(kubeconfig=kubeconfig, namespace=_NAMESPACE))
        for pod in pods:
            if pod.name.startswith(f"{service_name}-") and pod.status == "Running":
                return pod.name
        return None

    async def _kube(self, cmd) -> Any:
        result = None
        async for event in self._kubectl.execute(cmd):
            if isinstance(event, Result):
                result = event.value
        return result

    async def create(
        self,
        cluster: ClusterRow,
        *,
        name: str,
        description: str | None,
        created_by: str,
        is_auto: bool = False,
    ) -> SnapshotRow:
        """Raises ``SnapshotCreationFailed``/``PermanentError``/a live
        ``ProviderError`` on any failure -- the caller (``POST /api/snapshots``)
        wants a real error, not a swallowed one. ``attempt_pre_destroy_snapshot``
        below is the fail-open wrapper DR-0020's pre-destroy call site uses."""
        kubeconfig, profile_name, raw_profile = await self._kubeconfig_and_profile(cluster)
        persistable = _persistable_services(raw_profile)
        if not persistable:
            raise SnapshotCreationFailed(cluster.id, f"profile {profile_name!r} has no persistable services")

        snapshot_id = self._id_gen()
        snapshot_dir = self._storage_dir / snapshot_id
        await asyncio.to_thread(snapshot_dir.mkdir, parents=True, exist_ok=True)

        service_infos: list[dict[str, Any]] = []
        total_size = 0
        for service_name, persistence in persistable:
            pod_name = await self._find_pod(kubeconfig, service_name)
            if pod_name is None:
                raise SnapshotCreationFailed(cluster.id, f"no running pod found for service {service_name!r}")
            dump_command = _command(persistence, "dump")
            output = await self._kube(
                KubeRun(
                    kubeconfig=kubeconfig,
                    args=("exec", pod_name, "-n", _NAMESPACE, "--", "sh", "-c", dump_command),
                    timeout_s=_DUMP_TIMEOUT_S,
                    binary=True,
                )
            )
            # Backlog P2 #8: match v1's compression on the WRITE side. v1 wrote
            # `<service>.dump.gz` (reference-code .../services/snapshot_service.py),
            # v2 wrote raw -- so v2's own snapshots were several times larger than
            # v1's for identical content, on a path that keeps every snapshot
            # forever. `_dump_bytes` has sniffed the gzip magic on the read side
            # since the smoke-10 restore fix, so BOTH forms already restore and
            # this changes nothing about reading -- including for snapshots taken
            # before this landed, which stay raw on disk and keep working.
            #
            # The compress+write+size is ONE `asyncio.to_thread` hop (module
            # docstring): this is the line that used to freeze the whole process
            # for the length of a `gzip.compress` over the user's database.
            dump_path = snapshot_dir / f"{service_name}.dump.gz"
            size = await asyncio.to_thread(_write_dump, dump_path, output.stdout)
            total_size += size
            service_infos.append(
                {
                    "service_name": service_name,
                    "persistence_type": persistence.get("type", ""),
                    "file": dump_path.name,
                    "size_bytes": size,
                    "database": persistence.get("database"),
                    "username": persistence.get("username"),
                }
            )

        row = SnapshotRow(
            id=snapshot_id, name=name, description=description, source_cluster_id=cluster.id,
            source_cluster_slug=cluster.slug, branch=cluster.branch, deployment_profile=profile_name,
            services=service_infos, storage_path=str(snapshot_dir), total_size_bytes=total_size,
            is_auto=is_auto, created_by=created_by, created_at=self._clock.now(),
        )
        async with self._uow() as tx:
            self._snapshots.insert(tx, row)
        return row

    async def attempt_pre_destroy_snapshot(self, cluster: ClusterRow, *, actor: str) -> SnapshotRow | None:
        """DR-0020: fail-open, best-effort. ANY failure (no kubeconfig, no
        deployment, unreadable profile, no persistable services, a live
        kubectl error) is swallowed and reported as ``None`` -- destroy proceeds
        regardless, verbatim v1 ``_attempt_auto_snapshot``'s own
        ``except Exception`` fail-open (cited by DR-0020).

        **The ``status == "active"`` gate is GONE (DR-0043), and its removal is the
        same argument that justified it.** It was right while this ran from inside
        ``ClusterService.destroy``, BEFORE anything was dispatched -- a cluster that
        was not active had no business being snapshotted on the way out. DR-0043
        moved the call site into the destroy WORKFLOW (``cluster.auto_snapshot``), by
        which point the machine has already moved the cluster to DESTROYING, so the
        gate would now skip 100% of the time and this would ship as silently inert.

        That is not a hypothetical: it is precisely the trap DR-0040 documents having
        avoided when it built ``attempt_auto_snapshot`` below with no such gate ("the
        same guard would have skipped 100% of the time. That would have shipped as
        'still inert', the exact bug this DR exists to fix"). The gate belonged to a
        call site that no longer exists; what actually protects against snapshotting a
        cluster that should not be is the guard chain in ``ClusterService.destroy``,
        which still runs before the event is dispatched at all."""
        name = f"auto-{cluster.slug}-{self._clock.now().strftime('%Y-%m-%d')}"
        try:
            return await self.create(
                cluster, name=name, description=f"Auto-snapshot before destroy (actor={actor})",
                created_by="system:pre-destroy", is_auto=True,
            )
        except Exception:  # noqa: BLE001 -- fail-open by design, see docstring above
            return None

    async def attempt_auto_snapshot(self, cluster: ClusterRow, *, actor: str) -> SnapshotRow | None:
        """DR-0040: the profile's own ``auto_snapshot`` block, honoured on an
        UNATTENDED destroy. Called by the ``cluster.auto_snapshot`` verb, which fires
        only when the machine stamped ``trigger="ttl_expiry"`` onto ``DestroyDue``.

        **No status gate, deliberately** -- unlike ``attempt_pre_destroy_snapshot``
        above, whose ``!= "active"`` check is right for ITS call site (inside
        ``ClusterService.destroy``, before anything is dispatched). This one runs from
        inside the destroy WORKFLOW, by which point the machine has already moved the
        cluster to DESTROYING, so the same guard would skip every single time. What
        actually matters -- is there a kubeconfig, is there a profile, does it have
        persistable services -- is already checked, and raised on, downstream.

        Fail-open like its sibling: ANY failure is swallowed and reported as ``None``.
        A TTL destroy is a deadline; a snapshot that cannot be taken must never strand
        a cluster the TTL says must die."""
        try:
            _kubeconfig, profile_name, raw_profile = await self._kubeconfig_and_profile(cluster)
        except Exception:  # noqa: BLE001 -- fail-open by design, see docstring
            return None

        config = raw_profile.get("auto_snapshot") or {}
        if not config.get("enabled", False):
            return None

        pattern = config.get("name_pattern") or "auto-{cluster_slug}-{date}"
        name = _format_snapshot_name(pattern, cluster, self._clock.now())
        try:
            return await self.create(
                cluster, name=name,
                description=f"Auto-snapshot before unattended destroy (profile={profile_name})",
                created_by=f"system:{actor}", is_auto=True,
            )
        except Exception:  # noqa: BLE001 -- fail-open by design, see docstring
            return None

    # -------------------------------------------------------------------
    # Restore
    # -------------------------------------------------------------------

    async def restore(
        self,
        snapshot_id: str,
        *,
        cluster_id: str,
        services: Sequence[str] | None = None,
        run_migrations: bool = True,  # noqa: ARG002 -- no migration-runner verb exists yet; accepted for wire parity
        actor: str,
    ) -> RestoreResult:
        snapshot = await self.get(snapshot_id)
        async with self._uow() as tx:
            cluster = self._repos.clusters.get(tx, cluster_id)
        if cluster is None:
            raise SnapshotNotFound(f"cluster {cluster_id} not found")  # 404 either way at the router

        restored: list[str] = []
        failed: list[str] = []
        error: str | None = None
        started_at = self._clock.now()

        try:
            # DR-0030 fix 2 -- pre-flight compatibility check, BEFORE attempting
            # anything (no kubeconfig decrypt, no pod-exec) -- v1's own ordering
            # (``deployment_job.py:300-313``): a snapshot whose services have no
            # persistence counterpart on the TARGET can never succeed, so it fails
            # immediately, naming the missing services, rather than late and
            # generically via "pod_name is None -> failed.append(...)".
            try:
                target_profile_name, target_persistent_services = await self._target_persistence_service_names(
                    cluster
                )
            except PermanentError:
                # v1's own fail-open guard (``deployment_job.py:298-300``):
                # ``profile = manifest_resolver.manifests.get(deployment_profile_name)``
                # then ``if profile:`` -- an unresolvable target profile (no
                # deployment row yet to derive one from, or a deployment
                # profile YAML since renamed/removed) SKIPS the compatibility
                # check rather than blocking the restore attempt on a fact v1
                # never required either. ``_target_persistence_service_names``
                # raises exactly two ``PermanentError`` subclasses for exactly
                # these two cases (``SnapshotCreationFailed``, and
                # ``load_deployment_profile``'s own ``PermanentError(NOT_FOUND)``
                # -- ``seedpod/app/services/profiles.py``) -- both are "could
                # not derive a profile to compare against", never "compared
                # and found incompatible", so both fall open here exactly like
                # v1's own bare ``if profile:``.
                target_profile_name, target_persistent_services = None, None

            if target_persistent_services is not None:
                snapshot_service_names = {info["service_name"] for info in snapshot.services}
                missing = sorted(snapshot_service_names - target_persistent_services)
                if missing:
                    # DR-0030 fix 2's own point: this must NOT be swallowed by
                    # the blanket `except Exception` below the way a bare raise
                    # here would be -- re-raised past it explicitly so
                    # `deploy.restore_snapshot` (and any other caller) can tell
                    # "permanently incompatible" apart from "not yet failed
                    # enough attempts". See the `except SnapshotIncompatible`
                    # arm below.
                    raise SnapshotIncompatible(
                        target_profile=target_profile_name, snapshot_profile=snapshot.deployment_profile,
                        missing_services=missing,
                    )

            if cluster.encrypted_kubeconfig is None or cluster.kubeconfig_key_class is None:
                raise SnapshotCreationFailed(cluster_id, "target cluster has no kubeconfig yet")
            kubeconfig = self._crypto.decrypt(cluster.encrypted_kubeconfig, cluster.kubeconfig_key_class)
            wanted = set(services) if services else None
            for info in snapshot.services:
                service_name = info["service_name"]
                if wanted is not None and service_name not in wanted:
                    continue
                dump_path = Path(snapshot.storage_path) / info["file"]
                pod_name = await self._find_pod(kubeconfig, service_name)
                if pod_name is None or not dump_path.exists():
                    failed.append(service_name)
                    continue
                persistence = {"type": info["persistence_type"], "database": info.get("database"), "username": info.get("username")}
                restore_command = _command(persistence, "restore")
                # Read + decompress off the loop (module docstring), hoisted out of
                # the KubeRun(...) construction so the thread hop is visible rather
                # than buried in an argument list.
                dump = await _read_dump_bytes(dump_path)
                await self._kube(
                    KubeRun(
                        kubeconfig=kubeconfig,
                        # `-i` is load-bearing: without it kubectl never attaches the
                        # pod's stdin and the dump is discarded at the remote end.
                        args=("exec", "-i", pod_name, "-n", _NAMESPACE, "--", "sh", "-c", restore_command),
                        timeout_s=_DUMP_TIMEOUT_S,
                        binary=True,
                        stdin=dump,
                    )
                )
                restored.append(service_name)
        except InfrastructureUnreachableError:
            # DR-0030 fix 1 -- CLAUDE.md's hard rule: InfrastructureUnreachableError
            # "never triggers compensation and is never conflated with absence". A
            # restore that could not DETERMINE whether it worked is not a restore
            # that FAILED -- propagate it, don't flatten it into `RestoreResult.error`
            # the way the blanket `except Exception` below would.
            raise
        except SnapshotIncompatible:
            # DR-0030 fix 2's own point, made structurally: a pre-flight
            # incompatibility is PROVABLY unfixable by retrying (unlike "pod
            # not up yet") -- it must reach the caller as a raised, distinctly
            # typed ``PermanentError``, not get flattened into
            # ``RestoreResult(success=False, error=str(exc))`` by the blanket
            # ``except Exception`` below, which would make it indistinguishable
            # from a merely-not-ready-yet failure and get silently RETRIED by
            # any caller (``deploy.restore_snapshot``) that treats every
            # ``success=False`` as transient.
            raise
        except Exception as exc:  # noqa: BLE001 -- recorded below, never crashes the request
            error = str(exc)

        finished_at = self._clock.now()
        status = "succeeded" if not failed and error is None else "failed"
        run_row = WorkflowRunRow(
            id=self._id_gen(), workflow="snapshot-restore", workflow_version=1, cluster_id=cluster_id,
            deployment_id=None, dedupe_key=None,
            args={
                "snapshot_id": snapshot_id, "snapshot_name": snapshot.name, "snapshot_branch": snapshot.branch,
                "services_total": len(snapshot.services), "services_completed": len(restored),
                "services_restored": restored, "services_failed": failed,
            },
            status=status, cancel_requested=False, failed_step=None,
            error={"message": error} if error else None, undo_incomplete=None,
            initiated_by=actor, created_at=started_at, started_at=started_at, finished_at=finished_at,
        )
        async with self._uow() as tx:
            self._repos.workflow_runs.insert(tx, run_row)

        return RestoreResult(success=status == "succeeded", services_restored=restored, services_failed=failed, error=error)

    async def _target_persistence_service_names(self, cluster: ClusterRow) -> tuple[str, frozenset[str]]:
        """DR-0030 fix 2's own profile lookup -- deliberately NOT
        ``_kubeconfig_and_profile`` above: v1's compatibility check
        (``deployment_job.py:300-313``) runs BEFORE any kubeconfig is needed
        (``manifest_resolver.manifests.get(...)`` there does no cluster IO
        either), so reusing ``_kubeconfig_and_profile`` here would reorder the
        failure -- a target with neither a kubeconfig NOR a compatible profile
        would report "no kubeconfig yet" instead of naming the real mismatch.
        Same deployment/profile resolution ``_kubeconfig_and_profile`` does
        (``active_for_cluster``, falling back to the newest deployment row),
        minus the kubeconfig decrypt. Returns ``(profile_name,
        persistence_declaring_service_names)``."""
        async with self._uow() as tx:
            deployment = self._deployments.active_for_cluster(tx, cluster.id)
            if deployment is None:
                rows = self._deployments.list_for_cluster(tx, cluster.id)
                deployment = rows[0] if rows else None
        if deployment is None:
            raise SnapshotCreationFailed(cluster.id, "cluster has no deployment to derive a profile from")
        _, raw_profile = load_deployment_profile(self._config_dir, deployment.manifest_version)
        persistent_names = frozenset(name for name, _ in _persistable_services(raw_profile))
        return deployment.manifest_version, persistent_names

    async def restore_history(self, cluster_id: str) -> list[WorkflowRunRow]:
        async with self._uow() as tx:
            rows = self._repos.workflow_runs.list_all(tx)
        return [r for r in rows if r.workflow == "snapshot-restore" and r.cluster_id == cluster_id]
