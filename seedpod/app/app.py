"""``App``/``Services`` -- the composition root's handle (docs/design/seam-d-
foundation.md Decision 8), AS AMENDED by docs/design/coherence-review.md
Conflict 15 (``TimerService``, ``Provider.check_ready`` called once at startup,
the amended ``start()``/``stop()`` order -- OVERRIDES Seam D's own excerpt per
CLAUDE.md's precedence rule). All IO lives here, not in ``factory.build_app()``.

``Services`` carries two already-built Round-5 runtime services
(``reconciliation``/``health``) plus the four thin application services
(``clusters``/``deployments``/``secrets``/``api_keys`` -- ``seedpod/app/
services/``), all fully wired by ``factory.build_app()``.

Also carries DR-0015's ``http_transport``/``ghcr``/``dns``/``manifest_resolver``
fields (the fourth ``build_app`` seam, ratified 2026-07-17): fully wired here too,
since DR-0015 assigns their construction to this composition-root component.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
from fastapi import FastAPI

from seedpod.app.config import AppConfig
from seedpod.core.clock import Clock
from seedpod.core.errors import ErrorCode, PermanentError
from seedpod.core.tempfiles import TempFileRegistry
from seedpod.data.database import Database
from seedpod.data.migrate import MIGRATIONS_DIR, migrate
from seedpod.data.repositories import Repositories
from seedpod.data.uow import UnitOfWork
from seedpod.engine.steps.cluster import SshIdentity
from seedpod.providers.contract import Provider
from seedpod.runtime.dispatcher import Dispatcher
from seedpod.runtime.effect_executor import EffectExecutor
from seedpod.runtime.health import HealthMonitor
from seedpod.runtime.reconciliation import ReconciliationService
from seedpod.runtime.sse import SSEHub
from seedpod.runtime.subprocess_manager import SubprocessManager
from seedpod.runtime.timers import TimerService
from seedpod.services.crypto import CryptoService
from seedpod.services.dns import DnsService
from seedpod.services.ghcr import GhcrService
from seedpod.services.manifests import ManifestResolver
from seedpod.services.rules import RuleEngine

if TYPE_CHECKING:
    from seedpod.engine.engine import WorkflowEngine

__all__ = ["App", "Services", "verify_ssh_identities"]


def verify_ssh_identities(
    enabled_providers: Iterable[str], ssh_identities: Mapping[str, SshIdentity]
) -> None:
    """A configured SSH private key that is not on disk fails at STARTUP, beside
    ``check_ready()``, not 35 seconds into a provision.

    ``TartProvider.check_ready`` already states this principle ("fail at startup, not
    mid-provision") and applies it to the tart binary and the base image -- but the SSH
    key is not one of its inputs (``TartConfig`` does not carry it; DR-0023 makes it the
    config loader's concern), so nothing checked it anywhere. The 2026-08-12 tart run
    found the cost: ``config/providers/tart.yml`` names ``~/.ssh/id_ed25519``, that file
    did not exist on the host, and ``ssh`` silently fell back to another key -- so
    provisioning worked by luck, while every failure on the ssh-k3s plane carried a
    ``Warning: Identity file ... not accessible`` prefix that was not the cause of
    anything and led diagnosis away from the real failure twice. Fail loudly, or do not
    mention it at all.

    Scoped to the ENABLED providers deliberately: a tart-only host legitimately has no
    DigitalOcean key and vice versa, so a provider nobody enabled must never stop the
    server booting. An UNCONFIGURED path (``None``) is not an error either -- ``LoadSpec``
    resolves it to ``SshIdentity()`` and ``engine/steps/k3s.py``'s ``_target`` raises its
    own clear message at the step that actually needed it."""
    for name in enabled_providers:
        identity = ssh_identities.get(name)
        if identity is None or identity.private_key_path is None:
            continue
        if not Path(identity.private_key_path).is_file():
            raise PermanentError(
                f"provider {name!r} is enabled and its config names SSH private key "
                f"{identity.private_key_path!r}, which does not exist -- ssh would "
                f"fall back to another key, so provisioning would work only by luck",
                code=ErrorCode.NOT_FOUND,
                provider=name,
                command="verify_ssh_identities",
                detail={"private_key_path": identity.private_key_path},
            )


@dataclass
class Services:
    """The application-service container ``api/`` handlers depend on
    (``Depends(get_app)`` -> ``app.services.<x>``). Grows additively, same
    discipline as ``data/repositories.py``'s ``Repositories`` bundle -- never
    restructures existing fields.

    ``clusters``/``deployments``/``secrets``/``api_keys``/``presets``/
    ``snapshots`` are typed ``Any | None`` (rather than importing
    ``seedpod.app.services`` here) to keep this module's own import surface
    minimal -- every field is always a real, fully-constructed
    ``ClusterService``/``DeploymentService``/``SecretService``/``ApiKeyService``/
    ``PresetService``/``SnapshotService`` instance by the time ``build_app()``
    returns; ``reconciliation``/``health`` are the real, already-committed
    Round-5 runtime services, always constructed. ``presets``/``snapshots`` are
    Round-6 api-features additions -- grows additively, same discipline as
    ``data/repositories.py``'s ``Repositories`` bundle."""

    clusters: Any | None
    deployments: Any | None
    secrets: Any | None
    api_keys: Any | None
    presets: Any | None
    snapshots: Any | None
    reconciliation: ReconciliationService
    health: HealthMonitor


@dataclass
class App:
    """The composition root's one handle. Every field is a fully-constructed,
    already-wired collaborator -- ``build_app()`` assembled the whole acyclic
    graph; nothing here is set post-construction except through
    ``start()``/``stop()``'s own lifecycle calls."""

    config: AppConfig
    db: Database
    crypto: CryptoService
    hub: SSEHub
    subprocesses: SubprocessManager
    repos: Repositories
    uow: UnitOfWork
    providers: Mapping[str, Provider]
    # DR-0023's per-provider SSH identity, carried here (not just into the step
    # registry) so `start()` can verify the key files exist alongside `check_ready`.
    ssh_identities: Mapping[str, SshIdentity]
    rules: RuleEngine  # Round 6, api-deployments: the SAME instance DeploymentService was
    #                     constructed with (factory.py's one `rules` local) -- exposed here too
    #                     so the rules-admin router (POST /api/rules/{name}/disable|enable,
    #                     POST /api/rules/reload) can mutate its live `.config` in place
    #                     (`seedpod/app/services/rules_admin.py`) without reaching into
    #                     DeploymentService's private `_rules` attribute. Mirrors this
    #                     dataclass's existing "grows additively" discipline (see docstring).
    dispatcher: Dispatcher
    timers: TimerService
    engine: WorkflowEngine
    executor: EffectExecutor
    services: Services
    api: FastAPI
    http_transport: httpx.AsyncClient  # DR-0015: shared outbound-HTTP seam for GHCR/DNS
    owns_http_transport: bool  # True iff build_app constructed http_transport itself
    #                            (http_transport=None) -- only then does stop() close it;
    #                            a caller/test-injected client stays open for reuse.
    ghcr: GhcrService | None  # DR-0015: credential-gated on config.github_token
    dns: DnsService | None  # DR-0015: credential-gated on config.cloudflare_api_token
    manifest_resolver: ManifestResolver  # always constructed; ghcr_service=None degrades gracefully
    clock: Clock  # Round 6, api-edge: the same Clock every other collaborator was built
    #                with (dispatcher/timers/executor/hub/... each hold their own private
    #                copy already) -- exposed here too because CLAUDE.md's "every timestamp
    #                via the injected Clock, no now()" binds seedpod/api/ handlers just as
    #                much as seedpod/core/, and no existing collaborator exposes a public
    #                accessor for the one it was constructed with.

    async def start(self) -> None:
        """ALL IO lives here, not in ``build_app()`` (Decision 8). Amended order
        (coherence-review Conflict 15):

        1. Apply migrations once -- the single schema authority.
        2. Sweep leaked temp files from a prior incarnation (H17, Seam C).
        3. ``check_ready()`` every enabled provider -- fail at startup, not
           mid-provision (Seam C's own ``check_ready`` docstring: "called once by
           the composition root before serving").
        4. Drain everything already pending in the outbox FIRST (H7 crash
           replay) -- the executor is correctness, always runs.
        5. Start the timer poller -- also correctness (a due TTL/health timer
           must fire whether or not the reconciler is on), always runs.
        6. Only when ``background_tasks`` (off in tests, per conftest): resume
           in-flight workflow runs, then start the reconciler and the health
           poller (mirrored, per this round's brief).

        Mirrored in ``stop()`` (DR-0024): only ``resume_inflight()`` sits behind
        ``background_tasks`` -- run ADMISSION does not, so ``stop()``'s
        ``engine.stop()`` is unconditional even though this ``start()`` step is
        not. The asymmetry is deliberate, not an oversight.
        """
        migrate(self.db.engine, MIGRATIONS_DIR)
        TempFileRegistry.sweep()
        for provider in self.providers.values():
            await provider.check_ready()
        self._verify_ssh_identities()
        await self.executor.start()
        await self.timers.start()
        if self.config.background_tasks:
            await self.engine.resume_inflight()
            await self.services.reconciliation.start()
            await self.services.health.start()
            # DR-0044. Behind the flag like the other periodic loops (tests stay
            # deterministic and call `flush_last_used()` directly), but note the
            # DELIBERATE asymmetry in `stop()`: the final flush is unconditional.
            await self.services.api_keys.start()

    def _verify_ssh_identities(self) -> None:
        """Thin bind of ``verify_ssh_identities`` to this App's own enabled providers.
        The logic is a module function so it is testable without constructing a whole
        ``App`` (whose ``start()`` also shells out to every provider's
        ``check_ready()``, making a start-level test depend on the developer's local
        tart/kubectl install)."""
        verify_ssh_identities(self.providers.keys(), self.ssh_identities)

    async def stop(self) -> None:
        """Exact reverse of ``start()``. Idempotent (every collaborator's own
        ``stop()`` is idempotent; calling ``App.stop()`` twice is safe).

        ``engine.stop()`` (DR-0024, ratified 2026-08-03) runs FIRST among the
        runtime collaborators and is deliberately NOT gated on
        ``background_tasks``: ``EffectExecutor`` admits runs via
        ``engine.start(run_id)`` from its drain loop, and the executor always
        runs, so live run tasks exist even with the flag off. Quiescing the
        engine before ``timers``/``executor``/``subprocesses``/``db`` is what
        makes the rest of this ordering meaningful -- otherwise
        ``subprocesses.shutdown()`` SIGKILLs a live ``k3s.install``'s ssh child
        out from under it, and ``db.dispose()`` can land mid-transaction.
        Shutdown is an INTERRUPTION, never a cancellation: see DR-0024 and
        ``WorkflowEngine.stop()``'s own docstring for why reusing ``cancel()``
        here would destroy in-flight clusters on every restart.

        (An earlier revision of this docstring recorded that no ``engine.stop()``
        existed and flagged it as a DR-worthy follow-up. DR-0024 is that DR.)
        """
        if self.config.background_tasks:
            await self.services.health.stop()
            await self.services.reconciliation.stop()
        await self.engine.stop()
        await self.timers.stop()
        await self.executor.stop()
        # DR-0044: NOT gated on `background_tasks`, unlike its `start()` -- the same
        # deliberate asymmetry `engine.stop()` above documents. `stop()` cancels the
        # loop (a no-op if it never started) and then flushes whatever `validate()`
        # buffered, so a clean shutdown never drops touches just because the interval
        # had not elapsed. MUST precede `db.dispose()`, which is why it sits here.
        await self.services.api_keys.stop()
        await self.subprocesses.shutdown()
        await self.hub.close(grace_period=0.5)
        self.db.dispose()
        if self.owns_http_transport:
            await self.http_transport.aclose()

    @asynccontextmanager
    async def running(self):
        await self.start()
        try:
            yield self
        finally:
            await self.stop()
