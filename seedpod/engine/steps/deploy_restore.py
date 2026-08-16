"""engine/steps/deploy_restore.py — Round 10's "restore-and-rehydrate"
component: ``deploy.restore_snapshot``, the LAST of the seven deploy-path
verbs. ``plane="domain"`` (``tests/engine/declared_verbs.py``'s own comment:
"only deploy.load_audit/plan_waves/restore_snapshot ... are domain") -- this
Step issues no Seam C ``ProviderCommand`` itself; it delegates to the
already-built, already-tested ``SnapshotService`` (``seedpod/app/services/
snapshot_service.py``, Round 6's DR-0020 collaborator) exactly the same way
``dns.delete_record`` (the ONE prior ``plane="service"`` verb) delegates to
``DnsService`` rather than re-issuing raw provider commands itself.

Salvaged from v1's ``_perform_snapshot_restore``
(``reference-code/seedpod/seedpod/jobs/state/deployment_job.py:222-300``):
resolve ``data_initialization`` to a concrete snapshot id (explicit
``restore_from_snapshot``, or ``restore_from_latest`` criteria -- branch/
profile/max_age_days, most-recent-first), then restore it. Two salvage
decisions worth naming precisely, because they diverge from a naive port:

- **No snapshot resolved is a no-op, not an error** -- v1's own log line,
  verbatim: "No snapshot found matching data_initialization criteria ...
  Not an error - just no data to restore" (``deployment_job.py:266-269``).
  ``execute`` below returns ``EmptyOutput()`` for both "``spec`` is ``None``"
  (DR-0028's own words: "a ``SnapshotRestoreSpec`` with NEITHER mode set
  resolves to ... a verb-level no-op ``deploy.restore_snapshot`` decides")
  AND "criteria matched zero snapshots" -- the SAME outcome, never a raise.
- **``restore_from_latest``'s criteria resolve HERE, at execute time, not at
  deployment birth** (DR-0028 decision 3's own ruling): "resolving it at
  deployment birth would freeze a choice that a newer snapshot may supersede
  before the restore actually runs". ``self._snapshots.list(...)`` (the
  ``SnapshotRepository``'s own ``ORDER BY created_at DESC``, already
  most-recent-first -- v1's own comment) is called fresh on every attempt,
  including every ``retry:`` re-attempt this workflow step's own explicit
  Schedule declares (``config/workflows/deploy-waves.yml``'s ``restore`` step,
  ``max_attempts: 19``) -- a snapshot that supersedes an in-flight restore's
  original choice is picked up automatically on the next attempt, never
  frozen.

**``ctx.cluster_id``, not a Params field.** ``RestoreSnapshotParams`` carries
no ``cluster_id`` (``tests/engine/declared_verbs.py``'s own committed shape:
``{kubeconfig, spec}``) -- ``deploy-waves.yml``'s ``restore`` step runs inside
a workflow whose run was admitted for THIS deployment
(``core/machine.py``'s ``RunWorkflow(workflow="deploy", cluster_id=record.
cluster_id, deployment_id=record.id)``), so ``StepContext.cluster_id`` IS the
deployment's cluster id, read implicitly -- the identical idiom
``declared_verbs.py``'s own ``EmptyParams`` docstring names for
``cluster.load_kubeconfig`` ("the step reads ``ctx.cluster_id``/``ctx.run_id``
implicitly rather than via a binding"), generalized here to a Step whose
Params are non-empty but still omit a fact ``ctx`` already carries.

**``params.kubeconfig`` is bound (matching every sibling step in this SAME
wave -- ``kube.apply_docs``/``deploy.ensure_rollouts``/``deploy.await_wave``
all take one) but is NOT read by this Step's own ``execute()``.**
``SnapshotService.restore`` (the collaborator this verb delegates to) derives
its OWN kubeconfig from ``cluster_id`` via ``ClusterRow.encrypted_kubeconfig``
+ the injected ``CryptoService`` -- the exact same source
``cluster.load_kubeconfig`` (this wave's ``kubecfg`` step) already decrypted
to produce the ``SecretStr`` this Step's own ``params.kubeconfig`` carries.
Two derivations of the identical plaintext, not two DIFFERENT ones: by the
time ``restore`` runs, ``preflight``/``apply`` have already proven THIS same
cluster's kubeconfig is live (``deploy-waves.yml``'s own step order), so
``SnapshotService.restore``'s independent re-decrypt can only ever reach the
SAME value ``params.kubeconfig`` already holds. Editing
``SnapshotService``'s own signature to accept a kubeconfig directly would
duplicate a real, already-tested collaborator's internals into this verb
(the DnsRecordRef lesson DR-0028 itself cites: "a verb built to the declared
shape could not have called its own service" -- the inverse failure mode
here would be inventing a service call ``SnapshotService`` was never built to
take) rather than binding to its ACTUAL signature, which this module reads,
not guesses.

**Restoring into a not-yet-ready database is a REAL, expected failure mode
here, not a rare edge case.** DR-0029's own ordering (``seedpod/core/
deploy_wave.py``'s ``Wave`` docstring, "This does re-open, deliberately, the
exact ordering question the now-withdrawn two-wave design existed to
dodge"): ``deploy-waves.yml``'s per-wave step order is fixed --
``prep -> apply -> restore -> restart -> ready`` -- so on the persistence
wave, THIS step runs immediately after that SAME wave's own ``apply``, with
NO readiness gate confirming the database pod is actually accepting
connections yet (unlike v1's explicit ``_wait_for_database_pods_ready
(timeout=180)`` phase, which ran BETWEEN apply and restore). Seam C taste
call 2 ("no command waits, all waiting is an engine gate") rules out a
step-internal poll/sleep here — the engine's OWN ``Schedule`` is the only
legal waiting mechanism, which is exactly what ``deploy-waves.yml``'s own
``restore`` step already leans on (``retry: kubectl_default``, wired by the
load-and-plan component specifically for this purpose — see that step's own
YAML comment). ``execute`` below therefore raises ``TransientError`` (not
``PermanentError``) for a failed restore attempt whose cause
``RestoreResult`` cannot further distinguish (below) — Schedule retries it
up to ``kubectl_default``'s budget before surfacing a terminal
``RETRY_EXHAUSTED`` failure, giving a cold-started database a real chance
without inventing a step-internal wait.

**Why ``RestoreResult(success=False)`` alone still gets a bare
``TransientError``, not a finer split -- and why that is now narrower than it
first appears.** DR-0030 (``docs/decisions/DR-0030-snapshot-restore-error-
fidelity.md``, ratified 2026-08-08, this same round) lifted the "Round 6
services are frozen" constraint for ``SnapshotService.restore`` specifically,
for exactly two fixes: it no longer swallows ``InfrastructureUnreachableError``
into ``RestoreResult.error`` (propagates instead -- see below), and it now
raises ``SnapshotIncompatible`` (a ``PermanentError``) for a pre-flight
service-name mismatch instead of failing late via ``pod_name is None``. Both
of those are now raised, real exceptions this Step must handle explicitly
(below) -- they never reach the ``result.success`` check at all. What
``RestoreResult(success=False)`` can STILL mean, once those two are carved
out, is narrower but still ambiguous: a not-yet-``Running`` pod, a
``pg_restore`` connection refused because Postgres is still starting, or a
genuinely broken restore COMMAND (a bad ``restore_command`` override in the
profile's ``persistence:`` block) -- ``SnapshotService.restore``'s own
remaining blanket ``except Exception`` (now narrower than DR-0030's audit
found it) still folds those three into the identical
``RestoreResult(success=False, error=str(exc))`` shape, and there is still no
signal left, once ``restore()`` returns THAT shape, to tell "try again, it
just isn't up yet" apart from "this will never work". Treating every such
failed restore as retryable remains the SAFE default given that residual
ambiguity: the direction this protects against (retrying a genuinely broken
restore COMMAND a few extra times before ``RETRY_EXHAUSTED`` surfaces it) is
merely wasteful, never silently wrong -- the operator still sees a real,
terminal failure, just after this step's own explicit retry budget
(``config/workflows/deploy-waves.yml``'s ``restore`` step, ~180s, sized to
replicate v1's ``_wait_for_database_pods_ready(timeout=180)``) rather than
immediately. Two exceptions now get distinct, non-retried classification:
``SnapshotNotFound`` (below): a stale/typo'd ``restore_from_snapshot`` id, or
a ``cluster_id`` with no matching cluster row, can never resolve no matter how
many times this step retries -- provably useless, ``PermanentError``'s own
definition. ``SnapshotIncompatible`` (below, DR-0030 fix 2): a snapshot whose
services have no persistence counterpart on the target profile can never
succeed either, for the identical reason -- retrying it would burn the whole
~180s budget on a restore that was doomed before the first ``pg_restore``
ever ran. ``InfrastructureUnreachableError`` (below, DR-0030 fix 1) is
neither retried NOR failed -- CLAUDE.md's hard rule -- it is left entirely
un-wrapped, propagating past this Step's own ``execute`` exactly as it
propagates past ``SnapshotService.restore``, so the engine's blocked-park law
(not Schedule, not ``PermanentError``) is what handles it."""

from __future__ import annotations

from datetime import timedelta

from pydantic import BaseModel, SecretStr

from seedpod.app.services.snapshot_service import (
    SnapshotIncompatible,
    SnapshotNotFound,
    SnapshotService,
)
from seedpod.core.clock import Clock
from seedpod.core.deploy_wave import SnapshotRestoreSpec
from seedpod.core.errors import ErrorCode, PermanentError, TransientError
from seedpod.engine.step import EmptyOutput, Step, StepContext

__all__ = ["RestoreSnapshotParams", "DeployRestoreSnapshot"]


class RestoreSnapshotParams(BaseModel):
    kubeconfig: SecretStr
    spec: SnapshotRestoreSpec | None = None


class DeployRestoreSnapshot(Step[RestoreSnapshotParams, EmptyOutput]):
    """``plane="domain"``/``thin=False`` (Step's own defaults -- unset below,
    matching ``deploy.load_audit``/``deploy.plan_waves``'s identical
    unset-defaults idiom, ``seedpod/engine/steps/deploy.py``).
    ``gateable=False``/``undoable=False``/``idempotent=True`` (also Step's own
    defaults, also unset): a restore is not something to gate on (it either
    completes this attempt or raises, matching every other composite in this
    wave); it is not undoable (restoring INTO a database has no sensible
    inverse -- matching ``kube.apply_docs``/``deploy.ensure_rollouts``'s
    identical ``undoable=False`` reasoning in the sibling ``deploy_apply.py``
    module, one wave over); it IS idempotent (a re-run after a crash mid-step,
    or after a Schedule retry, re-issues the SAME ``pg_restore --clean
    --if-exists`` the target already declares as its own restore command --
    ``SnapshotService``'s own ``_command`` helper -- so a second attempt
    against an already-restored target converges, it does not corrupt)."""

    verb = "deploy.restore_snapshot"
    Params = RestoreSnapshotParams
    Output = EmptyOutput

    def __init__(self, *, snapshots: SnapshotService, clock: Clock) -> None:
        self._snapshots = snapshots
        self._clock = clock

    async def execute(self, params: RestoreSnapshotParams, ctx: StepContext) -> EmptyOutput:
        if params.spec is None:
            # DR-0028's own words: a SnapshotRestoreSpec with NEITHER mode set
            # resolves to "not an error - just no data to restore" -- and
            # `Wave.restore: SnapshotRestoreSpec | None` (Seam B §2.2 Proof 1)
            # means `None` IS the typed no-op (DR-0022 P4), never reaching this
            # branch by way of some empty-but-non-None spec.
            return EmptyOutput()

        snapshot_id = await self._resolve_snapshot_id(params.spec)
        if snapshot_id is None:
            # v1's own outcome for "no snapshot found matching criteria" --
            # deployment_job.py:266-269's own log line, verbatim in this
            # module's docstring. Applies to BOTH restore modes: an explicit
            # `restore_from_snapshot` that happens to be a falsy string (""),
            # or `restore_from_latest` criteria matching zero snapshots.
            return EmptyOutput()

        try:
            result = await self._snapshots.restore(
                snapshot_id,
                cluster_id=ctx.cluster_id,
                services=params.spec.services,
                # v1's own comment (deployment_job.py:335): "Perform restore
                # (without running migrations - they'll run via init
                # containers)" -- migrations run via init containers after
                # manifest apply, not this step's concern. SnapshotService.
                # restore's own `run_migrations` parameter is accepted for
                # wire parity only (that method's own ARG002-suppressed
                # unused-argument marker -- "no migration-runner verb exists
                # yet"); passed `False` here purely to keep this call site's
                # intent legible against v1's own, not because the parameter
                # currently does anything.
                run_migrations=False,
                actor="system:deploy",
            )
        except SnapshotIncompatible:
            # DR-0030 fix 2: ``SnapshotIncompatible`` IS already a
            # ``PermanentError`` (``seedpod/app/services/snapshot_service.py``'s
            # own class definition) -- an explicit pass-through here, not a
            # re-wrap, purely so a reader does not have to infer "uncaught
            # here == propagates unchanged" from the ABSENCE of a clause.
            # Never retried (``Schedule.classify``: ``PermanentError`` ->
            # ``FAIL`` immediately) -- a snapshot whose services have no
            # persistence counterpart on the target profile can never
            # succeed no matter how many of this step's ~180s of retry
            # attempts it burns.
            raise
        except SnapshotNotFound as exc:
            # A stale/typo'd explicit id, or a cluster_id with no matching
            # cluster row -- provably useless to retry (PermanentError's own
            # definition), unlike everything RestoreResult.success=False below
            # can mean. See module docstring's own paragraph on this split.
            raise PermanentError(
                f"deploy.restore_snapshot: {exc}",
                code=ErrorCode.NOT_FOUND,
                provider="engine",
                command=self.verb,
                detail={"snapshot_id": snapshot_id, "cluster_id": ctx.cluster_id},
            ) from exc

        if result.success:
            # DR-0035 decision 3: the SPA's live signal for an IN-WORKFLOW restore.
            # Deliberately `ctx.progress` (-> `workflow_progress`), not a second
            # `snapshot_restore_completed` broadcast: this step retries up to 19
            # times by design (a not-yet-ready database is a normal early outcome,
            # see this module's docstring), and a terminal-sounding per-attempt
            # topic would put up to 18 spurious "restore failed" events in the HUD
            # for a run that then succeeds. `api/routers/snapshots.py` keeps the
            # bespoke topic for the single-attempt, user-initiated REST path.
            await ctx.progress(
                "deploy.restore_snapshot: restore completed",
                snapshot_id=snapshot_id,
                services_restored=list(result.services_restored),
            )
            return EmptyOutput()

        # Say why, per attempt (DR-0033): without this a 19-attempt restore is
        # silent until the budget is exhausted -- smoke 10's restore took 39s and
        # emitted nothing at all.
        await ctx.progress(
            "deploy.restore_snapshot: restore attempt did not complete",
            snapshot_id=snapshot_id,
            services_failed=list(result.services_failed),
            error=result.error or "",
        )
        raise TransientError(
            f"deploy.restore_snapshot: restore of snapshot {snapshot_id!r} into cluster "
            f"{ctx.cluster_id!r} did not complete this attempt (services_failed="
            f"{list(result.services_failed)!r}, error={result.error!r}) -- this wave's own "
            "`apply` may not have produced a database pod ready to accept connections yet "
            "(no readiness gate runs between apply and restore on this wave, by design -- "
            "see Wave's own docstring, seedpod/core/deploy_wave.py); retried via this "
            "step's own explicit retry schedule (config/workflows/deploy-waves.yml's "
            "`restore` step, ~180s), the engine's own Schedule, never a step-internal wait.",
            code=ErrorCode.SCRIPT_FAILED,
            provider="engine",
            command=self.verb,
            detail={
                "snapshot_id": snapshot_id,
                "cluster_id": ctx.cluster_id,
                "services_failed": ",".join(result.services_failed) or "<none>",
                "error": result.error or "",
            },
        )

    async def _resolve_snapshot_id(self, spec: SnapshotRestoreSpec) -> str | None:
        """v1's own two-mode precedence (``deployment_job.py:244-265``):
        ``restore_from_snapshot`` wins outright when present (an explicit id
        needs no lookup); ``restore_from_latest`` criteria are resolved via
        the ALREADY-COMMITTED ``SnapshotService.list`` (``branch``/``profile``
        filters, ``ORDER BY created_at DESC`` -- ``SnapshotRepository.list``'s
        own SQL, ``seedpod/data/repositories.py``, matching v1's own
        "already sorted by created_at desc" comment) -- never re-derived here.
        ``max_age_days`` filters via the INJECTED ``Clock`` (CLAUDE.md's hard
        rule: no ambient ``now()``), not v1's own bare
        ``datetime.now(timezone.utc)`` -- a genuine, deliberate divergence
        from v1's literal code, not a behaviour change: the filtered SET is
        identical for a real wall clock, and this keeps the verb testable with
        a ``FrozenClock`` the way every other timestamp-touching Step in this
        tree already is.

        **A narrower, DOCUMENTED divergence in the precedence itself.** v1
        decides which mode wins by dict KEY PRESENCE with ``elif``
        (``if "restore_from_snapshot" in data_initialization: ... elif
        "restore_from_latest" in data_initialization: ...``,
        ``deployment_job.py:244-248``) -- an explicitly-set-but-EMPTY
        ``restore_from_snapshot`` (``""``) still wins outright in v1 and never
        falls through to ``restore_from_latest``. This method tests TRUTHINESS
        (``if spec.restore_from_snapshot:``) instead, because
        ``SnapshotRestoreSpec`` is a plain pydantic model with no
        wire-preserved "was this key present" fact once it has crossed this
        workflow's scope-binding boundary (``Wave.restore``, constructed by
        ``deploy.plan_waves`` from ``resolved_config`` JSON, then re-bound
        onto THIS step's own ``Params`` -- key-presence is not guaranteed to
        survive that hop the way it does inside v1's own single
        long-lived ``dict``). The ONLY observable divergence this produces:
        ``restore_from_snapshot: ""`` set EXPLICITLY alongside a ALSO-set
        ``restore_from_latest`` falls through to evaluate the latter here,
        where v1 would short-circuit to a no-op without ever looking at it.
        Every other combination (either mode alone, or neither) resolves
        identically in both. Accepted as documented, not silently regressed
        -- see ``tests/engine/steps/test_deploy_restore_steps.py``'s own test
        pinning this exact combination, so a future change cannot make it
        worse without a test failing."""
        if spec.restore_from_snapshot:
            return spec.restore_from_snapshot
        if spec.restore_from_latest is not None:
            criteria = spec.restore_from_latest
            snapshots = await self._snapshots.list(branch=criteria.branch, profile=criteria.profile)
            # v1's own guard is truthy, not presence-based (`deployment_job.py:254`:
            # ``if criteria.get("max_age_days"):``) -- ``0`` means "do not filter",
            # not "cutoff = now" (which would filter out every snapshot and silently
            # drop an explicitly requested restore -- the exact failure class
            # DR-0028 Erratum E2 names, applied here to a second case it did not
            # itself audit but the same principle covers). Mirrored with a truthy
            # check, not ``is not None``, so ``max_age_days: 0`` behaves identically
            # to "not set" here too.
            if criteria.max_age_days:
                cutoff = self._clock.now() - timedelta(days=criteria.max_age_days)
                snapshots = [s for s in snapshots if s.created_at > cutoff]
            if snapshots:
                return snapshots[0].id
        return None
