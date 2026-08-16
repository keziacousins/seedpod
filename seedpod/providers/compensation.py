"""seedpod/providers/compensation.py — Seam C §5.5 ``undo_for``, amended by
docs/design/coherence-review.md Conflicts 7 and 12.

A pure module-level function the engine consults when pushing a step's undo onto the
Scope (``seedpod/engine/provider_step.py``'s ``ProviderStep.undo``). **It takes
``Observed`` (the folded stream, §5.2), not the terminal result** — so a
``CreateInstance`` that died mid-stream after ``RESOURCE_ALLOCATED`` still yields a real
``DestroyInstance``. This is the structural C1 close.

Conflict 7 amendment (binding — overrides this module's literal §5.5 pseudocode where
they disagree): ``Observed.data`` is normatively the persisted ``workflow_steps.notes``,
and ``ProviderStep.execute`` writes ``RESOURCE_ALLOCATED`` progress through
``ctx.note(**{k: str(v) for k, v in d.get("resource_ids", {}).items()})`` — i.e. the
resource-id mapping is flattened directly into the notes dict's top-level keys, with no
``"resource_ids"`` wrapper key. So a truncated-stream ``CreateInstance`` undo reads the
ids straight off ``observed.data`` itself, not ``observed.data.get("resource_ids")``
(the seam spec's own pseudocode assumes a wrapper key that the actual bridge never
writes; the bridge is the newer, coherence-review-adjudicated source of truth per
CLAUDE.md's precedence rule).

Conflict 12 amendment: the seam's "workflow-declared compensator" phrasing for deploy
rollback is **void**. ``deploy-waves.yml`` stays ``on_failure: report``, and deploy-cancel
rollback runs as a distinct machine-decided workflow (``workflows/deploy-rollback.yml``,
dispatched via ``RunWorkflow(rollback, ...)`` on ``DEPLOYING × CancelRequested``), not as
this module's inverse of the deploy step. ``undo_for(KubeApplyManifest) -> KubeDeleteManifest``
remains below, but DR-0022 ruling 3 (D1's fix) makes application deploy waves'
non-participation in it **structural rather than YAML-conditioned**: ``kube.apply_docs``
(the ``deploy-waves.yml`` verb) is declared ``undoable=False`` at the registry
(``tests/engine/declared_verbs.py``), so ``ProviderStep.undo`` — and therefore this
function — is never even called for it (``engine/engine.py`` only calls ``step.undo`` when
``step.undoable``), regardless of what any workflow's ``on_failure:`` key says. The arm
below is reachable only via ``kube.apply_file`` (the distinct infra-shim verb, e.g. the
Traefik parity workflows that ``kubectl-apply`` a static manifest; ``undoable=True``) —
both verbs map to the same ``KubeApplyManifest`` Seam C command, so it is each verb's
*registry* ``undoable`` flag, not the command shape, that keeps them apart.

**Undo laws (pinned, §5.5):** every undo command is idempotent and absent-tolerant
(``DESTROYED`` + already-absent note, ``existed=False``, ``ignore_not_found`` are all
success); undos run in reverse completion order; ``TransientError`` during undo retries
on the step's Schedule; ``PermanentError`` during undo is recorded and reconciliation
inherits the leak (the backstop the plan keeps); ``InfrastructureUnreachableError``
never starts compensation (§5.1 engine-behavior table — Conflict 5's park-never-
compensate law). v1's ``retain_on_failure: true`` survives as a workflow-level
skip-compensation debug flag, not provider code.
"""

from __future__ import annotations

from seedpod.providers.contract import (
    CreateInstance,
    DestroyInstance,
    KubeApplyManifest,
    KubeDeleteManifest,
    Observed,
    ProviderCommand,
)

__all__ = ["undo_for"]


def undo_for(cmd: ProviderCommand, observed: Observed) -> ProviderCommand | None:
    """Pure ``command -> inverse-command`` mapping. Total over the whole
    ``ProviderCommand`` union: every command not explicitly matched below has no
    inverse (``None``) — reads, already-compensating destructions, and the
    workflow-declared rollback path (Conflict 12) all fall through to the catch-all.
    """
    match cmd:
        case CreateInstance(slug=slug):
            if observed.value is not None:
                ids = observed.value.resource_ids  # type: ignore[attr-defined]
            else:
                # Conflict 7: notes ARE the flattened resource_ids, no wrapper key.
                ids = observed.data
            if ids:
                return DestroyInstance(slug=slug, resource_ids=dict(ids))
            # ids never escaped ⇒ nothing was allocated OR tag-before-boot +
            # reconciliation (Zombie/CreateUnmanaged next cycle) is the backstop.
            return None

        case KubeApplyManifest(kubeconfig=kubeconfig, manifest_yaml=manifest_yaml):
            # Literal inverse — reachable ONLY via `kube.apply_file` (the infra-shim
            # verb, e.g. Traefik parity manifests; `declared_verbs.py` fixture:
            # `undoable=True`). `kube.apply_docs` (deploy waves' verb) is declared
            # `undoable=False` at the registry (DR-0022 ruling 3, D1's fix) — Step.undo
            # is never even called for it, so this arm is STRUCTURALLY unreachable from
            # deploy-waves.yml regardless of that workflow's `on_failure:` key. This
            # supersedes the pre-DR-0022 comment here, which relied on `deploy-waves.yml`
            # happening to say `on_failure: report` (Conflict 12) — a YAML-editable fact,
            # not a structural guarantee; ruling 3 exists to replace exactly that
            # dependency with this one, unrepresentable-by-construction, guarantee.
            return KubeDeleteManifest(kubeconfig=kubeconfig, manifest_yaml=manifest_yaml, ignore_not_found=True)

        case _:
            return None
