"""seedpod/core/deploy_wave.py — the five deploy-path DTOs Round 10's seven verbs
bind: ``ManifestDoc``, ``DeploymentProfile``, ``SnapshotRestoreSpec`` (+ its
``RestoreFromLatest`` criteria), ``Wave``, ``ApplyChangeSummary``.

DR-0028 (docs/decisions/DR-0028-deploy-path-dtos.md) is this module's governing
decision. ``tests/engine/declared_verbs.py`` carried these five as fixture
STAND-INS, each flagged with a ``TODO`` admitting the shape was inferred, never
audited. DR-0028 audited all five against v1 and found four of five wrong:
``DeploymentProfile.data_initialization`` was a bool where v1 has a mapping with
two modes, sourced from the wrong place entirely; ``ApplyParams.docs`` needed a
serialization boundary the stand-in never provided; ``SnapshotRestoreSpec`` was
missing v1's second restore mode outright; ``ApplyChangeSummary``'s shape was
right but its load-bearing restart semantic was undocumented anywhere. This
module makes the ratified shapes real, tested, and importable — replacing the
stand-ins rather than merely typing them more strictly.

**Why this lives in ``core/``, not ``services/``.** ``core/`` is pure (no IO, no
``now()``, CLAUDE.md's hard rule) — the same discipline ``core/cluster_spec.py``
and ``core/dns_record.py`` already hold, and this module's direct precedent: one
module per coherent DTO cluster, a type may carry a pure classmethod/module-level
helper alongside it (``DnsRecordRef.from_provider_config``), cited against the v1
source line range it salvages. Every type below is pure data (at most a computed
property or a pure string<->object transform — ``yaml.safe_load_all``/
``yaml.safe_dump_all`` operate entirely on in-memory strings, no disk/network IO,
the same "a library's pure surface only" reading CLAUDE.md's jinja2 clarification
already establishes for third-party imports inside ``core/``).

``seedpod/services/manifests.py`` (the OTHER candidate location DR-0028 and this
round's brief both name) was considered and rejected: that module's own docstring
scopes it tightly to GHCR image resolution and Jinja template rendering, not
deploy-wave orchestration, and — decisively — ``core/`` never imports
``services/`` anywhere in this tree (verified by grep; only the reverse direction
exists, e.g. ``services/manifests.py`` importing ``seedpod.core.errors``).
``Wave.docs: list[ManifestDoc]`` (Seam B §2.2 Proof 1, pinned verbatim by DR-0028
decision 5) forces ``Wave`` and ``ManifestDoc`` into the same layer or the same
import direction becomes impossible; since ``Wave`` has no IO of its own either,
``core/`` is the only home that fits both types without a layering violation.

Round 10's seven deploy-path verbs (``deploy.load_audit``, ``deploy.plan_waves``,
``deploy.prepare_wave``, ``kube.apply_docs``, ``deploy.restore_snapshot``,
``deploy.ensure_rollouts``, ``deploy.await_wave``) are NOT built by this module —
this is the type foundation those verbs stand on, exactly as this round's own
"dtos" component brief describes it. Every "not built by this component" note
below names the verb that will consume the fact in question, for the next
component's benefit.

This module also carries ``DEFERRED_MANIFEST_RENDERING_KEY`` (below, added by the
"load-and-plan" component): not a DTO, but the same kind of shared, load-bearing
literal — the ``resolved_config`` marker DR-0025 Erratum E2's DEFERRED case reads
and (eventually) writes. It lives here for the identical reason the five DTOs do:
one producer, one literal, importable by both the reader (``deploy.load_audit``)
and the future writer (``deployment_service.py``) without either redeclaring it.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Final

import yaml
from pydantic import BaseModel

from seedpod.core.errors import ErrorCode, PermanentError

__all__ = [
    "ManifestDoc",
    "parse_manifest_documents",
    "serialize_manifest_documents",
    "DeploymentProfile",
    "RestoreFromLatest",
    "SnapshotRestoreSpec",
    "Wave",
    "ApplyChangeSummary",
    "DEFERRED_MANIFEST_RENDERING_KEY",
    "MANIFEST_RENDERING_REHYDRATED_KEY",
]


# ---------------------------------------------------------------------------
# DR-0025 Erratum E2 -- the resolved_config marker for a DEFERRED audit row.
# ---------------------------------------------------------------------------

# The literal `resolved_config` key DR-0025 Erratum E2's DEFERRED case marks a
# `deployment_audits` row with (docs/decisions/DR-0025-hostname-resolution-
# ordering.md, Erratum E2): "an audit row may now legitimately exist WITH NO
# RENDERED MANIFESTS, marked pending deploy-time rendering" -- a provider_host
# profile whose host was unknowable at decision time but will be known once
# the cluster is ACTIVE. `resolved_config[DEFERRED_MANIFEST_RENDERING_KEY]`
# truthy IS that marker; its absence (or a falsy value) means "this audit's
# manifests, if empty, are empty for some other reason". See
# `seedpod/engine/steps/deploy.py`'s `DeployLoadAudit` (this key's reader) for
# how the two are told apart and why NEITHER is allowed to read as
# `manifests=[]` silently.
#
# A module-level constant, not a literal string re-typed at both ends, because
# Erratum E2 exists ONLY because DR-0025's own parts 1 and 2 -- read: this
# key's future WRITER (`deployment_service.py`'s decision-time deferral, the
# LATER restore-and-rehydrate component's job, not this one's) and its READER
# (`deploy.load_audit`) -- independently assumed shapes that turned out not to
# agree ("parts 1 and 2 CONTRADICTED each other" is that erratum's own opening
# sentence). One shared name instead of each end spelling the key
# independently is the identical contract-drift fix DR-0028 already applied to
# `persistence_services`/`data_initialization`/`rollout_timeout_seconds`: one
# producer, one literal, never two that can quietly disagree.
#
# `resolved_config` (a JSON-typed column already on the SAME `deployment_audits`
# row) rather than a new DB column is this component's deliberate,
# scope-respecting choice: its brief authorizes no `seedpod/data/` schema
# edit, and `resolved_config` already carries `persistence_services`/
# `rollout_timeout_seconds`/`data_initialization` as flat facts read back
# "like every other resolved fact" (DR-0028 decision 2's own words) -- this is
# the fourth. It still satisfies DR-0025's own language ("a real, queryable
# fact ON THE ROW"): `resolved_config` IS a column on that row, merely a JSON
# one, not an inference from `resolved_manifests` being empty.
DEFERRED_MANIFEST_RENDERING_KEY: Final[str] = "manifest_rendering_deferred"

# The restore-and-rehydrate component's own sibling marker (docs/decisions/
# DR-0025-hostname-resolution-ordering.md, Erratum E2, point (ii): "rewrites the
# SAME audit row in place, recording that it was rehydrated"). Set TRUE by
# `seedpod/engine/steps/deploy.py`'s `DeployLoadAudit` at the exact moment it
# rewrites a previously-deferred row's `resolved_manifests`/`resolved_config`
# with the real, deploy-time-rendered content -- the SAME write also clears
# `DEFERRED_MANIFEST_RENDERING_KEY` (a row is never both "still pending" and
# "already rehydrated" at once). Never set on a row that was never deferred in
# the first place (an ordinary, immediately-rendered deploy has no rehydration
# event to record) -- so this key's mere PRESENCE (any value) on a real audit
# row is itself the "this row's manifests were re-rendered at deploy time, not
# decision time" fact DR-0025's own Consequences ask for ("the audit must not
# silently diverge from what was applied").
MANIFEST_RENDERING_REHYDRATED_KEY: Final[str] = "manifest_rendering_rehydrated"


# ---------------------------------------------------------------------------
# ManifestDoc — DR-0028 decision 1
# ---------------------------------------------------------------------------


class ManifestDoc(BaseModel):
    """One Kubernetes resource document, parsed out of a rendered multi-document
    manifest YAML string (``seedpod/services/manifests.py``'s
    ``ResolvedManifest.rendered_manifests``, persisted verbatim as
    ``DeploymentAuditRow.resolved_manifests`` — ``seedpod/data/repositories.py``).

    Same four fields as the ``tests/engine/declared_verbs.py`` stand-in this type
    replaces (``kind``, ``name``, ``namespace``, ``body``) — DR-0028 did not fault
    the field SET, only that it lived only in a test fixture. ``deploy.plan_waves``
    (Round 10, not this component) inspects ``kind``/``name``/``namespace`` to
    classify a document by service, generalizing v1's ``_split_manifests_by_service``
    (``reference-code/seedpod/seedpod/jobs/state/deployment_job.py:66-127``), which
    reads exactly these three facts off ``doc.get("kind")``/
    ``doc.get("metadata", {}).get("name"/"namespace")`` — parsing once here and
    carrying typed documents beats every later step re-parsing the same opaque YAML
    string (DR-0028 decision 1's own rationale).

    ``body`` is the FULL parsed document — ``apiVersion``/``kind``/``metadata``/
    ``spec``/... all of it, including ``kind``/``name``/``namespace`` again.
    ``kind``/``name``/``namespace`` are a deliberate, lossless denormalization for
    classification convenience, never a trimmed alternative to ``body`` --
    ``serialize_manifest_documents`` below round-trips from ``body`` alone.

    Absent ``metadata.namespace`` (any cluster-scoped resource — a ``ClusterRole``,
    a ``Namespace`` itself, or any resource relying on kubectl's default namespace)
    defaults to ``""``. v1's ``_split_manifests_by_service`` only ever reads
    ``name`` this defensively (``doc.get("metadata", {}).get("name", "")``,
    ``deployment_job.py:99`` — it never reads ``namespace`` at all); this type
    applies the identical `.get(..., "")` discipline to `namespace`/`kind` too, a
    deliberate, narrow generalization of v1's own pattern, not a v1 behaviour it
    is copying verbatim (v1 never needed a document's namespace for anything)."""

    kind: str = ""
    name: str = ""
    namespace: str = ""
    body: Mapping[str, Any] = {}


def parse_manifest_documents(rendered_yaml: str) -> list[ManifestDoc]:
    """The counterpart to ``serialize_manifest_documents`` below, and — in v2's
    redesigned deploy pipeline — the ONE place a rendered manifest's raw YAML text
    is ever parsed (DR-0028 decision 1's own rationale: parsing once beats
    re-parsing an opaque string at every step, so ``deploy.plan_waves`` never
    touches raw YAML text at all once ``deploy.load_audit``, Round 10, not this
    component, has called this once).

    Salvaged in effect from v1's own multi-doc iteration
    (``reference-code/seedpod/seedpod/jobs/state/deployment_job.py:91-92``,
    ``_split_manifests_by_service``'s ``for doc in yaml.safe_load_all(...): if doc
    is None: continue``): a leading/trailing ``---`` separator (exactly what
    ``services/manifests.py``'s own ``"\\n---\\n".join(rendered_parts)`` can
    produce when concatenating per-template renders) yields a ``None`` document
    from ``yaml.safe_load_all`` and is silently skipped, never turned into a
    placeholder ``ManifestDoc``.

    Genuine correctness fix, not a v1 bug pin: v1's OWN caller of this identical
    ``yaml.safe_load_all`` loop (``_split_manifests_by_service``,
    ``deployment_job.py:127-129``) wrapped it in
    ``except yaml.YAMLError: ...; return "", rendered_manifests`` — fail OPEN,
    silently downgrading a malformed manifest into "no database manifests, apply
    everything as one wave, skip the restore phase entirely", logged but never
    surfaced to the caller. DR-0028's own Consequences section names this "a
    candidate not-ported" and requires a deliberate, loud call either way. v2's
    redesign removes even the option of silently falling open into that outcome:
    parsing happens exactly once, here, so there is no later per-split parse to
    fall open into — a malformed manifest raises here instead, naming the
    failure, matching the crown-jewel-#1 posture ``services/manifests.py``'s own
    ``StrictUndefined`` decision already holds for template rendering.

    A second, narrower malformed-input case a bare ``yaml.YAMLError`` catch
    does NOT cover: a document that parses as perfectly valid YAML but is not a
    *mapping* (a bare scalar, a list, a number -- anything a stray top-level
    ``---`` misplacement or a hand-edited template can produce). Left
    unguarded, ``raw.get("metadata")`` below would raise a bare
    ``AttributeError``, escaping the one error taxonomy this whole codebase
    funnels through (CLAUDE.md's hard rule) from the ONE place a rendered
    manifest is ever parsed. Guarded the same way the committed idiom one
    module over already guards an analogous "must be a mapping" input
    (``seedpod/core/environment_config.py``'s
    ``create_environment_variables_from_dict``,
    ``PermanentError(ErrorCode.INVALID_INPUT)`` naming the actual type).

    Two more guards, one level deeper, close the identical hole for
    ``metadata`` itself and for the three denormalized scalar fields:
    ``metadata: oops`` (a mapping's ``metadata`` key holding a non-mapping)
    would raise the same bare ``AttributeError`` on ``metadata.get(...)``
    without the analogous check below; and a YAML-typed scalar under
    ``metadata.name``/``metadata.namespace``/top-level ``kind`` -- an
    all-numeric name (a real case: a template rendering ``name: {{ branch
    }}`` with a numeric branch, e.g. ``2024``, is a legal DNS-1123 k8s name)
    or YAML 1.1's bare ``on``/``no``/``off`` resolving to a bool -- would
    otherwise raise a raw pydantic ``ValidationError`` instead of this
    module's own taxonomy, on input v1 passed straight through to ``kubectl``
    without complaint. Coercing with ``str(...)`` (never silently dropping the
    value) is the genuine correctness fix: v1 accepted these; nothing here
    should regress that."""
    try:
        raw_docs = list(yaml.safe_load_all(rendered_yaml))
    except yaml.YAMLError as exc:
        raise PermanentError(
            f"deploy_wave.parse_manifest_documents: rendered manifest is not valid YAML: {exc}",
            code=ErrorCode.INVALID_INPUT,
            provider="deploy_wave",
            command="parse_manifest_documents",
        ) from exc
    docs: list[ManifestDoc] = []
    for index, raw in enumerate(raw_docs):
        if raw is None:
            continue
        if not isinstance(raw, Mapping):
            raise PermanentError(
                f"deploy_wave.parse_manifest_documents: document {index} is not a mapping "
                f"(a Kubernetes manifest document must be a YAML mapping), got {type(raw).__name__}",
                code=ErrorCode.INVALID_INPUT,
                provider="deploy_wave",
                command="parse_manifest_documents",
                detail={"document_index": str(index), "type": type(raw).__name__},
            )
        metadata = raw.get("metadata")
        if metadata is None:
            metadata = {}
        elif not isinstance(metadata, Mapping):
            raise PermanentError(
                f"deploy_wave.parse_manifest_documents: document {index}'s metadata is not a "
                f"mapping (a Kubernetes manifest's metadata must be a YAML mapping), got "
                f"{type(metadata).__name__}",
                code=ErrorCode.INVALID_INPUT,
                provider="deploy_wave",
                command="parse_manifest_documents",
                detail={"document_index": str(index), "field": "metadata", "type": type(metadata).__name__},
            )
        docs.append(
            ManifestDoc(
                kind=str(raw.get("kind") or ""),
                name=str(metadata.get("name") or ""),
                namespace=str(metadata.get("namespace") or ""),
                body=raw,
            )
        )
    return docs


def serialize_manifest_documents(docs: Iterable[ManifestDoc]) -> str:
    """The inverse of ``parse_manifest_documents``, and DR-0028 decision 1's own
    explicit ask: ``kube.apply_docs`` (Round 10, not this component) calls this to
    turn ``list[ManifestDoc]`` back into the single YAML string
    ``seedpod/providers/contract.py``'s ``KubeApplyManifest.manifest_yaml: str``
    requires (``providers/kubectl.py``'s ``_apply_manifest`` writes that string to
    a temp file and runs ``kubectl apply -f`` against it) — the frozen
    ``seedpod/providers/`` tree is untouched; DR-0028 puts the serialization
    boundary here, in the verb layer's DTO, instead.

    A module-level function, not a ``ManifestDoc.to_yaml()`` instance method: the
    operation is inherently list-shaped (kubectl applies a multi-document stream,
    and every real caller — ``kube.apply_docs`` over a whole ``Wave.docs`` — holds
    a LIST, never a single document in isolation), matching this codebase's own
    precedent for list/stream-shaped YAML helpers
    (``services/manifests.py``'s ``normalize_resolved_manifests``, also a
    module-level function rather than a method on whatever it normalizes).

    ``yaml.safe_dump_all`` (not a manual ``"\\n---\\n".join(...)``, matching v1's
    own reconversion call, ``yaml.dump_all(database_docs, default_flow_style=False)``
    at ``deployment_job.py:134``) inserts the ``---`` document separators itself.
    Every document dumps from ``body`` — the full parsed document — so this is
    lossless for anything ``parse_manifest_documents`` produced; only YAML
    comments and the original on-disk key ORDER are not preserved (an inherent
    property of any parse/reserialize round trip through a data model rather than
    a text editor — kubectl and every provider conformance test are indifferent to
    both)."""
    return yaml.safe_dump_all((dict(doc.body) for doc in docs), default_flow_style=False)


# ---------------------------------------------------------------------------
# DeploymentProfile — DR-0028 decision 2
# ---------------------------------------------------------------------------


class DeploymentProfile(BaseModel):
    """The deploy-wave verbs' own view of "the deployment profile" — built by
    ``deploy.load_audit`` (Round 10, not this component) from the deployment
    audit's ``resolved_config`` JSON (``DeploymentAuditRow.resolved_config``,
    ``seedpod/data/repositories.py``), NOT from ``config/deployment-profiles/*.yml``
    directly. That richer, disk-loaded shape is ``seedpod/services/manifests.py``'s
    ``ManifestProfile`` — a DIFFERENT type for a DIFFERENT consumer (image
    resolution and Jinja rendering, not wave planning); this type is not a rename
    of it and does not replace it.

    DR-0028 decision 2 replaces the fixture stand-in's ``data_initialization:
    bool`` outright rather than narrowing its type: that fact is not a profile
    field at all (see ``SnapshotRestoreSpec``'s own docstring) — "a per-deployment
    choice, not a property of a profile" (DR-0028's own words), sourced from the
    deploy REQUEST, not profile YAML, and no shipped profile ever declared it.

    ``persistence_services``: v1's ``_get_database_services``
    (``reference-code/seedpod/seedpod/jobs/state/deployment_job.py:47-63``) reads
    ``resolved_config.get("persistence_services", [])``, and its own docstring
    says plainly "this comes from the deployment profile": each name is a service
    the profile's OWN YAML declares a ``persistence:`` block for (real example:
    ``config/deployment-profiles/exampleco-dev-stack-nodns.yml``'s ``postgres``
    service). ``seedpod/app/services/deployment_service.py``'s
    ``_build_resolved_config`` (already committed, Round 9) already computes this
    exact list — ``[name for name, service_raw in (raw_profile.get("services") or
    {}).items() if isinstance(service_raw, dict) and
    service_raw.get("persistence") is not None]`` — and puts it in
    ``resolved_config["persistence_services"]`` when non-empty. Unlike
    ``data_initialization``, this genuinely IS "a property of a profile", not a
    per-deployment choice — DR-0028's own reasoning for excluding the former
    argues FOR including this one. This field still, alone, drives RESTORE
    attachment (``DR-0029`` decision 5: "the restore attaches to the wave carrying
    persistence services").

    **``deploy_wave`` — DR-0029 (docs/decisions/DR-0029-wave-orchestration-is-
    built.md), superseding DR-0028 decision 5 and Erratum E1's wave-model
    framing.** An EARLIER version of this docstring argued at length that a
    per-service wave-rank field was unnecessary — that seam-b's three-tier
    outcome was fully reproducible from ``persistence_services`` plus a
    document's own ``kind`` alone. That argument is WITHDRAWN: DR-0029 records
    that it is exactly the reasoning that "STRUCTURALLY PREVENTED
    ``deploy.plan_waves`` from computing 'matched to any service'" and halted
    Round 10's "load-and-plan" component three times, because a kind test
    cannot express the real rule — ``StatefulSet``/``DaemonSet``/``CronJob`` are
    workloads that belong to services (not automatically wave 0), and a
    ``Secret`` belonging to a named service is not automatically infrastructure
    either. Kind answers a different question (what ``Wave.jobs``/
    ``Wave.deployments`` gate on), never "which wave".

    DR-0029 also corrects the record on where the per-service ranking comes
    from: ``deploy_wave`` is not v1 shipped behaviour (it never appears outside
    ``reference-code/seedpod/PLAN-wave-orchestration.md``, a design plan v1
    never executed — v1's actual, shipped code has only the binary
    ``persistence_services`` split) — v2 *builds* wave orchestration here,
    realising that plan, rather than porting anything. The plan's own worked
    example (``PLAN-wave-orchestration.md``): datastores wave 1,
    migration/init Jobs wave 2, application services wave 3 — default 3 so a
    profile declaring no ``deploy_wave`` anywhere behaves exactly like today's
    single apply (DR-0029's own back-compat rule).

    Field shape: ``Mapping[str, int]``, service name -> wave rank, mirroring
    the plan's own profile-YAML schema (``services.<name>.deploy_wave: N``)
    field for field — ``seedpod/app/services/deployment_service.py``'s
    ``_build_resolved_config`` is the (out-of-scope-for-this-component) writer
    that will populate ``resolved_config["deploy_wave"]`` from that schema, and
    ``deploy.load_audit``/``deploy.plan_waves`` (Round 10's "load-and-plan"
    component, ``seedpod/engine/steps/deploy.py``) are this field's readers.

    **The KEY SET is the contract, and it means two different things depending
    on which side of the writer you are on — this is the one thing to get
    exactly right (DR-0029 decision 2: "``DeploymentProfile`` ... gains the
    FULL service-name-to-``deploy_wave`` mapping", because the trimmed
    ``persistence_services``-only shape "structurally prevents computing
    'matched to any service'").** By construction, the WRITER
    (``_build_resolved_config``) puts an entry in this mapping for **every
    service the profile declares** (its whole ``services:`` YAML block, the
    same set ``persistence_services`` is filtered from), regardless of whether
    that service's YAML sets ``deploy_wave`` explicitly — a service that
    doesn't is filled with the DEFAULT, 3, at WRITE time, not looked up with a
    default later. So by the time ``deploy.plan_waves`` reads this field, "is
    this service's key present" and "is this service declared by the profile
    at all" are the SAME question, always. Two distinct outcomes follow, and
    they are not in tension despite reading similarly:

    - A document whose classified service (via the three-heuristic matcher —
      see ``_split_manifests_by_service`` above) IS one of this profile's
      declared services **always finds a key** in this mapping (never a
      reader-side default) — DR-0029's own worked example: datastores 1,
      migration/init Jobs 2, application services 3, and any DECLARED service
      that never set ``deploy_wave`` in YAML also reads back 3, because the
      WRITER put it there, not because the reader guessed.
    - A document that matches **no declared service at all** (its name/labels
      match no key in this mapping, because it belongs to none of the
      profile's ``services:`` — RBAC, a bare ``ConfigMap``, the ghcr pull
      secret) goes to **wave 0**, the leading infrastructure tier (DR-0029
      decision 3: "documents matching no service go to wave 0"). This is
      never 3 and is not a "default" in the ``.get(key, 3)`` sense — it is a
      structurally different case (no key exists for ANY value of the
      lookup), not a present-but-unset one.

    An entirely EMPTY mapping (``{}``, this field's own default — a profile
    declaring literally no services at all) is the degenerate case of the
    second bullet: no key can ever match, so every document — including ones
    that would otherwise be workloads — falls to wave 0. This is distinct from
    "every declared service defaults to 3" (DR-0029's actual back-compat
    case, Consequences: "a profile declaring no ``deploy_wave`` anywhere
    produces exactly one wave"), which is a NON-empty mapping — one entry per
    declared service, every value 3 — so every document matches some service,
    all at rank 3, one wave.

    ``rollout_timeout_seconds`` and the restore criteria are DELIBERATELY NOT
    modeled here even though ``_build_resolved_config`` computes the first
    alongside ``persistence_services``: both already sit as their OWN top-level
    fields on ``DeployLoadAuditOutput``/``PlanWavesParams``
    (``rollout_timeout_seconds`` predates this rekeying; ``data_initialization``
    is added alongside it, for the identical reason — see ``SnapshotRestoreSpec``'s
    docstring). Duplicating either inside ``profile`` too would give two
    disagreeing paths to the same fact with no consumer reading either duplicate —
    exactly the speculative-field shape this docstring exists to rule out (a
    standard that ``persistence_services``/``deploy_wave`` both meet and
    ``rollout_timeout_seconds``/``data_initialization`` do not, which is why
    those two stay excluded even as ``deploy_wave`` joins)."""

    persistence_services: list[str] = []
    deploy_wave: Mapping[str, int] = {}


# ---------------------------------------------------------------------------
# SnapshotRestoreSpec (+ RestoreFromLatest) — DR-0028 decision 3
# ---------------------------------------------------------------------------


class RestoreFromLatest(BaseModel):
    """``data_initialization.restore_from_latest``'s criteria
    (``reference-code/seedpod/seedpod/jobs/state/deployment_job.py:249-260``):
    ``deploy.restore_snapshot`` (Round 10, not this component) lists snapshots
    filtered by ``branch``/``profile``, keeps only those newer than
    ``max_age_days`` when set, then takes the most recent. Every field optional,
    matching v1's own ``criteria.get(...)`` reads — an absent criterion just
    doesn't filter on that axis — and the ALREADY-COMMITTED, identically-shaped
    ``seedpod/api/routers/presets.py`` ``RestoreFromLatest`` (Round 6): that
    module's own request type for ``POST /presets/{id}/deploy``'s
    ``data_initialization``, which is the exact request DR-0028 cites as this
    fact's origin (``DeployFromPresetRequest.data_initialization``,
    ``reference-code/seedpod/seedpod/api/presets.py:386,554-564``).

    A SEPARATE class, not an import of the API-layer one: ``core/`` is the
    innermost layer and never imports from ``api/`` (the same rule that keeps
    ``core/`` from importing ``services/`` — see this module's own docstring). The
    API type still exists, still does the request parsing for
    ``POST /presets/{id}/deploy``, and is currently accepted but not executed
    (``seedpod/app/services/preset_service.py``'s own module docstring); wiring
    ``PresetService.deploy``/``DeploymentService.deploy_direct`` to map its
    ``DataInitialization``/``RestoreFromLatest`` onto this module's
    ``SnapshotRestoreSpec``/``RestoreFromLatest``, field for field, is Round 10
    verb-building work this component does not do (``seedpod/app`` is out of
    this component's authorized scope)."""

    branch: str | None = None
    profile: str | None = None
    max_age_days: int | None = None


class SnapshotRestoreSpec(BaseModel):
    """DR-0028 decision 3: replaces the fixture stand-in's incomplete
    ``snapshot_id: str`` with v1's real two modes
    (``reference-code/seedpod/seedpod/jobs/state/deployment_job.py:220-265``,
    ``_perform_snapshot_restore``) — an explicit ``restore_from_snapshot`` id, OR
    ``restore_from_latest`` criteria (``RestoreFromLatest`` above).
    ``deploy.restore_snapshot`` (Round 10, not this component) resolves criteria
    to a concrete snapshot id AT EXECUTE TIME, via the existing snapshot service:
    "resolving it at deployment birth would freeze a choice that a newer snapshot
    may supersede before the restore actually runs" (DR-0028's own words) — this
    type only carries the criteria; it never resolves them itself (a pure
    ``core/`` type cannot call a service; resolving needs IO).

    ``services`` mirrors v1's own ``data_initialization.get("services")``
    (``deployment_job.py:274`` — just past DR-0028's own "244-265" citation for
    the two-mode resolution, but the same restore flow, read a few lines later in
    the same function): an optional allow-list restricting the restore to
    selected persisted services ("Restoring only selected services", v1's own log
    message). Carried, not invented: the ALREADY-COMMITTED
    ``seedpod/api/routers/presets.py`` ``DataInitialization`` (Round 6) already
    accepts this field over HTTP, and dropping it here would silently strand a
    request field v2 already accepts with nowhere left for it to flow.

    ``Wave.restore: SnapshotRestoreSpec | None`` (Seam B §2.2 Proof 1) is the
    conditional-as-data exemplar DR-0022 P4 names: ``None`` is the typed no-op,
    never an empty ``SnapshotRestoreSpec()``. This type does not itself validate
    "exactly one mode set" — mirroring ``DataInitialization``, which carries no
    such check either: v1's own resolution is precedence-based, not
    mutually-exclusive-enforced (``restore_from_snapshot`` wins outright when both
    happen to be present, ``deployment_job.py:246-248``), and a
    ``SnapshotRestoreSpec`` with NEITHER mode set resolves to "not an error - just
    no data to restore" (v1's own log line, ``deployment_job.py:269``) — a
    verb-level no-op ``deploy.restore_snapshot`` decides, not a construction-time
    rejection this DTO should impose (that would be strictly stricter than v1, a
    behaviour change DR-0028 does not ask for).

    Sourced from the deploy REQUEST, carried through
    ``resolved_config["data_initialization"]`` (DR-0028 decision 2; wiring
    ``deploy_direct``/``PresetService.deploy`` to actually populate that key is
    Round 10 verb-building work, outside ``seedpod/app`` which this component does
    not touch), and read back by ``deploy.load_audit`` (Round 10) "like every
    other resolved fact" (DR-0028's own words) onto a NEW top-level
    ``data_initialization`` field on its Output — mirrored on ``PlanWavesParams``,
    exactly like the already-committed ``rollout_timeout_seconds``/
    ``resolved_images`` are top-level, NOT nested inside ``profile`` (see
    ``DeploymentProfile``'s own docstring for why: decision 2's "not a profile
    field" is the identical reason). ``deploy.plan_waves`` (Round 10) is what
    actually attaches a resolved ``SnapshotRestoreSpec`` to some ``Wave.restore``
    in its output list — see ``Wave``'s own docstring below for why that must
    NOT be the same ``Wave`` object as the one carrying the persistence-service
    ``docs``."""

    restore_from_snapshot: str | None = None
    restore_from_latest: RestoreFromLatest | None = None
    services: list[str] | None = None


# ---------------------------------------------------------------------------
# Wave — Seam B §2.2 Proof 1, verbatim, unchanged (grouping model: DR-0029)
# ---------------------------------------------------------------------------


class Wave(BaseModel):
    """Seam B §2.2 Proof 1's field list, verbatim and unchanged (DR-0028 decision
    5's field SHAPE was never faulted, and neither DR-0029 nor the erratum it
    supersedes touches it): one ordered step of ``deploy-waves.yml``'s manifest
    rollout, produced by ``deploy.plan_waves`` (Round 10's "load-and-plan"
    component, ``seedpod/engine/steps/deploy.py``).

    ``restore`` is ``None`` for every wave except one, and only when the
    deployment declared ``data_initialization`` (``SnapshotRestoreSpec``'s own
    docstring) — the conditional-as-data exemplar (DR-0022 P4).
    ``gate_timeout_seconds`` generalizes v1's single hardcoded
    database-readiness wait (``deployment_job.py:526-560``'s ``timeout=180``,
    passed to ``_wait_for_database_pods_ready``) to a per-wave value carried as
    data, sourced from ``rollout_timeout_seconds`` (never re-hardcoded).

    **DR-0029 (docs/decisions/DR-0029-wave-orchestration-is-built.md) governs
    which wave carries what and how documents are grouped into waves --
    superseding DR-0028 decision 5's framing and DR-0028 Erratum E1's wave-model
    framing outright (DR-0029 §3-§5).** v2 *builds* wave orchestration --
    realising ``reference-code/seedpod/PLAN-wave-orchestration.md``, a plan v1
    itself never executed -- rather than porting a v1 binary split.

    **Grouping is by service NAME, never by document kind (DR-0029 §3).** Match
    a document's ``metadata.name``/labels against ``DeploymentProfile.deploy_wave``
    using the same three-heuristic classifier DR-0028's Consequences already
    require (name equality-or-prefix, ``metadata.labels.app`` /
    ``spec.template.metadata.labels.app``, ``spec.selector.matchLabels.app``) to
    find WHICH service a document belongs to, then look up that service's rank
    (default 3, "back-compat single apply" -- DR-0029 §1/§2/Consequences).
    **Documents matching no service go to wave 0** -- the leading, always-
    applied-first infrastructure tier (RBAC/ConfigMaps/Secrets/ghcr-secret).
    Not a kind test: a ``StatefulSet``/``DaemonSet``/``CronJob`` belonging to a
    named service is a workload in THAT service's wave, not automatically wave
    0; a ``Secret`` belonging to a named service is not automatically
    infrastructure either (DR-0029 §3, "Not a kind test"). Document kind still
    answers a *different* question -- what ``Wave.jobs``/``Wave.deployments``
    gate on (DR-0029 §4) -- just never "which wave".

    **The restore attaches to the wave carrying ``persistence_services``**
    (DR-0029 §5, ``seam-b:225`` verbatim: "restore attached to the persistence
    wave only when the profile declares data_initialization... as data, one
    loop"). ``restore`` is attached DIRECTLY to the SAME ``Wave`` whose ``docs``
    are the matched persistence-service documents -- there is NO separate,
    empty-``docs`` wave carrying only ``restore`` (DR-0028 Erratum E1 point 2
    survives DR-0029 unchanged; only its "three unmatched/non-workload tiers"
    wave-MODEL framing was withdrawn). Erratum E1's own words: "This dissolves
    the empty-wave question rather than answering it."

    **This does re-open, deliberately, the exact ordering question the
    now-withdrawn two-wave design existed to dodge, and it is not this type's
    job to fully resolve it -- naming it here, precisely, so it is not
    silently lost.** ``config/workflows/deploy-waves.yml``'s per-wave step
    order is fixed and IDENTICAL for every wave: ``prep`` -> ``apply``
    (`kube.apply_docs`) -> ``restore`` (`deploy.restore_snapshot`) ->
    ``restart`` -> ``ready`` (`deploy.await_wave`, the readiness GATE) -- so
    on the persistence wave, ``restore`` runs straight after that SAME wave's
    own `apply`, with NO gate between them confirming the database pod is
    actually up yet, unlike v1's explicit Phase 1b
    `_wait_for_database_pods_ready(timeout=180)` (`deployment_job.py:552-566`)
    which ran BETWEEN apply and restore.

    **Correction to an earlier revision of this docstring**: it attributed a
    sentence -- "the persistence workload... its own `ready` gate confirms
    the database pods" -- to DR-0028 Erratum E1 point 1. That sentence does
    not appear in E1; E1 point 1's only readiness-gate mention is the wave-0
    deadlock rationale (a Secret/ServiceAccount landing in a LATER wave would
    deadlock that wave's OWN readiness gate -- nothing to do with `restore`
    inside the SAME wave), and DR-0029 §5 does not revisit ordering at all.
    Struck, not merely reworded, so a future reader does not inherit a
    citation that never checks out.

    Making the restore itself safe against a not-yet-ready database is
    `deploy.restore_snapshot`'s own problem to solve in full (the later
    restore-and-rehydrate component, not `deploy.plan_waves` or this type),
    but this component's own fix-pass wires the one mechanism available that
    v1's own inline `asyncio.sleep`-free equivalent did not have:
    `config/workflows/deploy-waves.yml`'s `restore` step now DOES declare a
    `retry:` policy -- the engine's OWN `Schedule` (CLAUDE.md: "the engine's
    `Schedule` owns retry"), not a step-internal poll loop. This absorbs a
    `TransientError` `deploy.restore_snapshot` raises for "the database isn't
    reachable yet" the same way `apply`/`restart` already absorb kubectl's own
    transient connectivity blips via the named `kubectl_default` policy --
    engine-scheduled attempts at the restore call itself, never something
    `Wave`'s own field SHAPE needs to change to express. **Sizing that budget
    correctly is `deploy.restore_snapshot`'s own component's job, discharged**:
    the named `kubectl_default` policy (`Schedule(3, 2.0, 2.0, 15.0)`, ~6s of
    total backoff, tuned for H6's kubectl connectivity retries) is NOT reused
    here -- `deploy-waves.yml`'s `restore` step instead declares its own
    explicit inline retry (`max_attempts: 19, base_delay_seconds: 10,
    factor: 1.0, max_delay_seconds: 10`, ~180s of fixed-interval backoff)
    sized to replicate v1's own `_wait_for_database_pods_ready(timeout=180)`
    budget for "wait for a just-applied database to accept connections",
    rather than freezing a number this docstring cannot justify on a policy
    tuned for a different failure mode.

    ``kube.apply_docs`` remains total on empty input (DR-0028 Erratum E1 point
    3, unchanged by DR-0029): an empty ``docs`` list is a typed no-op returning
    an empty ``ApplyChangeSummary``, issuing no ``KubeApplyManifest`` --
    defensive, not load-bearing, once the "no separate empty-docs wave" rule
    above holds."""

    index: int
    docs: list[ManifestDoc]
    jobs: list[str]
    deployments: list[str]
    gate_timeout_seconds: int
    restore: SnapshotRestoreSpec | None = None


# ---------------------------------------------------------------------------
# ApplyChangeSummary — DR-0028 decision 4
# ---------------------------------------------------------------------------


class ApplyChangeSummary(BaseModel):
    """``kube.apply_docs``'s Output (Round 10, not this component): kubectl's own
    three-way per-resource verdict, bucketed by resource identity — v1's own
    ``kubectl apply`` stdout lines read literally, e.g.
    ``deployment.apps/foo configured`` / ``service/bar unchanged`` /
    ``configmap/baz created`` (the three literal substrings
    ``deployment_job.py:600-607``'s comment names).
    ``seedpod/providers/kubectl.py``'s ``_apply_manifest`` already returns this
    exact stdout as ``Result(stdout)`` (default, un-flagged ``kubectl apply -f``,
    no ``-o`` output-format override) — DR-0028 decision 4's own citation. DR-0028
    decision 4: the shape (three lists) was already right; what was missing was
    the SEMANTIC, and ``all_unchanged`` below is where it now lives, tested,
    rather than nowhere.

    ``all_unchanged`` is the one fact ``deploy.ensure_rollouts`` (Round 10, not
    this component) needs to reproduce the "was anything actually applied"
    half of v1's real rule (``deployment_job.py:598-614``, gotcha 4): force a
    rollout restart ONLY if every resource kubectl reported on was
    'unchanged' — if anything was 'configured' or 'created', kubectl already
    triggered the rollout itself, and restarting again would be redundant, not
    merely harmless (v1's own comment: "skipping rollout restart (kubectl
    apply already triggered rollout)"). v1 decides this with a substring scan
    of the WHOLE kubectl stdout blob (``"configured" in output_lower or
    "created" in output_lower`` etc) rather than genuine per-resource
    bucketing; ``kube.apply_docs``'s own parsing of ``Result(stdout)`` into
    these three lists (Round 10, not this component) is the modernization
    DR-0028 calls for, and ``all_unchanged`` is its output-independent,
    pinned-by-test decision function — the load-bearing semantic DR-0028's
    audit found undocumented, now documented AND tested here rather than left
    for the verb to (re)derive.

    **Precision on "v1's real rule", not just DR-0028's own shorthand for it**
    (DR-0028 decision 4 itself states the rule exactly as ``all_unchanged``
    implements it: "force a rollout restart only if every resource was
    unchanged"): v1's ACTUAL condition, read off ``deployment_job.py:518-524``
    and ``:609-626`` together, is ``is_update and not manifest_changed`` — a
    SECOND gate, ``is_update`` ("the cluster already has Kubernetes
    Deployments", checked ONCE, before any apply, at the top of the whole
    deployment), that ``all_unchanged`` alone does not encode. In practice
    ``all_unchanged`` implies the "not manifest_changed" half only:
    ``manifest_changed`` is False in v1 exactly when
    (a) every reported resource was 'unchanged' (this type's own condition) or
    (b) ``kubectl_output`` was empty (v1's ``manifest_changed = False`` never
    gets set to True inside ``if kubectl_output:``, see next paragraph). This
    type does not (and structurally cannot) carry ``is_update`` — it is a
    per-apply resource-identity bucketing, not cluster state — so it is
    ``deploy.ensure_rollouts``'s job, not this DTO's, to decide whether v1's
    ``is_update`` gate still needs a home once Seam B's per-WAVE restart
    decision (``deploy.ensure_rollouts`` runs once per ``Wave``, unlike v1's
    single whole-deployment decision) is built; this docstring intentionally
    does not resolve that for the verb, only names it so it is not
    silently missed.

    Seam B's "unknown => assume changed" is retained here as: no resource counted
    as ``unchanged`` at all (``unchanged`` empty) is NEVER ``all_unchanged``, even
    when ``configured``/``created`` are ALSO both empty. An apply that produced no
    parseable per-resource lines whatsoever (an unparseable or empty kubectl
    result — the literal 'unknown' case) must not read as "everything was
    unchanged" by the vacuous truth that no resource was found NOT unchanged
    either. DR-0028's own words: "assume-changed implies do not restart" — this
    is the property that makes that safe: an unparseable apply result never
    forces a rollout restart of resources ``kube.apply_docs`` could not even
    confirm still exist. **This is a deliberate divergence from v1, not a
    coincidence of the shape above**: v1's OWN empty-``kubectl_output`` path
    leaves ``manifest_changed = False`` (the ``if kubectl_output:`` guard at
    ``deployment_job.py:610`` never runs, so the variable keeps its
    initial value) and therefore, combined with ``is_update``, v1 CAN restart
    on an empty/unparseable apply result — exactly the case this type's
    ``all_unchanged`` refuses to treat as "safe to restart" (see the property
    below)."""

    configured: list[str] = []
    created: list[str] = []
    unchanged: list[str] = []

    @property
    def all_unchanged(self) -> bool:
        """True iff at least one resource was reported, and every one of them was
        'unchanged'. This is DR-0028 decision 4's own stated rule for when v1
        forces a rollout restart (deployment_job.py:598-614) — but see the
        class docstring's "Precision on v1's real rule" paragraph: v1's full
        condition also gated on ``is_update``, which this property alone does
        not encode (deliberately -- this type cannot see cluster state).
        See the class docstring for why an entirely empty summary (the
        'unknown' case) must return False, not vacuously True, and why that
        is a deliberate divergence from v1's own empty-output behaviour."""
        return bool(self.unchanged) and not self.configured and not self.created
