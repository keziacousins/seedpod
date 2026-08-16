"""tests/core/test_deploy_wave.py — the five DR-0028 deploy-path DTOs
(``seedpod/core/deploy_wave.py``): ``ManifestDoc``'s YAML round trip,
``SnapshotRestoreSpec``'s two v1 restore modes, ``ApplyChangeSummary``'s pinned
restart semantic, and ``DeploymentProfile``/``Wave``'s field sets.

Zero Mock/patch (CLAUDE.md testing posture): every fixture below is either a real
shipped manifest template read straight off disk, or a plain literal.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from seedpod.core.deploy_wave import (
    ApplyChangeSummary,
    DeploymentProfile,
    ManifestDoc,
    RestoreFromLatest,
    SnapshotRestoreSpec,
    Wave,
    parse_manifest_documents,
    serialize_manifest_documents,
)
from seedpod.core.errors import ErrorCode, PermanentError

REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLECO_STACK_TEMPLATES = REPO_ROOT / "config" / "manifest-templates" / "exampleco-stack"


def _real_multi_doc_manifest() -> str:
    """Two REAL shipped templates, joined the same way
    ``services/manifests.py``'s ``_render_templates`` joins per-file renders
    (``"\\n---\\n".join(...)``): ``rbac.yaml`` (ServiceAccount/Role/RoleBinding,
    all ``namespace: default``, and itself starts with a bare ``---``) +
    ``postgres.yaml`` (Deployment/Service, no ``metadata.namespace`` at all --
    the cluster-default case). Both are free of Jinja ``{% %}`` control tags
    (grep-verified across ``config/manifest-templates/exampleco-stack/``); every
    ``{{ }}`` in them sits inside an already-quoted YAML string, so plain
    ``yaml.safe_load_all`` parses it as literal text, never rendered -- this
    module is about ``ManifestDoc``'s own parse/serialize mechanics, not
    template rendering (``tests/services/test_manifests.py``'s job)."""
    rbac = (EXAMPLECO_STACK_TEMPLATES / "rbac.yaml").read_text()
    postgres = (EXAMPLECO_STACK_TEMPLATES / "postgres.yaml").read_text()
    return rbac + "\n---\n" + postgres


# ---------------------------------------------------------------------------
# ManifestDoc: parse_manifest_documents / serialize_manifest_documents
# ---------------------------------------------------------------------------


def test_parse_manifest_documents_against_a_real_shipped_multi_doc_manifest():
    docs = parse_manifest_documents(_real_multi_doc_manifest())
    assert [(d.kind, d.name, d.namespace) for d in docs] == [
        ("ServiceAccount", "exampleco-api-sa", "default"),
        ("Role", "job-reader", "default"),
        ("RoleBinding", "exampleco-api-job-reader", "default"),
        ("Deployment", "postgres", ""),  # postgres.yaml declares no metadata.namespace
        ("Service", "postgres", ""),
    ]
    # `body` is the FULL parsed document, not just the three denormalized fields.
    assert docs[0].body["apiVersion"] == "v1"
    assert docs[3].body["spec"]["replicas"] == 1
    assert docs[3].body["spec"]["template"]["spec"]["containers"][0]["name"] == "postgres"


def test_round_trip_manifest_doc_to_yaml_to_manifest_doc_is_lossless():
    """ManifestDoc -> YAML -> ManifestDoc, against a real multi-document
    manifest: parse it, serialize it back, re-parse the result, and confirm the
    SECOND parse is identical to the first. This is the round trip DR-0028
    decision 1 requires -- ``kube.apply_docs`` must be able to serialize
    ``list[ManifestDoc]`` back to the ``str`` ``KubeApplyManifest.manifest_yaml``
    requires, without losing anything ``parse_manifest_documents`` captured."""
    original = parse_manifest_documents(_real_multi_doc_manifest())
    assert len(original) == 5  # sanity: both files' documents actually landed

    serialized = serialize_manifest_documents(original)
    assert isinstance(serialized, str)

    round_tripped = parse_manifest_documents(serialized)
    assert round_tripped == original


def test_serialized_output_round_trips_through_plain_yaml_too():
    """``seedpod/providers/contract.py``'s ``KubeApplyManifest.manifest_yaml`` is
    a bare ``str`` that ``providers/kubectl.py`` writes to a file for
    ``kubectl apply -f`` -- confirms the serialized form is plain, valid
    multi-document YAML on its own terms (not just round-trippable through this
    module's own parser), and that no document was dropped or merged."""
    docs = parse_manifest_documents(_real_multi_doc_manifest())
    manifest_yaml = serialize_manifest_documents(docs)
    reparsed_raw = [d for d in yaml.safe_load_all(manifest_yaml) if d is not None]
    assert len(reparsed_raw) == len(docs) == 5
    assert [d["kind"] for d in reparsed_raw] == [doc.kind for doc in docs]


def test_serialize_manifest_documents_constructs_the_real_kube_apply_manifest_command():
    """DR-0028's whole reason for existing, pinned directly rather than merely
    argued in a docstring (DR-0028's Problem section: "a verb built to the
    declared shape could not have called its own service"). ``kube.apply_docs``
    (Round 10, not this component) is ``ManifestDoc``'s real consumer, and its
    real Seam C command is ``seedpod/providers/contract.py``'s
    ``KubeApplyManifest`` -- a frozen dataclass (no IO on construction) whose
    ``manifest_yaml: str`` field is exactly what ``serialize_manifest_documents``
    produces, fed to ``kubectl apply -f`` by ``providers/kubectl.py``'s
    ``_apply_manifest``. Constructing the real command type (not just asserting
    "it is a str" -- the two tests above) is the DR-0028 consumer check; the
    ``FrozenInstanceError`` assertion pins "frozen dataclass" itself, the
    property this module's own docstring leans on to call the boundary pure."""
    from dataclasses import FrozenInstanceError

    from seedpod.providers.contract import KubeApplyManifest

    docs = parse_manifest_documents(_real_multi_doc_manifest())
    manifest_yaml = serialize_manifest_documents(docs)

    command = KubeApplyManifest(kubeconfig="apiVersion: v1\nkind: Config\n", manifest_yaml=manifest_yaml)

    assert command.manifest_yaml == manifest_yaml
    with pytest.raises(FrozenInstanceError):
        command.manifest_yaml = "mutated"  # type: ignore[misc]


def test_parse_manifest_documents_skips_none_documents():
    """v1's own ``_split_manifests_by_service``
    (``reference-code/seedpod/seedpod/jobs/state/deployment_job.py:91-92``) skips
    a ``None`` document produced by an empty ``---``-delimited section -- exactly
    what joining several per-template renders with ``"\\n---\\n"`` can produce
    when one render is blank."""
    text = "---\n---\nkind: ConfigMap\nmetadata: {name: cm-1}\n---\nkind: Secret\nmetadata: {name: sec-1}\n"
    docs = parse_manifest_documents(text)
    assert [(d.kind, d.name) for d in docs] == [("ConfigMap", "cm-1"), ("Secret", "sec-1")]


def test_parse_manifest_documents_empty_string_yields_no_documents():
    """``normalize_resolved_manifests(None)`` (``services/manifests.py``) returns
    ``""`` -- this function must tolerate that input cleanly, not raise."""
    assert parse_manifest_documents("") == []


def test_parse_manifest_documents_raises_permanent_error_on_malformed_yaml():
    """Genuine correctness fix, not a v1 bug pin: v1's
    ``_split_manifests_by_service`` fails OPEN on a YAML parse error
    (``deployment_job.py:127-129``, ``except yaml.YAMLError: ...; return "",
    rendered_manifests``), silently downgrading a malformed manifest into "no
    database manifests, apply everything as one wave, skip the restore phase
    entirely" with only a log line. This is the one place in v2's redesigned
    pipeline that parses the raw manifest text at all, so it raises instead,
    naming the failure via the one taxonomy home."""
    malformed = "kind: ConfigMap\n  bad_indent: [\n"
    with pytest.raises(PermanentError) as exc_info:
        parse_manifest_documents(malformed)
    assert exc_info.value.code == ErrorCode.INVALID_INPUT


@pytest.mark.parametrize(
    "malformed,doc_index,expected_type",
    [
        ("just a string\n", 0, "str"),
        ("- a\n- b\n", 0, "list"),
        ("---\nkind: ConfigMap\nmetadata: {name: cm-1}\n---\n42\n", 1, "int"),
    ],
)
def test_parse_manifest_documents_raises_permanent_error_on_non_mapping_document(
    malformed, doc_index, expected_type
):
    """A document that is valid YAML but not a *mapping* (a bare scalar, a
    list, a number) is a narrower malformed-input case than
    ``test_..._raises_permanent_error_on_malformed_yaml`` above -- it never
    raises ``yaml.YAMLError`` at all, so that guard alone does not catch it.
    Unguarded, ``raw.get("metadata")`` would raise a bare ``AttributeError``,
    escaping the one error taxonomy this whole codebase funnels through
    (CLAUDE.md's hard rule) from the ONE place a rendered manifest is ever
    parsed -- verified: all three inputs below raise exactly that
    ``AttributeError`` against the unguarded code. Matches the committed idiom
    one module over (``environment_config.py``'s
    ``create_environment_variables_from_dict``,
    ``PermanentError(ErrorCode.INVALID_INPUT)`` naming the actual type)."""
    with pytest.raises(PermanentError) as exc_info:
        parse_manifest_documents(malformed)
    assert exc_info.value.code == ErrorCode.INVALID_INPUT
    assert exc_info.value.detail == {"document_index": str(doc_index), "type": expected_type}


def test_manifest_doc_defaults_are_empty_strings_not_none():
    """Absent kind/name/namespace default to ``""`` (matching v1's
    ``doc.get(..., "")`` pattern), not ``None`` -- a bare ``ManifestDoc()`` with
    no arguments must still satisfy the field types."""
    doc = ManifestDoc()
    assert (doc.kind, doc.name, doc.namespace, dict(doc.body)) == ("", "", "", {})


def test_parse_manifest_documents_raises_permanent_error_on_non_mapping_metadata():
    """A narrower malformed-input case one level DEEPER than the top-level
    document guard above: a document that IS itself a valid mapping but whose
    ``metadata`` key holds a non-mapping (``metadata: oops``) -- unguarded,
    ``metadata.get("name")`` would raise the same bare ``AttributeError`` this
    module's error taxonomy exists to prevent from escaping."""
    with pytest.raises(PermanentError) as exc_info:
        parse_manifest_documents("kind: ConfigMap\nmetadata: oops\n")
    assert exc_info.value.code == ErrorCode.INVALID_INPUT
    assert exc_info.value.detail == {"document_index": "0", "field": "metadata", "type": "str"}


def test_parse_manifest_documents_coerces_yaml_typed_scalar_names_to_str():
    """v1 passed the rendered manifest text straight to ``kubectl`` -- an
    all-numeric ``metadata.name`` (a real case: a template rendering
    ``name: {{ branch }}`` with a numeric branch, e.g. ``2024``, is a legal
    DNS-1123 k8s name) is valid YAML that pydantic's strict ``str`` field would
    otherwise reject with a raw ``ValidationError``, outside this module's
    error taxonomy. Coercing with ``str(...)`` is the genuine correctness fix:
    this must not fail a deploy v1 would have accepted."""
    docs = parse_manifest_documents("kind: ConfigMap\nmetadata:\n  name: 12345\n")
    assert docs == [ManifestDoc(kind="ConfigMap", name="12345", body={"kind": "ConfigMap", "metadata": {"name": 12345}})]


# ---------------------------------------------------------------------------
# SnapshotRestoreSpec / RestoreFromLatest: both v1 restore modes, real shapes
# ---------------------------------------------------------------------------


def test_snapshot_restore_spec_parses_the_explicit_snapshot_id_mode():
    """v1's ``data_initialization["restore_from_snapshot"]``
    (``reference-code/seedpod/seedpod/jobs/state/deployment_job.py:246-248``) --
    the real dict shape a deploy request's ``data_initialization`` carries."""
    spec = SnapshotRestoreSpec.model_validate({"restore_from_snapshot": "snap-abc123"})
    assert spec.restore_from_snapshot == "snap-abc123"
    assert spec.restore_from_latest is None


def test_snapshot_restore_spec_parses_the_restore_from_latest_mode():
    """v1's ``data_initialization["restore_from_latest"]`` criteria dict
    (``deployment_job.py:249-260``): ``branch``/``profile``/``max_age_days``."""
    spec = SnapshotRestoreSpec.model_validate(
        {"restore_from_latest": {"branch": "main", "profile": "exampleco-web-2", "max_age_days": 14}}
    )
    assert spec.restore_from_snapshot is None
    assert spec.restore_from_latest == RestoreFromLatest(branch="main", profile="exampleco-web-2", max_age_days=14)


def test_restore_from_latest_criteria_are_all_individually_optional():
    """v1's own ``criteria.get(...)`` reads (``deployment_job.py:249-260``) -- an
    absent criterion just doesn't filter on that axis, never a required field."""
    spec = SnapshotRestoreSpec.model_validate({"restore_from_latest": {}})
    assert spec.restore_from_latest == RestoreFromLatest(branch=None, profile=None, max_age_days=None)


def test_snapshot_restore_spec_carries_the_services_allow_list():
    """v1's ``data_initialization.get("services")`` (``deployment_job.py:274``,
    just past DR-0028's own "244-265" citation but the same restore flow) --
    already accepted over HTTP by the committed
    ``seedpod/api/routers/presets.py`` ``DataInitialization`` (Round 6); this
    type must carry it forward, not silently drop it."""
    spec = SnapshotRestoreSpec.model_validate(
        {"restore_from_snapshot": "snap-1", "services": ["postgres", "keycloak-postgres"]}
    )
    assert spec.services == ["postgres", "keycloak-postgres"]


def test_snapshot_restore_spec_with_neither_mode_is_not_rejected():
    """v1's own resolution treats "neither mode present" as a benign no-op --
    "not an error - just no data to restore" (``deployment_job.py:269``) -- not a
    construction-time error this DTO should impose (that would be a stricter-
    than-v1 behaviour change DR-0028 does not ask for)."""
    spec = SnapshotRestoreSpec()
    assert (spec.restore_from_snapshot, spec.restore_from_latest, spec.services) == (None, None, None)


def test_snapshot_restore_spec_matches_the_committed_api_layer_shape():
    """``seedpod/api/routers/presets.py``'s ``DataInitialization``/
    ``RestoreFromLatest`` (Round 6) already accept exactly this shape over HTTP
    -- confirms the field NAMES line up 1:1, so mapping one onto the other
    (Round 10 verb-building work, not this component's) is a straight copy,
    never a re-derivation."""
    from seedpod.api.routers.presets import DataInitialization as ApiDataInitialization
    from seedpod.api.routers.presets import RestoreFromLatest as ApiRestoreFromLatest

    assert set(SnapshotRestoreSpec.model_fields) == set(ApiDataInitialization.model_fields)
    assert set(RestoreFromLatest.model_fields) == set(ApiRestoreFromLatest.model_fields)


# ---------------------------------------------------------------------------
# ApplyChangeSummary: the restart semantic (DR-0028 decision 4)
# ---------------------------------------------------------------------------


def test_all_unchanged_true_when_every_bucket_but_unchanged_is_empty():
    summary = ApplyChangeSummary(unchanged=["deployment.apps/exampleco-api", "service/exampleco-api"])
    assert summary.all_unchanged is True


def test_all_unchanged_false_if_anything_was_configured():
    """v1's real rule: ANY 'configured' resource means kubectl already triggered
    the rollout -- restarting again would be redundant, not merely harmless
    (``deployment_job.py:598-614``)."""
    summary = ApplyChangeSummary(configured=["deployment.apps/exampleco-api"], unchanged=["service/exampleco-api"])
    assert summary.all_unchanged is False


def test_all_unchanged_false_if_anything_was_created():
    summary = ApplyChangeSummary(created=["configmap/exampleco-api-config"], unchanged=["service/exampleco-api"])
    assert summary.all_unchanged is False


def test_all_unchanged_false_when_everything_is_empty_the_unknown_case():
    """Seam B's "unknown => assume changed", DR-0028's own words: "assume-changed
    implies do not restart" -- an unparseable/empty apply result must not read
    as "everything was unchanged" by vacuous truth (no bucket is non-empty, so a
    naive ``not configured and not created`` alone would wrongly be True here)."""
    summary = ApplyChangeSummary()
    assert summary.all_unchanged is False


def test_all_unchanged_is_false_when_mixed_even_with_unchanged_present():
    summary = ApplyChangeSummary(
        configured=["deployment.apps/a"], unchanged=["service/a"], created=["configmap/a"]
    )
    assert summary.all_unchanged is False


def test_the_inverted_restart_condition_would_fail_this_pin():
    """DR-0028 decision 4: "must be pinned by a test that would fail if the
    condition were inverted." An inverted rule (restart whenever ANYTHING
    changed, rather than only when NOTHING did) disagrees with ``all_unchanged``
    on this exact case -- proving the property above is load-bearing, not a
    vacuous truism any definition would satisfy."""
    changed_something = ApplyChangeSummary(configured=["deployment.apps/a"], unchanged=["service/a"])
    inverted_would_restart = bool(changed_something.configured or changed_something.created)
    assert inverted_would_restart != changed_something.all_unchanged


# ---------------------------------------------------------------------------
# DeploymentProfile / Wave: field sets
# ---------------------------------------------------------------------------


def test_deployment_profile_has_no_data_initialization_field():
    """DR-0028 decision 2: not narrowed to a typed mapping ON THIS TYPE -- removed
    outright, since it is a per-deployment choice, not a profile property.

    ``deploy_wave`` joined the field set under DR-0029 (wave orchestration is
    BUILT, realising v1's never-implemented PLAN-wave-orchestration.md): DR-0028
    decision 2's ``persistence_services``-only shape structurally prevented
    ``deploy.plan_waves`` from computing "matched to any service", which is what
    halted Round 10 three times."""
    assert "data_initialization" not in DeploymentProfile.model_fields
    assert set(DeploymentProfile.model_fields) == {"persistence_services", "deploy_wave"}


def test_deployment_profile_persistence_services_defaults_empty():
    assert DeploymentProfile().persistence_services == []


def test_deployment_profile_carries_the_real_persistence_services_shape():
    """``seedpod/app/services/deployment_service.py``'s ``_build_resolved_config``
    (Round 9, already committed) computes exactly this list shape for
    ``resolved_config["persistence_services"]`` -- a real example being
    ``config/deployment-profiles/exampleco-dev-stack-nodns.yml``'s ``postgres``
    service, which declares a ``persistence:`` block."""
    profile = DeploymentProfile(persistence_services=["postgres", "keycloak-postgres"])
    assert profile.persistence_services == ["postgres", "keycloak-postgres"]


def test_deployment_profile_deploy_wave_defaults_empty():
    """DR-0029 decision 2: the field DOES exist, but a bare ``DeploymentProfile``
    (no ``resolved_config["deploy_wave"]`` at all -- an audit row written before
    this round, or a profile with no ``services:`` block) carries an empty
    mapping, never ``None``."""
    assert DeploymentProfile().deploy_wave == {}


def test_deployment_profile_deploy_wave_a_declared_service_with_no_explicit_yaml_rank_is_3():
    """DR-0029 decision 2/Consequences: the WRITER (``_build_resolved_config``,
    out of this component's scope) fills every DECLARED service into this
    mapping at write time -- a service whose YAML never sets ``deploy_wave``
    reads back the plan's own default, 3, not because the READER guesses a
    default, but because the key is ALREADY there by the time
    ``deploy.plan_waves`` looks it up. Pinned here as the type-level contract
    the docstring states: a key present in this mapping IS a declared service,
    whether or not its rank was explicit in YAML."""
    profile = DeploymentProfile(persistence_services=[], deploy_wave={"exampleco-api": 3, "postgres": 1})
    assert profile.deploy_wave["exampleco-api"] == 3
    assert profile.deploy_wave["postgres"] == 1


def test_deployment_profile_deploy_wave_back_compat_all_declared_services_default_3_is_not_empty():
    """DR-0029 Consequences: "a profile declaring no ``deploy_wave`` anywhere
    produces exactly one wave, behaving like today's single apply" -- this is
    the writer filling EVERY declared service with 3, a mapping with one entry
    PER SERVICE, not the empty ``{}`` case below. Every document then matches
    some service, all at rank 3 -- one wave, not wave 0 for everything."""
    profile = DeploymentProfile(deploy_wave={"exampleco-api": 3, "postgres": 3, "keycloak": 3})
    assert set(profile.deploy_wave.values()) == {3}
    assert profile.deploy_wave != {}


def test_deployment_profile_deploy_wave_empty_mapping_means_no_service_can_ever_match():
    """DR-0029 decision 3: "documents matching no service go to wave 0" -- the
    degenerate case of a genuinely EMPTY mapping (as opposed to the
    back-compat case above, which is non-empty with every value 3): no key
    exists for any document to match, so EVERY document -- including what
    would otherwise be an ordinary workload -- is "matching no service" and
    would fall to wave 0. Pinned at the type level since this module carries
    no classifier of its own (``deploy.plan_waves``, Round 10's
    "load-and-plan" component, owns the actual document-to-service matching);
    this test only pins that an empty mapping structurally cannot produce a
    match for ANY service name, which is the property that rule depends on."""
    profile = DeploymentProfile(deploy_wave={})
    assert "exampleco-api" not in profile.deploy_wave
    assert "postgres" not in profile.deploy_wave
    assert len(profile.deploy_wave) == 0


def test_wave_field_list_matches_seam_b_proof_1_verbatim():
    doc = ManifestDoc(kind="Deployment", name="postgres", namespace="", body={"kind": "Deployment"})
    wave = Wave(
        index=0,
        docs=[doc],
        jobs=["exampleco-atlas-migrations"],
        deployments=["postgres"],
        gate_timeout_seconds=180,
        restore=SnapshotRestoreSpec(restore_from_snapshot="snap-1"),
    )
    assert wave.index == 0
    assert wave.docs == [doc]
    assert wave.jobs == ["exampleco-atlas-migrations"]
    assert wave.deployments == ["postgres"]
    assert wave.gate_timeout_seconds == 180
    assert wave.restore == SnapshotRestoreSpec(restore_from_snapshot="snap-1")


def test_wave_restore_defaults_to_none_the_conditional_as_data_case():
    """DR-0022 P4's exemplar: ``None``, never an empty ``SnapshotRestoreSpec()``,
    is the typed no-op for a wave with nothing to restore."""
    wave = Wave(index=3, docs=[], jobs=[], deployments=[], gate_timeout_seconds=300)
    assert wave.restore is None
