"""tests/services/test_manifests.py — golden-render tests for
``seedpod.services.manifests.ManifestResolver`` over small inline template fixtures
(``tmp_path``), plus the per-service resolution order (override -> external ->
commit-tag -> primary-branch -> fallback-chain) against a FAKE GhcrService, and the
``normalize_resolved_manifests`` gotcha-12 tolerance.

Reuses ``tests.services.fake_ghcr``'s ``FakeGhcrBackend``/``FakeGhcrTransport`` — the
real transport seam ``GhcrService`` talks to — so GHCR fault injection here is the same
mechanism as ``tests/services/test_ghcr.py``. No Mock/patch anywhere.

Services-layer suite: every ``ManifestProfile`` here is a hand-built, inline-fixture
value (this module never reads ``config/deployment-profiles/*.yml`` off disk and never
imports from ``seedpod.app.*`` -- ``seedpod/services/manifests.py``'s own module
docstring: "the caller now builds and passes in a ``ManifestProfile`` directly", i.e.
the services layer knows nothing of ``app/``). The end-to-end tests that exercise the
REAL, committed, shipped ``config/`` tree (combining ``seedpod.app.services.profiles.
load_deployment_profile``/``_build_resolved_config`` with this module's own
``ManifestResolver``) live in ``tests/app/test_manifest_resolution_end_to_end.py`` --
relocated there (Round 9 fix pass, review finding) precisely because they need both
layers and previously imported the app-layer ``_build_resolved_config`` into this
services-layer file, inverting that dependency direction. They pin the 2026-08-03
smoke's ``exampleco-web-2`` manifest-resolution crash, plus DR-0025's real-profile
regression pin.
"""

from __future__ import annotations

import base64
import json
from pathlib import Path

import httpx
import pytest

from seedpod.core.environment_config import EnvironmentVariables
from seedpod.core.errors import PermanentError
from seedpod.services.ghcr import GhcrConfig, GhcrService
from seedpod.services.manifests import (
    ManifestProfile,
    ManifestResolver,
    ServiceSpec,
    normalize_resolved_manifests,
)
from tests.services.fake_ghcr import FakeGhcrBackend, FakeGhcrTransport

# asyncio_mode = "auto" (pyproject.toml) picks up every `async def test_*` here
# without an explicit marker; the normalize_resolved_manifests tests below are
# plain sync functions and don't need one either.

_ORG = "exampleco"


def _ghcr(backend: FakeGhcrBackend) -> GhcrService:
    transport = httpx.AsyncClient(transport=FakeGhcrTransport(backend))
    return GhcrService(
        GhcrConfig(token="fake-token", organization=_ORG), transport
    )  # pragma: allowlist secret


def _write_template(manifests_dir: Path, filename: str, content: str) -> None:
    manifests_dir.mkdir(parents=True, exist_ok=True)
    (manifests_dir / filename).write_text(content)


# ============================================================================
# golden render: images/secrets/config vars, service-name hyphen->underscore,
# only declared-service-stem templates rendered, non-empty concatenation
# ============================================================================


async def test_golden_render_over_inline_template_fixture(tmp_path: Path):
    _write_template(
        tmp_path,
        "exampleco-core.yaml",
        "kind: Deployment\nimage: {{ images.exampleco_core }}\ndb: {{ secrets.db_password }}\nreplicas: {{ config.replicas }}\n",
    )
    _write_template(tmp_path, "exampleco-web.yaml", "kind: Deployment\nimage: {{ images.exampleco_web }}\n")
    # not a declared service -> must be skipped entirely, even though it's a .yaml file
    _write_template(tmp_path, "unrelated-thing.yaml", "should: never-appear\n")

    profile = ManifestProfile(
        name="exampleco-stack",
        manifests_dir=tmp_path,
        services={
            "exampleco-core": ServiceSpec(repository="exampleco-core"),
            "exampleco-web": ServiceSpec(
                repository="exampleco-web", image_override="ghcr.io/x/exampleco-web:pinned"
            ),
        },
    )
    resolver = ManifestResolver(ghcr_service=None)

    result = await resolver.resolve(
        profile,
        triggering_repo="exampleco-core",
        triggering_branch="main",
        triggering_image="ghcr.io/exampleco/exampleco-core:main-abc123",
        secrets={"db_password": "hunter2"},
        config={"replicas": 3},
    )

    assert result.template_files == ("exampleco-core.yaml", "exampleco-web.yaml")
    assert "unrelated-thing.yaml" not in result.rendered_manifests
    assert "# Generated from exampleco-core.yaml" in result.rendered_manifests
    assert "# Generated from exampleco-web.yaml" in result.rendered_manifests
    assert "image: ghcr.io/exampleco/exampleco-core:main-abc123" in result.rendered_manifests
    assert "db: hunter2" in result.rendered_manifests
    assert "replicas: 3" in result.rendered_manifests
    assert "image: ghcr.io/x/exampleco-web:pinned" in result.rendered_manifests
    # rendered_parts = [header1, content1, header2, content2] -> 3 separators
    assert result.rendered_manifests.count("\n---\n") == 3


async def test_resolve_render_false_skips_rendering_but_still_resolves_images_and_secrets(tmp_path: Path):
    """DR-0025 Erratum E2 point (i): ``resolve(render=False)`` runs image
    resolution + GHCR-auth secret synthesis in FULL, but issues no template
    render at all -- proving a template that WOULD raise (StrictUndefined on
    an unrelated undefined name) never actually gets a chance to."""
    _write_template(tmp_path, "exampleco-core.yaml", "kind: Deployment\nimage: {{ images.exampleco_core }}\nhost: {{ this_name_is_undefined_and_would_raise }}\n")

    profile = ManifestProfile(
        name="exampleco-stack", manifests_dir=tmp_path,
        services={"exampleco-core": ServiceSpec(repository="exampleco-core")},
    )
    resolver = ManifestResolver(ghcr_service=None)

    result = await resolver.resolve(
        profile, triggering_repo="exampleco-core", triggering_branch="main",
        triggering_image="ghcr.io/exampleco/exampleco-core:main-abc123",
        secrets={}, config={}, render=False,
    )

    assert result.rendered_manifests == ""
    assert result.template_files == ()
    assert result.resolved_images["exampleco-core"].image_url == "ghcr.io/exampleco/exampleco-core:main-abc123"


async def test_render_only_re_renders_against_already_decided_images_and_secrets(tmp_path: Path):
    """DR-0025 Erratum E2 point (ii)'s deploy-time half: ``render_only`` takes
    the FLAT ``resolved_images`` shape a persisted audit row actually carries
    (``{name: image_url}``, never the richer typed ``ResolvedImage``) and
    reconstructs enough to render -- no GHCR call, no image re-resolution."""
    _write_template(
        tmp_path, "exampleco-core.yaml",
        "kind: Deployment\nimage: {{ images.exampleco_core }}\nhost: {{ cluster_hostname }}\n",
    )
    profile = ManifestProfile(
        name="exampleco-stack", manifests_dir=tmp_path,
        services={"exampleco-core": ServiceSpec(repository="exampleco-core")},
    )
    resolver = ManifestResolver(ghcr_service=None)

    template_files, rendered = resolver.render_only(
        profile,
        resolved_image_urls={"exampleco-core": "ghcr.io/exampleco/exampleco-core:main-abc123"},
        resolved_secrets={},
        resolved_config={"cluster_hostname": "203.0.113.42"},
    )

    assert template_files == ("exampleco-core.yaml",)
    assert "image: ghcr.io/exampleco/exampleco-core:main-abc123" in rendered
    assert "host: 203.0.113.42" in rendered


async def test_empty_render_excluded_from_concatenation(tmp_path: Path):
    _write_template(tmp_path, "exampleco-core.yaml", "{% if false %}never{% endif %}")
    _write_template(tmp_path, "exampleco-web.yaml", "kind: Deployment\n")

    profile = ManifestProfile(
        name="p",
        manifests_dir=tmp_path,
        services={
            "exampleco-core": ServiceSpec(repository="exampleco-core"),
            "exampleco-web": ServiceSpec(repository="exampleco-web", image_override="x"),
        },
    )
    resolver = ManifestResolver(ghcr_service=None)

    result = await resolver.resolve(
        profile,
        triggering_repo="exampleco-core",
        triggering_branch="main",
        triggering_image="ghcr.io/x/exampleco-core:main",
    )

    assert result.template_files == ("exampleco-web.yaml",)
    assert "exampleco-core" not in "".join(result.template_files)


async def test_missing_manifests_dir_raises_permanent_error(tmp_path: Path):
    profile = ManifestProfile(
        name="p",
        manifests_dir=tmp_path / "does-not-exist",
        services={"exampleco-core": ServiceSpec(repository="exampleco-core")},
    )
    resolver = ManifestResolver(ghcr_service=None)

    with pytest.raises(PermanentError):
        await resolver.resolve(
            profile,
            triggering_repo="exampleco-core",
            triggering_branch="main",
            triggering_image="ghcr.io/x/exampleco-core:main",
        )


async def test_env_var_context_missing_cluster_id_raises_naming_cluster_id_not_empty_string(tmp_path: Path):
    """Review finding pin: `env_var_context` copies `cluster_id`/`environment` by
    PRESENCE, never `.get(key, "")` -- the exact DR-0025 defect shape (docs/
    decisions/DR-0025-hostname-resolution-ordering.md), for two keys that aren't
    the hostname. `config={}` (a `resolve(config=...)` caller that omits
    `cluster_id` entirely -- Round 10's deploy verbs re-resolving against a
    freshly-built config is the imminent real case) must raise naming
    `cluster_id`, never silently render `CLUSTER_ID: ""`. Not reachable from
    either current `DeploymentService` call site (`_build_resolved_config`
    always sets both keys unconditionally), but `resolve(config=...)` is a
    public service API, not a private detail of those two callers."""
    profile = ManifestProfile(
        name="p",
        manifests_dir=tmp_path,
        services={"svc": ServiceSpec(repository="svc")},
        environment_variables=EnvironmentVariables(shared={"CLUSTER_ID": "{{ cluster_id }}"}),
    )
    resolver = ManifestResolver(ghcr_service=None)

    with pytest.raises(PermanentError, match="cluster_id"):
        await resolver.resolve(
            profile,
            triggering_repo="svc",
            triggering_branch="main",
            triggering_image="ghcr.io/x/svc:main",
            config={},  # cluster_id genuinely absent, never ""
        )


async def test_worker_image_placeholder_is_deliberately_empty_not_skipped(tmp_path: Path):
    """v1-faithful, not a silent-empty regression this round exists to close
    (seedpod/services/manifests.py's own module docstring, the ``images_dict``
    concession paragraph): a declared, non-required service whose image never
    resolved (GHCR came back empty) still renders ITS OWN template -- with a
    literal empty ``image:`` -- rather than being silently dropped. Deliberate:
    v1's own lenient ``Environment()`` produces the IDENTICAL empty-string
    output via default-``Undefined``-to-``""`` coercion (reference-code .../
    manifest_resolver.py:864-868), and an empty ``image:`` does not deploy
    green (unlike the hostname/``https:///`` case), so there is no
    silently-broken infrastructure here for ``StrictUndefined`` to guard
    against."""
    _write_template(tmp_path, "worker.yaml", 'kind: Deployment\nimage: "{{ images.worker }}"\n')
    profile = ManifestProfile(
        name="p",
        manifests_dir=tmp_path,
        services={
            "core": ServiceSpec(repository="core"),
            "worker": ServiceSpec(repository="worker", required=False),
        },
        fallback_branches=("main",),
    )
    backend = FakeGhcrBackend()  # no images at all -> worker resolves to nothing
    resolver = ManifestResolver(ghcr_service=_ghcr(backend))

    result = await resolver.resolve(
        profile,
        triggering_repo="core",
        triggering_branch="main",
        triggering_image="ghcr.io/x/core:main",
    )

    assert "worker" not in result.resolved_images
    assert "worker.yaml" in result.template_files
    assert 'image: ""' in result.rendered_manifests


# ============================================================================
# GHCR pull-secret auto-generation (Round 9, org-and-ghcr component) --
# ManifestResolver._add_ghcr_auth_if_needed + _render_templates' infra-template
# pass (module-level _INFRASTRUCTURE_TEMPLATES). Byte-identical in SHAPE to the
# real shipped config/manifest-templates/{exampleco-misc,exampleco-stack}/
# ghcr-secret.yaml (both READ before writing this fixture) -- see
# tests/app/test_manifest_resolution_end_to_end.py for the same mechanism
# exercised against the REAL exampleco-web-2 profile end to end.
# ============================================================================

_GHCR_SECRET_TEMPLATE = (
    "apiVersion: v1\n"
    "kind: Secret\n"
    "metadata:\n"
    "  name: ghcr-secret\n"
    "  labels:\n"
    '    environment: "{{ config.environment }}"\n'
    '    cluster-id: "{{ config.cluster_id }}"\n'
    "type: kubernetes.io/dockerconfigjson\n"
    "data:\n"
    '  .dockerconfigjson: "{{ secrets.ghcr_dockerconfig_json }}"\n'
)


def _decode_dockerconfigjson(value: str) -> dict:
    return json.loads(base64.b64decode(value).decode())


async def test_ghcr_secret_rendered_when_ghcr_image_resolved_and_decodes_to_valid_dockerconfigjson(
    tmp_path: Path,
):
    """DELIVERABLE 2's core proof: a profile whose only service resolves to a
    real ``ghcr.io`` image, with a GHCR token configured, gets a rendered
    ``ghcr-secret.yaml`` -- ABSENT from ``template_files`` before this
    component (module docstring's now-CLOSED bullet, ``seedpod/services/
    manifests.py``) -- whose ``.dockerconfigjson`` decodes to the standard
    docker-config-json structure, naming ``ghcr.io`` and the configured
    organization/token as username/password, with the ``auth`` field's own
    inner base64 layer correct too (not double-encoded)."""
    _write_template(tmp_path, "app.yaml", 'kind: Deployment\nimage: "{{ images.app }}"\n')
    _write_template(tmp_path, "ghcr-secret.yaml", _GHCR_SECRET_TEMPLATE)
    profile = ManifestProfile(name="p", manifests_dir=tmp_path, services={"app": ServiceSpec(repository="app")})
    backend = FakeGhcrBackend()
    backend.add_version("app", digest="sha256:aaa", tags=["main-abc123"])
    resolver = ManifestResolver(ghcr_service=_ghcr(backend))

    result = await resolver.resolve(
        profile,
        triggering_repo="__nothing_triggers_this__",
        triggering_branch="main",
        triggering_image="unused:unused",
        config={"environment": "ephemeral", "cluster_id": "c-1"},
    )

    assert result.resolved_images["app"].image_url.startswith("ghcr.io/exampleco/app:")
    assert "ghcr-secret.yaml" in result.template_files

    ghcr_secret_value = result.resolved_secrets["ghcr_dockerconfig_json"]
    decoded = _decode_dockerconfigjson(ghcr_secret_value)
    auth_entry = decoded["auths"]["ghcr.io"]
    assert auth_entry["username"] == "exampleco"
    assert auth_entry["password"] == "fake-token"  # pragma: allowlist secret
    assert base64.b64decode(auth_entry["auth"]).decode() == "exampleco:fake-token"
    # The template plumbs the value through UNMODIFIED -- no second, template-side
    # b64encode layered on top of what this resolver already computed.
    assert f'.dockerconfigjson: "{ghcr_secret_value}"' in result.rendered_manifests


async def test_ghcr_secret_not_emitted_when_no_service_resolves_a_ghcr_io_image(tmp_path: Path):
    """DELIVERABLE 3: a profile with no ``ghcr.io`` images anywhere must not
    emit a pull secret, even with a real GHCR token configured and a
    ``ghcr-secret.yaml`` template present in ``manifests_dir`` -- the file
    existing is not enough; the CONDITION (some resolved, non-external image
    referencing ``ghcr.io``) must actually be met."""
    _write_template(tmp_path, "app.yaml", 'kind: Deployment\nimage: "{{ images.app }}"\n')
    _write_template(tmp_path, "ghcr-secret.yaml", _GHCR_SECRET_TEMPLATE)
    profile = ManifestProfile(
        name="p",
        manifests_dir=tmp_path,
        services={"app": ServiceSpec(repository="app", image_override="docker.io/library/nginx:latest")},
    )
    resolver = ManifestResolver(ghcr_service=_ghcr(FakeGhcrBackend()))

    result = await resolver.resolve(
        profile,
        triggering_repo="__nothing_triggers_this__",
        triggering_branch="main",
        triggering_image="unused:unused",
        config={"environment": "ephemeral", "cluster_id": "c-1"},
    )

    assert "ghcr_dockerconfig_json" not in result.resolved_secrets
    assert "ghcr-secret.yaml" not in result.template_files


async def test_ghcr_secret_not_emitted_when_ghcr_service_is_none(tmp_path: Path):
    """No token configured (``ghcr_service=None``) is a legitimate, silent
    degradation -- never an error -- even when a service's own
    ``image_override`` happens to be a ``ghcr.io`` URL (an operator-pinned
    image needing no registry lookup at all): the condition requires BOTH a
    resolved ``ghcr.io`` image AND ``self.ghcr_service`` configured."""
    _write_template(tmp_path, "app.yaml", 'kind: Deployment\nimage: "{{ images.app }}"\n')
    _write_template(tmp_path, "ghcr-secret.yaml", _GHCR_SECRET_TEMPLATE)
    profile = ManifestProfile(
        name="p",
        manifests_dir=tmp_path,
        services={"app": ServiceSpec(repository="app", image_override="ghcr.io/exampleco/app:pinned")},
    )
    resolver = ManifestResolver(ghcr_service=None)

    result = await resolver.resolve(
        profile,
        triggering_repo="__nothing_triggers_this__",
        triggering_branch="main",
        triggering_image="unused:unused",
        config={"environment": "ephemeral", "cluster_id": "c-1"},
    )

    assert "ghcr_dockerconfig_json" not in result.resolved_secrets
    assert "ghcr-secret.yaml" not in result.template_files


async def test_ghcr_secret_excludes_external_images_from_the_condition(tmp_path: Path):
    """v1's own exclusion (reference-code .../manifest_resolver.py:1078-1081,
    ``if not img.is_external``), salvaged: an ``external: true`` service whose
    ``image_override`` happens to be a ``ghcr.io`` URL does not, by itself,
    trigger pull-secret generation -- an external service's image was never
    actually resolved BY this resolver (module docstring's own "found by
    definition, but nothing to resolve" framing for ``external``), so it says
    nothing about whether the cluster needs GHCR auth."""
    _write_template(tmp_path, "app.yaml", 'kind: Deployment\nimage: "{{ images.app }}"\n')
    _write_template(tmp_path, "ghcr-secret.yaml", _GHCR_SECRET_TEMPLATE)
    profile = ManifestProfile(
        name="p",
        manifests_dir=tmp_path,
        services={
            "app": ServiceSpec(repository="app", image_override="docker.io/library/nginx:latest"),
            "sidecar": ServiceSpec(
                repository="sidecar",
                external=True,
                required=False,
                image_override="ghcr.io/exampleco/sidecar:pinned",
            ),
        },
    )
    resolver = ManifestResolver(ghcr_service=_ghcr(FakeGhcrBackend()))

    result = await resolver.resolve(
        profile,
        triggering_repo="__nothing_triggers_this__",
        triggering_branch="main",
        triggering_image="unused:unused",
        config={"environment": "ephemeral", "cluster_id": "c-1"},
    )

    assert "ghcr_dockerconfig_json" not in result.resolved_secrets
    assert "ghcr-secret.yaml" not in result.template_files


async def test_ghcr_secret_overwrites_a_caller_supplied_value_when_condition_met(tmp_path: Path):
    """v1's own unconditional assignment (reference-code .../
    manifest_resolver.py:1095, ``resolved_secrets['ghcr_dockerconfig_json'] =
    ghcr_auth``), salvaged verbatim: a caller-supplied
    ``secrets={"ghcr_dockerconfig_json": ...}`` is REPLACED, not preserved,
    once the condition is met -- this resolver invents no new precedence rule
    for a key v1 never let a caller win on."""
    _write_template(tmp_path, "app.yaml", 'kind: Deployment\nimage: "{{ images.app }}"\n')
    _write_template(tmp_path, "ghcr-secret.yaml", _GHCR_SECRET_TEMPLATE)
    profile = ManifestProfile(name="p", manifests_dir=tmp_path, services={"app": ServiceSpec(repository="app")})
    backend = FakeGhcrBackend()
    backend.add_version("app", digest="sha256:aaa", tags=["main-abc123"])
    resolver = ManifestResolver(ghcr_service=_ghcr(backend))

    result = await resolver.resolve(
        profile,
        triggering_repo="__nothing_triggers_this__",
        triggering_branch="main",
        triggering_image="unused:unused",
        secrets={"ghcr_dockerconfig_json": "caller-supplied-placeholder"},
        config={"environment": "ephemeral", "cluster_id": "c-1"},
    )

    value = result.resolved_secrets["ghcr_dockerconfig_json"]
    assert value != "caller-supplied-placeholder"
    assert _decode_dockerconfigjson(value)["auths"]["ghcr.io"]["username"] == "exampleco"


async def test_ghcr_secret_render_failure_raises_permanent_error_not_swallowed(tmp_path: Path):
    """Genuine correctness fix, not a v1 bug pin (``_render_templates``'s own
    infra-template-pass comment, ``seedpod/services/manifests.py``): v1 wraps
    THIS specific render in ``except Exception: logger.warning(...); continue``
    (reference-code .../manifest_resolver.py:942-943) and ships with no pull
    secret at all, silently. This resolver does not carry that forward -- a
    template failure once the condition is met is a real defect. Isolated via
    a ``config`` that supplies everything ``app.yaml`` needs (nothing) but
    omits what ``ghcr-secret.yaml`` itself needs (``environment``/
    ``cluster_id``), so the per-service loop succeeds and ONLY the
    infra-template pass fails."""
    _write_template(tmp_path, "app.yaml", 'kind: Deployment\nimage: "{{ images.app }}"\n')
    _write_template(tmp_path, "ghcr-secret.yaml", _GHCR_SECRET_TEMPLATE)
    profile = ManifestProfile(name="p", manifests_dir=tmp_path, services={"app": ServiceSpec(repository="app")})
    backend = FakeGhcrBackend()
    backend.add_version("app", digest="sha256:aaa", tags=["main-abc123"])
    resolver = ManifestResolver(ghcr_service=_ghcr(backend))

    with pytest.raises(PermanentError, match="ghcr-secret.yaml"):
        await resolver.resolve(
            profile,
            triggering_repo="__nothing_triggers_this__",
            triggering_branch="main",
            triggering_image="unused:unused",
            config={},  # ghcr-secret.yaml needs config.environment/config.cluster_id; app.yaml needs neither
        )


async def test_ghcr_secret_absent_from_manifests_dir_is_silently_skipped_even_when_condition_met(
    tmp_path: Path,
):
    """Existence-gated, matching v1's own ``template_file.exists()`` check
    (reference-code .../manifest_resolver.py:924): a profile's
    ``manifests_dir`` need not carry ``ghcr-secret.yaml`` at all -- a normal,
    silent skip, never an error, even when the condition (a real ``ghcr.io``
    image + a configured token) is otherwise met. Auth generation itself
    (``resolved_secrets``) is unaffected by whether a template exists to
    consume it -- only the RENDER pass is existence-gated."""
    _write_template(tmp_path, "app.yaml", 'kind: Deployment\nimage: "{{ images.app }}"\n')
    # deliberately no ghcr-secret.yaml written to tmp_path
    profile = ManifestProfile(name="p", manifests_dir=tmp_path, services={"app": ServiceSpec(repository="app")})
    backend = FakeGhcrBackend()
    backend.add_version("app", digest="sha256:aaa", tags=["main-abc123"])
    resolver = ManifestResolver(ghcr_service=_ghcr(backend))

    result = await resolver.resolve(
        profile,
        triggering_repo="__nothing_triggers_this__",
        triggering_branch="main",
        triggering_image="unused:unused",
        config={"environment": "ephemeral", "cluster_id": "c-1"},
    )

    assert "ghcr_dockerconfig_json" in result.resolved_secrets
    assert result.template_files == ("app.yaml",)


# ============================================================================
# triggering-repo shortcut + branch-slash sanitization
# ============================================================================


async def test_triggering_repo_uses_triggering_image_sanitized_never_queries_ghcr(tmp_path: Path):
    _write_template(tmp_path, "exampleco-core.yaml", "image: {{ images.exampleco_core }}\n")

    profile = ManifestProfile(
        name="p",
        manifests_dir=tmp_path,
        services={"exampleco-core": ServiceSpec(repository="exampleco-core")},
    )
    backend = (
        FakeGhcrBackend()
    )  # empty — if the resolver queries it, find_image returns None -> KeyError below
    resolver = ManifestResolver(ghcr_service=_ghcr(backend))

    result = await resolver.resolve(
        profile,
        triggering_repo="exampleco-core",
        triggering_branch="feature/FIN-123",
        triggering_image="ghcr.io/exampleco/exampleco-core:feature/FIN-123",
    )

    assert backend.call_count == 0
    image = result.resolved_images["exampleco-core"]
    assert (
        image.image_url == "ghcr.io/exampleco/exampleco-core:feature-FIN-123"
    )  # '/' -> '-' in the TAG half only
    assert image.resolved_branch == "feature/FIN-123"
    assert image.is_override is False


# ============================================================================
# per-service resolution order: override > external > commit-tag > primary >
# fallback-chain
# ============================================================================


async def test_image_override_wins_outright_no_ghcr_call(tmp_path: Path):
    _write_template(tmp_path, "sidecar.yaml", "image: {{ images.sidecar }}\n")
    profile = ManifestProfile(
        name="p",
        manifests_dir=tmp_path,
        services={
            "core": ServiceSpec(repository="core"),
            "sidecar": ServiceSpec(repository="sidecar", image_override="ghcr.io/x/sidecar:pinned"),
        },
    )
    backend = FakeGhcrBackend()
    backend.add_version("core", digest="sha256:a", tags=["main-abc0001"])
    resolver = ManifestResolver(ghcr_service=_ghcr(backend))

    result = await resolver.resolve(
        profile,
        triggering_repo="core",
        triggering_branch="main",
        triggering_image="ghcr.io/x/core:main",
    )

    sidecar = result.resolved_images["sidecar"]
    assert sidecar.image_url == "ghcr.io/x/sidecar:pinned"
    assert sidecar.is_override is True
    assert backend.call_count == 0  # core is the triggering repo (shortcut), sidecar is an override


async def test_runtime_image_overrides_take_precedence_over_profile_image_override(tmp_path: Path):
    _write_template(tmp_path, "sidecar.yaml", "image: {{ images.sidecar }}\n")
    profile = ManifestProfile(
        name="p",
        manifests_dir=tmp_path,
        services={
            "core": ServiceSpec(repository="core"),
            "sidecar": ServiceSpec(
                repository="sidecar", image_override="ghcr.io/x/sidecar:profile-default"
            ),
        },
    )
    resolver = ManifestResolver(ghcr_service=None)

    result = await resolver.resolve(
        profile,
        triggering_repo="core",
        triggering_branch="main",
        triggering_image="ghcr.io/x/core:main",
        image_overrides={"sidecar": "ghcr.io/x/sidecar:runtime-override"},
    )

    assert result.resolved_images["sidecar"].image_url == "ghcr.io/x/sidecar:runtime-override"


async def test_external_service_skips_registry_lookup_entirely(tmp_path: Path):
    """External services always resolve to ``None`` (v1: "found by definition" but
    nothing to look up) — a caller that declares one must also mark it
    ``required=False``, exactly like v1's ``ServiceConfig`` did, or resolution raises
    same as any other genuinely-unresolved required service."""
    _write_template(tmp_path, "postgres.yaml", "external: true\n")
    profile = ManifestProfile(
        name="p",
        manifests_dir=tmp_path,
        services={
            "core": ServiceSpec(repository="core"),
            "postgres": ServiceSpec(repository="postgres", external=True, required=False),
        },
    )
    backend = FakeGhcrBackend()
    resolver = ManifestResolver(ghcr_service=_ghcr(backend))

    result = await resolver.resolve(
        profile,
        triggering_repo="core",
        triggering_branch="main",
        triggering_image="ghcr.io/x/core:main",
    )

    assert "postgres" not in result.resolved_images  # external -> found=True but nothing to resolve
    assert backend.call_count == 0
    external_query = next(q for q in result.registry_queries if q.repository == "postgres")
    assert external_query.found is True
    assert external_query.resolved_branch == "external"


async def test_commit_specific_tag_preferred_over_primary_branch(tmp_path: Path):
    _write_template(tmp_path, "worker.yaml", "image: {{ images.worker }}\n")
    profile = ManifestProfile(
        name="p",
        manifests_dir=tmp_path,
        services={
            "core": ServiceSpec(repository="core"),
            "worker": ServiceSpec(repository="worker"),
        },
    )
    backend = FakeGhcrBackend()
    backend.add_version("worker", digest="sha256:branch", tags=["main-branchonly"])
    backend.add_version("worker", digest="sha256:commit", tags=["main-abc1234"])
    resolver = ManifestResolver(ghcr_service=_ghcr(backend))

    result = await resolver.resolve(
        profile,
        triggering_repo="core",
        triggering_branch="main",
        triggering_image="ghcr.io/x/core:main",
        commit_sha="abc1234",
    )

    assert result.resolved_images["worker"].image_url == f"ghcr.io/{_ORG}/worker:main-abc1234"
    assert result.resolved_images["worker"].resolved_branch == "main-abc1234"
    assert result.resolved_images["worker"].is_fallback is False


async def test_commit_specific_tag_skipped_when_service_has_branch_override(tmp_path: Path):
    """v1 reference-code .../manifest_resolver.py:608: ``if commit_sha and not
    service_config.branch_override:`` — a branch-pinned service must resolve strictly
    against its own branch, never opportunistically matched against the triggering repo's
    commit SHA. A regressed bare ``if commit_sha:`` guard takes the early
    ``commit_tag = f"{branch_override}-{commit_sha}"`` return (``resolved_branch`` pinned to
    that literal tag, ``is_override`` never set even though this IS an override service) instead
    of the normal override-aware branch lookup — observable via ``resolved_branch``/
    ``is_override`` even when both paths happen to land on the same GHCR tag (GHCR's own
    newest-pattern-match step also matches ``{branch}-{hex}`` tags, so ``image_url`` alone
    isn't a reliable enough signal for this regression)."""
    _write_template(tmp_path, "worker.yaml", "image: {{ images.worker }}\n")
    profile = ManifestProfile(
        name="p",
        manifests_dir=tmp_path,
        services={
            "core": ServiceSpec(repository="core"),
            "worker": ServiceSpec(repository="worker", branch_override="pinned"),
        },
    )
    backend = FakeGhcrBackend()
    backend.add_version("worker", digest="sha256:commit", tags=["pinned-abc1234"])
    resolver = ManifestResolver(ghcr_service=_ghcr(backend))

    result = await resolver.resolve(
        profile,
        triggering_repo="core",
        triggering_branch="main",
        triggering_image="ghcr.io/x/core:main",
        commit_sha="abc1234",
    )

    worker = result.resolved_images["worker"]
    assert worker.resolved_branch == "pinned", "must resolve via the QUERIED branch, not the commit-tag lookup"
    assert worker.is_override is True


async def test_primary_branch_used_when_no_commit_sha_supplied(tmp_path: Path):
    _write_template(tmp_path, "worker.yaml", "image: {{ images.worker }}\n")
    profile = ManifestProfile(
        name="p",
        manifests_dir=tmp_path,
        services={
            "core": ServiceSpec(repository="core"),
            "worker": ServiceSpec(repository="worker"),
        },
    )
    backend = FakeGhcrBackend()
    backend.add_version("worker", digest="sha256:x", tags=["main-def5678"])
    resolver = ManifestResolver(ghcr_service=_ghcr(backend))

    result = await resolver.resolve(
        profile,
        triggering_repo="core",
        triggering_branch="main",
        triggering_image="ghcr.io/x/core:main",
    )

    # resolved_branch is the QUERIED branch, not the matched tag (v1 lines 636-656) —
    # the matched tag lives in image_url.
    worker = result.resolved_images["worker"]
    assert worker.resolved_branch == "main"
    assert "main-def5678" in worker.image_url
    assert worker.is_fallback is False


async def test_fallback_branches_tried_in_profile_order_skipping_already_tried(tmp_path: Path):
    _write_template(tmp_path, "worker.yaml", "image: {{ images.worker }}\n")
    profile = ManifestProfile(
        name="p",
        manifests_dir=tmp_path,
        services={
            "core": ServiceSpec(repository="core"),
            "worker": ServiceSpec(repository="worker"),
        },
        fallback_branches=("dev", "main"),
    )
    backend = FakeGhcrBackend()
    # nothing on "feature/xyz" (the primary branch); "dev" also empty; "main" has an image
    backend.add_version("worker", digest="sha256:x", tags=["main-cafe001"])
    resolver = ManifestResolver(ghcr_service=_ghcr(backend))

    result = await resolver.resolve(
        profile,
        triggering_repo="core",
        triggering_branch="feature/xyz",
        triggering_image="ghcr.io/x/core:feature/xyz",
    )

    # resolved_branch is the fallback branch name tried ("main"), not the matched
    # tag ("main-cafe001") — same "queried, not matched" shape as the primary path.
    worker = result.resolved_images["worker"]
    assert worker.resolved_branch == "main"
    assert "main-cafe001" in worker.image_url
    assert worker.is_fallback is True

    query = next(q for q in result.registry_queries if q.repository == "worker")
    assert query.fallback_used is True


async def test_fallback_branch_equal_to_primary_branch_is_skipped(tmp_path: Path):
    _write_template(tmp_path, "worker.yaml", "image: {{ images.worker }}\n")
    profile = ManifestProfile(
        name="p",
        manifests_dir=tmp_path,
        services={
            "core": ServiceSpec(repository="core"),
            "worker": ServiceSpec(repository="worker", required=False),
        },
        fallback_branches=("main",),
    )
    backend = FakeGhcrBackend()  # no images at all
    resolver = ManifestResolver(ghcr_service=_ghcr(backend))

    result = await resolver.resolve(
        profile,
        triggering_repo="core",
        triggering_branch="main",
        triggering_image="ghcr.io/x/core:main",
    )

    assert "worker" not in result.resolved_images


async def test_required_service_unresolved_raises_permanent_error(tmp_path: Path):
    _write_template(tmp_path, "worker.yaml", "image: {{ images.worker }}\n")
    profile = ManifestProfile(
        name="p",
        manifests_dir=tmp_path,
        services={
            "core": ServiceSpec(repository="core"),
            "worker": ServiceSpec(repository="worker", required=True),
        },
    )
    backend = FakeGhcrBackend()  # no images anywhere
    resolver = ManifestResolver(ghcr_service=_ghcr(backend))

    with pytest.raises(PermanentError) as excinfo:
        await resolver.resolve(
            profile,
            triggering_repo="core",
            triggering_branch="main",
            triggering_image="ghcr.io/x/core:main",
        )

    assert "worker" in str(excinfo.value)


async def test_optional_service_unresolved_is_dropped_not_raised(tmp_path: Path):
    _write_template(tmp_path, "core.yaml", "image: {{ images.core }}\n")
    profile = ManifestProfile(
        name="p",
        manifests_dir=tmp_path,
        services={
            "core": ServiceSpec(repository="core"),
            "worker": ServiceSpec(repository="worker", required=False),
        },
    )
    backend = FakeGhcrBackend()
    resolver = ManifestResolver(ghcr_service=_ghcr(backend))

    result = await resolver.resolve(
        profile,
        triggering_repo="core",
        triggering_branch="main",
        triggering_image="ghcr.io/x/core:main",
    )

    assert "worker" not in result.resolved_images


async def test_ghcr_service_none_and_service_not_overridden_or_external_is_not_found(
    tmp_path: Path,
):
    _write_template(tmp_path, "core.yaml", "image: {{ images.core }}\n")
    profile = ManifestProfile(
        name="p",
        manifests_dir=tmp_path,
        services={
            "core": ServiceSpec(repository="core"),
            "worker": ServiceSpec(repository="worker", required=False),
        },
    )
    resolver = ManifestResolver(ghcr_service=None)

    result = await resolver.resolve(
        profile,
        triggering_repo="core",
        triggering_branch="main",
        triggering_image="ghcr.io/x/core:main",
    )

    assert "worker" not in result.resolved_images
    query = next(q for q in result.registry_queries if q.repository == "worker")
    assert query.found is False
    assert query.error is not None


# ============================================================================
# normalize_resolved_manifests — gotcha 12 str/dict tolerance
# ============================================================================


def test_normalize_none_is_empty_string():
    assert normalize_resolved_manifests(None) == ""


def test_normalize_str_passthrough():
    assert normalize_resolved_manifests("kind: Deployment\n") == "kind: Deployment\n"


def test_normalize_dict_prefers_yaml_key():
    assert (
        normalize_resolved_manifests({"yaml": "kind: Pod\n", "content": "ignored"}) == "kind: Pod\n"
    )


def test_normalize_dict_falls_back_to_content_key():
    assert normalize_resolved_manifests({"content": "kind: Service\n"}) == "kind: Service\n"


def test_normalize_dict_with_neither_key_dumps_whole_mapping():
    result = normalize_resolved_manifests({"kind": "Pod", "metadata": {"name": "x"}})
    assert "kind: Pod" in result
    assert "name: x" in result


# ============================================================================
# DR-0025 Erratum E1 regression pins, at the `_render_templates` mechanism
# level (docs/decisions/DR-0025-hostname-resolution-ordering.md) -- isolated
# from `_build_resolved_config`/real profiles (those integration tests moved to
# tests/app/test_manifest_resolution_end_to_end.py -- see this file's own
# module docstring for why). These two inline-fixture tests pin the
# `template_vars`'s OWN top-level `cluster_hostname` alias directly (the one
# real `{% if cluster_hostname %}` feature gates read), both halves, mirroring
# the shape of the real `config/manifest-templates/exampleco-stack/*.yaml` gates.
# ============================================================================


async def test_cluster_hostname_none_in_config_renders_feature_gate_false_not_raise(tmp_path: Path):
    """Erratum E1: `cluster_hostname` PRESENT in `config`, valued `None` (the
    "profile deliberately has no hostname" case) must let a real `{% if
    cluster_hostname %}` feature gate evaluate FALSE cleanly -- not raise, and
    not leak a placeholder into the rendered output. No shipped profile combines
    a "none" hostname strategy with the exampleco-stack template family (grep-
    verified: every exampleco-stack profile declares either `provider_host` or an
    enabled `dns:` block), so this pins the mechanism directly against an inline
    template mirroring the real gate shape (config/manifest-templates/
    exampleco-stack/mailhog.yaml's own `{% if cluster_hostname %}host: {{
    cluster_hostname }}{% endif %}`)."""
    _write_template(
        tmp_path, "svc.yaml",
        "kind: ConfigMap\ndata:\n"
        "  ingress_host: \"{% if cluster_hostname %}{{ cluster_hostname }}{% else %}none{% endif %}\"\n",
    )
    profile = ManifestProfile(name="p", manifests_dir=tmp_path, services={"svc": ServiceSpec(repository="svc")})
    resolver = ManifestResolver(ghcr_service=None)

    result = await resolver.resolve(
        profile,
        triggering_repo="svc",
        triggering_branch="main",
        triggering_image="ghcr.io/x/svc:main",
        config={"cluster_hostname": None},  # strategy "none" -- key present, valued None
    )

    assert 'ingress_host: "none"' in result.rendered_manifests


async def test_cluster_hostname_omitted_from_config_raises_at_feature_gate(tmp_path: Path):
    """The complementary half, at the SAME `_render_templates` mechanism level:
    `cluster_hostname` OMITTED from `config` entirely (a strategy WANTED a host
    and could not produce one, DR-0025 part 1) must raise the instant a `{% if
    cluster_hostname %}` gate is evaluated -- never silently skip it (which would
    ship infrastructure with no ingress config and no signal that anything is
    wrong). ``test_provider_host_profile_with_no_known_host_raises_naming_
    cluster_hostname`` above already pins this against a real profile, but that
    profile's ``environment_variables:`` block happens to raise FIRST, before any
    template is even opened -- this isolates the OTHER consumer
    (``template_vars``'s own top-level alias) directly."""
    _write_template(tmp_path, "svc.yaml", "gated: \"{% if cluster_hostname %}yes{% endif %}\"\n")
    profile = ManifestProfile(name="p", manifests_dir=tmp_path, services={"svc": ServiceSpec(repository="svc")})
    resolver = ManifestResolver(ghcr_service=None)

    with pytest.raises(PermanentError) as excinfo:
        await resolver.resolve(
            profile,
            triggering_repo="svc",
            triggering_branch="main",
            triggering_image="ghcr.io/x/svc:main",
            config={},  # cluster_hostname genuinely absent -- no strategy resolved it
        )

    assert "cluster_hostname" in str(excinfo.value)


# ============================================================================
# Backlog #17 -- the `is dns_name` template test, at the mechanism level.
# The real-template regression pins live in
# tests/app/test_manifest_resolution_end_to_end.py; these pin the predicate
# itself, including the two DR-0025 states it must NOT disturb.
# ============================================================================


_DNS_GATE = "gated: \"{% if cluster_hostname is dns_name %}{{ cluster_hostname }}{% else %}omitted{% endif %}\"\n"


async def _render_dns_gate(tmp_path: Path, config: dict):
    _write_template(tmp_path, "svc.yaml", _DNS_GATE)
    profile = ManifestProfile(name="p", manifests_dir=tmp_path, services={"svc": ServiceSpec(repository="svc")})
    return await ManifestResolver(ghcr_service=None).resolve(
        profile,
        triggering_repo="svc",
        triggering_branch="main",
        triggering_image="ghcr.io/x/svc:main",
        config=config,
    )


@pytest.mark.parametrize(
    ("hostname", "expected"),
    [
        ("cluster.example.com", "cluster.example.com"),  # ordinary DNS name
        ("minimax.local", "minimax.local"),  # tart/kind's provider host -- why 8 smokes missed #17
        ("minimax", "minimax"),  # single-label: valid for Ingress, and not an IP
        ("203.0.113.40", "omitted"),  # smoke 8's actual droplet IP
        ("203.0.113.42", "omitted"),  # IPv4
        ("2001:db8::1", "omitted"),  # IPv6, bare
        ("[2001:db8::1]", "omitted"),  # IPv6, bracketed -- no more a DNS name than the bare form
        ("  203.0.113.42  ", "omitted"),  # surrounding whitespace must not smuggle an IP through
        ("", "omitted"),  # empty string: DR-0025 bans it outright, but never emit it if one arrives
    ],
)
async def test_dns_name_test_accepts_dns_names_and_refuses_ip_literals(tmp_path: Path, hostname: str, expected: str):
    """`is dns_name` refuses IP literals and NOTHING else -- deliberately narrower
    than real RFC1123 validation. The two errors are not symmetric: too permissive
    and kubectl rejects the apply loudly, exactly as today; too restrictive and the
    host vanishes into a catch-all Ingress that looks fine and routes wrong."""
    result = await _render_dns_gate(tmp_path, {"cluster_hostname": hostname})
    assert f'gated: "{expected}"' in result.rendered_manifests


async def test_dns_name_test_leaves_dr_0025s_none_case_evaluating_false(tmp_path: Path):
    """DR-0025 Erratum E1: `cluster_hostname` present and valued `None` ("this profile
    deliberately has no hostname") must evaluate the gate FALSE cleanly, exactly as the
    plain `{% if cluster_hostname %}` form did. #17 narrows the truthy branch; it must
    not disturb either of the other two states."""
    result = await _render_dns_gate(tmp_path, {"cluster_hostname": None})
    assert 'gated: "omitted"' in result.rendered_manifests


async def test_dns_name_test_still_raises_when_cluster_hostname_is_omitted(tmp_path: Path):
    """DR-0025 part 1, the state that must keep RAISING: key OMITTED means a strategy
    wanted a host and could never produce one. `StrictUndefined` must still fire when
    the gate is a Jinja *test* rather than plain truthiness -- if `_is_dns_name` had
    swallowed `Undefined` into a tidy `False`, #17's fix would have converted a loud
    DR-0025 failure into a silently host-less Ingress."""
    with pytest.raises(PermanentError) as excinfo:
        await _render_dns_gate(tmp_path, {})

    assert "cluster_hostname" in str(excinfo.value)


def test_normalize_rejects_other_types():
    with pytest.raises(TypeError):
        normalize_resolved_manifests(123)  # type: ignore[arg-type]
