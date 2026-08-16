"""tests/app/test_manifest_resolution_end_to_end.py — end-to-end manifest-resolution
tests over the REAL, committed, shipped ``config/`` tree (deployment profiles +
manifest templates), combining ``seedpod.app.services.profiles.load_deployment_profile``
+ ``seedpod.app.services.deployment_service._build_resolved_config`` (app layer) with
``seedpod.services.manifests.ManifestResolver`` (services layer) exactly the way
``DeploymentService._deploy``/``deployment_preview`` do.

**Relocated from ``tests/services/test_manifests.py`` (Round 9 fix pass, review
finding).** That file is the services-layer suite and, per ``seedpod/services/
manifests.py``'s own module docstring ("the caller now builds and passes in a
``ManifestProfile`` directly"), the services layer knows nothing of ``app/`` --
importing the PRIVATE ``_build_resolved_config`` from ``seedpod.app.services.
deployment_service`` into that file inverted that dependency direction. These four
tests are genuinely app+services INTEGRATION tests wearing the wrong layer's file
name; they belong next to ``tests/app/test_deployment_service_resolved_config.py``
(the app-layer unit tests for the same ``_build_resolved_config``/``_resolve_hostname``
functions), not inside the services-layer suite. ``tests/services/test_manifests.py``
itself keeps the two DR-0025 Erratum E1 tests that exercise the SAME
``_render_templates`` mechanism through hand-built, inline-fixture ``ManifestProfile``
objects (no app/ import needed for those).

Pins the 2026-08-03 smoke's ``exampleco-web-2`` manifest-resolution crash (``'environment_
variables' is undefined``) as a permanent, sub-second offline regression check, plus
DR-0025's mandatory both-halves regression pin for ``exampleco-dev-stack-nodns`` (a real
``provider_host`` profile)."""

from __future__ import annotations

import base64
import dataclasses
import json
from pathlib import Path

import httpx
import pytest
import yaml

from seedpod.app.services.deployment_service import _build_resolved_config
from seedpod.app.services.profiles import load_deployment_profile
from seedpod.core.errors import PermanentError
from seedpod.services.ghcr import GhcrConfig, GhcrService
from seedpod.services.manifests import ManifestResolver
from tests.services.fake_ghcr import FakeGhcrBackend, FakeGhcrTransport

_REPO_CONFIG_DIR = Path(__file__).resolve().parents[2] / "config"

# asyncio_mode = "auto" (pyproject.toml) picks up every `async def test_*` here
# without an explicit marker.


# ============================================================================
# real shipped exampleco-web-2 profile — pins the 2026-08-03 smoke's manifest-
# resolution crash ("manifest resolution failed: 'environment_variables' is
# undefined") as a permanent, sub-second offline regression check.
# ============================================================================


async def test_exampleco_web_2_end_to_end_render_pins_2026_08_03_smoke_environment_variables_crash():
    """Offline repro + permanent regression pin for the third smoke's (2026-08-03)
    failure: every real deployment of ``exampleco-web-2`` came back rejected with
    ``manifest resolution failed: 'environment_variables' is undefined'``. Root
    cause (confirmed): ``_render_templates`` built ``template_vars`` with exactly
    three keys (``images``/``secrets``/``config``); ``config/manifest-templates/
    exampleco-misc/exampleco-web-2.yaml`` additionally references ``environment_variables``.

    ``ghcr_service=None`` reproduces the WHOLE ``exampleco-web-2`` profile offline with
    ZERO GHCR calls: ``exampleco-web-2`` IS the triggering repo (the shortcut path --
    ``ManifestResolver._resolve_service_images``), and ``tailscale`` carries an
    ``image_override``.

    Exercises the REAL, committed, shipped ``config/deployment-profiles/
    exampleco-web-2.yml`` + ``config/manifest-templates/exampleco-misc/*.yaml`` -- not a
    ``tmp_path`` fixture -- via the REAL loader (``load_deployment_profile``) and
    the REAL resolved-config builder (``_build_resolved_config``), so an edit to
    either shipped file that reintroduces this crash (or a sibling one) fails THIS
    test, not just a real DigitalOcean smoke."""
    profile, raw_profile = load_deployment_profile(_REPO_CONFIG_DIR, "exampleco-web-2")

    cluster_id = "3c8cf9ed-8229-45b1-a188-7cdcd726fe02"  # allocate_cluster_cidrs' own docstring example
    cluster_slug = "exampleco-web-2-feature-smoke-3c8cf9ed"
    resolved_config = _build_resolved_config(
        cluster_id,
        raw_profile.get("environment_type", "ephemeral"),
        raw_profile,
        config_overrides={},
        cluster_slug=cluster_slug,
        profile_name="exampleco-web-2",
    )
    # exampleco-web-2.yml declares NEITHER `hostname:` nor `dns:` -- v1's backward-
    # compat inference (no dns: block to infer "dns" from) must correctly yield
    # "none". DR-0025 Erratum E1: that means the key is PRESENT, valued None
    # (never omitted, never "") -- see tests/app/test_deployment_service_
    # resolved_config.py's own dedicated coverage of the None-vs-omitted split.
    assert resolved_config["cluster_hostname"] is None
    assert resolved_config["cluster_id"] == cluster_id
    assert resolved_config["environment"] == "ephemeral"

    resolver = ManifestResolver(ghcr_service=None)

    # SUCCEEDS where the 2026-08-03 smoke's real deployments came back
    # 'manifest resolution failed: 'environment_variables' is undefined'.
    result = await resolver.resolve(
        profile,
        triggering_repo="exampleco-web-2",
        triggering_branch="feature/smoke-test",
        triggering_image="ghcr.io/exampleco/exampleco-web-2:feature-smoke-test-abc1234",
        secrets={"tailscale_auth_key": "tskey-test-not-a-real-key"},  # pragma: allowlist secret
        config=resolved_config,
    )

    assert "exampleco-web-2.yaml" in result.template_files
    assert "tailscale.yaml" in result.template_files
    # ghcr-secret.yaml stays absent HERE specifically because this test's whole
    # premise is `ghcr_service=None` (zero GHCR calls, offline, sub-second --
    # this docstring's own opening paragraph): no token configured means
    # `ManifestResolver._add_ghcr_auth_if_needed` never populates
    # `secrets.ghcr_dockerconfig_json`, so `_INFRASTRUCTURE_TEMPLATES`'s own
    # condition for ghcr-secret.yaml never fires -- a normal, silent skip, not
    # the structural "stem is never a declared service name" skip this comment
    # used to describe (that mechanism is now CLOSED, Round 9's org-and-ghcr
    # component -- seedpod/services/manifests.py's module docstring). See
    # tests/services/test_manifests.py for the WITH-a-real-token coverage that
    # proves ghcr-secret.yaml DOES render, and decodes to a valid
    # dockerconfigjson, when the condition is actually met.
    assert "ghcr-secret.yaml" not in result.template_files

    docs = [d for d in yaml.safe_load_all(result.rendered_manifests) if d is not None]
    kinds = [d["kind"] for d in docs]
    # exampleco-web-2.yaml -> 3 docs (Deployment/Service/Ingress), tailscale.yaml -> 5
    # docs (ServiceAccount/Role/RoleBinding/Secret/DaemonSet) -- DIAGNOSIS, PROVEN.
    assert kinds == [
        "Deployment", "Service", "Ingress",
        "ServiceAccount", "Role", "RoleBinding", "Secret", "DaemonSet",
    ]

    deployment = next(d for d in docs if d["kind"] == "Deployment")
    # config.cluster_id / config.environment actually appear in the rendered
    # labels -- i.e. NOT empty.
    labels = deployment["metadata"]["labels"]
    assert labels["environment"] == "ephemeral"
    assert labels["cluster-id"] == cluster_id

    env = {e["name"]: e["value"] for e in deployment["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["ENVIRONMENT_NAME"] == "ephemeral"
    assert env["CLUSTER_ID"] == cluster_id
    assert env["APP_NAME"] == "exampleco-web-2"
    assert env["SERVICE_PORT"] == "8000"

    # Deliverable 2's silent-empty close: the tailscale-auth Secret ships the REAL
    # secret value, never an empty string (the DIAGNOSIS's own silent-empty case).
    #
    # Backlog #14: the field is `data` (base64) and NOT `stringData`. stringData is
    # write-only -- the API server converts it to `data` and never echoes it back -- so
    # kubectl re-sends it on every apply and reports the Secret `configured` forever,
    # which permanently poisons ApplyChangeSummary.all_unchanged and so disables
    # deploy.ensure_rollouts' restart rule for the whole wave. Decoding here rather
    # than comparing the base64 blob keeps this assertion about the SECRET VALUE (the
    # silent-empty close it was written for) rather than about the encoding.
    secret_doc = next(d for d in docs if d["kind"] == "Secret" and d["metadata"]["name"] == "tailscale-auth")
    assert "stringData" not in secret_doc, "stringData never round-trips through kubectl apply (#14)"
    decoded = base64.b64decode(secret_doc["data"]["TS_AUTHKEY"]).decode()
    assert decoded == "tskey-test-not-a-real-key"  # pragma: allowlist secret


async def test_exampleco_web_2_end_to_end_render_with_ghcr_token_includes_rendered_ghcr_secret():
    """Round 9's org-and-ghcr component, pinned against the SAME real shipped
    profile the crash-pin test above uses -- ``config/manifest-templates/
    exampleco-misc/ghcr-secret.yaml`` (READ, not guessed) exists on disk for this
    profile, so with a GHCR token configured it must now actually render,
    where it was unconditionally absent from ``template_files`` before this
    component (that test's own now-updated comment, and ``seedpod/services/
    manifests.py``'s module docstring, explain why).

    Needs ZERO real GHCR network calls to prove this, for the SAME reason the
    crash-pin test above needs none: ``exampleco-web-2`` IS the triggering repo
    (the shortcut path -- its OWN ``triggering_image`` already contains
    ``ghcr.io``, never queried), and ``tailscale`` carries an
    ``image_override``. A real ``GhcrService`` only needs to EXIST (a token
    configured) for ``_add_ghcr_auth_if_needed``'s condition to fire -- the
    ``FakeGhcrBackend`` below is asserted to receive zero calls, proving the
    pull-secret generation itself is also a pure, local operation on
    ``GhcrConfig``, not a registry round-trip."""
    profile, raw_profile = load_deployment_profile(_REPO_CONFIG_DIR, "exampleco-web-2")

    cluster_id = "3c8cf9ed-8229-45b1-a188-7cdcd726fe02"
    cluster_slug = "exampleco-web-2-feature-smoke-3c8cf9ed"
    resolved_config = _build_resolved_config(
        cluster_id,
        raw_profile.get("environment_type", "ephemeral"),
        raw_profile,
        config_overrides={},
        cluster_slug=cluster_slug,
        profile_name="exampleco-web-2",
    )

    backend = FakeGhcrBackend()  # deliberately empty -- must never be queried (see docstring)
    transport = httpx.AsyncClient(transport=FakeGhcrTransport(backend))
    ghcr_service = GhcrService(GhcrConfig(token="fake-token", organization="exampleco"), transport)
    resolver = ManifestResolver(ghcr_service=ghcr_service)

    result = await resolver.resolve(
        profile,
        triggering_repo="exampleco-web-2",
        triggering_branch="feature/smoke-test",
        triggering_image="ghcr.io/exampleco/exampleco-web-2:feature-smoke-test-abc1234",
        secrets={"tailscale_auth_key": "tskey-test-not-a-real-key"},  # pragma: allowlist secret
        config=resolved_config,
    )

    assert backend.call_count == 0

    assert "ghcr-secret.yaml" in result.template_files
    ghcr_secret_value = result.resolved_secrets["ghcr_dockerconfig_json"]
    decoded = json.loads(base64.b64decode(ghcr_secret_value).decode())
    auth_entry = decoded["auths"]["ghcr.io"]
    assert auth_entry["username"] == "exampleco"
    assert auth_entry["password"] == "fake-token"  # pragma: allowlist secret

    docs = [d for d in yaml.safe_load_all(result.rendered_manifests) if d is not None]
    ghcr_secret_doc = next(d for d in docs if d["kind"] == "Secret" and d["metadata"]["name"] == "ghcr-secret")
    assert ghcr_secret_doc["data"][".dockerconfigjson"] == ghcr_secret_value
    assert ghcr_secret_doc["metadata"]["labels"]["environment"] == "ephemeral"
    assert ghcr_secret_doc["metadata"]["labels"]["cluster-id"] == cluster_id


async def test_exampleco_web_2_missing_tailscale_secret_fails_loudly_not_silently():
    """The other half of the silent-empty decision, pinned directly:
    ``secrets={}`` (no ``tailscale_auth_key`` at all -- exactly what shipped
    before Round 9's secrets wiring landed in ``DeploymentService``) must now
    raise, not silently render ``TS_AUTHKEY: ""`` into a Secret that would deploy
    green and fail at Tailscale-auth time (DIAGNOSIS fact 3)."""
    profile, raw_profile = load_deployment_profile(_REPO_CONFIG_DIR, "exampleco-web-2")
    resolved_config = _build_resolved_config(
        "3c8cf9ed-8229-45b1-a188-7cdcd726fe02", "ephemeral", raw_profile,
        config_overrides={}, cluster_slug="exampleco-web-2-smoke-3c8cf9ed", profile_name="exampleco-web-2",
    )
    resolver = ManifestResolver(ghcr_service=None)

    with pytest.raises(PermanentError, match="tailscale_auth_key"):
        await resolver.resolve(
            profile,
            triggering_repo="exampleco-web-2",
            triggering_branch="feature/smoke-test",
            triggering_image="ghcr.io/exampleco/exampleco-web-2:feature-smoke-test-abc1234",
            secrets={},
            config=resolved_config,
        )


# ============================================================================
# DR-0025 regression pin (docs/decisions/DR-0025-hostname-resolution-ordering.md)
# — a `provider_host` profile with NO known host must fail LOUDLY (a value that
# cannot be resolved is ABSENT, never empty); supplied WITH a known host, no
# malformed 'https:///' URL may ever escape. Both halves use the REAL shipped
# `config/deployment-profiles/exampleco-dev-stack-nodns.yml` -- the profile DR-0025
# itself names as the concrete evidence (several `environment_variables:` values
# interpolate `{{ cluster_hostname }}` into URLs, e.g. API_BASE_URL,
# ADMIN_DASHBOARD_URL, REDIRECT_URI).
# ============================================================================

_NODNS_CLUSTER_ID = "3c8cf9ed-8229-45b1-a188-7cdcd726fe02"


async def test_provider_host_profile_with_no_known_host_raises_naming_cluster_hostname():
    """DR-0025 part 1's mandatory regression pin. Round 9's FIRST attempt at this
    component defaulted the context handed to environment-variable resolution to
    ``resolved_config.get("cluster_hostname", "")`` -- that makes the NAME defined
    (just empty), so ``StrictUndefined`` renders happily and every
    ``"https://{{ cluster_hostname }}/..."`` value in this profile's
    ``environment_variables:`` block (``BUYER_UI_PAGE_URL``/``SUPPLIER_UI_PAGE_URL``
    in ``shared``, ``KEYCLOAK_PUBLIC_URL`` et al under ``services.exampleco-api``) would
    silently ship as ``https:///...``. A test that only checked "resolve() raised
    *something*" would NOT catch this regression: the unfixed code raises too, but
    for an unrelated reason (a downstream template's missing secret, since
    ``sorted(profile.manifests_dir.glob("*.yaml"))`` happens to hit
    ``audit-postgres.yaml`` before any hostname-bearing template) -- so this
    asserts specifically that ``cluster_hostname`` is named, and that the message
    never itself carries the malformed-URL shape.

    ``image_overrides`` covers every one of this profile's declared services so
    ``_resolve_service_images`` succeeds trivially (zero GHCR calls, offline,
    sub-second) -- this test is about the HOSTNAME failure, which
    ``_render_templates`` hits unconditionally BEFORE it ever looks at a single
    template file, not about registry lookups."""
    profile, raw_profile = load_deployment_profile(_REPO_CONFIG_DIR, "exampleco-dev-stack-nodns")

    resolved_config = _build_resolved_config(
        _NODNS_CLUSTER_ID, "ephemeral", raw_profile, config_overrides={},
        cluster_slug="exampleco-dev-nodns-3c8cf9ed", profile_name="exampleco-dev-stack-nodns",
    )
    # provider_host strategy, no known host anywhere -- DR-0025 part 1's premise.
    assert "cluster_hostname" not in resolved_config

    resolver = ManifestResolver(ghcr_service=None)
    image_overrides = {name: f"test.registry/{name}:pinned" for name in profile.services}

    with pytest.raises(PermanentError) as excinfo:
        await resolver.resolve(
            profile,
            triggering_repo="__nothing-triggers-this-repo__",
            triggering_branch="feature/dr-0025",
            triggering_image="unused:unused",
            image_overrides=image_overrides,
            config=resolved_config,
            secrets={},
        )

    message = str(excinfo.value)
    assert "cluster_hostname" in message
    assert "https:///" not in message


async def test_provider_host_profile_with_known_host_renders_no_malformed_url():
    """DR-0025 part 1's other half: WITH a known host (simulating what Round 10's
    deploy-time re-resolution will eventually supply via ``config_overrides``),
    the SAME real ``environment_variables:`` block must interpolate the REAL host
    everywhere -- never a placeholder, never ``https:///``. A test that only
    asserts "it raised" (the test above) does not pin this half of the DR.

    Trims the REAL, loaded profile's ``services`` down to just ``exampleco-api`` --
    everything else about the profile, including its actual
    ``environment_variables`` (both the ``shared`` keys AND ``exampleco-api``'s own)
    and its real ``manifests_dir``, is untouched -- so the real
    ``config/manifest-templates/exampleco-stack/exampleco-api.yaml`` template (the one
    carrying ``API_BASE_URL``/``ADMIN_DASHBOARD_URL``/``REDIRECT_URI``,
    DR-0025's own named examples) fully renders without needing all the secrets
    the untrimmed multi-service profile would."""
    profile, raw_profile = load_deployment_profile(_REPO_CONFIG_DIR, "exampleco-dev-stack-nodns")
    exampleco_api_only = dataclasses.replace(profile, services={"exampleco-api": profile.services["exampleco-api"]})

    resolved_config = _build_resolved_config(
        _NODNS_CLUSTER_ID, "ephemeral", raw_profile, config_overrides={"provider_host": "203.0.113.42"},
        cluster_slug="exampleco-dev-nodns-3c8cf9ed", profile_name="exampleco-dev-stack-nodns",
    )
    assert resolved_config["cluster_hostname"] == "203.0.113.42"

    resolver = ManifestResolver(ghcr_service=None)
    exampleco_api_secrets = dict.fromkeys(
        ("database_password", "cache_password", "s3_access_key", "s3_secret_key", "jwt_secret", "mail_password"),
        "x",
    )

    result = await resolver.resolve(
        exampleco_api_only,
        triggering_repo="__nothing-triggers-this-repo__",
        triggering_branch="feature/dr-0025",
        triggering_image="unused:unused",
        image_overrides={"exampleco-api": "test.registry/exampleco-api:pinned"},
        config=resolved_config,
        secrets=exampleco_api_secrets,
    )

    # No malformed URL escaped anywhere in the actual K8s YAML that ships.
    assert "https:///" not in result.rendered_manifests

    docs = list(yaml.safe_load_all(result.rendered_manifests))
    deployment = next(d for d in docs if d["kind"] == "Deployment")
    env = {e["name"]: e["value"] for e in deployment["spec"]["template"]["spec"]["containers"][0]["env"]}
    # services.exampleco-api's own cluster_hostname-bearing keys:
    assert env["API_BASE_URL"] == "https://203.0.113.42/api"
    assert env["ADMIN_DASHBOARD_URL"] == "https://203.0.113.42/admin"
    assert env["REDIRECT_URI"] == "https://203.0.113.42/*"
    # shared's own cluster_hostname-bearing keys (apply to every service, exampleco-api
    # included):
    assert env["APP_PUBLIC_URL"] == "https://203.0.113.42"
    assert env["WEBHOOK_CALLBACK_URL"] == "https://203.0.113.42/webhooks"


# ---------------------------------------------------------------------------
# Backlog #17 -- `provider_host` + Ingress on an IP-host provider
#
# Against the REAL shipped `exampleco-stack/exampleco-api.yaml`, both halves, because the
# whole defect was that an IP is TRUTHY: an inline fixture mirroring the gate would
# have passed before the fix as easily as after it. `postgres.yaml`'s own Deployment/
# Service carry no Ingress at all; `test_every_shipped_ingress_host_is_dns_name_gated`
# below pins that every Ingress-bearing template shipped keeps the same guard.
# ---------------------------------------------------------------------------


async def _render_nodns_exampleco_api(host: str):
    """The `test_provider_host_profile_with_known_host_renders_no_malformed_url`
    setup, parameterized by host -- same trim-to-exampleco-api trick, same reasons."""
    profile, raw_profile = load_deployment_profile(_REPO_CONFIG_DIR, "exampleco-dev-stack-nodns")
    exampleco_api_only = dataclasses.replace(profile, services={"exampleco-api": profile.services["exampleco-api"]})
    resolved_config = _build_resolved_config(
        _NODNS_CLUSTER_ID, "ephemeral", raw_profile, config_overrides={"provider_host": host},
        cluster_slug="exampleco-dev-nodns-3c8cf9ed", profile_name="exampleco-dev-stack-nodns",
    )
    assert resolved_config["cluster_hostname"] == host
    secrets = dict.fromkeys(
        ("database_password", "cache_password", "s3_access_key", "s3_secret_key", "jwt_secret", "mail_password"),
        "x",
    )
    result = await ManifestResolver(ghcr_service=None).resolve(
        exampleco_api_only,
        triggering_repo="__nothing-triggers-this-repo__",
        triggering_branch="feature/backlog-17",
        triggering_image="unused:unused",
        image_overrides={"exampleco-api": "test.registry/exampleco-api:pinned"},
        config=resolved_config,
        secrets=secrets,
    )
    docs = list(yaml.safe_load_all(result.rendered_manifests))
    return result, next(d for d in docs if d["kind"] == "Ingress")


async def test_provider_host_ip_is_omitted_from_the_ingress_host():
    """Backlog #17: smoke 8 died here, after wave 1's ten other documents had applied.

        The Ingress "mailhog" is invalid: spec.rules[0].host:
          Invalid value: "203.0.113.40": must be a DNS name, not an IP address

    The template's `{% if cluster_hostname %}` guard was written for DR-0025's `None`
    case; an IP is truthy, so it sailed through. `spec.tls[].hosts` is gated the same
    way and for the same reason -- Kubernetes validates it identically, and a
    certificate for an IP is not a thing either.

    The Ingress becomes a catch-all, which is the correct shape for an IP-addressed
    cluster: you reach it at the IP, on Traefik's default self-signed certificate,
    which is exactly what `dns.enabled: false` + `ssl.enabled: true` asks for.
    """
    result, ingress = await _render_nodns_exampleco_api("203.0.113.42")

    assert "host" not in ingress["spec"]["rules"][0]
    assert "tls" not in ingress["spec"]
    # The rule still routes -- the host is dropped, not the whole rule.
    assert ingress["spec"]["rules"][0]["http"]["paths"]

    # The SAME variable still fills the env-var URLs, where an IP is perfectly valid.
    # This is the half a "resolve an IP hostname to None" fix would have broken, and
    # it is the `https:///auth` rendering DR-0025 exists to prevent.
    docs = list(yaml.safe_load_all(result.rendered_manifests))
    deployment = next(d for d in docs if d["kind"] == "Deployment")
    env = {e["name"]: e["value"] for e in deployment["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["API_BASE_URL"] == "https://203.0.113.42/api"
    assert "https:///" not in result.rendered_manifests


async def test_provider_host_dns_name_still_reaches_the_ingress_host():
    """#17's other half, and the one that makes the fix a narrowing rather than a
    removal: a `provider_host` that IS a DNS name -- `tart`/`kind`'s `minimax.local`,
    the reason eight smokes never saw this -- must render exactly as it always did,
    TLS block included."""
    _, ingress = await _render_nodns_exampleco_api("minimax.local")

    assert ingress["spec"]["rules"][0]["host"] == "minimax.local"
    assert ingress["spec"]["tls"] == [{"hosts": ["minimax.local"]}]


@pytest.mark.parametrize(
    "template", sorted((_REPO_CONFIG_DIR / "manifest-templates").rglob("*.yaml")), ids=lambda p: p.name
)
def test_every_shipped_ingress_host_is_dns_name_gated(template):
    """A sixth Ingress template added with a bare `{% if cluster_hostname %}host:`
    guard would reintroduce #17 silently, on a profile nothing routinely smokes. This
    is the same class of gate as `test_shipped_templates_avoid_fields_kubectl_can_
    never_report_unchanged` below: cheap, mechanical, and it fails on the ONE line
    that matters rather than after a droplet exists."""
    body = template.read_text()
    for line in body.splitlines():
        if "host: {{ cluster_hostname }}" in line or "- {{ cluster_hostname }}" in line:
            guard = line if "{% if" in line else body
            assert "cluster_hostname is dns_name" in guard, (
                f"{template.name}: an Ingress host/tls entry renders `cluster_hostname` without the "
                f"`is dns_name` guard (backlog #17) -- an IP would be emitted and kubectl would reject it"
            )


# ---------------------------------------------------------------------------
# Backlog #14 -- the two "never round-trips through kubectl apply" patterns
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "template", sorted((_REPO_CONFIG_DIR / "manifest-templates").rglob("*.yaml")), ids=lambda p: p.name
)
def test_shipped_templates_avoid_fields_kubectl_can_never_report_unchanged(template):
    """No shipped manifest may use a field the API server refuses to echo back.

    Both patterns below make `kubectl apply` report a resource `configured` on EVERY
    apply, forever, even though `kubectl diff` shows no difference and the object never
    actually changes (verified on a live cluster: `generation` stayed 1 across three
    applies). Harmless in isolation -- but `ApplyChangeSummary.all_unchanged` is the ONE
    signal `deploy.ensure_rollouts` uses to decide whether to force a rollout restart, so
    a single occurrence silently disables that rule for its entire wave. Smoke 4 hit
    exactly this: two consecutive redeploys of an untouched stack could never report
    all-unchanged, making the restart branch unreachable on real infrastructure.

    - `stringData:` is WRITE-ONLY. The server converts it to `data` and never returns it,
      so kubectl's three-way merge re-sends it every time. Use `data:` with the
      `| b64encode` filter (`seedpod/services/manifests.py`).
    - `value: ""` on an env var is the ZERO VALUE, which the server drops entirely -- the
      live object stores `{name: X}` with no `value` key. Omit the line; a valueless env
      var already means "". Semantically identical, verified against a live cluster.

    v1 shipped both in its own tailscale templates, so this is an inherited defect v2
    deliberately does not port (CLAUDE.md: don't pin v1 bugs).
    """
    body = template.read_text()
    offenders = [
        (n, line.strip())
        for n, line in enumerate(body.splitlines(), start=1)
        if line.strip().startswith("stringData:") or line.strip() == 'value: ""'
    ]
    assert not offenders, (
        f"{template.relative_to(_REPO_CONFIG_DIR)} uses a field kubectl can never report "
        f"'unchanged' (backlog #14): {offenders}"
    )
