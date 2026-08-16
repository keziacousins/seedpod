"""``seedpod/app/factory.py`` -- ``build_app()`` is pure construction (docs/design/
seam-d-foundation.md Decision 8): no IO, no DB connection, no schema apply. Zero
Mock/patch (CLAUDE.md) -- real sqlite tmp db, ``FrozenClock``, ``FakeProvider``.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from cryptography.fernet import Fernet

from seedpod.app.config import AppConfig
from seedpod.app.factory import load_enabled_providers, load_workflow_definitions
from seedpod.core.clock import FrozenClock
from seedpod.core.errors import ErrorCode, PermanentError
from seedpod.runtime.subprocess_manager import SubprocessManager
from tests.fakes import FakeProvider, sequential_ids
from tests.services.fake_ghcr import FakeGhcrBackend, FakeGhcrTransport

REPO_ROOT = Path(__file__).resolve().parents[2]
_NOW = datetime(2026, 1, 1, tzinfo=UTC)


def _config(tmp_path: Path, test_config_dir: Path, **overrides) -> AppConfig:
    overrides.setdefault("background_tasks", False)
    return AppConfig(
        database_url=f"sqlite:///{tmp_path}/t.db",
        secret_key_dev=Fernet.generate_key().decode(),
        config_dir=test_config_dir,
        **overrides,
    )


def test_build_app_creates_no_db_file_before_start(tmp_path, test_config_dir):
    """Decision 8: build_app is pure construction; the schema is applied by
    App.start(), never by build_app() itself."""
    from seedpod.app.factory import build_app

    config = _config(tmp_path, test_config_dir)
    db_path = tmp_path / "t.db"
    assert not db_path.exists()

    app = build_app(config, providers={"fake": FakeProvider()}, clock=FrozenClock(_NOW), id_gen=sequential_ids())

    assert not db_path.exists(), "build_app() must not touch the database"
    assert app.config is config


async def test_start_creates_db_file_and_stop_is_idempotent(tmp_path, test_config_dir):
    from seedpod.app.factory import build_app

    config = _config(tmp_path, test_config_dir)
    db_path = tmp_path / "t.db"
    app = build_app(config, providers={"fake": FakeProvider()}, clock=FrozenClock(_NOW), id_gen=sequential_ids())

    await app.start()
    try:
        assert db_path.exists()
    finally:
        await app.stop()
    await app.stop()  # idempotent -- must not raise


def test_importing_seedpod_app_has_zero_side_effects(tmp_path):
    """CLAUDE.md / Decision 8: "Nothing is constructed at import time -- importing
    any v2 module has zero side effects." A subprocess import (fresh interpreter,
    never-before-imported module) must not create any file under a scratch cwd."""
    import subprocess
    import sys

    before = set(tmp_path.iterdir())
    result = subprocess.run(
        [sys.executable, "-c", "import seedpod.app.config, seedpod.app.app, seedpod.app.factory"],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    after = set(tmp_path.iterdir())
    assert after == before, f"import created files: {after - before}"


def test_load_enabled_providers_absence_is_disabled(tmp_path, test_config_dir):
    """Decision 8 step 5: a provider name not in `enabled_providers` (or whose
    own config/providers/*.yml says enabled: false) is simply absent from the
    returned mapping -- no ProviderDisabledError exists in v2."""
    config = _config(tmp_path, test_config_dir, enabled_providers=("kind",))
    providers = load_enabled_providers(config, SubprocessManager())
    assert set(providers) == {"kind"}


def test_load_enabled_providers_constructs_all_four_by_default(tmp_path, test_config_dir):
    config = _config(tmp_path, test_config_dir)
    providers = load_enabled_providers(config, SubprocessManager())
    assert set(providers) == {"digitalocean", "kind", "tart", "orbstack"}
    # DR-0005: tart's transport is wrapped for its detached `tart run` launch;
    # every other provider gets the plain tracked runner. Both are SubprocessRunner-
    # shaped, so the only externally-observable proof at this layer is that
    # construction succeeded with a real config object per provider.
    assert providers["digitalocean"].config.project_id == "00000000-0000-0000-0000-000000000000"
    assert providers["kind"].config.api_server_host == "minimax.local"


def test_load_enabled_providers_unknown_name_constructs_nothing(tmp_path, test_config_dir):
    config = _config(tmp_path, test_config_dir, enabled_providers=("not-a-real-provider",))
    providers = load_enabled_providers(config, SubprocessManager())
    assert providers == {}


def test_ssh_identities_reads_v1s_own_per_provider_values(tmp_path, test_config_dir):
    """DR-0023: each provider's SSH identity is read from that provider's OWN
    `config/providers/*.yml` (the same file v1 read), with `~` expanded here (the
    "config loader" DR-0023 point 4 names). That MECHANISM is what this pins.

    The literals are whatever those files currently say, and they are not frozen
    at v1's: digitalocean is still v1's `root`/`id_exampleco_testing`, but tart moved
    to `admin`/`id_tart_minimax` on 2026-08-15 (commit `11cf673`) because sharing
    the general-purpose `id_ed25519` meant an unrelated host-key rotation on
    2026-08-14 silently locked provisioning out of every VM it booted. Update
    these assertions when a provider's YAML legitimately changes -- they exist to
    catch the loader breaking, not to freeze operational choices the YAML owns."""
    from seedpod.app.factory import _ssh_identities

    config = _config(tmp_path, test_config_dir)
    identities = _ssh_identities(config)

    do = identities["digitalocean"]
    assert do.user == "root"
    assert do.private_key_path == str(Path.home() / ".ssh" / "id_exampleco_testing")

    tart = identities["tart"]
    assert tart.user == "admin"
    assert tart.private_key_path == str(Path.home() / ".ssh" / "id_tart_minimax")


def test_ssh_identities_has_no_ssh_plane_entry_for_kind_or_orbstack(tmp_path, test_config_dir):
    """kind/orbstack have no SSH plane (DR-0023 decision 1) -- absent from the
    mapping entirely, not present with `None` values, so `LoadSpec`'s own
    ``.get(provider, SshIdentity())`` fallback is what produces `(None, None)`
    for them (the same code path a genuinely unconfigured provider hits)."""
    from seedpod.app.factory import _ssh_identities

    config = _config(tmp_path, test_config_dir)
    identities = _ssh_identities(config)

    assert "kind" not in identities
    assert "orbstack" not in identities


def test_verify_ssh_identities_raises_when_an_enabled_providers_key_is_missing(tmp_path):
    """The 2026-08-12 tart run: tart.yml named ~/.ssh/id_ed25519, the file was not on
    the host, ssh silently fell back to another key, and provisioning worked by luck --
    while every ssh-k3s failure carried a "Identity file ... not accessible" warning
    that was not the cause of anything. Fail at startup instead."""
    from seedpod.app.app import verify_ssh_identities
    from seedpod.engine.steps.cluster import SshIdentity

    missing = tmp_path / "no-such-key"
    identities = {"tart": SshIdentity(user="admin", private_key_path=str(missing))}

    with pytest.raises(PermanentError) as exc_info:
        verify_ssh_identities(["tart"], identities)

    assert exc_info.value.code is ErrorCode.NOT_FOUND
    assert str(missing) in str(exc_info.value)


def test_verify_ssh_identities_ignores_a_provider_that_is_not_enabled(tmp_path):
    """A tart-only host legitimately carries no DigitalOcean key (minimax is exactly
    this). A provider nobody enabled must never stop the server booting."""
    from seedpod.app.app import verify_ssh_identities
    from seedpod.engine.steps.cluster import SshIdentity

    identities = {
        "tart": SshIdentity(user="admin", private_key_path=str(tmp_path / "present")),
        "digitalocean": SshIdentity(user="root", private_key_path=str(tmp_path / "absent")),
    }
    (tmp_path / "present").write_text("key")

    verify_ssh_identities(["tart"], identities)  # digitalocean absent -> not consulted


def test_verify_ssh_identities_accepts_an_unconfigured_path(tmp_path):
    """`None` means "no SSH plane / not configured" -- LoadSpec resolves it to
    SshIdentity() and k3s.py's _target raises its own clear message at the step that
    needed it. Not this check's business."""
    from seedpod.app.app import verify_ssh_identities
    from seedpod.engine.steps.cluster import SshIdentity

    verify_ssh_identities(["tart", "kind"], {"tart": SshIdentity(user="admin", private_key_path=None)})


def test_load_workflow_definitions_parses_all_shipped_files():
    """The 8 files under config/workflows/ (tests/engine/test_shipped_workflows.py's
    own inventory) all parse clean against the frozen grammar -- structural only
    (V7/V9/V10), no verb registry needed (see factory.py's module docstring)."""
    definitions = load_workflow_definitions(REPO_ROOT / "config" / "workflows")
    assert set(definitions) == {
        "deploy-waves",
        "deploy-rollback",
        "destroy-cloud",
        "destroy-shared",
        "provision-digitalocean",
        "provision-kind",
        "provision-orbstack",
        "provision-tart",
    }
    for wf in definitions.values():
        assert wf.version >= 1


def test_config_dir_overlay_has_real_provider_yaml(test_config_dir):
    """Sanity: the session-scoped overlay fixture copies the real config/ tree
    (providers/, workflows/) verbatim, only replacing deployment-rules.yml."""
    assert (test_config_dir / "providers" / "kind.yml").exists()
    assert (test_config_dir / "workflows" / "provision-kind.yml").exists()


def test_deployment_rules_overlay_is_the_test_fixture(tmp_path, test_config_dir):
    """The fixture rules (main -> staging, hotfix -> no_action, ...), not the
    production ones -- test_config_dir's own docstring."""
    fixture_text = (REPO_ROOT / "tests" / "fixtures" / "deployment-rules.yml").read_text()
    assert (test_config_dir / "deployment-rules.yml").read_text() == fixture_text


async def test_build_app_wires_the_acyclic_graph_with_no_extra_setters(tmp_path, test_config_dir):
    """Decision 8: "the constructor graph is acyclic -- no post-hoc setters, no
    bind_* calls" beyond dispatcher.attach_executor/attach_timers (coherence-review
    Conflict 15's amendment). Proven behaviorally: apply() reaches the executor's
    poke() and the App boots end-to-end with a real (if verb-catalog-empty)
    WorkflowEngine wired all the way through."""
    from seedpod.app.factory import build_app

    config = _config(tmp_path, test_config_dir)
    app = build_app(config, providers={"fake": FakeProvider()}, clock=FrozenClock(_NOW), id_gen=sequential_ids())
    try:
        await app.start()
        # Every shipped workflow definition made it all the way through the DAG
        # into the constructed WorkflowEngine.
        assert set(app.engine.definitions) == set(
            load_workflow_definitions(test_config_dir / "workflows")
        )
        assert app.executor.running
        assert app.timers.running
    finally:
        await app.stop()


def test_build_app_makes_no_ghcr_or_dns_without_credentials(tmp_path, test_config_dir):
    """DR-0015: default fixtures (github_token=None, cloudflare_api_token=None)
    wire neither supporting service; ManifestResolver still constructs, degraded
    (ghcr_service=None) -- the no-token acceptance test's "limited manifest
    resolution", never a missing collaborator."""
    from seedpod.app.factory import build_app

    config = _config(tmp_path, test_config_dir)
    app = build_app(config, providers={"fake": FakeProvider()}, clock=FrozenClock(_NOW), id_gen=sequential_ids())

    assert app.ghcr is None
    assert app.dns is None
    assert app.manifest_resolver.ghcr_service is None


def test_build_app_wires_ghcr_when_github_token_set(tmp_path, test_config_dir):
    """DR-0015: github_token present -> a real GhcrService, routed through
    whatever transport was injected (or the default-constructed one) -- and
    ManifestResolver picks it up."""
    from seedpod.app.factory import build_app

    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("no request should be made during construction")

    fake_transport = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    config = _config(tmp_path, test_config_dir, github_token="ghp_test", github_organization="exampleco")
    app = build_app(
        config,
        providers={"fake": FakeProvider()},
        clock=FrozenClock(_NOW),
        id_gen=sequential_ids(),
        http_transport=fake_transport,
    )

    assert app.ghcr is not None
    assert app.ghcr.config.token == "ghp_test"
    assert app.ghcr.config.organization == "exampleco"
    assert app.ghcr.transport is fake_transport
    assert app.manifest_resolver.ghcr_service is app.ghcr


# ---------------------------------------------------------------------------
# PARITY-BACKLOG #0b: config/org.yml -- _resolve_github_organization
# ---------------------------------------------------------------------------


def test_resolve_github_organization_env_var_wins_over_org_yml(tmp_path, test_config_dir):
    """Precedence: an explicit ``github_organization`` (what
    ``AppConfig.from_env()``'s ``GITHUB_ORGANIZATION`` read, or a direct test
    override, both look like from this function's own point of view) wins
    over ``config/org.yml`` -- even though the real shipped ``org.yml``
    (copied into ``test_config_dir`` verbatim) also carries a value."""
    from seedpod.app.factory import _resolve_github_organization

    config = _config(tmp_path, test_config_dir, github_organization="override-org")
    assert _resolve_github_organization(config) == "override-org"


def test_resolve_github_organization_falls_back_to_org_yml_when_env_var_unset(tmp_path, test_config_dir):
    """No override -> reads the real shipped ``config/org.yml``'s own
    ``organization.github_organization`` (``exampleco`` --
    ``test_config_dir`` copies the real ``config/`` tree verbatim, per its own
    fixture docstring)."""
    from seedpod.app.factory import _resolve_github_organization

    config = _config(tmp_path, test_config_dir)
    assert _resolve_github_organization(config) == "exampleco"


def test_resolve_github_organization_treats_blank_env_var_as_unset(tmp_path, test_config_dir):
    """An operator who exports ``GITHUB_ORGANIZATION=""`` almost certainly
    meant "unset", not "the empty organization" -- matches
    ``AppConfig._require``'s own ``if not value`` treatment of every other
    required env var (this function's own docstring)."""
    from seedpod.app.factory import _resolve_github_organization

    config = _config(tmp_path, test_config_dir, github_organization="")
    assert _resolve_github_organization(config) == "exampleco"


def test_resolve_github_organization_raises_when_neither_env_var_nor_file_has_it(tmp_path):
    """Both halves empty: no override, and no ``config/org.yml`` at all in
    ``config_dir`` (a bare ``tmp_path``, not ``test_config_dir``) -- fails
    loudly naming the two places an operator can fix it, never silently
    returning ``""`` (the double-slash bug this closes)."""
    from seedpod.app.factory import MissingGithubOrganization, _resolve_github_organization

    config = AppConfig(
        database_url=f"sqlite:///{tmp_path}/t.db",
        secret_key_dev=Fernet.generate_key().decode(),
        config_dir=tmp_path,
    )
    with pytest.raises(MissingGithubOrganization):
        _resolve_github_organization(config)


def test_build_app_raises_when_github_token_set_and_org_unresolvable(tmp_path, test_config_dir):
    """The loud-failure half at its ACTUAL chosen point: composition-root
    (``build_app()``) construction time, not first use --
    ``_resolve_github_organization``'s own docstring has the full "why
    startup, why gated inside `if config.github_token:`" reasoning. A
    config_dir with every OTHER real file but no ``org.yml`` (built by
    copying ``test_config_dir`` and deleting just that one file, never
    mutating the session-scoped fixture itself) plus ``github_token`` set and
    no ``github_organization`` override must raise, before a single HTTP
    request is ever served."""
    import shutil

    from seedpod.app.factory import MissingGithubOrganization, build_app

    no_org_config_dir = tmp_path / "config-no-org"
    shutil.copytree(test_config_dir, no_org_config_dir)
    (no_org_config_dir / "org.yml").unlink()

    config = _config(tmp_path, no_org_config_dir, github_token="ghp_test")

    with pytest.raises(MissingGithubOrganization):
        build_app(config, providers={"fake": FakeProvider()}, clock=FrozenClock(_NOW), id_gen=sequential_ids())


def test_build_app_no_github_token_never_checks_organization_even_without_org_yml(tmp_path, test_config_dir):
    """The "earliest point that does not break the no-token degradation path"
    claim, proven behaviorally, not just asserted in a docstring: with NO
    ``github_token``, ``_resolve_github_organization`` is never even called
    (module comment: "no token means ... an org is never even resolved, let
    alone validated"), so a ``config_dir`` missing ``org.yml`` entirely still
    builds fine and still degrades exactly as
    ``test_build_app_makes_no_ghcr_or_dns_without_credentials`` (above)
    already pins for the real shipped config tree."""
    import shutil

    from seedpod.app.factory import build_app

    no_org_config_dir = tmp_path / "config-no-org"
    shutil.copytree(test_config_dir, no_org_config_dir)
    (no_org_config_dir / "org.yml").unlink()

    config = _config(tmp_path, no_org_config_dir)  # github_token unset -- the default
    app = build_app(config, providers={"fake": FakeProvider()}, clock=FrozenClock(_NOW), id_gen=sequential_ids())

    assert app.ghcr is None
    assert app.manifest_resolver.ghcr_service is None


async def test_build_app_wires_ghcr_organization_from_org_yml_no_double_slash_for_exampleco_web_2(
    tmp_path, test_config_dir
):
    """DELIVERABLE 1, proven at its actual downstream consumer: the exact
    symptom PARITY-BACKLOG #0b names is a double-slash image URL
    (``ghcr.io//<repo>:<tag>``) from ``config.github_organization or ""``.
    With NO ``github_organization`` override at all (the common real-world
    case -- CI sets ``GITHUB_TOKEN`` and relies on ``config/org.yml`` for the
    org), a GHCR image-URL lookup for the real shipped ``exampleco-web-2``
    profile's own repository name must come out
    ``ghcr.io/exampleco/exampleco-web-2:...``, never
    ``ghcr.io//exampleco-web-2:...``."""
    from seedpod.app.factory import build_app

    backend = FakeGhcrBackend()
    backend.add_version("exampleco-web-2", digest="sha256:aaa", tags=["main-abc123"])
    fake_transport = httpx.AsyncClient(transport=FakeGhcrTransport(backend))
    config = _config(tmp_path, test_config_dir, github_token="ghp_test")  # no github_organization override
    app = build_app(
        config,
        providers={"fake": FakeProvider()},
        clock=FrozenClock(_NOW),
        id_gen=sequential_ids(),
        http_transport=fake_transport,
    )

    assert app.ghcr.config.organization == "exampleco"
    image_url = await app.ghcr.find_image("exampleco-web-2", "main")
    assert image_url == "ghcr.io/exampleco/exampleco-web-2:main-abc123"
    assert "ghcr.io//" not in image_url


def test_build_app_wires_dns_when_cloudflare_token_set(tmp_path, test_config_dir):
    """DR-0015: cloudflare_api_token present -> a real DnsService sharing the same
    transport as GHCR."""
    from seedpod.app.factory import build_app

    def handler(request: httpx.Request) -> httpx.Response:
        raise AssertionError("no request should be made during construction")

    fake_transport = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    config = _config(tmp_path, test_config_dir, cloudflare_api_token="cf-token")
    app = build_app(
        config,
        providers={"fake": FakeProvider()},
        clock=FrozenClock(_NOW),
        id_gen=sequential_ids(),
        http_transport=fake_transport,
    )

    assert app.dns is not None
    assert app.dns.config.api_token == "cf-token"
    assert app.dns.transport is fake_transport


def test_build_app_constructs_its_own_transport_by_default(tmp_path, test_config_dir):
    """DR-0015: http_transport=None (the default) -> build_app constructs one real
    shared httpx.AsyncClient and owns its lifecycle."""
    from seedpod.app.factory import build_app

    config = _config(tmp_path, test_config_dir)
    app = build_app(config, providers={"fake": FakeProvider()}, clock=FrozenClock(_NOW), id_gen=sequential_ids())

    assert isinstance(app.http_transport, httpx.AsyncClient)
    assert app.owns_http_transport is True


def test_build_app_does_not_own_an_injected_transport(tmp_path, test_config_dir):
    from seedpod.app.factory import build_app

    fake_transport = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200)))
    config = _config(tmp_path, test_config_dir)
    app = build_app(
        config,
        providers={"fake": FakeProvider()},
        clock=FrozenClock(_NOW),
        id_gen=sequential_ids(),
        http_transport=fake_transport,
    )

    assert app.http_transport is fake_transport
    assert app.owns_http_transport is False


async def test_stop_closes_owned_transport_but_leaves_injected_one_open(tmp_path, test_config_dir):
    from seedpod.app.factory import build_app

    config_owned = _config(tmp_path, test_config_dir)
    owned_app = build_app(
        config_owned, providers={"fake": FakeProvider()}, clock=FrozenClock(_NOW), id_gen=sequential_ids()
    )
    await owned_app.start()
    await owned_app.stop()
    assert owned_app.http_transport.is_closed

    fake_transport = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200)))
    config_injected = AppConfig(
        database_url=f"sqlite:///{tmp_path}/t2.db",
        secret_key_dev=Fernet.generate_key().decode(),
        config_dir=test_config_dir,
        background_tasks=False,
    )
    injected_app = build_app(
        config_injected,
        providers={"fake": FakeProvider()},
        clock=FrozenClock(_NOW),
        id_gen=sequential_ids(),
        http_transport=fake_transport,
    )
    await injected_app.start()
    await injected_app.stop()
    assert not fake_transport.is_closed
    await fake_transport.aclose()


def test_config_from_env_requires_database_url_and_secret_key(monkeypatch):
    from seedpod.app.config import MissingEnvironmentVariable

    monkeypatch.delenv("SEEDPOD_DATABASE_URL", raising=False)
    monkeypatch.delenv("SEEDPOD_SECRET_KEY_DEV", raising=False)
    try:
        AppConfig.from_env()
    except MissingEnvironmentVariable:
        pass
    else:
        raise AssertionError("from_env() must fail loudly when required vars are unset")


def test_config_from_env_reads_required_and_optional_vars(monkeypatch):
    monkeypatch.setenv("SEEDPOD_DATABASE_URL", "sqlite:///./x.db")
    monkeypatch.setenv("SEEDPOD_SECRET_KEY_DEV", "dev-key")
    monkeypatch.setenv("DIGITALOCEAN_TOKEN", "do-token")
    monkeypatch.setenv("SEEDPOD_ENABLED_PROVIDERS", "kind, tart")
    config = AppConfig.from_env()
    assert config.database_url == "sqlite:///./x.db"
    assert config.secret_key_dev == "dev-key"
    assert config.digitalocean_token == "do-token"
    assert config.enabled_providers == ("kind", "tart")
    assert config.background_tasks is True  # default, unset
