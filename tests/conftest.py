"""build_app conftest — docs/design/seam-d-foundation.md "Test construction", AS
AMENDED by DR-0015 (ratified 2026-07-17).

The four keyword seams on build_app (providers, clock, id_gen, http_transport) are the
entire test surface. No init_database repointing, no app.dependency_overrides, no
patch() anywhere. There is no production-DB default on any path: every database comes
from the AppConfig constructed here. http_transport (DR-0015) is the shared outbound-
HTTP seam for the httpx-based supporting services (GHCR/DNS) — tests that need to
exercise them inject an httpx.AsyncClient(transport=httpx.MockTransport(handler)); left
at its default (None) here, build_app makes zero outbound HTTP for the default
github_token=None/cloudflare_api_token=None fixtures.
"""

import shutil
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
from cryptography.fernet import Fernet

from seedpod.core.clock import FrozenClock
from tests.fakes import FakeProvider, sequential_ids

REPO_ROOT = Path(__file__).parent.parent

# FrozenClock requires an aware `at:` (seedpod/core/clock.py -- naive datetimes are
# banned core-wide, and there is no sensible "current time" default to fall back to).
# Matches tests/runtime/conftest.py's NOW convention.
_NOW = datetime(2026, 7, 15, 12, 0, 0, tzinfo=UTC)


@pytest.fixture(scope="session")
def test_config_dir(tmp_path_factory):
    """The real config/ tree with the test deployment rules overlaid.

    v1 pointed the RuleEngine at tests/fixtures/deployment-rules.yml via a global
    setter; v2 loads rules fail-fast from config_dir at build time, so the overlay
    happens on disk instead. The acceptance assertions (main -> staging, hotfix ->
    no_action, ...) depend on the fixture rules, not the production ones.

    Also overlays ``tests/fixtures/deployment-profiles/`` (+ matching empty
    ``manifest-templates/<profile>/`` dirs) with two test-only profiles neither
    tree ever shipped as a real file: ``infrastructure-only`` (the parity gate's
    ``test_deployment_preview_to_actual_deployment`` previews it; salvaged from
    v1's own test-fixture generator, that module's docstring) and
    ``ephemeral-stack`` (``DeploymentService``'s hardcoded ``default_profile``
    fallback -- see ``tests/fixtures/deployment-profiles/ephemeral-stack.yml``'s
    own comment for why the fixture rules' ``feature_branches`` config needs it
    to satisfy the parity gate's ``cluster_id is not None`` assertion). Neither
    is ever present in production ``config/``.
    """
    dst = tmp_path_factory.mktemp("config")
    shutil.copytree(REPO_ROOT / "config", dst, dirs_exist_ok=True)
    shutil.copy(
        REPO_ROOT / "tests" / "fixtures" / "deployment-rules.yml",
        dst / "deployment-rules.yml",
    )
    shutil.copytree(
        REPO_ROOT / "tests" / "fixtures" / "deployment-profiles",
        dst / "deployment-profiles",
        dirs_exist_ok=True,
    )
    shutil.copytree(
        REPO_ROOT / "tests" / "fixtures" / "manifest-templates",
        dst / "manifest-templates",
        dirs_exist_ok=True,
    )
    return dst


@pytest.fixture
async def make_app(tmp_path, test_config_dir):
    """App factory, for tests that need AppConfig overrides (e.g. github_token).

    Every app gets its own SQLite file and is torn down in reverse construction order.
    Accepts an optional ``http_transport`` kwarg (DR-0015) forwarded straight to
    build_app, for tests exercising the GHCR/DNS supporting services via
    httpx.MockTransport.
    """
    apps = []

    try:
        from seedpod.app.config import AppConfig
        from seedpod.app.factory import build_app
    except ModuleNotFoundError as exc:
        pytest.skip(f"seedpod.app composition root not built yet ({exc})")

    async def _make(*, http_transport=None, **overrides):
        config = AppConfig(
            database_url=f"sqlite:///{tmp_path}/t{len(apps)}.db",
            secret_key_dev=Fernet.generate_key().decode(),
            config_dir=test_config_dir,
            background_tasks=False,  # reconciler + orphan-resume off; the executor ALWAYS runs
            **overrides,
        )
        a = build_app(
            config,
            providers={"fake": FakeProvider()},
            clock=FrozenClock(_NOW),
            id_gen=sequential_ids(),
            http_transport=http_transport,
        )
        await a.start()
        apps.append(a)
        return a

    yield _make
    for a in reversed(apps):
        await a.stop()


@pytest.fixture
async def app(make_app):
    return await make_app()


def make_client(app) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=app.api)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


@pytest.fixture
async def client(app):
    async with make_client(app) as c:
        yield c


async def make_auth_headers(app) -> dict[str, str]:
    """Mint a full-permission key through the service layer.

    Pins the ApiKeyService surface the spine must provide:
    create_api_key(username=..., environment=..., permissions=[...]) -> (record, plaintext)
    — the shape of v1's api_key_manager.create_api_key. environment='all' is the
    sentinel kept verbatim in the api_keys DDL (Seam D).
    """
    _, plaintext = await app.services.api_keys.create_api_key(
        username="test-user", environment="all", permissions=["*"]
    )
    return {"Authorization": f"Bearer {plaintext}", "Content-Type": "application/json"}


@pytest.fixture
async def auth_headers(app):
    return await make_auth_headers(app)
