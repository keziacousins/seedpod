"""tests/app/test_preset_provider.py — DR-0046: a preset can pin its provider, and
the deploy response says which one was chosen.

What this exists to prevent (observed 2026-08-16): `deployment_presets` had
`default_branch` and `default_ttl_hours` but no provider column; `exampleco-dev-stack-nodns`
deliberately has no `provider:` key; and `DeploymentService`'s
`provider_override or raw_profile.get("provider", self._default_provider)` falls back to
`"digitalocean"`. So deploying the preset named `exampleco-dev-tart` produced a BILLING
DigitalOcean droplet (100000000) where a free local tart VM was intended. The name was
the only record of the intent, and names do not execute.

The failure was silent and biased toward cost: the safe-looking action — reuse the
preset someone already configured — was the one that bills. Hence both halves here: the
column that carries the intent, and the response field that tells you what you got.
"""

from __future__ import annotations

import pytest

from seedpod.app.services.deployment_service import DeploymentService
from seedpod.app.services.preset_service import PresetService
from seedpod.data.repositories import PresetRepository

# Mirrors production rather than the neutral "fake" other suites use: the failure this
# pins is that the global fallback is a BILLING provider. In practice every fixture
# profile pins `provider: "fake"`, so this rung is never reached here -- which is
# itself the useful shape, because it means the profile pin is what these tests have
# to beat, not a harmless default.
_GLOBAL_DEFAULT = "digitalocean"


@pytest.fixture
def preset_service(
    dispatcher, repos, uow, rules, crypto, clock, manifest_resolver, id_gen, test_config_dir,
    deployment_audits_repo, secrets_repo,
):
    deployments = DeploymentService(
        dispatcher, repos, uow, rules=rules, crypto=crypto, clock=clock,
        manifest_resolver=manifest_resolver, dns=None, id_gen=id_gen, config_dir=test_config_dir,
        deployment_audits=deployment_audits_repo, secrets=secrets_repo,
        default_provider=_GLOBAL_DEFAULT,
    )
    return PresetService(PresetRepository(), deployments, uow, clock, id_gen, test_config_dir)


async def _make_preset(service, *, default_provider=None, name="p1"):
    return await service.create(
        name=name,
        description=None,
        profile_name="infrastructure-only",
        service_overrides=None,
        default_branch="main",
        default_ttl_hours=1,
        default_provider=default_provider,
        naming_strategy=None,
        created_by="api:test",
    )


async def test_a_preset_pins_its_provider_and_beats_the_profile(preset_service, repos, uow):
    """The whole point, AND decision 2a's contentious ordering in one assertion.

    No `--provider-override` at the call site, and the profile (`infrastructure-only`)
    pins `provider: "fake"` -- so this proves both that the preset's value is honoured
    at all, and that a preset BEATS a profile. That ordering was argued rather than
    assumed: a preset's provider is operator intent, the same kind of thing as the
    call-time flag; the counter-argument is that a profile's pin might be a
    correctness constraint. Resolved by making the choice visible (see the response
    test below) rather than by refusing on disagreement."""
    preset = await _make_preset(preset_service, default_provider="tart")
    result = await preset_service.deploy(preset.id, actor="api:test")

    async with uow() as tx:
        cluster = repos.clusters.get(tx, result.cluster_id)
    assert cluster.provider == "tart"  # not "fake", which the profile asked for


async def test_a_call_time_override_still_beats_the_preset(preset_service, repos, uow):
    """DR-0046 decision 2a precedence, rung 1 over rung 2. Nothing that worked before
    changes: the explicit flag is still the most specific thing anyone can say."""
    preset = await _make_preset(preset_service, default_provider="tart")
    result = await preset_service.deploy(preset.id, provider_override="digitalocean", actor="api:test")

    async with uow() as tx:
        cluster = repos.clusters.get(tx, result.cluster_id)
    assert cluster.provider == "digitalocean"


async def test_a_preset_with_no_provider_falls_through_to_the_profile(preset_service, repos, uow):
    """Rung 3. A preset that genuinely does not care must not acquire an opinion just
    because the column now exists -- it falls through to the profile's own pin, which
    for `infrastructure-only` is `fake`.

    Rung 4 (the global default, when NEITHER names one) is the pre-existing
    `provider_override or raw_profile.get("provider", self._default_provider)` and is
    covered by `tests/app/test_services_deployment.py`; every fixture profile here
    pins a provider, so it is deliberately not re-asserted through this path rather
    than contrived into reach."""
    preset = await _make_preset(preset_service, default_provider=None)
    result = await preset_service.deploy(preset.id, actor="api:test")

    async with uow() as tx:
        cluster = repos.clusters.get(tx, result.cluster_id)
    assert cluster.provider == "fake"


async def test_the_deploy_response_names_the_provider_it_chose(preset_service):
    """DR-0046 decision 4, and arguably the more valuable half -- it needs no column
    and no migration.

    Whether a call just created a billing droplet or a free local VM is the most
    consequential thing about it, and until now you learned it by querying the cluster
    afterwards, or from the invoice. This is also what makes decision 2a's
    "preset beats profile" safe: the override is visible rather than silent."""
    preset = await _make_preset(preset_service, default_provider="tart")
    result = await preset_service.deploy(preset.id, actor="api:test")
    assert result.provider == "tart"

    other = await _make_preset(preset_service, default_provider=None, name="p2")
    assert (await preset_service.deploy(other.id, actor="api:test")).provider == "fake"


async def test_the_provider_survives_a_round_trip_through_the_row(preset_service):
    """`default_provider` is durable, not a call-time argument that merely looks stored
    -- which was exactly the old failure: `PresetService.deploy` accepted
    `provider_override` and nothing persisted it."""
    preset = await _make_preset(preset_service, default_provider="tart")
    assert (await preset_service.get(preset.id)).default_provider == "tart"

    updated = await preset_service.update(preset.id, default_provider="orbstack")
    assert updated.default_provider == "orbstack"
    assert (await preset_service.get(preset.id)).default_provider == "orbstack"
