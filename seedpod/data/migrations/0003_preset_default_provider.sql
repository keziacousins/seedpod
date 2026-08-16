-- DR-0046: a preset can pin its provider.
--
-- `deployment_presets` carried `default_branch` and `default_ttl_hours` but no
-- provider, while `exampleco-dev-stack-nodns` deliberately has no `provider:` key and
-- `deployment_service.py`'s `provider_override or raw_profile.get("provider",
-- self._default_provider)` falls back to "digitalocean". So the preset named
-- `exampleco-dev-tart` provisioned a billing DigitalOcean droplet unless the caller
-- remembered `--provider-override tart` -- the name was the only record of the
-- intent, and names do not execute. Observed 2026-08-16: droplet 100000000.
--
-- `PresetRow`'s own docstring already claimed "/api/presets is the only Tart
-- provider-override deploy path (Decision 6)", which the schema made impossible.
-- This is the column that makes that sentence true.
--
-- Named `default_provider` to match the existing `default_*` convention on this
-- table rather than inventing a third naming style; nullable, so a preset that
-- genuinely does not care keeps today's behaviour (profile, then global default).
--
-- NOT backfilled here. A migration that guessed which existing presets "meant"
-- tart from their names would be doing exactly what this DR says software must not
-- do -- infer intent from a string. `exampleco-dev-tart` is set explicitly as a
-- rollout step, by someone who knows.
ALTER TABLE deployment_presets ADD COLUMN default_provider TEXT;

PRAGMA user_version = 3;
