"""tests/app/test_deployment_service_resolved_config.py — unit tests for
``seedpod.app.services.deployment_service``'s ``_build_resolved_config``/
``_resolve_hostname`` (Round 9, the resolved-config component -- this round's own
brief calls it "the SPINE and the component whose mistakes propagate furthest").

Complements ``tests/services/test_manifests.py``'s end-to-end ``exampleco-web-2`` render
test with direct, exhaustive coverage of the hostname-strategy dispatch (v1's
``_resolve_hostname``, salvaged reference-code/seedpod/seedpod/orchestrator/
manifest_resolver.py:694-767: four strategies + backward-compat inference) and every
key ``_build_resolved_config`` (reference-code .../manifest_resolver.py:769-836)
produces. Both functions are PURE (dicts in, dict/None out, no IO) -- plain dict
fixtures, no Mock/patch, no database."""

from __future__ import annotations

from pathlib import Path

import pytest

from seedpod.app.services.deployment_service import (
    _build_resolved_config,
    _hostname_deferred,
    _provider_config_from,
    _resolve_hostname,
    rehydrate_cluster_hostname,
)
from seedpod.core.acme import AcmeConfig
from seedpod.core.cluster_spec import allocate_cluster_cidrs
from seedpod.core.dns_record import DnsIntent
from seedpod.core.errors import PermanentError

_CLUSTER_ID = "3c8cf9ed-8229-45b1-a188-7cdcd726fe02"  # allocate_cluster_cidrs' own docstring example


# ---------------------------------------------------------------------------
# _resolve_hostname -- v1's four-strategy dispatch + backward-compat inference
# ---------------------------------------------------------------------------


def test_hostname_strategy_none_explicit_yields_none():
    raw_profile = {"hostname": {"strategy": "none"}}
    assert _resolve_hostname(raw_profile, {}, "some-slug", None) is None


def test_hostname_no_hostname_and_no_dns_block_infers_none():
    """config/deployment-profiles/exampleco-web-2.yml's own shape -- pinned again here,
    directly, in addition to tests/services/test_manifests.py's end-to-end render."""
    assert _resolve_hostname({}, {}, "some-slug", None) is None


def test_hostname_no_hostname_block_but_dns_enabled_infers_dns_strategy():
    raw_profile = {"dns": {"enabled": True, "zone": "example.com", "subdomain_pattern": "{cluster_slug}"}}
    assert _resolve_hostname(raw_profile, {}, "my-slug", None) == "my-slug.example.com"


def test_hostname_no_hostname_block_dns_present_but_disabled_infers_none():
    raw_profile = {"dns": {"enabled": False, "zone": "example.com"}}
    assert _resolve_hostname(raw_profile, {}, "my-slug", None) is None


def test_hostname_strategy_dns_explicit_uses_subdomain_pattern():
    raw_profile = {
        "hostname": {"strategy": "dns"},
        "dns": {"zone": "cluster.example.com", "subdomain_pattern": "{cluster_slug}.preview"},
    }
    assert _resolve_hostname(raw_profile, {}, "feature-x", None) == "feature-x.preview.cluster.example.com"


def test_hostname_strategy_dns_defaults_subdomain_pattern_to_bare_slug():
    raw_profile = {"hostname": {"strategy": "dns"}, "dns": {"zone": "example.com"}}
    assert _resolve_hostname(raw_profile, {}, "my-slug", None) == "my-slug.example.com"


def test_hostname_strategy_dns_missing_cluster_slug_yields_none():
    raw_profile = {"hostname": {"strategy": "dns"}, "dns": {"zone": "example.com"}}
    assert _resolve_hostname(raw_profile, {}, None, None) is None


def test_hostname_strategy_dns_missing_dns_block_yields_none():
    raw_profile = {"hostname": {"strategy": "dns"}}
    assert _resolve_hostname(raw_profile, {}, "my-slug", None) is None


def test_hostname_strategy_dns_reads_dns_config_override_when_profile_has_no_dns_block():
    raw_profile = {"hostname": {"strategy": "dns"}}
    overrides = {"dns_config": {"zone": "override.example.com", "subdomain_pattern": "{cluster_slug}"}}
    assert _resolve_hostname(raw_profile, overrides, "my-slug", None) == "my-slug.override.example.com"


def test_hostname_strategy_provider_host_uses_provider_host_argument():
    raw_profile = {"hostname": {"strategy": "provider_host"}}
    assert _resolve_hostname(raw_profile, {}, None, "203.0.113.5") == "203.0.113.5"


def test_hostname_strategy_provider_host_falls_back_to_override():
    raw_profile = {"hostname": {"strategy": "provider_host"}}
    assert _resolve_hostname(raw_profile, {"provider_host": "203.0.113.9"}, None, None) == "203.0.113.9"


def test_hostname_strategy_provider_host_missing_yields_none():
    raw_profile = {"hostname": {"strategy": "provider_host"}}
    assert _resolve_hostname(raw_profile, {}, None, None) is None


def test_hostname_strategy_custom_formats_pattern():
    raw_profile = {"hostname": {"strategy": "custom", "custom_pattern": "{cluster_slug}.{provider_host}.nip.io"}}
    assert _resolve_hostname(raw_profile, {}, "my-slug", "1.2.3.4") == "my-slug.1.2.3.4.nip.io"


def test_hostname_strategy_custom_missing_pattern_yields_none():
    raw_profile = {"hostname": {"strategy": "custom"}}
    assert _resolve_hostname(raw_profile, {}, "my-slug", "1.2.3.4") is None


def test_hostname_strategy_custom_referencing_provider_host_with_none_known_yields_none():
    """DR-0025's own rule (docs/decisions/DR-0025-hostname-resolution-ordering.md),
    generalized to the one branch it originally missed: a `custom` pattern that
    references `{provider_host}` with no host known anywhere must yield None --
    never a plausible-looking hostname with an empty segment (e.g.
    'my-slug..nip.io', proven by direct execution before this fix)."""
    raw_profile = {"hostname": {"strategy": "custom", "custom_pattern": "{cluster_slug}.{provider_host}.nip.io"}}
    assert _resolve_hostname(raw_profile, {}, "my-slug", None) is None


def test_hostname_strategy_custom_referencing_provider_host_falls_back_to_override():
    raw_profile = {"hostname": {"strategy": "custom", "custom_pattern": "{cluster_slug}.{provider_host}.nip.io"}}
    result = _resolve_hostname(raw_profile, {"provider_host": "1.2.3.4"}, "my-slug", None)
    assert result == "my-slug.1.2.3.4.nip.io"


def test_hostname_strategy_custom_not_referencing_provider_host_ignores_missing_host():
    """The None-guard is scoped to patterns that actually need a host -- a custom
    pattern that never mentions {provider_host} must still resolve even when none
    is known."""
    raw_profile = {"hostname": {"strategy": "custom", "custom_pattern": "{cluster_slug}.example.com"}}
    assert _resolve_hostname(raw_profile, {}, "my-slug", None) == "my-slug.example.com"


def test_hostname_unknown_strategy_raises_permanent_error_not_silently_none():
    """Genuine correctness fix, not a v1 bug pin: v1's ``HostnameConfig.strategy``
    pydantic ``field_validator`` makes this branch UNREACHABLE in v1 (rejected at
    profile-LOAD time); ``raw_profile`` here is an unvalidated dict, so the same
    typo CAN reach this function in v2 -- it must fail loudly, not silently drop
    the hostname (and every ``{% if cluster_hostname %}`` riding on it)."""
    raw_profile = {"hostname": {"strategy": "bogus"}}
    with pytest.raises(PermanentError, match="bogus"):
        _resolve_hostname(raw_profile, {}, "my-slug", None)


# ---------------------------------------------------------------------------
# _build_resolved_config
# ---------------------------------------------------------------------------


def test_five_template_keys_always_present():
    config = _build_resolved_config(_CLUSTER_ID, "ephemeral", {}, {}, "my-slug", "test-profile")
    assert config["cluster_id"] == _CLUSTER_ID
    assert config["environment"] == "ephemeral"
    assert config["cluster_slug"] == "my-slug"
    pod_cidr, service_cidr = allocate_cluster_cidrs(_CLUSTER_ID)
    assert config["pod_cidr"] == pod_cidr
    assert config["service_cidr"] == service_cidr


def test_cidrs_come_from_the_committed_allocate_cluster_cidrs_not_reimplemented():
    """Tailscale-critical (CLAUDE.md): pinned against the SAME committed function's
    output for a DIFFERENT cluster_id, so a reimplementation drifting from the real
    hash would be caught here, not just by coincidence on the one id every other
    test in this file shares."""
    other_id = "ffffffff-8229-45b1-a188-7cdcd726fe02"
    config = _build_resolved_config(other_id, "ephemeral", {}, {}, "slug", "p")
    assert (config["pod_cidr"], config["service_cidr"]) == allocate_cluster_cidrs(other_id)


def test_cluster_hostname_deliberately_none_is_explicit_none_not_omitted():
    """DR-0025 Erratum E1: a profile that deliberately has no hostname (strategy
    "none", or -- as here -- no strategy resolvable to one at all, v1's own
    backward-compat inference from an absent ``hostname:``/``dns:`` block --
    ``config/deployment-profiles/exampleco-web-2.yml``'s own shape) means the KEY IS
    PRESENT, valued ``None`` -- never omitted. A real, shipped ``{% if
    cluster_hostname %}`` feature gate (config/manifest-templates/exampleco-stack/
    *.yaml) needs exactly this to evaluate False cleanly under StrictUndefined
    instead of raising (an OMITTED name would crash every ``hostname.strategy:
    none`` profile at its first ``{% if %}`` -- the erratum's own account of why
    the ORIGINAL DR-0025 "omit whenever unresolved" wording was wrong)."""
    config = _build_resolved_config(_CLUSTER_ID, "ephemeral", {}, {}, "my-slug", "test-profile")
    assert "cluster_hostname" in config
    assert config["cluster_hostname"] is None


def test_cluster_hostname_omitted_when_strategy_wants_a_host_but_none_is_known():
    """The OTHER half of DR-0025 Erratum E1's split, distinct from the test
    above: a strategy that WANTS a host (``provider_host``, the load-bearing
    case -- a new cluster has no droplet/VM/IP yet at resolve() time) and could
    not produce one means the key is OMITTED ENTIRELY, so StrictUndefined raises
    downstream naming ``cluster_hostname`` -- never a placeholder, and never the
    SAME representation as the deliberate-none case above, even though
    ``_resolve_hostname`` itself returns bare ``None`` for both."""
    raw_profile = {"hostname": {"strategy": "provider_host"}}
    config = _build_resolved_config(_CLUSTER_ID, "ephemeral", raw_profile, {}, "my-slug", "test-profile")
    assert "cluster_hostname" not in config


def test_cluster_hostname_present_when_resolvable():
    raw_profile = {"hostname": {"strategy": "provider_host"}}
    config = _build_resolved_config(
        _CLUSTER_ID, "ephemeral", raw_profile, {"provider_host": "1.2.3.4"}, "my-slug", "test-profile"
    )
    assert config["cluster_hostname"] == "1.2.3.4"


def test_cluster_hostname_dns_strategy_wanting_a_host_but_unresolvable_is_omitted():
    """The "wanted a host, couldn't produce one" bucket isn't exclusive to
    ``provider_host`` -- an explicit ``hostname.strategy: dns`` with no ``dns:``
    block to resolve against is the SAME "strategy is not none, but produced no
    value" shape, and gets the SAME omitted treatment (never conflated with the
    deliberate-none case, which requires strategy "none" specifically)."""
    raw_profile = {"hostname": {"strategy": "dns"}}
    config = _build_resolved_config(_CLUSTER_ID, "ephemeral", raw_profile, {}, "my-slug", "test-profile")
    assert "cluster_hostname" not in config


def test_config_versions_ported_from_raw_profile():
    raw_profile = {"version": "2.0", "resolution_strategy": "branch_discovery_with_fallback"}
    config = _build_resolved_config(_CLUSTER_ID, "ephemeral", raw_profile, {}, "slug", "my-profile-name")
    assert config["_config_versions"] == {
        "deployment_profile_version": "2.0",
        "deployment_profile_name": "my-profile-name",
        "resolution_strategy": "branch_discovery_with_fallback",
    }


def test_config_versions_defaults_when_raw_profile_omits_them():
    config = _build_resolved_config(_CLUSTER_ID, "ephemeral", {}, {}, "slug", "my-profile-name")
    assert config["_config_versions"] == {
        "deployment_profile_version": "1.0",
        "deployment_profile_name": "my-profile-name",
        "resolution_strategy": "branch_discovery_with_fallback",
    }


def test_rollout_timeout_seconds_ported_from_raw_profile_defaults_to_300():
    assert _build_resolved_config(_CLUSTER_ID, "ephemeral", {}, {}, "slug", "p")["rollout_timeout_seconds"] == 300
    raw_profile = {"rollout_timeout_seconds": 900}
    config = _build_resolved_config(_CLUSTER_ID, "ephemeral", raw_profile, {}, "slug", "p")
    assert config["rollout_timeout_seconds"] == 900


def test_persistence_services_lists_only_services_with_a_persistence_block():
    raw_profile = {
        "services": {
            "postgres": {"repository": "postgres", "persistence": {"type": "postgres"}},
            "web": {"repository": "web"},
        }
    }
    config = _build_resolved_config(_CLUSTER_ID, "ephemeral", raw_profile, {}, "slug", "p")
    assert config["persistence_services"] == ["postgres"]


def test_persistence_services_key_absent_when_none_declared():
    raw_profile = {"services": {"web": {"repository": "web"}}}
    config = _build_resolved_config(_CLUSTER_ID, "ephemeral", raw_profile, {}, "slug", "p")
    assert "persistence_services" not in config


def test_deploy_wave_gets_an_entry_for_every_declared_service_defaulted_to_3():
    """DR-0029 §2/§8: EVERY declared service gets a key, defaulted to 3 at
    WRITE time (not looked up with a default later) when its own YAML sets no
    explicit ``deploy_wave`` -- "is this service's key present" and "is this
    service declared at all" are the SAME question by construction
    (``seedpod/core/deploy_wave.py``'s ``DeploymentProfile.deploy_wave``)."""
    raw_profile = {
        "services": {
            "postgres": {"repository": "postgres", "deploy_wave": 1, "persistence": {"type": "postgres"}},
            "web": {"repository": "web"},
        }
    }
    config = _build_resolved_config(_CLUSTER_ID, "ephemeral", raw_profile, {}, "slug", "p")
    assert config["deploy_wave"] == {"postgres": 1, "web": 3}


def test_deploy_wave_key_absent_when_no_services_declared_at_all():
    """The degenerate case (``DeploymentProfile.deploy_wave``'s own docstring):
    a profile declaring no services at all omits the key entirely, matching
    ``persistence_services``'s own "if x:" presence discipline -- the reader's
    own default (``{}``) covers this case identically to an absent key."""
    config = _build_resolved_config(_CLUSTER_ID, "ephemeral", {}, {}, "slug", "p")
    assert "deploy_wave" not in config


@pytest.mark.parametrize("bad_value", [None, "one", ["3"], True, False, {}])
def test_deploy_wave_non_integer_raises_permanent_error_naming_service_and_value(bad_value):
    """An earlier revision called ``int(service_raw.get("deploy_wave", 3))``
    unguarded: ``deploy_wave: null``/a string/a list raised a bare
    ``TypeError``/``ValueError`` out of ``_build_resolved_config``, escaping
    the one error-taxonomy home (CLAUDE.md). ``bool`` is rejected too, even
    though ``isinstance(True, int)`` is true in Python -- a YAML
    ``deploy_wave: true`` almost certainly means the WRONG field was set
    (v1/v2 have no boolean ``deploy_wave`` concept anywhere)."""
    raw_profile = {"services": {"postgres": {"repository": "postgres", "deploy_wave": bad_value}}}
    with pytest.raises(PermanentError) as exc_info:
        _build_resolved_config(_CLUSTER_ID, "ephemeral", raw_profile, {}, "slug", "p")
    assert exc_info.value.detail["service"] == "postgres"


def test_deploy_wave_negative_raises_permanent_error():
    """A negative rank would otherwise sort ahead of wave 0 -- accepted by a
    bare ``int(...)`` but rejected here, loudly, rather than silently
    behaving like a valid-if-unusual wave index."""
    raw_profile = {"services": {"postgres": {"repository": "postgres", "deploy_wave": -1}}}
    with pytest.raises(PermanentError) as exc_info:
        _build_resolved_config(_CLUSTER_ID, "ephemeral", raw_profile, {}, "slug", "p")
    assert exc_info.value.detail["service"] == "postgres"


def test_data_initialization_key_absent_when_not_passed():
    """DR-0028 Erratum E2's default: an ordinary deploy (``version_update``, or
    a preset deploy whose request carries no ``data_initialization``) must
    never manufacture a spurious key -- ``deploy.load_audit`` treats an absent
    key as "nothing to restore" (``SnapshotRestoreSpec | None``, never an
    empty ``SnapshotRestoreSpec()``)."""
    config = _build_resolved_config(_CLUSTER_ID, "ephemeral", {}, {}, "slug", "p")
    assert "data_initialization" not in config


def test_data_initialization_written_when_passed():
    """DR-0028 decision 2 / Erratum E2, closed: ``PresetService.deploy`` ->
    ``DeploymentService.deploy_direct`` threads the deploy REQUEST's
    ``data_initialization`` here -- NOT a ``raw_profile`` key (no shipped
    profile ever declares it), sourced entirely from the ``data_initialization``
    parameter, dict-shaped exactly like ``seedpod/api/routers/presets.py``'s
    ``DataInitialization.model_dump(exclude_none=True)``."""
    config = _build_resolved_config(
        _CLUSTER_ID, "ephemeral", {}, {}, "slug", "p",
        data_initialization={"restore_from_snapshot": "snap-abc123"},
    )
    assert config["data_initialization"] == {"restore_from_snapshot": "snap-abc123"}


def test_data_initialization_key_absent_when_passed_but_empty():
    """An empty/falsy ``data_initialization`` (e.g. ``{}``, matching what
    ``DataInitialization().model_dump(exclude_none=True)`` produces for a
    request that sets neither restore mode) is treated the SAME as "not
    passed" -- matching ``persistence_services``'s own "if x: config[...] = x"
    pattern immediately above, never a spurious empty-dict key."""
    config = _build_resolved_config(_CLUSTER_ID, "ephemeral", {}, {}, "slug", "p", data_initialization={})
    assert "data_initialization" not in config


def test_ingress_strategy_nested_inside_cluster_config_shape():
    """exampleco-web-2[-kind].yml's own shape: nested directly inside cluster_config,
    no sibling present -- must still be read."""
    raw_profile = {"cluster_spec": {"cluster_config": {"ingress_strategy": {"type": "traefik"}}}}
    config = _build_resolved_config(_CLUSTER_ID, "ephemeral", raw_profile, {}, "slug", "p")
    assert config["ingress_strategy"] == {"type": "traefik"}


def test_ingress_strategy_sibling_of_cluster_config_shape():
    """exampleco-dev-stack-nodns.yml / exampleco-staging-stack[-nodns].yml's own shape:
    ingress_strategy is a SIBLING of cluster_config, not nested inside it -- 3 of
    the 5 shipped profiles use exactly this shape. A nested-only read (v1's own
    bug, matched by an earlier revision of this function) silently drops it for
    all three; this reuses the SAME sibling-overlay rule
    seedpod/engine/steps/cluster.py's _cluster_specification_from already applies
    (that step's own "v1 parity trap" docstring), so the two normalizations can
    never drift apart."""
    raw_profile = {
        "cluster_spec": {
            "cluster_config": {"node_count": 1},
            "ingress_strategy": {"type": "traefik"},
        }
    }
    config = _build_resolved_config(_CLUSTER_ID, "ephemeral", raw_profile, {}, "slug", "p")
    assert config["ingress_strategy"] == {"type": "traefik"}


def test_ingress_strategy_sibling_wins_over_a_different_nested_value():
    """The precedence itself, not just presence: when BOTH shapes are present (no
    shipped profile does this, but the rule must still resolve deterministically),
    the SIBLING wins -- verbatim v1/cluster.load_spec parity, never guarded on
    what cluster_config already carries."""
    raw_profile = {
        "cluster_spec": {
            "cluster_config": {"ingress_strategy": {"type": "nodeport"}},
            "ingress_strategy": {"type": "traefik"},
        }
    }
    config = _build_resolved_config(_CLUSTER_ID, "ephemeral", raw_profile, {}, "slug", "p")
    assert config["ingress_strategy"] == {"type": "traefik"}


def test_ingress_strategy_absent_when_not_declared():
    config = _build_resolved_config(_CLUSTER_ID, "ephemeral", {}, {}, "slug", "p")
    assert "ingress_strategy" not in config


def test_ssl_and_dns_enabled_default_false():
    config = _build_resolved_config(_CLUSTER_ID, "ephemeral", {}, {}, "slug", "p")
    assert config["ssl_enabled"] is False
    assert config["dns_enabled"] is False


def test_ssl_and_dns_enabled_read_from_raw_profile():
    raw_profile = {"ssl": {"enabled": True}, "dns": {"enabled": True, "zone": "x"}}
    config = _build_resolved_config(_CLUSTER_ID, "ephemeral", raw_profile, {}, "slug", "p")
    assert config["ssl_enabled"] is True
    assert config["dns_enabled"] is True


def test_timestamp_is_not_present():
    """Deliberate: seedpod/core bans ambient now() and this module already threads
    an injected Clock elsewhere, but no shipped template reads config.timestamp and
    no v2 consumer wants one -- left out entirely rather than wired to an unused
    Clock parameter (module docstring's own reasoning)."""
    config = _build_resolved_config(_CLUSTER_ID, "ephemeral", {}, {}, "slug", "p")
    assert "timestamp" not in config


def test_config_overrides_applied_last_and_win():
    """v1's own precedence (config.update(config_overrides) LAST), preserved
    verbatim: overrides can replace ANYTHING _build_resolved_config itself just
    computed, including the five template keys."""
    config = _build_resolved_config(
        _CLUSTER_ID, "ephemeral", {}, {"environment": "overridden", "pod_cidr": "9.9.9.9/24"}, "slug", "p"
    )
    assert config["environment"] == "overridden"
    assert config["pod_cidr"] == "9.9.9.9/24"


# ---------------------------------------------------------------------------
# DR-0025 Erratum E2 -- _hostname_deferred / rehydrate_cluster_hostname.
# Pure-function unit coverage of the DECISION-time split (defer vs raise-now)
# and the DEPLOY-time re-resolution, isolated from the full _deploy() pipeline
# (tests/app/test_services_deployment.py's own end-to-end test covers that).
# ---------------------------------------------------------------------------


def test_hostname_deferred_true_for_provider_host_when_omitted():
    """The load-bearing case: a NEW cluster's `_build_resolved_config` always
    omits `cluster_hostname` for `provider_host` (no provider_host known yet
    at decision time) -- `_hostname_deferred` must recognise this as DEFER,
    not raise-now."""
    raw_profile = {"hostname": {"strategy": "provider_host"}}
    resolved_config = _build_resolved_config(_CLUSTER_ID, "ephemeral", raw_profile, {}, "slug", "p")
    assert "cluster_hostname" not in resolved_config
    assert _hostname_deferred(raw_profile, resolved_config) is True


def test_hostname_deferred_true_for_custom_pattern_needing_provider_host():
    raw_profile = {"hostname": {"strategy": "custom", "custom_pattern": "{cluster_slug}.{provider_host}.example"}}
    resolved_config = _build_resolved_config(_CLUSTER_ID, "ephemeral", raw_profile, {}, "slug", "p")
    assert "cluster_hostname" not in resolved_config
    assert _hostname_deferred(raw_profile, resolved_config) is True


def test_hostname_deferred_false_when_cluster_hostname_already_resolved():
    """A `dns` strategy resolves deterministically from `cluster_slug` alone --
    never omitted, never deferred."""
    raw_profile = {"dns": {"enabled": True, "zone": "example.com"}}
    resolved_config = _build_resolved_config(_CLUSTER_ID, "ephemeral", raw_profile, {}, "slug", "p")
    assert "cluster_hostname" in resolved_config
    assert _hostname_deferred(raw_profile, resolved_config) is False


def test_hostname_deferred_false_for_strategy_none():
    raw_profile = {"hostname": {"strategy": "none"}}
    resolved_config = _build_resolved_config(_CLUSTER_ID, "ephemeral", raw_profile, {}, "slug", "p")
    assert resolved_config["cluster_hostname"] is None
    assert _hostname_deferred(raw_profile, resolved_config) is False


def test_hostname_deferred_false_for_custom_with_no_pattern_a_genuine_config_error():
    """E2 point (iii): a `custom` strategy declaring NO pattern at all can
    NEVER be fixed by provisioning -- this must stay in the raise-now bucket,
    never silently reclassified as deferrable."""
    raw_profile = {"hostname": {"strategy": "custom"}}
    resolved_config = _build_resolved_config(_CLUSTER_ID, "ephemeral", raw_profile, {}, "slug", "p")
    assert "cluster_hostname" not in resolved_config
    assert _hostname_deferred(raw_profile, resolved_config) is False


def test_hostname_deferred_false_for_dns_with_missing_config_a_genuine_config_error():
    """A `dns` strategy that itself omits `zone`/the whole `dns:` block resolves
    to None for a reason no amount of provisioning fixes -- E2 point (iii)
    keeps this in the raise-now bucket."""
    raw_profile = {"hostname": {"strategy": "dns"}}
    resolved_config = _build_resolved_config(_CLUSTER_ID, "ephemeral", raw_profile, {}, "slug", "p")
    assert "cluster_hostname" not in resolved_config
    assert _hostname_deferred(raw_profile, resolved_config) is False


def test_rehydrate_cluster_hostname_provider_host_uses_the_real_host():
    raw_profile = {"hostname": {"strategy": "provider_host"}}
    assert rehydrate_cluster_hostname(raw_profile, cluster_slug="my-slug", provider_host="203.0.113.42") == (
        True,
        "203.0.113.42",
    )


def test_rehydrate_cluster_hostname_custom_pattern_formats_both_placeholders():
    raw_profile = {"hostname": {"strategy": "custom", "custom_pattern": "{cluster_slug}.{provider_host}.example"}}
    assert rehydrate_cluster_hostname(raw_profile, cluster_slug="my-slug", provider_host="203.0.113.42") == (
        True,
        "my-slug.203.0.113.42.example",
    )


def test_rehydrate_cluster_hostname_none_strategy_is_present_but_none():
    """`(True, None)` -- Erratum E1's "deliberately has no hostname" case:
    PRESENT (so `_rehydrate` sets `cluster_hostname=None` in resolved_config
    and feature gates evaluate false cleanly), never OMITTED. Defensive:
    `rehydrate_cluster_hostname`'s one real caller only ever reaches it for a
    row DR-0025 Erratum E2 itself only ever defers (`provider_host`/
    `custom`-needing-a-host), never `strategy: "none"` -- pinned directly
    anyway, since a bare `str | None` return could not express this and a
    prior revision of `_rehydrate` mis-handled it (a fix-pass finding)."""
    raw_profile = {"hostname": {"strategy": "none"}}
    assert rehydrate_cluster_hostname(raw_profile, cluster_slug="my-slug", provider_host="203.0.113.42") == (
        True,
        None,
    )


def test_rehydrate_cluster_hostname_unresolvable_custom_pattern_is_absent():
    """`(False, None)` -- Erratum E1's "wanted a host, could not produce one"
    case: ABSENT, so `_rehydrate` leaves the key OMITTED and the subsequent
    `render_only` raises loudly on `cluster_hostname` rather than rendering a
    template with the feature silently disabled. A `custom` pattern that
    references `{provider_host}` with no host available is the one case
    `_resolve_hostname` can still return `None` for even after this
    function's own `strategy == "none"` short-circuit."""
    raw_profile = {"hostname": {"strategy": "custom", "custom_pattern": "{provider_host}.example"}}
    assert rehydrate_cluster_hostname(raw_profile, cluster_slug="my-slug", provider_host="") == (False, None)


# ---------------------------------------------------------------------------
# DR-0034 — the DNS record and the rendered hostname must be the same string.
# ---------------------------------------------------------------------------


def _shipped_profile(name: str) -> dict:
    import yaml

    path = Path(__file__).resolve().parents[2] / "config" / "deployment-profiles" / name
    return yaml.safe_load(path.read_text())


def test_provider_config_carries_the_dns_block_only_when_enabled():
    """v1's own rule at its own row-synthesis site (cluster_manager.py:318-321). The
    "only when enabled" half is load-bearing: it is what lets
    `DnsIntent.from_provider_config` treat absence as absence of intent."""
    spec = {"node_specification": {"cpu_cores": 2}, "cluster_config": {"node_count": 1}}

    enabled = _provider_config_from({"cluster_spec": spec, "dns": {"enabled": True, "zone": "example.com"}})
    disabled = _provider_config_from({"cluster_spec": spec, "dns": {"enabled": False}})
    absent = _provider_config_from({"cluster_spec": spec})

    assert enabled["dns_config"] == {"enabled": True, "zone": "example.com"}
    assert "dns_config" not in disabled
    assert "dns_config" not in absent
    # ... and the cluster_spec passthrough is untouched by any of it.
    assert absent == spec


def test_the_created_record_and_the_rendered_hostname_are_the_same_string():
    """**DR-0034 decision 8, on the real DNS profile smoke 10 ran.** The record
    `dns.create_record` makes at PROVISION time and the hostname `_resolve_hostname`
    renders into every Ingress `host` at DEPLOY time are computed by two different
    functions. If they ever disagree, #22 comes back in a subtler form: a record that
    resolves, for a name nothing serves.

    They agree because both read the same profile block -- one live, one via
    `provider_config` -- so this drives both ends off one shipped file."""
    profile = _shipped_profile("exampleco-staging-stack.yml")
    slug = "preset-abc-e35dbd4d"

    rendered = _resolve_hostname(profile, {}, slug, None)
    intent = DnsIntent.from_provider_config(_provider_config_from(profile))

    assert intent is not None
    assert intent.fqdn_for(slug) == rendered
    assert rendered == f"{slug}.cluster.example.com"


def test_a_nodns_profile_yields_neither_a_record_nor_a_hostname():
    """The other shipped shape (smoke 9's). `dns.enabled: false` must produce no
    intent AND no hostname -- if it produced one but not the other, either the
    manifests would render a name with no record or a record would exist for a name
    nothing uses."""
    profile = _shipped_profile("exampleco-staging-stack-nodns.yml")

    assert DnsIntent.from_provider_config(_provider_config_from(profile)) is None
    assert _resolve_hostname(profile, {}, "preset-abc-e35dbd4d", None) is None


def test_provider_config_carries_the_ssl_block_only_when_enabled():
    """DR-0036 decision 3, the same shape as the dns block above -- v1 wrote both at
    the same site, each guarded on its own `enabled` (cluster_manager.py:318-332)."""
    spec = {"node_specification": {"cpu_cores": 2}, "cluster_config": {"node_count": 1}}

    enabled = _provider_config_from({"cluster_spec": spec, "ssl": {"enabled": True, "acme_email": "a@b.c"}})
    disabled = _provider_config_from({"cluster_spec": spec, "ssl": {"enabled": False}})

    assert enabled["ssl_config"] == {"enabled": True, "acme_email": "a@b.c"}
    assert "ssl_config" not in disabled


def test_the_certresolver_annotation_and_the_resolver_config_agree():
    """**DR-0036 decision 2, the load-bearing test.** The Ingress templates render
    `router.tls.certresolver: letsencrypt` under `use_acme_certs`; `AcmeConfig` decides
    whether Traefik is configured with a resolver by that name. #24 was those two
    disagreeing. Driven off the real shipped profiles, both directions."""
    for name in ("exampleco-staging-stack.yml", "exampleco-staging-stack-nodns.yml", "exampleco-dev-stack-nodns.yml"):
        profile = _shipped_profile(name)
        resolved = _build_resolved_config(_CLUSTER_ID, "ephemeral", profile, {}, "some-slug", name)
        # `use_acme_certs` is v1's rule, computed by services/manifests.py from the
        # same two flags this config carries.
        annotation_rendered = bool(resolved.get("ssl_enabled")) and bool(resolved.get("dns_enabled"))
        resolver_configured = AcmeConfig.from_provider_config(_provider_config_from(profile)) is not None
        assert annotation_rendered == resolver_configured, name


def test_only_the_dns_profile_gets_a_resolver():
    """The concrete expectation behind the equivalence above: the two `-nodns` profiles
    set `ssl.enabled: true` deliberately (their own comment: "no certresolver
    annotation = Traefik uses auto-generated self-signed cert") and must NOT get one."""
    assert AcmeConfig.from_provider_config(_provider_config_from(_shipped_profile("exampleco-staging-stack.yml")))
    for name in ("exampleco-staging-stack-nodns.yml", "exampleco-dev-stack-nodns.yml"):
        assert AcmeConfig.from_provider_config(_provider_config_from(_shipped_profile(name))) is None


def test_the_ephemeral_profile_points_at_lets_encrypt_staging():
    """DR-0036 decision 5. LE production allows 50 certs/week per REGISTERED DOMAIN,
    and every ephemeral cluster burns one; this profile is `environment_type:
    ephemeral`. If someone points it back at production, that is a deliberate act and
    this test is where they say so."""
    cfg = AcmeConfig.from_provider_config(_provider_config_from(_shipped_profile("exampleco-staging-stack.yml")))
    assert cfg is not None
    assert cfg.server == "https://acme-staging-v02.api.letsencrypt.org/directory"
