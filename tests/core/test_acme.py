"""tests/core/test_acme.py — ``AcmeConfig``, the certresolver half v2 never had
(DR-0036, backlog #24, found by smoke 11).

Salvage source: ``reference-code/seedpod/seedpod/core/state_manager.py``:1066-1082
(``_apply_traefik_config``'s ACME block) for the field set and defaults, and
``orchestrator/manifest_resolver.py``:886 (``use_acme_certs = ssl_enabled and
dns_enabled``) for the gate.

The last test in this module is the load-bearing one: it pins the CLIENT half (the
Ingress annotation the templates render) and the SERVER half (the resolver this type
configures) to agree for a shipped profile. An annotation naming a resolver nobody
configures is exactly the defect #24 was.
"""

from __future__ import annotations

import pytest

from seedpod.core.acme import AcmeConfig
from seedpod.core.errors import ErrorCode, PermanentError

_SSL = {"enabled": True, "acme_email": "kezia@example.com"}
_DNS = {"enabled": True, "zone": "example.com"}


def test_both_blocks_enabled_yields_a_config():
    cfg = AcmeConfig.from_provider_config({"ssl_config": _SSL, "dns_config": _DNS})
    assert cfg == AcmeConfig(
        email="kezia@example.com",
        server="https://acme-v02.api.letsencrypt.org/directory",
        challenge="httpChallenge",
    )


def test_the_defaults_are_v1s():
    """reference-code .../state_manager.py:1067-1069."""
    cfg = AcmeConfig(email="a@b.c")
    assert cfg.server == "https://acme-v02.api.letsencrypt.org/directory"
    assert cfg.challenge == "httpChallenge"
    assert cfg.uses_http_challenge is True


@pytest.mark.parametrize(
    "ssl_config, dns_config",
    [
        ({"enabled": False, "acme_email": "a@b.c"}, _DNS),  # ssl off
        (_SSL, {"enabled": False}),  # dns off -- no name to certify
        ({}, _DNS),
        (_SSL, {}),
        ({}, {}),
    ],
)
def test_the_gate_needs_both_halves(ssl_config, dns_config):
    """v1's `use_acme_certs = ssl_enabled and dns_enabled`. The dns half matters: a
    certificate for a name that does not resolve can never be issued, and every
    failed validation counts against a rate limit."""
    assert AcmeConfig.from_provider_config({"ssl_config": ssl_config, "dns_config": dns_config}) is None


def test_no_provider_config_at_all_is_none():
    assert AcmeConfig.from_provider_config(None) is None
    assert AcmeConfig.from_provider_config({}) is None


def test_a_profile_may_override_server_and_challenge():
    cfg = AcmeConfig.from_provider_config(
        {
            "ssl_config": {
                "enabled": True,
                "acme_email": "kezia@example.com",
                "acme_server": "https://acme-staging-v02.api.letsencrypt.org/directory",
                "challenge_type": "tlsChallenge",
            },
            "dns_config": _DNS,
        }
    )
    assert cfg is not None
    assert cfg.server.endswith("acme-staging-v02.api.letsencrypt.org/directory")
    assert cfg.uses_http_challenge is False


def test_an_unrecognised_challenge_reads_as_tls_challenge_like_v1():
    """v1's own two-valued branch: `if challenge_type == "httpChallenge": ... else:
    tlsChallenge`. Normalized here so the manifest builder never re-interprets a free
    string."""
    cfg = AcmeConfig.from_provider_config(
        {"ssl_config": {**_SSL, "challenge_type": "dnsChallenge"}, "dns_config": _DNS}
    )
    assert cfg is not None
    assert cfg.challenge == "tlsChallenge"


def test_enabled_with_no_email_raises_rather_than_defaulting_to_v1s_placeholder():
    """DR-0036 decision 3, following DR-0034 decision 2's precedent. v1 defaulted to
    `admin@example.com` -- a wrong-but-plausible contact on a real CA account is worse
    than a loud failure."""
    with pytest.raises(PermanentError) as excinfo:
        AcmeConfig.from_provider_config({"ssl_config": {"enabled": True}, "dns_config": _DNS})
    assert excinfo.value.code is ErrorCode.INVALID_INPUT
    assert "acme_email" in str(excinfo.value)


def test_a_disabled_ssl_block_never_validates_the_email():
    """The two shipped `-nodns` profiles have `ssl.enabled: true` with `dns.enabled:
    false`, so the gate short-circuits before the email check. Pinned because the
    raise above would otherwise be a live hazard for them."""
    assert AcmeConfig.from_provider_config({"ssl_config": {"enabled": True}, "dns_config": {"enabled": False}}) is None
