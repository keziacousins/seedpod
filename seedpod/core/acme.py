"""core/acme.py — ``AcmeConfig``: the Let's Encrypt certresolver a cluster's profile
asked for, carried from ``cluster.load_spec`` to ``k3s.install`` (DR-0036).

Pure data (Pillar 1: no IO, no ``now()``), the same shape and home discipline as
``core/dns_record.py``'s ``DnsIntent`` — which this deliberately mirrors, because the
two answer the same kind of question about the same profile.

**Why this exists at all.** v2 rendered the CLIENT half of ACME correctly and never
configured the server half: ``use_acme_certs = ssl_enabled and dns_enabled``
(``services/manifests.py``, v1 verbatim) put
``traefik.ingress.kubernetes.io/router.tls.certresolver: letsencrypt`` on every
Ingress, while nothing anywhere defined a resolver by that name, so Traefik fell back
to ``CN=TRAEFIK DEFAULT CERT`` (backlog #24, found by smoke 11).

**The gate is the same rule the templates use** — both blocks present and enabled —
and a test pins the two halves to agree for a shipped profile. That equivalence is the
whole point: an annotation naming a resolver nobody configures is the bug this closes,
and it must not be able to come back.

**No hostname here, deliberately.** v1 gated its ACME block on ``dns_hostname`` being
present (``reference-code/seedpod/seedpod/core/state_manager.py``:1039, 1066). The
resolver configuration itself contains no hostname — Traefik requests certificates for
whatever ``Host`` rules its routers carry — so the same intent ("don't ask a CA for a
name that does not resolve") is served by gating on ``dns.enabled``, decided at install
time instead of deploy time. See DR-0036 decision 2.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel

from seedpod.core.errors import ErrorCode, PermanentError

__all__ = ["AcmeConfig"]

# v1's own defaults (reference-code .../core/state_manager.py:1067-1069).
_DEFAULT_SERVER = "https://acme-v02.api.letsencrypt.org/directory"
_HTTP_CHALLENGE = "httpChallenge"
_TLS_CHALLENGE = "tlsChallenge"


class AcmeConfig(BaseModel):
    """What the profile's ``ssl:`` block asked for, once it is known that a DNS name
    will exist to certify.

    ``challenge`` keeps v1's two-valued protocol (``httpChallenge`` | anything else ⇒
    tlsChallenge, v1's own ``if challenge_type == "httpChallenge": ... else: ...``),
    normalized here to the two literals so the manifest builder never has to
    re-interpret a free string."""

    email: str
    server: str = _DEFAULT_SERVER
    challenge: str = _HTTP_CHALLENGE

    @property
    def uses_http_challenge(self) -> bool:
        return self.challenge == _HTTP_CHALLENGE

    @classmethod
    def from_provider_config(cls, provider_config: Mapping[str, Any] | None) -> AcmeConfig | None:
        """``None`` unless BOTH ``ssl_config`` and ``dns_config`` are present and
        enabled — v1's ``use_acme_certs = ssl_enabled and dns_enabled``
        (``manifest_resolver.py``:886), which is also exactly the condition under
        which the Ingress templates render the certresolver annotation.

        ``_provider_config_from`` writes each block only when that block is enabled,
        so presence and enabled-ness agree; both are checked anyway, because this
        type is what the two halves are pinned to agree ON and it should not depend
        on a caller's discipline to be correct.

        **Deliberate divergence from v1 (DR-0036 decision 3)**: an enabled block with
        no ``acme_email`` raises instead of defaulting to v1's ``admin@example.com``.
        A profile asking a real CA for real certificates with no contact address is
        malformed, and a wrong-but-plausible default is worse than a loud failure.
        Unreachable for every shipped profile."""
        config = provider_config or {}
        ssl_config = config.get("ssl_config") or {}
        dns_config = config.get("dns_config") or {}
        if not ssl_config.get("enabled", False) or not dns_config.get("enabled", False):
            return None

        email = ssl_config.get("acme_email")
        if not email:
            raise PermanentError(
                "acme-config: profile enables ssl + dns but names no ssl.acme_email",
                code=ErrorCode.INVALID_INPUT,
                provider="acme",
                command="load_config",
                detail={"ssl_config": str(dict(ssl_config))},
            )

        declared: dict[str, Any] = {}
        server = ssl_config.get("acme_server")
        if server:
            declared["server"] = str(server)
        challenge = ssl_config.get("challenge_type")
        if challenge:
            # v1's own two-valued reading: httpChallenge, else tlsChallenge.
            declared["challenge"] = _HTTP_CHALLENGE if challenge == _HTTP_CHALLENGE else _TLS_CHALLENGE
        return cls(email=str(email), **declared)
