"""THE single home of the error taxonomy (docs/design/coherence-review.md Conflict 6).

All seams (`core/cluster_spec.py`, `engine/errors.py`, `providers/*`) import from here;
nothing else may define ``ProviderError`` or a sibling leaf. ``ErrorCode`` is Seam C's 17
members verbatim (docs/design/seam-c-provider.md §5.1). ``InfrastructureUnreachableError``'s
docstring is salvaged verbatim from ``reference-code/seedpod/seedpod/core/cluster_spec.py:298``
(Conflict 6 pins this address for plan-letter fidelity even though the class itself now
lives here, not in ``cluster_spec.py``).

``InvalidTransition``/``StaleVersion`` are machine-layer errors, NOT ``ProviderError``s —
they live in ``seedpod/core/machine.py`` (Conflict 6's final comment line).
"""

from __future__ import annotations

from enum import StrEnum

__all__ = [
    "ErrorCode",
    "ProviderError",
    "TransientError",
    "PermanentError",
    "InfrastructureUnreachableError",
]


class ErrorCode(StrEnum):
    API_TIMEOUT = "api_timeout"  # HTTP/CLI call exceeded its deadline
    DAEMON_UNREACHABLE = "daemon_unreachable"  # tart/docker binary missing or hung
    ENDPOINT_UNREACHABLE = "endpoint_unreachable"  # conn refused/reset/no-route/TLS-handshake-timeout
    MALFORMED_RESPONSE = "malformed_response"  # empty/garbage body where JSON expected
    RATE_LIMITED = "rate_limited"
    API_5XX = "api_5xx"
    RESOURCE_BUSY = "resource_busy"  # tart delete-after-stop failure, docker busy
    HOST_KEYS_PENDING = "host_keys_pending"  # ssh-keyscan returned empty output
    AUTH = "auth"  # 401/403-auth, bad kubeconfig creds
    INVALID_INPUT = "invalid_input"  # bad manifest, missing resource_ids, bad zone
    NOT_FOUND = "not_found"  # required referent absent (base image, DNS zone)
    ALREADY_EXISTS = "already_exists"
    CAPACITY = "capacity"  # kind port-range exhausted, DO quota
    SCRIPT_FAILED = "script_failed"  # ssh/k3s/kubectl non-zero exit, non-network
    UNSUPPORTED = "unsupported"  # command outside provider's supported set
    READINESS_TIMEOUT = "readiness_timeout"  # ENGINE-synthesized: wait-gate budget exhausted
    RETRY_EXHAUSTED = "retry_exhausted"  # ENGINE-synthesized: Schedule budget exhausted


class ProviderError(Exception):
    """Base. Never raised directly — one of the three leaves only."""

    def __init__(
        self,
        message: str,
        *,
        code: ErrorCode,
        provider: str = "engine",  # default: domain steps / engine synthesis
        command: str = "",  # default: domain steps / engine synthesis
        detail: dict[str, str] | None = None,
    ) -> None:
        super().__init__(message)
        self.code, self.provider, self.command = code, provider, command
        self.detail = detail or {}  # raw stderr / http_status / exit_code live HERE only


class TransientError(ProviderError):
    """The same call may succeed if repeated. Engine retries per the step's Schedule."""

    def __init__(self, *args, retry_after: float | None = None, **kw) -> None:
        super().__init__(*args, **kw)
        self.retry_after = retry_after  # e.g. GHCR Retry-After header


class PermanentError(ProviderError):
    """Retrying is provably useless. Engine fails the step and runs the undo scope."""


class InfrastructureUnreachableError(ProviderError):  # SIBLING leaf, not a Transient subclass
    """
    Raised when we cannot determine infrastructure state.

    This is NOT an error indicating infrastructure is gone - it means
    we cannot authoritatively determine the current state due to
    connectivity issues, timeouts, or other transient failures.

    Reconciliation should SKIP clusters when this is raised, not
    mark them as orphaned.
    """  # docstring salvaged VERBATIM from reference-code/seedpod/seedpod/core/cluster_spec.py:298

    def __init__(self, *args, host: str | None = None, **kw) -> None:
        super().__init__(*args, **kw)
        self.host = host  # api.digitalocean.com / docker host / apiserver URL / "localhost"
