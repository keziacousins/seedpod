"""seedpod/providers/classify.py — Seam C §5.1 shared classifier functions, amended by
docs/design/coherence-review.md Conflict 6 (taxonomy home moved to
``seedpod.core.errors``; this module imports it, never redefines it).

THE one place string-sniffing survives (CLAUDE.md: "String-sniffing lives ONLY in
providers/classify.py"). Every concrete provider converts a raw subprocess/HTTP symptom
into one of the three taxonomy leaves *at the edge*, through ``classify_subprocess`` or
``classify_http`` — never by constructing ``TransientError``/``PermanentError``/
``InfrastructureUnreachableError`` ad hoc from a symptom string. (Providers MAY still
raise these directly for symptoms that are *not* raw strings — e.g. a typed HTTP client
exception, or a caller that has already mapped a symptom to absence-as-data — because
there is nothing to sniff there.)

Absence is DATA, unreachable is a RAISE — never conflated (crown jewel #1). Neither
function here ever returns something that *looks* like absence; "not found" / "not
ready yet" / "already destroyed" results are constructed by the calling provider as
typed ``Result`` values (§5.3), never routed through this module.

Deviations from the seam's illustrative four-parameter ``classify_http`` docstring,
flagged loudly rather than silently: the docstring's prose ("garbage body ⇒
Transient(MALFORMED_RESPONSE) for services, Unreachable(MALFORMED_RESPONSE) for machine
providers") requires two signals — "the body was garbage" and "is this a machine
provider observing infra" — that the four documented kwargs (`status`, `rate_limited`,
`retry_after`) cannot carry (a garbage body can arrive under any status, including 200).
``malformed_body`` and ``observing_infra`` are added as explicit keyword parameters
(mirroring ``classify_subprocess``'s own ``observing_infra``) so the decision-table rows
(10, 35) are actually reachable, rather than inventing a magic sentinel status.
"""

from __future__ import annotations

from seedpod.core.errors import (
    ErrorCode,
    InfrastructureUnreachableError,
    PermanentError,
    ProviderError,
    TransientError,
)

__all__ = [
    "TRANSIENT_STDERR_PHRASES",
    "TART_NOT_FOUND_PHRASES",
    "classify_subprocess",
    "classify_http",
]

# Merged from v1's installer + DO-SSH + kind/docker phrase lists — the only string
# sniffing this contract performs, and it lives here only.
TRANSIENT_STDERR_PHRASES = frozenset(
    {
        "connection refused",
        "connection timed out",
        "no route to host",
        "network is unreachable",
        "cannot connect",
        "i/o timeout",
        # The sshd-is-up-but-not-ready family, added 2026-08-09 after smoke 8. A booting
        # guest accepts the TCP connection (so `k3s.await_ssh`'s bare dial passes) and then
        # drops it mid-handshake while sshd restarts under cloud-init. None of the phrases
        # above match what ssh prints for that, so it fell through to "clean non-zero exit
        # => Permanent" and failed a whole provision that a single retry fixed.
        "kex_exchange_identification",  # "Connection closed by remote host"
        "connection closed by",         # "Connection closed by 1.2.3.4 port 22"
        "connection reset by",          # "Connection reset by 1.2.3.4 port 22"
        "banner exchange",              # "Connection to 1.2.3.4 port 22: invalid format"
        "temporary failure in name resolution",
        "operation timed out",          # what macOS prints where Linux says "connection timed out"
        # The guest-side-download family, added 2026-08-13 after the first tart run of
        # the dev exampleco stack. `k3s.install` runs `curl -sfL https://get.k3s.io | sudo
        # sh -` INSIDE the guest, so the failure that matters is a download made by a
        # machine we are not talking to. The k3s installer exits 1 for "the GitHub
        # release download failed" exactly as it does for "you passed a bad flag", and
        # rc=1 with none of the phrases above matched fell through to "clean non-zero
        # exit => Permanent". `retry: ssh_default` (3 attempts) was therefore declared
        # on that step and never used: attempt=1, then `on_failure: compensate`
        # destroyed the VM. A whole provision died in 35s over a download that
        # succeeded on every later attempt, by hand, three times.
        #
        # This is the same sentence as the smoke-8 family above, one exit code over
        # (255 -> 1), which is why it belongs in the same list rather than in a new
        # branch: a clean non-zero exit is only "authoritative" about the command we
        # ran, and `sh -` is a pipe to someone else's network.
        "download failed",              # the k3s installer's own fatal()
        "could not resolve host",       # curl (6), inside the guest
        "failed to connect",            # curl (7), inside the guest
    }
)

# Salvaged verbatim from v1 ``_tart_cli._classify_not_found``. Exported for callers
# (the tart adapter) to pre-map "not found" subprocess symptoms to absence-as-data
# *before* calling ``classify_subprocess`` — never consulted inside this module, per
# the decision table's row 7 ("caller pre-mapped it to absence-as-data").
TART_NOT_FOUND_PHRASES = frozenset(
    {
        "not found",
        "does not exist",
        "doesn't exist",
        "no such virtual machine",
    }
)


def _stderr_has_transient_phrase(stderr: str) -> bool:
    lowered = (stderr or "").lower()
    return any(phrase in lowered for phrase in TRANSIENT_STDERR_PHRASES)


def classify_subprocess(
    *,
    provider: str,
    command: str,
    host: str,
    rc: int,
    stderr: str,
    timed_out: bool,
    binary_missing: bool,
    observing_infra: bool,
) -> ProviderError:
    """``observing_infra=True`` for machine providers + kubectl talking to their
    control plane: timeout / missing daemon / connection phrases ⇒
    ``InfrastructureUnreachableError``. ``observing_infra=False`` (SSH to a booting
    guest, supporting services): same symptoms ⇒ ``TransientError``. A clean non-zero
    exit is an AUTHORITATIVE answer ⇒ ``PermanentError(SCRIPT_FAILED)`` — unless the
    caller pre-mapped it to absence-as-data (docker inspect rc≠0, ``TartNotFound``),
    in which case the caller never calls this function at all.
    """
    detail = {"exit_code": str(rc), "stderr": stderr}
    connectivity_symptom = timed_out or binary_missing or _stderr_has_transient_phrase(stderr)

    if connectivity_symptom:
        if binary_missing:
            code = ErrorCode.DAEMON_UNREACHABLE
        elif timed_out:
            code = ErrorCode.API_TIMEOUT
        else:
            code = ErrorCode.ENDPOINT_UNREACHABLE

        message = f"{provider}.{command}: could not reach {host or 'infrastructure'} ({code})"
        if observing_infra:
            return InfrastructureUnreachableError(
                message, code=code, provider=provider, command=command, detail=detail, host=host
            )
        return TransientError(message, code=ErrorCode.ENDPOINT_UNREACHABLE, provider=provider, command=command, detail=detail)

    # Clean non-zero exit: an authoritative answer, not a connectivity symptom.
    message = f"{provider}.{command}: exited {rc}"
    return PermanentError(message, code=ErrorCode.SCRIPT_FAILED, provider=provider, command=command, detail=detail)


def classify_http(
    *,
    provider: str,
    command: str,
    host: str,
    status: int,
    rate_limited: bool = False,
    retry_after: float | None = None,
    malformed_body: bool = False,
    observing_infra: bool = False,
) -> ProviderError:
    """401/403-auth ⇒ ``Permanent(AUTH)``; 403+rate-limit signal / 429 ⇒
    ``Transient(RATE_LIMITED, retry_after)``; 408/5xx ⇒ ``Transient``; garbage body ⇒
    ``Transient(MALFORMED_RESPONSE)`` for services, ``Unreachable(MALFORMED_RESPONSE)``
    for machine providers (v1's DO "Expecting value" rule — ``observing_infra`` is this
    module's stand-in for "machine provider"). 404 never reaches here: reads map it to
    absence-as-data before classifying.
    """
    detail = {"status": str(status)}

    if malformed_body:
        message = f"{provider}.{command}: malformed response body from {host}"
        if observing_infra:
            return InfrastructureUnreachableError(
                message, code=ErrorCode.MALFORMED_RESPONSE, provider=provider, command=command, detail=detail, host=host
            )
        return TransientError(message, code=ErrorCode.MALFORMED_RESPONSE, provider=provider, command=command, detail=detail)

    if status in (401, 403) and not rate_limited:
        message = f"{provider}.{command}: auth failed ({status})"
        return PermanentError(message, code=ErrorCode.AUTH, provider=provider, command=command, detail=detail)

    if status == 429 or rate_limited:
        message = f"{provider}.{command}: rate limited ({status})"
        return TransientError(
            message,
            code=ErrorCode.RATE_LIMITED,
            provider=provider,
            command=command,
            detail=detail,
            retry_after=retry_after,
        )

    if status == 408 or 500 <= status < 600:
        message = f"{provider}.{command}: server-side failure ({status})"
        code = ErrorCode.API_5XX if 500 <= status < 600 else ErrorCode.API_TIMEOUT
        return TransientError(message, code=code, provider=provider, command=command, detail=detail)

    # Anything else with a body we could parse is an authoritative, non-network answer
    # the caller misrouted here (e.g. a validation error) — never guess at Unreachable.
    message = f"{provider}.{command}: unexpected status {status}"
    return PermanentError(message, code=ErrorCode.INVALID_INPUT, provider=provider, command=command, detail=detail)
