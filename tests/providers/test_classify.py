"""tests/providers/test_classify.py — table-driven over the decision-table cells
docs/design/seam-c-provider.md §5.1 assigns to ``classify_subprocess``/``classify_http``
(rows describing a raw symptom -> classification; rows that are typed ``Result`` values,
or that need provider-specific pre-mapping before reaching this module, are out of
scope here — see ``seedpod/providers/classify.py``'s module docstring).

No ``Mock``/``patch`` anywhere: both functions under test are pure, so cases are plain
keyword-argument tables.
"""

from __future__ import annotations

import pytest

from seedpod.core.errors import (
    ErrorCode,
    InfrastructureUnreachableError,
    PermanentError,
    TransientError,
)
from seedpod.providers.classify import (
    TART_NOT_FOUND_PHRASES,
    TRANSIENT_STDERR_PHRASES,
    classify_http,
    classify_subprocess,
)

# ============================================================================
# classify_subprocess — decision table rows 1, 16, 17, 21, 23, 24, 27
# ============================================================================

SUBPROCESS_CASES = [
    # (row, kwargs, expected_type, expected_code, expects_host)
    pytest.param(
        1,
        {
            "provider": "tart",
            "command": "clone",
            "host": "localhost",
            "rc": -1,
            "stderr": "",
            "timed_out": False,
            "binary_missing": True,
            "observing_infra": True,
        },
        InfrastructureUnreachableError,
        ErrorCode.DAEMON_UNREACHABLE,
        id="row1-tart-binary-missing",
    ),
    pytest.param(
        1,
        {
            "provider": "tart",
            "command": "clone",
            "host": "localhost",
            "rc": -1,
            "stderr": "",
            "timed_out": True,
            "binary_missing": False,
            "observing_infra": True,
        },
        InfrastructureUnreachableError,
        ErrorCode.API_TIMEOUT,
        id="row1-tart-subprocess-timeout",
    ),
    pytest.param(
        21,
        {
            "provider": "kind",
            "command": "create",
            "host": "docker_host",
            "rc": -1,
            "stderr": "",
            "timed_out": True,
            "binary_missing": False,
            "observing_infra": True,
        },
        InfrastructureUnreachableError,
        ErrorCode.API_TIMEOUT,
        id="row21-kind-subprocess-timeout",
    ),
    pytest.param(
        23,
        {
            "provider": "kind",
            "command": "docker_inspect",
            "host": "docker_host",
            "rc": 1,
            "stderr": "Error: connection refused",
            "timed_out": False,
            "binary_missing": False,
            "observing_infra": True,
        },
        InfrastructureUnreachableError,
        ErrorCode.ENDPOINT_UNREACHABLE,
        id="row23-kind-docker-conn-refused",
    ),
    pytest.param(
        27,
        {
            "provider": "kubectl",
            "command": "get_pods",
            "host": "https://apiserver.example",
            "rc": 1,
            "stderr": "dial tcp: i/o timeout",
            "timed_out": False,
            "binary_missing": False,
            "observing_infra": True,
        },
        InfrastructureUnreachableError,
        ErrorCode.ENDPOINT_UNREACHABLE,
        id="row27-kubectl-io-timeout-stderr",
    ),
    pytest.param(
        16,
        {
            "provider": "ssh-k3s",
            "command": "install",
            "host": "10.0.0.5",
            "rc": 255,
            "stderr": "ssh: connect to host 10.0.0.5 port 22: Connection refused",
            "timed_out": False,
            "binary_missing": False,
            "observing_infra": False,
        },
        TransientError,
        ErrorCode.ENDPOINT_UNREACHABLE,
        id="row16-ssh-k3s-transient-stderr",
    ),
    pytest.param(
        16,
        {
            "provider": "ssh-k3s",
            "command": "install",
            "host": "10.0.0.5",
            "rc": -1,
            "stderr": "",
            "timed_out": True,
            "binary_missing": False,
            "observing_infra": False,
        },
        TransientError,
        ErrorCode.ENDPOINT_UNREACHABLE,
        id="row16-ssh-k3s-timeout",
    ),
    pytest.param(
        17,
        {
            "provider": "ssh-k3s",
            "command": "install",
            "host": "10.0.0.5",
            "rc": 1,
            "stderr": "curl: unknown flag --bogus",
            "timed_out": False,
            "binary_missing": False,
            "observing_infra": False,
        },
        PermanentError,
        ErrorCode.SCRIPT_FAILED,
        id="row17-ssh-k3s-other-nonzero",
    ),
    pytest.param(
        24,
        {
            "provider": "kind",
            "command": "create",
            "host": "docker_host",
            "rc": 1,
            "stderr": "ERROR: failed to create cluster: node(s) already exist",
            "timed_out": False,
            "binary_missing": False,
            "observing_infra": True,
        },
        PermanentError,
        ErrorCode.SCRIPT_FAILED,
        id="row24-kind-create-nonzero",
    ),
]


@pytest.mark.parametrize(("row", "kwargs", "expected_type", "expected_code"), SUBPROCESS_CASES)
def test_classify_subprocess_decision_table(row, kwargs, expected_type, expected_code):
    err = classify_subprocess(**kwargs)
    assert isinstance(err, expected_type), f"row {row}: expected {expected_type}, got {type(err)}"
    assert err.code == expected_code, f"row {row}: expected code {expected_code}, got {err.code}"
    assert err.provider == kwargs["provider"]
    assert err.command == kwargs["command"]


def test_classify_subprocess_unreachable_carries_host():
    err = classify_subprocess(
        provider="tart",
        command="clone",
        host="localhost",
        rc=-1,
        stderr="",
        timed_out=False,
        binary_missing=True,
        observing_infra=True,
    )
    assert isinstance(err, InfrastructureUnreachableError)
    assert err.host == "localhost"


def test_classify_subprocess_transient_never_conflated_with_unreachable():
    """Same connectivity symptom, non-infra-observing caller (SSH to a booting guest)
    ⇒ Transient, never Unreachable — the observing_infra split (crown jewel #1's
    'never conflate absence with unreachable' extends to 'never conflate an ordinary
    retry with an epistemic unreachable')."""
    err = classify_subprocess(
        provider="ssh-k3s",
        command="probe",
        host="10.0.0.5",
        rc=255,
        stderr="connection refused",
        timed_out=False,
        binary_missing=False,
        observing_infra=False,
    )
    assert isinstance(err, TransientError)
    assert not isinstance(err, InfrastructureUnreachableError)


def test_classify_subprocess_clean_nonzero_is_authoritative_permanent():
    err = classify_subprocess(
        provider="kubectl",
        command="apply",
        host="https://apiserver.example",
        rc=1,
        stderr="error validating data: unknown field",
        timed_out=False,
        binary_missing=False,
        observing_infra=True,
    )
    assert isinstance(err, PermanentError)
    assert err.code == ErrorCode.SCRIPT_FAILED
    assert err.detail["stderr"] == "error validating data: unknown field"
    assert err.detail["exit_code"] == "1"


def test_k3s_installer_download_failure_is_transient_not_permanent():
    """The VERBATIM stderr of the 2026-08-12 tart run's failed provision. `k3s.install`
    runs `curl -sfL https://get.k3s.io | sudo sh -` inside the guest; the installer
    exits 1 for a failed GitHub release download exactly as it does for a bad flag, so
    this fell through to "clean non-zero exit => Permanent". `retry: ssh_default` was
    declared on that step and never fired (attempt=1), `on_failure: compensate`
    destroyed the VM, and the provision died in 35s over a download that succeeded on
    three later hand-run attempts.

    Transient, NOT InfrastructureUnreachableError: we reached the guest fine (the ssh
    gate and the host-key scan both passed first). It is the guest's own download that
    failed, which is an ordinary retry, not "cannot determine state"."""
    stderr = (
        "Warning: Identity file /Users/kezia/.ssh/id_ed25519 not accessible: "
        "No such file or directory.\n[ERROR]  Download failed"
    )
    err = classify_subprocess(
        provider="ssh-k3s",
        command="install_k3s",
        host="192.168.65.21",
        rc=1,
        stderr=stderr,
        timed_out=False,
        binary_missing=False,
        observing_infra=False,
    )
    assert isinstance(err, TransientError)
    assert not isinstance(err, InfrastructureUnreachableError)
    assert not isinstance(err, PermanentError)
    assert err.detail["stderr"] == stderr


@pytest.mark.parametrize("phrase", sorted(TRANSIENT_STDERR_PHRASES))
def test_every_transient_phrase_is_detected(phrase):
    err = classify_subprocess(
        provider="kind",
        command="docker_inspect",
        host="docker_host",
        rc=1,
        stderr=f"some prefix: {phrase} some suffix",
        timed_out=False,
        binary_missing=False,
        observing_infra=True,
    )
    assert isinstance(err, InfrastructureUnreachableError)


def test_tart_not_found_phrases_exported_but_not_consulted_by_classify_subprocess():
    """Row 7: TartNotFound is caller-pre-mapped absence-as-data, never routed through
    classify_subprocess — so a TART_NOT_FOUND_PHRASES-only stderr with no connectivity
    phrase still lands as an authoritative PermanentError(SCRIPT_FAILED), never
    Unreachable and never a Result. The caller is responsible for checking
    TART_NOT_FOUND_PHRASES *before* ever calling classify_subprocess."""
    phrase = next(iter(TART_NOT_FOUND_PHRASES))
    assert phrase not in TRANSIENT_STDERR_PHRASES
    err = classify_subprocess(
        provider="tart",
        command="delete",
        host="localhost",
        rc=1,
        stderr=f"Error: {phrase}",
        timed_out=False,
        binary_missing=False,
        observing_infra=True,
    )
    assert isinstance(err, PermanentError)
    assert err.code == ErrorCode.SCRIPT_FAILED


# ============================================================================
# classify_http — decision table rows 10, 11, 12, 28, 32, 33, 35
# ============================================================================

HTTP_CASES = [
    pytest.param(
        11,
        {"provider": "digitalocean", "command": "create_droplet", "host": "api.digitalocean.com", "status": 401},
        PermanentError,
        ErrorCode.AUTH,
        id="row11-do-401",
    ),
    pytest.param(
        11,
        {"provider": "digitalocean", "command": "create_droplet", "host": "api.digitalocean.com", "status": 403},
        PermanentError,
        ErrorCode.AUTH,
        id="row11-do-403",
    ),
    pytest.param(
        12,
        {"provider": "digitalocean", "command": "list_droplets", "host": "api.digitalocean.com", "status": 429},
        TransientError,
        ErrorCode.RATE_LIMITED,
        id="row12-do-429",
    ),
    pytest.param(
        28,
        {"provider": "kubectl", "command": "get_pods", "host": "https://apiserver.example", "status": 401},
        PermanentError,
        ErrorCode.AUTH,
        id="row28-kubectl-401",
    ),
    pytest.param(
        32,
        {"provider": "ghcr", "command": "list_tags", "host": "ghcr.io", "status": 401},
        PermanentError,
        ErrorCode.AUTH,
        id="row32-ghcr-401",
    ),
    pytest.param(
        33,
        {"provider": "ghcr", "command": "list_tags", "host": "ghcr.io", "status": 403, "rate_limited": True, "retry_after": 30.0},
        TransientError,
        ErrorCode.RATE_LIMITED,
        id="row33-ghcr-403-rate-limit",
    ),
    pytest.param(
        35,
        {"provider": "ghcr", "command": "list_tags", "host": "ghcr.io", "status": 503},
        TransientError,
        ErrorCode.API_5XX,
        id="row35-ghcr-5xx",
    ),
]


@pytest.mark.parametrize(("row", "kwargs", "expected_type", "expected_code"), HTTP_CASES)
def test_classify_http_decision_table(row, kwargs, expected_type, expected_code):
    err = classify_http(**kwargs)
    assert isinstance(err, expected_type), f"row {row}: expected {expected_type}, got {type(err)}"
    assert err.code == expected_code, f"row {row}: expected code {expected_code}, got {err.code}"


def test_classify_http_rate_limit_carries_retry_after():
    err = classify_http(
        provider="ghcr", command="list_tags", host="ghcr.io", status=403, rate_limited=True, retry_after=30.0
    )
    assert isinstance(err, TransientError)
    assert err.retry_after == 30.0


def test_classify_http_malformed_body_machine_provider_is_unreachable():
    """Row 10: DO garbage/empty JSON body ⇒ Unreachable/MALFORMED_RESPONSE (v1: 'treat
    like timeout'). observing_infra=True marks 'machine provider'."""
    err = classify_http(
        provider="digitalocean",
        command="get_droplet",
        host="api.digitalocean.com",
        status=200,
        malformed_body=True,
        observing_infra=True,
    )
    assert isinstance(err, InfrastructureUnreachableError)
    assert err.code == ErrorCode.MALFORMED_RESPONSE
    assert err.host == "api.digitalocean.com"


def test_classify_http_malformed_body_service_is_transient():
    """Row 35: GHCR JSON garbage ⇒ Transient (service: never Unreachable)."""
    err = classify_http(
        provider="ghcr",
        command="list_tags",
        host="ghcr.io",
        status=200,
        malformed_body=True,
        observing_infra=False,
    )
    assert isinstance(err, TransientError)
    assert not isinstance(err, InfrastructureUnreachableError)
    assert err.code == ErrorCode.MALFORMED_RESPONSE


def test_classify_http_408_is_transient():
    err = classify_http(provider="kubectl", command="get_pods", host="https://apiserver.example", status=408)
    assert isinstance(err, TransientError)


def test_classify_http_errors_never_look_like_absence():
    """404 is documented as never reaching classify_http (reads map it to
    absence-as-data first); if it somehow did, this module must never fabricate a
    Result-shaped return — it always returns a raised-type ProviderError."""
    err = classify_http(provider="cloudflare", command="get_record", host="api.cloudflare.com", status=404)
    assert isinstance(err, PermanentError)
    assert not hasattr(err, "found")
    assert not hasattr(err, "existed")
