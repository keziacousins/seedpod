"""DR-0054: which environments an API key may act on.

An API key has an ``environment`` (``ApiKeyRow.environment``). ``'all'`` is the
unscoped sentinel. Any other value scopes the key to that one environment: the key
may read or change a resource only if the resource is in the same environment.
DR-0010 states the same rule for SSE (``runtime/sse.py``); this is its REST half.

The rule is about environments, not permissions. ``*`` grants every permission and
no environment: a ``*`` key scoped to ``staging`` cannot touch ``production``.

Pure: the API layer and the services both call it, so it lives here and not under
``seedpod/api/``. ``EnvironmentNotAdmitted`` is the API's 403
(``seedpod/api/factory.py``). It is an authorization refusal, not a provider
failure, so it is not part of the ``core/errors.py`` taxonomy -- the same
reasoning that keeps ``InvalidTransition`` in ``core/machine.py``.
"""

from __future__ import annotations

__all__ = ["ALL_ENVIRONMENTS", "EnvironmentNotAdmitted", "check_environment", "environment_admits"]

ALL_ENVIRONMENTS = "all"


class EnvironmentNotAdmitted(Exception):
    """A key scoped to one environment asked for a resource in another. The
    message names both, so the caller can see which key to use instead."""

    def __init__(self, key_environment: str, resource_environment: str, *, subject: str) -> None:
        super().__init__(
            f"key is scoped to environment {key_environment!r}; {subject} is in {resource_environment!r}"
        )
        self.key_environment = key_environment
        self.resource_environment = resource_environment
        self.subject = subject


def environment_admits(key_environment: str, resource_environment: str) -> bool:
    """``True`` iff the key is ``'all'``-scoped, or the two environments are equal."""
    return key_environment == ALL_ENVIRONMENTS or key_environment == resource_environment


def check_environment(key_environment: str, resource_environment: str, *, subject: str) -> None:
    """Raise ``EnvironmentNotAdmitted`` unless ``environment_admits``. ``subject``
    names the resource in the message, e.g. ``"cluster c1"``."""
    if not environment_admits(key_environment, resource_environment):
        raise EnvironmentNotAdmitted(key_environment, resource_environment, subject=subject)
