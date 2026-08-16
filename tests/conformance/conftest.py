"""tests/conformance/conftest.py — fixtures for the shared C-01..C-24 conformance suite
(docs/design/seam-c-provider.md §5.6).

Two parametrized fixtures cover every "Applies" column in the §5.6 table:

- ``harness``: all six providers (``digitalocean``, ``kind``, ``tart``, ``orbstack``,
  ``ssh-k3s``, ``kubectl``) — for rows marked "all".
- ``machine_harness``: just the four machine-plane providers — for rows marked "machine".

Each test gets a *fresh* harness instance (a fresh in-memory fake backend) per parametrized
case; nothing is shared across tests, so provider statelessness (C-03) isn't accidentally
masked by fixture reuse.
"""

from __future__ import annotations

import pytest

from tests.conformance._support import HARNESS_CLASSES, HARNESS_IDS, MACHINE_HARNESS_CLASSES

_MACHINE_IDS = tuple(cls.name for cls in MACHINE_HARNESS_CLASSES)


@pytest.fixture(params=HARNESS_CLASSES, ids=HARNESS_IDS)
def harness(request):
    return request.param()


@pytest.fixture(params=MACHINE_HARNESS_CLASSES, ids=_MACHINE_IDS)
def machine_harness(request):
    return request.param()
