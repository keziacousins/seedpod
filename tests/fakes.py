"""Deterministic test doubles for build_app's three seams (providers, clock, id_gen).

No Mock/patch anywhere in this suite — if a test seems to need one, the seam has
leaked; return the problem, don't work around it (CLAUDE.md testing posture).
"""

import itertools
import uuid


def sequential_ids():
    """Deterministic uuid4-shaped ids: ...-000000000001, -000000000002, ...

    The acceptance spec asserts 36-char dashed ids, so these must stay UUID-formatted.
    """
    counter = itertools.count(1)
    return lambda: str(uuid.UUID(int=next(counter)))


class FakeProvider:
    """Phase-0 placeholder. Pillar 3's conformance suite (tests/conformance/) brings
    the real fake + Harness; this must then satisfy the Provider protocol in
    seedpod/providers/contract.py.
    """

    async def check_ready(self) -> None:
        return None

    def execute(self, command):
        raise NotImplementedError("Pillar 3 conformance fakes replace this placeholder")
