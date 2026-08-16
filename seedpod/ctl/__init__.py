"""``seedpodctl`` -- the authenticated HTTP user CLI (docs/decisions/DR-0021
§0c/point 3). The third of v2's three trust-model entry points, and the only
one an everyday operator/CI job uses: a thin client over the SAME
authenticated API the SPA speaks (``seedpod/api/routers/*.py``), never a
direct database/filesystem tool.

**The trust boundary is structural, not conventional (DR-0021's rationale).**
Nothing under this package may import ``seedpod.data`` / ``seedpod.app.services``
/ ``seedpod.services.crypto`` / ``sqlalchemy``, or open a database connection --
every guard the server enforces (auth, permission scopes, the Dispatcher, the
state machine) is inherited for free by speaking only HTTP, exactly like the
SPA. ``tests/cli/test_seedpodctl.py`` asserts this import-graph shape directly,
in a fresh subprocess, so a future edit can't silently reintroduce a direct-DB
shortcut.

Zero import-time side effects (CLAUDE.md / DR-0021): importing this package,
or any module beneath it, reads no environment variable, opens no socket, and
makes no network call. Every effect happens inside ``seedpod.ctl.cli``'s
``main()`` / command functions.
"""

from __future__ import annotations

__all__: list[str] = []
