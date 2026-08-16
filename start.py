#!/usr/bin/env python3
"""Dev-convenience launcher (docs/decisions/DR-0021 §0a/point 1) -- matches v1's
repo-root location (``reference-code/seedpod/start.py``).

**This script is now a shim, and that is the point (DR-0041 Amendment B).** It
used to own three operational behaviours -- ``load_dotenv()``, a PID-file
singleton (``check_pid_file()``), and ``rotate_logs_on_startup()`` -- while
``seedpod/__main__.py``, the ``seedpod`` console script that a packaged artifact
actually ships, owned none of them. The packaged path was the *less* capable one:
an appliance got no ``.env``, no single-instance guard, and no per-boot log
separation, and nobody would notice until a second server was quietly serving
stale code (which is exactly what happened on 2026-08-13).

All three now live in ``seedpod/__main__.py:main()``, so both entry points get
identical behaviour from ONE implementation. The singleton in particular is no
longer this file's read-pid/``kill(pid, 0)``/unlink/write sequence -- that is
check-then-act, and an advisory file besides -- but a kernel-held ``flock``
(``seedpod/app/singleton.py``, which explains why at length).

What remains here is the only thing that was ever genuinely local: putting the
repo root on ``sys.path`` so ``python start.py`` works in a checkout with no
install step. Running the console script (``seedpod``) or ``python -m seedpod``
is equivalent in every other respect.

Guarded by ``if __name__ == "__main__"`` -- importing this module runs nothing.
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.absolute()


if __name__ == "__main__":
    sys.path.insert(0, str(PROJECT_ROOT))

    from seedpod.__main__ import main

    raise SystemExit(main())
