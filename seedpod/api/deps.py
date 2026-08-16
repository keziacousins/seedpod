"""``get_app`` -- the one FastAPI dependency every handler in this package uses to
reach the composition root (docs/design/seam-d-foundation.md Decision 8's
``api/deps.py`` excerpt; the "v1 global -> v2 injection point" table's last row:
"``api/dependencies.py`` late-import accessors + conftest's 5-seam override dance ->
one seam: ``api.state.app``").

``factory.build_app()`` stamps ``api.state.app = app`` immediately after
constructing the ``FastAPI`` instance (``seedpod/app/factory.py``, step 10) -- every
request handler in this package reaches ``App``/``Services``/``SSEHub``/``Repositories``/
``UnitOfWork`` fresh, per request, through this one function, never through a
module global or a constructor-time capture."""

from __future__ import annotations

from fastapi import Request

from seedpod.app.app import App

__all__ = ["get_app"]


def get_app(request: Request) -> App:
    return request.app.state.app
