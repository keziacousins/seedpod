"""core/paths.py — the ONE home for "a config-relative path string, written as it
reads in the shipped YAML, resolved against the injected ``AppConfig.config_dir``".

Pure path arithmetic: no IO, no cwd read, no ``Path.resolve()`` (which would
consult the process's working directory). Lives in ``core/`` because two layers
that must not import each other both need it -- ``app/services/profiles.py``
(deployment profiles' ``manifests_dir``) and ``engine/steps/kube.py``
(``kube.apply_file``'s ``manifest_path``).

**The convention, stated once.** Every shipped path literal is written the way it
reads from the repo root -- ``config/manifest-templates/infrastructure/
traefik-kind.yaml`` -- while ``config_dir`` itself is conventionally *named*
``config`` (``AppConfig.config_dir``'s default) and is env-overridable
(``SEEDPOD_CONFIG_DIR``). The test fixture (``tests/conftest.py``'s
``test_config_dir``) copies the whole ``config/`` tree into a tmp dir whose ROOT
already plays the ``config`` role. So joining the raw value onto ``config_dir``
would double the ``config/`` segment, and joining it onto ``config_dir.parent``
would work only when the process cwd happens to be the repo root. Splitting the
difference -- strip ONE leading ``config`` segment, then join what remains onto
``config_dir`` -- is correct under both layouts and depends on cwd not at all.
``config_dir`` is thereby the single source of truth for where ``config/`` lives,
exactly as ``RuleEngine.load(config.config_dir / "deployment-rules.yml")`` already
treats it at the composition root.

Salvaged from ``app/services/profiles.py``'s ``_resolve_manifests_dir`` (which
now delegates here), whose own docstring reasoned this trap out first. Hoisted to
one home by the Round-8a gate's M-2 finding: ``kube.apply_file`` had reimplemented
the join as a bare ``Path(manifest_path).read_text()`` against the process cwd,
which -- combined with ``on_failure: continue`` on both shipped Traefik steps --
made ``provision-{kind,orbstack}`` "succeed" with no ingress controller whenever
the server ran from anywhere but the repo root. v1 was cwd-independent here
(``reference-code/seedpod/seedpod/core/paths.py``'s ``get_config_dir()``;
``reference-code/seedpod/seedpod/providers/orbstack.py``'s
``Path(__file__).parent.parent.parent / "config"``), so that was a silent
regression of behaviour v1 already got right -- CLAUDE.md's named #1 failure mode.
"""

from __future__ import annotations

from pathlib import Path, PurePosixPath

__all__ = ["resolve_under_config_dir"]


def resolve_under_config_dir(config_dir: Path, raw_value: str) -> Path:
    """``raw_value`` is a config-relative path as written in shipped YAML (POSIX
    separators; a leading ``config/`` segment optional). Returns it resolved
    against ``config_dir``.

    An ABSOLUTE ``raw_value`` is returned unchanged: an operator who writes an
    absolute path means it, and silently re-rooting it under ``config_dir``
    would produce a nonsense path rather than an honest error."""
    raw = raw_value.replace("\\", "/")
    if PurePosixPath(raw).is_absolute() or Path(raw_value).is_absolute():
        return Path(raw_value)
    parts = PurePosixPath(raw).parts
    if parts and parts[0] == "config":
        parts = parts[1:]
    return config_dir.joinpath(*parts) if parts else config_dir
