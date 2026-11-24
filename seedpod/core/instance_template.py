"""The shape of an instance template, and the pure rules for boot data (DR-0055 decisions 5 and 8).

A template is the ONLY source of an image: a request names a template and a size from its list, and
never supplies an image or an entrypoint. ``app/factory.py``'s ``load_instance_templates`` reads the
files. A size's hourly price and a template's lease bounds are what admission reads
(``api/routers/instances.py``). No IO.
"""

from __future__ import annotations

import json
import re

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

__all__ = [
    "BOOT_PAYLOAD_DIR",
    "BOOT_PAYLOAD_PATH",
    "InstanceSize",
    "InstanceTemplate",
    "build_boot_payload",
    "validate_entrypoint",
]

# Fixed by seedpod: the entrypoint is handed this path as its one argument.
BOOT_PAYLOAD_DIR = "/etc/seedpod-boot"
BOOT_PAYLOAD_PATH = f"{BOOT_PAYLOAD_DIR}/payload.json"

# An absolute path of plain components: no spaces, no shell characters, no `.`/`..` component.
_ENTRYPOINT = re.compile(r"(?:/[A-Za-z0-9_][A-Za-z0-9_.+-]*)+")
# ONE authorized_keys line. `[ -~]` is printable ASCII, so a newline (a second key) cannot match.
_SSH_KEY = re.compile(
    r"(?:ssh-ed25519|ssh-rsa|ecdsa-sha2-nistp(?:256|384|521)|sk-ssh-ed25519@openssh\.com) "
    r"[A-Za-z0-9+/]+={0,3}(?: [ -~]*)?"
)


# These two reach a command line as arguments, so neither may start with `-`.
_SSH_USER = re.compile(r"[A-Za-z_][A-Za-z0-9_.-]*")
_IMAGE = re.compile(r"[A-Za-z0-9][A-Za-z0-9._/:@-]*")


def _validate_ssh_user(value: str) -> str:
    if len(value) > 64 or _SSH_USER.fullmatch(value) is None:
        raise ValueError(f"{value!r} is not an SSH user")
    return value


def _validate_image(value: str) -> str:
    if len(value) > 255 or _IMAGE.fullmatch(value) is None:
        raise ValueError(f"{value!r} is not an image reference")
    return value


def validate_entrypoint(path: str) -> str:
    if not isinstance(path, str) or len(path) > 255 or _ENTRYPOINT.fullmatch(path) is None:
        raise ValueError(f"{path!r} is not an entrypoint: use an absolute path of letters, digits and _.+- only")
    return path


def build_boot_payload(*, ssh_public_key: str) -> bytes:
    """The payload: one JSON object holding the requester's SSH public key and no secret. Boot
    secrets wait for encrypted run inputs (DR-0055 decision 8)."""
    if not isinstance(ssh_public_key, str) or len(ssh_public_key) > 8192 or _SSH_KEY.fullmatch(ssh_public_key) is None:
        raise ValueError("not a single-line OpenSSH public key")
    return json.dumps({"ssh_public_key": ssh_public_key}, sort_keys=True).encode()


class InstanceSize(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    vcpu: int = Field(gt=0)
    memory_mb: int = Field(gt=0)
    disk_gb: int | None = Field(default=None, gt=0)  # None leaves the image's disk
    hourly_price: float | None = Field(default=None, ge=0)  # None: admission refuses the size (decision 6)


class InstanceTemplate(BaseModel):
    """``config/instance-templates/{name}.yml``. ``ssh_user`` is the user seedpod logs in as;
    ``entrypoint`` is the program, inside the image, that consumes the boot payload."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    provider: str
    image: str
    ssh_user: str
    entrypoint: str
    sizes: dict[str, InstanceSize] = Field(min_length=1)
    lease_seconds: int = Field(gt=0)  # the default lease (decision 7)
    max_lifetime_seconds: int = Field(gt=0)  # `hard_expires_at` is this long after creation

    _entrypoint_is_safe = field_validator("entrypoint")(validate_entrypoint)
    _ssh_user_is_safe = field_validator("ssh_user")(_validate_ssh_user)
    _image_is_safe = field_validator("image")(_validate_image)

    @model_validator(mode="after")
    def _lease_fits_lifetime(self) -> InstanceTemplate:
        if self.max_lifetime_seconds < self.lease_seconds:
            raise ValueError("max_lifetime_seconds must be at least lease_seconds")
        return self
