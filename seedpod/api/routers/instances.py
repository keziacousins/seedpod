"""``/api/instances`` -- a bare VM on a template's image (DR-0055 decisions 5 to 9).

Thin HTTP over the ``Dispatcher``: admission (template, size, lease, slug, limits) and the birth
share ONE ``app.uow()`` transaction, so two requests cannot both take the last slot. Every other
state change is a command through ``Dispatcher.apply()`` in the transaction that read the row.

Visibility (decision 9): a key without ``*`` sees and acts only on instances it owns, and another
key's instance is 404, not 403. DR-0054's environment rule applies on top, to ``*`` keys too (403,
as for clusters). The request body forbids unknown fields, so a boot secret is a 422: boot secrets
wait for encrypted run inputs (decision 8). No response carries ``known_hosts``, ``ssh_public_key``
or ``resource_ids``.
"""

from __future__ import annotations

import re
from datetime import timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, Field, field_validator

from seedpod.api.auth import require_permission
from seedpod.api.deps import get_app
from seedpod.core.environment_scope import check_environment, environment_admits
from seedpod.core.errors import PermanentError
from seedpod.core.events import (
    InstanceCreateRequested,
    InstanceDestroyRequested,
    InstanceRenewRequested,
)
from seedpod.core.instance_limits import refusal
from seedpod.core.instance_template import build_boot_payload
from seedpod.core.machine import InvalidTransition
from seedpod.data.repositories import ApiKeyRow, InstanceRow

__all__ = ["router"]

router = APIRouter(tags=["instances"])

# What `_slugify` (deployment_service.py) makes for a cluster: lowercase runs of letters and digits
# joined by single hyphens, at most 40 characters and a `-{8 characters}` suffix. A slug is part of
# a tart VM name and a command line, so a request may not use anything wider.
_SLUG = r"^[a-z0-9]+(-[a-z0-9]+)*$"  # pydantic searches, so the anchors are needed
_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")


class CreateInstanceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    slug: str = Field(pattern=_SLUG, max_length=49)
    template: str
    size: str
    environment: str = Field(min_length=1)
    ssh_public_key: str
    lease_seconds: int | None = Field(default=None, gt=0)

    @field_validator("ssh_public_key")
    @classmethod
    def _one_openssh_key(cls, value: str) -> str:
        build_boot_payload(ssh_public_key=value)  # the boot step's own rule; raises ValueError -> 422
        return value

    @field_validator("slug")
    @classmethod
    def _not_an_id(cls, value: str) -> str:
        # Routes take an id or a slug, id first. A slug shaped like an id could name another
        # key's instance, so a caller acting "by slug" would act on that instance instead.
        if _UUID.fullmatch(value):
            raise ValueError("a slug may not have the shape of an instance id")
        return value


def _iso(value) -> str | None:
    return value.isoformat() if value is not None else None


def _serialize(row: InstanceRow) -> dict[str, Any]:
    return {
        "id": row.id,
        "slug": row.slug,
        "provider": row.provider,
        "environment": row.environment,
        "template": row.template,
        "size": row.size,
        "status": row.status,
        "address": row.address,
        "owner_key_id": row.owner_key_id,
        "lease_seconds": row.lease_seconds,
        "expires_at": _iso(row.expires_at),
        "hard_expires_at": _iso(row.hard_expires_at),
        "hourly_price": row.hourly_price,
        "vcpu": row.vcpu,
        "memory_mb": row.memory_mb,
        "failure_reason": row.failure_reason,
        "created_at": _iso(row.created_at),
        "updated_at": _iso(row.updated_at),
        "destroyed_at": _iso(row.destroyed_at),
    }


def _owns(api_key: ApiKeyRow, row: InstanceRow) -> bool:
    return "*" in api_key.permissions or row.owner_key_id == api_key.id


def _in_scope(app: Any, tx: Any, api_key: ApiKeyRow, id_or_slug: str) -> InstanceRow:
    """The instance, if this key may see it: 404 when it does not exist or is another key's (the
    two look the same), then 403 (``EnvironmentNotAdmitted``) when its environment is not the key's."""
    row = app.repos.instances.get(tx, id_or_slug) or app.repos.instances.get_by_slug(tx, id_or_slug)
    if row is None or not _owns(api_key, row):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"instance {id_or_slug!r} not found")
    check_environment(api_key.environment, row.environment, subject=f"instance {row.id}")
    return row


@router.post("/instances", status_code=status.HTTP_201_CREATED)
async def create_instance(
    body: CreateInstanceRequest,
    request: Request,
    api_key=Depends(require_permission("instances:create")),  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    template = app.instance_templates.get(body.template)
    if template is None:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, f"unknown template {body.template!r}")
    size = template.sizes.get(body.size)
    if size is None:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, f"template {body.template!r} has no size {body.size!r}")
    if template.provider not in app.providers:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, f"provider {template.provider!r} is not configured")
    if size.hourly_price is None:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, f"size {body.size!r} has no hourly price")
    lease = body.lease_seconds or template.lease_seconds
    if lease > template.max_lifetime_seconds:
        raise HTTPException(
            status.HTTP_422_UNPROCESSABLE_CONTENT, f"lease_seconds must be at most {template.max_lifetime_seconds}"
        )
    try:  # the check key creation uses: a real environment name, so "all" and typos are refused
        app.crypto.key_class_for_environment(body.environment)
    except PermanentError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, f"unknown environment {body.environment!r}") from exc
    check_environment(api_key.environment, body.environment, subject=f"environment {body.environment!r}")

    now = app.clock.now()
    row = InstanceRow(
        id=app.id_gen(), slug=body.slug, provider=template.provider, environment=body.environment,
        template=body.template, size=body.size, status="new", version=0, owner_key_id=api_key.id,
        resource_ids={}, address=None, known_hosts=None, lease_seconds=lease,
        expires_at=now + timedelta(seconds=lease),
        hard_expires_at=now + timedelta(seconds=template.max_lifetime_seconds),
        hourly_price=size.hourly_price, vcpu=size.vcpu, memory_mb=size.memory_mb, failure_reason=None,
        created_at=now, updated_at=now, destroyed_at=None, ssh_public_key=body.ssh_public_key,
    )
    async with app.uow() as tx:  # one transaction: the slug and limit checks, and the birth
        if app.repos.instances.get_by_slug(tx, body.slug) is not None:
            raise HTTPException(status.HTTP_409_CONFLICT, f"slug {body.slug!r} is taken (a slug is never reused)")
        limit = refusal(
            app.instance_limits, app.repos.instances.list_live(tx), key_id=api_key.id,
            environment=row.environment, provider=row.provider, vcpu=row.vcpu, memory_mb=row.memory_mb,
            hourly_price=row.hourly_price,
        )
        if limit is not None:
            raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS, f"instance limit exceeded: {limit}")
        event = InstanceCreateRequested(at=now, actor=f"api:{api_key.username}")
        await app.dispatcher.apply("instance", row.id, event, record=row, tx=tx)
        row = app.repos.instances.get(tx, row.id)
    return _serialize(row)


@router.get("/instance-templates")
async def list_instance_templates(
    request: Request, api_key=Depends(require_permission("instances:read"))  # noqa: B008
) -> dict[str, Any]:
    """What a create form offers: the templates whose provider this app has, and their sizes. Never the
    image, entrypoint, SSH user or provider size (a template's internals stay in the config)."""
    app = get_app(request)
    return {
        "templates": [
            {
                "name": name, "provider": t.provider, "region": t.region, "lease_seconds": t.lease_seconds,
                "max_lifetime_seconds": t.max_lifetime_seconds,
                "sizes": [
                    {"name": n, "vcpu": z.vcpu, "memory_mb": z.memory_mb, "disk_gb": z.disk_gb, "hourly_price": z.hourly_price}
                    for n, z in t.sizes.items()
                ],
            }
            for name, t in sorted(app.instance_templates.items())
            if t.provider in app.providers
        ]
    }


@router.get("/instances")
async def list_instances(
    request: Request, api_key=Depends(require_permission("instances:read"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    async with app.uow() as tx:
        rows = app.repos.instances.list_all(tx)
    rows = [r for r in rows if _owns(api_key, r) and environment_admits(api_key.environment, r.environment)]
    return {"instances": [_serialize(r) for r in rows]}


@router.get("/instances/{id_or_slug}")
async def get_instance(
    id_or_slug: str, request: Request, api_key=Depends(require_permission("instances:read"))  # noqa: B008
) -> dict[str, Any]:
    app = get_app(request)
    async with app.uow() as tx:
        return _serialize(_in_scope(app, tx, api_key, id_or_slug))


@router.get("/instances/{id_or_slug}/audit")
async def instance_audit(
    id_or_slug: str, request: Request, api_key=Depends(require_permission("instances:read"))  # noqa: B008
) -> dict[str, Any]:
    """The instance's state changes, oldest first. The same scope rules as ``GET`` one."""
    app = get_app(request)
    async with app.uow() as tx:
        rows = app.repos.instance_state_audits.list_for_instance(tx, _in_scope(app, tx, api_key, id_or_slug).id)
    return {
        "audit": [
            {
                "from_state": r.from_state, "to_state": r.to_state, "event": r.event, "actor": r.actor,
                "reason": r.reason, "created_at": _iso(r.created_at),
            }
            for r in reversed(rows)
        ]
    }


async def _command(app: Any, api_key: ApiKeyRow, id_or_slug: str, event_type: type) -> dict[str, Any]:
    """Look the instance up in scope, apply ``event_type`` to it, and return the row as it then is,
    all in one transaction. A refused transition is 409."""
    async with app.uow() as tx:
        row = _in_scope(app, tx, api_key, id_or_slug)
        try:
            await app.dispatcher.apply(
                "instance", row.id, event_type(at=app.clock.now(), actor=f"api:{api_key.username}"), tx=tx
            )
        except InvalidTransition as exc:
            raise HTTPException(status.HTTP_409_CONFLICT, str(exc)) from exc
        return _serialize(app.repos.instances.get(tx, row.id))


@router.post("/instances/{id_or_slug}/renew")
async def renew_instance(
    id_or_slug: str, request: Request, api_key=Depends(require_permission("instances:renew"))  # noqa: B008
) -> dict[str, Any]:
    return await _command(get_app(request), api_key, id_or_slug, InstanceRenewRequested)


@router.delete("/instances/{id_or_slug}", status_code=status.HTTP_202_ACCEPTED)
async def destroy_instance(
    id_or_slug: str, request: Request, api_key=Depends(require_permission("instances:delete"))  # noqa: B008
) -> dict[str, Any]:
    return await _command(get_app(request), api_key, id_or_slug, InstanceDestroyRequested)
