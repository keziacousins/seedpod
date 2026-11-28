"""engine/steps/instance.py -- the ``instance.*`` verbs (DR-0055): a bare VM, no k3s, no kubeconfig.
``provision-instance-tart.yml`` also reuses ``infra.await_instance``, ``k3s.await_ssh`` and
``k3s.trust_host_keys``. ``instance.create`` is undone by ``DestroyBareInstance`` from the notes, or by
the VM's own name if no note landed. ``instance.destroy`` is not a gate: the command is synchronous and
idempotent, so a failure raises and the engine retries or reports it.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import ClassVar

from pydantic import BaseModel

from seedpod.core.errors import ErrorCode, PermanentError
from seedpod.core.instance_template import InstanceTemplate, build_boot_payload
from seedpod.data.repositories import InstanceRepository
from seedpod.data.uow import UnitOfWork
from seedpod.engine.provider_step import ProviderStep
from seedpod.engine.step import EmptyOutput, Step, StepContext
from seedpod.engine.steps.cluster import SshIdentity
from seedpod.engine.steps.infra import _expect
from seedpod.engine.steps.k3s import _target
from seedpod.engine.steps.late_bound import LateBoundProviderStep
from seedpod.providers.contract import (
    BareInstanceCreated,
    BootInstance,
    CreateBareInstance,
    DestroyBareInstance,
    DestroyOutcome,
    DestroyStatus,
)

__all__ = ["InstanceBoot", "InstanceCreate", "InstanceDestroy", "InstanceLoadInfra", "InstanceLoadSpec"]

# A bare instance's tags (decision 2): NEVER `seedpod-managed`, `cluster-{slug}` or `cluster-uuid:{id}`,
# or a provider's cluster discovery would adopt the VM as an unmanaged cluster.
_TAGS = ("seedpod-instance",)


def _refuse(verb: str, message: str, code: ErrorCode = ErrorCode.INVALID_INPUT) -> PermanentError:
    return PermanentError(f"{verb}: {message}", code=code, provider="engine", command=verb)


class LoadSpecParams(BaseModel):
    instance_id: str


class LoadSpecOutput(BaseModel):
    provider: str
    slug: str
    image: str
    vcpu: int
    memory_mb: int
    disk_gb: int | None
    provider_size: str | None = None
    region: str | None = None
    entrypoint: str
    ssh_user: str
    ssh_private_key_path: str
    ssh_public_key: str


class InstanceLoadSpec(Step[LoadSpecParams, LoadSpecOutput]):
    verb = "instance.load_spec"
    Params = LoadSpecParams
    Output = LoadSpecOutput

    def __init__(
        self,
        *,
        uow: UnitOfWork,
        instances: InstanceRepository,
        templates: Mapping[str, InstanceTemplate],
        ssh_identities: Mapping[str, SshIdentity],
    ) -> None:
        """The private key is ``cluster.load_spec``'s (DR-0023); the user is the template's. The
        public key is the requester's, recorded on the row at admission."""
        self._uow, self._instances, self._templates = uow, instances, templates
        self._ssh_identities = ssh_identities

    async def execute(self, params: LoadSpecParams, ctx: StepContext) -> LoadSpecOutput:
        async with self._uow() as tx:
            row = self._instances.get(tx, params.instance_id)
        if row is None:
            raise _refuse(self.verb, f"instance {params.instance_id!r} not found", ErrorCode.NOT_FOUND)
        template = self._templates.get(row.template)
        if template is None or template.provider != row.provider:
            raise _refuse(self.verb, f"no {row.provider!r} instance template {row.template!r}")
        size = template.sizes.get(row.size)
        if size is None:
            raise _refuse(self.verb, f"template {row.template!r} has no size {row.size!r}")
        key_path = self._ssh_identities.get(row.provider, SshIdentity()).private_key_path
        public_key = row.ssh_public_key
        if key_path is None or public_key is None:
            raise _refuse(self.verb, f"provider {row.provider!r} has no SSH key, or instance {row.id!r} has no public key")
        try:
            build_boot_payload(ssh_public_key=public_key)  # fail here, before any VM exists
        except ValueError as e:
            raise _refuse(self.verb, str(e)) from e
        return LoadSpecOutput(
            provider=row.provider, slug=row.slug, image=template.image, vcpu=size.vcpu, memory_mb=size.memory_mb,
            disk_gb=size.disk_gb, provider_size=size.provider_size, region=template.region,
            entrypoint=template.entrypoint, ssh_user=template.ssh_user,
            ssh_private_key_path=key_path, ssh_public_key=public_key,
        )


class CreateParams(BaseModel):
    provider: str
    instance_id: str
    slug: str
    image: str
    vcpu: int
    memory_mb: int
    disk_gb: int | None
    provider_size: str | None = None
    region: str | None = None


class CreateOutput(BaseModel):
    resource_ids: Mapping[str, str]
    address: str | None = None
    adopted_existing: bool = False


class InstanceCreate(LateBoundProviderStep[CreateParams, CreateOutput]):
    verb = "instance.create"
    Params = CreateParams
    Output = CreateOutput
    thin = True
    idempotent = False  # as infra.create_instance: a crash mid-create fails the run, and compensation destroys by name

    def command(self, params: CreateParams) -> CreateBareInstance:
        return CreateBareInstance(
            instance_id=params.instance_id, slug=params.slug, image=params.image, vcpu=params.vcpu,
            memory_mb=params.memory_mb, disk_gb=params.disk_gb, tags=_TAGS,
            provider_size=params.provider_size, region=params.region,
        )

    def output_from(self, value: object) -> CreateOutput:
        created = _expect(value, BareInstanceCreated, verb=self.verb)
        return CreateOutput(
            resource_ids=created.resource_ids, address=created.address, adopted_existing=created.adopted_existing
        )


class BootParams(BaseModel):
    host: str
    known_hosts: str
    ssh_user: str
    ssh_private_key_path: str
    ssh_public_key: str
    entrypoint: str


class InstanceBoot(ProviderStep[BootParams, EmptyOutput]):
    verb = "instance.boot"
    provider_name: ClassVar[str] = "ssh-k3s"
    Params = BootParams
    Output = EmptyOutput
    undoable = False  # destroying the VM (instance.create's undo) is the compensation
    thin = True
    idempotent = False  # the entrypoint is the template's: seedpod cannot know it is safe to run twice

    def command(self, params: BootParams) -> BootInstance:
        try:
            payload = build_boot_payload(ssh_public_key=params.ssh_public_key)
        except ValueError as e:
            raise _refuse(self.verb, str(e)) from e
        return BootInstance(
            ssh=_target(params.host, params.ssh_user, params.ssh_private_key_path, verb=self.verb),
            known_hosts=params.known_hosts, payload=payload, entrypoint=params.entrypoint,
        )

    def output_from(self, value: object) -> EmptyOutput:
        return EmptyOutput()


class LoadInfraOutput(BaseModel):
    provider: str
    slug: str
    resource_ids: Mapping[str, str]


class InstanceLoadInfra(Step[LoadSpecParams, LoadInfraOutput]):
    verb = "instance.load_infra"
    Params = LoadSpecParams
    Output = LoadInfraOutput

    def __init__(self, *, uow: UnitOfWork, instances: InstanceRepository) -> None:
        self._uow, self._instances = uow, instances

    async def execute(self, params: LoadSpecParams, ctx: StepContext) -> LoadInfraOutput:
        async with self._uow() as tx:
            row = self._instances.get(tx, params.instance_id)
        if row is None:
            raise _refuse(self.verb, f"instance {params.instance_id!r} not found", ErrorCode.NOT_FOUND)
        return LoadInfraOutput(provider=row.provider, slug=row.slug, resource_ids=dict(row.resource_ids or {}))


class DestroyParams(BaseModel):
    provider: str
    instance_id: str
    slug: str
    resource_ids: Mapping[str, str]


class InstanceDestroy(LateBoundProviderStep[DestroyParams, EmptyOutput]):
    verb = "instance.destroy"
    Params = DestroyParams
    Output = EmptyOutput
    undoable = False  # destroying a destroy is not a thing
    thin = True

    def command(self, params: DestroyParams) -> DestroyBareInstance:
        return DestroyBareInstance(instance_id=params.instance_id, slug=params.slug, resource_ids=params.resource_ids)

    def output_from(self, value: object) -> EmptyOutput:
        # Unreachable parks the run (never caught here); only a DESTROYED the provider returned succeeds.
        outcome = _expect(value, DestroyOutcome, verb=self.verb)
        if outcome.status is not DestroyStatus.DESTROYED:
            raise _refuse(self.verb, f"provider answered {outcome.status}", ErrorCode.SCRIPT_FAILED)
        return EmptyOutput()
