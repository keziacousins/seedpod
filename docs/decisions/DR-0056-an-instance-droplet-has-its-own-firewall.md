---
title: DR-0056 — a DigitalOcean instance has its own tag firewall, an explicit droplet size, and no cluster network
type: decision
status: active
created: 2026-11-28
updated: 2026-11-28
---

# DR-0056: an instance droplet has its own firewall

**Status: ACTIVE — ratified by Kezia, 2026-11-28.** It amends DR-0055 and closes its open item
"DigitalOcean inbound firewall".

## Context

DR-0055 puts a bare VM on DigitalOcean as a droplet tagged `seedpod-instance` and
`instance-uuid:{id}`. It does not say how that droplet is protected.

- The cluster firewalls (`seedpod-mgmt-{region}`, `seedpod-apps-{region}`) are attached to each cluster
  droplet by id, and their internal rules trust the `seedpod-managed` tag. An instance has neither.
- A droplet with no firewall accepts traffic on every port.
- Cluster sizes come from a closest match on CPU and memory. A template states an hourly price, so the
  droplet that is billed must be the one the template names.

## Decision

**1. One firewall for all instances, attached by tag.** Its name is `seedpod-instances`. It applies to
the tag `seedpod-instance`, so DigitalOcean attaches it when the droplet is created, before the droplet
boots.

- Inbound: TCP 22 only, from `ssh_access.allowed_sources` in `config/providers/digitalocean.yml`. This
  is the same list the cluster management firewall uses. One list controls SSH to everything seedpod
  makes.
- Outbound: all traffic.
- `CreateBareInstance` makes sure the firewall exists and has these rules before it creates the droplet.
  If the firewall is missing, seedpod creates it. If its rules differ, seedpod replaces them. If this
  fails, the create fails. An instance is never created without the firewall.

**2. A DigitalOcean template names its droplet size.** Each size in the template has `provider_size`,
for example `s-2vcpu-4gb`. A DigitalOcean template that has a size with no `provider_size` is refused when
the templates load. A template can also name a `region`. Without one, the provider's default region is
used. There is no closest-match fallback.

**3. An instance is not in the cluster network.** It does not join the `seedpod-vpc-{region}` VPC. It
uses the region's default network. It does not get the provider's `default_tags`, because those include
`seedpod-managed`, which the cluster firewalls trust and which DigitalOcean cluster reconcile acts on.

**4. Identity is the tag, so creation never adopts another instance's droplet.** A repeated create
finds its own droplet by `instance-uuid:{id}`. A droplet with that tag can only belong to this instance.
The tart adoption gap (DR-0055, a reused slug after database loss) does not exist on DigitalOcean.

**5. Destroy acts only on the instance's own droplet.** Destroy reads the droplet first. If the droplet
does not carry `instance-uuid:{id}`, destroy refuses it. A 404 on that read is "destroyed". A failed
read is never "destroyed".

## Consequences

- SSH to instances is as open as SSH to clusters. Today the shipped list is `0.0.0.0/0`. Narrowing it
  narrows both.
- An instance can reach the internet, and other droplets in the account through their public addresses.
  As DR-0055 says, an instance is not a containment boundary.
- Instance droplets are not assigned to the configured DigitalOcean project. They appear in the
  account's default project.
- The template image on DigitalOcean is a droplet image slug or a snapshot id, and it must contain the
  template's entrypoint. Seedpod does not build that image.

## Not decided here

- A separate SSH source list for instances, if crew hosts join the network in a way that differs from
  clusters.
- Project assignment for instance droplets.
