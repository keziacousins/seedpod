-- 0002_cluster_dns_record_id.sql — DR-0034 decision 4.
--
-- `clusters` already carried `dns_hostname`/`dns_zone` from 0001 and nothing ever
-- wrote either (backlog #22). The third field the destroy path needs — the
-- provider's own record id, which is what `DnsService.delete_record` is keyed on —
-- had no home at all, so `DnsRecordRef` was reading all three out of the
-- `provider_config` JSON blob (v1's storage shape) that v2 never populates.
--
-- The record is a provisioning OUTPUT, so it belongs beside the two columns that
-- already exist, not in the INPUTS blob. It deliberately does NOT go in
-- `provider_resources`: that whole map is bound wholesale into
-- `infra.destroy_instance`'s `resource_ids`, so a dns key there would be handed to
-- the machine provider as one of its own resources.
ALTER TABLE clusters ADD COLUMN dns_record_id TEXT;

PRAGMA user_version = 2;
