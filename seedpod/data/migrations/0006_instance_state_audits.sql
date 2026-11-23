-- 0006_instance_state_audits.sql — DR-0055 decision 1: the instance machine's audit trail.
--
-- One new table, one trigger, and one index swap. 0005 is not touched, and no existing table is
-- rebuilt, so this needs no `foreign_keys` dance. To reverse it, drop the trigger and the table,
-- and put the 0005 index back (this fails if two instances now share a slug, which 0005 allowed
-- only for destroyed ones):
--
--   DROP TRIGGER instances_hard_expires_at_is_fixed;
--   DROP TABLE instance_state_audits;
--   DROP INDEX ux_instances_slug;
--   CREATE UNIQUE INDEX ux_instances_slug_live ON instances(slug) WHERE status != 'destroyed';
--   PRAGMA user_version = 5;
--
-- An instance slug is NEVER reused, not even after `destroyed`. 0005 made the slug unique only
-- among instances that are not destroyed, which let a second instance take a destroyed one's
-- slug, and then a zombie (a destroyed row whose VM is still running) could not return to a live
-- state without breaking that index, and tart finds a VM by `spi-{slug}` so two live VMs could
-- have collided. DR-0055 decision 3 says unique among instances that are not destroyed. This is
-- stricter and compatible with it. The API derives a slug with an id suffix. 0005 has not
-- shipped, so no row exists that this could refuse.

-- `instance_state_audits` has the shape of `cluster_state_audits` (0001): one row per
-- non-Ignore transition, written by the Dispatcher in the same transaction as the Persist.
-- `actor` is `event.actor`, `created_at` is `event.at` (aware UTC 'Z'), `reason` is the event's
-- own `reason` field when it has one, and `context` is the full tagged event (DR-0007).
--
-- The trigger makes the hard limit a fact of the database as well as of the machine. DR-0055
-- decision 7: `hard_expires_at` is fixed at creation and never moves. No code writes it after
-- the birth INSERT, and this refuses the write if some later code does.

BEGIN;

DROP INDEX ux_instances_slug_live;
CREATE UNIQUE INDEX ux_instances_slug ON instances(slug);

CREATE TABLE instance_state_audits (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id  TEXT NOT NULL REFERENCES instances(id),
    from_state   TEXT NOT NULL,
    to_state     TEXT NOT NULL,
    event        TEXT NOT NULL,
    actor        TEXT NOT NULL,
    reason       TEXT,
    context      TEXT,
    created_at   TEXT NOT NULL
);
CREATE INDEX ix_isa_instance_time ON instance_state_audits(instance_id, created_at DESC);

CREATE TRIGGER instances_hard_expires_at_is_fixed
BEFORE UPDATE OF hard_expires_at ON instances
WHEN NEW.hard_expires_at IS NOT OLD.hard_expires_at
BEGIN
    SELECT RAISE(ABORT, 'instances.hard_expires_at is fixed at creation (DR-0055)');
END;

PRAGMA user_version = 6;
COMMIT;
