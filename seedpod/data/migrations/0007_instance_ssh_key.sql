-- 0007_instance_ssh_key.sql — DR-0055 decisions 8 and 9: the requester's SSH public key, on the row.
--
-- `instance.load_spec` reads it, and puts it in the boot payload. It is a public key, so it is
-- stored as plain text. NULL for a row born before this migration: `load_spec` refuses by name.
-- To reverse it: `ALTER TABLE instances DROP COLUMN ssh_public_key; PRAGMA user_version = 6;`

ALTER TABLE instances ADD COLUMN ssh_public_key TEXT;
PRAGMA user_version = 7;
