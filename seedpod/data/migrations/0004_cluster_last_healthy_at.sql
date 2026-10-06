-- 0004_cluster_last_healthy_at.sql — DR-0050 decision 3.
--
-- The health monitor skips a tick when it cannot determine a cluster's health: the
-- apiserver is out of reach, or kubectl failed in a way the classifier does not know.
-- That is correct, and it is silent. An ACTIVE cluster that seedpod has not been able
-- to see for a day looks exactly like one it confirmed a minute ago.
--
-- `last_healthy_at` is when a health probe last succeeded. Same kind of column as
-- `last_reconciled_at` (bookkeeping, written by one repo method, never read by the
-- state machine), and nullable for the same reason: NULL is "never confirmed", which
-- is every cluster that exists when this migration runs, until its next healthy tick.
ALTER TABLE clusters ADD COLUMN last_healthy_at TEXT;

PRAGMA user_version = 4;
