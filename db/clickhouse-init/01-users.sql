-- ClickHouse's first-boot script: the analytics database and its two users.
--
-- Runs once, by the image's entrypoint, when the data volume is empty — the same
-- hook db/init uses for Postgres. The admin user it runs as comes from
-- CLICKHOUSE_USER in docker-compose.yml and is used for nothing else.
--
-- TWO USERS, AND THE SPLIT IS PLAIN LEAST PRIVILEGE. In Postgres the owner/app
-- split exists because a table owner bypasses row-level security. ClickHouse has
-- nothing equivalent to bypass; the reason here is simpler: the running service
-- must not be able to DROP TABLE, and a compromised service credential must not
-- be able to rewrite history. Schema changes happen only through
-- analytics_migrator, and only the migration runner holds that credential.
--
-- Local passwords, like the Postgres roles in db/init. A real deployment injects
-- its own and never commits them.

CREATE DATABASE IF NOT EXISTS analytics;

CREATE USER IF NOT EXISTS analytics_migrator IDENTIFIED WITH sha256_password BY 'analytics_migrator';
GRANT ALL ON analytics.* TO analytics_migrator;

-- SELECT and INSERT, nothing else: no ALTER, no DROP, no TRUNCATE, no DELETE.
-- scripts/verify-analytics-schema.mjs asserts each refusal rather than trusting
-- this line, because a grant that silently failed to apply looks identical to
-- one that worked until something tries to exceed it.
CREATE USER IF NOT EXISTS analytics_app IDENTIFIED WITH sha256_password BY 'analytics_app';
GRANT SELECT, INSERT ON analytics.* TO analytics_app;

-- Part counts per table, for the ingest's /metrics: the number that says
-- inserts are about to be delayed (1000 active parts in a partition) and then
-- refused (3000). Four columns of system.parts and no more: the thresholds apply
-- per partition, so `partition` is one of them. The first version of this grant
-- had `rows` instead, and the metric's own query was refused on the column it
-- left out, found by running the ingest against it before writing a test.
--
-- No row policy is needed to keep other databases' tables out of view.
-- Measured on 25.8 with a user holding exactly these grants: system.parts showed
-- the five analytics tables and nothing else, while the admin saw system and
-- scratch databases too. ClickHouse lists only the parts of tables the user can
-- already see. Without this grant the same query is ACCESS_DENIED.
GRANT SELECT(database, table, partition, active) ON system.parts TO analytics_app;

-- And the two thresholds those counts are read against, so /metrics reports the
-- server's own limits rather than a copy that goes stale. Refused without this
-- grant, to analytics_app and analytics_migrator alike (measured).
GRANT SELECT(name, value) ON system.merge_tree_settings TO analytics_app;
