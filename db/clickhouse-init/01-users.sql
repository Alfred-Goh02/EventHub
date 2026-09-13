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
