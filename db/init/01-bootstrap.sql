-- Cluster bootstrap. Runs ONCE, on an empty data directory, via the Postgres
-- image's /docker-entrypoint-initdb.d hook.
--
-- This file exists so the cluster can be rebuilt from the repository. Anything
-- created by hand in psql is invisible to your teammates, to CI, and to you in
-- six months — and `docker compose down -v` erases it with no way back.
--
-- Roles are CLUSTER-wide; databases are not. So roles are created once here,
-- and per-database privileges are granted inside each database below.

-- ---------------------------------------------------------------------------
-- Roles: two per service.
--   *_owner  runs migrations and owns the tables. Needs CREATEDB because
--            `prisma migrate dev` builds a temporary shadow database to detect
--            drift; without CREATEDB it fails with P3014.
--   *_app    is what the running service connects as. Owns nothing, so RLS
--            policies actually apply to it. A table OWNER silently bypasses
--            its own policies, and a SUPERUSER bypasses them even with FORCE.
-- ---------------------------------------------------------------------------
CREATE ROLE eventhub_auth_owner  LOGIN CREATEDB PASSWORD 'eventhub_auth_owner';
CREATE ROLE eventhub_auth_app    LOGIN          PASSWORD 'eventhub_auth_app';

CREATE ROLE eventhub_event_owner LOGIN CREATEDB PASSWORD 'eventhub_event_owner';
CREATE ROLE eventhub_event_app   LOGIN          PASSWORD 'eventhub_event_app';

-- Drains the transactional outbox. Reads by delivery state rather than by
-- tenant, so it gets its own role and its own policy instead of the superuser.
CREATE ROLE eventhub_event_relay LOGIN          PASSWORD 'eventhub_event_relay';

-- ---------------------------------------------------------------------------
-- One database per service. No cross-database joins exist in Postgres, so the
-- service boundary is enforced by the engine rather than by discipline.
-- ---------------------------------------------------------------------------
CREATE DATABASE eventhub_auth  OWNER eventhub_auth_owner;
CREATE DATABASE eventhub_event OWNER eventhub_event_owner;

-- ---------------------------------------------------------------------------
-- Connection isolation. By default PUBLIC may CONNECT to any database, so the
-- Event service's credential can open a session against the Auth database and
-- enumerate its table names from pg_catalog. Table grants would still deny the
-- reads, but the boundary should hold at the connection, not one layer in.
-- ---------------------------------------------------------------------------
REVOKE CONNECT ON DATABASE eventhub_auth  FROM PUBLIC;
GRANT  CONNECT ON DATABASE eventhub_auth  TO eventhub_auth_owner, eventhub_auth_app;

REVOKE CONNECT ON DATABASE eventhub_event FROM PUBLIC;
GRANT  CONNECT ON DATABASE eventhub_event TO eventhub_event_owner, eventhub_event_app, eventhub_event_relay;

-- ---------------------------------------------------------------------------
-- Schema-level access, granted inside each database.
--
-- Deliberately NOT using ALTER DEFAULT PRIVILEGES here. It would grant the app
-- role SELECT/INSERT/UPDATE/DELETE on every future table automatically — but
-- RLS is per-table and Prisma never emits it, so a table whose migration forgot
-- its RLS block would be readable across tenants with no error anywhere.
-- Granting explicitly per table in each migration means a forgotten RLS block
-- is also a forgotten grant, and the service fails loudly with "permission
-- denied" instead of quietly serving another tenant's rows. Fail closed.
-- ---------------------------------------------------------------------------
\connect eventhub_auth
GRANT USAGE ON SCHEMA public TO eventhub_auth_app;

\connect eventhub_event
GRANT USAGE ON SCHEMA public TO eventhub_event_app, eventhub_event_relay;
