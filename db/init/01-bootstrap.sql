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

CREATE ROLE eventhub_ticketing_owner LOGIN CREATEDB PASSWORD 'eventhub_ticketing_owner';
CREATE ROLE eventhub_ticketing_app   LOGIN          PASSWORD 'eventhub_ticketing_app';

CREATE ROLE eventhub_live_owner LOGIN CREATEDB PASSWORD 'eventhub_live_owner';
CREATE ROLE eventhub_live_app   LOGIN          PASSWORD 'eventhub_live_app';

CREATE ROLE eventhub_notification_owner LOGIN CREATEDB PASSWORD 'eventhub_notification_owner';
CREATE ROLE eventhub_notification_app   LOGIN          PASSWORD 'eventhub_notification_app';

-- Drains Ticketing's own outbox. Same shape and same reasoning as the Event
-- relay: it reads by delivery state across every tenant, so it is a separate
-- role with its own policy rather than a reason to reach for the superuser.
CREATE ROLE eventhub_ticketing_relay LOGIN PASSWORD 'eventhub_ticketing_relay';
ALTER ROLE eventhub_ticketing_relay SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE eventhub_ticketing_relay SET statement_timeout = '15s';

-- Releases expired seat holds.
--
-- It gets its own role because it does something no request-scoped role should
-- be able to do: operate ACROSS tenants. A TTL sweep has no tenant context —
-- it selects by expiry, not by customer — so it cannot use the tenant-scoped
-- policy the app role uses.
--
-- A partial index cannot express "expired" either. now() is STABLE, not
-- IMMUTABLE, so `WHERE expiresAt > now()` is rejected outright:
--   ERROR: functions in index predicate must be marked IMMUTABLE
-- (verified). Expiry is therefore a swept state transition, not a view — which
-- is why this role exists at all.
CREATE ROLE eventhub_ticketing_sweeper LOGIN PASSWORD 'eventhub_ticketing_sweeper';
ALTER ROLE eventhub_ticketing_sweeper SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE eventhub_ticketing_sweeper SET statement_timeout = '15s';

-- Drains the transactional outbox. Reads by delivery state rather than by
-- tenant, so it gets its own role and its own policy instead of the superuser.
CREATE ROLE eventhub_event_relay LOGIN          PASSWORD 'eventhub_event_relay';

-- Bound the relay's transactions.
--
-- The drain holds a transaction open across a Kafka round-trip, so that
-- FOR UPDATE SKIP LOCKED actually claims rows for the duration. An open
-- transaction pins xmin and blocks VACUUM across the WHOLE cluster, so a relay
-- that hangs mid-batch quietly degrades every other table in every database.
--
-- These are ALTER ROLE, which is cluster-level. They cannot live in the Event
-- service's migration: eventhub_event_owner is deliberately not a superuser and
-- has no CREATEROLE, so it cannot alter another role — attempting it fails with
-- "permission denied to alter role". Role attributes belong here, where the
-- superuser runs them.
ALTER ROLE eventhub_event_relay SET idle_in_transaction_session_timeout = '30s';
ALTER ROLE eventhub_event_relay SET statement_timeout = '15s';

-- ---------------------------------------------------------------------------
-- One database per service. No cross-database joins exist in Postgres, so the
-- service boundary is enforced by the engine rather than by discipline.
-- ---------------------------------------------------------------------------
CREATE DATABASE eventhub_auth  OWNER eventhub_auth_owner;
CREATE DATABASE eventhub_event OWNER eventhub_event_owner;
CREATE DATABASE eventhub_ticketing OWNER eventhub_ticketing_owner;
CREATE DATABASE eventhub_live OWNER eventhub_live_owner;
CREATE DATABASE eventhub_notification OWNER eventhub_notification_owner;

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

REVOKE CONNECT ON DATABASE eventhub_ticketing FROM PUBLIC;
GRANT  CONNECT ON DATABASE eventhub_ticketing TO eventhub_ticketing_owner, eventhub_ticketing_app,
                                               eventhub_ticketing_relay, eventhub_ticketing_sweeper;

REVOKE CONNECT ON DATABASE eventhub_live FROM PUBLIC;
GRANT  CONNECT ON DATABASE eventhub_live TO eventhub_live_owner, eventhub_live_app;

REVOKE CONNECT ON DATABASE eventhub_notification FROM PUBLIC;
GRANT  CONNECT ON DATABASE eventhub_notification TO eventhub_notification_owner, eventhub_notification_app;

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

\connect eventhub_ticketing
GRANT USAGE ON SCHEMA public TO eventhub_ticketing_app, eventhub_ticketing_relay, eventhub_ticketing_sweeper;

\connect eventhub_live
GRANT USAGE ON SCHEMA public TO eventhub_live_app;

\connect eventhub_notification
GRANT USAGE ON SCHEMA public TO eventhub_notification_app;
