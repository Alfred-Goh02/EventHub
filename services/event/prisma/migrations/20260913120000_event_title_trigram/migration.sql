-- Degraded search.
--
-- When OpenSearch is unavailable, the Search service falls back to this
-- database: GET /events/search, a title ILIKE capped at 50 rows. OpenSearch is a
-- derived read model, so losing it should cost relevance and facets, not the
-- ability to find an event at all.
--
-- A leading-wildcard ILIKE cannot use a btree index: '%jazz%' has no prefix to
-- seek on, so without this every fallback query is a scan of the tenant's
-- events. A trigram GIN index turns the pattern into the three-character pieces
-- it must contain, which it can look up.
--
-- The cap is the other half, and it lives in the route. An uncapped fallback
-- turns a search outage into a Postgres outage, which is strictly worse.
--
-- pg_trgm is a TRUSTED extension (Postgres 13+), so the database owner can
-- create it without superuser — eventhub_event_owner owns eventhub_event and is
-- deliberately not a superuser.
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Partial, like the slug index: soft-deleted rows are never searched, so they
-- need not be indexed.
CREATE INDEX "Event_title_trgm_live"
  ON "Event" USING gin ("title" gin_trgm_ops)
  WHERE "deletedAt" IS NULL;
