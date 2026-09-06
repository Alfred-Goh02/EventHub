-- ---------------------------------------------------------------------------
-- An index for the relay's per-aggregate ordering guard.
--
-- The claim query now refuses a row while an OLDER row for the same aggregate
-- is waiting out a retry backoff. Without an index for it, that NOT EXISTS
-- planned as a nested-loop anti join whose inner side had no index condition at
-- all -- only a Filter -- so it rescanned every undelivered row once per
-- candidate:
--
--   Nested Loop Anti Join
--     ->  Index Scan using "OutboxMessage_undelivered_idx" on "OutboxMessage" o
--     ->  Index Scan using "OutboxMessage_undelivered_idx" on "OutboxMessage" older
--           Filter: ... ("nextAttemptAt" > now())
--
-- That is worst in the normal case, where nothing is backing off and the scan
-- therefore finds nothing and runs to the end. And it is worst in absolute terms
-- during a broker outage -- when the backlog is large and the guard matters most
-- -- which is the wrong direction for the feedback loop, and eventually meets
-- the relay role's statement_timeout and fails permanently.
--
-- The predicate holds only rows that are BACKING OFF, which is normally none.
-- So the index is empty most of the time, and during an outage it holds one row
-- per stuck aggregate rather than one per queued message. Like
-- OutboxMessage_undelivered_idx, it self-empties as rows publish.
--
-- Column order matches the subquery: equality on tenantId, aggregateId and
-- topic, then a range on seq.
--
-- Ticketing drains its outbox with the SAME relay code as Event -- the relay is
-- routing-free and takes only a different connection string -- so the guard and
-- its index have to exist in both databases or the guard is unindexed in one.
--
-- Raw SQL because Prisma cannot express a partial index; the schema carries a
-- NOTE saying so.
-- ---------------------------------------------------------------------------
CREATE INDEX "OutboxMessage_backoff_idx"
  ON "OutboxMessage" ("tenantId", "aggregateId", "topic", "seq")
  WHERE "publishedAt" IS NULL
    AND "deadLetteredAt" IS NULL
    AND "nextAttemptAt" IS NOT NULL;
