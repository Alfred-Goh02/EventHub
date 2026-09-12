/**
 * The relay's claim query, in one place because two things need the SAME text.
 *
 * drain.ts runs it. scripts/verify-query-plans.mjs asserts its PLAN -- that the
 * partial index still supplies the ordering rather than the planner falling back
 * to a bitmap scan with a Sort on top, which is the shape
 * OutboxMessage_undelivered_idx exists to avoid.
 *
 * IT USED TO BE TWO COPIES, AND THAT IS WHY THE PLAN CHECK MISSED A REAL BUG.
 * verify-query-plans.mjs kept its own hand-written version, justified in a
 * comment on the grounds that "a copy that drifts would show up as this file
 * passing while the real query regressed". That sentence describes the failure
 * rather than preventing it, and the failure duly happened: the copy selected a
 * bare `id` from the CTE, so it never had the outer `seq::text AS "seq"` alias.
 *
 * That alias is the whole of #64. A bare `ORDER BY seq` resolves to the OUTPUT
 * column, which was text, so '10' sorted before '9' and any batch straddling a
 * power of ten published out of order. The plan check could not have caught it,
 * because the query it explained did not contain the alias that caused it. It
 * was explaining a query nobody runs.
 *
 * With one copy, it can. A bare `ORDER BY seq` over a text output column cannot
 * be answered by the index ordering, so the plan grows a Sort node and the
 * assertion goes red -- verified by making exactly that edit and watching it
 * fail.
 *
 * The parameter is the batch size.
 */
export const CLAIM_SQL =
      `WITH claimable AS (
         SELECT o.id, o.seq, o."tenantId", o.topic, o."eventType", o."schemaVersion",
                o."aggregateId", o.payload, o.attempts, o."createdAt"
           FROM "OutboxMessage" o
          WHERE o."publishedAt" IS NULL
            AND o."deadLetteredAt" IS NULL
            AND (o."nextAttemptAt" IS NULL OR o."nextAttemptAt" <= now())
            -- Nothing for an aggregate may go while something older for that
            -- same aggregate is STUCK BEHIND A BACKOFF.
            --
            -- Note what does NOT block: an older row that is claimable right
            -- now. ORDER BY seq puts it earlier in this very batch and the loop
            -- publishes in that order, so excluding it would drain one message
            -- per aggregate per pass and turn a three-version event into three
            -- round trips.
            AND NOT EXISTS (
              SELECT 1
                FROM "OutboxMessage" older
               WHERE older."tenantId" = o."tenantId"
                 AND older."aggregateId" = o."aggregateId"
                 AND older.topic = o.topic
                 AND older.seq < o.seq
                 AND older."publishedAt" IS NULL
                 -- A dead-lettered row is never going to publish, so it must
                 -- not block its aggregate for ever. That leaves a real gap in
                 -- ordering; the alternative is a queue that stops on one
                 -- poison row, and the operator already has an error log
                 -- naming the dead letter.
                 AND older."deadLetteredAt" IS NULL
                 AND older."nextAttemptAt" IS NOT NULL
                 AND older."nextAttemptAt" > now()
            )
          ORDER BY o.seq
          LIMIT $1
            FOR UPDATE SKIP LOCKED
       )
       SELECT id, seq::text AS "seq", "tenantId", topic, "eventType", "schemaVersion",
              "aggregateId", payload, attempts, "createdAt"
         FROM claimable c
        -- ONE RELAY AT A TIME PER AGGREGATE.
        --
        -- SKIP LOCKED hides a row another relay is holding, which is the point
        -- -- but it hides it from the ORDER BY too. Relay A claims seq 1 and
        -- stalls inside the Kafka round-trip this transaction is deliberately
        -- held across; seq 2 and 3 commit meanwhile; relay B drains, cannot see
        -- seq 1, and publishes 2 and 3 ahead of it. Measured on this schema
        -- before this clause existed: [2,3,1].
        --
        -- The lock releases at COMMIT, so B simply gets nothing for that
        -- aggregate and picks the whole stream up on its next pass.
        --
        -- chr(31) is the unit separator: it cannot appear in an id, so the
        -- concatenation is unambiguous. Not chr(0), which Postgres rejects in
        -- text outright.
        --
        -- hashtext is int4, so two streams can collide and one then waits a
        -- drain it did not have to. A delay, not a correctness problem, and the
        -- alternative is a lock table.
        WHERE pg_try_advisory_xact_lock(
                hashtext(c."tenantId" || chr(31) || c.topic || chr(31) || c."aggregateId")::bigint
              )
        -- c.seq, NOT a bare seq. A bare name in ORDER BY resolves to the
        -- OUTPUT column first, and the output column named "seq" is
        -- seq::text -- so a bare ORDER BY seq sorted text, and '10' comes
        -- before '9'. Any batch straddling a power of ten went out of
        -- order: CI caught v3 published before v2 for one event.
        ORDER BY c.seq`;
