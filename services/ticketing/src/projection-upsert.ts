/**
 * The monotonic projection write, as ONE statement.
 *
 * The guard used to be a read and then a write:
 *
 *   const existing = await tx.eventProjection.findFirst({ ... });
 *   if (existing && existing.sourceVersion >= version) skip;
 *   else await tx.eventProjection.upsert({ ... });
 *
 * which is the read-then-write shape this repository has now found in four
 * separate places. Two writers both read sourceVersion 5, one carrying 6 and
 * one carrying 7; both pass the check; both write; and whichever commits last
 * wins. If that is the 6, the projection has gone BACKWARDS — an event shows a
 * stale capacity, or a cancelled event returns to PUBLISHED and becomes
 * sellable again.
 *
 * WHAT WAS ACTUALLY HOLDING IT UP, and why that is not enough:
 *
 * Messages are keyed by aggregate id, so every message about one event lands on
 * one partition, and one partition is consumed by one member of the group,
 * sequentially. There is no concurrency — while the assignment is stable.
 *
 * That is a property of the DEPLOYMENT, not of this code, and it is invisible
 * from here. It stops holding during a rebalance, when a partition can briefly
 * be processed by its old owner and its new one at once; and it stops holding
 * silently the day somebody raises the partition count, changes the key, or
 * runs a second consumer group that writes the same table. Nothing would fail
 * at that moment. The projection would just occasionally be wrong.
 *
 * So the guard is moved into the WHERE clause of the write, where Postgres
 * evaluates it under the row lock ON CONFLICT already takes. Two concurrent
 * writers are then serialised by the database: the second one re-evaluates
 * against what the first committed, and a lower version updates nothing.
 *
 * Kept in its own module, holding SQL rather than running it, for two reasons.
 * It is the same idiom as the other two raw statements in the consumer —
 * ProcessedMessage's INSERT ... ON CONFLICT DO NOTHING and the checkpoint's
 * DO UPDATE — none of which Prisma's query builder can express. And it lets
 * scripts/verify-projection-monotonic.mjs execute THE STATEMENT ITSELF rather
 * than a copy of it, so the assertion cannot drift away from the code.
 */

/**
 * Parameters, in order:
 *
 *   $1 id            $2 tenantId     $3 title       $4 slug
 *   $5 status        $6 capacity     $7 startsAt    $8 endsAt
 *   $9 organiserId  $10 deleted     $11 sourceVersion
 *
 * updatedAt is set to now() here rather than by Prisma's @updatedAt, which only
 * applies to writes made through the query builder.
 *
 * Returns a row count: 1 if the snapshot was applied, 0 if it was stale. Zero is
 * not an error — it is the guard doing its job.
 */
export const PROJECTION_UPSERT_SQL = `
INSERT INTO "EventProjection"
  ("id", "tenantId", "title", "slug", "status", "capacity",
   "startsAt", "endsAt", "organiserId", "deleted", "sourceVersion", "updatedAt")
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, now())
ON CONFLICT ("id") DO UPDATE
  SET "tenantId"      = EXCLUDED."tenantId",
      "title"         = EXCLUDED."title",
      "slug"          = EXCLUDED."slug",
      "status"        = EXCLUDED."status",
      "capacity"      = EXCLUDED."capacity",
      "startsAt"      = EXCLUDED."startsAt",
      "endsAt"        = EXCLUDED."endsAt",
      "organiserId"   = EXCLUDED."organiserId",
      "deleted"       = EXCLUDED."deleted",
      "sourceVersion" = EXCLUDED."sourceVersion",
      "updatedAt"     = now()
  WHERE "EventProjection"."sourceVersion" < EXCLUDED."sourceVersion"
`;
