import type { Prisma } from '../generated/prisma/client.js';

/**
 * The write half of the hybrid feed.
 *
 * A post in a SMALL space is fanned out: one FeedEntry row per member, written when
 * the post appears, so a member's feed is one indexed read of their own rows. A post
 * in a LARGE space is not, because fanning one post out to a hundred thousand members
 * is a hundred thousand rows nobody may ever read. Those spaces are queried directly
 * at read time instead.
 *
 * Measured on this machine, one INSERT ... SELECT over generate_series:
 *
 *     1 000 members      23 ms
 *     10 000 members    213 ms
 *     100 000 members   971 ms
 *
 * So latency is not what sets the threshold — this runs in a consumer, not in the
 * request, and the post is already committed and answered before any of it. What sets
 * it is write amplification: at 100 000 members a hundred posts a day is ten million
 * rows a day from one space.
 */

/**
 * What a fan-out did, as three OUTCOMES rather than a boolean.
 *
 * "fanned out" and "not fanned out" hid a third case: a space this consumer cannot
 * see. The topic keeps a week of messages, so a replay reaches posts whose space has
 * since been deleted — and with a boolean those were reported, and logged, as "past
 * the fan-out threshold", which is a confident and wrong diagnosis of a space that is
 * simply gone. Exactly the shape CLAUDE.md warns about.
 */
export type FanOut =
  | { outcome: 'fanned-out'; memberCount: number; rows: number }
  | { outcome: 'too-large'; memberCount: number; rows: 0 }
  | { outcome: 'no-such-space'; memberCount: 0; rows: 0 };

/**
 * Fan a post out to the members of its space, when the space is small enough.
 *
 * ONE STATEMENT, NOT A LOOP. The members are never read into this process: the insert
 * selects straight from Membership, so a five-thousand-member space is one round trip
 * rather than five thousand.
 *
 * IDEMPOTENT BY THE UNIQUE KEY. Delivery is at-least-once, so this runs again on a
 * redelivery and must not double-write; (tenantId, userId, postId) is unique and the
 * conflict is ignored. `rows` is therefore 0 on a redelivery, which is the truth and
 * not a failure.
 *
 * BANNED MEMBERS ARE EXCLUDED. They cannot read the space, so a row in their feed is
 * one the read path would have to filter out for ever afterwards.
 */
export async function fanOutPost(
  tx: Prisma.TransactionClient,
  message: { tenantId: string; spaceId: string; postId: string },
  maxMembers: number,
): Promise<FanOut> {
  /**
   * memberCount, not count(*) on Membership.
   *
   * The counter is denormalised and moved by the statements that join and leave, for
   * exactly this: deciding fan-out with an aggregate would run one COUNT over
   * Membership per post, which is the query that falls over first.
   */
  const spaces = await tx.$queryRaw<Array<{ memberCount: number }>>`
    SELECT "memberCount" FROM "CommunitySpace" WHERE id = ${message.spaceId}`;
  if (spaces.length === 0) return { outcome: 'no-such-space', memberCount: 0, rows: 0 };
  const memberCount = spaces[0]!.memberCount;
  if (memberCount > maxMembers) return { outcome: 'too-large', memberCount, rows: 0 };

  /**
   * The score is copied from the post rather than recomputed here. One formula, in one
   * place, is the rule the post routes already follow; a second copy of it in the
   * consumer would drift from the first the day either changed.
   *
   * createdAt IS THE POST'S, NOT THIS ROW'S, and that is deliberate even though the
   * column defaults to now(). A feed is ordered by when the POST happened; taking the
   * moment the consumer got round to writing the row would order it by consumer lag
   * instead, so a backlog being worked off would shuffle everyone's feed. It also
   * makes the column safe to page on, because unlike rankScore it never changes after
   * the row exists.
   */
  const rows = await tx.$executeRaw`
    INSERT INTO "FeedEntry" (id, "tenantId", "userId", "postId", "spaceId", "rankScore", "createdAt")
    SELECT gen_random_uuid()::text, ${message.tenantId}, m."userId", p.id, p."spaceId", p."rankScore", p."createdAt"
      FROM "Membership" m
      JOIN "Post" p ON p.id = ${message.postId}
     WHERE m."tenantId" = ${message.tenantId}
       AND m."spaceId" = ${message.spaceId}
       AND m.status = 'ACTIVE'::"MemberStatus"
    ON CONFLICT ("tenantId", "userId", "postId") DO NOTHING`;

  /**
   * And the post is marked as belonging to the materialised half, IN THE SAME
   * TRANSACTION as the rows. A reader's single statement then sees the rows and the
   * flag together or neither, so the post is never in both halves and never in none.
   * Judging "large" at read time instead loses every post of a space that shrinks
   * back under the threshold; see migration 20260924000000.
   */
  await tx.$executeRaw`UPDATE "Post" SET "fannedOut" = true WHERE id = ${message.postId} AND "fannedOut" = false`;

  return { outcome: 'fanned-out', memberCount, rows };
}

/**
 * Bring the fanned-out copies of a post's score back in line with the post.
 *
 * THE VALUE IS COPIED, NEVER ADJUSTED, and that is the whole reason this is safe.
 * `SET "rankScore" = "rankScore" + x` would count a redelivered reaction twice and
 * would apply two reorderings in the wrong order if the partition ever replayed out of
 * sequence. Copying whatever the Post row currently says is idempotent — running it
 * twice is running it once — and self-correcting: a message that arrives late still
 * leaves the copy equal to the truth, because the truth is what it reads.
 *
 * So this deliberately ignores the counts in the event. The event is only a signal
 * that something moved; Postgres holds the answer.
 *
 * The app role is granted UPDATE on this column ALONE (see the migration), so this
 * cannot quietly move a row into somebody else's feed even if it tried.
 */
export async function resyncScore(
  tx: Prisma.TransactionClient,
  message: { tenantId: string; postId: string },
): Promise<number> {
  return tx.$executeRaw`
    UPDATE "FeedEntry" f
       SET "rankScore" = p."rankScore"
      FROM "Post" p
     WHERE p.id = ${message.postId}
       AND f."postId" = ${message.postId}
       AND f."tenantId" = ${message.tenantId}
       AND f."rankScore" IS DISTINCT FROM p."rankScore"`;
}
