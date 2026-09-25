import { randomUUID } from 'node:crypto';
import { Router, type RequestHandler } from 'express';
import { z } from 'zod';
import { getAuth } from '@eventhub/auth-middleware';
import type { Logger } from '@eventhub/logger';
import { ah } from '@eventhub/http';
import { withTenant } from '../db.js';
import { env } from '../env.js';
import { Prisma } from '../generated/prisma/client.js';
import { sortableId } from '../ids.js';
import { readMembership } from '../middleware/space.js';

/**
 * Posts, threaded comments and reactions.
 *
 * EVERY WRITE PUTS ITS EVENT IN THE SAME TRANSACTION. Calling Kafka from a handler
 * gives two outcomes and no third: a broker hiccup either fails a user's post or loses
 * the event. Writing the outbox row beside the post makes "the post exists" and "the
 * world will hear about it" the same fact, and the relay publishes it afterwards.
 */

const TOPIC = 'community.feed';
const SCHEMA_VERSION = 1;

/** The most comments one request returns. Overflow is reported, never silently dropped. */
const THREAD_LIMIT = env.THREAD_PAGE_LIMIT;

/**
 * ONE FORMULA, IN ONE PLACE.
 *
 * A score and the counters it derives from drifting apart is a bug nobody notices
 * until the feed order looks wrong, so insert and every update build it from here.
 * Additive and time-stable: a post's score changes only when someone acts on it, which
 * is what lets an index answer the feed. A decay score has to be recomputed for every
 * row before it can be ordered.
 *
 * The counters are passed in because the SET expressions of an UPDATE all see the
 * PRE-update row: the caller adding a reaction passes `"reactionCount" + 1`, or the
 * score lands one behind for ever.
 */
/**
 * EVERY INTERPOLATED FRAGMENT IS PARENTHESISED, and that is not style.
 *
 * Without the brackets these arguments are pasted in as text and Postgres applies its
 * own precedence. The caller adding a comment passes `"commentCount" + 1`, and
 * `2 * ${comments}` became `2 * "commentCount" + 1` — so a comment was worth 1 instead
 * of 2, and the FIRST comment moved the score not at all: greatest(0 + 1, 1) and
 * greatest(0, 1) are both 1.
 *
 * It hid for a whole part because the verifier recomputed the formula from the stored
 * columns and compared, which agreed with itself, and because the reaction path
 * happens to be unaffected — `rc + 1 + 2 * cc` parses the same either way. It was
 * found by asking a different question: did the number I am about to compare actually
 * move?
 */
const rankScore = (reactions: Prisma.Sql, comments: Prisma.Sql, createdAt: Prisma.Sql) =>
  Prisma.sql`ln(greatest((${reactions}) + 2 * (${comments}), 1) + 1) + extract(epoch from ${createdAt}) / 45000.0`;

/**
 * THE REACTION KINDS ARE A CLOSED SET, and that is a ranking control, not tidiness.
 *
 * `kind` is part of the Reaction unique key, so two different kinds from the same user
 * are two rows and two increments of reactionCount — which feeds rankScore. Accepting
 * any upper-case string, as the first version did, let one account invent a kind per
 * request and push a post's rank as far as the rate limiter allowed. The verifier
 * measured it: one user, two invented kinds, the count went from one to three.
 *
 * A closed set makes the unique key do what it looks like it does — one reaction per
 * person per post.
 */
const REACTION_KINDS = ['LIKE', 'LOVE', 'LAUGH', 'INSIGHTFUL', 'CELEBRATE', 'SAD'] as const;

const postSchema = z.object({ body: z.string().min(1).max(10_000) });
const commentSchema = z.object({ body: z.string().min(1).max(10_000), parentId: z.string().min(1).optional() });
const reactionSchema = z.object({
  kind: z.enum(REACTION_KINDS, { errorMap: () => ({ message: `a reaction kind is one of ${REACTION_KINDS.join(', ')}` }) }),
});

interface CommentRow {
  id: string;
  parentId: string | null;
  authorId: string;
  body: string;
  depth: number;
  path: string;
  reactionCount: number;
  createdAt: Date;
}

/** The outbox row every write leaves behind, in the shape community.feed-value declares. */
function activity(
  tx: Prisma.TransactionClient,
  message: {
    tenantId: string;
    spaceId: string;
    kind: 'POST_CREATED' | 'COMMENT_CREATED' | 'REACTION_ADDED' | 'REACTION_REMOVED';
    actorId: string;
    postId: string;
    commentId?: string | null;
    parentCommentId?: string | null;
    body?: string | null;
  },
) {
  const id = randomUUID();
  const payload = {
    // The outbox row's own id: stable across relay retries, which is what lets a
    // consumer recognise a redelivery. A Kafka offset changes on republish.
    messageId: id,
    // Overwritten below from the DATABASE clock. This placeholder only fixes the key's
    // position in the object; nothing reads it.
    occurredAt: null as string | null,
    tenantId: message.tenantId,
    spaceId: message.spaceId,
    kind: message.kind,
    actorId: message.actorId,
    postId: message.postId,
    commentId: message.commentId ?? null,
    parentCommentId: message.parentCommentId ?? null,
    body: message.body ?? null,
  };
  /**
   * occurredAt COMES FROM THE DATABASE, not from this process.
   *
   * The schema says it is when the change was committed, and consumers order on it.
   * Built from `new Date()` it was really the app's clock, which disagrees with the
   * row's own createdAt by whatever the two clocks disagree by — and Community is one
   * of several services writing to topics a consumer merges. Stamping it in the same
   * statement, from the same now() the row's createdAt defaults to, makes the payload
   * and the row agree and removes app-to-database skew from the ordering field.
   *
   * to_char rather than to_jsonb(now()): Postgres renders a timestamptz as
   * "2026-09-18 19:42:01.123+00", which is not ISO-8601 and not what the schema
   * promises.
   */
  return tx.$executeRaw`
    INSERT INTO "OutboxMessage" (id, "tenantId", topic, "eventType", "schemaVersion", "aggregateId", payload, "createdAt")
    SELECT ${id}, ${message.tenantId}, ${TOPIC}, ${`community.${message.kind.toLowerCase()}`}, ${SCHEMA_VERSION},
           ${message.spaceId},
           jsonb_set(${JSON.stringify(payload)}::jsonb, '{occurredAt}',
                     to_jsonb(to_char(t.at AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'))),
           t.at
      FROM (SELECT date_trunc('milliseconds', now()) AS at) t`;
}

export function postRoutes(
  logger: Logger,
  limit: { middleware: (action: string, perMinute: number) => RequestHandler },
  limits: { post: number; comment: number; reaction: number },
): Router {
  const router = Router();

  /**
   * May this caller read or write in this space?
   *
   * A public space is open to anyone in the tenant; a private one only to its members,
   * and it answers 404 rather than 403 so it does not confirm its own existence.
   */
  async function reach(tenantId: string, spaceId: string, userId: string, need: 'read' | 'write') {
    const rows = await withTenant(tenantId, (tx) =>
      tx.$queryRaw<Array<{ visibility: string }>>`
        SELECT visibility::text AS visibility FROM "CommunitySpace" WHERE id = ${spaceId}`,
    );
    if (!rows[0]) return { status: 404 as const };
    const membership = await readMembership(tenantId, spaceId, userId, logger);
    if (membership?.status === 'BANNED') return { status: 403 as const, error: 'banned from this space' };
    if (rows[0].visibility === 'PRIVATE' && !membership) return { status: 404 as const };
    // Reading a public space needs nothing; writing in any space needs membership.
    if (need === 'write' && !membership) return { status: 403 as const, error: 'join the space first' };
    return { status: 200 as const };
  }

  /** The space a post belongs to, or null. Tenant-scoped by the policy, not by this query. */
  async function spaceOfPost(tenantId: string, postId: string): Promise<string | null> {
    const rows = await withTenant(tenantId, (tx) =>
      tx.$queryRaw<Array<{ spaceId: string }>>`SELECT "spaceId" FROM "Post" WHERE id = ${postId}`,
    );
    return rows[0]?.spaceId ?? null;
  }

  router.post('/spaces/:spaceId/posts', limit.middleware('post', limits.post), ah(async (req, res) => {
    const parsed = postSchema.safeParse(req.body);
    if (!parsed.success) return res.status(400).json({ error: parsed.error.issues[0]?.message ?? 'invalid body' });
    const { tenantId, userId } = getAuth(req);
    const spaceId = req.params['spaceId']!;

    const allowed = await reach(tenantId, spaceId, userId, 'write');
    if (allowed.status !== 200) return res.status(allowed.status).json({ error: allowed.error ?? 'not found' });

    const postId = randomUUID();
    await withTenant(tenantId, async (tx) => {
      /**
       * ONE TIMESTAMP, USED TWICE.
       *
       * The score is derived from createdAt, and the column is TIMESTAMP(3) while
       * now() carries microseconds — so scoring with now() and storing the rounded
       * value made the stored score disagree with the formula over the stored columns
       * from the first row onward. The verifier recomputes the formula and caught it.
       * Rounding once, in a subquery both expressions read, makes them the same number.
       */
      await tx.$executeRaw`
        INSERT INTO "Post" (id, "tenantId", "spaceId", "authorId", body, status, "rankScore", "createdAt")
        SELECT ${postId}, ${tenantId}, ${spaceId}, ${userId}, ${parsed.data.body}, 'VISIBLE'::"ContentStatus",
               ${rankScore(Prisma.sql`0`, Prisma.sql`0`, Prisma.sql`t.at`)}, t.at
          FROM (SELECT date_trunc('milliseconds', now()) AS at) t`;
      await activity(tx, { tenantId, spaceId, kind: 'POST_CREATED', actorId: userId, postId, body: parsed.data.body });
    });

    return res.status(201).json({ id: postId, spaceId });
  }));

  router.get('/spaces/:spaceId/posts', ah(async (req, res) => {
    const { tenantId, userId } = getAuth(req);
    const spaceId = req.params['spaceId']!;
    const allowed = await reach(tenantId, spaceId, userId, 'read');
    if (allowed.status !== 200) return res.status(allowed.status).json({ error: allowed.error ?? 'not found' });

    const posts = await withTenant(tenantId, (tx) =>
      tx.$queryRaw<Array<{ id: string; authorId: string; body: string; reactionCount: number; commentCount: number; rankScore: number; createdAt: Date }>>`
        SELECT id, "authorId", body, "reactionCount", "commentCount", "rankScore", "createdAt"
          FROM "Post"
         WHERE "spaceId" = ${spaceId} AND status = 'VISIBLE'::"ContentStatus"
         ORDER BY "rankScore" DESC, id DESC
         LIMIT 50`,
    );
    return res.json({ posts });
  }));

  /**
   * A reply's place in the thread is derived IN THE INSERT.
   *
   * The parent's path is looked up in the same statement, so a parent id that does not
   * exist, or belongs to another post, makes the path expression NULL and the NOT NULL
   * constraint refuses the row. A separate SELECT to validate the parent would leave a
   * window between the check and the write.
   */
  router.post('/posts/:postId/comments', limit.middleware('comment', limits.comment), ah(async (req, res) => {
    const parsed = commentSchema.safeParse(req.body);
    if (!parsed.success) return res.status(400).json({ error: parsed.error.issues[0]?.message ?? 'invalid body' });
    const { tenantId, userId } = getAuth(req);
    const postId = req.params['postId']!;

    const spaceId = await spaceOfPost(tenantId, postId);
    if (!spaceId) return res.status(404).json({ error: 'not found' });
    const allowed = await reach(tenantId, spaceId, userId, 'write');
    if (allowed.status !== 200) return res.status(allowed.status).json({ error: allowed.error ?? 'not found' });

    const commentId = sortableId();
    const parentId = parsed.data.parentId ?? null;

    try {
      await withTenant(tenantId, async (tx) => {
        await tx.$executeRaw`
          WITH parent AS (
            SELECT path FROM "Comment" WHERE id = ${parentId} AND "postId" = ${postId}
          )
          INSERT INTO "Comment" (id, "tenantId", "postId", "authorId", "parentId", path, depth, body, status, "createdAt")
          VALUES (
            ${commentId}, ${tenantId}, ${postId}, ${userId}, ${parentId},
            CASE WHEN ${parentId}::text IS NULL THEN ${commentId}::ltree
                 ELSE (SELECT path FROM parent) || ${commentId}::ltree END,
            CASE WHEN ${parentId}::text IS NULL THEN 1
                 ELSE (SELECT nlevel(path) FROM parent) + 1 END,
            ${parsed.data.body}, 'VISIBLE'::"ContentStatus", now())`;
        await tx.$executeRaw`
          UPDATE "Post"
             SET "commentCount" = "commentCount" + 1,
                 "rankScore" = ${rankScore(Prisma.sql`"reactionCount"`, Prisma.sql`"commentCount" + 1`, Prisma.sql`"createdAt"`)}
           WHERE id = ${postId}`;
        await activity(tx, {
          tenantId,
          spaceId,
          kind: 'COMMENT_CREATED',
          actorId: userId,
          postId,
          commentId,
          parentCommentId: parentId,
          body: parsed.data.body,
        });
      });
    } catch (err) {
      const code = (err as { meta?: { code?: string } }).meta?.code ?? '';
      const text = err instanceof Error ? err.message : String(err);
      // 23514 is the depth cap; 23502 is the NOT NULL on path, which is how an unknown
      // or foreign parent arrives here.
      if (code === '23514' || /comment_depth_cap|Comment_depth_cap/.test(text)) {
        return res.status(422).json({ error: 'this thread is as deep as it goes' });
      }
      if (code === '23502' || /null value in column "path"/.test(text)) {
        return res.status(404).json({ error: 'no such parent comment on this post' });
      }
      throw err;
    }

    return res.status(201).json({ id: commentId, postId, parentId });
  }));

  /**
   * The whole thread in one query, in render order.
   *
   * ORDER BY path is depth-first pre-order — the sequence a threaded view draws — so
   * the database does the interleaving and this does one O(n) pass with no recursion
   * and no sorting.
   */
  router.get('/posts/:postId/comments', ah(async (req, res) => {
    const { tenantId, userId } = getAuth(req);
    const postId = req.params['postId']!;
    const spaceId = await spaceOfPost(tenantId, postId);
    if (!spaceId) return res.status(404).json({ error: 'not found' });
    const allowed = await reach(tenantId, spaceId, userId, 'read');
    if (allowed.status !== 200) return res.status(allowed.status).json({ error: allowed.error ?? 'not found' });

    /**
     * ONE MORE THAN THE CAP, SO TRUNCATION IS VISIBLE.
     *
     * Reading exactly THREAD_LIMIT and returning `count: rows.length` reports 500 for
     * a thread of 500 and for a thread of fifty thousand, and the caller cannot tell
     * which it got. Asking for one extra row makes the overflow a fact the response
     * carries rather than a cap it silently hits.
     */
    const rows = await withTenant(tenantId, (tx) =>
      tx.$queryRaw<CommentRow[]>`
        SELECT id, "parentId", "authorId", body, depth, path::text AS path, "reactionCount", "createdAt"
          FROM "Comment"
         WHERE "postId" = ${postId} AND status = 'VISIBLE'::"ContentStatus"
         ORDER BY path
         LIMIT ${THREAD_LIMIT + 1}`,
    );
    const truncated = rows.length > THREAD_LIMIT;
    if (truncated) rows.length = THREAD_LIMIT;

    const nodes = new Map<string, CommentRow & { replies: unknown[] }>();
    const thread: unknown[] = [];
    for (const row of rows) {
      const node = { ...row, replies: [] as unknown[] };
      nodes.set(row.id, node);
      /**
       * A parent always precedes its children in path order, so one pass is enough.
       *
       * A node whose parent is not in this map is attached at the top. Today that only
       * happens at the truncation boundary. Once a moderator can HIDE a comment it will
       * also happen to the replies of a hidden parent, and attaching them at the top
       * would promote them to standalone comments — the opposite of hiding a thread.
       * The moderation part has to filter by hidden ANCESTOR, not by the row's own
       * status; this comment is here so that arrives as a decision rather than a
       * surprise.
       */
      const parent = row.parentId ? nodes.get(row.parentId) : undefined;
      if (parent) parent.replies.push(node);
      else thread.push(node);
    }
    return res.json({ comments: thread, count: rows.length, truncated });
  }));

  /**
   * Reacting twice is reacting once.
   *
   * The unique key does the work and the row count says whether anything changed, so a
   * double tap and a retry from a flaky connection are both safe. The counter moves in
   * one statement, so two concurrent reactions serialise on the row lock instead of
   * racing in JavaScript.
   */
  router.post('/posts/:postId/reactions', limit.middleware('reaction', limits.reaction), ah(async (req, res) => {
    const parsed = reactionSchema.safeParse(req.body);
    if (!parsed.success) return res.status(400).json({ error: parsed.error.issues[0]?.message ?? 'invalid body' });
    const { tenantId, userId } = getAuth(req);
    const postId = req.params['postId']!;
    const spaceId = await spaceOfPost(tenantId, postId);
    if (!spaceId) return res.status(404).json({ error: 'not found' });
    const allowed = await reach(tenantId, spaceId, userId, 'write');
    if (allowed.status !== 200) return res.status(allowed.status).json({ error: allowed.error ?? 'not found' });

    const added = await withTenant(tenantId, async (tx) => {
      const inserted = await tx.$executeRaw`
        INSERT INTO "Reaction" (id, "tenantId", "targetType", "targetId", "userId", kind, "createdAt")
        VALUES (${randomUUID()}, ${tenantId}, 'POST'::"TargetType", ${postId}, ${userId}, ${parsed.data.kind}, now())
        ON CONFLICT ("tenantId", "targetType", "targetId", "userId", kind) DO NOTHING`;
      // Nothing follows a no-op insert: the counter, the score and the event all belong
      // to a reaction that was actually added.
      if (inserted !== 1) return false;
      await tx.$executeRaw`
        UPDATE "Post"
           SET "reactionCount" = "reactionCount" + 1,
               "rankScore" = ${rankScore(Prisma.sql`"reactionCount" + 1`, Prisma.sql`"commentCount"`, Prisma.sql`"createdAt"`)}
         WHERE id = ${postId}`;
      await activity(tx, { tenantId, spaceId, kind: 'REACTION_ADDED', actorId: userId, postId });
      return true;
    });

    return res.status(added ? 201 : 200).json({ postId, kind: parsed.data.kind, added });
  }));

  // Metered like every other write here. It was the one that was not, and un-reacting
  // is a write: a valid token could run spaceOfPost, reach and a DELETE in a loop for
  // free. That it can only remove the caller's own reactions bounds the damage, not the
  // load.
  router.delete('/posts/:postId/reactions/:kind', limit.middleware('reaction', limits.reaction), ah(async (req, res) => {
    const { tenantId, userId } = getAuth(req);
    const postId = req.params['postId']!;
    const kind = req.params['kind']!;
    const spaceId = await spaceOfPost(tenantId, postId);
    if (!spaceId) return res.status(404).json({ error: 'not found' });
    const allowed = await reach(tenantId, spaceId, userId, 'write');
    if (allowed.status !== 200) return res.status(allowed.status).json({ error: allowed.error ?? 'not found' });

    const removed = await withTenant(tenantId, async (tx) => {
      const deleted = await tx.$executeRaw`
        DELETE FROM "Reaction"
         WHERE "tenantId" = ${tenantId} AND "targetType" = 'POST'::"TargetType"
           AND "targetId" = ${postId} AND "userId" = ${userId} AND kind = ${kind}`;
      if (deleted !== 1) return false;
      // greatest(...) rather than a bare subtraction: a counter that can go negative
      // through some path nobody has thought of yet produces a feed order nobody can explain.
      await tx.$executeRaw`
        UPDATE "Post"
           SET "reactionCount" = greatest("reactionCount" - 1, 0),
               "rankScore" = ${rankScore(Prisma.sql`greatest("reactionCount" - 1, 0)`, Prisma.sql`"commentCount"`, Prisma.sql`"createdAt"`)}
         WHERE id = ${postId}`;
      await activity(tx, { tenantId, spaceId, kind: 'REACTION_REMOVED', actorId: userId, postId });
      return true;
    });

    return res.status(removed ? 204 : 404).end();
  }));

  return router;
}
