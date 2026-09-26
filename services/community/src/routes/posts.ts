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
import { reach as reachSpace, readMembership } from '../middleware/space.js';
import { rankScore } from '../rank.js';
import { hiddenAbove, recordAction, recountComments } from '../visibility.js';

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
  /** Null on a tombstone: the author deleted it, and who wrote it goes with the words. */
  authorId: string | null;
  body: string | null;
  deleted: boolean;
  depth: number;
  path: string;
  reactionCount: number;
  createdAt: Date;
}

type ThreadNode = CommentRow & { replies: ThreadNode[] };

/**
 * Drops every tombstone with nothing under it, bottom-up, and counts what is left.
 *
 * A deleted comment is only shown to hold the replies that answered it. With no reply
 * left — none written, or all of them deleted too — it would be a "[deleted]" line
 * holding nothing, so it goes. Bottom-up, because a chain of tombstones ending in
 * nothing goes entirely: removing the last one empties its parent in turn.
 */
function prune(nodes: ThreadNode[]): { kept: ThreadNode[]; count: number } {
  const kept: ThreadNode[] = [];
  let count = 0;
  for (const node of nodes) {
    const below = prune(node.replies);
    node.replies = below.kept;
    if (node.deleted && below.kept.length === 0) continue;
    kept.push(node);
    count += 1 + below.count;
  }
  return { kept, count };
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
    recipientId?: string | null;
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
    recipientId: message.recipientId ?? null,
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

  /** The shared check, bound to this router's logger. See middleware/space.ts. */
  const reach = (tenantId: string, spaceId: string, userId: string, need: 'read' | 'write') =>
    reachSpace(tenantId, spaceId, userId, need, logger);

  /** The space a post belongs to, or null. Tenant-scoped by the policy, not by this query. */
  /**
   * VISIBLE POSTS ONLY, for every caller here: commenting, reacting, un-reacting and
   * reading the thread. A hidden post that still took comments and reactions could be
   * kept alive — and its rank pushed up — by exactly the people it was hidden from, and
   * its thread would stay readable to anyone holding the id. Moderation looks posts up
   * on its own, because un-hiding has to find the hidden ones.
   */
  async function spaceOfPost(tenantId: string, postId: string): Promise<string | null> {
    const rows = await withTenant(tenantId, (tx) =>
      tx.$queryRaw<Array<{ spaceId: string }>>`
        SELECT "spaceId" FROM "Post" WHERE id = ${postId} AND status = 'VISIBLE'::"ContentStatus"`,
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
            /*
             * The parent must be VISIBLE, and nothing above it HIDDEN. A reply under
             * hidden content would be invisible the moment it was written — its
             * ancestor hides it — while still counting towards the post's rank, and it
             * gives anyone holding the id a way to keep writing into a thread a
             * moderator closed. Same refusal as an unknown parent: the path goes NULL
             * and the NOT NULL constraint stops the row.
             *
             * A DELETED comment is no parent either: there is nothing left to answer.
             * But one ABOVE the parent is fine — its replies are still in the thread,
             * and still answerable. See visibility.ts.
             */
            SELECT c.path FROM "Comment" c
             WHERE c.id = ${parentId} AND c."postId" = ${postId}
               AND c.status = 'VISIBLE'::"ContentStatus"
               AND NOT ${hiddenAbove('c')}
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
        /**
         * WHOM THIS COMMENT ANSWERS, decided here and nowhere else.
         *
         * The author of the comment replied to, or of the post for a top-level comment
         * — and only if they can still read the space NOW: an active member, or anyone
         * not banned from a public one. A reply email carries the reply's words, so
         * telling someone who was banned, or who left a private space, would hand them
         * what the space no longer shows them. Community is the only service that can
         * see who may read what, so a consumer is told the answer rather than left to
         * guess it. Never the commenter themselves.
         *
         * And only if what they wrote can still be seen. The post and the parent were
         * checked earlier, but each statement here reads a newer snapshot, so a hide
         * that commits in between is missed — and the author of hidden content would be
         * emailed a reply nobody can read in the space. So they are checked again HERE,
         * after `UPDATE "Post"` above took the post's row lock. Every hide takes that
         * lock too — a post's directly, a comment's through recountComments — so any
         * hide that committed first is seen by this statement, and any that did not
         * waits for this comment to commit, and comes after it.
         */
        const answered = await tx.$queryRaw<Array<{ recipientId: string }>>`
          SELECT r.author AS "recipientId"
            FROM (SELECT CASE WHEN ${parentId}::text IS NULL THEN p."authorId" ELSE c."authorId" END AS author
                    FROM "Post" p
                    LEFT JOIN "Comment" c ON c.id = ${parentId} AND c."postId" = p.id
                          AND c.status = 'VISIBLE'::"ContentStatus" AND NOT ${hiddenAbove('c')}
                   WHERE p.id = ${postId} AND p.status = 'VISIBLE'::"ContentStatus") r
            JOIN "CommunitySpace" s ON s.id = ${spaceId}
            LEFT JOIN "Membership" m ON m."spaceId" = s.id AND m."userId" = r.author
           WHERE r.author IS NOT NULL AND r.author <> ${userId}
             AND (m.status = 'ACTIVE'::"MemberStatus"
                  OR (m.id IS NULL AND s.visibility = 'PUBLIC'::"Visibility"))`;
        await activity(tx, {
          tenantId,
          spaceId,
          kind: 'COMMENT_CREATED',
          actorId: userId,
          postId,
          commentId,
          parentCommentId: parentId,
          body: parsed.data.body,
          recipientId: answered[0]?.recipientId ?? null,
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
        SELECT c.id, c."parentId",
               /*
                * A tombstone's words and author are blanked HERE, so they never leave
                * the database. Blanking them in JavaScript would work until the day a
                * change returned the row before the blanking ran.
                */
               CASE WHEN c.status = 'DELETED'::"ContentStatus" THEN NULL ELSE c."authorId" END AS "authorId",
               CASE WHEN c.status = 'DELETED'::"ContentStatus" THEN NULL ELSE c.body END AS body,
               c.status = 'DELETED'::"ContentStatus" AS deleted,
               c.depth, c.path::text AS path, c."reactionCount", c."createdAt"
          FROM "Comment" c
         WHERE c."postId" = ${postId} AND c.status <> 'HIDDEN'::"ContentStatus"
           /*
            * And nothing HIDDEN above it. Filtering on the row's own status alone left
            * the replies of a hidden comment in the result with their parent missing
            * from it, and the one-pass nesting below attached every orphan at the top —
            * so hiding a comment PROMOTED its replies to standalone comments, the
            * opposite of hiding a thread. Part 3 wrote that down as the problem
            * moderation would have to solve; this is the answer.
            *
            * DELETED comments come back, blanked, because their replies hang from them;
            * prune() drops the ones with nothing left under them.
            */
           AND NOT ${hiddenAbove('c')}
         ORDER BY c.path
         LIMIT ${THREAD_LIMIT + 1}`,
    );
    const truncated = rows.length > THREAD_LIMIT;
    if (truncated) rows.length = THREAD_LIMIT;

    const nodes = new Map<string, ThreadNode>();
    const top: ThreadNode[] = [];
    for (const row of rows) {
      const node: ThreadNode = { ...row, replies: [] };
      nodes.set(row.id, node);
      /**
       * A parent always precedes its children in path order, so one pass is enough.
       *
       * A node whose parent is not in this map is attached at the top. That happens
       * only at the truncation boundary: the query excludes every comment with a hidden
       * ancestor, and returns a deleted parent as a tombstone, so no reply arrives here
       * without its parent.
       */
      const parent = row.parentId ? nodes.get(row.parentId) : undefined;
      if (parent) parent.replies.push(node);
      else top.push(node);
    }
    const { kept, count } = prune(top);
    return res.json({ comments: kept, count, truncated });
  }));

  /**
   * An author deletes their own post or comment.
   *
   * AUTHORSHIP IS THE PERMISSION, NOT MEMBERSHIP. Leaving a space must not strand what
   * you wrote there, so a former member can still delete their own words. A BAN stops
   * it, as it stops every other act in the space. Moderators do not delete: they hide,
   * which can be undone, and deleting is the author's own decision.
   *
   * VISIBLE → DELETED, compare-and-set, audited as DELETE with the author as the actor,
   * in the same transaction. The row stays, because reports and the audit trail name it.
   * A second delete finds nothing to delete and answers 404: the content is gone, which
   * is what the first call asked for.
   *
   * Nothing is published. The feed reads each post's status, and nothing outside this
   * service consumes community.feed.
   */
  const refuseBanned = async (tenantId: string, spaceId: string, userId: string) =>
    (await readMembership(tenantId, spaceId, userId, logger))?.status === 'BANNED';

  /**
   * Someone else's content: 403 to a caller who can read the space, and 404 to one who
   * cannot. A flat 403 would confirm to a stranger that a post in a PRIVATE space
   * exists, which every other route here refuses to do.
   */
  const notYours = async (tenantId: string, spaceId: string, userId: string) =>
    (await reach(tenantId, spaceId, userId, 'read')).status === 200
      ? { status: 403, error: 'only its author can delete this' }
      : { status: 404, error: 'not found' };

  // Two buckets, not one: the limiter keys on the action name, and two routes sharing a
  // name with different limits would refill one bucket at two rates.
  router.delete('/posts/:postId', limit.middleware('delete-post', limits.post), ah(async (req, res) => {
    const { tenantId, userId } = getAuth(req);
    const postId = req.params['postId']!;

    const rows = await withTenant(tenantId, (tx) =>
      tx.$queryRaw<Array<{ spaceId: string; authorId: string }>>`
        SELECT "spaceId", "authorId" FROM "Post" WHERE id = ${postId} AND status = 'VISIBLE'::"ContentStatus"`,
    );
    const post = rows[0];
    if (!post) return res.status(404).json({ error: 'not found' });
    if (post.authorId !== userId) {
      const refusal = await notYours(tenantId, post.spaceId, userId);
      return res.status(refusal.status).json({ error: refusal.error });
    }
    if (await refuseBanned(tenantId, post.spaceId, userId)) return res.status(403).json({ error: 'banned from this space' });

    const deleted = await withTenant(tenantId, async (tx) => {
      const moved = await tx.$executeRaw`
        UPDATE "Post" SET status = 'DELETED'::"ContentStatus"
         WHERE id = ${postId} AND "authorId" = ${userId} AND status = 'VISIBLE'::"ContentStatus"`;
      // Hidden or deleted in the meantime: nothing of the author's left to remove.
      if (moved !== 1) return false;
      await recordAction(tx, { tenantId, spaceId: post.spaceId, actorId: userId, targetType: 'POST', targetId: postId, action: 'DELETE' });
      return true;
    });
    return deleted ? res.status(204).end() : res.status(404).json({ error: 'not found' });
  }));

  router.delete('/comments/:commentId', limit.middleware('delete-comment', limits.comment), ah(async (req, res) => {
    const { tenantId, userId } = getAuth(req);
    const commentId = req.params['commentId']!;

    // Only what the author can see: on a visible post, visible itself, nothing hidden
    // above it. A comment inside a hidden subtree is the same 404 as one that never was.
    const rows = await withTenant(tenantId, (tx) =>
      tx.$queryRaw<Array<{ postId: string; spaceId: string; authorId: string }>>`
        SELECT c."postId", p."spaceId", c."authorId"
          FROM "Comment" c JOIN "Post" p ON p.id = c."postId"
         WHERE c.id = ${commentId} AND c.status = 'VISIBLE'::"ContentStatus"
           AND p.status = 'VISIBLE'::"ContentStatus" AND NOT ${hiddenAbove('c')}`,
    );
    const comment = rows[0];
    if (!comment) return res.status(404).json({ error: 'not found' });
    if (comment.authorId !== userId) {
      const refusal = await notYours(tenantId, comment.spaceId, userId);
      return res.status(refusal.status).json({ error: refusal.error });
    }
    if (await refuseBanned(tenantId, comment.spaceId, userId)) return res.status(403).json({ error: 'banned from this space' });

    const deleted = await withTenant(tenantId, async (tx) => {
      const moved = await tx.$executeRaw`
        UPDATE "Comment" SET status = 'DELETED'::"ContentStatus"
         WHERE id = ${commentId} AND "authorId" = ${userId} AND status = 'VISIBLE'::"ContentStatus"`;
      if (moved !== 1) return false;
      // The replies stay, so the count drops by one — but it is recomputed, not
      // decremented, for the reasons in visibility.ts.
      await recountComments(tx, comment.postId);
      await recordAction(tx, {
        tenantId,
        spaceId: comment.spaceId,
        actorId: userId,
        targetType: 'COMMENT',
        targetId: commentId,
        action: 'DELETE',
      });
      return true;
    });
    return deleted ? res.status(204).end() : res.status(404).json({ error: 'not found' });
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
