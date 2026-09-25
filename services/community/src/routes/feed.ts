import { Router } from 'express';
import { z } from 'zod';
import { getAuth } from '@eventhub/auth-middleware';
import { ah } from '@eventhub/http';
import { withTenant } from '../db.js';
import { Prisma } from '../generated/prisma/client.js';

/**
 * A member's feed: the posts of every space they belong to, newest first.
 *
 * TWO HALVES, ONE STATEMENT.
 *   - MATERIALISED: the FeedEntry rows the consumer wrote for this member.
 *   - READ-TIME: posts the consumer did NOT fan out, fetched straight from each space
 *     the member belongs to.
 * Which half a post is in was decided once, by the consumer, and recorded on the post
 * (`fannedOut`). Deciding by the space's size at read time instead loses the posts of
 * a space that shrank back under the threshold — they were never fanned out, and a
 * size rule would stop querying the space. Both halves run in ONE statement, so one
 * snapshot: the consumer's rows and flag are seen together or not at all.
 *
 * PAGED ON (createdAt, id), which never changes. Decided 2026-09-21 from a
 * measurement: a cursor over rankScore skips a post whose score rises past it between
 * pages and repeats one whose score falls. Ranking is applied WITHIN each page.
 */

const PAGE_MAX = 50;
const query = z.object({
  limit: z.coerce.number().int().min(1).max(PAGE_MAX).default(20),
  cursor: z.string().min(1).max(512).optional(),
});

interface Cursor {
  /** UTC wall-clock with milliseconds, exactly as Postgres renders the column. */
  at: string;
  id: string;
}

/**
 * THE CURSOR NEVER BECOMES A JS DATE ON ITS WAY TO THE DATABASE.
 *
 * createdAt is `timestamp WITHOUT time zone`, and node-postgres reads and writes such a
 * column in the process's LOCAL zone — on this machine eight hours off. So the cursor
 * carries Postgres's own rendering of the value, and goes back to Postgres as that same
 * text. The Date below only VALIDATES the string, and nothing reads it afterwards.
 *
 * An impossible date is refused here rather than handed to Postgres. `2026-02-30` would
 * be a 500 from the cast, and a cursor is user input.
 */
function decodeCursor(raw: string): Cursor | null {
  try {
    const parsed = JSON.parse(Buffer.from(raw, 'base64url').toString('utf8')) as { at?: unknown; id?: unknown };
    const { at, id } = parsed;
    if (typeof at !== 'string' || typeof id !== 'string') return null;
    if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}$/.test(at)) return null;
    const check = new Date(`${at}Z`);
    if (Number.isNaN(check.getTime()) || check.toISOString().slice(0, 23) !== at) return null;
    if (id.length === 0 || id.length > 64) return null;
    return { at, id };
  } catch {
    return null;
  }
}

const encodeCursor = (c: Cursor) => Buffer.from(JSON.stringify(c), 'utf8').toString('base64url');

interface FeedRow {
  id: string;
  spaceId: string;
  authorId: string;
  body: string;
  reactionCount: number;
  commentCount: number;
  rankScore: number;
  /** Postgres's rendering, for the cursor. Never parsed here. */
  cursorAt: string;
  createdAt: string;
}

export function feedRoutes(): Router {
  const router = Router();

  router.get('/feed', ah(async (req, res) => {
    const parsed = query.safeParse(req.query);
    if (!parsed.success) return res.status(400).json({ error: parsed.error.issues[0]?.message ?? 'invalid query' });
    const { tenantId, userId } = getAuth(req);
    const { limit } = parsed.data;

    let cursor: Cursor | null = null;
    if (parsed.data.cursor !== undefined) {
      cursor = decodeCursor(parsed.data.cursor);
      if (!cursor) return res.status(400).json({ error: 'that cursor is not one this feed issued' });
    }

    /**
     * One more than a page, so "there is more" is a fact rather than a guess. A full
     * page that happened to be the last one would otherwise offer a cursor to nothing.
     */
    const take = limit + 1;

    /**
     * The keyset predicate as a fragment that is ABSENT on the first page, rather than
     * `$1 IS NULL OR ...`. A generic plan cannot fold that OR away, and the keyset only
     * uses the index when the planner can see it as an index condition.
     */
    const afterMine = cursor
      ? Prisma.sql`AND (f."createdAt", f."postId") < (${cursor.at}::timestamp(3), ${cursor.id})`
      : Prisma.empty;
    const afterDirect = cursor
      ? Prisma.sql`AND (p."createdAt", p.id) < (${cursor.at}::timestamp(3), ${cursor.id})`
      : Prisma.empty;

    const rows = await withTenant(tenantId, (tx) =>
      tx.$queryRaw<FeedRow[]>`
        WITH mine AS (
          /*
           * VISIBILITY AND MEMBERSHIP ARE CHECKED BEFORE THE LIMIT, not after. A
           * FeedEntry row outlives the member leaving or being banned, and the post
           * being hidden. Filtering after the limit would drop those from a page
           * already cut to size, so a page could come back short, or empty, while
           * more remained, and the reader would be told the feed had ended.
           */
          SELECT f."postId" AS id, f."createdAt"
            FROM "FeedEntry" f
            JOIN "Post" p
              ON p.id = f."postId" AND p.status = 'VISIBLE'::"ContentStatus"
            JOIN "Membership" m
              ON m."tenantId" = f."tenantId" AND m."spaceId" = f."spaceId"
             AND m."userId" = f."userId" AND m.status = 'ACTIVE'::"MemberStatus"
           WHERE f."tenantId" = ${tenantId} AND f."userId" = ${userId}
             ${afterMine}
           ORDER BY f."createdAt" DESC, f."postId" DESC
           LIMIT ${take}
        ),
        spaces AS (
          SELECT m."spaceId" FROM "Membership" m
           WHERE m."tenantId" = ${tenantId} AND m."userId" = ${userId}
             AND m.status = 'ACTIVE'::"MemberStatus"
        ),
        direct AS (
          /*
           * LATERAL, ONE SPACE AT A TIME — not "spaceId = ANY(...)". Measured on 20
           * spaces and 20 000 posts: ANY() read the whole table and sorted it; this
           * reads the index a page per space and stops. The first grows with the table,
           * this grows with spaces times page size.
           */
          SELECT d.id, d."createdAt"
            FROM spaces s
            CROSS JOIN LATERAL (
              SELECT p.id, p."createdAt" FROM "Post" p
               WHERE p."tenantId" = ${tenantId} AND p."spaceId" = s."spaceId"
                 AND p.status = 'VISIBLE'::"ContentStatus" AND p."fannedOut" = false
                 ${afterDirect}
               ORDER BY p."createdAt" DESC, p.id DESC
               LIMIT ${take}
            ) d
        ),
        page AS (
          /*
           * CUT TO ONE PAGE HERE, BEFORE Post IS JOINED.
           *
           * The candidates are up to a page from the member's own rows plus up to a page
           * per space, so hundreds of ids for a page of twenty. Joining all of them to
           * Post first made the planner hash-join a SEQUENTIAL SCAN of every post in the
           * table: measured on 20 000 posts, "Seq Scan on Post (rows=20000)" for a page of
           * 21. That grows with the table, the very thing LATERAL is here to avoid. Cut
           * first, and the join is a page of primary-key lookups.
           */
          SELECT id, "createdAt" FROM (
            -- The flag should keep the halves apart; DISTINCT ON makes sure of it.
            SELECT DISTINCT ON (u.id) u.id, u."createdAt"
              FROM (SELECT id, "createdAt" FROM mine UNION ALL SELECT id, "createdAt" FROM direct) u
          ) candidates
          ORDER BY "createdAt" DESC, id DESC
          LIMIT ${take}
        )
        SELECT p.id, p."spaceId", p."authorId", p.body, p."reactionCount", p."commentCount", p."rankScore",
               to_char(pg."createdAt", 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS "cursorAt",
               to_char(pg."createdAt", 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "createdAt"
          FROM page pg
          JOIN "Post" p ON p.id = pg.id
         ORDER BY pg."createdAt" DESC, pg.id DESC
         LIMIT ${take}`,
    );

    const hasMore = rows.length > limit;
    const pageRows = rows.slice(0, limit);
    // The cursor is the OLDEST post on the page by (createdAt, id): taken from the
    // time-ordered rows BEFORE ranking reorders them for display.
    const last = pageRows[pageRows.length - 1];
    const nextCursor = hasMore && last ? encodeCursor({ at: last.cursorAt, id: last.id }) : null;

    /**
     * Ranked within the page, as decided. A COPY is sorted: `Array.prototype.sort` works
     * in place, and CLAUDE.md records an ordering check that could never fail because a
     * display loop had already sorted the array it then examined.
     */
    const ranked = [...pageRows].sort(
      (a, b) => b.rankScore - a.rankScore || (a.cursorAt < b.cursorAt ? 1 : a.cursorAt > b.cursorAt ? -1 : 0) || (a.id < b.id ? 1 : -1),
    );

    return res.json({
      posts: ranked.map(({ cursorAt: _cursorAt, ...post }) => post),
      nextCursor,
    });
  }));

  return router;
}
