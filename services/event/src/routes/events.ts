import { Router } from 'express';
import { randomBytes } from 'node:crypto';
import { z } from 'zod';
import { withTenant } from '../db.js';
import { Prisma } from '../generated/prisma/client.js';
import { requireAuth, requireRole } from '../auth.js';
import { ah } from '@eventhub/http';
import { SNAPSHOT_SELECT, emitEventSnapshot } from '../outbox.js';

// The explicit `: Router` annotation is required, not stylistic. With
// declaration emit on, TypeScript must name this type in the generated .d.ts —
// and the inferred type lives in @types/express-serve-static-core, a transitive
// package pnpm does not hoist, so the declaration would reference an
// unresolvable path (TS2742). Annotating it names a type we do import.
export const events: Router = Router();

const MAX_PAGE = 100;
const DEFAULT_PAGE = 20;

/**
 * A URL-safe slug, which for a title in any non-Latin script is EMPTY.
 *
 * The transform keeps [a-z0-9] and throws everything else away, so:
 *
 *   'Summer Party'    -> 'summer-party'
 *   '活动预告'          -> ''
 *   'Летний концерт'  -> ''
 *   'مهرجان'          -> ''
 *
 * There is a partial unique index on (tenantId, slug) WHERE deletedAt IS
 * NULL, so the FIRST non-Latin title in a tenant took the empty slug and every
 * one after it was refused — with "an event with that title already exists",
 * about two titles that share nothing at all. Measured: a CJK title created
 * fine, a Cyrillic one came back 409.
 *
 * So a tenant could hold exactly one event whose title was not mostly ASCII,
 * and the message told them the wrong reason.
 *
 * The empty case falls back to a random suffix rather than transliterating.
 * Transliteration needs a table per script and gets opinionated fast, and
 * nothing in this service looks an event up BY slug — reads go by id. The slug
 * is a human-readable convenience under a uniqueness constraint, so the thing
 * that matters is that it exists and does not collide.
 *
 * A duplicate LATIN title still collides, and that is left alone: two events
 * genuinely called 'Summer Party' in one tenant is a real conflict, and the
 * message is true in that case.
 */
const slugify = (title: string): string => {
  const base = title
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80);

  return base === '' ? `event-${randomBytes(5).toString('hex')}` : base;
};

const createEvent = z
  .object({
    title: z.string().min(1).max(200),
    description: z.string().max(5000).optional(),
    capacity: z.number().int().positive(),
    startsAt: z.coerce.date(),
    endsAt: z.coerce.date(),
    venueId: z.string().optional(),
  })
  .refine((v) => v.endsAt > v.startsAt, {
    message: 'endsAt must be after startsAt',
    path: ['endsAt'],
  });

/**
 * Keyset (cursor) pagination, not OFFSET.
 *
 * OFFSET 40000 makes Postgres walk and discard 40,000 rows for every page. It
 * is also wrong on a moving list: insert a row while someone is paging and they
 * see an entry twice, or miss one entirely. A cursor says "resume after THIS
 * row", which is stable and uses the index.
 *
 * The cursor carries (startsAt, id) because startsAt alone is not unique — two
 * events starting at the same instant would make the boundary ambiguous and
 * silently drop rows.
 *
 * It is base64 to discourage clients from parsing it, not to hide anything. It
 * is deliberately NOT signed: RLS filters every query by tenant, so a forged
 * cursor can only ever page within the caller's own tenant. The isolation does
 * not depend on the cursor being trustworthy.
 */
const encodeCursor = (e: { startsAt: Date; id: string }) =>
  Buffer.from(JSON.stringify({ s: e.startsAt.toISOString(), i: e.id })).toString('base64url');

function decodeCursor(raw: string): { startsAt: Date; id: string } | null {
  try {
    const { s, i } = JSON.parse(Buffer.from(raw, 'base64url').toString('utf8'));
    const startsAt = new Date(s);
    if (typeof i !== 'string' || Number.isNaN(startsAt.getTime())) return null;
    return { startsAt, id: i };
  } catch {
    return null;
  }
}

// ---------------------------------------------------------------------------
// Create
// ---------------------------------------------------------------------------
events.post('/', requireAuth, requireRole('ORGANIZER', 'ADMIN'), ah(async (req, res) => {
  const parsed = createEvent.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.flatten() });
  }

  const { tenantId, userId } = req.auth!;
  const body = parsed.data;

  try {
    const event = await withTenant(tenantId, async (tx) => {
      const created = await tx.event.create({
        data: {
          tenantId,
          // Taken from the verified token, NEVER from the request body. There is
          // no foreign key to fall back on — User lives in Auth's database,
          // which this service cannot even connect to — so if this came from the
          // client, any member could attribute events to a fabricated user, and
          // Chapter 4's payouts would follow that attribution.
          organiserId: userId,
          title: body.title,
          slug: slugify(body.title),
          description: body.description ?? null,
          capacity: body.capacity,
          startsAt: body.startsAt,
          endsAt: body.endsAt,
          venueId: body.venueId ?? null,
        },
        select: SNAPSHOT_SELECT,
      });

      // Emitted on creation too, not only on publish. events.activity is a
      // snapshot topic describing the current state of an event, so a consumer
      // that has never heard of a DRAFT event cannot reason about it later
      // becoming PUBLISHED. Ticketing simply ignores anything not PUBLISHED.
      await emitEventSnapshot(tx, created, 'CREATED');
      return created;
    });

    return res.status(201).json(event);
  } catch (err) {
    if (err instanceof Prisma.PrismaClientKnownRequestError) {
      // The partial unique index on (tenantId, slug) WHERE deletedAt IS NULL.
      if (err.code === 'P2002') {
        return res.status(409).json({ error: 'an event with that title already exists' });
      }
      // Composite FK to (tenantId, venueId): a venue id from another tenant is
      // not merely forbidden, it does not exist as far as this query is concerned.
      if (err.code === 'P2003') {
        return res.status(400).json({ error: 'unknown venue' });
      }
    }
    throw err;
  }
}));

/**
 * A DRAFT event is not visible to someone who cannot publish one.
 *
 * ATTENDEE is the DEFAULT role and the schema calls it "buys tickets, joins
 * the community" — so attendees are customers living in the same tenant as the
 * organisers. Neither read route filtered on status, so any of them could list
 * every draft in the tenant and fetch one by id, title and description and all.
 * Reproduced: an attendee reading an unpublished event got 200 and its title.
 *
 * That contradicts the meaning the rest of the system gives this field.
 * Ticketing refuses to sell anything that is not PUBLISHED, and /publish exists
 * precisely to mark the moment an event becomes public. An organiser drafting
 * next quarter's programme should not be doing it in front of the customers.
 *
 * ONLY draft is hidden. CANCELLED and COMPLETED stay visible to everyone, and
 * that is not an oversight: someone holding a ticket for a cancelled event is
 * the person who most needs to see it.
 */
const canSeeDrafts = (role: string): boolean => role === 'ORGANIZER' || role === 'ADMIN';

/**
 * Who may CANCEL or DELETE an event, as opposed to edit one.
 *
 * Every write route already requires ORGANIZER or ADMIN, and until now that was
 * the whole check -- so any organiser could cancel any other organiser's event.
 * organiserId was set from the token at creation and carried into the Kafka
 * snapshot, and no route ever read it back.
 *
 * That was not a decision anybody made. The security boundary in this system is
 * the TENANT -- every RLS policy reads app.tenant_id and nothing else -- so once
 * tenant scoping was right the routes felt finished. Ticketing later drew the
 * line differently: /pay filters on userId, with a comment saying RLS scopes to
 * a tenant and not to a user. The Event service never did.
 *
 * EDITING STAYS OPEN, deliberately. Patching a title or publishing an event is
 * collaborative work inside one organisation, and locking it to the creator
 * makes a team account worse for no safety gained.
 *
 * Cancel and delete are different in kind. Both propagate to Ticketing and stop
 * sales for people who already hold tickets, and neither has an undo. A larger
 * blast radius earns a higher bar.
 *
 * ADMIN keeps the override, and that is not politeness -- pure ownership means
 * an organiser who leaves takes their events with them.
 */
const ownerScope = (auth: { role: string; userId: string }) =>
  auth.role === 'ADMIN' ? {} : { organiserId: auth.userId };

/**
 * Turn a zero-row destructive write into the right answer.
 *
 * 403, not 404, when the row exists and the caller is not its organiser. The
 * caller can already GET this event -- same tenant, and not a draft they cannot
 * see -- so hiding it would deny something they can plainly observe. That is the
 * opposite case to another tenant's row, which 404s precisely because
 * confirming it exists is itself the leak.
 */
type OwnershipMiss =
  | { kind: 'missing' }
  | { kind: 'not-owner' }
  | { kind: 'owned'; status: string };

async function classifyMiss(
  tx: Prisma.TransactionClient,
  id: string,
  auth: { role: string; userId: string },
): Promise<OwnershipMiss> {
  const exists = await tx.event.findFirst({
    where: { id, deletedAt: null },
    select: { status: true, organiserId: true },
  });

  if (!exists) return { kind: 'missing' };
  if (auth.role !== 'ADMIN' && exists.organiserId !== auth.userId) return { kind: 'not-owner' };
  return { kind: 'owned', status: exists.status };
}

/**
 * Added to every read for a caller who cannot publish.
 *
 * NOT rather than `status: { not: ... }`, because it has to COMPOSE with the
 * caller's own ?status filter rather than replace it. My first attempt spread
 * this alongside `...(status ? { status } : {})` and whichever came last won:
 * one ordering let ?status=DRAFT walk straight past the filter, the other threw
 * away a legitimate ?status=PUBLISHED. NOT is a separate key, so both
 * constraints apply — ?status=DRAFT for an attendee is simply an empty result
 * rather than a bypass or an error.
 */
const draftFilter = (role: string) =>
  canSeeDrafts(role) ? {} : { NOT: { status: 'DRAFT' as const } };
// ---------------------------------------------------------------------------
// List
// ---------------------------------------------------------------------------
events.get('/', requireAuth, ah(async (req, res) => {
  const query = z
    .object({
      limit: z.coerce.number().int().min(1).max(MAX_PAGE).default(DEFAULT_PAGE),
      cursor: z.string().optional(),
      status: z.enum(['DRAFT', 'PUBLISHED', 'CANCELLED', 'COMPLETED']).optional(),
    })
    .safeParse(req.query);

  if (!query.success) {
    return res.status(400).json({ error: query.error.flatten() });
  }

  const { limit, cursor, status } = query.data;

  const after = cursor ? decodeCursor(cursor) : null;
  if (cursor && !after) {
    return res.status(400).json({ error: 'malformed cursor' });
  }

  const rows = await withTenant(req.auth!.tenantId, (tx) =>
    tx.event.findMany({
      where: {
        // RLS does NOT filter soft-deleted rows — it only knows about tenants.
        // This predicate belongs in every read, and forgetting it is the most
        // likely bug in this file.
        deletedAt: null,
        ...(status ? { status } : {}),
        // A caller who cannot publish never sees a DRAFT, including when they
        // ask for one by name -- ?status=DRAFT must not be a way around this.
        ...draftFilter(req.auth!.role),
        ...(after
          ? {
              OR: [
                { startsAt: { gt: after.startsAt } },
                { startsAt: after.startsAt, id: { gt: after.id } },
              ],
            }
          : {}),
      },
      orderBy: [{ startsAt: 'asc' }, { id: 'asc' }],
      // One extra row to discover whether another page exists, without a
      // second COUNT query over the whole table.
      take: limit + 1,
      select: {
        id: true, title: true, slug: true, status: true, capacity: true,
        startsAt: true, endsAt: true, version: true, organiserId: true,
      },
    }),
  );

  const hasMore = rows.length > limit;
  const page = hasMore ? rows.slice(0, limit) : rows;
  const last = page[page.length - 1];

  return res.json({
    data: page,
    nextCursor: hasMore && last ? encodeCursor(last) : null,
  });
}));

// ---------------------------------------------------------------------------
// Search, degraded
// ---------------------------------------------------------------------------

/**
 * The most rows the degraded path will ever return, whatever is asked.
 *
 * The cap is the point of this route. It exists for when OpenSearch is down, and
 * an uncapped fallback would turn a search outage into a Postgres outage — every
 * search the cluster was absorbing, now arriving here as an ILIKE.
 */
const DEGRADED_CAP = 50;

/**
 * What the Search service calls when OpenSearch is unavailable.
 *
 * DECLARED BEFORE /:id, because Express matches in order and /:id would take
 * "search" as an event id.
 *
 * Search forwards the CALLER'S bearer token rather than holding a credential of
 * its own, so the tenant comes from the signed claim and RLS enforces it here
 * exactly as on every other read.
 *
 * PUBLISHED only, for every role: this stands in for public search, which shows
 * nothing else. No relevance, no facets, no typo tolerance — a title substring,
 * soonest first, which is what "degraded" honestly means.
 *
 * The caller's text is escaped before it becomes a pattern. Unescaped, a search
 * for "%" is a pattern that matches every title.
 */
events.get('/search', requireAuth, ah(async (req, res) => {
  const query = z
    .object({
      q: z.string().trim().min(1).max(200).optional(),
      includePast: z.enum(['true', 'false']).default('false').transform((v) => v === 'true'),
    })
    .strict()
    .safeParse(req.query);

  if (!query.success) {
    return res.status(400).json({ error: query.error.flatten() });
  }

  const { q, includePast } = query.data;
  const pattern = q ? `%${q.replace(/[\\%_]/g, (c) => `\\${c}`)}%` : null;

  const rows = await withTenant(req.auth!.tenantId, (tx) =>
    tx.$queryRaw<Array<{ id: string; slug: string; title: string; startsAt: Date; endsAt: Date }>>`
      SELECT e.id, e.slug, e.title, e."startsAt", e."endsAt"
        FROM "Event" e
       WHERE e."deletedAt" IS NULL
         AND e.status = 'PUBLISHED'
         AND (${includePast} OR e."startsAt" >= now())
         AND (${pattern}::text IS NULL OR e.title ILIKE ${pattern} ESCAPE '\\')
       ORDER BY e."startsAt" ASC, e.id ASC
       LIMIT ${DEGRADED_CAP}
    `,
  );

  return res.json({ data: rows, cap: DEGRADED_CAP });
}));

// ---------------------------------------------------------------------------
// Read one
// ---------------------------------------------------------------------------
events.get('/:id', requireAuth, ah(async (req, res) => {
  const event = await withTenant(req.auth!.tenantId, (tx) =>
    tx.event.findFirst({
      // findFirst, not findUnique: there is deliberately no @@unique on
      // (tenantId, slug), and the deletedAt predicate cannot be expressed in a
      // findUnique where-clause anyway.
      // 404 rather than 403 for a draft, matching how this file already treats
      // another tenant's row: refusing tells the caller the id is real.
      where: { id: req.params.id, deletedAt: null, ...draftFilter(req.auth!.role) },
      select: {
        id: true, title: true, slug: true, description: true, status: true,
        capacity: true, startsAt: true, endsAt: true, version: true,
        organiserId: true, venueId: true, createdAt: true,
      },
    }),
  );

  // A row belonging to another tenant is filtered out by RLS and arrives here
  // as null — indistinguishable from "no such id", which is the correct
  // behaviour: 404 reveals nothing about other tenants' data.
  if (!event) return res.status(404).json({ error: 'event not found' });

  return res.json(event);
}));

// ---------------------------------------------------------------------------
// Optimistic concurrency
// ---------------------------------------------------------------------------

/**
 * Parses an If-Match header.
 *
 * RFC 9110 entity-tags are QUOTED, so the header value is literally `"3"`, not
 * `3`. `Number(req.header('If-Match'))` yields NaN, which becomes
 * `where: { version: NaN }` and a spurious 409 on a request that should have
 * succeeded. `*` is also legal and means "any current representation" — an
 * existence check with no version predicate.
 */
type IfMatch =
  | { kind: 'missing' }
  | { kind: 'any' }
  | { kind: 'version'; version: number }
  | { kind: 'invalid' };

function parseIfMatch(raw: string | undefined): IfMatch {
  if (raw === undefined) return { kind: 'missing' };

  const value = raw.trim();
  if (value === '*') return { kind: 'any' };

  // W/ marks a weak validator; the quotes are part of the syntax, not decoration.
  const unquoted = value.replace(/^W\//, '').replace(/^"|"$/g, '');
  const version = Number(unquoted);

  if (!Number.isInteger(version) || version < 1) return { kind: 'invalid' };
  return { kind: 'version', version };
}

const patchEvent = z
  .object({
    title: z.string().min(1).max(200).optional(),
    description: z.string().max(5000).nullable().optional(),
    capacity: z.number().int().positive().optional(),
    startsAt: z.coerce.date().optional(),
    endsAt: z.coerce.date().optional(),
  })
  .refine((v) => Object.keys(v).length > 0, { message: 'no fields to update' });

events.patch('/:id', requireAuth, requireRole('ORGANIZER', 'ADMIN'), ah(async (req, res) => {
  const ifMatch = parseIfMatch(req.header('If-Match'));

  if (ifMatch.kind === 'missing') {
    return res.status(428).json({ error: 'If-Match header is required' });
  }
  // 400, not 409: a malformed header is a bad request, not a version conflict.
  if (ifMatch.kind === 'invalid') {
    return res.status(400).json({ error: 'malformed If-Match header' });
  }

  const parsed = patchEvent.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.flatten() });
  }

  const { tenantId } = req.auth!;
  const id = req.params.id;

  const outcome = await withTenant(tenantId, async (tx) => {
    /**
     * The dates have to be checked against the MERGED row, not the request.
     *
     * POST refuses a backwards pair with a Zod .refine(). PATCH is PARTIAL, so
     * half the comparison lives in the database: moving startsAt past the
     * stored endsAt is a backwards pair that no refinement on the body can
     * see. Both variants were accepted with a 200 before this.
     *
     * This read is the error message, not the guarantee. Two concurrent
     * patches could each merge to something valid and still land on something
     * invalid, and an If-Match of * or none does not stop them. The CHECK
     * constraint added in 20260905083200_event_ends_after_starts is what makes
     * that impossible; this exists so the ordinary case gets a 400 that names
     * the problem instead of a 500 from a constraint violation.
     */
    if (parsed.data.startsAt !== undefined || parsed.data.endsAt !== undefined) {
      const current = await tx.event.findFirst({
        where: { id, deletedAt: null },
        select: { startsAt: true, endsAt: true },
      });

      if (current) {
        const startsAt = parsed.data.startsAt ?? current.startsAt;
        const endsAt = parsed.data.endsAt ?? current.endsAt;
        if (endsAt <= startsAt) {
          return { kind: 'bad-dates' as const, startsAt, endsAt };
        }
      }
    }
    const { count } = await tx.event.updateMany({
      where: {
        id,
        deletedAt: null,
        ...(ifMatch.kind === 'version' ? { version: ifMatch.version } : {}),
        // Capacity is fixed once an event is published.
        //
        // This service structurally cannot check how many tickets have been
        // sold — that lives in eventhub_ticketing, behind a REVOKE CONNECT it
        // has no credential for. So lowering capacity from 500 to 50 after 300
        // tickets were sold would put the system in a state its own invariants
        // call impossible, silently and with nothing to detect it.
        //
        // Adding the predicate here rather than checking first keeps it atomic:
        // a concurrent publish cannot slip between a check and the update.
        ...(parsed.data.capacity !== undefined ? { status: 'DRAFT' as const } : {}),
      },
      data: {
        ...parsed.data,
        // Prisma has no @version, so nothing increments this automatically.
        // Omit it and count is still 1, the write succeeds, and every later
        // If-Match on the same stale value also succeeds — optimistic
        // concurrency silently does nothing while appearing to work, which is
        // worse than not having it at all.
        version: { increment: 1 },
      },
    });

    if (count === 1) {
      const updated = await tx.event.findFirst({
        where: { id, deletedAt: null },
        select: SNAPSHOT_SELECT,
      });
      // Emitted in the same transaction, so a downstream projection cannot
      // drift. Without this, a consumer would hold whatever it learned at
      // publish time and never hear about a change — an organiser publishes at
      // capacity 500, patches to 200, and Ticketing keeps selling 500.
      await emitEventSnapshot(tx, updated!, 'UPDATED');
      return { kind: 'updated' as const, event: updated! };
    }

    // count === 0 has four possible causes: no such id, stale version, another
    // tenant's row, or soft-deleted. Mapping all four to 409 makes a genuinely
    // missing id report a version conflict.
    //
    // The disambiguation runs INSIDE this transaction, on `tx`. Re-reading with
    // the bare `prisma` client would escape the tenant context and build a
    // cross-tenant existence oracle — one line, and it looks entirely correct.
    const exists = await tx.event.findFirst({
      where: { id, deletedAt: null },
      select: { version: true, status: true },
    });

    if (!exists) return { kind: 'missing' as const };

    // Distinguish "you tried to change capacity on a published event" from a
    // genuine version conflict. Both produce count 0, and reporting a version
    // conflict for a rule violation sends the client into a retry loop that can
    // never succeed.
    if (parsed.data.capacity !== undefined && exists.status !== 'DRAFT') {
      return { kind: 'capacity-locked' as const, status: exists.status };
    }

    return { kind: 'conflict' as const, currentVersion: exists.version };
  });

  if (outcome.kind === 'missing') {
    return res.status(404).json({ error: 'event not found' });
  }

  if (outcome.kind === 'bad-dates') {
    // 400 rather than 409: this is a malformed request, not a conflict with
    // another writer, and a 409 would invite a retry that cannot succeed.
    return res.status(400).json({
      error: 'endsAt must be after startsAt',
      startsAt: outcome.startsAt,
      endsAt: outcome.endsAt,
    });
  }

  if (outcome.kind === 'capacity-locked') {
    return res.status(409).json({
      error: `capacity is fixed once an event is published (status is ${outcome.status})`,
    });
  }

  if (outcome.kind === 'conflict') {
    // Returning the current version is safe precisely because that lookup was
    // tenant-scoped: it can only ever describe a row the caller may already see.
    res.setHeader('ETag', `"${outcome.currentVersion}"`);
    return res.status(409).json({
      error: 'version conflict - the event changed since you read it',
      currentVersion: outcome.currentVersion,
    });
  }

  res.setHeader('ETag', `"${outcome.event.version}"`);
  return res.json(outcome.event);
}));

// ---------------------------------------------------------------------------
// Publish - the first write that must also reach Kafka
// ---------------------------------------------------------------------------
events.post('/:id/publish', requireAuth, requireRole('ORGANIZER', 'ADMIN'), ah(async (req, res) => {
  const { tenantId } = req.auth!;
  const id = req.params.id;

  const outcome = await withTenant(tenantId, async (tx) => {
    // Only a DRAFT may be published, and that condition lives in the WHERE
    // clause rather than a read-then-check. It is therefore atomic: two
    // concurrent publishes cannot both match, so only one can emit an outbox row.
    const { count } = await tx.event.updateMany({
      where: { id, deletedAt: null, status: 'DRAFT' },
      data: { status: 'PUBLISHED', version: { increment: 1 } },
    });

    if (count === 0) {
      const exists = await tx.event.findFirst({
        where: { id, deletedAt: null },
        select: { status: true },
      });
      return exists
        ? { kind: 'wrong-state' as const, status: exists.status }
        : { kind: 'missing' as const };
    }

    const event = await tx.event.findFirst({
      where: { id },
      select: SNAPSHOT_SELECT,
    });

    // THE POINT OF THE OUTBOX: this write is in the SAME transaction as the
    // status change above. You cannot atomically write to Postgres and publish
    // to Kafka - a crash between the two either loses the message or announces
    // something that never happened. Committing both together makes the database
    // the single source of truth, and a separate relay drains the table.
    await emitEventSnapshot(tx, event!, 'PUBLISHED');

    return { kind: 'published' as const, event: event! };
  });

  if (outcome.kind === 'missing') {
    return res.status(404).json({ error: 'event not found' });
  }
  if (outcome.kind === 'wrong-state') {
    return res.status(409).json({ error: `cannot publish an event in state ${outcome.status}` });
  }

  return res.json(outcome.event);
}));

// ---------------------------------------------------------------------------
// Soft delete
// ---------------------------------------------------------------------------
events.delete('/:id', requireAuth, requireRole('ORGANIZER', 'ADMIN'), ah(async (req, res) => {
  const id = req.params.id;

  const outcome = await withTenant(req.auth!.tenantId, async (tx) => {
    // Soft delete rather than DELETE, so the row survives for audit and Sessions
    // pointing at it do not become dangling. It also frees the slug, because the
    // unique index covers only rows where deletedAt IS NULL.
    const { count } = await tx.event.updateMany({
      where: { id, deletedAt: null, ...ownerScope(req.auth!) },
      data: { deletedAt: new Date(), version: { increment: 1 } },
    });

    if (count === 0) {
      const miss = await classifyMiss(tx, id, req.auth!);
      // 'owned' here means already deleted, not a state clash: delete has no
      // source-state predicate that could have failed.
      return miss.kind === 'not-owner' ? miss : { kind: 'missing' as const };
    }

    const deleted = await tx.event.findFirst({ where: { id }, select: SNAPSHOT_SELECT });

    // A deletion MUST be announced. Ticketing has no foreign key to this table
    // and cannot discover the row is gone — without this message it keeps
    // selling tickets for an event that no longer exists.
    await emitEventSnapshot(tx, deleted!, 'DELETED');

    return { kind: 'deleted' as const };
  });

  if (outcome.kind === 'missing') return res.status(404).json({ error: 'event not found' });
  if (outcome.kind === 'not-owner') {
    return res.status(403).json({
      error: 'only the organiser who created this event, or an admin, can delete it',
    });
  }

  return res.status(204).send();
}));

// ---------------------------------------------------------------------------
// Cancel
// ---------------------------------------------------------------------------
//
// Distinct from delete. A cancelled event still happened as far as everyone
// downstream is concerned: refunds are owed, attendees must be told, and the
// record stays visible. A deleted event is one that should not have existed.
//
// Ticketing's safety story depends on this arriving — it is the signal to stop
// selling and start refunding, and there is no other way for it to find out.
events.post('/:id/cancel', requireAuth, requireRole('ORGANIZER', 'ADMIN'), ah(async (req, res) => {
  const id = req.params.id;

  const outcome = await withTenant(req.auth!.tenantId, async (tx) => {
    // The allowed source states live in the WHERE clause, so two concurrent
    // cancels cannot both match and emit.
    const { count } = await tx.event.updateMany({
      where: {
        id,
        deletedAt: null,
        status: { in: ['DRAFT', 'PUBLISHED'] },
        // In the WHERE clause rather than a read-then-check, so it stays atomic
        // for the same reason the status predicate is.
        ...ownerScope(req.auth!),
      },
      data: { status: 'CANCELLED', version: { increment: 1 } },
    });

    if (count === 0) {
      const miss = await classifyMiss(tx, id, req.auth!);
      if (miss.kind === 'owned') {
        return { kind: 'wrong-state' as const, status: miss.status };
      }
      return miss;
    }

    const cancelled = await tx.event.findFirst({ where: { id }, select: SNAPSHOT_SELECT });
    await emitEventSnapshot(tx, cancelled!, 'CANCELLED');

    return { kind: 'cancelled' as const, event: cancelled! };
  });

  if (outcome.kind === 'missing') {
    return res.status(404).json({ error: 'event not found' });
  }
  if (outcome.kind === 'not-owner') {
    return res.status(403).json({
      error: 'only the organiser who created this event, or an admin, can cancel it',
    });
  }
  if (outcome.kind === 'wrong-state') {
    return res.status(409).json({ error: `cannot cancel an event in state ${outcome.status}` });
  }

  return res.json({
    id: outcome.event.id,
    status: outcome.event.status,
    version: outcome.event.version,
  });
}));
