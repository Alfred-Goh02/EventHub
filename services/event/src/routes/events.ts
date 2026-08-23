import { Router } from 'express';
import { z } from 'zod';
import { withTenant } from '../db.js';
import { Prisma } from '../generated/prisma/client.js';
import { requireAuth, requireRole } from '../auth.js';
import { ah } from '../async.js';

// The explicit `: Router` annotation is required, not stylistic. With
// declaration emit on, TypeScript must name this type in the generated .d.ts —
// and the inferred type lives in @types/express-serve-static-core, a transitive
// package pnpm does not hoist, so the declaration would reference an
// unresolvable path (TS2742). Annotating it names a type we do import.
export const events: Router = Router();

const MAX_PAGE = 100;
const DEFAULT_PAGE = 20;

const slugify = (title: string) =>
  title
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 80);

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
    const event = await withTenant(tenantId, (tx) =>
      tx.event.create({
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
        select: {
          id: true, title: true, slug: true, status: true, capacity: true,
          startsAt: true, endsAt: true, version: true, organiserId: true,
        },
      }),
    );

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
// Read one
// ---------------------------------------------------------------------------
events.get('/:id', requireAuth, ah(async (req, res) => {
  const event = await withTenant(req.auth!.tenantId, (tx) =>
    tx.event.findFirst({
      // findFirst, not findUnique: there is deliberately no @@unique on
      // (tenantId, slug), and the deletedAt predicate cannot be expressed in a
      // findUnique where-clause anyway.
      where: { id: req.params.id, deletedAt: null },
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
    const { count } = await tx.event.updateMany({
      where: {
        id,
        deletedAt: null,
        ...(ifMatch.kind === 'version' ? { version: ifMatch.version } : {}),
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
        select: {
          id: true, title: true, slug: true, description: true, status: true,
          capacity: true, startsAt: true, endsAt: true, version: true,
        },
      });
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
      select: { version: true },
    });

    return exists
      ? { kind: 'conflict' as const, currentVersion: exists.version }
      : { kind: 'missing' as const };
  });

  if (outcome.kind === 'missing') {
    return res.status(404).json({ error: 'event not found' });
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
      select: {
        id: true, title: true, slug: true, status: true, capacity: true,
        startsAt: true, endsAt: true, organiserId: true, version: true,
      },
    });

    // THE POINT OF THE OUTBOX: this insert is in the SAME transaction as the
    // status change above. You cannot atomically write to Postgres and publish
    // to Kafka - a crash between the two either loses the message or announces
    // something that never happened. Committing both together makes the
    // database the single source of truth, and a separate relay drains this
    // table afterwards. That relay is Chapter 4; the row is written now so the
    // pattern is in place before money depends on it.
    await tx.outboxMessage.create({
      data: {
        tenantId,
        eventType: 'event.published',
        schemaVersion: 1,
        // The Kafka partition key. Keyed by the event so all messages about one
        // event stay ordered - never by tenantId, which would put a whole
        // tenant's traffic on a single partition.
        aggregateId: event!.id,
        payload: {
          eventId: event!.id,
          tenantId,
          title: event!.title,
          slug: event!.slug,
          capacity: event!.capacity,
          startsAt: event!.startsAt.toISOString(),
          endsAt: event!.endsAt.toISOString(),
          organiserId: event!.organiserId,
        },
      },
    });

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
  const { count } = await withTenant(req.auth!.tenantId, (tx) =>
    tx.event.updateMany({
      where: { id: req.params.id, deletedAt: null },
      data: { deletedAt: new Date(), version: { increment: 1 } },
    }),
  );

  // Soft delete rather than DELETE, so the row survives for audit and Sessions
  // pointing at it do not become dangling. It also frees the slug, because the
  // unique index covers only rows where deletedAt IS NULL.
  if (count === 0) return res.status(404).json({ error: 'event not found' });

  return res.status(204).send();
}));
