import { Router } from 'express';
import { z } from 'zod';
import { withTenant } from '../db.js';
import { Prisma } from '../generated/prisma/client.js';
import { requireAuth, requireRole } from '../auth.js';
import { ah } from '../async.js';
import { SNAPSHOT_SELECT, emitEventSnapshot } from '../outbox.js';

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
      where: { id, deletedAt: null },
      data: { deletedAt: new Date(), version: { increment: 1 } },
    });

    if (count === 0) return { kind: 'missing' as const };

    const deleted = await tx.event.findFirst({ where: { id }, select: SNAPSHOT_SELECT });

    // A deletion MUST be announced. Ticketing has no foreign key to this table
    // and cannot discover the row is gone — without this message it keeps
    // selling tickets for an event that no longer exists.
    await emitEventSnapshot(tx, deleted!, 'DELETED');

    return { kind: 'deleted' as const };
  });

  if (outcome.kind === 'missing') return res.status(404).json({ error: 'event not found' });

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
      where: { id, deletedAt: null, status: { in: ['DRAFT', 'PUBLISHED'] } },
      data: { status: 'CANCELLED', version: { increment: 1 } },
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

    const cancelled = await tx.event.findFirst({ where: { id }, select: SNAPSHOT_SELECT });
    await emitEventSnapshot(tx, cancelled!, 'CANCELLED');

    return { kind: 'cancelled' as const, event: cancelled! };
  });

  if (outcome.kind === 'missing') {
    return res.status(404).json({ error: 'event not found' });
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
