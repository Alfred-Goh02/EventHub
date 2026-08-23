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
