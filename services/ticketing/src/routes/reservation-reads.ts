import { Router } from 'express';
import { z } from 'zod';
import { getAuth } from '@eventhub/auth-middleware';
import { ah } from '@eventhub/http';
import { withTenantRetry } from '../db.js';
import { requireAuth } from '../auth.js';
import { Prisma } from '../generated/prisma/client.js';
import { CURRENCY } from './pay.js';

/**
 * What a buyer can see of what they bought: GET /reservations/:id and GET /reservations.
 *
 * NOTHING ANSWERED THIS BEFORE. POST /reservations returned the hold and POST .../pay
 * returned the outcome, and after that a buyer had no way to ask again. It matters most
 * in the case the payment design is built around: the provider does not answer, /pay
 * says 202 "being confirmed", and the reconciler settles it later -- and the buyer could
 * never learn which way it went. Nor could anyone list the tickets they had bought.
 *
 * WHOSE. A reservation is its buyer's. Anyone else, the event's organiser included, is
 * told it does not exist: 404, not 403, because whether someone else bought tickets is
 * not theirs to learn. An ADMIN may read any one reservation in the tenant, for support.
 * The list is always the caller's own, an ADMIN's too. Row security keeps every other
 * tenant out.
 *
 * TIMES ARE THE DATABASE'S. Every timestamp is rendered by Postgres, and the countdown,
 * `holdSecondsLeft`, is computed against Postgres's now() -- the clock the sweeper
 * reclaims the seats by -- so a client counts down to when the seats actually go,
 * whatever its own clock or this process's says.
 *
 * PAGED ON (createdAt, id), newest first, the keyset Community's feed uses. An offset
 * would show a reservation again on the next page whenever the buyer took a new hold
 * between pages, and createdAt alone would skip one of two taken in the same millisecond.
 */
export const reservationReads: Router = Router();

/** Reservation ids: what this system issues, and nothing a database refuses. */
const ID = /^[A-Za-z0-9_-]{1,64}$/;
const STATUSES = ['HELD', 'PENDING_PAYMENT', 'CONFIRMED', 'EXPIRED', 'CANCELLED'] as const;
const PAGE_MAX = 50;

const listQuery = z.object({
  limit: z.coerce.number().int().min(1).max(PAGE_MAX).default(20),
  cursor: z.string().min(1).max(512).optional(),
  status: z.enum(STATUSES).optional(),
});

interface Cursor {
  /** UTC wall-clock with milliseconds, exactly as Postgres renders the column. */
  at: string;
  id: string;
}

/**
 * The cursor goes back to Postgres as the text Postgres rendered, never through a JS
 * Date (see Community's feed for why). The Date below only refuses an impossible one,
 * which the cast would otherwise answer with a 500.
 */
function decodeCursor(raw: string): Cursor | null {
  try {
    const { at, id } = JSON.parse(Buffer.from(raw, 'base64url').toString('utf8')) as { at?: unknown; id?: unknown };
    if (typeof at !== 'string' || typeof id !== 'string') return null;
    if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}$/.test(at)) return null;
    const check = new Date(`${at}Z`);
    if (Number.isNaN(check.getTime()) || check.toISOString().slice(0, 23) !== at) return null;
    if (!ID.test(id)) return null;
    return { at, id };
  } catch {
    return null;
  }
}

const encodeCursor = (c: Cursor) => Buffer.from(JSON.stringify(c), 'utf8').toString('base64url');

interface Row {
  id: string;
  eventId: string;
  status: string;
  /** Postgres's rendering, for the cursor. Never parsed here. */
  cursorAt: string;
  createdAt: string;
  confirmedAt: string | null;
  holdExpiresAt: string | null;
  holdSecondsLeft: number | null;
  eventTitle: string | null;
  eventSlug: string | null;
  eventStartsAt: string | null;
  eventEndsAt: string | null;
  paymentId: string | null;
  paymentStatus: string | null;
  paymentAmountCents: number | null;
  paymentCurrency: string | null;
}

interface Item {
  reservationId: string;
  ticketTypeId: string;
  name: string;
  quantity: number;
  unitPriceCents: number;
}

/**
 * Reservations matching `where`, newest first, each with its event and its LATEST
 * payment attempt -- a declined card followed by a second one is two attempts, and the
 * buyer is asking about the one that counts now. The hold's deadline and countdown are
 * shown only while it is a hold: once paid or lapsed, neither means anything.
 */
function readReservations(tx: Prisma.TransactionClient, where: Prisma.Sql, take: number) {
  return tx.$queryRaw<Row[]>`
    SELECT r.id,
           r."eventId",
           r.status::text AS status,
           to_char(r."createdAt", 'YYYY-MM-DD"T"HH24:MI:SS.MS') AS "cursorAt",
           to_char(r."createdAt", 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "createdAt",
           to_char(r."confirmedAt", 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "confirmedAt",
           CASE WHEN r.status = 'HELD' THEN to_char(r."expiresAt", 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') END AS "holdExpiresAt",
           CASE WHEN r.status = 'HELD' THEN greatest(0, ceil(extract(epoch FROM r."expiresAt" - now())))::int END AS "holdSecondsLeft",
           e.title AS "eventTitle",
           e.slug AS "eventSlug",
           to_char(e."startsAt", 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "eventStartsAt",
           to_char(e."endsAt", 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS "eventEndsAt",
           p.id AS "paymentId",
           p.status::text AS "paymentStatus",
           p."amountCents" AS "paymentAmountCents",
           p.currency AS "paymentCurrency"
      FROM "Reservation" r
      LEFT JOIN "EventProjection" e ON e."tenantId" = r."tenantId" AND e.id = r."eventId"
      LEFT JOIN LATERAL (
             SELECT q.id, q.status, q."amountCents", q.currency
               FROM "Payment" q
              WHERE q."tenantId" = r."tenantId" AND q."reservationId" = r.id
              ORDER BY q."startedAt" DESC, q.id DESC
              LIMIT 1
           ) p ON true
     WHERE ${where}
     ORDER BY r."createdAt" DESC, r.id DESC
     LIMIT ${take}`;
}

/** The items of these reservations, by reservation. The name is the ticket type's now. */
async function readItems(tx: Prisma.TransactionClient, tenantId: string, ids: string[]): Promise<Map<string, Item[]>> {
  const byReservation = new Map<string, Item[]>();
  if (ids.length === 0) return byReservation;
  const rows = await tx.$queryRaw<Item[]>`
    SELECT i."reservationId", i."ticketTypeId", t.name, i.quantity, i."unitPriceCents"
      FROM "ReservationItem" i
      JOIN "TicketType" t ON t."tenantId" = i."tenantId" AND t.id = i."ticketTypeId"
     WHERE i."tenantId" = ${tenantId}
       AND i."reservationId" IN (${Prisma.join(ids)})
     ORDER BY i."reservationId", i."unitPriceCents", t.name, i."ticketTypeId"`;
  for (const row of rows) byReservation.set(row.reservationId, [...(byReservation.get(row.reservationId) ?? []), row]);
  return byReservation;
}

function view(r: Row, items: Item[]) {
  return {
    id: r.id,
    status: r.status,
    event: { id: r.eventId, title: r.eventTitle, slug: r.eventSlug, startsAt: r.eventStartsAt, endsAt: r.eventEndsAt },
    items: items.map((i) => ({ ticketTypeId: i.ticketTypeId, name: i.name, quantity: i.quantity, unitPriceCents: i.unitPriceCents })),
    totalCents: items.reduce((sum, i) => sum + i.quantity * i.unitPriceCents, 0),
    currency: CURRENCY,
    createdAt: r.createdAt,
    holdExpiresAt: r.holdExpiresAt,
    holdSecondsLeft: r.holdSecondsLeft,
    confirmedAt: r.confirmedAt,
    payment: r.paymentId ? { id: r.paymentId, status: r.paymentStatus, amountCents: r.paymentAmountCents, currency: r.paymentCurrency } : null,
  };
}

reservationReads.get('/', requireAuth, ah(async (req, res) => {
  const parsed = listQuery.safeParse(req.query);
  if (!parsed.success) return res.status(400).json({ error: parsed.error.issues[0]?.message ?? 'invalid query' });
  const { tenantId, userId } = getAuth(req);
  const { limit, status } = parsed.data;

  let cursor: Cursor | null = null;
  if (parsed.data.cursor !== undefined) {
    cursor = decodeCursor(parsed.data.cursor);
    if (!cursor) return res.status(400).json({ error: 'that cursor is not one this list issued' });
  }

  const where = Prisma.sql`r."tenantId" = ${tenantId} AND r."userId" = ${userId}
    ${status ? Prisma.sql`AND r.status = ${status}::"ReservationStatus"` : Prisma.empty}
    ${cursor ? Prisma.sql`AND (r."createdAt", r.id) < (${cursor.at}::timestamp(3), ${cursor.id})` : Prisma.empty}`;

  // One more than a page, so "there is more" is a fact rather than a guess.
  const page = await withTenantRetry(tenantId, async (tx) => {
    const rows = await readReservations(tx, where, limit + 1);
    const shown = rows.slice(0, limit);
    return { more: rows.length > limit, shown, items: await readItems(tx, tenantId, shown.map((r) => r.id)) };
  });
  const last = page.shown.at(-1);
  return res.json({
    data: page.shown.map((r) => view(r, page.items.get(r.id) ?? [])),
    nextCursor: page.more && last ? encodeCursor({ at: last.cursorAt, id: last.id }) : null,
  });
}));

reservationReads.get('/:id', requireAuth, ah(async (req, res) => {
  const id = req.params['id']!;
  if (!ID.test(id)) return res.status(404).json({ error: 'reservation not found' });
  const { tenantId, userId, role } = getAuth(req);
  const whose = role === 'ADMIN' ? Prisma.empty : Prisma.sql`AND r."userId" = ${userId}`;
  const found = await withTenantRetry(tenantId, async (tx) => {
    const [row] = await readReservations(tx, Prisma.sql`r."tenantId" = ${tenantId} AND r.id = ${id} ${whose}`, 1);
    if (!row) return null;
    return view(row, (await readItems(tx, tenantId, [row.id])).get(row.id) ?? []);
  });
  if (!found) return res.status(404).json({ error: 'reservation not found' });
  return res.json(found);
}));
