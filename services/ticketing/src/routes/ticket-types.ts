import { Router } from 'express';
import { z } from 'zod';
import { getAuth } from '@eventhub/auth-middleware';
import { ah } from '@eventhub/http';
import { withTenantRetry } from '../db.js';
import { requireAuth, requireRole } from '../auth.js';
import type { Prisma } from '../generated/prisma/client.js';

/**
 * The inventory editor: an organiser's ticket types for an event.
 *
 * NOTHING CREATED THESE. Reservations, payments, the sweeper and every verifier of them
 * took a TicketType row as given, and the verifiers inserted their own -- so through the
 * API no organiser could put a ticket on sale, and no attendee could buy one. Walking
 * every service through the gateway found it.
 *
 * Ticketing owns them because it owns the inventory: `remaining`, `held` and `sold` are
 * this service's counters, and the buyer path's conditional UPDATE is what keeps them
 * honest. The paths are /ticket-types rather than /events/:id/ticket-types because the
 * gateway sends /events to the Event service.
 *
 * WHO. An ADMIN manages any event in the tenant; an ORGANIZER only the events they
 * organise, by the organiserId the projection carries. An attendee may list the types of
 * a PUBLISHED event and nothing else -- a draft's tickets are as unannounced as the
 * draft. Everything is under row security for the token's tenant, so another tenant's
 * event or ticket type is not found, not forbidden.
 *
 * WRITES SERIALISE ON THE EVENT. Two rules span rows: the totals of an event's types
 * may not exceed its capacity, and two types of one event may not share a name. A
 * check of either that only READS is write skew -- two creates each see room for
 * themselves and both commit -- so every write first locks the event's projection row
 * (SELECT ... FOR NO KEY UPDATE), and the second writer waits and then sees the first.
 *
 * EDITS ARE OPTIMISTIC (the guide's inventory editor): an organiser looks at a form for
 * minutes, and no lock can be held across that. PATCH requires If-Match with the
 * REVISION the form was read at; a stale one is 412, "reload". Not `version`: every
 * reservation, expiry and payment bumps that, so a form open during a sale would always
 * be stale. `revision` moves only with an edit.
 * A total may not go below what is already held or sold, and `remaining` is derived
 * from the new total in the same statement. A price change does not reach existing
 * holds: each reservation captured its unit price when it was taken.
 */

export const ticketTypes: Router = Router();

const MAX_PRICE_CENTS = 10_000_000; // 100,000.00 in the event's currency
const MAX_TOTAL = 1_000_000;

/**
 * Text Postgres can store as written. A NUL is refused by Postgres itself, as a 500; a
 * lone surrogate is stored as U+FFFD by the driver, so two different names could become
 * the same one after the duplicate check passed. Refused here, before either.
 */
const storable = (s: string) => s.isWellFormed() && !s.includes('\u0000');
const name = z
  .string()
  .trim()
  .min(1, 'a name is required')
  .max(100, 'a name is at most 100 characters')
  .refine(storable, 'a name must be well-formed text');
/** Event and ticket type ids: what this system issues, and nothing a database refuses. */
const ID = /^[A-Za-z0-9_-]{1,64}$/;
const priceCents = z.number().int('priceCents is a whole number of cents').min(0).max(MAX_PRICE_CENTS);
const total = z.number().int('total is a whole number').min(1).max(MAX_TOTAL);

const createBody = z.object({ eventId: z.string().regex(ID, 'eventId is not an event id'), name, priceCents, total }).strict();
const patchBody = z
  .object({ name: name.optional(), priceCents: priceCents.optional(), total: total.optional() })
  .strict()
  .refine((v) => v.name !== undefined || v.priceCents !== undefined || v.total !== undefined, 'nothing to change');

const view = (t: { id: string; eventId: string; name: string; priceCents: number; total: number; remaining: number; held: number; sold: number; revision: number }) => ({
  id: t.id,
  eventId: t.eventId,
  name: t.name,
  priceCents: t.priceCents,
  total: t.total,
  remaining: t.remaining,
  held: t.held,
  sold: t.sold,
  revision: t.revision,
});

/** A refusal raised inside a transaction, so it rolls back, and answered outside it. */
class Refusal extends Error {
  constructor(
    readonly status: number,
    readonly body: Record<string, unknown>,
  ) {
    super(String(body['error']));
  }
}

interface LockedEvent {
  status: string;
  deleted: boolean;
  capacity: number;
  organiserId: string;
}

/**
 * Locks the event's projection row for the rest of the transaction and checks the
 * caller may manage it. Not found and not-theirs are both 404 for an organiser: whether
 * a colleague's event exists is not theirs to learn either.
 */
async function lockManagedEvent(tx: Prisma.TransactionClient, eventId: string, auth: { userId: string; role: string }): Promise<LockedEvent> {
  // NO KEY UPDATE, not UPDATE: editors of one event still queue behind each other, and
  // this is the weakest lock that does that; a buyer's foreign-key check takes KEY SHARE
  // on this row, which FOR UPDATE would also block. (Not separately asserted: an editor's
  // transaction is too short for a buyer to notice either way.) Both take this row
  // before any ticket type's, so the two cannot wait on each other in a cycle.
  const rows = await tx.$queryRaw<LockedEvent[]>`
    SELECT status, deleted, capacity, "organiserId"
      FROM "EventProjection"
     WHERE id = ${eventId}
       FOR NO KEY UPDATE`;
  const event = rows[0];
  if (!event || (auth.role !== 'ADMIN' && event.organiserId !== auth.userId)) {
    throw new Refusal(404, { error: 'no such event' });
  }
  if (event.deleted || event.status === 'CANCELLED' || event.status === 'COMPLETED') {
    throw new Refusal(409, { error: `tickets cannot be changed on a ${event.deleted ? 'deleted' : event.status.toLowerCase()} event` });
  }
  return event;
}

/** The other types of the event: their totals, and whether one already has this name. */
async function siblings(tx: Prisma.TransactionClient, eventId: string, exceptId: string | null) {
  const rows = await tx.ticketType.findMany({ where: { eventId, ...(exceptId ? { id: { not: exceptId } } : {}) }, select: { name: true, total: true } });
  return { totals: rows.reduce((sum, r) => sum + r.total, 0), names: new Set(rows.map((r) => r.name.toLocaleLowerCase('en'))) };
}

function answer(err: unknown, res: import('express').Response) {
  if (err instanceof Refusal) return res.status(err.status).json(err.body);
  throw err;
}

ticketTypes.post('/', requireAuth, requireRole('ORGANIZER', 'ADMIN'), ah(async (req, res) => {
  const parsed = createBody.safeParse(req.body);
  if (!parsed.success) return res.status(400).json({ error: parsed.error.issues[0]?.message ?? 'invalid body' });
  const auth = getAuth(req);
  const { eventId, name: typeName, priceCents: price, total: count } = parsed.data;
  try {
    const created = await withTenantRetry(auth.tenantId, async (tx) => {
      const event = await lockManagedEvent(tx, eventId, auth);
      const others = await siblings(tx, eventId, null);
      if (others.names.has(typeName.toLocaleLowerCase('en'))) throw new Refusal(409, { error: `this event already has a ticket type called "${typeName}"` });
      if (others.totals + count > event.capacity) {
        throw new Refusal(409, { error: `the event's capacity is ${event.capacity}, and ${others.totals} are already in other ticket types`, capacity: event.capacity, allocated: others.totals });
      }
      return tx.ticketType.create({
        data: { tenantId: auth.tenantId, eventId, name: typeName, priceCents: price, total: count, remaining: count },
      });
    });
    return res.status(201).json(view(created));
  } catch (err) {
    return answer(err, res);
  }
}));

ticketTypes.get('/', requireAuth, ah(async (req, res) => {
  const eventId = typeof req.query['eventId'] === 'string' ? req.query['eventId'] : '';
  if (!ID.test(eventId)) return res.status(400).json({ error: 'eventId is required, and is an event id' });
  const auth = getAuth(req);
  const found = await withTenantRetry(auth.tenantId, async (tx) => {
    const event = await tx.eventProjection.findFirst({ where: { id: eventId }, select: { status: true, deleted: true, organiserId: true } });
    const manages = auth.role === 'ADMIN' || (auth.role === 'ORGANIZER' && event?.organiserId === auth.userId);
    if (!event || event.deleted || (!manages && event.status !== 'PUBLISHED')) return null;
    return tx.ticketType.findMany({ where: { eventId }, orderBy: [{ priceCents: 'asc' }, { createdAt: 'asc' }, { id: 'asc' }] });
  });
  if (!found) return res.status(404).json({ error: 'no such event' });
  return res.json({ data: found.map(view) });
}));

ticketTypes.patch('/:id', requireAuth, requireRole('ORGANIZER', 'ADMIN'), ah(async (req, res) => {
  const ifMatch = req.header('If-Match');
  if (!ifMatch) return res.status(428).json({ error: 'If-Match with the revision you are editing is required' });
  const expected = Number(ifMatch.replace(/^W\//, '').replace(/"/g, ''));
  if (!Number.isInteger(expected) || expected < 1) return res.status(400).json({ error: 'If-Match must be a revision number' });
  const parsed = patchBody.safeParse(req.body);
  if (!parsed.success) return res.status(400).json({ error: parsed.error.issues[0]?.message ?? 'invalid body' });
  const auth = getAuth(req);
  const id = req.params['id']!;
  if (!ID.test(id)) return res.status(404).json({ error: 'no such ticket type' });
  try {
    const updated = await withTenantRetry(auth.tenantId, async (tx) => {
      const current = await tx.ticketType.findFirst({ where: { id }, select: { eventId: true } });
      if (!current) throw new Refusal(404, { error: 'no such ticket type' });
      const event = await lockManagedEvent(tx, current.eventId, auth);
      // Read again under the event's lock: what the revision check and the floor below
      // compare against is the row as it is now.
      const row = await tx.ticketType.findFirstOrThrow({ where: { id } });
      if (row.revision !== expected) throw new Refusal(412, { error: 'this ticket type changed while you were editing it; reload', revision: row.revision });
      const others = await siblings(tx, row.eventId, id);
      const next = { name: parsed.data.name ?? row.name, priceCents: parsed.data.priceCents ?? row.priceCents, total: parsed.data.total ?? row.total };
      if (parsed.data.name !== undefined && others.names.has(next.name.toLocaleLowerCase('en'))) {
        throw new Refusal(409, { error: `this event already has a ticket type called "${next.name}"` });
      }
      // Only an edit that ADDS tickets is held to the capacity. An event can already be
      // over it -- Event lets a draft's capacity be lowered after its tickets were set
      // up, and reservations then refuse to sell -- and the organiser must still be able
      // to reduce a total, or change a price or a name, to put it right.
      if (next.total > row.total && others.totals + next.total > event.capacity) {
        throw new Refusal(409, { error: `the event's capacity is ${event.capacity}, and ${others.totals} are already in other ticket types`, capacity: event.capacity, allocated: others.totals });
      }
      // One statement, conditional on what it relies on: the revision, and a total that
      // still covers what is held and sold. A sale between the read above and this
      // statement moves `held`, so `remaining` is derived here, not from the read --
      // and `version` moves too, because the counters changed.
      const rows = await tx.$queryRaw<Array<Parameters<typeof view>[0]>>`
        UPDATE "TicketType"
           SET name = ${next.name},
               "priceCents" = ${next.priceCents},
               total = ${next.total},
               remaining = ${next.total} - held - sold,
               revision = revision + 1,
               version = version + 1,
               "updatedAt" = now()
         WHERE id = ${id}
           AND revision = ${expected}
           AND ${next.total} >= held + sold
        RETURNING id, "eventId", name, "priceCents", total, remaining, held, sold, revision`;
      if (rows.length === 0) {
        const now = await tx.ticketType.findFirstOrThrow({ where: { id }, select: { revision: true, held: true, sold: true } });
        if (now.revision !== expected) throw new Refusal(412, { error: 'this ticket type changed while you were editing it; reload', revision: now.revision });
        throw new Refusal(409, { error: `${now.held + now.sold} of these are already held or sold, so the total cannot be ${next.total}`, held: now.held, sold: now.sold });
      }
      return rows[0]!;
    });
    return res.json(view(updated));
  } catch (err) {
    return answer(err, res);
  }
}));
