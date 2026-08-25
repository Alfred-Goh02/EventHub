import { createHash } from 'node:crypto';
import { Router } from 'express';
import { z } from 'zod';
import { withTenantRetry } from '../db.js';
import { ah } from '../async.js';
import { requireAuth } from '../auth.js';
import { emit } from '../outbox.js';

export const reservations: Router = Router();

/** How long a hold survives without payment. */
const HOLD_TTL_MS = 10 * 60 * 1000;

/** How long a claimed idempotency key is held before another attempt may take it. */
const LEASE_TTL_MS = 60 * 1000;

const createReservation = z.object({
  eventId: z.string().min(1),
  items: z
    .array(
      z.object({
        ticketTypeId: z.string().min(1),
        quantity: z.number().int().positive().max(20),
      }),
    )
    .min(1)
    .max(10),
});

const fingerprint = (method: string, path: string, body: unknown) =>
  createHash('sha256').update(`${method}\n${path}\n${JSON.stringify(body)}`).digest('hex');

/**
 * POST /reservations — take a hold on seats.
 *
 * THE CENTRAL PROBLEM: two people buy the last seat at the same time.
 *
 * The naive version reads the count, checks it, then writes:
 *
 *     SELECT remaining FROM "TicketType" WHERE id = $1;   -- both see 1
 *     if (remaining >= 1)                                  -- both pass
 *     UPDATE "TicketType" SET remaining = remaining - 1;   -- both write
 *
 * Under READ COMMITTED — Postgres's default — that has NO protection at all.
 * The SELECT takes no lock, so both transactions read 1, both conclude they may
 * proceed, and the seat is sold twice. Wrapping it in a transaction changes
 * nothing: transactions give atomicity, not mutual exclusion.
 *
 * The fix is to make the CHECK AND THE WRITE the same statement:
 *
 *     UPDATE "TicketType"
 *        SET remaining = remaining - $n, held = held + $n
 *      WHERE id = $1 AND remaining >= $n;
 *
 * A single UPDATE takes a row lock and re-evaluates its WHERE clause against
 * the newest committed version of the row. The second transaction blocks until
 * the first commits, then re-checks against remaining = 0, matches nothing, and
 * reports rowCount 0. No overselling, no application-level locking, no retry.
 *
 * Alternatives, and why not:
 *   SELECT ... FOR UPDATE  — correct, but holds the row lock across an
 *                            application round trip rather than only from the
 *                            UPDATE to COMMIT.
 *   SERIALIZABLE           — correct, but turns contention into serialisation
 *                            failures you must retry; needless when one
 *                            statement already suffices.
 *   A Redis lock           — a distributed lock to protect data that already
 *                            lives in a system with locks, plus clock-drift and
 *                            fencing problems Redlock is criticised for.
 */
reservations.post('/', requireAuth, ah(async (req, res) => {
  const parsed = createReservation.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.flatten() });
  }

  const idempotencyKey = req.header('Idempotency-Key');
  if (!idempotencyKey) {
    return res.status(400).json({ error: 'Idempotency-Key header is required' });
  }

  const { tenantId, userId } = req.auth!;
  const { eventId, items } = parsed.data;
  const requestHash = fingerprint('POST', '/reservations', req.body);

  // Collapse duplicate ticket types so one cart cannot lock the same row twice.
  const merged = new Map<string, number>();
  for (const i of items) merged.set(i.ticketTypeId, (merged.get(i.ticketTypeId) ?? 0) + i.quantity);

  // Deterministic lock order. Two carts holding {A,B} and {B,A} would otherwise
  // each hold one row and wait for the other — a textbook deadlock. Sorting the
  // ids means every transaction takes the same rows in the same sequence, so one
  // simply waits.
  const ordered = [...merged.entries()].sort(([a], [b]) => a.localeCompare(b));

  type Outcome =
    | { kind: 'created'; body: unknown }
    | { kind: 'replay'; status: number; body: unknown }
    | { kind: 'in-flight' }
    | { kind: 'mismatch' }
    | { kind: 'retry' }
    | { kind: 'unknown-event' }
    | { kind: 'not-on-sale'; status: string }
    | { kind: 'sold-out'; ticketTypeId: string; remaining?: number }
    | { kind: 'unknown-ticket-type'; ticketTypeId: string };

  const run = async (): Promise<Outcome> => withTenantRetry(tenantId, async (tx) => {
    // -----------------------------------------------------------------------
    // 1. Claim the idempotency key FIRST, before touching anything contended.
    //
    // A single INSERT ... ON CONFLICT does claim-or-detect in one round trip.
    // Reading first and inserting second would be the same read-then-write race
    // this endpoint exists to avoid.
    // -----------------------------------------------------------------------
    const claimed = await tx.$queryRaw<Array<{ id: string; state: string; requestHash: string; responseStatus: number | null; responseBody: unknown }>>`
      INSERT INTO "IdempotencyKey"
        ("id", "tenantId", "userId", "key", "endpoint", "requestHash", "state", "leaseExpiresAt", "updatedAt")
      VALUES (gen_random_uuid()::text, ${tenantId}, ${userId}, ${idempotencyKey}, 'POST /reservations',
              ${requestHash}, 'in_progress', now() + ${`${LEASE_TTL_MS} milliseconds`}::interval, now())
      ON CONFLICT ("tenantId", "userId", "endpoint", "key") DO UPDATE
        SET "leaseExpiresAt" = now() + ${`${LEASE_TTL_MS} milliseconds`}::interval,
            "updatedAt" = now()
        WHERE "IdempotencyKey"."state" = 'in_progress'
          AND "IdempotencyKey"."leaseExpiresAt" < now()
      RETURNING "id", "state", "requestHash", "responseStatus", "responseBody"
    `;

    if (claimed.length === 0) {
      // The conflict target matched but the DO UPDATE's WHERE did not, so the
      // key is either completed or still in flight under a live lease.
      const existing = await tx.idempotencyKey.findUnique({
        where: {
          tenantId_userId_endpoint_key: {
            tenantId, userId, endpoint: 'POST /reservations', key: idempotencyKey,
          },
        },
        select: { state: true, requestHash: true, responseStatus: true, responseBody: true },
      });

      if (!existing) return { kind: 'retry' as const };

      // Same key, different body. Someone is replaying an observed key against
      // a different request — return neither the cached response nor a new one.
      if (existing.requestHash !== requestHash) return { kind: 'mismatch' as const };

      if (existing.state === 'completed') {
        return {
          kind: 'replay' as const,
          status: existing.responseStatus ?? 200,
          body: existing.responseBody,
        };
      }

      return { kind: 'in-flight' as const };
    }

    const keyRow = claimed[0]!;
    if (keyRow.requestHash !== requestHash) return { kind: 'mismatch' as const };

    // -----------------------------------------------------------------------
    // 2. The event must exist in the local projection, be published, and not be
    //    cancelled or deleted. This service cannot ask the Event service — it
    //    has no credential for that database — so the projection IS the answer.
    // -----------------------------------------------------------------------
    const projection = await tx.eventProjection.findFirst({
      where: { id: eventId },
      select: { status: true, deleted: true },
    });

    if (!projection) return { kind: 'unknown-event' as const };
    if (projection.deleted || projection.status !== 'PUBLISHED') {
      return { kind: 'not-on-sale' as const, status: projection.status };
    }

    // -----------------------------------------------------------------------
    // 3. Uncontended inserts BEFORE the contended update.
    //
    // Statement order is the whole performance story. The conditional UPDATE
    // below holds a row lock from the moment it runs until COMMIT, and every
    // other buyer for that ticket type queues behind it. Anything done after it
    // directly reduces the throughput ceiling, so all the cheap work happens
    // first.
    // -----------------------------------------------------------------------
    const reservation = await tx.reservation.create({
      data: {
        tenantId,
        eventId,
        userId,
        status: 'HELD',
        expiresAt: new Date(Date.now() + HOLD_TTL_MS),
      },
      select: { id: true, expiresAt: true, version: true },
    });

    await tx.reservationItem.createMany({
      data: ordered.map(([ticketTypeId, quantity]) => ({
        tenantId,
        reservationId: reservation.id,
        ticketTypeId,
        quantity,
      })),
    });

    // -----------------------------------------------------------------------
    // 4. THE CONTENDED STATEMENT, as late as possible.
    //
    // Check and write in ONE statement. rowCount 0 means the guard failed —
    // either not enough left, or no such ticket type in this tenant.
    // -----------------------------------------------------------------------
    for (const [ticketTypeId, quantity] of ordered) {
      const updated = await tx.ticketType.updateMany({
        where: { id: ticketTypeId, eventId, remaining: { gte: quantity } },
        data: {
          remaining: { decrement: quantity },
          held: { increment: quantity },
          version: { increment: 1 },
        },
      });

      if (updated.count === 0) {
        // Disambiguate INSIDE the transaction, on tx — a tenant-scoped read.
        // Using the bare client here would escape tenant context and build a
        // cross-tenant existence oracle.
        const tt = await tx.ticketType.findFirst({
          where: { id: ticketTypeId, eventId },
          select: { remaining: true, name: true },
        });

        // Throwing rolls the whole transaction back, so a partially-filled cart
        // cannot leave some inventory decremented. All or nothing.
        throw Object.assign(new Error('inventory'), {
          kind: tt ? 'sold-out' : 'unknown-ticket-type',
          ticketTypeId,
          remaining: tt?.remaining,
          name: tt?.name,
        });
      }
    }

    // -----------------------------------------------------------------------
    // 5. The outbox row, in the SAME transaction. It must come after step 4
    //    because it describes the reservation that step made real.
    // -----------------------------------------------------------------------
    await emit(tx, {
      tenantId,
      topic: 'tickets.reserved',
      eventType: 'reservation.held',
      aggregateId: eventId,
      payload: {
        reservationId: reservation.id,
        tenantId,
        eventId,
        userId,
        state: 'RESERVED',
        items: ordered.map(([ticketTypeId, quantity]) => ({ ticketTypeId, quantity })),
        expiresAt: reservation.expiresAt.toISOString(),
      },
    });

    const body = {
      id: reservation.id,
      eventId,
      status: 'HELD',
      expiresAt: reservation.expiresAt.toISOString(),
      items: ordered.map(([ticketTypeId, quantity]) => ({ ticketTypeId, quantity })),
    };

    // Complete the idempotency key with the response, so a retry replays it.
    await tx.idempotencyKey.update({
      where: { id: keyRow.id },
      data: { state: 'completed', responseStatus: 201, responseBody: body },
    });

    return { kind: 'created' as const, body };
  });

  // The inventory guard signals by THROWING, so the whole transaction rolls
  // back — a partially-filled cart must not leave some ticket types decremented
  // and others not. Translating that back into a typed outcome happens here,
  // outside the transaction.
  let outcome: Outcome;
  try {
    outcome = await run();
  } catch (err) {
    const e = err as { kind?: string; ticketTypeId?: string; remaining?: number };
    if (e.kind === 'sold-out') {
      outcome = { kind: 'sold-out', ticketTypeId: e.ticketTypeId!, remaining: e.remaining };
    } else if (e.kind === 'unknown-ticket-type') {
      outcome = { kind: 'unknown-ticket-type', ticketTypeId: e.ticketTypeId! };
    } else {
      throw err;
    }
  }

  switch (outcome.kind) {
    case 'created':
      return res.status(201).json(outcome.body);
    case 'replay':
      // Byte-identical to the original response. That is the contract: a retry
      // is indistinguishable from the first call.
      return res.status(outcome.status).json(outcome.body);
    case 'in-flight':
      return res.status(409).json({ error: 'a request with this Idempotency-Key is in progress' });
    case 'mismatch':
      return res.status(422).json({ error: 'this Idempotency-Key was used with a different request body' });
    case 'retry':
      return res.status(503).json({ error: 'please retry' });
    case 'unknown-event':
      return res.status(404).json({ error: 'event not found' });
    case 'not-on-sale':
      return res.status(409).json({ error: `event is not on sale (status ${outcome.status})` });
    case 'sold-out':
      return res.status(409).json({
        error: 'not enough tickets remaining',
        ticketTypeId: outcome.ticketTypeId,
        remaining: outcome.remaining,
      });
    case 'unknown-ticket-type':
      return res.status(400).json({
        error: 'unknown ticket type for this event',
        ticketTypeId: outcome.ticketTypeId,
      });
    default:
      return res.status(500).json({ error: 'internal error' });
  }
}));
