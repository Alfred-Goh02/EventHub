import { createHash } from 'node:crypto';
import { Router } from 'express';
import { z } from 'zod';
import { withTenantRetry } from '../db.js';
import { ah } from '@eventhub/http';
import { requireAuth } from '../auth.js';
import { emit } from '../outbox.js';
import { projectionIsCurrent, secondsSinceHeartbeat } from '../consumer.js';
import { createLogger } from '@eventhub/logger';

export const reservations: Router = Router();

/** How long a hold survives without payment. */
const logger = createLogger('reservations');

const HOLD_TTL_MS = 10 * 60 * 1000;


/** What to put in Retry-After. One consumer poll plus room to actually apply it. */
const PROJECTION_RETRY_AFTER_S = 2;


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
    //
    // DO NOTHING, and there used to be a DO UPDATE here that TOOK THE KEY OVER
    // once a 60-second lease expired -- meaning it ran the work again. For this
    // endpoint "run the work again" is a SECOND HOLD, which is the exact thing
    // an Idempotency-Key exists to prevent. Reproduced by planting such a row:
    // one key, two reservations, held 1 -> 2.
    //
    // The lease was there for a crash between claiming and completing. That
    // cannot happen: the claim and the work commit in ONE transaction, so a
    // crash rolls both back and no claimed-but-unfinished row is left behind.
    // Measured after that became true -- 12 concurrent requests on one key gave
    // 12 x 201, a single reservation id, and zero in-flight 409s. So the only
    // rows the takeover could ever fire on were ones that should not exist.
    //
    // pay.ts already made this argument for the payment route: "an expiring
    // lease would authorise exactly the double charge it exists to prevent.
    // Where a lease can expire, a status cannot." Same reasoning, other route.
    //
    // The failure direction flips, deliberately. A leftover in_progress row now
    // makes that ONE key answer 409 until somebody deletes it -- recoverable by
    // an operator. A duplicate hold on scarce inventory is not.
    // -----------------------------------------------------------------------
    const claimed = await tx.$queryRaw<Array<{ id: string; state: string; requestHash: string; responseStatus: number | null; responseBody: unknown }>>`
      INSERT INTO "IdempotencyKey"
        ("id", "tenantId", "userId", "key", "endpoint", "requestHash", "state", "updatedAt")
      VALUES (gen_random_uuid()::text, ${tenantId}, ${userId}, ${idempotencyKey}, 'POST /reservations',
              ${requestHash}, 'in_progress', now())
      ON CONFLICT ("tenantId", "userId", "endpoint", "key") DO NOTHING
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

    /**
     * THROWN, not returned, for exactly the reason the unknown-ticket-type
     * guard further down this transaction already gives: returning COMMITS the
     * idempotency-key claim made in step 1, and the retry of a request that
     * did nothing is then told its key is already in flight.
     *
     * These two were the only guards in the endpoint still returning, and
     * unknown-event is the one where it does real damage. That branch can
     * answer 503 with Retry-After: 2 when the projection consumer is not
     * current -- and the committed claim holds a 60-second lease, so the
     * service spends the next 58 seconds refusing the retry it just asked
     * for. Measured against the unfixed code: 404, then 409, then 409.
     *
     * not-on-sale is the milder half of the same mistake. The status code
     * happens to stay 409 either way, so only the message changes -- but a
     * client reading it is told a key is in flight when nothing is.
     */
    if (!projection) throw Object.assign(new Error('projection'), { kind: 'unknown-event' });
    if (projection.deleted || projection.status !== 'PUBLISHED') {
      throw Object.assign(new Error('projection'), {
        kind: 'not-on-sale',
        status: projection.status,
      });
    }

    // -----------------------------------------------------------------------
    // 2b. Capture the price BEFORE anything is written.
    //
    // What the buyer pays must be what they were quoted when the hold was taken,
    // not whatever priceCents happens to say when they get round to paying. An
    // organiser editing a price mid-checkout would otherwise silently change the
    // amount charged for a cart someone had already agreed to.
    //
    // It also makes the charge amount reconstructible from the reservation
    // alone, which is what reconciliation needs: the amount must not depend on a
    // mutable row somewhere else that may have moved since.
    //
    // This read is deliberately here rather than later. It takes no row lock, so
    // it costs a round trip and nothing in throughput, and it stays on the cheap
    // side of the contended UPDATE below.
    // -----------------------------------------------------------------------
    const priced = await tx.ticketType.findMany({
      where: { id: { in: ordered.map(([ticketTypeId]) => ticketTypeId) }, eventId },
      select: { id: true, priceCents: true },
    });
    const priceOf = new Map(priced.map((t) => [t.id, t.priceCents]));

    for (const [ticketTypeId] of ordered) {
      if (!priceOf.has(ticketTypeId)) {
        // Thrown, not returned, so the transaction rolls back and takes the
        // idempotency-key claim with it. Returning here would COMMIT the claim,
        // and the retry of a request that never did anything would then be told
        // its key is already in flight instead of getting the same 400 again.
        throw Object.assign(new Error('inventory'), { kind: 'unknown-ticket-type', ticketTypeId });
      }
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

    try {
      await tx.reservationItem.createMany({
        data: ordered.map(([ticketTypeId, quantity]) => ({
          tenantId,
          reservationId: reservation.id,
          ticketTypeId,
          quantity,
          unitPriceCents: priceOf.get(ticketTypeId)!,
        })),
      });
    } catch (err) {
      // The composite FK to (tenantId, ticketTypeId) fires HERE, before the
      // inventory guard below ever runs — so an unknown ticket type surfaces as
      // a P2003 rather than reaching the guard's clean "unknown-ticket-type"
      // path. Without this translation it becomes a 500 for what is plainly a
      // bad request.
      //
      // Note the FK is doing real work beyond typo-catching: because it is
      // composite, a ticket type belonging to ANOTHER tenant is not merely
      // forbidden, it does not exist as far as this statement is concerned.
      if ((err as { code?: string }).code === 'P2003') {
        throw Object.assign(new Error('inventory'), {
          kind: 'unknown-ticket-type',
          ticketTypeId: ordered[0]![0],
        });
      }
      throw err;
    }

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
    const e = err as { kind?: string; ticketTypeId?: string; remaining?: number; status?: string };
    if (e.kind === 'sold-out') {
      outcome = { kind: 'sold-out', ticketTypeId: e.ticketTypeId!, remaining: e.remaining };
    } else if (e.kind === 'unknown-ticket-type') {
      outcome = { kind: 'unknown-ticket-type', ticketTypeId: e.ticketTypeId! };
    } else if (e.kind === 'unknown-event') {
      outcome = { kind: 'unknown-event' };
    } else if (e.kind === 'not-on-sale') {
      outcome = { kind: 'not-on-sale', status: e.status! };
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
    case 'unknown-event': {
      /**
       * A missing projection row means one of two very different things, and
       * this used to answer 404 for both.
       *
       * Either the event genuinely does not exist, or THIS SERVICE HAS NOT
       * CAUGHT UP YET. Ticketing learns about events by consuming a topic, so
       * there is always a window after publication in which the event is real
       * and the projection does not know it. Answering 404 in that window tells
       * a customer an event does not exist while it is on sale — and a client
       * that trusts 404 will not retry.
       *
       * ConsumerCheckpoint exists precisely to tell those apart, and the schema
       * comment on it has described this behaviour since it was written. But
       * projectionLagSeconds() was exported and called by NOTHING: the mechanism
       * was built, the documentation described it, and the decision it was built
       * for was never actually made.
       *
       * A null lag means there is no checkpoint row at all — the consumer has
       * never committed anything — so this service cannot claim any event is
       * missing.
       */
      if (!projectionIsCurrent()) {
        logger.warn(
          { eventId, secondsSinceHeartbeat: secondsSinceHeartbeat() },
          'answering 503 rather than 404: the projection consumer is not current, so a missing row proves nothing',
        );
        res.setHeader('Retry-After', String(PROJECTION_RETRY_AFTER_S));
        return res.status(503).json({
          error: 'this service has not caught up with recent events yet; please retry',
        });
      }

      return res.status(404).json({ error: 'event not found' });
    }
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
