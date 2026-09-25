import { createHmac, timingSafeEqual } from 'node:crypto';
import { Router, type Request } from 'express';
import pg from 'pg';
import { z } from 'zod';
import { createLogger } from '@eventhub/logger';
import { ah } from '@eventhub/http';
import { settleAttempt } from '../payments/settle.js';
import { attachPoolErrorHandler, withClient } from '../pool.js';

export const webhooks: Router = Router();

const logger = createLogger('payment-webhook');

/**
 * How far out of date a signature may be. Five minutes, the usual figure.
 *
 * Without a timestamp in the signed payload, a signature is valid forever: an
 * intercepted webhook could be replayed indefinitely. The settlement it triggers
 * is idempotent, so a replay of a HANDLED event is harmless — but "harmless
 * because of a guard three layers down" is not a reason to accept unbounded
 * replay at the door.
 */
const TOLERANCE_SECONDS = 300;

const secret = process.env['PAYMENT_WEBHOOK_SECRET'];
if (!secret) {
  // Fail at startup, not on the first request. A webhook route with no secret is
  // an unauthenticated write path into payment state — the kind of thing that
  // works perfectly in testing and is discovered by somebody else.
  throw new Error('PAYMENT_WEBHOOK_SECRET is required — the webhook route is unauthenticated without it');
}
const WEBHOOK_SECRET = secret;

const lookupUrl = process.env['SWEEPER_DATABASE_URL'];
if (!lookupUrl) {
  // Also at startup. Without it pg falls back to PG* environment defaults and
  // fails on the FIRST WEBHOOK instead — which is to say, in production, on a
  // real payment, at the moment the route matters most.
  throw new Error(
    'SWEEPER_DATABASE_URL is required — the webhook uses it to resolve which tenant a ' +
      'payment belongs to, because a webhook arrives with no tenant of its own',
  );
}

/**
 * A SECOND connection pool, on the cross-tenant credential, used for EXACTLY ONE
 * STATEMENT.
 *
 * THE PROBLEM: a webhook arrives from the payment provider. It carries no
 * session, no token, and no tenant. Every policy in this database reads
 * `app.tenant_id` and nothing else, and every table is FORCE ROW LEVEL SECURITY,
 * so the ordinary application role would see ZERO ROWS for a payment that
 * plainly exists. There is nothing to scope the query by, because the caller is
 * not a user of any tenant.
 *
 * THE TEMPTING ANSWER, which is wrong: take the tenant from the webhook body.
 * Providers do echo metadata you sent them, and the payload is signature-
 * verified, so the value is authentic in the sense that the provider really did
 * send it. But authenticity is not authority. It would make the request name the
 * tenant whose rows it gets to modify, and the entire tenancy model of this
 * system is that a request never does that. One signing-key leak would then be a
 * cross-tenant write primitive rather than a nuisance.
 *
 * THE ANSWER USED HERE: the webhook says WHICH PAYMENT. This database says WHOSE.
 *
 * That resolution needs to cross tenants, so it runs on the reconciler's
 * credential — and it is confined to a single SELECT of a single column. Every
 * write that follows goes through the ordinary application role with
 * `app.tenant_id` set to what the database said, under exactly the policies that
 * govern a normal request. The cross-tenant reach is one column wide.
 *
 * The pool is deliberately NOT exported. A cross-tenant credential inside the
 * API process is only safe while nothing else can reach it.
 */
const lookupPool = attachPoolErrorHandler(
  new pg.Pool({
    connectionString: lookupUrl,
    // Named so pg_stat_activity says which pool a connection belongs to. An
    // operator looking at a stuck backend should not have to guess, and the
    // resilience verifier uses it to find exactly the connection it means to
    // terminate rather than any connection on the same role.
    application_name: 'webhook-tenant-lookup',
    max: 2,
    connectionTimeoutMillis: 5_000,
    idleTimeoutMillis: 30_000,
  }),
  logger,
  'webhook-tenant-lookup',
);

/**
 * The pool the settlement actually runs on: the ordinary restricted role, the
 * one RLS applies to. Raw pg rather than Prisma because settleAttempt is shared
 * with the reconciler, which cannot use Prisma at all.
 */
const appPool = attachPoolErrorHandler(
  new pg.Pool({
    connectionString: process.env['APP_DATABASE_URL'],
    // Named so pg_stat_activity says which pool a connection belongs to. An
    // operator looking at a stuck backend should not have to guess, and the
    // resilience verifier uses it to find exactly the connection it means to
    // terminate rather than any connection on the same role.
    application_name: 'webhook-settlement',
    max: 5,
    connectionTimeoutMillis: 5_000,
    idleTimeoutMillis: 30_000,
  }),
  logger,
  'webhook-settlement',
);

const event = z.object({
  paymentId: z.string().min(1),
  type: z.enum(['charge.succeeded', 'charge.failed']),
  providerRef: z.string().min(1).optional(),
  reason: z.string().min(1).optional(),
});

/**
 * Verifies `t=<unix seconds>,v1=<hex hmac>` over `${t}.${rawBody}`.
 *
 * The RAW body, not the parsed one. Re-serialising the parsed JSON would produce
 * different bytes — key order, whitespace, number formatting — and the signature
 * would never match. index.ts stashes the buffer from express.json's verify
 * hook for this reason.
 */
function verifySignature(req: Request): { ok: true } | { ok: false; why: string } {
  const header = req.header('X-Payment-Signature');
  if (!header) return { ok: false, why: 'missing signature header' };

  const parts = new Map(
    header.split(',').map((kv) => {
      const i = kv.indexOf('=');
      return [kv.slice(0, i).trim(), kv.slice(i + 1).trim()] as const;
    }),
  );

  const t = parts.get('t');
  const v1 = parts.get('v1');
  if (!t || !v1) return { ok: false, why: 'malformed signature header' };

  const age = Math.abs(Math.floor(Date.now() / 1000) - Number(t));
  if (!Number.isFinite(age) || age > TOLERANCE_SECONDS) {
    return { ok: false, why: 'signature timestamp outside tolerance' };
  }

  const raw = (req as Request & { rawBody?: Buffer }).rawBody;
  if (!raw) return { ok: false, why: 'raw body unavailable' };

  const expected = createHmac('sha256', WEBHOOK_SECRET).update(`${t}.${raw.toString('utf8')}`).digest();
  let given: Buffer;
  try {
    given = Buffer.from(v1, 'hex');
  } catch {
    return { ok: false, why: 'signature is not hex' };
  }

  // Length must match before timingSafeEqual, which throws on a mismatch — and
  // the comparison is constant-time so that a wrong signature cannot be refined
  // byte by byte from response timing.
  if (given.length !== expected.length) return { ok: false, why: 'signature length mismatch' };
  if (!timingSafeEqual(given, expected)) return { ok: false, why: 'signature mismatch' };

  return { ok: true };
}

/**
 * POST /webhooks/payments
 *
 * The provider telling us what it already decided. Same fact the reconciler
 * would eventually discover by asking, arriving in seconds instead of on the
 * next pass — which is the whole reason to accept one.
 *
 * NO AUTHENTICATION MIDDLEWARE, because the caller is not a user. The signature
 * is the authentication, and it is checked before anything is parsed or read.
 *
 * NO DEDUPLICATION TABLE. Providers redeliver, sometimes many times, and every
 * write in settleAttempt is guarded on state that a first delivery has already
 * changed. A redelivery finds `settledAt` non-null and does nothing.
 *
 * STATUS CODES ARE INSTRUCTIONS TO THE PROVIDER, not descriptions of our
 * feelings about the request. A provider retries on non-2xx, so:
 *   400  the signature is wrong or the body is malformed — it will never become
 *        valid, so do not retry
 *   200  handled, ALSO for a payment we do not recognise — retrying will not
 *        make it recognisable, and an endless retry loop over an unknown id is
 *        worse than dropping it
 *   500  something transient broke here — please do retry
 */
webhooks.post('/payments', ah(async (req, res) => {
  const signature = verifySignature(req);
  if (!signature.ok) {
    logger.warn({ why: signature.why }, 'rejected webhook');
    return res.status(400).json({ error: signature.why });
  }

  const parsed = event.safeParse(req.body);
  if (!parsed.success) {
    return res.status(400).json({ error: parsed.error.flatten() });
  }
  const { paymentId, type, providerRef, reason } = parsed.data;

  // The one cross-tenant statement. Nothing else in this handler crosses a
  // tenant boundary, and nothing else in this service uses this pool.
  const { rows } = await lookupPool.query<{
    id: string;
    tenantId: string;
    reservationId: string;
    amountCents: number;
    currency: string;
    settledAt: Date | null;
  }>(
    `SELECT id, "tenantId", "reservationId", "amountCents", currency, "settledAt"
       FROM "Payment" WHERE id = $1`,
    [paymentId],
  );

  const payment = rows[0];
  if (!payment) {
    // Deliberately 200. A payment id we have never issued is not going to start
    // existing because the provider tries again.
    logger.warn({ paymentId }, 'webhook for an unknown payment');
    return res.status(200).json({ received: true, ignored: 'unknown payment' });
  }

  if (payment.settledAt) {
    // Already resolved, by the request handler or by the reconciler. Answering
    // before opening a transaction saves the work; settleAttempt would reach the
    // same conclusion if we did not.
    return res.status(200).json({ received: true, ignored: 'already settled' });
  }

  const outcome =
    type === 'charge.succeeded'
      ? ({ status: 'succeeded', providerRef: providerRef ?? `webhook_${paymentId}` } as const)
      : ({ status: 'failed', reason: reason ?? 'charge_failed' } as const);

  return withClient(appPool, logger, 'webhook-settle', async (client) => {
    try {
      await client.query('BEGIN');
      // The tenant comes from the DATABASE, never from the payload. From here on
      // this is an ordinary tenant-scoped transaction under the same policies as
      // any request.
      await client.query(`SELECT set_config('app.tenant_id', $1, true)`, [payment.tenantId]);

      const settled = await settleAttempt(client, payment, outcome, logger);
      await client.query('COMMIT');

      logger.info({ paymentId, type, settled }, 'webhook applied');
      return res.status(200).json({ received: true, result: settled });
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {});
      // 500 so the provider retries. Nothing was written, and the reconciler would
      // pick this up anyway — the webhook is an accelerator, never the only route.
      logger.error({ err, paymentId }, 'failed to apply webhook');
      return res.status(500).json({ error: 'could not apply webhook' });
    }
  });
}));
