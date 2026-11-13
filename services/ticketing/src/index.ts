import express from 'express';
import { loadBaseEnv } from '@eventhub/config';
import { createLogger } from '@eventhub/logger';
import { errorHandler, readiness, refuseUnstorableText } from '@eventhub/http';
import { requireAuth } from './auth.js';
import { assertRuntimeRoleCannotBypassRls, prisma } from './db.js';
import { reservations } from './routes/reservations.js';
import { pay } from './routes/pay.js';
import { reservationReads } from './routes/reservation-reads.js';
import { webhooks } from './routes/webhooks.js';
import { ticketTypes } from './routes/ticket-types.js';
import { createProjectionConsumer, DEFAULT_GROUP } from './consumer.js';

const env = loadBaseEnv();
const logger = createLogger('ticketing-service');

/**
 * The projection consumer's group, and where a new one starts. Unset, they are what
 * compose runs: ticketing-event-projection, from the earliest offset. Set but empty, or
 * set to anything else, the process refuses to start, as Notification's schema does,
 * rather than quietly falling back to the deployed group.
 */
function projectionSettings(): { groupId: string; startFrom: 'earliest' | 'latest' } {
  const group = process.env['KAFKA_CONSUMER_GROUP'];
  const start = process.env['PROJECTION_START_FROM'];
  if (group !== undefined && group.trim() === '') {
    throw new Error('KAFKA_CONSUMER_GROUP is set but empty: unset it for the default, or name the group');
  }
  if (start !== undefined && start !== 'earliest' && start !== 'latest') {
    throw new Error(`PROJECTION_START_FROM must be earliest or latest, not "${start}"`);
  }
  return { groupId: group ?? DEFAULT_GROUP, startFrom: start ?? 'earliest' };
}

const app = express();
app.use(
  express.json({
    /**
     * Keep the RAW bytes. The payment webhook's signature is computed over the
     * body exactly as sent, and re-serialising the parsed object produces
     * different bytes -- key order, whitespace, number formatting -- so the
     * signature would never match. This hook is the only place the original
     * buffer is still available.
     */
    verify: (req, _res, buf) => {
      (req as express.Request & { rawBody?: Buffer }).rawBody = buf;
    },
  }),
);
/**
 * Text Postgres cannot store as sent answers 400 here, before any route. See
 * @eventhub/http. It reads the parsed body and leaves rawBody alone, so the webhook's
 * signature is still checked over the bytes as sent.
 *
 * THE WEBHOOK IS NOT EXEMPT, because its status codes are instructions to the provider
 * and 400 is the right one. Without this, a signed body with a NUL in paymentId reached
 * the tenant lookup, Postgres refused the parameter, and the route answered 500 -- which
 * tells the provider to retry a body that can never succeed. 400 says "do not retry",
 * which is what the route already answers for a body it cannot use.
 *
 * Two consequences, both accepted: the refusal comes before the signature check, so an
 * unsigned body with a NUL is told 400 about its text rather than about its signature
 * (the status, and what is read or written, are the same: nothing); and a signed body
 * with a NUL in a field the route ignores is refused rather than settled. The reconciler
 * settles that payment on its next pass anyway -- the webhook is an accelerator, never
 * the only route.
 */
app.use(refuseUnstorableText());

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'ticketing' });
});

/** Readiness: the database answers as this process, the app role. See @eventhub/http. */
app.get('/ready', readiness({ database: () => prisma.$queryRaw`SELECT 1` }));

app.get('/whoami', requireAuth, (req, res) => {
  res.json({ service: 'ticketing', ...req.auth });
});

app.use('/reservations', reservations);
// Mounted on the same base path: /reservations/:id/pay.
app.use('/reservations', pay);
// And the buyer's reads: GET /reservations and /reservations/:id.
app.use('/reservations', reservationReads);
// The inventory editor: what an organiser puts on sale. See routes/ticket-types.ts.
app.use('/ticket-types', ticketTypes);
// Unauthenticated by design: the caller is the payment provider, not a user.
// The HMAC signature is the authentication. See routes/webhooks.ts.
app.use('/webhooks', webhooks);

// Last, after the routes. A body the client got wrong answers 4xx; everything else
// is logged and answers 500. See @eventhub/http.
app.use(errorHandler(logger));

const consumer = createProjectionConsumer({
  brokers: (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(',').map((b) => b.trim()),
  registryUrl: process.env['SCHEMA_REGISTRY_URL'] ?? 'http://localhost:8081',
  logger,
  ...projectionSettings(),
});

async function shutdown(signal: string): Promise<void> {
  logger.info({ signal }, 'shutting down');
  await consumer.stop();
  process.exit(0);
}

process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('SIGINT', () => void shutdown('SIGINT'));

async function main() {
  await assertRuntimeRoleCannotBypassRls();

  // Started before the HTTP listener, so the service is not answering requests
  // about events it has not yet heard of. It still may be behind — that is what
  // the ConsumerCheckpoint lag is for — but it will not be behind by the whole
  // topic simply because nothing was consuming.
  await consumer.start();

  app.listen(env.PORT, () => {
    logger.info(`ticketing-service listening on port ${env.PORT}`);
  });
}

main().catch((err) => {
  logger.error({ err }, 'failed to start');
  process.exit(1);
});
