import express from 'express';
import { loadBaseEnv } from '@eventhub/config';
import { createLogger } from '@eventhub/logger';
import { errorHandler } from '@eventhub/http';
import { requireAuth } from './auth.js';
import { assertRuntimeRoleCannotBypassRls } from './db.js';
import { reservations } from './routes/reservations.js';
import { pay } from './routes/pay.js';
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

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'ticketing' });
});

app.get('/whoami', requireAuth, (req, res) => {
  res.json({ service: 'ticketing', ...req.auth });
});

app.use('/reservations', reservations);
// Mounted on the same base path: /reservations/:id/pay.
app.use('/reservations', pay);
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
