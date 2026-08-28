import express from 'express';
import { loadBaseEnv } from '@eventhub/config';
import { createLogger } from '@eventhub/logger';
import { requireAuth } from './auth.js';
import { assertRuntimeRoleCannotBypassRls } from './db.js';
import { reservations } from './routes/reservations.js';
import { pay } from './routes/pay.js';
import { createProjectionConsumer } from './consumer.js';

const env = loadBaseEnv();
const logger = createLogger('ticketing-service');

const app = express();
app.use(express.json());

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'ticketing' });
});

app.get('/whoami', requireAuth, (req, res) => {
  res.json({ service: 'ticketing', ...req.auth });
});

app.use('/reservations', reservations);
// Mounted on the same base path: /reservations/:id/pay.
app.use('/reservations', pay);

// Express 4 identifies an error handler by its ARITY: four parameters. Drop
// `_next` and it silently becomes ordinary middleware that never runs on error.
app.use((err: unknown, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
  logger.error({ err }, 'unhandled error');
  res.status(500).json({ error: 'internal error' });
});

const consumer = createProjectionConsumer({
  brokers: (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(',').map((b) => b.trim()),
  registryUrl: process.env['SCHEMA_REGISTRY_URL'] ?? 'http://localhost:8081',
  logger,
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
