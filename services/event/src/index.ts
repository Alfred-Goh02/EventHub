import express from 'express';
import { loadBaseEnv } from '@eventhub/config';
import { createLogger } from '@eventhub/logger';
import { requireAuth } from './auth.js';
import { assertRuntimeRoleCannotBypassRls } from './db.js';
import { events } from './routes/events.js';

const env = loadBaseEnv();
const logger = createLogger('event-service');

const app = express();
app.use(express.json());

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'event' });
});

// Proves this service verifies a token AUTH signed, holding only the public
// key — no call to Auth, no shared session store.
app.get('/whoami', requireAuth, (req, res) => {
  res.json({ service: 'event', ...req.auth });
});

app.use('/events', events);

// Express 4 identifies an error handler by its ARITY: four parameters. Drop
// `_next` and it silently becomes ordinary middleware that never runs on error,
// and every thrown error turns into a hung request instead of a 500.
app.use((err: unknown, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
  logger.error({ err }, 'unhandled error');
  res.status(500).json({ error: 'internal error' });
});

async function main() {
  // Refuse to start rather than run with tenant isolation silently disabled.
  await assertRuntimeRoleCannotBypassRls();

  app.listen(env.PORT, () => {
    logger.info(`event-service listening on port ${env.PORT}`);
  });
}

main().catch((err) => {
  logger.error({ err }, 'failed to start');
  process.exit(1);
});
