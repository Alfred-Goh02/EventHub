import express from 'express';
import { loadBaseEnv } from '@eventhub/config';
import { createLogger } from '@eventhub/logger';
import { drainOnSignal, errorHandler, readiness, refuseUnstorableText } from '@eventhub/http';
import { requireAuth } from './auth.js';
import { assertRuntimeRoleCannotBypassRls, prisma } from './db.js';
import { events } from './routes/events.js';

const env = loadBaseEnv();
const logger = createLogger('event-service');

const app = express();
app.use(express.json());
// Text Postgres cannot store as sent answers 400 here, before any route. See @eventhub/http.
app.use(refuseUnstorableText());

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'event' });
});

/** Readiness: the database answers as this process, the app role. See @eventhub/http. */
app.get('/ready', readiness({ database: () => prisma.$queryRaw`SELECT 1` }));

// Proves this service verifies a token AUTH signed, holding only the public
// key — no call to Auth, no shared session store.
app.get('/whoami', requireAuth, (req, res) => {
  res.json({ service: 'event', ...req.auth });
});

app.use('/events', events);

// Last, after the routes. A body the client got wrong answers 4xx; everything else
// is logged and answers 500. See @eventhub/http.
app.use(errorHandler(logger));

async function main() {
  // Refuse to start rather than run with tenant isolation silently disabled.
  await assertRuntimeRoleCannotBypassRls();

  const server = app.listen(env.PORT, () => {
    logger.info(`event-service listening on port ${env.PORT}`);
  });
  drainOnSignal(server, { logger, cleanup: () => prisma.$disconnect() });
}

main().catch((err) => {
  logger.error({ err }, 'failed to start');
  process.exit(1);
});
