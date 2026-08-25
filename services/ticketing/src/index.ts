import express from 'express';
import { loadBaseEnv } from '@eventhub/config';
import { createLogger } from '@eventhub/logger';
import { requireAuth } from './auth.js';
import { assertRuntimeRoleCannotBypassRls } from './db.js';
import { reservations } from './routes/reservations.js';

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

// Express 4 identifies an error handler by its ARITY: four parameters. Drop
// `_next` and it silently becomes ordinary middleware that never runs on error.
app.use((err: unknown, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
  logger.error({ err }, 'unhandled error');
  res.status(500).json({ error: 'internal error' });
});

async function main() {
  await assertRuntimeRoleCannotBypassRls();

  app.listen(env.PORT, () => {
    logger.info(`ticketing-service listening on port ${env.PORT}`);
  });
}

main().catch((err) => {
  logger.error({ err }, 'failed to start');
  process.exit(1);
});
