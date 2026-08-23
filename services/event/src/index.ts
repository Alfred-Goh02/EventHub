import express from 'express';
import { loadBaseEnv } from '@eventhub/config';
import { createLogger } from '@eventhub/logger';
import { requireAuth } from './auth.js';

const env = loadBaseEnv();
const logger = createLogger('event-service');

const app = express();
app.use(express.json());

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'event' });
});

// The first cross-service proof: this service verifies a token that AUTH signed,
// using only the public key, with no call to Auth at all. That is the whole
// argument for asymmetric JWTs over server-side sessions — no shared state and
// no network round trip on the hot path.
app.get('/whoami', requireAuth, (req, res) => {
  res.json({
    service: 'event',
    verifiedBy: 'event-service, using auth public key only',
    ...req.auth,
  });
});

app.listen(env.PORT, () => {
  logger.info(`event-service listening on port ${env.PORT}`);
});
