import { createServer } from 'node:http';
import { setTimeout as sleep } from 'node:timers/promises';
import express from 'express';
import { createLogger } from '@eventhub/logger';
import { env } from './env.js';
import { assertRuntimeRoleCannotBypassRls, prisma } from './db.js';

const logger = createLogger('live-service');
const app = express();

app.use(express.json());

/**
 * ONE http.Server for Express and (from the next change) the WebSocket upgrade.
 *
 * Sharing it means one port, one TLS termination, and one shutdown path. A
 * separate server for sockets would need its own of each, and the drain below
 * would have to coordinate two.
 */
const server = createServer(app);

/**
 * LIVENESS AND READINESS ARE DIFFERENT ENDPOINTS BECAUSE THEY ANSWER DIFFERENT
 * QUESTIONS, and conflating them is the most common Kubernetes mistake there is.
 *
 * Readiness controls ROUTING. It flips to 503 on SIGTERM so the load balancer
 * stops sending new connections here while the existing ones drain.
 *
 * Liveness controls RESTARTS. It must keep returning 200 throughout the drain —
 * a failing liveness probe makes the kubelet restart the container immediately,
 * which skips the drain entirely and drops every connected client at once.
 */
let ready = true;

app.get('/health', (_req, res) => {
  res.json({ status: 'ok', service: 'live' });
});

app.get('/ready', (_req, res) => {
  if (ready) {
    res.json({ ready: true });
    return;
  }
  res.status(503).json({ ready: false });
});

app.use((err: unknown, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
  logger.error({ err }, 'unhandled error');
  res.status(500).json({ error: 'internal error' });
});

/**
 * How long to keep serving after readiness fails.
 *
 * The load balancer does not stop routing the instant the probe fails; it stops
 * after its own deregistration delay. Hanging up before that means clients
 * reconnect into a target group that still lists this pod and land straight back
 * on it. In production this must be shorter than terminationGracePeriodSeconds
 * and longer than deregistration_delay, or the kubelet SIGKILLs the drain.
 */
const DRAIN_DELAY_MS = Number(process.env['LIVE_DRAIN_DELAY_MS'] ?? 15_000);

let draining = false;

async function shutdown(signal: string): Promise<void> {
  if (draining) return;
  draining = true;
  logger.info({ signal }, 'draining');

  // 1. Stop being routed to. 2. Wait for that to actually take effect.
  ready = false;
  await sleep(DRAIN_DELAY_MS);

  // 3. Stop accepting new connections.
  server.close();

  await prisma.$disconnect().catch(() => {});
  logger.info('drained');
  process.exit(0);
}

process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('SIGINT', () => void shutdown('SIGINT'));

async function main(): Promise<void> {
  // Refuse to start rather than run with tenant isolation silently disabled.
  await assertRuntimeRoleCannotBypassRls();

  server.listen(env.PORT, () => {
    logger.info(`live-service listening on port ${env.PORT}`);
  });
}

main().catch((err) => {
  logger.error({ err }, 'live-service failed to start');
  process.exit(1);
});
