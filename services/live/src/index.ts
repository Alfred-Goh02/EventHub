import { createServer } from 'node:http';
import { setTimeout as sleep } from 'node:timers/promises';
import express from 'express';
import { createLogger } from '@eventhub/logger';
import { env } from './env.js';
import { assertRuntimeRoleCannotBypassRls, prisma, withTenant } from './db.js';
import { closeRedis } from './redis.js';
import { connectProducer, disconnectProducer, liveKafkaProduceFailuresTotal } from './kafka.js';
import { attachUpgrade, wss } from './ws/server.js';
import { stopHeartbeat } from './ws/connection.js';
import { CLOSE } from './ws/protocol.js';
import { roomCount } from './ws/rooms.js';
import { requireAuth } from './http/auth.js';

const logger = createLogger('live-service');
const app = express();

app.use(express.json());

/**
 * ONE http.Server for Express and the WebSocket upgrade.
 *
 * Sharing it means one port, one TLS termination, and one shutdown path. A
 * separate server for sockets would need its own of each, and the drain below
 * would have to coordinate two.
 */
const server = createServer(app);
attachUpgrade(server);

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

/**
 * Connections and rooms, not CPU.
 *
 * Idle WebSockets consume almost no CPU but do consume memory and file
 * descriptors, so a pod holding twenty thousand sockets sits near zero CPU right
 * up until it OOMs or hits EMFILE. Anything autoscaling this service has to read
 * these numbers instead.
 *
 * kafkaProduceFailures is here because live.interactions is produced
 * fire-and-forget: without a counter that loss is completely silent.
 */
app.get('/metrics', (_req, res) => {
  res.json({
    connections: wss.clients.size,
    rooms: roomCount(),
    kafkaProduceFailures: liveKafkaProduceFailuresTotal(),
  });
});

/**
 * THE SNAPSHOT ENDPOINT, AND IT IS NOT A CONVENIENCE.
 *
 * Redis Pub/Sub is at-most-once with no replay: a message published while a
 * pod's subscriber was reconnecting is simply gone. That is an acceptable bus
 * ONLY because a client can re-read authoritative state cheaply, which is this.
 * Take it away and every gap in the fan-out becomes a permanently wrong client.
 *
 * Ordered the way the UI asks for it, which is the index on Question: most
 * upvoted first, oldest first as the tie-break.
 */
app.get('/live/events/:eventId/questions', requireAuth, (req, res, next) => {
  const auth = req.auth;
  if (!auth) {
    res.status(401).json({ error: 'not authenticated' });
    return;
  }
  const eventId = req.params.eventId;

  withTenant(auth.tenantId, (tx) =>
    tx.question.findMany({
      where: { eventId },
      orderBy: [{ voteCount: 'desc' }, { createdAt: 'asc' }],
      select: { id: true, body: true, voteCount: true, createdAt: true, userId: true },
      take: 200,
    }),
  )
    .then((questions) => res.json({ eventId, questions }))
    .catch(next);
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
  logger.info({ signal, connections: wss.clients.size }, 'draining');

  // 1. Stop being routed to. 2. Wait for that to actually take effect.
  ready = false;
  await sleep(DRAIN_DELAY_MS);

  // 3. Stop accepting new connections and new upgrades.
  server.close();
  stopHeartbeat();

  /**
   * 4. Hang up, with the retry hint in a MESSAGE rather than the close reason.
   *
   * The close reason is capped at 123 bytes of UTF-8 and `ws` throws a
   * RangeError past it, so a JSON payload cannot go there.
   *
   * The delay is pre-jittered per client. A rolling deploy disconnects an
   * event's audience a third at a time, and a fixed delay would bring each third
   * back in one synchronised spike against a cluster that is already down a pod.
   * Full jitter — a uniform draw from zero to the ceiling — spreads them.
   */
  for (const client of wss.clients) {
    try {
      client.send(
        JSON.stringify({ t: 'shutdown', retryAfterMs: Math.floor(Math.random() * 9000) + 1000 }),
      );
      client.close(CLOSE.GOING_AWAY, 'draining');
    } catch {
      // A socket that died mid-drain is already where we want it.
    }
  }

  await Promise.allSettled([disconnectProducer(), closeRedis(), prisma.$disconnect()]);
  logger.info('drained');
  process.exit(0);
}

process.on('SIGTERM', () => void shutdown('SIGTERM'));
process.on('SIGINT', () => void shutdown('SIGINT'));

async function main(): Promise<void> {
  // Refuse to start rather than run with tenant isolation silently disabled.
  await assertRuntimeRoleCannotBypassRls();
  await connectProducer();

  server.listen(env.PORT, () => {
    logger.info(`live-service listening on port ${env.PORT}`);
  });
}

main().catch((err) => {
  logger.error({ err }, 'live-service failed to start');
  process.exit(1);
});
