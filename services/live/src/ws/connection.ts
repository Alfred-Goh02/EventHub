import type { WebSocket as WsSocket } from 'ws';
import { createLogger } from '@eventhub/logger';
import type { Role } from '@eventhub/types';
import type { SocketClaims } from '../jwt.js';
import { CLOSE, clientMessage, type ServerMessage } from './protocol.js';
import { leaveAll } from './rooms.js';
import { wss } from './server.js';
import { handleClientMessage } from '../handlers/question.js';

const logger = createLogger('live-ws');

/**
 * A `ws` socket plus the things this service needs to remember about it.
 *
 * The tenant lives HERE, for the socket's whole life, taken once from the signed
 * claim at the handshake. There is no per-message header to read and no
 * Kong-injected X-Tenant-ID to trust on this route.
 */
export interface LiveSocket extends WsSocket {
  userId: string;
  tenantId: string;
  role: Role;
  rooms: Set<string>;
  isAlive: boolean;
  tokens: number;
  lastRefill: number;
}

export const send = (ws: WsSocket, msg: ServerMessage): void => {
  if (ws.readyState === ws.OPEN) ws.send(JSON.stringify(msg));
};

/**
 * A token bucket per connection, because THE GATEWAY CANNOT SEE PAST THE 101.
 *
 * Kong counted the upgrade as one request and will never see another. Every
 * message after it bypasses gateway rate limiting entirely, so a scripted client
 * could drive thousands of Postgres writes per second through one connection
 * that looks, from the outside, like a single idle request.
 *
 * Capacity 10 so a burst of legitimate activity is fine; 5/second sustained,
 * which is far above anyone typing questions and far below anything scripted.
 */
const RATE_CAPACITY = 10;
const RATE_PER_SEC = 5;

function allow(ws: LiveSocket): boolean {
  const now = Date.now();
  ws.tokens = Math.min(RATE_CAPACITY, ws.tokens + ((now - ws.lastRefill) / 1000) * RATE_PER_SEC);
  ws.lastRefill = now;

  if (ws.tokens < 1) return false;
  ws.tokens -= 1;
  return true;
}

export function onConnection(raw: WsSocket, claims: SocketClaims): void {
  const ws = raw as LiveSocket;
  ws.userId = claims.userId;
  ws.tenantId = claims.tenantId;
  ws.role = claims.role;
  ws.rooms = new Set();
  ws.isAlive = true;
  ws.tokens = RATE_CAPACITY;
  ws.lastRefill = Date.now();

  /**
   * A 15-minute token now backs a connection that may live for hours.
   *
   * Without this, a socket authenticated once keeps working forever — so a user
   * who is banned, or removed from the tenant, keeps their live session
   * indefinitely. Closing at `exp` with 4401 caps revocation lag at one token
   * lifetime and tells the client exactly what to do: refresh, reconnect.
   *
   * Re-verifying on every message would be pure CPU: the token does not change.
   */
  const msToExpiry = claims.exp * 1000 - Date.now();
  const expiryTimer = setTimeout(
    () => ws.close(CLOSE.TOKEN_EXPIRED, 'token expired'),
    Math.max(0, msToExpiry),
  );

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  ws.on('message', (data) => {
    void (async () => {
      if (!allow(ws)) {
        ws.close(CLOSE.RATE_LIMITED, 'slow down');
        return;
      }

      let json: unknown;
      try {
        json = JSON.parse(data.toString());
      } catch {
        ws.close(CLOSE.POLICY, 'invalid json');
        return;
      }

      const parsed = clientMessage.safeParse(json);
      if (!parsed.success) {
        send(ws, { t: 'error', code: 'bad_message' });
        return;
      }

      try {
        await handleClientMessage(ws, parsed.data);
      } catch (err) {
        logger.error({ err, t: parsed.data.t }, 'handler failed');
        send(ws, { t: 'error', code: 'internal' });
      }
    })();
  });

  ws.on('close', () => {
    clearTimeout(expiryTimer);
    void leaveAll(ws);
  });

  ws.on('error', (err) => logger.debug({ err }, 'socket error'));

  send(ws, { t: 'ready', expiresAt: claims.exp });
}

/**
 * TCP DOES NOT TELL YOU THE PEER IS GONE.
 *
 * If a laptop lid closes or a NAT table entry expires, no FIN and no RST is ever
 * sent. The socket sits in readyState OPEN forever, holding heap and a file
 * descriptor, and every message written to it disappears. WebSocket has
 * protocol-level ping/pong frames for exactly this.
 *
 * terminate(), not close(): close() starts a closing handshake and waits for a
 * reply from a peer that no longer exists, so the socket lingers anyway.
 *
 * 30 seconds sits under the shortest idle timeout in a typical path — an ALB
 * defaults to 60 s and a Kong route to 60000 ms — and ping frames count as
 * traffic, so the sweep doubles as what keeps those timers from firing.
 */
const HEARTBEAT_MS = Number(process.env['LIVE_HEARTBEAT_MS'] ?? 30_000);

const sweep = setInterval(() => {
  for (const client of wss.clients) {
    const ws = client as LiveSocket;
    if (!ws.isAlive) {
      ws.terminate();
      continue;
    }
    ws.isAlive = false;
    ws.ping();
  }
}, HEARTBEAT_MS);

// Do not hold the process open for a timer whose only job is housekeeping.
sweep.unref();

export const stopHeartbeat = (): void => clearInterval(sweep);
