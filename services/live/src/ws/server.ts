import type { IncomingMessage, Server as HttpServer } from 'node:http';
import type { Duplex } from 'node:stream';
import { WebSocketServer, type WebSocket as WsSocket } from 'ws';
import { createLogger } from '@eventhub/logger';
import { env } from '../env.js';
import { verifyAccessToken } from '../jwt.js';
import { onConnection } from './connection.js';

const logger = createLogger('live-ws');

const TOKEN_PREFIX = 'eh.token.';
const APP_PROTOCOL = 'eventhub.v1';

/**
 * noServer:true, because authentication has to finish BEFORE the handshake does.
 *
 * A WebSocket does not start as a WebSocket. The browser sends an ordinary
 * HTTP/1.1 GET with `Upgrade: websocket`; if the server agrees it replies
 * `101 Switching Protocols` and from that instant the socket stops speaking HTTP.
 * Everything you can do at the door happens during that one request — read the
 * token, check Origin, reject with a real HTTP status. After the 101 there are
 * no status codes, only close codes.
 *
 * Letting `ws` auto-accept would mean tracking a connection from anyone and
 * killing it later, which is a free memory and file-descriptor DoS plus a timer
 * to reap sockets that never authenticate.
 */
export const wss = new WebSocketServer({
  noServer: true,

  /**
   * The default is 100 MiB. One frame from a barely-authenticated client would
   * force an allocation that size. Nothing this protocol sends approaches 8 KB —
   * a question body is capped at 500 characters — and `ws` closes an oversize
   * frame with 1009 on its own.
   */
  maxPayload: 8 * 1024,

  /**
   * Off: it allocates a zlib context per connection, and ws's own documentation
   * warns about the memory cost. These are small JSON deltas; the compression
   * would buy little and cost per-socket heap at exactly the scale that matters.
   */
  perMessageDeflate: false,

  /**
   * MUST return only the public protocol, never the token entry.
   *
   * Whatever this returns is echoed in the Sec-WebSocket-Protocol RESPONSE
   * header. Returning the offered token would put the JWT straight back into a
   * response header — and the whole reason the token travels as a subprotocol
   * rather than a query parameter is to keep it out of places that get logged.
   */
  handleProtocols: (offered: Set<string>) => (offered.has(APP_PROTOCOL) ? APP_PROTOCOL : false),
});

/** A real HTTP response, which is only possible before the 101. */
function reject(socket: Duplex, status: number, text: string): void {
  socket.write(`HTTP/1.1 ${status} ${text}\r\nConnection: close\r\n\r\n`);
  socket.destroy();
}

/**
 * The browser WebSocket constructor takes a URL and a list of subprotocols, and
 * nothing else — there is no API for request headers, so `Authorization: Bearer`
 * is unavailable. The token rides in the subprotocol list as `eh.token.<jwt>`.
 *
 * Explicitly NOT the query string, which is the most common way this is done:
 * URLs land in load balancer access logs, the gateway's access log, browser
 * history and any Referer header. A 15-minute bearer token sitting in a log
 * aggregator that half the company can read is a real incident.
 */
function extractToken(header: string | string[] | undefined): string | null {
  if (!header) return null;
  const raw = Array.isArray(header) ? header.join(',') : header;

  for (const entry of raw.split(',')) {
    const value = entry.trim();
    if (value.startsWith(TOKEN_PREFIX)) return value.slice(TOKEN_PREFIX.length);
  }
  return null;
}

export function attachUpgrade(server: HttpServer): void {
  server.on('upgrade', (req: IncomingMessage, socket: Duplex, head: Buffer) => {
    // Before handleUpgrade the socket has no error listener of its own, and an
    // unhandled 'error' on a stream takes the process down.
    socket.on('error', () => socket.destroy());

    const url = new URL(req.url ?? '/', `http://${req.headers.host ?? 'localhost'}`);
    if (url.pathname !== '/live') {
      reject(socket, 404, 'Not Found');
      return;
    }

    /**
     * WebSocket handshakes are NOT subject to CORS. The browser sends Origin and
     * enforces nothing at all, so a page on any origin can open a socket unless
     * the server refuses. Checking it costs one comparison.
     *
     * A missing Origin is allowed: non-browser clients (the test client, a CLI)
     * do not send one, and the token is what actually authenticates. Origin is
     * defence against a browser being used as a confused deputy.
     */
    const origin = req.headers.origin;
    if (origin) {
      let hostname: string;
      try {
        hostname = new URL(origin).hostname;
      } catch {
        reject(socket, 403, 'Forbidden');
        return;
      }
      if (!hostname.endsWith(env.ALLOWED_ORIGIN_SUFFIX)) {
        reject(socket, 403, 'Forbidden');
        return;
      }
    }

    const token = extractToken(req.headers['sec-websocket-protocol']);
    if (!token) {
      reject(socket, 401, 'Unauthorized');
      return;
    }

    verifyAccessToken(token).then(
      (claims) => {
        // One tenant's token on another tenant's subdomain, refused exactly as the
        // HTTP middleware refuses it (@eventhub/auth-middleware), and only when the
        // gateway has named a subdomain.
        const subdomain = req.headers['x-tenant-slug'];
        if (subdomain !== undefined && claims.tenantSlug !== subdomain) {
          reject(socket, 403, 'Forbidden');
          return;
        }
        // handleUpgrade adds the socket to wss.clients, which is what the
        // heartbeat sweep iterates.
        wss.handleUpgrade(req, socket, head, (ws) => onConnection(ws as WsSocket, claims));
      },
      (err: unknown) => {
        logger.debug({ err }, 'rejected an upgrade with an unusable token');
        reject(socket, 401, 'Unauthorized');
      },
    );
  });
}
