import { subscriber } from '../redis.js';
import { CLOSE } from './protocol.js';
import type { LiveSocket } from './connection.js';

/**
 * The pod-local room registry, and the heart of the chapter.
 *
 * A WebSocket is a file descriptor owned by ONE process. Pod B cannot write to a
 * socket held by pod A, so a question posted on B reaches subscribers on A and C
 * only by going through a bus every pod listens to. This map is the local half —
 * which of this pod's sockets care about a channel — and Redis is the other half.
 */
const rooms = new Map<string, Set<LiveSocket>>();

/**
 * 512 KB of unsent bytes and the socket goes.
 *
 * ws.send() never blocks. If the client's TCP receive window is full — bad wifi,
 * a laptop that slept, a phone in a tunnel — the data queues in THIS process's
 * heap. One stalled client is a slow leak; a few hundred during a network event
 * is an OOMKill that drops every other connection on the pod.
 *
 * There are exactly three options for a slow consumer: buffer without bound
 * (unacceptable), drop messages (fine for a ticker where the latest value
 * supersedes the last, wrong for chat where gaps are visible), or disconnect.
 * We disconnect, because the snapshot endpoint makes reconnecting cheap and
 * correct.
 */
const MAX_BUFFERED = 512 * 1024;

/**
 * ONE dispatcher for every channel.
 *
 * ioredis emits a single `message` event for all subscriptions rather than
 * per-channel callbacks, so routing by channel name happens here. Registered
 * once at module load; registering it per join would add a listener per room and
 * deliver each message as many times as there are rooms.
 */
subscriber.on('message', (channel: string, payload: string) => {
  deliver(channel, payload);
});

function deliver(channel: string, payload: string): void {
  const members = rooms.get(channel);
  if (!members) return;

  for (const ws of members) {
    if (ws.readyState !== ws.OPEN) continue;
    if (ws.bufferedAmount > MAX_BUFFERED) {
      ws.close(CLOSE.SLOW_CONSUMER, 'slow consumer');
      continue;
    }
    // Already a JSON string off the wire. Re-serialising per socket would be
    // the same object stringified N times for no reason.
    ws.send(payload);
  }
}

/**
 * Join, subscribing to Redis only on the 0-to-1 transition.
 *
 * REFCOUNTED PER CHANNEL, NOT `PSUBSCRIBE live:t:*`. The pattern subscribe is
 * one line and would deliver every tenant's traffic to every pod — fan-out cost
 * becomes O(pods x total traffic) instead of O(pods x relevant traffic), and a
 * single routing bug becomes cross-tenant data exposure rather than a dropped
 * message.
 */
export async function join(ws: LiveSocket, channel: string): Promise<void> {
  let members = rooms.get(channel);

  if (!members) {
    members = new Set();
    rooms.set(channel, members);
    await subscriber.subscribe(channel);
  }

  members.add(ws);
  ws.rooms.add(channel);
}

/** Leave, unsubscribing only on the 1-to-0 transition. */
export async function leave(ws: LiveSocket, channel: string): Promise<void> {
  const members = rooms.get(channel);
  if (!members) return;

  members.delete(ws);
  ws.rooms.delete(channel);
  if (members.size > 0) return;

  rooms.delete(channel);
  await subscriber.unsubscribe(channel);

  /**
   * A join can land while that unsubscribe is in flight — the await is a real
   * suspension point, and join() would have created a fresh Set and returned
   * early without subscribing, because the map entry it checks is the one this
   * function had already deleted. Re-check and repair.
   */
  if (rooms.has(channel)) await subscriber.subscribe(channel);
}

/** Everything this socket was in, for the close path. */
export async function leaveAll(ws: LiveSocket): Promise<void> {
  for (const channel of [...ws.rooms]) await leave(ws, channel);
}

export const roomCount = (): number => rooms.size;
export const membersOf = (channel: string): number => rooms.get(channel)?.size ?? 0;
