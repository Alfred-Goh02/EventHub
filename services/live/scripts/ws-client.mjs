/**
 * A Live client you can type into, for seeing fan-out with your own eyes.
 *
 * The automated proof is scripts/verify-live-fanout.mjs. This is the manual one:
 * open it against two pods, type into one, watch the question appear in the
 * other terminal. A single-pod demo shows nothing, because the interesting claim
 * is that a message crosses processes.
 *
 *   node services/live/scripts/ws-client.mjs 3004 <accessToken> <eventId>
 *   node services/live/scripts/ws-client.mjs 3014 <accessToken> <eventId>
 *
 * Then `docker compose stop redis` and type again: the message now reaches
 * nobody at all, including you — delivery happens only in the Redis subscriber
 * callback, so the author's own copy travels the same path as everyone else's.
 */

import { randomUUID } from 'node:crypto';
import WebSocket from 'ws';

const [port, token, eventId] = process.argv.slice(2);

if (!port || !token || !eventId) {
  console.error('usage: ws-client.mjs <port> <accessToken> <eventId>');
  process.exit(2);
}

const ws = new WebSocket(`ws://127.0.0.1:${port}/live`, ['eventhub.v1', `eh.token.${token}`]);

ws.on('open', () => {
  // Must print eventhub.v1. If it ever prints the token entry, the JWT is being
  // echoed back in the Sec-WebSocket-Protocol response header.
  console.log(`open on :${port}, subprotocol = ${ws.protocol}`);
  ws.send(JSON.stringify({ t: 'subscribe', eventId }));
  console.log('type a question and press enter');
});

ws.on('message', (data) => console.log('<-', data.toString()));
ws.on('close', (code, reason) => console.log(`closed ${code} ${reason.toString()}`));
ws.on('error', (err) => console.error('error', err.message));

process.stdin.on('data', (line) => {
  const body = line.toString().trim();
  if (!body) return;
  ws.send(JSON.stringify({ t: 'question.create', eventId, body, clientMsgId: randomUUID() }));
});
