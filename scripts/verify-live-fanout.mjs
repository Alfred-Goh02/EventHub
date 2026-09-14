/**
 * Asserts that a question posted on one Live pod reaches a client on ANOTHER
 * Live pod — the one claim Chapter 5 exists to make good on.
 *
 * WHY TWO PODS AND NOT ONE. A WebSocket is a file descriptor owned by one
 * process; pod B cannot write to a socket held by pod A. A single-pod test
 * passes identically whether the fan-out layer exists or not, so it proves
 * nothing at all about the thing that was built. Until a message is seen
 * crossing between two processes, Redis Pub/Sub has not been demonstrated to do
 * anything.
 *
 * THE POSITIVE CONTROL IS FREE, AND THAT IS NOT AN ACCIDENT. Delivery happens
 * ONLY in the Redis subscriber callback — the publishing pod deliberately does
 * not also write to its own sockets, because Redis delivers the message back to
 * it and local clients would see everything twice. So the author's own copy
 * travels through Redis too: if the bus were broken, the poster would receive
 * nothing either. Asserting the author gets exactly one copy therefore proves
 * both that the pipeline works and that the double-delivery bug is absent.
 *
 * Run: node scripts/verify-live-fanout.mjs
 */

import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import WebSocket from 'ws';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const AUTH = process.env['AUTH_URL'] ?? 'http://localhost:3001';

const POD_A = 3104;
const POD_B = 3114;

const TENANT = 'tenant_acme';
const OTHER_TENANT = 'tenant_globex';
const USER = { email: 'alice@acme.com', password: 'hunter2222' };

/** Fresh per run, so nothing a previous run left behind can be mistaken for this one. */
const eventId = `evt_fanout_${Date.now()}`;

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    // HERE, not only in the gate at the end, per CLAUDE.md: a return inside the
    // try below runs the finally and skips that gate.
    process.exitCode = 1;
  }
}
function fatal(msg) {
  console.error(`\nFAIL: ${msg}`);
  process.exitCode = 1;
}

async function login(tenantId) {
  const res = await fetch(`${AUTH}/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Tenant-ID': tenantId },
    body: JSON.stringify(USER),
  });
  const body = await res.json().catch(() => null);
  if (!body?.accessToken) throw new Error(`could not log in to ${tenantId} (${res.status})`);
  return body.accessToken;
}

/**
 * A private Live instance on its own port.
 *
 * Private rather than reusing a running one for the same reason
 * verify-login-rate-limit.mjs spawns its own auth: the test needs two pods that
 * it starts and stops, and it must not depend on whatever happens to be up.
 */
async function startPod(port) {
  const child = spawn(process.execPath, ['--env-file-if-exists=.env', 'dist/index.js'], {
    cwd: join(ROOT, 'services', 'live'),
    env: { ...process.env, PORT: String(port), LIVE_DRAIN_DELAY_MS: '0' },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  let log = '';
  child.stdout.on('data', (d) => (log += d));
  child.stderr.on('data', (d) => (log += d));

  for (let i = 0; i < 80; i += 1) {
    try {
      const res = await fetch(`http://localhost:${port}/health`);
      if (res.ok) return { child, up: true, log: () => log };
    } catch {
      // not listening yet
    }
    if (child.exitCode !== null) break;
    await new Promise((r) => setTimeout(r, 250));
  }
  return { child, up: false, log: () => log };
}

/** A connected client that records every server message it is sent. */
function connect(port, token) {
  const ws = new WebSocket(`ws://127.0.0.1:${port}/live`, [
    'eventhub.v1',
    `eh.token.${token}`,
  ]);
  const seen = [];

  ws.on('message', (raw) => {
    try {
      seen.push(JSON.parse(raw.toString()));
    } catch {
      seen.push({ t: 'unparseable', raw: raw.toString() });
    }
  });

  const ready = new Promise((res, rej) => {
    ws.once('open', () => res(ws));
    ws.once('error', rej);
    ws.once('close', (code) => rej(new Error(`closed before open: ${code}`)));
  });

  return { ws, seen, ready, of: (t) => seen.filter((m) => m.t === t) };
}

const settle = (ms = 600) => new Promise((r) => setTimeout(r, ms));

async function main() {
  const podA = await startPod(POD_A);
  const podB = await startPod(POD_B);

  try {
    check(podA.up, podA.up ? `pod A is listening on :${POD_A}` : 'pod A never came up');
    check(podB.up, podB.up ? `pod B is listening on :${POD_B}` : 'pod B never came up');
    if (!podA.up || !podB.up) {
      console.error(podA.up ? podB.log() : podA.log());
      fatal('cannot test fan-out without two pods');
      return;
    }

    const token = await login(TENANT);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  two clients, two pods, one event');
    // -----------------------------------------------------------------------
    const a = connect(POD_A, token);
    const b = connect(POD_B, token);
    await Promise.all([a.ready, b.ready]);

    check(
      a.ws.protocol === 'eventhub.v1' && b.ws.protocol === 'eventhub.v1',
      `the negotiated subprotocol is "${a.ws.protocol}" — the token entry is NOT echoed back`,
    );

    a.ws.send(JSON.stringify({ t: 'subscribe', eventId }));
    b.ws.send(JSON.stringify({ t: 'subscribe', eventId }));
    await settle();

    check(
      a.of('subscribed').length === 1 && b.of('subscribed').length === 1,
      'both clients are subscribed to the same event on different pods',
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  a question posted on pod A reaches the client on pod B');
    // -----------------------------------------------------------------------
    const clientMsgId = randomUUID();
    const body = `does fan-out work? ${eventId}`;
    a.ws.send(JSON.stringify({ t: 'question.create', eventId, body, clientMsgId }));
    await settle(1200);

    const onB = b.of('question.created');
    check(
      onB.length === 1,
      onB.length === 1
        ? 'pod B delivered it, so the message crossed process boundaries'
        : `pod B saw ${onB.length} copies — with 0, Redis Pub/Sub is not carrying anything ` +
          'between pods and every client only ever sees its own pod',
    );
    check(
      onB[0]?.question?.body === body && onB[0]?.clientMsgId === clientMsgId,
      'and it arrived intact, with the clientMsgId the author can match it by',
    );

    /**
     * EXACTLY ONE, on the author's own socket.
     *
     * Zero would mean the publish never happened or the subscriber is not
     * wired — the author's copy travels through Redis like everyone else's.
     * Two would mean the handler wrote locally AND published, which is the
     * classic bug: every client on the posting pod sees the message twice.
     */
    const onA = a.of('question.created');
    check(
      onA.length === 1,
      onA.length === 1
        ? 'and the author received exactly one copy — published, never written locally'
        : `the author received ${onA.length} copies. 2 means the handler wrote to local ` +
          'sockets as well as publishing; Redis delivers the message back to the publishing ' +
          'pod, so publishing alone is the whole job.',
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  and another tenant on the same event id hears nothing');
    // -----------------------------------------------------------------------
    /**
     * Not a check that can be forgotten — a construction. The channel is
     * live:t:{tenantId}:e:{eventId} with the tenant taken from the signed claim,
     * so a client in another tenant subscribes to a channel nobody publishes to.
     * Same event id, deliberately: guessing one must buy nothing.
     */
    const otherToken = await login(OTHER_TENANT);
    const c = connect(POD_B, otherToken);
    await c.ready;
    c.ws.send(JSON.stringify({ t: 'subscribe', eventId }));
    await settle();

    const before = c.of('question.created').length;
    a.ws.send(
      JSON.stringify({
        t: 'question.create',
        eventId,
        body: 'tenant B must never see this',
        clientMsgId: randomUUID(),
      }),
    );
    await settle(1200);

    check(
      c.of('question.created').length === before,
      c.of('question.created').length === before
        ? 'a client in another tenant, subscribed to the same event id, received nothing'
        : 'a client in ANOTHER TENANT received the question — the channel key is not isolating',
    );

    // The author's own pod still works, which proves the message was published
    // at all rather than the whole send having failed.
    check(
      a.of('question.created').length === 2,
      `and the second question still reached its own tenant (${a.of('question.created').length} total)`,
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  an upvote counts for the event its question belongs to, and no other');
    // -----------------------------------------------------------------------
    /**
     * The vote row and the count key only on the question id, and the Kafka
     * message Analytics counts carried the eventId the CLIENT sent. Unchecked,
     * any member of a tenant could credit an upvote to any event id, one that
     * does not exist included, and every engagement dashboard would disagree
     * with Live's own Postgres. Found by the review of Analytics' ingest.
     */
    const questionId = b.of('question.created')[0]?.question?.id;
    check(typeof questionId === 'string', `CONTROL: the question posted above has an id to vote on (${questionId})`);

    const errorsBefore = a.of('error').length;
    a.ws.send(JSON.stringify({ t: 'question.upvote', eventId: `${eventId}_elsewhere`, questionId }));
    await settle(1200);
    const refused = a.of('error').slice(errorsBefore);
    check(
      refused.length === 1 && refused[0].code === 'no_such_question',
      `an upvote naming another event id is refused as no_such_question (got ${JSON.stringify(refused)})`,
    );

    // The control that makes the refusal mean something: had the wrong-event vote
    // committed, this one would be already_voted and no count would arrive.
    a.ws.send(JSON.stringify({ t: 'question.upvote', eventId, questionId }));
    await settle(1200);
    const counted = b.of('question.upvoted').filter((m) => m.id === questionId);
    check(
      counted.length === 1 && counted[0].voteCount === 1,
      `CONTROL: the same vote under the question's own event counts, as its first: ${JSON.stringify(counted)}`,
    );

    for (const client of [a, b, c]) client.ws.close();
    await settle(200);
  } finally {
    podA.child.kill();
    podB.child.kill();
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('a question posted on one pod reaches the other, and only its own tenant');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
