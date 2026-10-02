/**
 * Asserts that a token is honoured only on its own tenant's subdomain, over HTTP and
 * over a WebSocket, and that Live's socket check is the same check as everyone else's.
 *
 * The gateway names the subdomain a request came in on in X-Tenant-Slug, and Auth puts
 * the token's tenant's slug in the token. When the two disagree, the service refuses
 * with 403 rather than quietly acting on the token's tenant: an acme session that worked
 * on globex's pages would corrupt every cache, CORS rule and audit trail that assumes a
 * subdomain is one tenant.
 *
 * HTTP is asserted on Event's /whoami, which is the shared middleware
 * (@eventhub/auth-middleware) and nothing else; every HTTP service uses that middleware.
 * The socket is asserted on Live's handshake, which verifies on its own.
 *
 *   - Matching slug: accepted. No slug header at all (a caller that did not come
 *     through the gateway): accepted, and the tenant is still the token's.
 *   - Another tenant's slug: 403, on both.
 *   - A token with no slug claim, through the gateway: 403, not waved through.
 *   - Live refuses a genuine signature with the wrong audience or issuer. It did not:
 *     its check passed anything Auth's key had signed in the right shape.
 *   - Live refuses an Origin that merely ends in the allowed suffix's letters, and
 *     refuses to start on a suffix with no leading dot, which is what allowed that.
 *
 * Every token here is minted with Auth's private key from services/auth/keys, so the
 * signature is genuine and only the claims differ. Needs Postgres, Redis and Kafka up,
 * and Event and Live built:
 *   node scripts/verify-token-tenant.mjs
 */

import { spawn } from 'node:child_process';
import { createPrivateKey, randomBytes, sign } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import WebSocket from 'ws';
import { serviceEnv } from './lib/service-env.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const EVENT_PORT = 3941;
const LIVE_PORT = 3942;
const RUN = randomBytes(4).toString('hex');

const PUBLIC_B64 = readFileSync(join(ROOT, 'services', 'auth', 'keys', 'public.pem')).toString('base64');
const PRIVATE = createPrivateKey(readFileSync(join(ROOT, 'services', 'auth', 'keys', 'private.pem')));
const eventEnv = serviceEnv(ROOT, 'event', { PORT: String(EVENT_PORT), AUTH_PUBLIC_KEY_B64: PUBLIC_B64 });
const { JWT_ISSUER: ISSUER, JWT_AUDIENCE: AUDIENCE } = eventEnv;

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const b64url = (v) => Buffer.from(v).toString('base64url');
/** A token signed with Auth's real key; `over` replaces or removes (undefined) claims. */
function token(over = {}) {
  const now = Math.floor(Date.now() / 1000);
  const claims = { sub: `user_tt_${RUN}`, tenantId: 'tenant_acme', tenantSlug: 'acme', role: 'ATTENDEE', iss: ISSUER, aud: AUDIENCE, iat: now, exp: now + 600, ...over };
  for (const [k, v] of Object.entries(claims)) if (v === undefined) delete claims[k];
  const head = b64url(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
  const body = b64url(JSON.stringify(claims));
  return `${head}.${body}.${b64url(sign('sha256', Buffer.from(`${head}.${body}`), PRIVATE))}`;
}

/** Starts a built service; resolves with the child and how it ended, if it did. */
function start(service, env) {
  const child = spawn(process.execPath, ['dist/index.js'], { cwd: join(ROOT, 'services', service), env, stdio: ['ignore', 'pipe', 'pipe'] });
  let output = '';
  child.stdout.on('data', (d) => (output += d));
  child.stderr.on('data', (d) => (output += d));
  const exited = new Promise((r) => child.on('exit', (code) => r(code)));
  return { child, exited, output: () => output };
}
async function waitHealthy(port, exited) {
  const deadline = Date.now() + 30_000;
  while (Date.now() < deadline) {
    const up = await fetch(`http://127.0.0.1:${port}/health`).then((r) => r.ok, () => false);
    if (up) return true;
    const gone = await Promise.race([exited.then(() => true), sleep(300).then(() => false)]);
    if (gone) return false;
  }
  return false;
}

async function whoami(bearer, headers = {}) {
  const res = await fetch(`http://127.0.0.1:${EVENT_PORT}/whoami`, { headers: { Authorization: `Bearer ${bearer}`, ...headers } });
  const body = await res.json().catch(() => null);
  return { status: res.status, tenantId: body?.tenantId ?? null, error: body?.error ?? null };
}

/** A Live handshake: 101 when it opened, else the HTTP status it was refused with. */
function handshake(bearer, headers = {}) {
  return new Promise((done) => {
    const ws = new WebSocket(`ws://127.0.0.1:${LIVE_PORT}/live`, ['eventhub.v1', `eh.token.${bearer}`], { headers });
    const timer = setTimeout(() => finish('timeout'), 5_000);
    function finish(result) {
      clearTimeout(timer);
      ws.removeAllListeners();
      ws.on('error', () => {});
      ws.terminate();
      done(result);
    }
    ws.on('open', () => finish(101));
    ws.on('unexpected-response', (_req, res) => finish(res.statusCode));
    ws.on('error', (err) => finish(`error ${err.message}`));
  });
}

async function main() {
  const event = start('event', eventEnv);
  const liveEnv = serviceEnv(ROOT, 'live', { PORT: String(LIVE_PORT), AUTH_PUBLIC_KEY_B64: PUBLIC_B64, LIVE_DRAIN_DELAY_MS: '0' });
  const live = start('live', liveEnv);
  try {
    const [eventUp, liveUp] = await Promise.all([waitHealthy(EVENT_PORT, event.exited), waitHealthy(LIVE_PORT, live.exited)]);
    check(eventUp && liveUp, `CONTROL: Event and Live started (event ${eventUp}, live ${liveUp})`);
    if (!eventUp || !liveUp) {
      console.error(`${event.output().slice(-1500)}\n${live.output().slice(-1500)}`);
      return;
    }

    console.log('');
    console.log('  over HTTP, through the shared middleware (Event /whoami)');
    const acme = token();
    const direct = await whoami(acme);
    check(direct.status === 200 && direct.tenantId === 'tenant_acme', `CONTROL: an acme token with no slug header is accepted, as tenant_acme (${direct.status}, ${direct.tenantId})`);
    const own = await whoami(acme, { 'X-Tenant-Slug': 'acme' });
    check(own.status === 200, `on acme's subdomain it is accepted (${own.status})`);
    const other = await whoami(acme, { 'X-Tenant-Slug': 'globex' });
    check(other.status === 403, `on globex's subdomain it is refused with 403 (${other.status} ${JSON.stringify(other.error)})`);
    const unslugged = token({ tenantSlug: undefined });
    const oldDirect = await whoami(unslugged);
    check(oldDirect.status === 200, `CONTROL: a token with no slug claim, called directly, is accepted (${oldDirect.status})`);
    const oldGateway = await whoami(unslugged, { 'X-Tenant-Slug': 'acme' });
    check(oldGateway.status === 403, `the same token through the gateway is refused, not waved through (${oldGateway.status})`);

    console.log('');
    console.log("  over a WebSocket, at Live's handshake");
    check((await handshake(acme)) === 101, 'CONTROL: an acme token opens a socket');
    check((await handshake(acme, { 'X-Tenant-Slug': 'acme' })) === 101, "and on acme's subdomain");
    const wsOther = await handshake(acme, { 'X-Tenant-Slug': 'globex' });
    check(wsOther === 403, `on globex's subdomain the handshake is refused with 403 (${wsOther})`);
    const wsOld = await handshake(unslugged, { 'X-Tenant-Slug': 'acme' });
    check(wsOld === 403, `a token with no slug claim, through the gateway, is refused (${wsOld})`);
    // A genuine signature on a token for someone else: what Auth's sealed email
    // requests are, given a role. The audience is what tells them apart.
    const wrongAud = await handshake(token({ aud: 'eventhub-notification' }));
    check(wrongAud === 401, `a genuine signature for another audience is refused (${wrongAud})`);
    const wrongIss = await handshake(token({ iss: 'https://someone-else.example' }));
    check(wrongIss === 401, `a genuine signature from another issuer is refused (${wrongIss})`);
    const suffix = liveEnv.ALLOWED_ORIGIN_SUFFIX;
    const goodOrigin = await handshake(acme, { Origin: `https://acme${suffix}` });
    check(goodOrigin === 101, `CONTROL: Origin https://acme${suffix} is allowed (${goodOrigin})`);
    const lookalike = `https://evil${suffix.slice(1)}`;
    const badOrigin = await handshake(acme, { Origin: lookalike });
    check(badOrigin === 403, `Origin ${lookalike}, which ends in the same letters, is refused (${badOrigin})`);
  } finally {
    event.child.kill();
    live.child.kill();
    await Promise.all([event.exited, live.exited]);
  }

  console.log('');
  console.log('  and Live will not start on a suffix that is not a label boundary');
  const undotted = start('live', serviceEnv(ROOT, 'live', { PORT: String(LIVE_PORT), AUTH_PUBLIC_KEY_B64: PUBLIC_B64, ALLOWED_ORIGIN_SUFFIX: 'eventhub.local' }));
  const code = await Promise.race([undotted.exited, sleep(20_000).then(() => 'still running')]);
  if (code === 'still running') {
    undotted.child.kill();
    await undotted.exited;
  }
  check(code !== 'still running' && code !== 0 && /must start with a dot/.test(undotted.output()), `ALLOWED_ORIGIN_SUFFIX=eventhub.local is refused at startup (exit ${code})`);

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    return;
  }
  console.log("a token is honoured only on its own tenant's subdomain, on every path in");
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
