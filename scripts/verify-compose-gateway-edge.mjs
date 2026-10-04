/**
 * Asserts what the gateway does to every request on its way through, besides routing it
 * (verify-compose-gateway.mjs): a request id, CORS for the tenants' own origins only, a
 * body-size ceiling, a per-address rate limit counted in Redis and shared by every Kong
 * node, and an access log with the credentials taken out.
 *
 *   - Every response carries an X-Request-ID, a fresh one per request, and the service
 *     receives the same id.
 *   - A preflight from a tenant's origin is answered by Kong with that origin allowed,
 *     credentials and all. Origins that merely contain ours, or use http in production,
 *     or are somebody else's, get no Access-Control-Allow-Origin.
 *   - A body over a megabyte is refused by Kong; one under it reaches the service.
 *   - The running node counts in Redis (keys appear in its database), at 600 a minute
 *     and 120 on Auth's routes. Two OTHER nodes, loaded with the same configuration and
 *     pointed at one Redis database, share one count: the request one node allows past
 *     the limit, the other refuses. The control is the same node on another database,
 *     which allows it -- so the refusal came from the shared count and nothing else.
 *     With Redis unreachable, the limit fails open rather than failing the request.
 *   - The access log has a line for the request, by its id, with no Authorization, no
 *     cookie and no query string in it, and the nginx access log is off.
 *   - Token signatures are checked at the edge on exactly the routes where every path
 *     needs one, with Auth's own public key, rendered into Kong's config at start (the
 *     render refuses without the key). No token, another key's signature, HS256 keyed
 *     with the public key, an expired or wrong-issuer token signed with Auth's REAL key,
 *     and a real token in the query string or a cookie are all refused by Kong, in
 *     Kong's shape -- a service answers { error } -- and a real token reaches Event.
 *
 * The limits are exercised on scratch nodes with a limit of 4 and their own Redis
 * databases, so the real stack's counters are never spent and nothing is left behind.
 *
 * Run with the compose stack up (docker compose up -d --wait):
 *   node scripts/verify-compose-gateway-edge.mjs
 */

import { execFile } from 'node:child_process';
import { createHmac, generateKeyPairSync, randomBytes, sign } from 'node:crypto';
import { request as httpRequest } from 'node:http';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { KONG_CONTAINER, admin, containerLogs, docker, echoConfig, loadConfig, runningConfig, send, sleep, startEcho, startScratchKong, waitFor } from './lib/kong.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const PROXY = process.env['KONG_PROXY_URL'] ?? 'http://127.0.0.1:8000';
const NEIGHBOUR = 'eventhub-auth';
const REDIS_CONTAINER = 'eventhub-redis';
const TENANT_HOST = 'acme.eventhub.localhost';
const RUN = randomBytes(4).toString('hex');
const SCRATCH_A = `eventhub-kong-edge-a-${RUN}`;
const SCRATCH_B = `eventhub-kong-edge-b-${RUN}`;
/** The scratch nodes' Redis databases: their own, never the real node's. */
const SHARED_DB = 15;
const OTHER_DB = 14;
const SCRATCH_LIMIT = 4;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

const redis = (db, ...args) => docker(['exec', REDIS_CONTAINER, 'redis-cli', '-n', String(db), ...args]);
const countKeys = async (db) => (await redis(db, '--scan', '--pattern', 'ratelimit:*')).split('\n').filter(Boolean).length;

/**
 * The time on the Docker VM, from Redis, in ms. Kong keys its counts by its own clock and
 * `docker logs --since` compares the daemon's; the host's can drift from both after a
 * sleep, and every minute window and log window here is measured against theirs.
 */
async function vmNow() {
  const [seconds, micros] = (await docker(['exec', REDIS_CONTAINER, 'redis-cli', 'TIME'])).split('\n').map(Number);
  return seconds * 1000 + Math.floor(micros / 1000);
}

/** Streams a chunked body through the gateway; the status, and how much had been written when it answered. */
function streamUntilAnswered(path, total) {
  const chunk = Buffer.alloc(64 * 1024, 120);
  return new Promise((done) => {
    let written = 0;
    let answered = false;
    const timer = setTimeout(() => finish({ error: 'no answer in 60 s' }), 60_000);
    function finish(result) {
      clearTimeout(timer);
      req.destroy();
      done({ atBytes: written, ...result });
    }
    const url = new URL(path, PROXY);
    const req = httpRequest(
      { hostname: url.hostname, port: url.port, path: url.pathname, method: 'POST', headers: { host: TENANT_HOST, 'content-type': 'application/json' } },
      (res) => {
        answered = true;
        res.resume();
        finish({ status: res.statusCode });
      },
    );
    req.on('error', (err) => !answered && finish({ error: err.code ?? err.message }));
    (function pump() {
      while (written < total && !answered) {
        written += chunk.length;
        if (!req.write(chunk)) return req.once('drain', pump);
      }
      if (!answered) req.end();
    })();
  });
}

async function main() {
  console.log('  the configuration the running gateway loaded');
  const appImage = await docker(['inspect', '-f', '{{.Config.Image}}', NEIGHBOUR]);
  const running = await runningConfig(appImage);
  const routeName = new Map(running.routes.map((r) => [r.id, r.name]));
  const global = (name) => running.plugins.filter((p) => p.name === name && !p.route && !p.service && p.enabled);
  const onRoute = (name, route) => running.plugins.filter((p) => p.name === name && p.route && routeName.get(p.route.id) === route && p.enabled);
  check(running.plugins.length > 0, `CONTROL: read ${running.plugins.length} plugins from the running node`);

  const [cid] = global('correlation-id');
  check(cid?.config.header_name === 'X-Request-ID' && cid?.config.echo_downstream === true, `a global correlation-id plugin sets X-Request-ID and echoes it (${cid ? `${cid.config.header_name}, echo ${cid.config.echo_downstream}` : 'none'})`);
  const [rl] = global('rate-limiting');
  check(
    rl?.config.policy === 'redis' && rl?.config.limit_by === 'ip' && rl?.config.fault_tolerant === true && rl?.config.redis?.host === 'redis',
    `a global rate limit per address, counted in Redis, failing open (${rl ? `${rl.config.policy}, by ${rl.config.limit_by}, ${rl.config.minute}/min, fault tolerant ${rl.config.fault_tolerant}` : 'none'})`,
  );
  const [rlAuth] = onRoute('rate-limiting', 'auth');
  check(
    rlAuth?.config.policy === 'redis' && rlAuth?.config.minute > 0 && rlAuth.config.minute < (rl?.config.minute ?? 0),
    `Auth's route has a tighter limit, in the same Redis (${rlAuth ? `${rlAuth.config.minute}/min, ${rlAuth.config.policy}` : 'none'})`,
  );
  const [log] = global('file-log');
  const fields = log?.config.custom_fields_by_lua ?? {};
  check(
    /get_headers\(\)/.test(fields['request.headers'] ?? '') && /get_headers\(\)/.test(fields['response.headers'] ?? '') &&
      fields['request.querystring'] === 'return nil' && fields['upstream_uri'] === 'return nil',
    `the JSON access log replaces both sides' headers with an allowlist and drops the query string and upstream URI (${Object.keys(fields).join(', ') || 'no file-log'})`,
  );
  const node = await admin(appImage, '/');
  const conf = node?.configuration ?? {};
  check(conf.proxy_access_log === 'off', `the nginx access log, which writes the request line whole, is off (${conf.proxy_access_log})`);
  check(conf.log_level === 'notice', `Kong logs at notice: at info, routine lines carried the request line, token and all (${conf.log_level})`);
  check(conf.nginx_http_client_max_body_size === '1m', `nginx's own body limit is 1m, so a chunked body is refused as it arrives (${conf.nginx_http_client_max_body_size})`);

  console.log('');
  console.log('  a request id on every request');
  const first = await send(PROXY, '/events', { host: TENANT_HOST });
  const second = await send(PROXY, '/events', { host: TENANT_HOST });
  const ids = [first.headers['x-request-id'], second.headers['x-request-id']];
  check(ids.every((v) => UUID.test(v ?? '')) && ids[0] !== ids[1], `each response has its own X-Request-ID (${ids.join(', ')})`);

  console.log('');
  console.log("  CORS for the tenants' own origins, and only those");
  const preflight = (origin) =>
    send(PROXY, '/events', {
      host: TENANT_HOST,
      method: 'OPTIONS',
      headers: { origin, 'access-control-request-method': 'POST', 'access-control-request-headers': 'authorization,content-type,idempotency-key,if-match' },
    });
  // Every header the API requires must be allowed, or a browser cannot send it at all:
  // If-Match was missing, and the ticket-type editor and Event's PATCH both demand it.
  const REQUIRED_HEADERS = ['authorization', 'content-type', 'idempotency-key', 'if-match'];
  for (const origin of ['http://acme.eventhub.localhost:5173', 'https://globex.eventhub.com']) {
    const res = await preflight(origin);
    const allowed = (res.headers['access-control-allow-headers'] ?? '').toLowerCase().split(/\s*,\s*/);
    const missing = REQUIRED_HEADERS.filter((h) => !allowed.includes(h));
    check(
      res.status === 200 &&
        res.headers['access-control-allow-origin'] === origin &&
        res.headers['access-control-allow-credentials'] === 'true' &&
        /POST/.test(res.headers['access-control-allow-methods'] ?? '') &&
        missing.length === 0,
      `a preflight from ${origin} is allowed, with credentials and every header the API requires (${res.status}, allow-origin ${res.headers['access-control-allow-origin'] ?? 'none'}${missing.length ? `; not allowed: ${missing.join(', ')}` : ''})`,
    );
  }
  for (const [origin, what] of [
    ['https://acme.eventhub.com.attacker.example', 'one that only begins like ours'],
    ['http://acme.eventhub.localhost.attacker.example:5173', 'a local one that only begins like ours'],
    ['http://acme.eventhub.com', 'production over http'],
    ['https://eventhub.com.attacker.example', 'the apex, extended'],
    ['https://attacker.example', "somebody else's"],
  ]) {
    const res = await preflight(origin);
    check(res.headers['access-control-allow-origin'] === undefined, `${what} (${origin}) is not allowed (allow-origin ${res.headers['access-control-allow-origin'] ?? 'none'})`);
  }

  console.log('');
  console.log('  a ceiling on the body');
  // On Auth's sign-in, which needs no token: on a route with the edge signature check,
  // a request without one is refused with 401 before its size is looked at.
  // A refusal by the gateway is a 413 in Kong's shape, { message }; a service answers
  // { error }.
  const kongRefusal = (res) => res.status === 413 && typeof res.json?.message === 'string' && res.json?.error === undefined;
  const twoMiB = `{"pad":"${'x'.repeat(2 * 1024 * 1024)}"}`;
  // nginx may answer and close before the body is written; a reset after the answer is
  // still an answer, so the error is reported rather than thrown.
  const declared = await send(PROXY, '/auth/login', { host: TENANT_HOST, method: 'POST', headers: { 'content-type': 'application/json', 'content-length': String(Buffer.byteLength(twoMiB)) }, body: twoMiB }).catch(
    (err) => ({ status: null, json: null, headers: {}, text: `connection ${err.code ?? err.message}` }),
  );
  check(kongRefusal(declared), `a 2 MiB body with its Content-Length is refused by the gateway (${declared.status} ${declared.text.slice(0, 60)})`);
  const small = await send(PROXY, '/auth/login', { host: TENANT_HOST, method: 'POST', headers: { 'content-type': 'application/json' }, body: `{"pad":"${'x'.repeat(512 * 1024)}"}` });
  check(!kongRefusal(small), `CONTROL: a 512 KiB body is passed on to the service (${small.status} ${small.text.slice(0, 60)})`);
  // A CHUNKED body has no length to refuse on. Streamed 50 MiB, the gateway must answer
  // while it is still arriving, not after taking it all: measured without nginx's limit,
  // it took every byte, to disk, and only then said 413.
  const streamed = await streamUntilAnswered('/auth/login', 50 * 1024 * 1024);
  check(
    streamed.status === 413 && streamed.atBytes < 16 * 1024 * 1024,
    `a 50 MiB chunked body is refused before it is taken: ${streamed.status ?? streamed.error} after ${(streamed.atBytes / 1048576).toFixed(1)} MiB had been sent`,
  );

  console.log('');
  console.log('  the running node counts in Redis');
  // /live for the global limit: it carries no edge signature check, which would refuse
  // a tokenless request before the limit is counted.
  const limitOf = async (path) => Number((await send(PROXY, path, { host: TENANT_HOST })).headers['ratelimit-limit']);
  const [globalLimit, authLimit] = [await limitOf('/live'), await limitOf('/auth/me')];
  check(globalLimit === rl?.config.minute && authLimit === rlAuth?.config.minute, `responses carry the limit that applied: /live ${globalLimit}, /auth/me ${authLimit}`);
  // The effect, not the setting: this minute's counter in Redis must rise when a request
  // goes through. Keys left by an earlier minute or an earlier configuration cannot
  // pass this; only a node counting in Redis now can.
  const realDb = rl?.config.redis?.database ?? 0;
  const minuteCounts = async () => {
    const keys = (await redis(realDb, '--scan', '--pattern', 'ratelimit:*:minute')).split('\n').filter(Boolean);
    const thisMinute = String(Math.floor((await vmNow()) / 60_000) * 60_000);
    const mine = keys.filter((k) => k.split(':').includes(thisMinute));
    let total = 0;
    for (const k of mine) total += Number(await redis(realDb, 'GET', k)) || 0;
    return total;
  };
  if ((await vmNow()) % 60_000 > 55_000) await sleep(6_000);
  const countBefore = await minuteCounts();
  await send(PROXY, '/live', { host: TENANT_HOST });
  const countAfter = await minuteCounts();
  check(countAfter > countBefore, `one request through the running node raises this minute's count in Redis database ${realDb} (${countBefore} -> ${countAfter})`);

  console.log('');
  console.log('  the log has the request, and none of its credentials');
  // Down a route the request is PROXIED on -- Auth's, with no signature check to refuse
  // it first -- carrying the secret everywhere a credential rides: the query string (an
  // unsubscribe link), Authorization, a cookie, Live's subprotocol token, the Referer of
  // the unsubscribe page's own button, and two more a client might send.
  const secret = `secret-${RUN}`;
  const since = new Date((await vmNow()) - 2000).toISOString();
  const logged = await send(PROXY, `/auth/me?token=${secret}`, {
    host: TENANT_HOST,
    headers: {
      authorization: `Bearer ${secret}`,
      cookie: `session=${secret}`,
      'sec-websocket-protocol': `eventhub.v1, eh.token.${secret}`,
      referer: `https://api.eventhub.localhost/unsubscribe?token=${secret}`,
      'proxy-authorization': `Basic ${secret}`,
      'x-api-key': secret,
    },
  });
  const loggedId = logged.headers['x-request-id'];
  const line = await waitFor(async () => (await containerLogs(KONG_CONTAINER, since)).split('\n').find((l) => l.includes(loggedId)), 10_000, 500);
  const entry = (() => {
    try {
      return JSON.parse(line ?? '');
    } catch {
      return null;
    }
  })();
  check(
    entry?.request?.uri === '/auth/me' && entry?.request?.headers?.['x-request-id'] === loggedId && Boolean(entry?.upstream_status ?? entry?.response?.status),
    `CONTROL: the proxied request is in the log by its id, headers and all (${entry ? `${entry.request?.uri}, logged headers: ${Object.keys(entry.request?.headers ?? {}).join(', ')}` : 'nothing found'})`,
  );
  const allowed = new Set(['host', 'user-agent', 'content-type', 'content-length', 'origin', 'x-request-id']);
  const extra = Object.keys(entry?.request?.headers ?? {}).filter((h) => !allowed.has(h));
  check(extra.length === 0, `only allowlisted request headers are logged (${extra.join(', ') || 'none other'})`);
  const all = await containerLogs(KONG_CONTAINER, since);
  const leaks = all.split('\n').filter((l) => l.includes(secret));
  check(leaks.length === 0, `the secret appears nowhere in Kong's output, either stream, from any of those seven places (${leaks.length} line(s))`);

  console.log('');
  console.log('  signatures checked at the edge');
  // Which routes: every route where every path needs a token, and no other. Read from
  // the running node, so a route added later has to be put on one side or the other.
  const SIGNED = ['analytics', 'community', 'event', 'notification', 'search', 'ticketing'];
  const jwtPlugins = running.plugins.filter((p) => p.name === 'jwt' && p.enabled);
  const jwtRoutes = jwtPlugins.map((p) => routeName.get(p.route?.id)).sort();
  check(
    JSON.stringify(jwtRoutes) === JSON.stringify(SIGNED) && jwtPlugins.every((p) => p.route && !p.service),
    `the signature check is on exactly ${SIGNED.join(', ')} -- not Auth, Live or the tenantless routes (${jwtRoutes.join(', ') || 'none'})`,
  );
  check(
    jwtPlugins.length > 0 &&
      jwtPlugins.every(
        (p) =>
          p.config.claims_to_verify?.includes('exp') &&
          JSON.stringify(p.config.header_names) === '["authorization"]' &&
          (p.config.uri_param_names ?? []).length === 0 &&
          (p.config.cookie_names ?? []).length === 0 &&
          p.config.run_on_preflight === false,
      ),
    'each checks exp, reads the token from Authorization only (never a cookie or the query string), and lets a preflight through',
  );
  const creds = await admin(appImage, '/consumers/eventhub-auth/jwt').catch(() => null);
  const cred = creds?.data?.[0];
  const authKey = await docker(['exec', NEIGHBOUR, 'cat', 'keys/public.pem']).catch(() => '');
  const norm = (pem) => (pem ?? '').replace(/\s+/g, '');
  check(
    creds?.data?.length === 1 && cred.algorithm === 'RS256' && norm(cred.rsa_public_key) === norm(authKey) && norm(authKey).length > 100,
    `one consumer, eventhub-auth, verifies with Auth's own public key, RS256, for issuer ${cred?.key ?? 'none'}`,
  );

  // The render step, run as compose runs it in a throwaway container: with Auth's key
  // volume it writes a config that Kong then PARSES -- the file Kong loads, not the
  // template CI parses -- and with the key's path pointing nowhere it refuses, rather
  // than start Kong on a config with no key in it.
  const kongDir = join(ROOT, 'infra', 'kong');
  const kongImage = await docker(['inspect', '-f', '{{.Config.Image}}', KONG_CONTAINER]);
  const keyVolume = await docker(['inspect', '-f', '{{range .Mounts}}{{if eq .Destination "/run/eventhub/keys"}}{{.Name}}{{end}}{{end}}', KONG_CONTAINER]);
  const render = (keyFile) =>
    new Promise((resolve) =>
      execFile(
        'docker',
        ['run', '--rm', '-v', `${kongDir}:/kong/declarative:ro`, '-v', `${keyVolume}:/run/eventhub/keys:ro`,
          '-e', `AUTH_PUBLIC_KEY_FILE=${keyFile}`, '-e', 'JWT_ISSUER=https://auth.eventhub.local', '-e', 'KONG_DECLARATIVE_CONFIG=/tmp/kong.yml', '-e', 'KONG_DATABASE=off',
          '--entrypoint', 'sh', kongImage, '-c', 'resty /kong/declarative/render-consumer.lua && kong config parse /tmp/kong.yml'],
        (err, stdout, stderr) => resolve({ code: err ? err.code : 0, out: `${stdout}${stderr}` }),
      ),
    );
  const rendered = await render('/run/eventhub/keys/public.pem');
  const missing = await render('/run/eventhub/keys/absent.pem');
  check(
    rendered.code === 0 && /wrote \/tmp\/kong\.yml/.test(rendered.out) && /parse successful/.test(rendered.out),
    `CONTROL: the render step writes a config Kong parses, with Auth's key volume (exit ${rendered.code}${/parse successful/.test(rendered.out) ? ', parse successful' : ''})`,
  );
  check(missing.code !== 0 && /cannot read/.test(missing.out), `and refuses, exit ${missing.code}, when the key is not there: ${missing.out.trim().slice(0, 90)}`);

  const realToken = (await send(PROXY, '/auth/login', {
    host: TENANT_HOST, method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ email: 'alice@acme.com', password: 'hunter2222' }),
  })).json?.accessToken;
  const issuer = cred?.key ?? '';
  const b64url = (v) => Buffer.from(v).toString('base64url');
  const now = Math.floor(Date.now() / 1000);
  const body = b64url(JSON.stringify({ sub: `u_edge_${RUN}`, tenantId: 'tenant_acme', tenantSlug: 'acme', role: 'ATTENDEE', iss: issuer, aud: 'eventhub-api', iat: now, exp: now + 600 }));
  const rs256 = (key) => {
    const head = b64url(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
    return `${head}.${body}.${b64url(sign('sha256', Buffer.from(`${head}.${body}`), key))}`;
  };
  const forged = rs256(generateKeyPairSync('rsa', { modulusLength: 2048 }).privateKey);
  // Keyed with the public key EXACTLY as Kong holds it -- the render keeps a trailing
  // newline that a trimmed copy lacks, and a token keyed with the wrong bytes would be
  // refused even by a Kong that let the token choose its algorithm. The trimmed copy is
  // tried too.
  const hsHead = b64url(JSON.stringify({ alg: 'HS256', typ: 'JWT' }));
  const hs = (key) => `${hsHead}.${body}.${b64url(createHmac('sha256', key).update(`${hsHead}.${body}`).digest())}`;
  const confused = hs(cred?.rsa_public_key ?? '');
  const confusedTrimmed = hs((cred?.rsa_public_key ?? '').trim());
  // Two tokens with Auth's REAL signature, minted inside Auth's container with its
  // private key: one expired, one naming another issuer. Only the edge's own checks
  // could be what refuses them there.
  const minted = JSON.parse(
    await docker([
      'exec', NEIGHBOUR, 'node', '--input-type=module', '-e',
      `import { importPKCS8, SignJWT } from 'jose';
       import { readFileSync } from 'node:fs';
       const key = await importPKCS8(readFileSync('keys/private.pem', 'utf8'), 'RS256');
       const now = Math.floor(Date.now() / 1000);
       const claims = { tenantId: 'tenant_acme', tenantSlug: 'acme', role: 'ATTENDEE' };
       const make = (iss, iat, exp) => new SignJWT(claims).setProtectedHeader({ alg: 'RS256' }).setSubject('u_edge_${RUN}')
         .setIssuer(iss).setAudience(process.env.JWT_AUDIENCE).setIssuedAt(iat).setExpirationTime(exp).sign(key);
       console.log(JSON.stringify({ expired: await make(process.env.JWT_ISSUER, now - 3600, now - 1800), otherIssuer: await make('https://someone-else.example', now, now + 600) }));`,
    ]).catch((err) => JSON.stringify({ error: err.message })),
  );
  check(Boolean(realToken) && Boolean(minted.expired), `CONTROL: a real token from the gateway's sign-in, and two minted with Auth's key (${minted.error ?? 'minted'})`);

  const atEvents = (headers, path = '/events') => send(PROXY, path, { host: TENANT_HOST, headers });
  const real = await atEvents({ authorization: `Bearer ${realToken}` });
  check(real.status === 200, `CONTROL: the real token reaches Event (${real.status})`);
  // Refused AT THE EDGE: 401 in Kong's shape. A service answers { error: ... }.
  const atEdge = (res) => res.status === 401 && res.json && res.json.error === undefined;
  for (const [what, headers, path] of [
    ['no token', {}],
    ['a token signed with another key', { authorization: `Bearer ${forged}` }],
    ['HS256, keyed with the public key as Kong holds it', { authorization: `Bearer ${confused}` }],
    ['HS256, keyed with the public key trimmed', { authorization: `Bearer ${confusedTrimmed}` }],
    ["Auth's signature, expired", { authorization: `Bearer ${minted.expired}` }],
    ["Auth's signature, another issuer", { authorization: `Bearer ${minted.otherIssuer}` }],
    ['the real token in the query string', {}, `/events?jwt=${realToken}`],
    ['the real token in a cookie', { cookie: `jwt=${realToken}` }],
  ]) {
    const res = await atEvents(headers, path);
    check(atEdge(res), `${what}: refused by the gateway (${res.status} ${res.text.slice(0, 60)})`);
  }

  console.log('');
  console.log('  two nodes, one count');
  const network = await docker(['inspect', '-f', '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}', KONG_CONTAINER]);
  const image = await docker(['inspect', '-f', '{{.Config.Image}}', KONG_CONTAINER]);
  // The Admin API returns the deprecated flat fields (redis_host, redis_database, ...)
  // beside the nested `redis` block. Posted back with a changed `redis`, the two
  // disagree and the node refuses the config with a 500, so the flat ones are dropped.
  const withoutShorthands = (config) => Object.fromEntries(Object.entries(config).filter(([k]) => !k.startsWith('redis_')));
  let echo = null;
  /** True once this run has found the scratch databases empty and may clear them after. */
  let owned = false;
  try {
    echo = await startEcho();
    const scratchConfig = (db, host = 'redis') =>
      echoConfig(running, echo.port, (p) =>
        p.name === 'jwt'
          ? null // the scratch nodes have no consumer to verify with; the section above asserts these
          : p.name === 'rate-limiting'
            ? { ...p, config: { ...withoutShorthands(p.config), minute: SCRATCH_LIMIT, redis: { ...p.config.redis, host, database: db } } }
            : p,
      );
    // Refuse rather than share: another run's live count in these databases would be
    // read as this one's, and deleting it would break that run.
    const before = await Promise.all([countKeys(SHARED_DB), countKeys(OTHER_DB)]);
    check(before[0] === 0 && before[1] === 0, `CONTROL: the scratch databases ${SHARED_DB} and ${OTHER_DB} start empty (${before.join(', ')} keys)`);
    if (before[0] !== 0 || before[1] !== 0) return;
    owned = true;
    const [a, b] = await Promise.all([startScratchKong({ name: SCRATCH_A, image, network }), startScratchKong({ name: SCRATCH_B, image, network })]);
    const loads = await Promise.all([loadConfig(a, scratchConfig(SHARED_DB)), loadConfig(b, scratchConfig(SHARED_DB))]);
    check(loads.every((l) => l.status === 201), `CONTROL: two scratch nodes loaded the running config, limited to ${SCRATCH_LIMIT} a minute, on Redis database ${SHARED_DB} (${loads.map((l) => l.status).join(', ')})`);
    if (!loads.every((l) => l.status === 201)) return console.error(loads.map((l) => l.text).join('\n'));

    /** A request through one node; whether the echo saw it, and the address that node saw. */
    const through = async (n, path) => {
      const seenBefore = echo.seen.length;
      const res = await send(n.proxy, path, { host: TENANT_HOST });
      await sleep(20);
      const up = echo.seen.length > seenBefore ? echo.seen[echo.seen.length - 1] : null;
      return { status: res.status, reached: Boolean(up), client: up?.headers['x-real-ip'] ?? null, limit: res.headers['ratelimit-limit'] ?? null };
    };
    // Both nodes must route before anything is counted. The warm-ups are counted too:
    // a global limit keeps one count per address across every route -- measured, the
    // two warm-ups left node A two requests short.
    const warm = await Promise.all([a, b].map((n) => waitFor(async () => (await through(n, '/reservations')).reached || null, 30_000, 250)));
    check(warm.every(Boolean), `CONTROL: both nodes route the loaded config (warm-ups reached the echo: ${warm.map(Boolean).join(', ')})`);
    if (!warm.every(Boolean)) return;

    // The id the client is given is the id the service received.
    const seenBeforeId = echo.seen.length;
    const idRes = await send(a.proxy, '/reservations', { host: TENANT_HOST });
    await sleep(20);
    const idUp = echo.seen.length > seenBeforeId ? echo.seen[echo.seen.length - 1].headers['x-request-id'] : null;
    check(UUID.test(idUp ?? '') && idUp === idRes.headers['x-request-id'], `the service receives the same X-Request-ID the client is given (${idUp ?? 'none'})`);

    // A count lives for one calendar minute -- Kong's, on the VM's clock -- so start the
    // test in a fresh one: the warm-ups' count stays behind in the last, and there is a
    // minute for what follows.
    await sleep(62_000 - ((await vmNow()) % 60_000));

    const fromA = [];
    for (let i = 0; i < SCRATCH_LIMIT; i += 1) fromA.push(await through(a, '/events'));
    const clientsA = [...new Set(fromA.map((r) => r.client).filter(Boolean))];
    check(fromA.every((r) => r.status === 200 && r.reached), `node A passes ${SCRATCH_LIMIT} requests, the limit (${fromA.map((r) => r.status).join(' ')}; it saw the client as ${clientsA.join(', ')})`);
    const overB = await through(b, '/events');
    check(overB.status === 429 && !overB.reached, `node B refuses the next one, counted with A's in Redis (${overB.status}, reached the service: ${overB.reached})`);

    // Each of the next two starts from a config that REFUSES the request, so a request
    // that gets through can only have met the new one: a 201 from /config does not mean
    // the worker is already serving it.
    //
    // Fail open: node B, over the shared limit and answering 429, now has Redis named
    // somewhere that does not exist. Only a node that lets the request through without
    // its count can pass it.
    const lost = await loadConfig(b, scratchConfig(SHARED_DB, 'redis-unreachable.invalid'));
    const open = lost.status === 201 ? await waitFor(async () => {
      const r = await through(b, '/events');
      return r.reached ? r : null;
    }, 15_000, 500) : null;
    check(Boolean(open), `with Redis unreachable the limit fails open: a request the shared count refused now reaches the service (${open ? `${open.status}, RateLimit-Limit ${open.limit ?? 'absent'}` : 'still refused, or failed'})`);

    // And the control, told apart from the config before it by the count it reports: on
    // its own database the node passes the request and says its limit is 4, so the 429
    // above came from the count it shared with A and from nothing else.
    const other = await loadConfig(b, scratchConfig(OTHER_DB));
    const ownB = other.status === 201 ? await waitFor(async () => {
      const r = await through(b, '/events');
      return r.reached && r.limit === String(SCRATCH_LIMIT) ? r : null;
    }, 10_000, 250) : null;
    check(Boolean(ownB), `CONTROL: the same node on database ${OTHER_DB} passes it, counting from its own store (${ownB ? `RateLimit-Limit ${ownB.limit}` : 'refused, or no count reported'})`);
  } finally {
    await Promise.all([docker(['rm', '-f', SCRATCH_A]).catch(() => {}), docker(['rm', '-f', SCRATCH_B]).catch(() => {})]);
    if (echo) await echo.close();
    const nodes = await docker(['ps', '-aq', '--filter', `name=eventhub-kong-edge-.-${RUN}`]).catch(() => '?');
    if (owned) {
      const removed = [];
      for (const db of [SHARED_DB, OTHER_DB]) {
        const keys = (await redis(db, '--scan', '--pattern', 'ratelimit:*')).split('\n').filter(Boolean);
        for (const k of keys) await redis(db, 'DEL', k);
        removed.push(`${keys.length} from ${db}`);
      }
      const left = (await Promise.all([countKeys(SHARED_DB), countKeys(OTHER_DB)])).reduce((s, n) => s + n, 0);
      check(left === 0 && nodes === '', `cleanup: deleted ${removed.join(' and ')}, ${left} left; scratch nodes ${nodes === '' ? 'removed' : 'STILL THERE'}`);
    } else {
      check(nodes === '', `cleanup: the scratch databases were not this run's, so nothing in them was touched; scratch nodes ${nodes === '' ? 'removed' : 'STILL THERE'}`);
    }
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    return;
  }
  console.log('every request gets an id, a CORS answer for our origins only, a body ceiling, a shared rate limit and a clean log line');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
