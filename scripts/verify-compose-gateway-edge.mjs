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
 *
 * The limits are exercised on scratch nodes with a limit of 4 and their own Redis
 * databases, so the real stack's counters are never spent and nothing is left behind.
 *
 * Run with the compose stack up (docker compose up -d --wait):
 *   node scripts/verify-compose-gateway-edge.mjs
 */

import { randomBytes } from 'node:crypto';
import { KONG_CONTAINER, admin, containerLogs, docker, echoConfig, loadConfig, runningConfig, send, sleep, startEcho, startScratchKong, waitFor } from './lib/kong.mjs';

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
  const scrubbed = ['request.headers.authorization', 'request.headers.cookie', 'request.querystring', 'upstream_uri'];
  check(
    log && scrubbed.every((f) => log.config.custom_fields_by_lua?.[f] === 'return nil'),
    `the JSON access log removes ${scrubbed.join(', ')} (${log ? Object.keys(log.config.custom_fields_by_lua ?? {}).join(', ') : 'no file-log'})`,
  );
  const node = await admin(appImage, '/');
  check(node?.configuration?.proxy_access_log === 'off', `the nginx access log, which writes the request line whole, is off (${node?.configuration?.proxy_access_log})`);

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
      headers: { origin, 'access-control-request-method': 'POST', 'access-control-request-headers': 'authorization,content-type,idempotency-key' },
    });
  for (const origin of ['http://acme.eventhub.localhost:5173', 'https://globex.eventhub.com']) {
    const res = await preflight(origin);
    check(
      res.status === 200 &&
        res.headers['access-control-allow-origin'] === origin &&
        res.headers['access-control-allow-credentials'] === 'true' &&
        /POST/.test(res.headers['access-control-allow-methods'] ?? '') &&
        /authorization/i.test(res.headers['access-control-allow-headers'] ?? ''),
      `a preflight from ${origin} is allowed, with credentials (${res.status}, allow-origin ${res.headers['access-control-allow-origin'] ?? 'none'})`,
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
  const kongRefusal = (res) => res.status === 413 && /Request size limit exceeded/.test(res.json?.message ?? '');
  const big = await send(PROXY, '/events', { host: TENANT_HOST, method: 'POST', headers: { 'content-type': 'application/json' }, body: `{"pad":"${'x'.repeat(2 * 1024 * 1024)}"}` });
  check(kongRefusal(big), `a 2 MiB body is refused by the gateway (${big.status} ${big.text.slice(0, 60)})`);
  const small = await send(PROXY, '/events', { host: TENANT_HOST, method: 'POST', headers: { 'content-type': 'application/json' }, body: `{"pad":"${'x'.repeat(512 * 1024)}"}` });
  check(!kongRefusal(small), `CONTROL: a 512 KiB body is passed on to the service (${small.status} ${small.text.slice(0, 60)})`);

  console.log('');
  console.log('  the running node counts in Redis');
  const limitOf = async (path) => Number((await send(PROXY, path, { host: TENANT_HOST })).headers['ratelimit-limit']);
  const [eventsLimit, authLimit] = [await limitOf('/events'), await limitOf('/auth/me')];
  check(eventsLimit === rl?.config.minute && authLimit === rlAuth?.config.minute, `responses carry the limit that applied: /events ${eventsLimit}, /auth/me ${authLimit}`);
  // The effect, not the setting: this minute's counter in Redis must rise when a request
  // goes through. Keys left by an earlier minute or an earlier configuration cannot
  // pass this; only a node counting in Redis now can.
  const realDb = rl?.config.redis?.database ?? 0;
  const minuteCounts = async () => {
    const keys = (await redis(realDb, '--scan', '--pattern', 'ratelimit:*:minute')).split('\n').filter(Boolean);
    const thisMinute = String(Math.floor(Date.now() / 60_000) * 60_000);
    const mine = keys.filter((k) => k.split(':').includes(thisMinute));
    let total = 0;
    for (const k of mine) total += Number(await redis(realDb, 'GET', k)) || 0;
    return total;
  };
  if (new Date().getUTCSeconds() > 55) await sleep(6_000);
  const countBefore = await minuteCounts();
  await send(PROXY, '/events', { host: TENANT_HOST });
  const countAfter = await minuteCounts();
  check(countAfter > countBefore, `one request through the running node raises this minute's count in Redis database ${realDb} (${countBefore} -> ${countAfter})`);

  console.log('');
  console.log('  the log has the request, and none of its credentials');
  const secret = `secret-${RUN}`;
  const since = new Date(Date.now() - 1000).toISOString();
  const logged = await send(PROXY, `/events?token=${secret}`, { host: TENANT_HOST, headers: { authorization: `Bearer ${secret}`, cookie: `session=${secret}` } });
  const loggedId = logged.headers['x-request-id'];
  const line = await waitFor(async () => (await containerLogs(KONG_CONTAINER, since)).split('\n').find((l) => l.includes(loggedId)), 10_000, 500);
  const entry = (() => {
    try {
      return JSON.parse(line ?? '');
    } catch {
      return null;
    }
  })();
  check(entry?.request?.uri === '/events', `CONTROL: the request is in the log by its id, as a JSON line for ${entry?.request?.uri ?? 'nothing found'}`);
  const all = await containerLogs(KONG_CONTAINER, since);
  const leaks = all.split('\n').filter((l) => l.includes(secret));
  check(leaks.length === 0, `its token, Authorization and cookie appear nowhere in Kong's output, either stream (${leaks.length} line(s))`);

  console.log('');
  console.log('  two nodes, one count');
  const network = await docker(['inspect', '-f', '{{range $k, $v := .NetworkSettings.Networks}}{{$k}}{{end}}', KONG_CONTAINER]);
  const image = await docker(['inspect', '-f', '{{.Config.Image}}', KONG_CONTAINER]);
  const echo = await startEcho();
  // The Admin API returns the deprecated flat fields (redis_host, redis_database, ...)
  // beside the nested `redis` block. Posted back with a changed `redis`, the two
  // disagree and the node refuses the config with a 500, so the flat ones are dropped.
  const withoutShorthands = (config) => Object.fromEntries(Object.entries(config).filter(([k]) => !k.startsWith('redis_')));
  const scratchConfig = (db, host = 'redis') =>
    echoConfig(running, echo.port, (p) =>
      p.name === 'rate-limiting'
        ? { ...p, config: { ...withoutShorthands(p.config), minute: SCRATCH_LIMIT, redis: { ...p.config.redis, host, database: db } } }
        : p,
    );
  const before = await Promise.all([countKeys(SHARED_DB), countKeys(OTHER_DB)]);
  check(before[0] === 0 && before[1] === 0, `CONTROL: the scratch databases ${SHARED_DB} and ${OTHER_DB} start empty (${before.join(', ')} keys)`);
  try {
    if (before[0] !== 0 || before[1] !== 0) return;
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
      return { status: res.status, reached: Boolean(up), client: up?.headers['x-real-ip'] ?? null };
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

    // A count lives for one calendar minute, so start the test in a fresh one: the
    // warm-ups' count stays behind in the last, and there are 60 s for five requests.
    await sleep((62 - new Date().getUTCSeconds()) * 1000);

    const fromA = [];
    for (let i = 0; i < SCRATCH_LIMIT; i += 1) fromA.push(await through(a, '/events'));
    const clientsA = [...new Set(fromA.map((r) => r.client).filter(Boolean))];
    check(fromA.every((r) => r.status === 200 && r.reached), `node A passes ${SCRATCH_LIMIT} requests, the limit (${fromA.map((r) => r.status).join(' ')}; it saw the client as ${clientsA.join(', ')})`);
    const overB = await through(b, '/events');
    check(overB.status === 429 && !overB.reached, `node B refuses the next one, counted with A's in Redis (${overB.status}, reached the service: ${overB.reached})`);

    const other = await loadConfig(b, scratchConfig(OTHER_DB));
    const ownB = other.status === 201 ? await waitFor(async () => (await through(b, '/events')).reached || null, 10_000, 250) : null;
    check(Boolean(ownB), `CONTROL: the same node on database ${OTHER_DB} passes it, so the refusal came from the shared count (${ownB ? 'passed' : 'refused'})`);

    const lost = await loadConfig(b, scratchConfig(SHARED_DB, 'redis-unreachable.invalid'));
    const open = lost.status === 201 ? await waitFor(async () => {
      const r = await through(b, '/events');
      return r.reached ? r : null;
    }, 15_000, 500) : null;
    check(Boolean(open), `with Redis unreachable the limit fails open: the request still reaches the service (${open ? open.status : 'refused or failed'})`);
  } finally {
    await Promise.all([docker(['rm', '-f', SCRATCH_A]).catch(() => {}), docker(['rm', '-f', SCRATCH_B]).catch(() => {})]);
    await echo.close();
    const removed = [];
    for (const db of [SHARED_DB, OTHER_DB]) {
      const keys = (await redis(db, '--scan', '--pattern', 'ratelimit:*')).split('\n').filter(Boolean);
      for (const k of keys) await redis(db, 'DEL', k);
      removed.push(`${keys.length} from ${db}`);
    }
    const left = (await Promise.all([countKeys(SHARED_DB), countKeys(OTHER_DB)])).reduce((s, n) => s + n, 0);
    const nodes = await docker(['ps', '-aq', '--filter', `name=eventhub-kong-edge-.-${RUN}`]);
    check(left === 0 && nodes === '', `cleanup: deleted ${removed.join(' and ')}, ${left} left; scratch nodes ${nodes === '' ? 'removed' : 'STILL THERE'}`);
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
