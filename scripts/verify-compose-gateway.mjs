/**
 * Asserts that the gateway routes each path to the service that owns it, and that no
 * client can tell a service which tenant it is.
 *
 * Shared-schema tenancy with row security holds only if the tenant a request carries is
 * one the client could not choose. Before the gateway, Auth read X-Tenant-ID from the
 * request and believed it. Kong now clears every tenant and consumer header a client
 * sends, and derives the tenant from the subdomain instead; this checks both halves, on
 * every route, against the configuration Kong is actually running.
 *
 * HOW, WITHOUT A TEST ROUTE IN THE REAL CONFIG. A header the gateway removed can only be
 * seen by whatever is behind it, and no real service echoes its headers. So the routes,
 * services and plugins are read from the running Kong's Admin API — the config it loaded,
 * not a copy of the file — and loaded into a second, throwaway Kong whose every upstream
 * is an echo server this script runs. Each upstream URL is that echo server plus
 * `/__upstream/<service>`, so one request shows which service a route reaches, the path
 * that service would receive, and every header that would arrive with it.
 *
 * EACH PROPERTY, AND WHAT WOULD LOOK THE SAME WITHOUT IT:
 *   - Every route reaches its own service, with the path that service routes on: Auth's
 *     /auth/ prefix stripped, every other path passed as it is. The expected table here
 *     is written out, not read from the config — it is what the config is checked
 *     against — and every route Kong has must be in it, so a new route cannot go
 *     unchecked.
 *   - Forged X-Tenant-ID, X-Tenant-Slug and the X-Consumer-* family are ABSENT upstream,
 *     not merely overwritten: an appended header would reach Node as "forged, real".
 *   - The tenant is the subdomain: X-Tenant-Slug is "acme" for acme.eventhub.localhost,
 *     also when the Host arrives upper-case and with a port. A reserved name, a second
 *     label, a malformed label or no tenant host at all never reaches a service.
 *   - The two routes with no tenant, on the api host, still clear the headers and add no
 *     slug.
 *   - The snippet that does this is global, so a route added later cannot miss it.
 *   - Retries are off on every service.
 *   - In the real stack: a path reaches the real service (its own error shape, not
 *     Kong's), Auth never sees a forged X-Tenant-ID, and nothing routes /health or
 *     /metrics.
 *   - The Admin API is unreachable from the host AND from every other container: it
 *     has no authentication, and POST /config replaces the whole configuration. It is
 *     read here from inside Kong's own network namespace.
 *
 * Run with the compose stack up (docker compose up -d --wait):
 *   node scripts/verify-compose-gateway.mjs
 */

import { execFile } from 'node:child_process';
import { createServer } from 'node:http';
import { randomBytes } from 'node:crypto';

const PROXY = process.env['KONG_PROXY_URL'] ?? 'http://127.0.0.1:8000';
const STATUS = process.env['KONG_STATUS_URL'] ?? 'http://127.0.0.1:8100';
const KONG_CONTAINER = 'eventhub-kong';
/** Another container on the compose network, with curl, to ask what Kong exposes there. */
const NEIGHBOUR = 'eventhub-auth';
const RUN = randomBytes(4).toString('hex');
const SCRATCH = `eventhub-kong-echo-${RUN}`;

/** What a client could send to pose as a tenant or a consumer. Every one must be gone upstream. */
const FORGED = {
  'X-Tenant-ID': 'tenant_victim',
  'X-Tenant-Slug': 'victim',
  'X-Consumer-ID': 'forged-consumer',
  'X-Consumer-Username': 'forged-user',
  'X-Consumer-Custom-ID': 'forged-custom',
  'X-Credential-Identifier': 'forged-credential',
  'X-Anonymous-Consumer': 'false',
  'X-Authenticated-Userid': 'forged-userid',
  'X-Authenticated-Scope': 'admin',
};

/**
 * Every route, the service it must reach, the host it answers on, and probes: what is
 * sent, and the path the service must receive.
 */
const EXPECTED = {
  auth: { service: 'auth', host: 'tenant', probes: [['/auth/login', '/login'], ['/auth/me', '/me']] },
  event: { service: 'event', host: 'tenant', probes: [['/events', '/events'], ['/events/evt_1?x=1', '/events/evt_1?x=1']] },
  ticketing: { service: 'ticketing', host: 'tenant', probes: [['/reservations', '/reservations']] },
  'ticketing-webhooks': { service: 'ticketing', host: 'api', probes: [['/webhooks/payment', '/webhooks/payment']] },
  live: { service: 'live', host: 'tenant', probes: [['/live', '/live']] },
  search: { service: 'search', host: 'tenant', probes: [['/search?q=jazz', '/search?q=jazz']] },
  analytics: { service: 'analytics', host: 'tenant', probes: [['/analytics/overview', '/analytics/overview']] },
  notification: { service: 'notification', host: 'tenant', probes: [['/preferences', '/preferences']] },
  'notification-unsubscribe': { service: 'notification', host: 'api', probes: [['/unsubscribe?token=t', '/unsubscribe?token=t']] },
  community: {
    service: 'community',
    host: 'tenant',
    probes: [['/spaces', '/spaces'], ['/posts/p1', '/posts/p1'], ['/comments/c1', '/comments/c1'], ['/feed', '/feed'], ['/reports', '/reports']],
  },
};
const TENANT_HOST = 'acme.eventhub.localhost';
const API_HOST = 'api.eventhub.localhost';

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
async function waitFor(fn, timeoutMs = 30_000, stepMs = 250) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await fn().catch(() => null);
    if (value) return value;
    await sleep(stepMs);
  }
  return null;
}
const docker = (args) =>
  new Promise((resolve, reject) =>
    execFile('docker', args, { maxBuffer: 8 * 1024 * 1024 }, (err, stdout, stderr) => (err ? reject(new Error(`${err.message}\n${stderr}`)) : resolve(stdout.trim()))),
  );
const getJson = async (url) => {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${url} answered ${res.status}`);
  return res.json();
};

/**
 * A request with an explicit Host header. fetch() will not send one, and a name like
 * acme.eventhub.localhost needs no resolving when the connection goes to loopback.
 */
async function send(base, path, { host, headers = {}, method = 'GET', body } = {}) {
  const { request } = await import('node:http');
  const url = new URL(path, base);
  return new Promise((resolve, reject) => {
    const req = request(
      { hostname: url.hostname, port: url.port, path: `${url.pathname}${url.search}`, method, headers: { ...(host ? { host } : {}), ...headers } },
      (res) => {
        let text = '';
        res.setEncoding('utf8');
        res.on('data', (d) => (text += d));
        res.on('end', () => {
          let json = null;
          try {
            json = JSON.parse(text);
          } catch {}
          resolve({ status: res.statusCode, headers: res.headers, text, json });
        });
      },
    );
    req.on('error', reject);
    if (body) req.write(body);
    req.end();
  });
}

/**
 * The Admin API listens on Kong's own loopback and is published nowhere, so it is read
 * from a container that joins Kong's network namespace: the application image, for its
 * curl. Kong's image has none.
 */
async function admin(appImage, path) {
  const out = await docker(['run', '--rm', '--network', `container:${KONG_CONTAINER}`, '--entrypoint', 'curl', appImage, '-sf', `http://127.0.0.1:8001${path}`]);
  return JSON.parse(out);
}

/** Every page of an Admin API listing. */
async function listAll(appImage, path) {
  const out = [];
  let next = `${path}?size=1000`;
  while (next) {
    const page = await admin(appImage, next);
    out.push(...page.data);
    next = page.next ? new URL(page.next, 'http://admin').pathname + new URL(page.next, 'http://admin').search : null;
  }
  return out;
}

/** What a neighbour on the compose network gets from one of Kong's ports: an HTTP status, or 000 and curl's exit code. */
async function fromNeighbour(port) {
  const out = await docker(['exec', NEIGHBOUR, 'sh', '-c', `curl -s -o /dev/null -w '%{http_code}' --max-time 3 http://kong:${port}/; echo " $?"`]);
  const [code, exit] = out.split(' ');
  return { code, exit: Number(exit) };
}

async function main() {
  console.log('  the gateway in the running stack');
  const status = await waitFor(async () => getJson(`${STATUS}/status`), 60_000, 1_000);
  check(Boolean(status?.server), `CONTROL: Kong is up and answers its status endpoint (${status ? 'yes' : 'no answer'})`);
  if (!status) return;

  // Every port the container publishes, and to which address. Only the proxy is for
  // the outside; the status endpoint is read-only and stays on loopback.
  const bindings = JSON.parse(await docker(['inspect', '-f', '{{json .NetworkSettings.Ports}}', KONG_CONTAINER]));
  const published = Object.entries(bindings)
    .filter(([, b]) => Array.isArray(b) && b.length > 0)
    .map(([port, b]) => `${port.replace('/tcp', '')}@${[...new Set(b.map((x) => x.HostIp))].join('+')}`)
    .sort();
  check(
    published.some((p) => p.startsWith('8000@')),
    `CONTROL: the proxy is published (${published.join(', ') || 'nothing'})`,
  );
  check(
    published.every((p) => p.startsWith('8000@') || p === '8100@127.0.0.1'),
    `nothing but the proxy leaves the host's loopback, and the Admin API is published nowhere (${published.join(', ')})`,
  );

  // And on the compose network, where every service is a neighbour: the Admin API has
  // no authentication, and POST /config would replace the tenant snippet. Asked from a
  // real service's container; the proxy answering the same neighbour is the control
  // that the name and the network work at all.
  const viaProxy = await fromNeighbour(8000);
  check(viaProxy.code === '404', `CONTROL: ${NEIGHBOUR} reaches Kong's proxy over the compose network (${viaProxy.code}, curl exit ${viaProxy.exit})`);
  for (const [port, what] of [[8001, 'the Admin API'], [8444, 'the Admin API over TLS'], [8002, 'Kong Manager'], [8445, 'Kong Manager over TLS']]) {
    const res = await fromNeighbour(port);
    check(res.code === '000' && res.exit === 7, `${what} (:${port}) refuses a connection from ${NEIGHBOUR} (${res.code}, curl exit ${res.exit})`);
  }

  const appImage = await docker(['inspect', '-f', '{{.Config.Image}}', NEIGHBOUR]);
  const services = await listAll(appImage, '/services');
  const routes = await listAll(appImage, '/routes');
  const plugins = await listAll(appImage, '/plugins');
  const serviceName = new Map(services.map((s) => [s.id, s.name]));
  check(services.length > 0 && routes.length > 0, `CONTROL: read the running config: ${services.length} services, ${routes.length} routes, ${plugins.length} plugin(s)`);

  const routeNames = routes.map((r) => r.name).sort();
  const expectedNames = Object.keys(EXPECTED).sort();
  check(
    JSON.stringify(routeNames) === JSON.stringify(expectedNames),
    `every route Kong has is one this checks, and the reverse (missing here: ${routeNames.filter((n) => !EXPECTED[n]).join(', ') || 'none'}; missing in Kong: ${expectedNames.filter((n) => !routeNames.includes(n)).join(', ') || 'none'})`,
  );
  const retrying = services.filter((s) => s.retries !== 0).map((s) => `${s.name}=${s.retries}`);
  check(retrying.length === 0, `no service retries a failed request (${retrying.join(', ') || 'all 0'})`);
  const hygiene = plugins.filter((p) => p.name === 'pre-function');
  check(
    hygiene.length === 1 && !hygiene[0].service && !hygiene[0].route && !hygiene[0].consumer && hygiene[0].enabled,
    `the tenant snippet is one global plugin, so no route can miss it (${hygiene.map((p) => (p.service || p.route ? 'scoped' : 'global')).join(', ') || 'none'})`,
  );

  // The real services, through the real gateway.
  const realEvent = await send(PROXY, '/events', { host: TENANT_HOST });
  check(
    realEvent.status === 401 && typeof realEvent.json?.error === 'string' && realEvent.json?.message === undefined,
    `a path reaches the real service: /events answers Event's own 401, not Kong's (${realEvent.status} ${realEvent.text.slice(0, 80)})`,
  );
  const realLogin = await send(PROXY, '/auth/login', {
    host: TENANT_HOST,
    method: 'POST',
    headers: { 'content-type': 'application/json', 'X-Tenant-ID': 'tenant_victim' },
    body: JSON.stringify({ email: 'nobody@example.com', password: 'not-the-password' }),
  });
  check(
    realLogin.status === 400 && /X-Tenant-ID header is required/.test(realLogin.json?.error ?? ''),
    `Auth never sees a forged X-Tenant-ID sent through the gateway: it answers that none arrived (${realLogin.status} ${realLogin.text.slice(0, 80)})`,
  );
  for (const path of ['/health', '/ready', '/metrics']) {
    const res = await send(PROXY, path, { host: TENANT_HOST });
    check(res.status === 404 && /no Route matched/.test(res.json?.message ?? ''), `nothing routes ${path} to a service (${res.status})`);
  }

  console.log('');
  console.log('  every route, against an echo of what the service would receive');
  const seen = [];
  const echo = createServer((req, res) => {
    seen.push({ url: req.url, method: req.method, headers: req.headers });
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ echoed: req.url }));
  });
  await new Promise((r) => echo.listen(0, '0.0.0.0', r));
  const echoPort = echo.address().port;

  let started = false;
  try {
    // The running config, with every upstream pointed at the echo server.
    const scratch = {
      _format_version: '3.0',
      services: services.map((s) => ({
        name: s.name,
        protocol: 'http',
        host: 'host.docker.internal',
        port: echoPort,
        path: `/__upstream/${s.name}`,
        retries: s.retries,
        connect_timeout: s.connect_timeout,
        read_timeout: s.read_timeout,
        write_timeout: s.write_timeout,
      })),
      routes: routes.map((r) => ({
        name: r.name,
        service: serviceName.get(r.service?.id),
        hosts: r.hosts,
        paths: r.paths,
        methods: r.methods,
        protocols: r.protocols,
        strip_path: r.strip_path,
        preserve_host: r.preserve_host,
        path_handling: r.path_handling,
        regex_priority: r.regex_priority,
        tags: r.tags,
      })),
      plugins: plugins.map((p) => ({
        name: p.name,
        config: p.config,
        enabled: p.enabled,
        protocols: p.protocols,
        ...(p.service ? { service: serviceName.get(p.service.id) } : {}),
        ...(p.route ? { route: routes.find((r) => r.id === p.route.id)?.name } : {}),
      })),
    };
    const image = await docker(['inspect', '-f', '{{.Config.Image}}', KONG_CONTAINER]);
    // ONE worker. A config posted to /config reaches each nginx worker on its own
    // time, so the first request after a 201 can land on a worker still routing the
    // empty config: measured, the first probe of the first run answered 404 while
    // every later one passed. With one worker, a warm-up request that reaches the
    // echo proves the whole router is live.
    await docker([
      'run', '-d', '--rm', '--name', SCRATCH,
      '-e', 'KONG_DATABASE=off', '-e', 'KONG_PROXY_LISTEN=0.0.0.0:8000', '-e', 'KONG_ADMIN_LISTEN=0.0.0.0:8001',
      '-e', 'KONG_NGINX_WORKER_PROCESSES=1',
      '-p', '127.0.0.1::8000', '-p', '127.0.0.1::8001',
      '--add-host', 'host.docker.internal:host-gateway',
      image,
    ]);
    started = true;
    const portOf = async (p) => (await docker(['port', SCRATCH, `${p}/tcp`])).split('\n')[0].split(':').pop();
    const scratchProxy = `http://127.0.0.1:${await portOf(8000)}`;
    const scratchAdmin = `http://127.0.0.1:${await portOf(8001)}`;
    const up = await waitFor(async () => (await fetch(`${scratchAdmin}/status`)).ok, 60_000, 500);
    const loaded = up ? await fetch(`${scratchAdmin}/config`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(scratch) }) : null;
    check(loaded?.status === 201, `CONTROL: a scratch Kong (${image}) loaded the running config with an echo behind every service (${loaded?.status ?? 'never up'})`);
    if (loaded?.status !== 201) {
      console.error(`  ${await loaded?.text().catch(() => '')}`);
      return;
    }

    /** One request through the scratch Kong; what the echo saw for it, if anything. */
    async function probe(path, host, headers = FORGED) {
      const before = seen.length;
      const res = await send(scratchProxy, path, { host, headers });
      await sleep(20);
      return { res, upstream: seen.length > before ? seen[seen.length - 1] : null };
    }
    const live = await waitFor(async () => (await probe('/events', TENANT_HOST, {})).upstream !== null, 30_000, 200);
    check(Boolean(live), 'CONTROL: the scratch Kong routes the loaded config (a warm-up request reached the echo)');
    if (!live) return;
    const forgedLeft = (h) => Object.keys(FORGED).filter((name) => name.toLowerCase() in h && !(name === 'X-Tenant-Slug' && h['x-tenant-slug'] === 'acme'));

    for (const [route, spec] of Object.entries(EXPECTED)) {
      for (const [sent, received] of spec.probes) {
        const host = spec.host === 'api' ? API_HOST : TENANT_HOST;
        const { res, upstream } = await probe(sent, host);
        const wantUrl = `/__upstream/${spec.service}${received}`;
        const left = upstream ? forgedLeft(upstream.headers) : [];
        const slug = upstream?.headers['x-tenant-slug'];
        const slugOk = spec.host === 'api' ? slug === undefined : slug === 'acme';
        check(
          res.status === 200 && upstream?.url === wantUrl && left.length === 0 && slugOk,
          `${route}: ${host}${sent} reaches ${spec.service} as ${received}, ${spec.host === 'api' ? 'with no tenant' : 'as tenant acme'}, every forged header gone ` +
            `(${upstream ? `${upstream.url}; slug ${slug ?? 'none'}; forged left: ${left.join(', ') || 'none'}` : `${res.status}, never reached a service`})`,
        );
      }
    }

    console.log('');
    console.log('  the tenant is the subdomain, and nothing else is one');
    const loud = await probe('/events', 'ACME.eventhub.localhost:8000');
    check(loud.upstream?.headers['x-tenant-slug'] === 'acme', `an upper-case Host with a port is still tenant acme (${loud.upstream?.headers['x-tenant-slug'] ?? loud.res.status})`);
    const prod = await probe('/events', 'acme.eventhub.com');
    check(prod.upstream?.headers['x-tenant-slug'] === 'acme', `so is acme.eventhub.com (${prod.upstream?.headers['x-tenant-slug'] ?? prod.res.status})`);
    const refused = [
      ['www.eventhub.localhost', 'a reserved name'],
      ['api.eventhub.localhost', 'the api host, on a tenant route'],
      ['a.b.eventhub.localhost', 'two labels'],
      ['ac_me.eventhub.localhost', 'a label with an underscore'],
      [`${'a'.repeat(64)}.eventhub.localhost`, 'a label longer than 63'],
      ['eventhub.localhost', 'no subdomain at all'],
      ['acme.evil.example', 'a host that is not ours'],
    ];
    for (const [host, what] of refused) {
      const { res, upstream } = await probe('/events', host);
      check(upstream === null && (res.status === 400 || res.status === 404), `${what} (${host.slice(0, 40)}) never reaches a service (${res.status})`);
    }
  } finally {
    if (started) await docker(['rm', '-f', SCRATCH]).catch(() => {});
    echo.close();
  }

  const left = await docker(['ps', '-aq', '--filter', `name=${SCRATCH}`]).catch(() => '');
  check(left === '', `cleanup: the scratch Kong is gone (${left === '' ? 'removed' : left})`);

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('every route reaches its own service, and no client can say which tenant it is');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
