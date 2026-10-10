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
 *     Kong's), and nothing routes /health or /metrics. Signing in at a subdomain is a
 *     token for that subdomain's tenant, also when the request forges both tenant
 *     headers for another one: the seed holds the same account in two tenants, so a
 *     forgery that got through would sign in, not fail.
 *   - The Admin API is unreachable from the host AND from every other container: it
 *     has no authentication, and POST /config replaces the whole configuration. It is
 *     read here from inside Kong's own network namespace.
 *
 * Run with the compose stack up (docker compose up -d --wait):
 *   node scripts/verify-compose-gateway.mjs
 */

import { randomBytes } from 'node:crypto';
import { KONG_CONTAINER, admin, docker, echoConfig, listAll, loadConfig, send, sleep, startEcho, startScratchKong, waitFor } from './lib/kong.mjs';

const PROXY = process.env['KONG_PROXY_URL'] ?? 'http://127.0.0.1:8000';
const STATUS = process.env['KONG_STATUS_URL'] ?? 'http://127.0.0.1:8100';
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
  ticketing: { service: 'ticketing', host: 'tenant', probes: [['/reservations', '/reservations'], ['/ticket-types?eventId=evt_1', '/ticket-types?eventId=evt_1']] },
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

const getJson = async (url) => {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`${url} answered ${res.status}`);
  return res.json();
};

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

  // And nothing ELSE in the stack is published beyond this machine. Every container
  // compose runs in Kong's project, read from the containers rather than the file: a
  // service published on every interface lets the LAN past the gateway, where a client
  // names its own tenant slug, and the databases answer with the passwords in the
  // compose file.
  const project = await docker(['inspect', '-f', '{{index .Config.Labels "com.docker.compose.project"}}', KONG_CONTAINER]);
  const ids = (await docker(['ps', '-q', '--filter', `label=com.docker.compose.project=${project}`])).split('\n').filter(Boolean);
  const exposed = [];
  let bindings2 = 0;
  for (const id of ids) {
    const [name, portsJson] = (await docker(['inspect', '-f', '{{.Name}}|{{json .NetworkSettings.Ports}}', id])).split('|');
    for (const [port, binds] of Object.entries(JSON.parse(portsJson) ?? {})) {
      for (const b of binds ?? []) {
        bindings2 += 1;
        const isProxy = name === `/${KONG_CONTAINER}` && port === '8000/tcp';
        if (b.HostIp !== '127.0.0.1' && !isProxy) exposed.push(`${name.slice(1)} ${port}@${b.HostIp}`);
      }
    }
  }
  check(ids.length >= 10 && bindings2 >= 10, `CONTROL: read the published ports of ${ids.length} running containers in project ${project} (${bindings2} bindings)`);
  check(exposed.length === 0, `every other published port in the stack is on 127.0.0.1 (${exposed.join(', ') || 'none beyond it'})`);

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

  // A recreated service can come back on another service's old address. Kong caches a
  // name for as long as Docker's DNS says, and kept proxying /events to what had
  // become Live, 502 for over 100 s until Kong was restarted. A short override makes
  // it follow within seconds. Read from the running node, not the compose file.
  const node = await admin(appImage, '/');
  const ttl = node?.configuration?.dns_valid_ttl;
  check(typeof ttl === 'number' && ttl > 0 && ttl <= 10, `Kong re-resolves service names within 10 s, whatever TTL Docker gives them (dns_valid_ttl ${ttl ?? 'unset'})`);
  const services = await listAll(appImage, '/services');
  const routes = await listAll(appImage, '/routes');
  const plugins = await listAll(appImage, '/plugins');
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
  // Auth's /me, because Auth's routes carry no signature check at the edge: with no
  // token the answer is Auth's own 401, in its shape, not Kong's.
  const realAuth = await send(PROXY, '/auth/me', { host: TENANT_HOST });
  check(
    realAuth.status === 401 && typeof realAuth.json?.error === 'string' && realAuth.json?.message === undefined,
    `a path reaches the real service: /auth/me answers Auth's own 401, not Kong's (${realAuth.status} ${realAuth.text.slice(0, 80)})`,
  );
  // Signing in through the gateway: the tenant is the subdomain, whatever the request
  // says. The seed puts the same address and password in both acme and globex, so a
  // forged header that worked would sign in somewhere real, not fail.
  const signIn = async (host, headers = {}) => {
    const res = await send(PROXY, '/auth/login', {
      host,
      method: 'POST',
      headers: { 'content-type': 'application/json', ...headers },
      body: JSON.stringify({ email: 'alice@acme.com', password: 'hunter2222' }),
    });
    const token = res.json?.accessToken;
    const claims = token ? JSON.parse(Buffer.from(token.split('.')[1], 'base64url').toString('utf8')) : null;
    return { status: res.status, tenant: claims?.tenantId ?? null, token: token ?? null };
  };
  const asAcme = await signIn(TENANT_HOST);
  check(asAcme.status === 200 && asAcme.tenant === 'tenant_acme', `CONTROL: signing in at ${TENANT_HOST} is a token for tenant_acme (${asAcme.status}, ${asAcme.tenant})`);
  const asGlobex = await signIn('globex.eventhub.localhost');
  check(asGlobex.status === 200 && asGlobex.tenant === 'tenant_globex', `the same account at globex.eventhub.localhost is a token for tenant_globex (${asGlobex.status}, ${asGlobex.tenant})`);
  const forged = await signIn(TENANT_HOST, { 'X-Tenant-ID': 'tenant_globex', 'X-Tenant-Slug': 'globex' });
  check(
    forged.status === 200 && forged.tenant === 'tenant_acme',
    `forged X-Tenant-ID and X-Tenant-Slug naming globex, at ${TENANT_HOST}, still sign in to tenant_acme (${forged.status}, ${forged.tenant})`,
  );
  const nowhere = await signIn('no-such-tenant.eventhub.localhost');
  check(nowhere.status === 401 && nowhere.tenant === null, `a subdomain with no tenant behind it signs in nowhere (${nowhere.status})`);

  // And a token is honoured only on its own tenant's subdomain: the real Event, through
  // the real gateway, with the token the gateway's sign-in just issued.
  const bearer = { authorization: `Bearer ${asAcme.token}` };
  const onOwn = await send(PROXY, '/events', { host: TENANT_HOST, headers: bearer });
  check(onOwn.status === 200, `CONTROL: acme's token lists events on ${TENANT_HOST} (${onOwn.status})`);
  const onOther = await send(PROXY, '/events', { host: 'globex.eventhub.localhost', headers: bearer });
  check(onOther.status === 403, `acme's token on globex.eventhub.localhost is refused with 403 (${onOther.status} ${onOther.text.slice(0, 80)})`);

  // Behind the gateway, Auth must still tell one client from another. Its login budget
  // that refuses a guesser is keyed on (tenant, email, ADDRESS) precisely so that
  // spending it locks out only the address that spent it. If Auth saw every request as
  // coming from Kong, one client would lock the account out for everyone. So: spend an
  // account's budget from this host, through the gateway, then ask for the same account
  // from another container, through the gateway. An invented account, per run, so no
  // real one is touched and no earlier run is counted.
  const target = JSON.stringify({ email: `lockout-${RUN}@acme.example`, password: 'not-the-password' });
  let spentAfter = 0;
  for (let i = 1; i <= 30; i += 1) {
    const res = await send(PROXY, '/auth/login', { host: TENANT_HOST, method: 'POST', headers: { 'content-type': 'application/json' }, body: target });
    if (res.status === 429) {
      spentAfter = i;
      break;
    }
  }
  check(spentAfter > 1, `CONTROL: this host, through the gateway, runs out of one account's budget (429 on attempt ${spentAfter || 'never, in 30'})`);
  const elsewhere = await docker([
    'exec', NEIGHBOUR, 'curl', '-s', '-o', '/dev/null', '-w', '%{http_code}', '--max-time', '5',
    '-X', 'POST', 'http://kong:8000/auth/login', '-H', `Host: ${TENANT_HOST}`, '-H', 'Content-Type: application/json', '-d', target,
  ]);
  check(elsewhere === '401', `the same account from another address, through the gateway, is still answered (${elsewhere}, want 401, not 429)`);
  for (const path of ['/health', '/ready', '/metrics']) {
    const res = await send(PROXY, path, { host: TENANT_HOST });
    check(res.status === 404 && /no Route matched/.test(res.json?.message ?? ''), `nothing routes ${path} to a service (${res.status})`);
  }

  console.log('');
  console.log('  every route, against an echo of what the service would receive');
  const echo = await startEcho();
  const { seen } = echo;

  let scratchNode = null;
  try {
    // The running config, with every upstream pointed at the echo server. Without its
    // rate limits and edge signature checks: this node is not on the compose network,
    // the limits are counted in Redis, and the signature checks need the consumer the
    // running node renders at start. verify-compose-gateway-edge.mjs asserts both.
    const scratch = echoConfig({ services, routes, plugins }, echo.port, (p) => (p.name === 'rate-limiting' || p.name === 'jwt' ? null : p));
    const image = await docker(['inspect', '-f', '{{.Config.Image}}', KONG_CONTAINER]);
    scratchNode = await startScratchKong({ name: SCRATCH, image });
    const loaded = scratchNode.up ? await loadConfig(scratchNode, scratch) : null;
    check(loaded?.status === 201, `CONTROL: a scratch Kong (${image}) loaded the running config with an echo behind every service (${loaded?.status ?? 'never up'})`);
    if (loaded?.status !== 201) {
      console.error(`  ${loaded?.text ?? ''}`);
      return;
    }

    /** One request through the scratch Kong; what the echo saw for it, if anything. */
    async function probe(path, host, headers = FORGED) {
      const before = seen.length;
      const res = await send(scratchNode.proxy, path, { host, headers });
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
    // By name, whether or not the start returned: a start that failed after `docker
    // run` would otherwise leave its container behind.
    await docker(['rm', '-f', SCRATCH]).catch(() => {});
    await echo.close();
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
