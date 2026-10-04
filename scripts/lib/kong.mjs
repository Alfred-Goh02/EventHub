import { execFile } from 'node:child_process';
import { createServer, request } from 'node:http';

/**
 * What the gateway verifiers share: talking to the running Kong, reading the
 * configuration it loaded, and standing up throwaway Kong nodes with that configuration
 * and an echo server behind every service.
 *
 * The configuration is always read from the RUNNING node, never from infra/kong/kong.yml:
 * a check against the file would pass for a file Kong never loaded.
 */

export const KONG_CONTAINER = 'eventhub-kong';

export const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

export async function waitFor(fn, timeoutMs = 30_000, stepMs = 250) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await fn().catch(() => null);
    if (value) return value;
    await sleep(stepMs);
  }
  return null;
}

export const docker = (args) =>
  new Promise((resolve, reject) =>
    execFile('docker', args, { maxBuffer: 8 * 1024 * 1024 }, (err, stdout, stderr) => (err ? reject(new Error(`${err.message}\n${stderr}`)) : resolve(stdout.trim()))),
  );

/**
 * A container's log, BOTH streams. `docker logs` writes the container's stderr to its own
 * stderr, and Kong's error log goes there; a search of stdout alone would call a leak in
 * the error log clean.
 */
export const containerLogs = (container, since) =>
  new Promise((resolve, reject) =>
    execFile('docker', ['logs', '--since', since, container], { maxBuffer: 64 * 1024 * 1024 }, (err, stdout, stderr) =>
      err ? reject(new Error(`${err.message}\n${stderr}`)) : resolve(`${stdout}\n${stderr}`),
    ),
  );

/**
 * A request with an explicit Host header. fetch() will not send one, and a name like
 * acme.eventhub.localhost needs no resolving when the connection goes to loopback.
 */
export function send(base, path, { host, headers = {}, method = 'GET', body } = {}) {
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
export async function admin(appImage, path, container = KONG_CONTAINER) {
  const out = await docker(['run', '--rm', '--network', `container:${container}`, '--entrypoint', 'curl', appImage, '-sf', `http://127.0.0.1:8001${path}`]);
  return JSON.parse(out);
}

/** Every page of an Admin API listing. */
export async function listAll(appImage, path) {
  const out = [];
  let next = `${path}?size=1000`;
  while (next) {
    const page = await admin(appImage, next);
    out.push(...page.data);
    next = page.next ? new URL(page.next, 'http://admin').pathname + new URL(page.next, 'http://admin').search : null;
  }
  return out;
}

/** The services, routes and plugins the running Kong has loaded. */
export async function runningConfig(appImage) {
  const [services, routes, plugins] = await Promise.all([listAll(appImage, '/services'), listAll(appImage, '/routes'), listAll(appImage, '/plugins')]);
  return { services, routes, plugins };
}

/**
 * The running configuration as a declarative document, with every service's upstream
 * replaced by the echo server: `/__upstream/<service>` in front of the path, so a
 * request shows which service a route reached and what it would have received.
 * `editPlugin` may return a changed copy of a plugin, or null to leave it out.
 */
export function echoConfig({ services, routes, plugins }, echoPort, editPlugin = (p) => p) {
  const serviceName = new Map(services.map((s) => [s.id, s.name]));
  return {
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
    plugins: plugins
      .map((p) => ({
        name: p.name,
        config: p.config,
        enabled: p.enabled,
        protocols: p.protocols,
        ...(p.service ? { service: serviceName.get(p.service.id) } : {}),
        ...(p.route ? { route: routes.find((r) => r.id === p.route.id)?.name } : {}),
      }))
      .map(editPlugin)
      .filter(Boolean),
  };
}

/** An HTTP server that records every request and answers 200 with what it saw. */
export async function startEcho() {
  const seen = [];
  const server = createServer((req, res) => {
    seen.push({ url: req.url, method: req.method, headers: req.headers });
    res.setHeader('content-type', 'application/json');
    res.end(JSON.stringify({ echoed: req.url }));
  });
  await new Promise((r) => server.listen(0, '0.0.0.0', r));
  return { port: server.address().port, seen, close: () => new Promise((r) => server.close(r)) };
}

/**
 * A throwaway Kong node, DB-less and empty until a config is posted, with its proxy and
 * Admin API on random loopback ports and the host reachable as host.docker.internal.
 * `network` puts it on a compose network as well, to reach Redis by name.
 *
 * ONE worker. A config posted to /config reaches each nginx worker on its own time, so
 * the first request after a 201 can land on a worker still routing the empty config:
 * measured, the first probe of the first run answered 404 while every later one passed.
 * With one worker, a warm-up request that reaches the echo proves the router is live.
 */
export async function startScratchKong({ name, image, network }) {
  await docker([
    'run', '-d', '--rm', '--name', name,
    '-e', 'KONG_DATABASE=off', '-e', 'KONG_PROXY_LISTEN=0.0.0.0:8000', '-e', 'KONG_ADMIN_LISTEN=0.0.0.0:8001',
    '-e', 'KONG_NGINX_WORKER_PROCESSES=1',
    '-p', '127.0.0.1::8000', '-p', '127.0.0.1::8001',
    '--add-host', 'host.docker.internal:host-gateway',
    ...(network ? ['--network', network] : []),
    image,
  ]);
  const portOf = async (p) => (await docker(['port', name, `${p}/tcp`])).split('\n')[0].split(':').pop();
  const node = {
    name,
    proxy: `http://127.0.0.1:${await portOf(8000)}`,
    admin: `http://127.0.0.1:${await portOf(8001)}`,
    stop: () => docker(['rm', '-f', name]).catch(() => {}),
  };
  node.up = await waitFor(async () => (await fetch(`${node.admin}/status`)).ok, 60_000, 500);
  return node;
}

/** Replaces a node's whole configuration; the status, and the body when refused. */
export async function loadConfig(node, config) {
  const res = await fetch(`${node.admin}/config`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(config) });
  return { status: res.status, text: res.status === 201 ? '' : await res.text().catch(() => '') };
}
