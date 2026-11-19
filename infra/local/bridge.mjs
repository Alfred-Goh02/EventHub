/**
 * The bridge from the kind cluster to the data stores compose runs: infra/local/up.mjs
 * makes it, and both it and scripts/verify-kind-bridge.mjs check it, through this file.
 *
 * EACH HOST IS AN ExternalName SERVICE NAMING THE CONTAINER, NEVER AN ADDRESS. The first
 * version gave each host a selector-less Service and an EndpointSlice holding the
 * container's IP on kind's network, as read when up.mjs ran. Docker hands those addresses
 * out again whenever its VM restarts. Measured 2026-11-14, after a Docker Desktop restart:
 * every store was back at a different address (postgres .6 -> .11, kafka .5 -> .4, and
 * five more), kafka's old address now belonged to a kind worker, and the slices still held
 * the old ones. Auth, Event, Live and Community crash-looped, Analytics sat at 0/1, all
 * fifteen ArgoCD apps showed Synced and Progressing, and nothing said why.
 *
 * A name is looked up each time it is used. A pod asks CoreDNS for `postgres`, gets a CNAME
 * to `eventhub-postgres`, and CoreDNS forwards that name to the node's resolver. On Docker
 * Desktop that is 192.168.65.254, which the node rewrites to Docker's own DNS (127.0.0.11).
 * Docker's DNS knows every container on the kind network by name, at the address it has
 * now. Measured: with postgres moved from .11 to .12 and another container on .11, a pod
 * reaching `postgres` through the old slice got ECONNREFUSED, and through the ExternalName
 * it got Postgres at .12. Then a real Docker Desktop restart: every one of the ten
 * addresses on the network changed, several to another store's old one, and with nothing
 * re-run all fifteen deployments came back ready and every ArgoCD app Synced and Healthy.
 * For about the first minute some lookups failed (EAI_AGAIN, ENOTFOUND: a store not yet
 * registered with Docker's DNS, and CoreDNS keeps a "no such name" for up to 30 s); they
 * settle by themselves. The nodes stay on the kind network alone (up.mjs, step 2).
 *
 * Fixed addresses (`docker network connect --ip`) were the other way to keep a slice true,
 * and they do survive a restart: measured, a container on 172.20.255.10 came back on it
 * while every other address moved. But they tie the bridge to a subnet that kind chooses
 * when it makes the network, and they still need a check for the case neither design
 * survives: compose recreating a store drops the extra network, and then the store has no
 * address on kind at all. So inspectBridges() asks the cluster, from a pod, where each
 * host actually leads.
 */

import { execFileSync, spawnSync } from 'node:child_process';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';

// Which compose hosts an environment names: shared with scripts/verify-store-chart.mjs,
// which requires infra/stores to serve on EKS every host bridged here on kind.
import { hostsOf } from '../lib/compose-hosts.mjs';

export const NETWORK = 'kind';
export const LABELS = { 'app.kubernetes.io/part-of': 'eventhub', 'eventhub/bridge': 'compose' };
export const SELECTOR = 'eventhub/bridge=compose';

/** Every chart under charts/ that is not a library. */
export function allCharts(root) {
  return readdirSync(join(root, 'charts'), { withFileTypes: true })
    .filter((d) => d.isDirectory() && existsSync(join(root, 'charts', d.name, 'Chart.yaml')))
    .map((d) => d.name)
    .filter((name) => !/^type:\s*library\s*$/m.test(readFileSync(join(root, 'charts', name, 'Chart.yaml'), 'utf8')))
    .sort();
}

/** Compose's own resolved configuration, for the project compose runs. */
export function composeConfig(root) {
  return JSON.parse(execFileSync('docker', ['compose', '-p', 'eventhub-rebuild', 'config', '--format', 'json'], { cwd: root, encoding: 'utf8' }));
}

/**
 * host -> { container, ports } for every compose host the charts' environments name. A host
 * that is itself one of the charts gets none: inside the cluster, `auth` means the auth
 * Service.
 */
export function bridgesFor(charts, services) {
  const composeNames = new Set(Object.keys(services));
  const found = new Map();
  for (const chart of charts) {
    if (!services[chart]) throw new Error(`charts/${chart} has no compose service of the same name to take its environment from`);
    for (const { host, port } of hostsOf(services[chart].environment ?? {}, composeNames)) {
      if (charts.includes(host)) continue;
      if (!found.has(host)) found.set(host, new Set());
      found.get(host).add(port);
    }
  }
  return new Map(
    [...found]
      .sort(([a], [b]) => a.localeCompare(b))
      .map(([host, ports]) => [host, { container: services[host].container_name ?? host, ports: [...ports].sort((a, b) => a - b) }]),
  );
}

/** The container's IPv4 address on kind's network, or null when it has none there. */
export function addressOnKind(container) {
  const r = spawnSync('docker', ['inspect', container, '--format', '{{json .NetworkSettings.Networks}}'], { encoding: 'utf8' });
  if (r.status !== 0) return null;
  return JSON.parse(r.stdout)[NETWORK]?.IPAddress || null;
}

/** An ExternalName Service per host, naming its container. The ports are for people reading it; DNS ignores them. */
export function serviceManifests(bridges) {
  return [...bridges].map(([host, { container, ports }]) => ({
    apiVersion: 'v1',
    kind: 'Service',
    metadata: { name: host, labels: LABELS },
    spec: { type: 'ExternalName', externalName: container, ports: ports.map((p) => ({ name: `p${p}`, port: p, targetPort: p, protocol: 'TCP' })) },
  }));
}

/**
 * Runs in a pod: what each host resolves to, the address a plain lookup puts first, and
 * whether each port answers. Its input arrives base64 encoded, so no quoting on the way
 * through kubectl can change it; its answer is one line that a missing or cut-off answer
 * cannot be mistaken for.
 */
const PROBE = `
const dns = require('dns').promises, net = require('net');
const input = JSON.parse(Buffer.from(process.argv[1], 'base64').toString());
const look = (name) => dns.lookup(name, { all: true }).then((a) => a.map((x) => x.address), (e) => ({ error: e.code || e.message }));
const tcp = (host, port) => new Promise((done) => {
  const s = net.connect({ host, port, timeout: 5000 });
  s.on('connect', () => { done({ port, ok: true, remote: s.remoteAddress }); s.destroy(); });
  s.on('timeout', () => { done({ port, ok: false, error: 'timeout' }); s.destroy(); });
  s.on('error', (e) => done({ port, ok: false, error: e.code || e.message }));
});
(async () => {
  const hosts = [];
  for (const h of input.hosts) {
    const first = await dns.lookup(h.host).then((a) => a.address, () => null);
    hosts.push({ host: h.host, resolved: await look(h.host), first, ports: await Promise.all(h.ports.map((p) => tcp(h.host, p))) });
  }
  const absent = await look(input.absent);
  console.log('BRIDGE-PROBE ' + JSON.stringify({ hosts, absent }));
  setTimeout(() => process.exit(0), 300);
})();
`;

function probeFromCluster(context, image, bridges, absent) {
  const input = Buffer.from(JSON.stringify({ hosts: [...bridges].map(([host, b]) => ({ host, ports: b.ports })), absent })).toString('base64');
  const name = `bridge-probe-${Date.now().toString(36)}`;
  const r = spawnSync(
    'kubectl',
    ['--context', context, 'run', name, '--rm', '-i', '--restart=Never', `--image=${image}`, '--image-pull-policy=Never', '--quiet', '--', 'node', '-e', PROBE, input],
    { encoding: 'utf8', timeout: 180_000 },
  );
  const line = (r.stdout ?? '').split(/\r?\n/).find((l) => l.startsWith('BRIDGE-PROBE '));
  // `--rm` removes the pod when kubectl lives to see it end; the timeout above kills kubectl
  // first, so the pod is removed here too.
  if (!line) spawnSync('kubectl', ['--context', context, 'delete', 'pod', name, '--ignore-not-found', '--wait=false'], { encoding: 'utf8' });
  if (!line) return { error: `the probe pod gave no answer (exit ${r.status}): ${((r.stderr ?? '') + (r.stdout ?? '')).trim() || r.error?.message || 'nothing printed'}` };
  return JSON.parse(line.slice('BRIDGE-PROBE '.length));
}

const isV4 = (a) => /^\d+\.\d+\.\d+\.\d+$/.test(a);

/**
 * Where each bridge leads now, judged: [{ ok, msg }] in the order they should be read.
 * Docker says where each container is; the cluster is asked where its name leads, from a
 * pod, as the services ask it. Anything that does not lead to the container named is a
 * failure that names its host.
 */
export function inspectBridges({ context, image, bridges }) {
  const out = [];
  const say = (ok, msg) => out.push({ ok, msg });
  const kube = (args) => JSON.parse(execFileSync('kubectl', ['--context', context, ...args, '-o', 'json'], { encoding: 'utf8' })).items;
  const services = new Map(kube(['get', 'services']).map((s) => [s.metadata.name, s]));
  const slices = kube(['get', 'endpointslices', '-l', SELECTOR]);

  const absent = `eventhub-bridge-absent-${Date.now().toString(36)}`;
  const probe = probeFromCluster(context, image, bridges, absent);
  say(!probe.error, probe.error ? `CONTROL: ${probe.error}` : `CONTROL: a pod answered for ${probe.hosts.length} host(s)`);
  // The lookups below mean something only if a lookup can fail. A resolver that answered
  // every name -- as some do for names they do not know -- would answer for a store that is
  // gone too. ENOTFOUND only: a timeout says the resolver is broken, not that it said no.
  if (!probe.error) {
    const said = Array.isArray(probe.absent) ? probe.absent.join(', ') || 'nothing' : probe.absent?.error;
    say(said === 'ENOTFOUND', `CONTROL: from a pod, a container name that does not exist does not resolve (${absent}: ${said})`);
  }

  for (const [host, { container, ports }] of bridges) {
    const ip = addressOnKind(container);
    say(Boolean(ip), ip ? `${host}: ${container} is on the ${NETWORK} network at ${ip}` : `${host}: ${container} has no address on the ${NETWORK} network -- compose recreated it, or it is not running; node infra/local/up.mjs joins it again`);

    const svc = services.get(host);
    const isName = svc?.spec?.type === 'ExternalName' && svc.spec.externalName === container;
    say(isName, `${host}: the Service is an ExternalName for ${container}${isName ? '' : ` (it is ${svc ? `${svc.spec.type}${svc.spec.externalName ? ` for ${svc.spec.externalName}` : ''}` : 'missing'})`}`);

    for (const s of slices.filter((x) => x.metadata.labels?.['kubernetes.io/service-name'] === host)) {
      const held = (s.endpoints ?? []).flatMap((e) => e.addresses ?? []);
      const stale = ip && !held.includes(ip);
      say(false, `${host}: ${s.metadata.name} still holds ${held.join(', ') || 'nothing'}${stale ? `, and ${container} is at ${ip}: STALE` : ''} (up.mjs removes it)`);
    }

    if (probe.error) continue;
    const seen = probe.hosts.find((h) => h.host === host);
    // Its one IPv4 address must be the container's, and come first: a client that takes the
    // first answer and tries nothing else connects there. Docker answers AAAA as well, and
    // the pods have no IPv6; Node's lookup puts the IPv4 first (measured), and this says so
    // if that ever changes.
    const got = Array.isArray(seen?.resolved) ? seen.resolved : [];
    const v4 = got.filter(isV4);
    const resolvesHere = Boolean(ip) && v4.length === 1 && v4[0] === ip && seen.first === ip;
    const said = got.length ? `${got.join(', ')}; first ${seen.first}` : seen?.resolved?.error ?? 'no answer';
    say(resolvesHere, `${host}: from a pod, it resolves to ${ip ?? '(no address)'}, first (got ${said})`);
    for (const port of ports) {
      const p = seen?.ports?.find((x) => x.port === port);
      const ok = Boolean(p?.ok) && Boolean(ip) && p.remote === ip;
      say(ok, `${host}: from a pod, ${host}:${port} answers at ${ip ?? '(no address)'} (${p?.ok ? `connected to ${p.remote}` : p?.error ?? 'no answer'})`);
    }
  }
  return out;
}
