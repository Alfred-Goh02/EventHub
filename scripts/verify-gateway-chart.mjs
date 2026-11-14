/**
 * Asserts that infra/kong runs on EKS the gateway compose runs, as compose runs it, in front
 * of Services that exist -- and exposes nothing compose keeps to itself.
 *
 * COMPOSE IS THE ONE DESCRIPTION. The chart is the directory compose mounts into its Kong
 * container, and is checked against compose's own resolved configuration:
 *   - compose pins Kong's image by digest, and the chart's image is exactly that reference;
 *     the container's args are compose's command; the environment is the Secret kong-env,
 *     which infra/eks/up.mjs makes from compose (verify-eks-install.mjs checks it does), and
 *     nothing is written into the chart beside it; every port compose publishes is declared;
 *     the grace period is compose's stop_grace_period;
 *   - compose's bind mount is this chart's own directory, and the pod mounts, read-only at
 *     the same path, a ConfigMap holding exactly kong.yml and render-consumer.lua as they are
 *     in it; compose's key volume is the public half of jwt-keys at the same path, under the
 *     name AUTH_PUBLIC_KEY_FILE gives it;
 *   - every upstream kong.yml routes to is a Service a chart under charts/ makes, on that
 *     port, and the Redis its rate limit counts in is a Service infra/stores makes;
 *   - the pod has no service links (Kong reads KONG_* as configuration), no API token, the
 *     image's own non-root uid, no privilege escalation and no capabilities; startup,
 *     liveness and readiness probes on the status port; a preStop that waits and then quits
 *     inside the grace period; requests and a memory limit;
 *   - the Service is ClusterIP and serves the proxy port only: not the status port, which
 *     compose publishes to the host's loopback, nor the Admin API, which listens on the
 *     container's own loopback.
 *
 * Needs helm and docker on PATH (CI runs it after the step that parses kong.yml with Kong's
 * image, so the image is there to ask which user it runs as).
 *   node scripts/verify-gateway-chart.mjs          against this tree
 *   node scripts/verify-gateway-chart.mjs [root]   against another (a red run in a copy)
 */

import { spawnSync } from 'node:child_process';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { dirname, join, posix, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}
const note = (msg) => console.log(`    note  ${msg}`);
const section = (title) => {
  console.log('');
  console.log(`  ${title}`);
};

const sh = (cmd, args) => {
  const r = spawnSync(cmd, args, { encoding: 'utf8', maxBuffer: 64 << 20 });
  return r.error ? { status: -1, stdout: '', stderr: String(r.error.message) } : { status: r.status, stdout: r.stdout ?? '', stderr: r.stderr ?? '' };
};

/** A chart rendered as JSON objects (render=json), or the error text. */
function render(chartDir, args = []) {
  const r = sh('helm', ['template', 'v', chartDir, '--set', 'render=json', ...args]);
  if (r.status !== 0) return { error: (r.stderr || r.stdout).trim() };
  return {
    docs: r.stdout
      .split(/^---\s*$/m)
      .map((d) => d.split(/\r?\n/).filter((l) => !l.startsWith('# Source:')).join('\n').trim())
      .filter(Boolean)
      .map((d) => JSON.parse(d)),
  };
}

/** compose's "15s", "1m30s", or a number of seconds. */
function seconds(value) {
  if (value === undefined || value === null) return undefined;
  if (typeof value === 'number') return value;
  const m = /^(?:(\d+)m)?(?:(\d+)s)?$/.exec(String(value));
  return m ? Number(m[1] ?? 0) * 60 + Number(m[2] ?? 0) : Number.NaN;
}

const lf = (text) => text.replace(/\r\n/g, '\n');

function main() {
  const rootArg = process.argv.slice(2).find((a) => a !== '--');
  const root = rootArg ? resolve(rootArg) : ROOT;
  const chartDir = join(root, 'infra', 'kong');
  console.log(`  reading ${rootArg ? root : 'this tree'}: infra/kong against docker-compose.yml`);

  const composeRun = sh('docker', ['compose', '-f', join(root, 'docker-compose.yml'), '-p', 'eventhub-rebuild', 'config', '--format', 'json']);
  if (composeRun.status !== 0) {
    check(false, `docker compose config: ${composeRun.stderr.trim().slice(-400)}`);
    return;
  }
  const compose = JSON.parse(composeRun.stdout);
  const kong = compose.services.kong;
  if (!kong) {
    check(false, 'compose has no service named kong');
    return;
  }

  const chart = render(chartDir);
  if (chart.error) {
    check(false, `helm template infra/kong did not render: ${chart.error.slice(-600)}`);
    return;
  }

  section('the chart renders');
  const deploy = chart.docs.find((d) => d.kind === 'Deployment');
  const cm = chart.docs.find((d) => d.kind === 'ConfigMap');
  const svc = chart.docs.find((d) => d.kind === 'Service');
  check(deploy !== undefined && cm !== undefined && svc !== undefined, `CONTROL: ${chart.docs.length} object(s): ${chart.docs.map((d) => `${d.kind}/${d.metadata.name}`).join(', ')}`);
  if (!deploy || !cm || !svc) return;
  const pod = deploy.spec.template.spec;
  const c = pod.containers[0];

  // ---------------------------------------------------------------------------
  section("compose's Kong: image, command, environment, ports and stop grace period");
  check(pod.containers.length === 1, `one container (${pod.containers.length})`);
  check(/@sha256:[0-9a-f]{64}$/.test(kong.image ?? ''), `compose pins Kong's image by digest, so compose, CI and EKS run the same bits (${kong.image})`);
  check(c.image === kong.image, `the chart's image is exactly compose's${c.image === kong.image ? '' : ` -- chart ${c.image}, compose ${kong.image}`}`);
  check(Array.isArray(kong.command) && JSON.stringify(c.args) === JSON.stringify(kong.command) && !c.command, `args are compose's command, and the image's entrypoint stays, as compose keeps it (${JSON.stringify(c.args)}${c.command ? `; command ${JSON.stringify(c.command)}` : ''})`);
  const fromKongEnv = (c.envFrom ?? []).length === 1 && c.envFrom[0].secretRef?.name === 'kong-env';
  check(fromKongEnv && !(c.env ?? []).length, `the environment is the Secret kong-env, made from compose's, and nothing is written beside it (envFrom ${JSON.stringify((c.envFrom ?? []).map((f) => f.secretRef?.name ?? f.configMapRef?.name))}, env ${(c.env ?? []).map((e) => e.name).join(', ') || 'none'})`);
  const composePorts = (kong.ports ?? []).map((p) => Number(p.target));
  const podPorts = new Set((c.ports ?? []).map((p) => p.containerPort));
  check(composePorts.length > 0 && composePorts.every((p) => podPorts.has(p)), `the pod declares every port compose publishes (${composePorts.join(', ')}; pod ${[...podPorts].join(', ')})`);
  const grace = seconds(kong.stop_grace_period);
  check(grace !== undefined && pod.terminationGracePeriodSeconds === grace, `terminationGracePeriodSeconds ${pod.terminationGracePeriodSeconds} is compose's stop_grace_period (${kong.stop_grace_period ?? 'unset'})`);

  // ---------------------------------------------------------------------------
  section('the files compose mounts: this directory, and the public half of the keypair');
  const env = kong.environment ?? {};
  const binds = (kong.volumes ?? []).filter((v) => v.type === 'bind');
  check(binds.length === 1 && resolve(binds[0].source) === resolve(chartDir), `compose bind-mounts exactly one directory, and it is this chart's own (${binds.map((b) => b.source).join(', ') || 'none'})`);
  const bind = binds[0];
  const mountAt = (target) => (c.volumeMounts ?? []).find((m) => m.mountPath === target);
  const volumeOf = (mount) => (pod.volumes ?? []).find((v) => v.name === mount?.name);
  if (bind) {
    const m = mountAt(bind.target);
    check(volumeOf(m)?.configMap?.name === cm.metadata.name && m.readOnly === true, `the pod mounts ConfigMap ${cm.metadata.name} read-only at ${bind.target}, where compose mounts the directory (${m ? (volumeOf(m)?.configMap ? volumeOf(m).configMap.name : 'not a ConfigMap') : 'nothing mounted there'})`);
    const want = ['kong.yml', 'render-consumer.lua'];
    const have = Object.keys(cm.data ?? {}).sort();
    check(JSON.stringify(have) === JSON.stringify(want.slice().sort()), `the ConfigMap holds exactly ${want.join(' and ')} (${have.join(', ') || 'nothing'})`);
    for (const f of want) {
      const onDisk = existsSync(join(chartDir, f)) ? lf(readFileSync(join(chartDir, f), 'utf8')) : null;
      check(onDisk !== null && cm.data?.[f] === onDisk, `${f} in the ConfigMap is the file compose mounts, byte for byte with LF endings (${onDisk === null ? 'no such file' : `${onDisk.length} bytes; ConfigMap ${String(cm.data?.[f] ?? '').length}`})`);
    }
    const script = String(c.args?.at(-1) ?? '');
    check(script.includes(`${bind.target}/render-consumer.lua`) && (env.KONG_TEMPLATE ?? `${bind.target}/kong.yml`).startsWith(`${bind.target}/`), `the command renders from ${bind.target}, the mounted copy`);
  }
  const vols = (kong.volumes ?? []).filter((v) => v.type === 'volume');
  check(vols.length === 1, `CONTROL: compose mounts ${vols.length} volume into Kong: ${vols.map((v) => `${v.source} at ${v.target}`).join(', ')}`);
  for (const v of vols) {
    const m = mountAt(v.target);
    const sec = volumeOf(m)?.secret;
    const keyFile = String(env.AUTH_PUBLIC_KEY_FILE ?? '');
    const item = posix.relative(v.target, keyFile);
    const items = sec?.items ?? [];
    check(
      sec?.secretName === 'jwt-keys' && m.readOnly === true && items.length === 1 && items[0].key === 'public.pem' && items[0].path === item && !item.startsWith('..'),
      `compose's ${v.source} at ${v.target} is jwt-keys' public half only, read-only, as ${item || '(nothing)'} -- the file AUTH_PUBLIC_KEY_FILE (${keyFile || 'unset'}) names (${sec ? `${sec.secretName}: ${items.map((i) => `${i.key} as ${i.path}`).join(', ') || 'every key, the private one too'}` : 'not a Secret'})`,
    );
  }

  // ---------------------------------------------------------------------------
  section('every upstream kong.yml routes to is a Service the charts make, on that port');
  const kongYml = existsSync(join(chartDir, 'kong.yml')) ? readFileSync(join(chartDir, 'kong.yml'), 'utf8') : '';
  const upstreams = [...kongYml.matchAll(/^\s*url:\s*(\S+)\s*$/gm)].map((m) => new URL(m[1]));
  const services = new Map();
  const add = (docs, who) => {
    for (const d of docs.filter((x) => x.kind === 'Service')) services.set(d.metadata.name, { ports: new Set((d.spec.ports ?? []).map((p) => p.port)), who });
  };
  const chartNames = readdirSync(join(root, 'charts'), { withFileTypes: true })
    .filter((d) => d.isDirectory() && existsSync(join(root, 'charts', d.name, 'Chart.yaml')))
    .map((d) => d.name)
    .filter((n) => !/^type:\s*library\s*$/m.test(readFileSync(join(root, 'charts', n, 'Chart.yaml'), 'utf8')));
  let rendered = 0;
  for (const name of chartNames) {
    const dep = sh('helm', ['dependency', 'build', join(root, 'charts', name)]);
    const r = dep.status === 0 ? render(join(root, 'charts', name), ['--set', 'image.tag=abc1234']) : { error: dep.stderr };
    if (r.error) {
      check(false, `charts/${name} did not render: ${r.error.trim().slice(-300)}`);
      continue;
    }
    rendered += 1;
    add(r.docs, `charts/${name}`);
  }
  const stores = render(join(root, 'infra', 'stores'));
  if (stores.error) check(false, `infra/stores did not render: ${stores.error.slice(-300)}`);
  else add(stores.docs, 'infra/stores');
  check(rendered === chartNames.length && upstreams.length > 0, `CONTROL: kong.yml routes to ${upstreams.length} upstream(s); ${rendered} of ${chartNames.length} chart(s) and infra/stores rendered ${services.size} Service(s)`);
  for (const u of upstreams) {
    const s = services.get(u.hostname);
    const port = Number(u.port || (u.protocol === 'https:' ? 443 : 80));
    check(s !== undefined && s.ports.has(port) && s.who.startsWith('charts/'), `${u.href}: ${s ? `Service ${u.hostname} from ${s.who}, ports ${[...s.ports].join(', ')}` : 'no Service of that name'}`);
  }
  const redis = /redis:\s*\r?\n\s*host:\s*(\S+)\s*\r?\n\s*port:\s*(\d+)/.exec(kongYml);
  check(redis !== null, `CONTROL: kong.yml's rate limit names a Redis (${redis ? `${redis[1]}:${redis[2]}` : 'none found'})`);
  if (redis) {
    const s = services.get(redis[1]);
    check(s !== undefined && s.ports.has(Number(redis[2])) && s.who === 'infra/stores', `the rate limit's Redis, ${redis[1]}:${redis[2]}, is a Service infra/stores makes (${s ? `${s.who}, ports ${[...s.ports].join(', ')}` : 'no such Service'}): unreachable, the limit would fail open without a word`);
  }

  // ---------------------------------------------------------------------------
  section('the pod: no service links or API token, its own non-root user, probes, a graceful stop, resources');
  check(pod.enableServiceLinks === false, `enableServiceLinks false: Kong reads KONG_* as configuration, and links would add KONG_PORT and seven more for the Service named kong (${pod.enableServiceLinks})`);
  check(pod.automountServiceAccountToken === false, `no Kubernetes API token mounted (${pod.automountServiceAccountToken})`);
  const sc = pod.securityContext ?? {};
  const user = sh('docker', ['image', 'inspect', kong.image, '--format', '{{.Config.User}}']);
  if (user.status === 0) {
    const id = sh('docker', ['run', '--rm', '--entrypoint', 'id', kong.image, '-u']);
    const uid = id.status === 0 ? Number(id.stdout.trim()) : Number.NaN;
    check(sc.runAsNonRoot === true && uid > 0 && sc.runAsUser === uid, `runs as the image's own user, ${user.stdout.trim()} (uid ${uid}), non-root (runAsNonRoot ${sc.runAsNonRoot}, runAsUser ${sc.runAsUser})`);
  } else if (process.env.CI) {
    check(false, `${kong.image} is not on the runner, so who it runs as cannot be checked; this step must run after the step that parses kong.yml with it`);
  } else {
    note(`${kong.image} is not on this machine, so the uid is not checked against the image`);
    check(sc.runAsNonRoot === true && sc.runAsUser > 0, `runs non-root (runAsNonRoot ${sc.runAsNonRoot}, runAsUser ${sc.runAsUser})`);
  }
  const csc = c.securityContext ?? {};
  check(csc.allowPrivilegeEscalation === false && JSON.stringify(csc.capabilities?.drop) === JSON.stringify(['ALL']), `no privilege escalation, every capability dropped (${csc.allowPrivilegeEscalation}, drop ${JSON.stringify(csc.capabilities?.drop)})`);
  const statusPort = (c.ports ?? []).find((p) => p.name === 'status');
  const onStatus = (probe) => probe?.httpGet && (probe.httpGet.port === 'status' || probe.httpGet.port === statusPort?.containerPort);
  check(String(env.KONG_STATUS_LISTEN ?? '').endsWith(`:${statusPort?.containerPort}`), `the status port the probes ask (${statusPort?.containerPort}) is where compose's KONG_STATUS_LISTEN listens (${env.KONG_STATUS_LISTEN})`);
  check(onStatus(c.startupProbe) && onStatus(c.livenessProbe) && onStatus(c.readinessProbe), 'startup, liveness and readiness probes, each on the status port');
  check(c.readinessProbe?.httpGet?.path === '/status/ready', `readiness asks /status/ready, which answers once the configuration is loaded (${c.readinessProbe?.httpGet?.path})`);
  const quit = c.lifecycle?.preStop?.exec?.command ?? [];
  const wait = Number(/^--wait=(\d+)$/.exec(quit.find((a) => a.startsWith('--wait=')) ?? '')?.[1]);
  const timeout = Number(/^--timeout=(\d+)$/.exec(quit.find((a) => a.startsWith('--timeout=')) ?? '')?.[1]);
  check(quit[0] === 'kong' && quit[1] === 'quit' && wait > 0 && timeout > 0 && wait + timeout < pod.terminationGracePeriodSeconds, `preStop keeps serving for ${wait} s while the pod leaves the Service, then quits gracefully for up to ${timeout} s: inside the ${pod.terminationGracePeriodSeconds} s grace period (${JSON.stringify(quit)})`);
  const req = c.resources?.requests ?? {};
  const lim = c.resources?.limits ?? {};
  check(Boolean(req.cpu) && Boolean(req.memory) && Boolean(lim.memory), `requests ${req.cpu}/${req.memory}, memory limit ${lim.memory}`);

  // ---------------------------------------------------------------------------
  section('what the Service serves: the proxy, and nothing compose keeps to itself');
  const proxyPort = (c.ports ?? []).find((p) => p.name === 'proxy')?.containerPort;
  check(String(env.KONG_PROXY_LISTEN ?? '').endsWith(`:${proxyPort}`), `the proxy port (${proxyPort}) is where compose's KONG_PROXY_LISTEN listens (${env.KONG_PROXY_LISTEN})`);
  check((svc.spec.type ?? 'ClusterIP') === 'ClusterIP', `the Service is ClusterIP until the domain is decided: with the AWS Load Balancer Controller, LoadBalancer would be an internet-facing NLB (${svc.spec.type ?? 'ClusterIP'})`);
  const served = (svc.spec.ports ?? []).map((p) => (typeof p.targetPort === 'string' ? (c.ports ?? []).find((x) => x.name === p.targetPort)?.containerPort : p.targetPort ?? p.port));
  check(served.length === 1 && served[0] === proxyPort, `the Service reaches the proxy port only (${served.join(', ')}), never the status port, which compose publishes to the host's loopback alone`);
  check(/^127\.0\.0\.1:\d+$/.test(String(env.KONG_ADMIN_LISTEN ?? '')), `the Admin API listens on the container's own loopback, so nothing in the cluster can reach it (${env.KONG_ADMIN_LISTEN}); it has no authentication, and POST /config replaces everything`);

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    return;
  }
  console.log(`infra/kong runs compose's gateway in front of the ${upstreams.length} upstreams kong.yml names, each a Service the charts make`);
}

main();
