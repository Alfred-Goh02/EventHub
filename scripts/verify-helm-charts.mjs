/**
 * Asserts that every service chart renders a workload Kubernetes can run, and that its
 * probes ask routes the service really has.
 *
 * THE FIRST INSTALL WOULD HAVE TAUGHT THIS THE HARD WAY. Auth, Event and Ticketing had
 * /health and no /ready, so a readiness probe on /ready answered 404 for ever: measured
 * on kind, two Auth pods Running, 0/2 ready, and `helm --wait` gave up after five
 * minutes. Nothing in the chart was wrong except the one thing that mattered. So the
 * probe paths are judged against the service's own source: each must be a route it
 * registers, with comments stripped first (a route named in a comment is not a route).
 *
 * PER CHART, derived from charts/ (every chart that is not a library):
 *   - it renders, through eventhub-common, a Deployment and the Secret its
 *     environment comes from; a Service when it listens, a PodDisruptionBudget when it
 *     runs more than one replica;
 *   - its working directory is a service in this repository, and its command runs a
 *     file that service builds;
 *   - liveness and readiness ask different paths, and each is a route the service
 *     registers. Liveness on a dependency restarts every replica when the dependency
 *     is slow; readiness on nothing says ready to a pod that cannot serve;
 *   - requests for CPU and memory and a memory limit are set: without requests the
 *     scheduler packs blind, and an HPA has nothing to compute against;
 *   - it runs as a non-root user and cannot escalate;
 *   - terminationGracePeriodSeconds is above the drain backstop in @eventhub/http, read
 *     from that file, so SIGTERM's drain finishes before the kubelet's SIGKILL;
 *   - more than one replica comes with a topology spread and a disruption budget;
 *   - Auth alone gets the private key; every other service gets the public half only.
 *
 * AND THE LIBRARY REFUSES `latest`: a render with image.tag=latest must fail.
 *
 * Needs helm on PATH (GitHub's Ubuntu runners have it):
 *   node scripts/verify-helm-charts.mjs
 */

import { execFileSync, spawnSync } from 'node:child_process';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
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

/** TypeScript with // and block comments removed, strings and templates kept whole. */
function stripComments(src) {
  let out = '';
  let quote = null;
  for (let i = 0; i < src.length; i += 1) {
    const c = src[i];
    if (quote) {
      out += c;
      if (c === '\\') out += src[(i += 1)] ?? '';
      else if (c === quote) quote = null;
    } else if (c === "'" || c === '"' || c === '`') {
      quote = c;
      out += c;
    } else if (c === '/' && src[i + 1] === '/') {
      while (i < src.length && src[i] !== '\n') i += 1;
      out += '\n';
    } else if (c === '/' && src[i + 1] === '*') {
      const end = src.indexOf('*/', i + 2);
      i = end === -1 ? src.length : end + 1;
    } else {
      out += c;
    }
  }
  return out;
}

/**
 * Every GET route a service registers: app.get('/x' or router.get('/x' in Express, and
 * req.url === '/x' in a bare node:http listener, which is how the relay answers.
 */
function routesOf(serviceDir) {
  const routes = new Set();
  const walk = (dir) => {
    for (const e of readdirSync(dir, { withFileTypes: true })) {
      if (e.isDirectory()) {
        if (e.name !== 'generated') walk(join(dir, e.name));
      } else if (e.name.endsWith('.ts')) {
        const code = stripComments(readFileSync(join(dir, e.name), 'utf8'));
        for (const m of code.matchAll(/\.get\(\s*'([^']+)'/g)) routes.add(m[1]);
        for (const m of code.matchAll(/req\.url\s*[!=]==\s*'([^']+)'/g)) routes.add(m[1]);
      }
    }
  };
  walk(join(serviceDir, 'src'));
  return routes;
}

/** Whether a source file starts a listener: Express's app.listen or node:http's createServer. */
const listens = (file) => /\.listen\(|createServer\(/.test(stripComments(readFileSync(file, 'utf8')));

/**
 * Every long-running process compose runs from the app image, with what it runs and the
 * port its healthcheck asks. setup is left out by name: it is a one-shot (migrations,
 * keys, topics), and on EKS it becomes a Job, not a Deployment.
 */
function composeProcesses() {
  const r = spawnSync('docker', ['compose', '-p', 'eventhub-rebuild', 'config', '--format', 'json'], { cwd: ROOT, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
  if (r.status !== 0) return { error: r.stderr.trim() || `docker compose config exited ${r.status}` };
  const services = JSON.parse(r.stdout).services;
  const processes = {};
  for (const [name, s] of Object.entries(services)) {
    if (!String(s.image ?? '').startsWith('eventhub/app') || name === 'setup') continue;
    const test = (s.healthcheck?.test ?? []).join(' ');
    processes[name] = {
      workingDir: s.working_dir,
      command: s.command,
      probePort: Number(/localhost:(\d+)\//.exec(test)?.[1]) || null,
    };
  }
  return { processes };
}

function helm(args) {
  return spawnSync('helm', args, { cwd: ROOT, encoding: 'utf8' });
}

/** The chart's resources, rendered as JSON through eventhub-common's render=json. */
function render(chart, extra = [], withEnv = true) {
  const r = helm(['template', chart, join('charts', chart), '--set', 'render=json', ...(withEnv ? ['--set', 'env.PORT=0'] : []), ...extra]);
  if (r.status !== 0) return { error: r.stderr.trim() };
  const docs = r.stdout
    .split(/^---\s*$/m)
    .map((d) => d.replace(/^#.*$/gm, '').trim())
    .filter(Boolean);
  try {
    return { resources: docs.map((d) => JSON.parse(d)) };
  } catch (err) {
    return { error: `not JSON: ${err.message}` };
  }
}

function main() {
  const version = spawnSync('helm', ['version', '--short'], { encoding: 'utf8' });
  if (version.status !== 0) {
    check(false, 'helm is on PATH');
    return;
  }
  console.log(`  helm ${version.stdout.trim()}`);

  // The detector's own controls: it finds a route, and not one inside a comment.
  const sample = stripComments("// app.get('/commented')\n/* router.get('/blocked') */\napp.get('/real', h); const s = '// not a comment';");
  check(
    sample.includes("app.get('/real'") && !sample.includes('/commented') && !sample.includes('/blocked') && sample.includes("'// not a comment'"),
    'CONTROL: comments are stripped before routes are read, and a // inside a string is kept',
  );

  const compose = composeProcesses();
  check(!compose.error, `CONTROL: compose's own configuration is read${compose.error ? `: ${compose.error}` : ` (${Object.keys(compose.processes).length} long-running process(es) from the app image)`}`);

  const drainSrc = readFileSync(join(ROOT, 'packages', 'http', 'src', 'index.ts'), 'utf8');
  const drainMs = Number(/graceMs \?\? ([\d_]+)/.exec(drainSrc)?.[1]?.replace(/_/g, ''));
  check(Number.isFinite(drainMs) && drainMs > 0, `CONTROL: the drain backstop is read from @eventhub/http (${drainMs} ms)`);

  const charts = readdirSync(join(ROOT, 'charts'), { withFileTypes: true })
    .filter((d) => d.isDirectory() && existsSync(join(ROOT, 'charts', d.name, 'Chart.yaml')))
    .map((d) => d.name)
    .sort();
  const libraries = charts.filter((c) => /^type:\s*library\s*$/m.test(readFileSync(join(ROOT, 'charts', c, 'Chart.yaml'), 'utf8')));
  const services = charts.filter((c) => !libraries.includes(c));
  check(libraries.includes('eventhub-common') && services.length > 0, `CONTROL: charts read from charts/: library ${libraries.join(', ')}; ${services.length} service chart(s): ${services.join(', ')}`);

  // A process compose runs with no chart would never run on the cluster -- the same gap
  // verify-compose-starts-every-process.mjs closes for compose -- and a chart with no
  // process would run something nothing else does.
  if (compose.processes) {
    const procs = Object.keys(compose.processes).sort();
    const noChart = procs.filter((p) => !services.includes(p));
    const noProcess = services.filter((c) => !procs.includes(c));
    check(noChart.length === 0, noChart.length === 0 ? `every process compose runs has a chart (${procs.length})` : `compose runs ${noChart.join(', ')}, and no chart does`);
    check(noProcess.length === 0, noProcess.length === 0 ? 'and every chart is a process compose runs' : `charts/${noProcess.join(', charts/')} run nothing compose does`);
  }

  for (const chart of services) {
    console.log('');
    console.log(`  charts/${chart}`);
    const dep = execFileSync('helm', ['dependency', 'build', join('charts', chart)], { cwd: ROOT, encoding: 'utf8' });
    void dep;
    const { resources, error } = render(chart);
    check(!error, `it renders${error ? `: ${error}` : ''}`);
    if (!resources) continue;
    const of = (kind) => resources.filter((r) => r.kind === kind);
    const [deploy] = of('Deployment');
    check(Boolean(deploy) && of('Secret').length === 1, `through eventhub-common: a Deployment and its Secret (${resources.map((r) => r.kind).join(', ')})`);
    if (!deploy) continue;
    const pod = deploy.spec.template.spec;
    const c = pod.containers[0];

    const dir = /^\/app\/services\/([a-z-]+)$/.exec(c.workingDir ?? '')?.[1];
    const serviceDir = dir ? join(ROOT, 'services', dir) : null;
    check(Boolean(serviceDir && existsSync(join(serviceDir, 'package.json'))), `its working directory is a service here (${c.workingDir})`);
    if (!serviceDir) continue;
    const entry = (c.command ?? []).find((a) => /^dist\/.+\.js$/.test(a));
    const source = entry ? join(serviceDir, entry.replace(/^dist\//, 'src/').replace(/\.js$/, '.ts')) : null;
    check(Boolean(source && existsSync(source)), `its command runs a file the service builds (${(c.command ?? []).join(' ')})`);

    const proc = compose.processes?.[chart];
    if (proc) {
      check(
        proc.workingDir === c.workingDir && JSON.stringify(proc.command) === JSON.stringify(c.command),
        `it runs what compose runs (${c.workingDir}: ${(c.command ?? []).join(' ')})`,
      );
    }

    const port = c.ports?.[0]?.containerPort;
    const isService = c.ports?.[0]?.name === 'http';
    const live = c.livenessProbe?.httpGet?.path;
    const ready = c.readinessProbe?.httpGet?.path;
    if (port) {
      const routes = routesOf(serviceDir);
      check(routes.has('/health'), `CONTROL: the route scan of services/${dir}/src finds its /health (${routes.size} GET route(s))`);
      if (proc) check(proc.probePort === port, `it is probed on ${port}, the port compose's healthcheck asks (${proc.probePort})`);
      check(Boolean(live) && routes.has(live), `liveness asks ${live}, a route services/${dir} registers`);
      if (isService) {
        check(Boolean(ready && ready !== live), `liveness and readiness ask different paths (${live}, ${ready})`);
        check(of('Service').length === 1, 'a Service, since it serves');
      }
      if (ready) check(routes.has(ready), `readiness asks ${ready}, a route services/${dir} registers`);
    } else {
      // Nothing to probe is only right for a process that never listens.
      check(Boolean(source) && !listens(source), `it has no listener to probe, and ${entry} starts none`);
      if (proc) check(proc.probePort === null, 'and compose has no healthcheck for it either');
    }

    const res = c.resources ?? {};
    check(Boolean(res.requests?.cpu && res.requests?.memory && res.limits?.memory), `requests for CPU and memory, and a memory limit (${JSON.stringify(res)})`);
    check(pod.securityContext?.runAsNonRoot === true && c.securityContext?.allowPrivilegeEscalation === false, 'runs as non-root, and cannot escalate');
    const grace = pod.terminationGracePeriodSeconds;
    const preStop = Number(c.lifecycle?.preStop?.exec?.command?.[1] ?? 0);
    if (isService) {
      check(
        c.lifecycle?.preStop?.exec?.command?.[0] === 'sleep' && preStop > 0,
        `it keeps serving ${preStop} s after termination starts, while its endpoint is removed (a rolling restart lost 8 of 690,712 requests without it)`,
      );
    }
    check(
      typeof grace === 'number' && grace * 1000 > preStop * 1000 + drainMs,
      `terminationGracePeriodSeconds ${grace} s holds the ${preStop} s preStop wait and the ${drainMs / 1000} s drain backstop`,
    );

    if (deploy.spec.replicas > 1) {
      check(Array.isArray(pod.topologySpreadConstraints) && pod.topologySpreadConstraints.length > 0, `${deploy.spec.replicas} replicas, spread across nodes`);
      check(of('PodDisruptionBudget').length === 1, 'and a PodDisruptionBudget keeps one through a drain');
    }

    const mount = c.volumeMounts?.find((m) => m.name === 'jwt-keys');
    const items = pod.volumes?.find((v) => v.name === 'jwt-keys')?.secret?.items;
    if (chart === 'auth') {
      check(mount?.mountPath === `${c.workingDir}/keys` && !items, 'Auth, the one service that signs, gets every key file where jwt.ts reads them');
    } else if (mount) {
      check(Boolean(items) && items.length === 1 && items[0].key === 'public.pem', 'the public key only: this service verifies tokens and mints none');
    }

    const latest = render(chart, ['--set', 'image.tag=latest']);
    check(Boolean(latest.error) && /latest/.test(latest.error), 'and the library refuses image.tag=latest');

    // As ArgoCD installs it, from git: no environment, because passwords are not in git.
    // The chart must then render no Secret -- an empty one would overwrite the one made
    // beside it -- and the Deployment must still name that Secret.
    const gitops = render(chart, [], false);
    const gitopsDeploy = gitops.resources?.find((r) => r.kind === 'Deployment');
    const envRef = gitopsDeploy?.spec?.template?.spec?.containers?.[0]?.envFrom?.[0]?.secretRef?.name;
    check(
      !gitops.error && !gitops.resources.some((r) => r.kind === 'Secret') && envRef === `${chart}-env`,
      `with no environment, as ArgoCD installs it: no Secret rendered, and the Deployment still reads ${envRef}`,
    );
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    return;
  }
  console.log('every service chart renders a workload Kubernetes can run, probing routes the service really has');
}

main();
