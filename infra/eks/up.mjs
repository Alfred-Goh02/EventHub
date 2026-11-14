/**
 * Installs EventHub on the EKS demo cluster, and takes it off again.
 *
 *   node infra/eks/up.mjs                 everything, in order, each step idempotent
 *   node infra/eks/up.mjs --down          the releases off, the stores' disks deleted first
 *   options: --context <kube context, default eventhub-demo> --region <default ap-southeast-2>
 *
 * Needs: the envs/demo Terraform applied, its kubeconfig entry
 * (`aws eks update-kubeconfig ... --alias eventhub-demo`), AWS_PROFILE set to a profile that
 * renews (infra/terraform/README.md), and docker, helm, kubectl and git on PATH. A clean
 * working tree: the image is tagged with the commit it is built from.
 *
 * THE SAME SOURCES KIND USES. Everything that goes into the cluster comes from where
 * infra/local/up.mjs takes it for kind, never from a copy:
 *   - each service's environment is compose's, from `docker compose config`, into the
 *     Secret `<chart>-env` its chart reads (the GitOps shape, as on kind), and Kong's into
 *     kong-env, for infra/kong, the chart that is the directory compose mounts into Kong;
 *   - Auth's and Notification's keypairs are the files in services/auth/keys and
 *     services/notification/keys, into jwt-keys and notification-keys;
 *   - the stores' Secrets come from infra/stores/secrets.mjs, from compose and db/;
 *   - setup runs compose's own setup command, with compose's environment for it.
 * The keys and passwords are therefore the local development ones. That is a demo's
 * choice, and the cluster has no way in from outside yet; real ones come with the step that
 * puts a public endpoint in front of it.
 *
 * IN ORDER, each step waiting for the last:
 *   1. the cluster answers, its nodes are Ready, and the EBS CSI driver is ACTIVE;
 *   2. the images (IMAGES): eventhub/app from the root, and eventhub/web from apps/web,
 *      each built from HEAD as <name>:<sha> if it is not here, and pushed to its ECR
 *      repository unless that tag is already there (tags are IMMUTABLE);
 *   3. the stores' Secrets, then infra/stores, waited for until every store is ready;
 *   4. the services' Secrets: keys, one environment per chart, and setup's;
 *   5. setup, as a Job named for the image, waited for until it has finished;
 *   6. every chart under charts/, on the ECR image, one replica each (pods, not memory,
 *      are this cluster's tight limit), each waited for;
 *   7. the gateway, infra/kong, once the services it routes to are there;
 *   8. the web app, infra/web, on its own image;
 *   9. every service's /ready, asked from inside the cluster;
 *  10. requests through the gateway and to the web app, from inside the cluster
 *      (gatewayProbe and webProbe say what each one shows). Both Services are ClusterIP:
 *      from a laptop, `kubectl port-forward` (infra/web/values.yaml says how).
 */

import { execFileSync, spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');

/**
 * Where each key Secret's files come from: the same directories infra/local/up.mjs reads
 * for kind, so both clusters hold the same keypairs. scripts/verify-eks-install.mjs checks
 * that up.mjs still reads these two.
 */
export const KEY_DIRS = { 'jwt-keys': 'services/auth/keys', 'notification-keys': 'services/notification/keys' };

/**
 * The images the installer builds and pushes, each to an ECR repository of the same name
 * (infra/terraform/envs/demo makes both), from its own build context. The web app is its own
 * project (apps/web/README.md), so its own image. scripts/verify-web-chart.mjs checks that
 * each context has a Dockerfile and that Terraform makes each repository.
 */
export const IMAGES = { 'eventhub/app': '.', 'eventhub/web': 'apps/web' };

class Refusal extends Error {}
const refuse = (msg) => {
  throw new Refusal(msg);
};

/** Every chart under charts/ that is not a library: the same set infra/local/up.mjs installs. */
export function allCharts(root = ROOT) {
  return readdirSync(join(root, 'charts'), { withFileTypes: true })
    .filter((d) => d.isDirectory() && existsSync(join(root, 'charts', d.name, 'Chart.yaml')))
    .map((d) => d.name)
    .filter((name) => !/^type:\s*library\s*$/m.test(readFileSync(join(root, 'charts', name, 'Chart.yaml'), 'utf8')))
    .sort();
}

/** A service's environment as compose resolves it: every value a string, an unset one empty. */
export function envOf(compose, service) {
  const svc = compose.services[service];
  if (!svc) refuse(`compose has no service named ${service}`);
  return Object.fromEntries(Object.entries(svc.environment ?? {}).map(([k, v]) => [k, v === null ? '' : String(v)]));
}

/** Compose's setup command, which the setup Job runs as it is. */
export function setupCommand(compose) {
  const command = compose.services.setup?.command;
  if (!Array.isArray(command) || command.length === 0) refuse('compose has no setup command to run');
  return command;
}

/**
 * Every Secret the services and setup read, and from where: { name: { kind, data, from } }.
 * `data` holds strings; nothing here is printed.
 */
export function appSecrets(compose, charts, root = ROOT) {
  const out = {};
  for (const chart of charts) out[`${chart}-env`] = { data: envOf(compose, chart), from: `docker-compose.yml ${chart}.environment` };
  out['setup-env'] = { data: envOf(compose, 'setup'), from: 'docker-compose.yml setup.environment' };
  out['kong-env'] = { data: envOf(compose, 'kong'), from: 'docker-compose.yml kong.environment' };
  for (const [name, dir] of Object.entries(KEY_DIRS)) {
    const abs = join(root, dir);
    const pems = existsSync(abs) ? readdirSync(abs).filter((f) => f.endsWith('.pem')).sort() : [];
    out[name] = { data: Object.fromEntries(pems.map((f) => [f, readFileSync(join(abs, f), 'utf8')])), from: `${dir}/*.pem` };
  }
  const auth = out['jwt-keys'].data;
  const notif = out['notification-keys'].data;
  for (const f of ['private.pem', 'public.pem', 'notification-public.pem']) {
    if (!auth[f]) refuse(`${KEY_DIRS['jwt-keys']} has no ${f}; run setup once locally (docker compose up), which makes the keypairs`);
  }
  for (const f of ['private.pem', 'public.pem']) {
    // Without both halves, Notification's generate-keys in setup would make a NEW keypair.
    if (!notif[f]) refuse(`${KEY_DIRS['notification-keys']} has no ${f}; run setup once locally (docker compose up)`);
  }
  if (notif['public.pem'] !== auth['notification-public.pem']) {
    refuse("services/notification/keys/public.pem is not the notification-public.pem Auth holds: Auth would seal requests Notification cannot open");
  }
  return out;
}

/**
 * A node script, run once in a pod of `image`, and what it printed: { status, stdout, stderr }.
 *
 * NOT `kubectl run -i --rm`. Attaching races the container: measured on kind, a pod that
 * printed "hi" and exited came back with no output and exit 0, three times out of three,
 * because it had finished before kubectl attached. The installer's /ready checks were built
 * that way and printed their answers on EKS, where nothing made that hold. So the pod runs detached,
 * is waited for until it has finished, and its log is read; then it is deleted.
 */
export function runInCluster(context, image, name, script, extra = []) {
  const k = (args) => {
    const r = spawnSync('kubectl', ['--context', context, '--namespace', 'default', ...args], { encoding: 'utf8', maxBuffer: 64 * 1024 * 1024 });
    return { status: r.error ? -1 : r.status, stdout: r.stdout ?? '', stderr: r.stderr ?? String(r.error?.message ?? '') };
  };
  const made = k(['run', name, '--restart=Never', `--image=${image}`, ...extra, '--', 'node', '-e', script]);
  if (made.status !== 0) return { status: made.status, stdout: '', stderr: made.stderr };
  try {
    let phase = '';
    for (let i = 0; i < 120; i += 1) {
      phase = k(['get', 'pod', name, '-o', 'jsonpath={.status.phase}']).stdout.trim();
      if (phase === 'Succeeded' || phase === 'Failed') break;
      spawnSync(process.execPath, ['-e', 'setTimeout(() => {}, 1000)']);
    }
    const logs = k(['logs', `pod/${name}`]);
    const done = phase === 'Succeeded' || phase === 'Failed';
    return { status: phase === 'Succeeded' ? 0 : 1, stdout: logs.stdout, stderr: done ? logs.stderr : `the pod is still ${phase || 'unknown'} after 120 s` };
  } finally {
    k(['delete', 'pod', name, '--wait=false', '--ignore-not-found']);
  }
}

/**
 * Requests through the gateway, as a script for `node -e` in a pod: five answers, as JSON.
 * node:http, because fetch will not send a Host header, and the tenant is the Host.
 */
export function gatewayProbe(host = 'kong', port = 8000) {
  return `
const http = require('node:http');
const ask = (h, path) => new Promise((done) => {
  const req = http.request({ host: ${JSON.stringify(host)}, port: ${Number(port)}, path, headers: { host: h } }, (res) => {
    let body = '';
    res.setEncoding('utf8');
    res.on('data', (d) => { body += d; });
    res.on('end', () => done({ status: res.statusCode, headers: res.headers, body: body.slice(0, 300) }));
  });
  req.on('error', (e) => done({ error: e.code || e.message }));
  req.setTimeout(10000, () => req.destroy(new Error('no answer in 10 s')));
  req.end();
});
(async () => {
  const out = {};
  out.me1 = await ask('acme.eventhub.localhost', '/auth/me');
  out.me2 = await ask('acme.eventhub.localhost', '/auth/me');
  out.events = await ask('acme.eventhub.localhost', '/events');
  out.noTenant = await ask('www.eventhub.localhost', '/auth/me');
  out.health = await ask('acme.eventhub.localhost', '/health');
  console.log(JSON.stringify(out));
})();`;
}

/**
 * What those answers must show, as [ok, message] pairs. Each is told apart by who answered:
 * a response Kong made itself carries X-Kong-Response-Latency, and one a service made
 * carries X-Kong-Upstream-Latency, so "Kong refused it" and "the service refused it" are
 * never the same 401.
 */
export function judgeGateway(a) {
  const h = (r, k) => r?.headers?.[k];
  const upstream = (r) => h(r, 'x-kong-upstream-latency') !== undefined;
  const said = (r) => (r?.error ? `no answer: ${r.error}` : `${r?.status} ${String(r?.body ?? '').replace(/\s+/g, ' ').slice(0, 80)}`);
  const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
  const r1 = Number(h(a.me1, 'ratelimit-remaining'));
  const r2 = Number(h(a.me2, 'ratelimit-remaining'));
  return [
    [a.me1?.status === 401 && upstream(a.me1) && /"error"/.test(a.me1.body ?? ''), `a tenant's /auth/me reaches Auth, through Kong, and Auth refuses it without a token (${said(a.me1)})`],
    [uuid.test(h(a.me1, 'x-request-id') ?? ''), `Kong gives it a request id (${h(a.me1, 'x-request-id') ?? 'none'})`],
    [h(a.me1, 'ratelimit-limit') === '120' && Number.isFinite(r1) && Number.isFinite(r2) && r2 < r1, `counted in Redis at Auth's 120 a minute: ${h(a.me1, 'ratelimit-remaining') ?? 'no count'}, then ${h(a.me2, 'ratelimit-remaining') ?? 'no count'} left (with Redis unreachable the limit fails open, and no count comes back)`],
    [a.events?.status === 401 && !upstream(a.events) && h(a.events, 'x-kong-response-latency') !== undefined, `/events without a token is refused by Kong itself, never reaching Event: the signature check with Auth's key is on (${said(a.events)})`],
    [a.noTenant?.status === 400 && /tenant subdomain required/.test(a.noTenant.body ?? '') && !upstream(a.noTenant), `a host with no tenant in it is refused at the edge (${said(a.noTenant)})`],
    // Kong's own 404, not anyone's: aimed at the web app instead of Kong, the probe got a 404
    // with no upstream header too, and this check alone passed.
    [a.health?.status === 404 && !upstream(a.health) && h(a.health, 'x-kong-response-latency') !== undefined, `/health is not routed: Kong itself answers 404, as it is the platform's, not the internet's (${said(a.health)})`],
  ];
}

/**
 * The web app, asked from inside the cluster as a browser would ask it: the page, the
 * script the page names, and a page that sends a signed-out reader to sign in.
 */
export function webProbe(host = 'web', port = 3000) {
  return `
const http = require('node:http');
const ask = (path) => new Promise((done) => {
  const req = http.request({ host: ${JSON.stringify(host)}, port: ${Number(port)}, path, headers: { host: 'acme.eventhub.localhost' } }, (res) => {
    let body = '';
    res.setEncoding('utf8');
    res.on('data', (d) => { body += d; });
    res.on('end', () => done({ status: res.statusCode, headers: res.headers, body }));
  });
  req.on('error', (e) => done({ error: e.code || e.message }));
  req.setTimeout(10000, () => req.destroy(new Error('no answer in 10 s')));
  req.end();
});
(async () => {
  const out = {};
  out.page = await ask('/');
  const script = /<script[^>]*type="module"[^>]*src="(\\/assets\\/[^"]+\\.js)"/.exec(out.page.body || '');
  out.scriptPath = script ? script[1] : null;
  out.script = out.scriptPath ? await ask(out.scriptPath) : null;
  if (out.script) out.script.body = String(out.script.body.length);
  out.login = await ask('/login');
  out.page.body = out.page.body.slice(0, 2000);
  out.login.body = '';
  console.log(JSON.stringify(out));
})();`;
}

/** What those answers must show, as [ok, message] pairs. */
export function judgeWeb(a) {
  const type = (r) => String(r?.headers?.['content-type'] ?? '');
  const said = (r) => (r?.error ? `no answer: ${r.error}` : `${r?.status}${type(r) ? ` ${type(r)}` : ''}`);
  return [
    [a.page?.status === 200 && type(a.page).startsWith('text/html') && /<title>EventHub<\/title>/.test(a.page.body ?? ''), `the web app renders its page (${said(a.page)}, ${/<title>[^<]*<\/title>/.exec(a.page?.body ?? '')?.[0] ?? 'no title'})`],
    [Boolean(a.scriptPath) && a.script?.status === 200 && /javascript/.test(type(a.script)) && Number(a.script.body) > 0, `and serves the bundle the page names (${a.scriptPath ?? 'no module script in the page'}: ${a.script ? `${said(a.script)}, ${a.script.body} bytes` : 'not asked'})`],
    [a.login?.status === 307 && a.login?.headers?.location === '/login?redirect=%2F', `a signed-out reader is sent to sign in (${said(a.login)} to ${a.login?.headers?.location ?? 'nowhere'})`],
  ];
}

// ---------------------------------------------------------------------------

const run = (cmd, args, opts = {}) => execFileSync(cmd, args, { cwd: ROOT, encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'], maxBuffer: 64 * 1024 * 1024, ...opts });
const tryRun = (cmd, args, opts = {}) => {
  const r = spawnSync(cmd, args, { cwd: ROOT, encoding: 'utf8', maxBuffer: 64 * 1024 * 1024, ...opts });
  return { status: r.error ? -1 : r.status, stdout: r.stdout ?? '', stderr: r.stderr ?? String(r.error?.message ?? '') };
};
const step = (msg) => console.log(`\n  ${msg}`);
let failed = 0;
const check = (ok, msg) => {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failed += 1;
    process.exitCode = 1;
  }
};

function main() {
  const argv = process.argv.slice(2).filter((a) => a !== '--');
  const option = (name, fallback) => {
    const i = argv.indexOf(name);
    return i === -1 ? fallback : argv[i + 1];
  };
  const CONTEXT = option('--context', 'eventhub-demo');
  const REGION = option('--region', 'ap-southeast-2');
  const DOWN = argv.includes('--down');
  const kubectl = (args, input) => run('kubectl', ['--context', CONTEXT, ...args], input === undefined ? {} : { input });
  const helmArgs = ['--kube-context', CONTEXT, '--namespace', 'default'];
  const charts = allCharts();

  step(`1. the cluster: ${CONTEXT}`);
  const nodes = JSON.parse(kubectl(['get', 'nodes', '-o', 'json'])).items;
  const ready = nodes.filter((n) => n.status.conditions.some((c) => c.type === 'Ready' && c.status === 'True'));
  check(nodes.length > 0 && ready.length === nodes.length, `${ready.length} of ${nodes.length} node(s) Ready`);
  if (failed) return;

  if (DOWN) return down({ kubectl, helmArgs, charts, CONTEXT, REGION });

  const addon = tryRun('aws', ['eks', 'describe-addon', '--region', REGION, '--cluster-name', CONTEXT, '--addon-name', 'aws-ebs-csi-driver', '--query', 'addon.status', '--output', 'text']);
  check(addon.stdout.trim() === 'ACTIVE', `the EBS CSI driver add-on is ${addon.stdout.trim() || addon.stderr.trim().split('\n').pop()}`);
  if (failed) return;

  step('2. the images, by the commit they are built from');
  // A change to a tracked file, or an untracked file the build would copy in, means no
  // commit describes the image. .claude/ is always untracked in the main checkout (the
  // agent's worktrees) and never reaches the image: .dockerignore excludes it, and
  // scripts/verify-eks-install.mjs checks that it does.
  const changed = run('git', ['status', '--porcelain', '--untracked-files=no']).trim();
  const untracked = run('git', ['ls-files', '--others', '--exclude-standard'])
    .split(/\r?\n/)
    .filter((f) => f && !f.startsWith('.claude/'));
  if (changed || untracked.length) refuse(`the working tree is not HEAD, so no commit describes the image:\n${[changed, ...untracked.map((f) => `?? ${f}`)].filter(Boolean).join('\n')}`);
  const sha = run('git', ['rev-parse', '--short', 'HEAD']).trim();
  const repos = {};
  for (const [name, context] of Object.entries(IMAGES)) {
    repos[name] = run('aws', ['ecr', 'describe-repositories', '--region', REGION, '--repository-names', name, '--query', 'repositories[0].repositoryUri', '--output', 'text']).trim();
    const local = `${name}:${sha}`;
    const pushed = `${repos[name]}:${sha}`;
    const there = tryRun('aws', ['ecr', 'describe-images', '--region', REGION, '--repository-name', name, '--image-ids', `imageTag=${sha}`, '--query', 'imageDetails[0].imageTags', '--output', 'text']);
    if (there.status === 0 && there.stdout.includes(sha)) {
      console.log(`    ok    ${pushed} is already in ECR; tags are immutable, so it is the same image`);
      continue;
    }
    if (tryRun('docker', ['image', 'inspect', local]).status !== 0) {
      console.log(`    ..    building ${local} from HEAD, in ${context}`);
      run('docker', ['build', '-t', local, context], { stdio: 'inherit' });
    }
    pushImage(local, pushed, repos[name].split('/')[0], REGION);
    check(true, `pushed ${pushed}`);
  }
  const repo = repos['eventhub/app'];
  const remote = `${repo}:${sha}`;

  step('3. the stores');
  run('node', [join('infra', 'stores', 'secrets.mjs'), '--context', CONTEXT, '--namespace', 'default'], { stdio: 'inherit' });
  run('helm', ['upgrade', '--install', 'stores', join('infra', 'stores'), ...helmArgs, '--wait', '--timeout', '15m'], { stdio: 'inherit' });
  check(true, 'infra/stores installed, every store ready');

  step("4. the services' Secrets");
  const compose = JSON.parse(run('docker', ['compose', '-p', 'eventhub-rebuild', 'config', '--format', 'json']));
  const secrets = appSecrets(compose, charts);
  const items = Object.entries(secrets).map(([name, s]) => ({
    apiVersion: 'v1',
    kind: 'Secret',
    metadata: { name, labels: { 'app.kubernetes.io/part-of': 'eventhub' } },
    type: 'Opaque',
    stringData: s.data,
  }));
  kubectl(['--namespace', 'default', 'apply', '-f', '-'], JSON.stringify({ apiVersion: 'v1', kind: 'List', items }));
  for (const [name, s] of Object.entries(secrets)) console.log(`    ok    ${name}: ${Object.keys(s.data).length} key(s) from ${s.from}`);

  step('5. setup, once for this image');
  const scratch = mkdtempSync(join(tmpdir(), 'eventhub-eks-'));
  try {
    const values = join(scratch, 'setup.json');
    writeFileSync(values, JSON.stringify({ image: { repository: repo, tag: sha }, command: setupCommand(compose) }));
    const r = tryRun('helm', ['upgrade', '--install', 'setup', join('infra', 'setup'), ...helmArgs, '-f', values, '--wait', '--wait-for-jobs', '--timeout', '25m']);
    const logs = tryRun('kubectl', ['--context', CONTEXT, '--namespace', 'default', 'logs', `job/setup-${sha}`, '--all-containers', '--tail', '40']);
    for (const line of logs.stdout.trim().split('\n').filter(Boolean)) console.log(`          ${line}`);
    check(r.status === 0, `setup-${sha} finished${r.status === 0 ? '' : `: ${r.stderr.trim().split('\n').pop()}`}`);
    if (failed) return;

    step('6. the services, on the ECR image, one replica each');
    for (const chart of charts) {
      run('helm', ['dependency', 'build', join('charts', chart)]);
      const c = tryRun('helm', ['upgrade', '--install', chart, join('charts', chart), ...helmArgs, '--set', `image.repository=${repo}`, '--set', `image.tag=${sha}`, '--set', 'replicas=1', '--wait', '--timeout', '10m']);
      check(c.status === 0, `${chart}${c.status === 0 ? '' : `: ${c.stderr.trim().split('\n').pop()}`}`);
    }
  } finally {
    rmSync(scratch, { recursive: true, force: true });
  }

  step('7. the gateway');
  const g = tryRun('helm', ['upgrade', '--install', 'kong', join('infra', 'kong'), ...helmArgs, '--wait', '--timeout', '5m']);
  check(g.status === 0, `infra/kong${g.status === 0 ? ', ready' : `: ${g.stderr.trim().split('\n').pop()}`}`);
  // And the OLD pods gone, not only the new ones ready. `--wait` returns once the new pods
  // are ready, while an old one is still in the Service: on kind, a probe straight after an
  // upgrade was answered by the old configuration. rollout status waits until every old pod
  // is being deleted; a pod being deleted still serves through its preStop wait, so then
  // wait for it to be gone.
  const rolled = tryRun('kubectl', ['--context', CONTEXT, '--namespace', 'default', 'rollout', 'status', 'deployment/kong', '--timeout', '2m']);
  let leaving = null;
  for (let i = 0; rolled.status === 0 && i < 60; i += 1) {
    leaving = JSON.parse(kubectl(['--namespace', 'default', 'get', 'pods', '-l', 'app.kubernetes.io/name=kong', '-o', 'json'])).items.filter((p) => p.metadata.deletionTimestamp).length;
    if (leaving === 0) break;
    spawnSync(process.execPath, ['-e', 'setTimeout(() => {}, 2000)']);
  }
  check(rolled.status === 0 && leaving === 0, `every Kong pod runs this configuration${rolled.status === 0 ? (leaving ? ` -- ${leaving} old pod(s) still leaving` : '') : `: ${rolled.stderr.trim().split('\n').pop()}`}`);

  step('8. the web app');
  const w = tryRun('helm', ['upgrade', '--install', 'web', join('infra', 'web'), ...helmArgs, '--set', `image.repository=${repos['eventhub/web']}`, '--set', `image.tag=${sha}`, '--wait', '--timeout', '5m']);
  check(w.status === 0, `infra/web, on ${repos['eventhub/web']}:${sha}${w.status === 0 ? ', ready' : `: ${w.stderr.trim().split('\n').pop()}`}`);

  step('9. /ready from inside the cluster');
  for (const chart of charts) {
    const port = compose.services[chart]?.environment?.PORT;
    if (!port) continue;
    const r = runInCluster(CONTEXT, remote, `ready-${chart}-${Date.now().toString(36)}`,
      `fetch('http://${chart}:${port}/ready').then(async r => { console.log(r.status, await r.text()); process.exit(r.ok ? 0 : 1) }, e => { console.log('unreachable', e.cause?.code ?? e.message); process.exit(1) })`);
    // The answer itself, not only the exit code (infra/local/up.mjs says why).
    const answer = r.stdout.trim();
    check(r.status === 0 && /^200 /.test(answer), `${chart}: ${answer || r.stderr.trim() || `(no answer, exit ${r.status})`}`);
  }

  step('10. through the gateway, and to the web app, from inside the cluster');
  for (const [name, script, judge] of [['gateway', gatewayProbe(), judgeGateway], ['web', webProbe(), judgeWeb]]) {
    const p = runInCluster(CONTEXT, remote, `${name}-${Date.now().toString(36)}`, script);
    let answers = null;
    try {
      answers = JSON.parse(p.stdout.trim().split('\n').pop());
    } catch {
      check(false, `the ${name} probe gave no answers: ${(p.stdout || p.stderr).trim().slice(-300) || `exit ${p.status}`}`);
    }
    if (answers) for (const [ok, msg] of judge(answers)) check(ok, msg);
  }
  console.log(
    failed
      ? `\nFAIL: ${failed} step(s) failed`
      : `\nEventHub runs on ${CONTEXT}, from ${sha}. To open it from here, stop compose's Kong (docker stop eventhub-kong: it holds 8000), then:\n  kubectl --context ${CONTEXT} port-forward svc/kong 8000:8000\n  kubectl --context ${CONTEXT} port-forward svc/web 5173:3000\n  and http://acme.eventhub.localhost:5173`,
  );
}

/**
 * The image to ECR, through a THROWAWAY DOCKER CONFIG that already holds the credential,
 * and no `docker login` at all. Each simpler way failed on 2026-11-14:
 *   - `docker login` into the user's own config hands ECR's token to the credential
 *     helper, and on Windows that is the Credential Manager, which refused it: "error
 *     storing credentials ... The stub received bad data". An ECR token is longer than a
 *     Windows credential may be.
 *   - `docker login` into an EMPTY fresh config did the same. The CLI picks the platform's
 *     helper itself whenever a config holds no credentials (DetectDefaultStore, skipped
 *     only when ContainsAuth()).
 * So the config is written with the credential in its `auths` already: the CLI then keeps
 * to the file, and the push reads it from there. The directory is made for this push alone
 * and deleted after it, token and all: nothing of AWS's stays on the machine. DOCKER_HOST
 * is the current context's engine, which a fresh config would not know.
 */
function pushImage(local, remote, registry, region) {
  const ctx = tryRun('docker', ['context', 'inspect', '--format', '{{.Endpoints.docker.Host}}']);
  const dc = mkdtempSync(join(tmpdir(), 'eventhub-docker-'));
  try {
    // Straight from aws to the file, never through a shell: by hand through a PowerShell
    // pipe the token was re-encoded with a line ending, and ECR answered 400.
    const token = run('aws', ['ecr', 'get-login-password', '--region', region]).trim();
    writeFileSync(join(dc, 'config.json'), JSON.stringify({ auths: { [registry]: { auth: Buffer.from(`AWS:${token}`).toString('base64') } } }), { mode: 0o600 });
    const env = { ...process.env, DOCKER_CONFIG: dc, ...(ctx.status === 0 && ctx.stdout.trim() ? { DOCKER_HOST: ctx.stdout.trim() } : {}) };
    run('docker', ['tag', local, remote], { env });
    run('docker', ['push', remote], { stdio: ['pipe', 'inherit', 'inherit'], env });
  } finally {
    rmSync(dc, { recursive: true, force: true });
  }
}

/**
 * Off again, in the order that leaves nothing billing: the services and setup first, then
 * the stores, whose StatefulSets take their claims with them and whose claims take their
 * volumes, while the EBS CSI driver still runs to delete them. Then AWS is asked: no volume
 * the driver made may be left.
 */
function down({ kubectl, helmArgs, charts, REGION }) {
  step('2. the releases: the web app, the gateway, the services and setup first, the stores last');
  const releases = new Set(JSON.parse(run('helm', ['list', ...helmArgs, '--output', 'json'])).map((r) => r.name));
  for (const name of ['web', 'kong', ...charts, 'setup', 'stores'].filter((n) => releases.has(n))) {
    const r = tryRun('helm', ['uninstall', name, ...helmArgs, '--wait', '--timeout', '10m']);
    check(r.status === 0, `uninstalled ${name}${r.status === 0 ? '' : `: ${r.stderr.trim().split('\n').pop()}`}`);
  }

  step("3. the stores' disks, gone from the cluster and from AWS");
  let claims = null;
  let pvs = null;
  for (let i = 0; i < 60; i += 1) {
    claims = JSON.parse(kubectl(['get', 'pvc', '--all-namespaces', '-o', 'json'])).items.length;
    pvs = JSON.parse(kubectl(['get', 'pv', '-o', 'json'])).items.length;
    if (claims === 0 && pvs === 0) break;
    spawnSync(process.execPath, ['-e', 'setTimeout(() => {}, 5000)']);
  }
  check(claims === 0 && pvs === 0, `${claims} claim(s) and ${pvs} volume(s) left in the cluster`);
  const vols = run('aws', ['ec2', 'describe-volumes', '--region', REGION, '--filters', 'Name=tag:ebs.csi.aws.com/cluster,Values=true', '--query', 'Volumes[].[VolumeId,State]', '--output', 'text']).trim();
  check(vols === '', `no volume the EBS CSI driver made is left in AWS${vols ? ` -- ${vols.replace(/\s+/g, ' ')}` : ''}`);
  console.log(failed ? '\nFAIL: something is left; do not destroy until it is gone' : '\nnothing of EventHub is left in the cluster: terraform destroy is next');
}

// Run only as a script: scripts/verify-eks-install.mjs imports appSecrets, setupCommand and
// the rest, and must not install anything by doing so.
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    main();
  } catch (e) {
    console.error(`FAIL: ${e instanceof Refusal ? e.message : (e.stderr || e.message || String(e)).toString().trim().slice(-1500)}`);
    process.exit(1);
  }
}
