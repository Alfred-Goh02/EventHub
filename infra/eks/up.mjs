/**
 * Installs EventHub on the EKS demo cluster, and takes it off again.
 *
 *   node infra/eks/up.mjs                 everything, in order, each step idempotent: this
 *                                         checkout's images, built here, pushed to ECR
 *   node infra/eks/up.mjs --gitops        main, from the images CI published to ghcr.io,
 *                                         deployed and kept by Argo CD (gitops() says how)
 *   node infra/eks/up.mjs --down          Argo CD's Applications and the releases off, the
 *                                         stores' disks deleted first
 *   options: --context <kube context, default eventhub-demo> --region <default ap-southeast-2>
 *
 * Needs: the envs/demo Terraform applied, its kubeconfig entry
 * (`aws eks update-kubeconfig ... --alias eventhub-demo`), AWS_PROFILE set to a profile that
 * renews (infra/terraform/README.md), and docker, helm, kubectl and git on PATH. A clean
 * working tree: the image is tagged with the commit it is built from.
 *
 * THE SAME SOURCES KIND USES, AND ITS OWN CREDENTIALS. Everything that goes into the
 * cluster comes from where infra/local/up.mjs takes it for kind, never from a copy:
 *   - each service's environment is compose's, from `docker compose config`, into the
 *     Secret `<chart>-env` its chart reads (the GitOps shape, as on kind), and Kong's into
 *     kong-env, for infra/kong, the chart that is the directory compose mounts into Kong;
 *   - the stores' Secrets are the ones infra/stores/secrets.mjs plans, from compose and db/;
 *   - setup runs compose's own setup command, with compose's environment for it.
 * Except the address the cluster is reached at, which infra/eks/public.mjs sets in one line
 * and derives every variable naming it from -- NODE_ENV, the link templates, Kong's domain,
 * Live's origin rule, the web app's -- and every credential in them. Compose's are development values written into this
 * repository, so each one -- every password, every secret, both keypairs -- is replaced by
 * this cluster's own, made in the cluster on the first install and kept there
 * (infra/eks/credentials.mjs). Nothing is read from services/*\/keys, and nothing of a
 * credential is printed.
 *
 * IN ORDER, each step waiting for the last:
 *   1. the cluster answers, its nodes are Ready, and the EBS CSI driver is ACTIVE;
 *   2. the images (IMAGES): eventhub/app from the root, and eventhub/web from apps/web,
 *      each built from HEAD as <name>:<sha> if it is not here, and pushed to its ECR
 *      repository unless that tag is already there (tags are IMMUTABLE);
 *   3. the credentials, read from the cluster or made there, then every Secret: the
 *      stores', the services', setup's and the gateway's, each with them in place;
 *   4. infra/stores, waited for until every store is ready;
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

import { ARGOCD_MANIFEST, ARGOCD_VERSION, DEPLOY_KEY, GHCR_TOKEN_FILE } from '../argocd/settings.mjs';
import { plan as storePlan, renderChart } from '../stores/secrets.mjs';
import { CREDENTIALS_SECRET, credentialsIn, ensure, keySecrets, withCredentials } from './credentials.mjs';
import { APP_URL_TEMPLATE, DEV_DOMAIN, GATEWAY_PORT, openAt, publicAddress, publicEnvironment } from './public.mjs';

export const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');

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
 * The environment Secrets, from compose: { name: { data, from } }, credentials still
 * compose's. installSecrets() is what the installer makes.
 */
export function envSecrets(compose, charts) {
  const out = {};
  for (const chart of charts) out[`${chart}-env`] = { data: envOf(compose, chart), from: `docker-compose.yml ${chart}.environment` };
  out['setup-env'] = { data: envOf(compose, 'setup'), from: 'docker-compose.yml setup.environment' };
  out['kong-env'] = { data: envOf(compose, 'kong'), from: 'docker-compose.yml kong.environment' };
  return out;
}

/**
 * Every Secret the installer makes, with this cluster's credentials in place of compose's:
 * { secrets: { name: data }, from: { name: where }, creds, made, found }.
 *
 *   storeDocs  infra/stores rendered (renderChart), whose Secrets infra/stores/secrets.mjs plans
 *   existing   what the cluster already holds in CREDENTIALS_SECRET; never replaced
 *
 * Refuses rather than let one compose credential through: after the replacement, nothing
 * found in a credential's place may still be a value compose had there.
 */
export function installSecrets(compose, charts, storeDocs, existing = {}, template = APP_URL_TEMPLATE) {
  const raw = {};
  const from = {};
  // The environment each value came from, where a password's *_USER is: the Secret itself
  // for a service's environment; compose's environment for that store, for a store Secret
  // that holds only the password (plan() names it: "docker-compose.yml <store>.environment.<KEY>").
  const env = {};
  // What the cluster's address sets (infra/eks/public.mjs), over compose's values. None of
  // it is a credential, so the credential search below sees compose's values and these alike.
  const address = publicEnvironment(template, charts, (chart) => envOf(compose, chart));
  for (const [name, s] of Object.entries(envSecrets(compose, charts))) {
    const set = address[name] ?? {};
    raw[name] = { ...s.data, ...set };
    from[name] = Object.keys(set).length ? `${s.from}, with ${Object.keys(set).sort().join(', ')} from the address in infra/eks/public.mjs` : s.from;
    env[name] = () => raw[name];
  }
  raw['web-env'] = address['web-env'];
  from['web-env'] = 'the address in infra/eks/public.mjs';
  env['web-env'] = () => raw['web-env'];
  const stray = Object.keys(address).filter((name) => !raw[name]);
  if (stray.length) refuse(`the address sets variables in Secrets nothing makes: ${stray.join(', ')}`);
  for (const [name, keys] of Object.entries(storePlan(storeDocs, compose))) {
    raw[name] = Object.fromEntries(Object.entries(keys).map(([k, v]) => [k, v.read()]));
    from[name] = Object.values(keys).map((v) => v.from).join(', ');
    env[name] = (key) => {
      const store = /^docker-compose\.yml (\S+?)\.environment\./.exec(keys[key]?.from ?? '')?.[1];
      return store ? envOf(compose, store) : raw[name];
    };
  }
  const envAt = (secret, key) => env[secret](key);
  const found = credentialsIn(raw, envAt);
  const { creds, made } = ensure(found, existing);
  const secrets = withCredentials(raw, creds, envAt);
  for (const [name, data] of Object.entries(keySecrets(creds))) {
    secrets[name] = data;
    from[name] = "this cluster's own keypairs";
  }
  const compose_ = new Set(found.map((f) => f.dev));
  const left = credentialsIn(secrets, (secret, key) => (env[secret] ? env[secret](key) : secrets[secret])).filter((f) => compose_.has(f.dev));
  if (left.length) refuse(`compose's credentials would reach the cluster: ${left.map((f) => `${f.secret}/${f.key}`).join(', ')}`);
  return { secrets, from, creds, made, found };
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

async function main() {
  const argv = process.argv.slice(2).filter((a) => a !== '--');
  const option = (name, fallback) => {
    const i = argv.indexOf(name);
    return i === -1 ? fallback : argv[i + 1];
  };
  const CONTEXT = option('--context', 'eventhub-demo');
  const REGION = option('--region', 'ap-southeast-2');
  const DOWN = argv.includes('--down');
  const GITOPS = argv.includes('--gitops');
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

  // A change to a tracked file, or an untracked file the build would copy in, means no
  // commit describes the image. .claude/ is always untracked in the main checkout (the
  // agent's worktrees) and never reaches the image: .dockerignore excludes it, and
  // scripts/verify-eks-install.mjs checks that it does.
  const changed = run('git', ['status', '--porcelain', '--untracked-files=no']).trim();
  const untracked = run('git', ['ls-files', '--others', '--exclude-standard'])
    .split(/\r?\n/)
    .filter((f) => f && !f.startsWith('.claude/'));
  if (changed || untracked.length) refuse(`the working tree is not HEAD, so no commit describes the image:\n${[changed, ...untracked.map((f) => `?? ${f}`)].filter(Boolean).join('\n')}`);

  if (GITOPS) return gitops({ kubectl, helmArgs, charts, CONTEXT });

  step('2. the images, by the commit they are built from');
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

  const compose = credentialsAndStores({ kubectl, helmArgs, charts, CONTEXT });

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

  checkRunning({ compose, charts, image: remote, CONTEXT, sha });
}

/** The pull Secret for GitHub's registry, on the default service account every pod here runs as. */
export const PULL_SECRET = 'ghcr-pull';
/** The Applications the EKS root makes: one per service chart, and the gateway, the web app and setup. */
export const gitopsApps = (charts) => [...charts, 'kong', 'web', 'setup'];

/** Where Argo CD reads a health check for argoproj.io/Application from: argocd-cm, under this key. */
export const APPLICATION_HEALTH_KEY = 'resource.customizations.health.argoproj.io_Application';
/** The script, infra/argocd/eks/application-health.lua, which says why the root's waves need it. */
export const applicationHealthScript = (root = ROOT) => readFileSync(join(root, 'infra', 'argocd', 'eks', 'application-health.lua'), 'utf8').replace(/\r\n/g, '\n');

/**
 * Puts the Application health check into argocd-cm, then reads argocd-cm back: { ok, why }.
 * Without it, the root's sync waves wait for nothing (application-health.lua says why), so
 * this must happen before the root is applied, and a write that did not land must not pass for
 * one that did. A merge patch on that one key: argocd-cm's other settings are left as they are.
 */
export function installApplicationHealth(kubectl, root = ROOT) {
  const script = applicationHealthScript(root);
  kubectl(['-n', 'argocd', 'patch', 'configmap', 'argocd-cm', '--type', 'merge', '-p', JSON.stringify({ data: { [APPLICATION_HEALTH_KEY]: script } })]);
  const back = JSON.parse(kubectl(['-n', 'argocd', 'get', 'configmap', 'argocd-cm', '-o', 'json'])).data?.[APPLICATION_HEALTH_KEY];
  if (back === undefined) return { ok: false, why: `argocd-cm has no ${APPLICATION_HEALTH_KEY} after the patch` };
  if (back.replace(/\r\n/g, '\n') !== script) return { ok: false, why: `argocd-cm's ${APPLICATION_HEALTH_KEY} is not application-health.lua` };
  return { ok: true, why: `${script.split('\n').length} lines of application-health.lua` };
}

/**
 * Whether ghcr.io has `ref`, asked as the cluster will ask: with the read-only token, through
 * the registry's own token endpoint. { ok, why }. The token goes to ghcr.io and nowhere else.
 */
async function ghcrHas(ref, user, token) {
  const [, name, tag] = /^ghcr\.io\/([^:]+):(.+)$/.exec(ref) ?? [];
  if (!name) return { ok: false, why: `not a ghcr.io reference: ${ref}` };
  const basic = Buffer.from(`${user}:${token}`).toString('base64');
  const t = await fetch(`https://ghcr.io/token?service=ghcr.io&scope=repository:${name}:pull`, { headers: { authorization: `Basic ${basic}` } });
  if (!t.ok) return { ok: false, why: `ghcr.io refused the token (${t.status}): it needs read:packages, for a user who can read the package` };
  const bearer = (await t.json()).token;
  const accept = ['application/vnd.oci.image.index.v1+json', 'application/vnd.docker.distribution.manifest.list.v2+json', 'application/vnd.oci.image.manifest.v1+json', 'application/vnd.docker.distribution.manifest.v2+json'].join(', ');
  const r = await fetch(`https://ghcr.io/v2/${name}/manifests/${tag}`, { method: 'HEAD', headers: { authorization: `Bearer ${bearer}`, accept } });
  if (r.status === 200) return { ok: true };
  return { ok: false, why: r.status === 404 ? 'not published: publish.yml has not finished for this commit (or failed)' : `ghcr.io answered ${r.status}` };
}

/**
 * --gitops: Argo CD runs what main describes, from the images CI published for it.
 *
 * The installer still owns what git cannot hold and what must outlive a commit: the
 * credentials and every Secret, the stores and their disks. Then it installs Argo CD (the
 * version kind runs, infra/argocd/settings.mjs), hands it the repository through the
 * read-only deploy key, and applies infra/argocd/eks/root.json, whose Applications deploy the
 * fifteen services, the gateway, the web app and setup -- each image named by the commit
 * deployed ($ARGOCD_APP_REVISION_SHORT), from GitHub's registry, pulled with a read-only
 * token (GHCR_TOKEN, read:packages only; GHCR_USER, by default the repository's owner).
 *
 * SETUP FIRST, every time. The root gives each Application the commit it resolved main to,
 * and syncs setup in an earlier wave than the rest, so nothing else is synced until setup's
 * Job has finished for that commit: on this install, and on every commit after it
 * (infra/argocd/eks/apps/values.yaml says why both halves are needed). A wave waits on a
 * child's health, and Argo CD has no health check for an Application of its own, so this puts
 * one into argocd-cm before the root is applied (installApplicationHealth).
 *
 * The checkout must BE main: the Secrets come from this checkout's compose, and the cluster
 * runs main's code. A checkout that differs would give main's services another commit's
 * environment.
 */
async function gitops({ kubectl, helmArgs, charts, CONTEXT }) {
  step("2. main, and its images in GitHub's registry");
  run('git', ['fetch', '--quiet', 'origin', 'main']);
  const head = run('git', ['rev-parse', 'HEAD']).trim();
  const mainSha = run('git', ['rev-parse', 'origin/main']).trim();
  if (head !== mainSha) refuse(`this checkout is ${head.slice(0, 7)} and Argo CD deploys main, ${mainSha.slice(0, 7)}: the Secrets must come from the commit the cluster runs. Switch to main and pull, then run again.`);
  const owner = /github\.com[:/]([^/]+)\//.exec(run('git', ['remote', 'get-url', 'origin']))?.[1];
  if (!owner) refuse('origin is not a github.com repository');
  const token = process.env.GHCR_TOKEN || (existsSync(GHCR_TOKEN_FILE) ? readFileSync(GHCR_TOKEN_FILE, 'utf8').trim() : '');
  const user = process.env.GHCR_USER || owner;
  if (!token) refuse(`no token at ${GHCR_TOKEN_FILE}, and GHCR_TOKEN is not set: a GitHub token with read:packages and nothing else, which the cluster pulls main's images with (infra/terraform/README.md says how to make one)`);
  const { plan: publishPlan } = await import('../../scripts/publish-images.mjs');
  const images = await publishPlan({ sha: mainSha, owner });
  for (const img of images) {
    const has = await ghcrHas(img.ref, user, token);
    check(has.ok, `${img.ref}${has.ok ? ', published' : `: ${has.why}`}`);
  }
  if (failed) return;

  const compose = credentialsAndStores({ kubectl, helmArgs, charts, CONTEXT });

  step("5. the pull Secret, on the service account every pod here runs as");
  const auth = Buffer.from(`${user}:${token}`).toString('base64');
  kubectl(['--namespace', 'default', 'apply', '-f', '-'], JSON.stringify(secretOf(PULL_SECRET, { '.dockerconfigjson': JSON.stringify({ auths: { 'ghcr.io': { auth } } }) }, 'kubernetes.io/dockerconfigjson')));
  kubectl(['--namespace', 'default', 'patch', 'serviceaccount', 'default', '--type', 'merge', '-p', JSON.stringify({ imagePullSecrets: [{ name: PULL_SECRET }] })]);
  check(true, `${PULL_SECRET}, read-only, for ${user} at ghcr.io; default/default pulls with it`);

  step(`6. Argo CD ${ARGOCD_VERSION}`);
  tryRun('kubectl', ['--context', CONTEXT, 'create', 'namespace', 'argocd']);
  run('kubectl', ['--context', CONTEXT, 'apply', '-n', 'argocd', '--server-side', '--force-conflicts', '-f', ARGOCD_MANIFEST]);
  run('kubectl', ['--context', CONTEXT, '-n', 'argocd', 'wait', '--for=condition=available', 'deployment', '--all', '--timeout=300s']);
  check(true, 'every Argo CD deployment available');
  if (!existsSync(DEPLOY_KEY)) refuse(`no deploy key at ${DEPLOY_KEY}: the read-only key kind's Argo CD reads the repository with (infra/argocd/bootstrap.mjs says how it was made)`);
  const rootApp = JSON.parse(readFileSync(join(ROOT, 'infra', 'argocd', 'eks', 'root.json'), 'utf8'));
  kubectl(['apply', '-f', '-'], JSON.stringify({
    apiVersion: 'v1',
    kind: 'Secret',
    metadata: { name: 'repo-eventhub', namespace: 'argocd', labels: { 'argocd.argoproj.io/secret-type': 'repository' } },
    type: 'Opaque',
    stringData: { type: 'git', url: rootApp.spec.source.repoURL, sshPrivateKey: readFileSync(DEPLOY_KEY, 'utf8') },
  }));
  check(true, `${rootApp.spec.source.repoURL}, read with the deploy key at ${DEPLOY_KEY}`);
  // Before the root: its first sync is the fresh install, and setup's wave must already have
  // something to wait on.
  const health = installApplicationHealth(kubectl);
  check(health.ok, `argocd-cm: the Application health check the root's waves wait on (${health.why})`);
  if (failed) return;
  kubectl(['apply', '-f', join('infra', 'argocd', 'eks', 'root.json')]);
  check(true, `root -> ${rootApp.spec.source.path} at ${rootApp.spec.source.targetRevision}: setup first, then everything else`);

  const want = gitopsApps(charts);
  step(`7. ${want.length} Applications, each Synced and Healthy at ${mainSha.slice(0, 7)}`);
  const deadline = Date.now() + 20 * 60_000;
  let last = '';
  for (;;) {
    const apps = Object.fromEntries(JSON.parse(kubectl(['-n', 'argocd', 'get', 'applications', '-o', 'json'])).items.map((a) => [a.metadata.name, a]));
    const state = (n) => `${apps[n]?.status?.sync?.status ?? 'absent'}/${apps[n]?.status?.health?.status ?? '-'}`;
    // A root whose sync ended will not try the same commit again, and what it waits on, the
    // rest, will never come: say why now, not in twenty minutes. Setup's Job failing is the
    // likely one, and its own Application says how. Only for this commit: an earlier commit's
    // failure is history, and the root tries a new commit afresh.
    const rootOp = apps.root?.status?.operationState;
    const rootTried = rootOp?.syncResult?.revision ?? rootOp?.operation?.sync?.revision;
    if (rootOp && (rootOp.phase === 'Failed' || rootOp.phase === 'Error') && rootTried === mainSha) {
      check(false, `the root's sync of ${mainSha.slice(0, 7)} ended ${rootOp.phase}: ${rootOp.message} | setup: ${state('setup')}, ${apps.setup?.status?.health?.message ?? ''}`.slice(0, 1500));
      return;
    }
    const notYet = want.filter((n) => state(n) !== 'Synced/Healthy');
    const line = notYet.length ? `waiting on ${notYet.map((n) => `${n}=${state(n)}`).join(' ')}` : 'all Synced and Healthy';
    if (line !== last) {
      console.log(`    ...   ${line}`);
      last = line;
    }
    if (notYet.length === 0) {
      const elsewhere = want.filter((n) => apps[n].status?.sync?.revision !== mainSha);
      check(elsewhere.length === 0, `every one at ${mainSha.slice(0, 7)}${elsewhere.length ? ` -- main has moved: ${elsewhere.map((n) => `${n}@${String(apps[n].status?.sync?.revision).slice(0, 7)}`).join(', ')}` : ''}`);
      break;
    }
    if (Date.now() > deadline) {
      const why = notYet.map((n) => `${n}: ${(apps[n]?.status?.conditions ?? []).map((c) => c.message).join('; ') || state(n)}`).join(' | ');
      check(false, `not Synced and Healthy in 20 minutes: ${why.slice(0, 1500)}`);
      return;
    }
    await new Promise((r) => setTimeout(r, 10_000));
  }

  checkRunning({ compose, charts, image: images.find((i) => i.name === 'eventhub/app').ref, CONTEXT, sha: mainSha.slice(0, 7) });
}

/**
 * Steps 3 and 4, the same either way: this cluster's credentials, read back or made, then
 * every Secret with them in place, then the stores. Returns compose's resolved configuration.
 */
function credentialsAndStores({ kubectl, helmArgs, charts, CONTEXT }) {
  step("3. this cluster's credentials, and every Secret with them in place");
  const compose = JSON.parse(run('docker', ['compose', '-p', 'eventhub-rebuild', 'config', '--format', 'json']));
  const held = tryRun('kubectl', ['--context', CONTEXT, '--namespace', 'default', 'get', 'secret', CREDENTIALS_SECRET, '-o', 'json']);
  if (held.status !== 0 && !/NotFound/.test(held.stderr)) refuse(`could not read ${CREDENTIALS_SECRET}: ${held.stderr.trim().split('\n').pop()}`);
  const existing = held.status === 0 ? Object.fromEntries(Object.entries(JSON.parse(held.stdout).data ?? {}).map(([k, v]) => [k, Buffer.from(v, 'base64').toString('utf8')])) : {};
  // A store that has started once keeps the passwords its first boot set, and those were
  // not this run's to know. New credentials would lock every service out of its database.
  if (held.status !== 0 && tryRun('helm', ['status', 'stores', ...helmArgs]).status === 0) {
    refuse(`infra/stores is installed and ${CREDENTIALS_SECRET} is not: the stores' disks hold credentials this run cannot know. Take EventHub off with --down, then install again.`);
  }
  const install = installSecrets(compose, charts, renderChart(join(ROOT, 'infra', 'stores')), existing);
  // Kept first, before anything is made from them: a run that fails after this point
  // leaves credentials the next run reads back, never a store set up with ones now lost.
  if (install.made.length) kubectl(['--namespace', 'default', 'apply', '-f', '-'], JSON.stringify(secretOf(CREDENTIALS_SECRET, install.creds)));
  check(true, `${CREDENTIALS_SECRET}: ${Object.keys(existing).length} kept, ${install.made.length} made, for ${install.found.length} credential(s) compose had in place`);
  const items = Object.entries(install.secrets).map(([name, data]) => secretOf(name, data));
  kubectl(['--namespace', 'default', 'apply', '-f', '-'], JSON.stringify({ apiVersion: 'v1', kind: 'List', items }));
  for (const [name, data] of Object.entries(install.secrets)) console.log(`    ok    ${name}: ${Object.keys(data).length} key(s) from ${install.from[name]}`);

  step('4. the stores');
  run('helm', ['upgrade', '--install', 'stores', join('infra', 'stores'), ...helmArgs, '--wait', '--timeout', '15m'], { stdio: 'inherit' });
  check(true, 'infra/stores installed, every store ready');
  return compose;
}

const secretOf = (name, data, type = 'Opaque') => ({ apiVersion: 'v1', kind: 'Secret', metadata: { name, labels: { 'app.kubernetes.io/part-of': 'eventhub' } }, type, stringData: data });

/** The last steps, the same either way: every /ready, and requests through the gateway and to the web app. */
function checkRunning({ compose, charts, image, CONTEXT, sha }) {
  step('/ready from inside the cluster');
  for (const chart of charts) {
    const port = compose.services[chart]?.environment?.PORT;
    if (!port) continue;
    const r = runInCluster(CONTEXT, image, `ready-${chart}-${Date.now().toString(36)}`,
      `fetch('http://${chart}:${port}/ready').then(async r => { console.log(r.status, await r.text()); process.exit(r.ok ? 0 : 1) }, e => { console.log('unreachable', e.cause?.code ?? e.message); process.exit(1) })`);
    // The answer itself, not only the exit code (infra/local/up.mjs says why).
    const answer = r.stdout.trim();
    check(r.status === 0 && /^200 /.test(answer), `${chart}: ${answer || r.stderr.trim() || `(no answer, exit ${r.status})`}`);
  }

  step('through the gateway, and to the web app, from inside the cluster');
  for (const [name, script, judge] of [['gateway', gatewayProbe(), judgeGateway], ['web', webProbe(), judgeWeb]]) {
    const p = runInCluster(CONTEXT, image, `${name}-${Date.now().toString(36)}`, script);
    let answers = null;
    try {
      answers = JSON.parse(p.stdout.trim().split('\n').pop());
    } catch {
      check(false, `the ${name} probe gave no answers: ${(p.stdout || p.stderr).trim().slice(-300) || `exit ${p.status}`}`);
    }
    if (answers) for (const [ok, msg] of judge(answers)) check(ok, msg);
  }
  // The address is this machine's own until a domain is set (infra/eks/public.mjs): then the
  // two port-forwards are the way in, onto the ports the address names.
  const { domain, port } = publicAddress(APP_URL_TEMPLATE);
  const local = domain === DEV_DOMAIN
    ? `To open it from here, stop compose's Kong (docker stop eventhub-kong: it holds ${GATEWAY_PORT}), then:\n  kubectl --context ${CONTEXT} port-forward svc/kong ${GATEWAY_PORT}:8000\n  kubectl --context ${CONTEXT} port-forward svc/web ${port || 80}:3000\n  and `
    : 'At ';
  console.log(failed ? `\nFAIL: ${failed} step(s) failed` : `\nEventHub runs on ${CONTEXT}, from ${sha}. ${local}${openAt(APP_URL_TEMPLATE)}`);
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
function down({ kubectl, helmArgs, charts, CONTEXT, REGION }) {
  // Argo CD first, when it runs here: with self-heal on, it puts back whatever is taken off.
  // Deleting the root deletes every Application under it, and each its resources (their
  // finalizers), so the services go before the stores they talk to.
  if (tryRun('kubectl', ['--context', CONTEXT, '-n', 'argocd', 'get', 'application', 'root']).status === 0) {
    step("2a. Argo CD's Applications, and then Argo CD");
    kubectl(['-n', 'argocd', 'delete', 'application', 'root', '--wait=false']);
    let left = null;
    for (let i = 0; i < 90; i += 1) {
      left = JSON.parse(kubectl(['-n', 'argocd', 'get', 'applications', '-o', 'json'])).items.map((a) => a.metadata.name);
      if (left.length === 0) break;
      spawnSync(process.execPath, ['-e', 'setTimeout(() => {}, 5000)']);
    }
    check(left?.length === 0, `every Application deleted, with what it deployed${left?.length ? ` -- left: ${left.join(', ')}` : ''}`);
    const ns = tryRun('kubectl', ['--context', CONTEXT, 'delete', 'namespace', 'argocd', '--wait=true', '--timeout=5m']);
    check(ns.status === 0, `Argo CD removed${ns.status === 0 ? '' : `: ${ns.stderr.trim().split('\n').pop()}`}`);
  }

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
  main().catch((e) => {
    console.error(`FAIL: ${e instanceof Refusal ? e.message : (e.stderr || e.message || String(e)).toString().trim().slice(-1500)}`);
    process.exit(1);
  });
}
