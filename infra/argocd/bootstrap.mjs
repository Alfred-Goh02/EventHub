/**
 * Puts ArgoCD in charge of the local kind cluster: from here, what is on `main` is what runs.
 *
 *   node infra/argocd/bootstrap.mjs
 *
 * Needs: the compose stack up, the kind cluster (infra/local/up.mjs makes it), and the
 * read-only deploy key at %USERPROFILE%\.eventhub\argocd-deploy-key, its public half added
 * to the repository's Deploy keys on GitHub. The key never enters the repository.
 *
 * IN ORDER, each step idempotent:
 *   1. `infra/local/up.mjs --gitops`: bridges, keys, image, and each chart's environment
 *      as a Secret, with no chart installed by Helm;
 *   2. ArgoCD itself, at the version pinned below, from its official manifest;
 *   3. the repository, as a credential Secret ArgoCD reads (the private key, from
 *      outside the repo);
 *   4. the root Application (infra/argocd/root.json), whose ApplicationSet generates one
 *      Application per chart under charts/;
 *   5. waits until there is one Application per service chart, each Synced and Healthy.
 *      Healthy means every Deployment is available: its readiness probes pass.
 */

import { execFileSync, spawnSync } from 'node:child_process';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

import { ARGOCD_MANIFEST, ARGOCD_VERSION, DEPLOY_KEY as KEY } from './settings.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const CONTEXT = 'kind-eventhub';

const run = (cmd, args, opts = {}) => execFileSync(cmd, args, { cwd: ROOT, encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'], ...opts });
const kubectl = (args, input) => run('kubectl', ['--context', CONTEXT, ...args], input === undefined ? {} : { input });
const step = (msg) => console.log(`\n  ${msg}`);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function fail(msg) {
  console.error(`FAIL: ${msg}`);
  process.exit(1);
}

const root = JSON.parse(readFileSync(join(ROOT, 'infra', 'argocd', 'root.json'), 'utf8'));
const repoURL = root.spec.source.repoURL;

/** The service charts the ApplicationSet should generate one Application for each. */
const serviceCharts = readdirSync(join(ROOT, 'charts'), { withFileTypes: true })
  .filter((d) => d.isDirectory() && existsSync(join(ROOT, 'charts', d.name, 'Chart.yaml')))
  .map((d) => d.name)
  .filter((n) => !/^type:\s*library\s*$/m.test(readFileSync(join(ROOT, 'charts', n, 'Chart.yaml'), 'utf8')))
  .sort();

if (!existsSync(KEY)) {
  fail(`no deploy key at ${KEY}. Make one with: ssh-keygen -t ed25519 -f "${KEY}" -N "" and add ${KEY}.pub to GitHub, repository Settings, Deploy keys, read-only`);
}

step('1. the cluster, prepared for ArgoCD');
const up = spawnSync(process.execPath, [join(ROOT, 'infra', 'local', 'up.mjs'), '--gitops'], { cwd: ROOT, encoding: 'utf8', stdio: 'inherit' });
if (up.status !== 0) fail('infra/local/up.mjs --gitops failed');

step(`2. ArgoCD ${ARGOCD_VERSION}`);
spawnSync('kubectl', ['--context', CONTEXT, 'create', 'namespace', 'argocd'], { encoding: 'utf8' });
run('kubectl', ['--context', CONTEXT, 'apply', '-n', 'argocd', '--server-side', '--force-conflicts', '-f', ARGOCD_MANIFEST]);
run('kubectl', ['--context', CONTEXT, '-n', 'argocd', 'wait', '--for=condition=available', 'deployment', '--all', '--timeout=300s']);
console.log('    ok    every ArgoCD deployment available');

step('3. the repository, read with the deploy key');
const repoSecret = {
  apiVersion: 'v1',
  kind: 'Secret',
  metadata: { name: 'repo-eventhub', namespace: 'argocd', labels: { 'argocd.argoproj.io/secret-type': 'repository' } },
  type: 'Opaque',
  stringData: { type: 'git', url: repoURL, sshPrivateKey: readFileSync(KEY, 'utf8') },
};
kubectl(['apply', '-f', '-'], JSON.stringify(repoSecret));
console.log(`    ok    ${repoURL}, with the key at ${KEY}`);

step('4. the root Application');
kubectl(['apply', '-f', join('infra', 'argocd', 'root.json')]);
console.log(`    ok    root -> ${root.spec.source.path} at ${root.spec.source.targetRevision}`);

step(`5. one Application per service chart (${serviceCharts.length}), each Synced and Healthy`);
const deadline = Date.now() + 15 * 60_000;
let last = '';
for (;;) {
  const apps = JSON.parse(kubectl(['-n', 'argocd', 'get', 'applications', '-o', 'json'])).items;
  const byName = Object.fromEntries(apps.map((a) => [a.metadata.name, a]));
  const rootApp = byName.root;
  const generated = serviceCharts.map((c) => byName[c]);
  const states = serviceCharts.map((c, i) => `${c}=${generated[i]?.status?.sync?.status ?? 'absent'}/${generated[i]?.status?.health?.status ?? '-'}`);
  const done = generated.every((a) => a?.status?.sync?.status === 'Synced' && a?.status?.health?.status === 'Healthy');
  const extra = apps.map((a) => a.metadata.name).filter((n) => n !== 'root' && !serviceCharts.includes(n));
  const line = `root=${rootApp?.status?.sync?.status ?? 'absent'}/${rootApp?.status?.health?.status ?? '-'} ${states.join(' ')}`;
  if (line !== last) {
    console.log(`    ...   ${line}`);
    last = line;
  }
  if (done) {
    if (extra.length) fail(`Applications no chart accounts for: ${extra.join(', ')}`);
    console.log(`    ok    ${serviceCharts.length} Applications, every one Synced and Healthy`);
    break;
  }
  if (Date.now() > deadline) {
    const bad = generated.filter((a) => a?.status?.health?.status !== 'Healthy').map((a) => a?.metadata?.name ?? '?');
    const why = bad[0] ? (byName[bad[0]]?.status?.conditions ?? []).map((c) => c.message).join('; ') : '';
    fail(`not every Application became Synced and Healthy in 15 minutes: ${bad.join(', ')}${why ? ` (${why})` : ''}`);
  }
  await sleep(10_000);
}
console.log('\nArgoCD runs the cluster from git: commit to main, and it follows');
