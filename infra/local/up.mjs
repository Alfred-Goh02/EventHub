/**
 * Runs the services on the local kind cluster, against the data stores compose runs.
 *
 *   node infra/local/up.mjs            every chart under charts/
 *   node infra/local/up.mjs auth event just those
 *
 * Needs: the compose stack up (`docker compose up -d`), kind, kubectl and helm on PATH,
 * and the image compose builds (eventhub/app:local).
 *
 * WHAT IT DOES, in order, each step idempotent:
 *   1. creates the kind cluster from infra/local/kind-cluster.yaml if it is not there;
 *   2. reads compose's resolved configuration, and checks each node is on kind's
 *      network alone;
 *   3. joins each data store compose runs to kind's network, and gives every host the
 *      services' environment names -- postgres, kafka, schema-registry, ... -- an
 *      ExternalName Service naming that compose container, so the address is looked up
 *      when it is used, not written down when this runs (infra/local/bridge.mjs says why).
 *      A pod reaches `postgres:5432` here as it will reach the RDS endpoint on EKS, and
 *      the environment is the same string in both places;
 *   4. puts Auth's keypair in the `jwt-keys` Secret, from services/auth/keys;
 *   5. loads the image into the nodes;
 *   6. asks, from a pod, where every bridge leads, and fails naming any host that does not
 *      reach its container (scripts/verify-kind-bridge.mjs asks the same, any time);
 *   7. installs each chart with its environment taken from compose's OWN resolved
 *      configuration (`docker compose config`), never a hand-written copy -- a copy falls
 *      behind the list it copies (CLAUDE.md, "Derive a list from its source");
 *   8. waits for each rollout, then asks every service's /ready from inside the cluster.
 *
 * A host that is itself one of the charts being installed gets no bridge: inside the
 * cluster, `auth` means the auth Service.
 */

import { execFileSync, spawnSync } from 'node:child_process';
import { existsSync, mkdtempSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { addressOnKind, allCharts, bridgesFor, composeConfig, inspectBridges, NETWORK, SELECTOR, serviceManifests } from './bridge.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');
const CLUSTER = 'eventhub';
const CONTEXT = `kind-${CLUSTER}`;
// The tag to run: compose's own build by default, or one named for the code it holds
// (EVENTHUB_IMAGE_TAG=<git sha>), so trying a branch never overwrites what compose runs.
const TAG = process.env['EVENTHUB_IMAGE_TAG'] || 'local';
const IMAGE = `eventhub/app:${TAG}`;

const run = (cmd, args, opts = {}) => execFileSync(cmd, args, { cwd: ROOT, encoding: 'utf8', stdio: ['pipe', 'pipe', 'pipe'], ...opts });
const kubectl = (args, input) => run('kubectl', ['--context', CONTEXT, ...args], input === undefined ? {} : { input });
const step = (msg) => console.log(`\n  ${msg}`);

function fail(msg) {
  console.error(`FAIL: ${msg}`);
  process.exit(1);
}

const args = process.argv.slice(2).filter((a) => a !== '--');
/**
 * --gitops: prepare the cluster for ArgoCD, which installs the charts from git. Every step
 * up to the image runs as usual; then, instead of installing, each chart's environment
 * goes into the Secret `<name>-env` the chart names -- passwords are not in git, so ArgoCD
 * cannot carry them -- and any release this script installed itself is removed, so that
 * ArgoCD alone owns what runs. infra/argocd/bootstrap.mjs runs this first.
 */
const GITOPS = args.includes('--gitops');
const wanted = args.filter((a) => !a.startsWith('--'));
const charts = wanted.length ? wanted : allCharts(ROOT);
for (const c of charts) if (!existsSync(join(ROOT, 'charts', c, 'Chart.yaml'))) fail(`no chart at charts/${c}`);

step('1. the kind cluster');
const clusters = run('kind', ['get', 'clusters']).split(/\r?\n/).filter(Boolean);
if (!clusters.includes(CLUSTER)) {
  run('kind', ['create', 'cluster', '--config', join('infra', 'local', 'kind-cluster.yaml')], { stdio: 'inherit' });
} else {
  console.log(`    ok    ${CONTEXT} exists`);
}
const nodes = run('kind', ['get', 'nodes', '--name', CLUSTER]).split(/\r?\n/).filter(Boolean);

step("2. compose's configuration");
const services = composeConfig(ROOT).services;
/**
 * THE DATA STORES JOIN KIND'S NETWORK; THE NODES NEVER JOIN COMPOSE'S. The first version
 * did it the other way round, and after Docker's VM restarted two nodes came back
 * registered under their compose-network address and one under kind's: traffic between
 * nodes broke, and with it DNS, so a pod on one worker could not resolve `postgres`
 * while the same pod on another could (measured: EAI_AGAIN on worker2 only). A node
 * with one network has one address. A container compose recreates loses the extra
 * network; this step puts it back.
 */
console.log(`    ok    ${Object.keys(services).length} compose service(s); the data stores join the ${NETWORK} network`);
for (const node of nodes) {
  const attached = Object.keys(JSON.parse(run('docker', ['inspect', node, '--format', '{{json .NetworkSettings.Networks}}'])));
  if (attached.length !== 1 || attached[0] !== NETWORK) {
    fail(`${node} is on ${attached.join(', ')}; a node must be on ${NETWORK} alone (docker network disconnect the others and restart it)`);
  }
}

step('3. a Service for every compose host the environment names');
for (const chart of charts) if (!services[chart]) fail(`charts/${chart} has no compose service of the same name to take its environment from`);
const bridges = bridgesFor(charts, services);
for (const [host, { container, ports }] of bridges) {
  const r = spawnSync('docker', ['network', 'connect', NETWORK, container], { encoding: 'utf8' });
  if (r.status !== 0 && !/already exists/.test(r.stderr)) fail(`could not join ${container} to ${NETWORK}: ${r.stderr.trim()}`);
  const ip = addressOnKind(container);
  if (!ip) fail(`${container} has no address on ${NETWORK}; is it running?`);
  console.log(`    ok    ${host} -> ExternalName ${container}, at ${ip} now, port(s) ${ports.join(',')}`);
}
if (bridges.size) kubectl(['apply', '-f', '-'], JSON.stringify({ apiVersion: 'v1', kind: 'List', items: serviceManifests(bridges) }));
// The EndpointSlices the first version wrote, each holding an address as it was when this
// last ran. An ExternalName Service ignores them, so they mislead only whoever reads them,
// and they go. Counted, so a delete that matched nothing says so -- counting the names it
// printed, not its lines: with nothing to delete it prints "No resources found", which a
// line count called one.
const removed = kubectl(['delete', 'endpointslices', '-l', SELECTOR, '--ignore-not-found', '-o', 'name']).split(/\r?\n/).filter((l) => /^endpointslice[\w.]*\/\S+$/.test(l));
console.log(`    ok    ${removed.length} EndpointSlice(s) holding a written-down address removed${removed.length ? `: ${removed.join(', ')}` : ''}`);

step("4. Auth's keys");
// EVERY FILE IN THE DIRECTORY, not a list of the ones known today. The first version
// named private.pem and public.pem, and Auth refused to start: it also reads
// Notification's public key from there, to seal the emails it asks for. Compose mounts
// the whole directory; so does this.
const keys = join(ROOT, 'services', 'auth', 'keys');
const pems = existsSync(keys) ? readdirSync(keys).filter((f) => f.endsWith('.pem')).sort() : [];
for (const need of ['private.pem', 'public.pem']) {
  if (!pems.includes(need)) fail(`services/auth/keys has no ${need}; run setup (docker compose up) or generate the keys`);
}
const secret = kubectl(['create', 'secret', 'generic', 'jwt-keys', ...pems.map((f) => `--from-file=${f}=${join(keys, f)}`), '--dry-run=client', '-o', 'json']);
kubectl(['apply', '-f', '-'], secret);
console.log(`    ok    jwt-keys: ${pems.join(', ')}`);

// Notification's own keypair, which only Notification mounts. Auth holds its public half
// as notification-public.pem; a different keypair here and every email request Auth
// seals is one Notification cannot open.
if (charts.includes('notification')) {
  const nkeys = join(ROOT, 'services', 'notification', 'keys');
  const npems = existsSync(nkeys) ? readdirSync(nkeys).filter((f) => f.endsWith('.pem')).sort() : [];
  if (!npems.includes('private.pem')) fail('services/notification/keys has no private.pem; run setup (docker compose up)');
  if (npems.includes('public.pem') && readFileSync(join(nkeys, 'public.pem'), 'utf8') !== readFileSync(join(keys, 'notification-public.pem'), 'utf8')) {
    fail("services/notification/keys/public.pem is not the notification-public.pem Auth holds: Auth would seal requests Notification cannot open");
  }
  const nsecret = kubectl(['create', 'secret', 'generic', 'notification-keys', ...npems.map((f) => `--from-file=${f}=${join(nkeys, f)}`), '--dry-run=client', '-o', 'json']);
  kubectl(['apply', '-f', '-'], nsecret);
  console.log(`    ok    notification-keys: ${npems.join(', ')}, its public half the one Auth seals to`);
}

step('5. the image');
run('kind', ['load', 'docker-image', IMAGE, '--name', CLUSTER], { stdio: 'inherit' });

step('6. where every bridge leads, asked from a pod');
const findings = inspectBridges({ context: CONTEXT, image: IMAGE, bridges });
for (const { ok, msg } of findings) console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
const wrong = findings.filter((f) => !f.ok);
if (wrong.length) fail(`${wrong.length} bridge check(s) failed; the services would start against the wrong address or none`);

const envOf = (chart) =>
  Object.fromEntries(Object.entries(services[chart].environment ?? {}).map(([k, v]) => [k, v === null ? '' : String(v)]));

if (GITOPS) {
  step('7. each chart\'s environment, for ArgoCD to find');
  for (const chart of charts) {
    // THE RELEASE GOES FIRST. A release this script installed owns a Secret of the same
    // name, `<chart>-env`, and uninstalling it deletes that Secret. Done the other way
    // round, the first run made each Secret and then lost it: every pod ArgoCD started
    // sat in CreateContainerConfigError, "secret auth-env not found", for 15 minutes.
    const owned = spawnSync('helm', ['status', chart, '--kube-context', CONTEXT], { encoding: 'utf8' }).status === 0;
    if (owned) run('helm', ['uninstall', chart, '--kube-context', CONTEXT, '--wait']);
    const env = envOf(chart);
    const s = {
      apiVersion: 'v1',
      kind: 'Secret',
      metadata: { name: `${chart}-env`, labels: { 'app.kubernetes.io/name': chart, 'app.kubernetes.io/part-of': 'eventhub' } },
      type: 'Opaque',
      stringData: env,
    };
    kubectl(['apply', '-f', '-'], JSON.stringify(s));
    console.log(`    ok    ${chart}-env: ${Object.keys(env).length} variable(s) from compose${owned ? '; the Helm release this script made is removed, so ArgoCD owns it' : ''}`);
  }
  console.log('\nthe cluster is ready for ArgoCD: node infra/argocd/bootstrap.mjs installs the charts from git');
  process.exit(0);
}

step('7. the charts');
const scratch = mkdtempSync(join(tmpdir(), 'eventhub-up-'));
for (const chart of charts) {
  const env = envOf(chart);
  const valuesFile = join(scratch, `${chart}.json`);
  writeFileSync(valuesFile, JSON.stringify({ env }));
  run('helm', ['dependency', 'build', join('charts', chart)]);
  run('helm', ['upgrade', '--install', chart, join('charts', chart), '--kube-context', CONTEXT, '-f', valuesFile, '--set', `image.tag=${TAG}`, '--wait', '--timeout', '5m'], { stdio: 'inherit' });
  console.log(`    ok    ${chart} installed, ${Object.keys(env).length} variable(s) from compose`);
}

step('8. /ready from inside the cluster');
let ready = true;
for (const chart of charts) {
  const port = services[chart].environment?.PORT;
  if (!port) continue;
  const name = `ready-${chart}-${Date.now().toString(36)}`;
  const r = spawnSync(
    'kubectl',
    ['--context', CONTEXT, 'run', name, '--rm', '-i', '--restart=Never', `--image=${IMAGE}`, '--image-pull-policy=Never', '--quiet', '--', 'node', '-e',
      `fetch('http://${chart}:${port}/ready').then(async r => { console.log(r.status, await r.text()); process.exit(r.ok ? 0 : 1) }, e => { console.log('unreachable', e.cause?.code ?? e.message); process.exit(1) })`],
    { encoding: 'utf8' },
  );
  // The answer itself, not only the exit code: one run printed "ok event:" with nothing
  // after it, which a zero exit alone would have called ready.
  const answer = r.stdout.trim();
  const ok = r.status === 0 && /^200 /.test(answer);
  ready &&= ok;
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${chart}: ${answer || r.stderr.trim() || `(no answer, exit ${r.status})`}`);
}
if (!ready) process.exitCode = 1;
console.log(ready ? '\nevery service answers ready from inside the cluster' : '\nFAIL: a service is not ready');
