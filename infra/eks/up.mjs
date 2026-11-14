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
 *     Secret `<chart>-env` its chart reads (the GitOps shape, as on kind);
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
 *   2. the image: built from HEAD as eventhub/app:<sha> if it is not here, and pushed to
 *      the ECR repository unless that tag is already there (tags are IMMUTABLE);
 *   3. the stores' Secrets, then infra/stores, waited for until every store is ready;
 *   4. the services' Secrets: keys, one environment per chart, and setup's;
 *   5. setup, as a Job named for the image, waited for until it has finished;
 *   6. every chart under charts/, on the ECR image, one replica each (pods, not memory,
 *      are this cluster's tight limit), each waited for;
 *   7. every service's /ready, asked from inside the cluster.
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

  step('2. the image, by the commit it is built from');
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
  const repo = run('aws', ['ecr', 'describe-repositories', '--region', REGION, '--repository-names', 'eventhub/app', '--query', 'repositories[0].repositoryUri', '--output', 'text']).trim();
  const local = `eventhub/app:${sha}`;
  const remote = `${repo}:${sha}`;
  const there = tryRun('aws', ['ecr', 'describe-images', '--region', REGION, '--repository-name', 'eventhub/app', '--image-ids', `imageTag=${sha}`, '--query', 'imageDetails[0].imageTags', '--output', 'text']);
  if (there.status === 0 && there.stdout.includes(sha)) {
    console.log(`    ok    ${remote} is already in ECR; tags are immutable, so it is the same image`);
  } else {
    if (tryRun('docker', ['image', 'inspect', local]).status !== 0) {
      console.log(`    ..    building ${local} from HEAD`);
      run('docker', ['build', '-t', local, '.'], { stdio: 'inherit' });
    }
    const password = run('aws', ['ecr', 'get-login-password', '--region', REGION]).trim();
    run('docker', ['login', '--username', 'AWS', '--password-stdin', repo.split('/')[0]], { input: password });
    run('docker', ['tag', local, remote]);
    run('docker', ['push', remote], { stdio: 'inherit' });
    check(true, `pushed ${remote}`);
  }

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

  step('7. /ready from inside the cluster');
  for (const chart of charts) {
    const port = compose.services[chart]?.environment?.PORT;
    if (!port) continue;
    const r = tryRun('kubectl', ['--context', CONTEXT, '--namespace', 'default', 'run', `ready-${chart}-${Date.now().toString(36)}`, '--rm', '-i', '--restart=Never', `--image=${remote}`, '--quiet', '--', 'node', '-e',
      `fetch('http://${chart}:${port}/ready').then(async r => { console.log(r.status, await r.text()); process.exit(r.ok ? 0 : 1) }, e => { console.log('unreachable', e.cause?.code ?? e.message); process.exit(1) })`]);
    // The answer itself, not only the exit code (infra/local/up.mjs says why).
    const answer = r.stdout.trim();
    check(r.status === 0 && /^200 /.test(answer), `${chart}: ${answer || r.stderr.trim() || `(no answer, exit ${r.status})`}`);
  }
  console.log(failed ? `\nFAIL: ${failed} step(s) failed` : `\nEventHub runs on ${CONTEXT}, from ${remote}`);
}

/**
 * Off again, in the order that leaves nothing billing: the services and setup first, then
 * the stores, whose StatefulSets take their claims with them and whose claims take their
 * volumes, while the EBS CSI driver still runs to delete them. Then AWS is asked: no volume
 * the driver made may be left.
 */
function down({ kubectl, helmArgs, charts, REGION }) {
  step('2. the releases, services and setup first, the stores last');
  const releases = new Set(JSON.parse(run('helm', ['list', ...helmArgs, '--output', 'json'])).map((r) => r.name));
  for (const name of [...charts, 'setup', 'stores'].filter((n) => releases.has(n))) {
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
