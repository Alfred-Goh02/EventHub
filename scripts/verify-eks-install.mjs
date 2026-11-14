/**
 * Asserts that infra/eks/up.mjs installs on EKS everything the charts need, from the
 * sources kind uses, and that setup there is compose's own setup.
 *
 * NOTHING HERE TALKS TO A CLUSTER. It renders every chart -- the fifteen services, the
 * stores, setup and the gateway -- and holds what they read against what the installer
 * makes (kong-env is compose's environment for Kong, variable for variable):
 *   - every Secret a pod reads (envFrom, a Secret volume and its items, a secretKeyRef) is
 *     one the installer or infra/stores/secrets.mjs makes, with every key read; and every
 *     Secret the installer makes is read by something, so neither list can rot;
 *   - the setup Job runs exactly compose's setup command, with compose's environment for
 *     it, once (backoffLimit 0, restartPolicy Never), as the image's own non-root user;
 *   - every directory compose's setup mounts a volume at is a writable emptyDir in the
 *     Job, and a directory whose volume is named like a key Secret is seeded from that
 *     Secret before setup starts -- so setup finds the keypairs and keeps them;
 *   - the key Secrets come from the directories infra/local/up.mjs reads for kind, so both
 *     clusters hold the same keypairs; and the installer refuses a missing half, or a
 *     Notification public key that is not the one Auth seals to;
 *   - .dockerignore keeps the agent's worktrees, keys, env files and Terraform out of the
 *     image the installer pushes.
 *
 * The keys are a fixture: what matters is their names, and a fixture can also be broken on
 * purpose to watch the installer refuse.
 *   node scripts/verify-eks-install.mjs          against this tree
 *   node scripts/verify-eks-install.mjs [root]   against another (a red run in a copy)
 */

import { spawnSync } from 'node:child_process';
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}
const section = (title) => {
  console.log('');
  console.log(`  ${title}`);
};

/** A chart rendered as JSON objects, or the error text. */
function render(chartDir, args) {
  const r = spawnSync('helm', ['template', 'v', chartDir, '--set', 'render=json', ...args], { encoding: 'utf8', maxBuffer: 64 << 20 });
  if (r.status !== 0) return { error: (r.stderr || r.stdout || '').trim() };
  const docs = r.stdout
    .split(/^---\s*$/m)
    .map((d) => d.split(/\r?\n/).filter((l) => !l.startsWith('# Source:')).join('\n').trim())
    .filter(Boolean)
    .map((d) => JSON.parse(d));
  return { docs };
}

/** What a set of rendered objects reads from Secrets: { secret: Set(keys) }, '*' for "all of it". */
function secretReads(docs) {
  const reads = {};
  const want = (name, key) => {
    reads[name] ??= new Set();
    reads[name].add(key);
  };
  for (const d of docs) {
    const pod = d.spec?.template?.spec;
    if (!pod) continue;
    for (const c of [...(pod.initContainers ?? []), ...(pod.containers ?? [])]) {
      for (const f of c.envFrom ?? []) if (f.secretRef) want(f.secretRef.name, '*');
      for (const e of c.env ?? []) if (e.valueFrom?.secretKeyRef) want(e.valueFrom.secretKeyRef.name, e.valueFrom.secretKeyRef.key);
    }
    for (const v of pod.volumes ?? []) {
      if (!v.secret) continue;
      if (v.secret.items?.length) for (const it of v.secret.items) want(v.secret.secretName, it.key);
      else want(v.secret.secretName, '*');
    }
  }
  return reads;
}

/** Keys that look like keypairs, for a fixture root: names are what is checked, not content. */
function keyFixture(break_ = {}) {
  const dir = mkdtempSync(join(tmpdir(), 'verify-eks-keys-'));
  const write = (rel, text) => {
    mkdirSync(dirname(join(dir, rel)), { recursive: true });
    writeFileSync(join(dir, rel), text);
  };
  const notifPublic = break_.mismatch ? 'NOT THE SAME KEY' : 'notification public half';
  if (!break_.noAuthPrivate) write('services/auth/keys/private.pem', 'auth private half');
  write('services/auth/keys/public.pem', 'auth public half');
  write('services/auth/keys/notification-public.pem', 'notification public half');
  write('services/notification/keys/private.pem', 'notification private half');
  if (!break_.noNotifPublic) write('services/notification/keys/public.pem', notifPublic);
  return dir;
}

async function main() {
  const rootArg = process.argv.slice(2).find((a) => a !== '--');
  const root = rootArg ? resolve(rootArg) : ROOT;
  console.log(`  reading ${rootArg ? root : 'this tree'}`);
  const eks = await import(pathToFileURL(join(root, 'infra', 'eks', 'up.mjs')).href);
  const stores = await import(pathToFileURL(join(root, 'infra', 'stores', 'secrets.mjs')).href);

  const composeRun = spawnSync('docker', ['compose', '-f', join(root, 'docker-compose.yml'), '-p', 'eventhub-rebuild', 'config', '--format', 'json'], { encoding: 'utf8', maxBuffer: 64 << 20 });
  if (composeRun.status !== 0) {
    check(false, `docker compose config: ${(composeRun.stderr || '').trim().slice(-400)}`);
    return;
  }
  const compose = JSON.parse(composeRun.stdout);
  const charts = eks.allCharts(root);

  // -------------------------------------------------------------------------
  section('the installer refuses keys that would break the cluster');
  const refusals = [
    ['noAuthPrivate', /no private\.pem/],
    ['noNotifPublic', /no public\.pem/],
    ['mismatch', /not the notification-public\.pem Auth holds/],
  ];
  for (const [what, expect] of refusals) {
    const dir = keyFixture({ [what]: true });
    let msg = null;
    try {
      eks.appSecrets(compose, charts, dir);
    } catch (e) {
      msg = e.message;
    } finally {
      rmSync(dir, { recursive: true, force: true });
    }
    check(msg !== null && expect.test(msg), `with ${what}, appSecrets refuses (${msg ?? 'it did not refuse'})`);
  }

  // -------------------------------------------------------------------------
  section('every Secret a pod reads is one the installer makes, and every one it makes is read');
  const keys = keyFixture();
  let made;
  try {
    made = eks.appSecrets(compose, charts, keys);
  } finally {
    rmSync(keys, { recursive: true, force: true });
  }
  const storeDocs = stores.renderChart(join(root, 'infra', 'stores'));
  const storeMade = stores.plan(storeDocs, compose);
  const produced = {};
  for (const [name, s] of Object.entries(made)) produced[name] = new Set(Object.keys(s.data));
  for (const [name, k] of Object.entries(storeMade)) produced[name] = new Set(Object.keys(k));

  const cmdFile = join(mkdtempSync(join(tmpdir(), 'verify-eks-')), 'setup.json');
  writeFileSync(cmdFile, JSON.stringify({ image: { repository: 'registry.example/eventhub/app', tag: 'abc1234' }, command: eks.setupCommand(compose) }));
  const setup = render(join(root, 'infra', 'setup'), ['-f', cmdFile]);
  rmSync(dirname(cmdFile), { recursive: true, force: true });
  if (setup.error) {
    check(false, `infra/setup did not render: ${setup.error.slice(-400)}`);
    return;
  }
  const reads = {};
  const merge = (r, who) => {
    for (const [name, ks] of Object.entries(r)) {
      reads[name] ??= { keys: new Set(), by: new Set() };
      for (const k of ks) reads[name].keys.add(k);
      reads[name].by.add(who);
    }
  };
  let rendered = 0;
  for (const chart of charts) {
    // The library chart is a dependency, fetched into the chart as verify-helm-charts does.
    const dep = spawnSync('helm', ['dependency', 'build', join(root, 'charts', chart)], { encoding: 'utf8' });
    if (dep.status !== 0) {
      check(false, `charts/${chart}: helm dependency build failed: ${(dep.stderr || '').trim().slice(-300)}`);
      continue;
    }
    const r = render(join(root, 'charts', chart), ['--set', 'image.tag=abc1234']);
    if (r.error) {
      check(false, `charts/${chart} did not render: ${r.error.slice(-300)}`);
      continue;
    }
    rendered += 1;
    merge(secretReads(r.docs), chart);
  }
  merge(secretReads(storeDocs), 'stores');
  merge(secretReads(setup.docs), 'setup');
  // The gateway: compose's environment for Kong as kong-env, and Auth's public key.
  const gateway = render(join(root, 'infra', 'kong'), []);
  if (gateway.error) {
    check(false, `infra/kong did not render: ${gateway.error.slice(-400)}`);
    return;
  }
  merge(secretReads(gateway.docs), 'kong');
  check(rendered === charts.length && charts.length > 0 && Object.keys(reads).length > 0, `CONTROL: ${rendered} of ${charts.length} chart(s) rendered, plus stores, setup and the gateway; ${Object.keys(reads).length} Secret(s) read`);
  check(JSON.stringify(made['kong-env']?.data) === JSON.stringify(eks.envOf(compose, 'kong')), `kong-env is compose's resolved environment for Kong (${Object.keys(made['kong-env']?.data ?? {}).length} variable(s))`);
  for (const [name, r] of Object.entries(reads).sort()) {
    const have = produced[name];
    const missing = [...r.keys].filter((k) => k !== '*' && !have?.has(k));
    check(have !== undefined && missing.length === 0, `${name}, read by ${[...r.by].join(', ')}: made${have ? ` with ${have.size} key(s)` : ' by nothing'}${missing.length ? ` -- missing ${missing.join(', ')}` : ''}`);
  }
  const unread = Object.keys(made).filter((n) => !reads[n]);
  check(unread.length === 0, `every Secret the installer makes is read by some pod${unread.length ? ` -- unread: ${unread.join(', ')}` : ''}`);

  // -------------------------------------------------------------------------
  section("setup on EKS is compose's setup");
  const job = setup.docs.find((d) => d.kind === 'Job');
  const pod = job?.spec?.template?.spec;
  const main_ = pod?.containers?.find((c) => c.name === 'setup');
  check(job !== undefined && main_ !== undefined, `CONTROL: infra/setup renders a Job with a setup container (${job ? job.metadata.name : 'no Job'})`);
  if (!job || !main_) return;
  const composeSetup = compose.services.setup;
  check(JSON.stringify(main_.command) === JSON.stringify(composeSetup.command), `the Job runs exactly compose's setup command (${main_.command?.length ?? 0} argument(s), ${String(main_.command?.at(-1) ?? '').split('\n').filter((l) => l.trim().startsWith('echo "setup:')).length} setup step(s))`);
  check((main_.envFrom ?? []).some((f) => f.secretRef?.name === 'setup-env') && made['setup-env'] !== undefined, "with compose's environment for setup, from setup-env");
  check(JSON.stringify(made['setup-env']?.data) === JSON.stringify(eks.envOf(compose, 'setup')), `setup-env is compose's resolved setup environment (${Object.keys(made['setup-env']?.data ?? {}).length} variable(s))`);
  check(job.spec.backoffLimit === 0 && pod.restartPolicy === 'Never', `once, as compose's restart: "no" runs it: backoffLimit ${job.spec.backoffLimit}, restartPolicy ${pod.restartPolicy}`);
  check(pod.securityContext?.runAsNonRoot === true && pod.securityContext?.runAsUser === 1000, `as the image's own user, node (runAsNonRoot ${pod.securityContext?.runAsNonRoot}, uid ${pod.securityContext?.runAsUser})`);
  check(main_.workingDir === '/app', `in /app, the image's working directory (${main_.workingDir})`);

  const volumes = (composeSetup.volumes ?? []).filter((v) => v.type === 'volume');
  check(volumes.length > 0, `CONTROL: compose's setup mounts ${volumes.length} volume(s): ${volumes.map((v) => `${v.source} at ${v.target}`).join(', ')}`);
  const seed = pod.initContainers?.find((c) => c.name === 'seed-keys');
  const seedScript = String(seed?.command?.at(-1) ?? '');
  for (const v of volumes) {
    const mount = (main_.volumeMounts ?? []).find((m) => m.mountPath === v.target);
    const vol = (pod.volumes ?? []).find((x) => x.name === mount?.name);
    check(Boolean(vol?.emptyDir) && !mount?.readOnly, `${v.target} (compose's ${v.source}) is a writable emptyDir in the Job${vol ? (vol.emptyDir ? '' : ' -- not an emptyDir') : ' -- not mounted'}`);
    if (produced[v.source]) {
      const secretMount = (seed?.volumeMounts ?? []).find((m) => (pod.volumes ?? []).find((x) => x.name === m.name)?.secret?.secretName === v.source);
      const seeded = secretMount && (seed.volumeMounts ?? []).some((m) => m.name === mount?.name && m.mountPath === v.target) && seedScript.includes(`cp -L ${secretMount.mountPath}/* ${v.target}/`);
      check(Boolean(seeded), `${v.target} is seeded from Secret ${v.source} before setup starts, so setup keeps that keypair instead of making one`);
    }
  }

  // -------------------------------------------------------------------------
  section('the keys come from where kind takes them, and nothing of .claude goes into the image');
  const strip = (src) => src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/(^|[^:'"`])\/\/.*$/gm, '$1');
  const upKind = strip(readFileSync(join(root, 'infra', 'local', 'up.mjs'), 'utf8'));
  for (const [name, dir] of Object.entries(eks.KEY_DIRS)) {
    const parts = dir.split('/').map((p) => `'${p}'`).join(',\\s*');
    check(new RegExp(parts).test(upKind), `${name} comes from ${dir}, the directory infra/local/up.mjs reads for kind`);
  }
  const ignore = readFileSync(join(root, '.dockerignore'), 'utf8').split(/\r?\n/).map((l) => l.trim()).filter((l) => l && !l.startsWith('#'));
  for (const rule of ['.claude', '**/keys', '**/.env', '**/.env.*', 'infra/terraform']) {
    check(ignore.includes(rule), `.dockerignore has ${rule}, so the image pushed to ECR does not carry it`);
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    return;
  }
  console.log(`infra/eks/up.mjs makes every Secret the ${charts.length} services, the stores, setup and the gateway read, and runs compose's setup`);
}

main().catch((e) => {
  check(false, `the verifier itself failed: ${e.stack || e.message}`);
});
