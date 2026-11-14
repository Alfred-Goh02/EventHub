/**
 * Asserts that infra/eks/up.mjs installs on EKS everything the charts need, from the
 * sources kind uses but with the cluster's own credentials, and that setup there is
 * compose's own setup.
 *
 * NOTHING HERE TALKS TO A CLUSTER. It runs the installer's own installSecrets() over compose,
 * renders every chart -- the fifteen services, the stores, setup and the gateway -- and holds
 * what they read against what the installer makes:
 *   - THE CREDENTIALS ARE THE CLUSTER'S. Every password, secret and URL password compose has
 *     is found (a planted one in a fixture too), and none of compose's values is left in a
 *     credential's place; compose's secrets appear nowhere at all. Each user's password is
 *     one value everywhere it appears, and every user a service logs in as is one a store's
 *     init file or a store's own superuser makes. What the cluster already holds is kept,
 *     never regenerated. A value made is 32 random bytes as hex, a different one each time;
 *   - the keypairs are the cluster's too: jwt-keys' public half is its private key's, Auth
 *     seals to exactly Notification's public key, and the installer reads no key file from
 *     the checkout;
 *   - every Secret a pod reads (envFrom, a Secret volume and its items, a secretKeyRef) is
 *     one the installer makes, with every key read; and every Secret it makes is read by
 *     something, so neither list can rot. kong-env is compose's environment for Kong;
 *   - the setup Job runs exactly compose's setup command, with the installer's setup-env,
 *     once (backoffLimit 0, restartPolicy Never), as the image's own non-root user;
 *   - every directory compose's setup mounts a volume at is a writable emptyDir in the
 *     Job, and a directory whose volume is named like a key Secret is seeded from that
 *     Secret before setup starts -- so setup finds the keypairs and keeps them;
 *   - .dockerignore keeps the agent's worktrees, keys, env files and Terraform out of the
 *     image the installer pushes.
 *
 * The real store logins are not here, as nothing here starts a store; they were measured by
 * booting compose's Postgres and ClickHouse on the rewritten init files (PR description).
 *   node scripts/verify-eks-install.mjs          against this tree
 *   node scripts/verify-eks-install.mjs [root]   against another (a red run in a copy)
 */

import { spawnSync } from 'node:child_process';
import { createPublicKey } from 'node:crypto';
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
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

/** The password a role's or user's CREATE statement sets in an init file, or undefined. */
function sqlPassword(sql, user) {
  const pg = new RegExp(`CREATE\\s+ROLE\\s+${user}\\b[^;]*?\\bPASSWORD\\s+'([^']*)'`, 'i').exec(sql);
  const ch = new RegExp(`CREATE\\s+USER\\s+(?:IF\\s+NOT\\s+EXISTS\\s+)?${user}\\s+IDENTIFIED\\s+WITH\\s+\\w+\\s+BY\\s+'([^']*)'`, 'i').exec(sql);
  return (pg ?? ch)?.[1];
}

async function main() {
  const rootArg = process.argv.slice(2).find((a) => a !== '--');
  const root = rootArg ? resolve(rootArg) : ROOT;
  console.log(`  reading ${rootArg ? root : 'this tree'}`);
  const eks = await import(pathToFileURL(join(root, 'infra', 'eks', 'up.mjs')).href);
  const cred = await import(pathToFileURL(join(root, 'infra', 'eks', 'credentials.mjs')).href);
  const stores = await import(pathToFileURL(join(root, 'infra', 'stores', 'secrets.mjs')).href);

  const composeRun = spawnSync('docker', ['compose', '-f', join(root, 'docker-compose.yml'), '-p', 'eventhub-rebuild', 'config', '--format', 'json'], { encoding: 'utf8', maxBuffer: 64 << 20 });
  if (composeRun.status !== 0) {
    check(false, `docker compose config: ${(composeRun.stderr || '').trim().slice(-400)}`);
    return;
  }
  const compose = JSON.parse(composeRun.stdout);
  const charts = eks.allCharts(root);
  const storeDocs = stores.renderChart(join(root, 'infra', 'stores'));

  // -------------------------------------------------------------------------
  section('the credentials found where they sit, a planted one of each kind too');
  const planted = {
    'x-env': { NEW_TOKEN: 'planted-token', DB_URL: 'postgresql://u_one:pw-one@db:5432/one', CLICKHOUSE_PASSWORD: 'pw-two', CLICKHOUSE_USER: 'u_two', ACCESS_TOKEN_TTL: '900', PLAIN: 'postgresql://db:5432/nopw' },
    'x-init': { '01.sql': "CREATE ROLE u_one LOGIN PASSWORD 'pw-one';\nCREATE USER IF NOT EXISTS u_two IDENTIFIED WITH sha256_password BY 'pw-two';\n" },
  };
  const pf = cred.credentialsIn(planted);
  const pslots = pf.map((f) => `${f.slot}(${f.kind})`).sort();
  const want = ['clickhouse.u_two(password)', 'clickhouse.u_two(sql)', 'postgres.u_one(sql)', 'postgres.u_one(url)', 'secret.NEW_TOKEN(secret)'];
  check(JSON.stringify(pslots) === JSON.stringify(want), `a planted secret, URL password, *_PASSWORD and both init statements are found, and ACCESS_TOKEN_TTL and a URL with no password are not (${pslots.join(', ')})`);
  const pc = cred.ensure(pf).creds;
  const pout = cred.withCredentials(planted, pc);
  check(
    pout['x-env'].NEW_TOKEN === pc['secret.NEW_TOKEN'] &&
      decodeURIComponent(new URL(pout['x-env'].DB_URL).password) === pc['postgres.u_one'] &&
      new URL(pout['x-env'].DB_URL).username === 'u_one' &&
      sqlPassword(pout['x-init']['01.sql'], 'u_one') === pc['postgres.u_one'] &&
      sqlPassword(pout['x-init']['01.sql'], 'u_two') === pc['clickhouse.u_two'] &&
      pout['x-env'].CLICKHOUSE_PASSWORD === pc['clickhouse.u_two'] &&
      pout['x-env'].ACCESS_TOKEN_TTL === '900' &&
      pout['x-env'].PLAIN === planted['x-env'].PLAIN,
    'and each is replaced in place -- the URL keeps its user, the init file its statement -- while everything else is kept exactly',
  );

  // -------------------------------------------------------------------------
  section("the credentials are the cluster's: none of compose's reaches a Secret");
  let install;
  try {
    install = eks.installSecrets(compose, charts, storeDocs, {});
  } catch (e) {
    check(false, `installSecrets refused: ${e.message}`);
    return;
  }
  const { secrets, found, creds } = install;
  const kinds = [...new Set(found.map((f) => f.kind))].sort();
  const slots = [...new Set(found.map((f) => f.slot))].sort();
  check(found.length > 0 && JSON.stringify(kinds) === JSON.stringify(['password', 'secret', 'sql', 'url']), `CONTROL: ${found.length} of compose's credentials found, ${slots.length} user(s) and secret(s), of every kind (${kinds.join(', ')})`);
  const wrong = [];
  for (const f of found) {
    const v = String(secrets[f.secret]?.[f.key] ?? '');
    const now = f.kind === 'url' ? decodeURIComponent(new URL(v).password) : f.kind === 'sql' ? sqlPassword(v, f.slot.split('.')[1]) : v;
    if (now !== creds[f.slot] || now === f.dev) wrong.push(`${f.secret}/${f.key}`);
  }
  check(wrong.length === 0, `every one of them now holds the cluster's value for its user or secret, never compose's: one value per user wherever it appears${wrong.length ? ` -- not so in ${wrong.join(', ')}` : ''}`);
  const secretDevs = found.filter((f) => f.kind === 'secret').map((f) => f.dev);
  const anywhere = Object.entries(secrets).flatMap(([s, data]) => Object.entries(data).filter(([, v]) => secretDevs.some((d) => String(v).includes(d))).map(([k]) => `${s}/${k}`));
  check(secretDevs.length > 0 && anywhere.length === 0, `compose's ${secretDevs.length} secrets appear nowhere in any Secret, not even inside another value${anywhere.length ? ` -- found in ${anywhere.join(', ')}` : ''}`);
  const setBy = new Set(found.filter((f) => f.kind === 'sql' || f.secret === 'stores-credentials').map((f) => f.slot));
  const orphans = [...new Set(found.filter((f) => (f.kind === 'url' || f.kind === 'password') && f.secret !== 'stores-credentials').map((f) => f.slot))].filter((s) => !setBy.has(s));
  check(setBy.size > 0 && orphans.length === 0, `every user a service logs in as is made, with that password, by a store's init file or as the store's own superuser${orphans.length ? ` -- made by nothing: ${orphans.join(', ')}` : ''}`);
  const values = slots.map((s) => creds[s]);
  check(values.every((v) => /^[0-9a-f]{64}$/.test(v)) && new Set(values).size === values.length, `each value made is 32 random bytes as hex, and no two are the same (${values.length})`);
  const again = cred.ensure(found).creds;
  check(slots.every((s) => again[s] !== creds[s]), 'a second cluster gets values of its own: none repeats');
  const kept = cred.ensure(found, { [slots[0]]: 'held-by-the-cluster' });
  check(kept.creds[slots[0]] === 'held-by-the-cluster' && !kept.made.includes(slots[0]) && kept.made.length === slots.length - 1 + Object.keys(cred.KEYPAIRS).length, `what the cluster already holds is kept, never made again (${slots[0]} kept; ${kept.made.length} made)`);

  // -------------------------------------------------------------------------
  section("the keypairs are the cluster's, and paired as the services need them");
  const jk = secrets['jwt-keys'] ?? {};
  const nk = secrets['notification-keys'] ?? {};
  const pub = (pem) => createPublicKey(pem).export({ type: 'spki', format: 'pem' });
  const bits = (pem) => createPublicKey(pem).asymmetricKeyDetails?.modulusLength;
  check(Boolean(jk['private.pem']) && pub(jk['private.pem']) === jk['public.pem'] && bits(jk['private.pem']) === 2048, `jwt-keys: public.pem is the public half of private.pem, RSA ${bits(jk['private.pem'] ?? '')}`);
  check(Boolean(nk['private.pem']) && pub(nk['private.pem']) === nk['public.pem'] && jk['notification-public.pem'] === nk['public.pem'], "notification-keys: its own pair, and the notification-public.pem Auth seals to is exactly its public half");
  check(jk['private.pem'] !== nk['private.pem'], 'two keypairs, not one used twice');
  const strip = (src) => src.replace(/\/\*[\s\S]*?\*\//g, '').replace(/(^|[^:'"`])\/\/.*$/gm, '$1');
  const code = ['up.mjs', 'credentials.mjs'].map((f) => strip(readFileSync(join(root, 'infra', 'eks', f), 'utf8'))).join('\n');
  check(!/services['"/\\,\s]+(auth|notification)['"/\\,\s]+keys/.test(code) && !/KEY_DIRS/.test(code), 'the installer reads no key file from the checkout: services/*/keys are development keys, and a worktree has none');

  // -------------------------------------------------------------------------
  section('every Secret a pod reads is one the installer makes, and every one it makes is read');
  const produced = {};
  for (const [name, data] of Object.entries(secrets)) produced[name] = new Set(Object.keys(data));
  const made_ = Object.fromEntries(Object.entries(secrets).map(([name, data]) => [name, { data }]));

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
  // compose's environment, value for value, except where a credential sat: there, and only
  // there, the cluster's own.
  const composeButCredentials = (name, env) => {
    const data = made_[name]?.data ?? {};
    const cred_ = new Set(found.filter((f) => f.secret === name).map((f) => f.key));
    const keysSame = JSON.stringify(Object.keys(data).sort()) === JSON.stringify(Object.keys(env).sort());
    const off = Object.keys(env).filter((k) => (cred_.has(k) ? data[k] === env[k] : data[k] !== env[k]));
    return { ok: keysSame && off.length === 0, n: Object.keys(data).length, creds: cred_.size, off };
  };
  const kongEnv = composeButCredentials('kong-env', eks.envOf(compose, 'kong'));
  check(kongEnv.ok && kongEnv.creds === 0, `kong-env is compose's resolved environment for Kong, holding no credential (${kongEnv.n} variable(s))`);
  for (const [name, r] of Object.entries(reads).sort()) {
    const have = produced[name];
    const missing = [...r.keys].filter((k) => k !== '*' && !have?.has(k));
    check(have !== undefined && missing.length === 0, `${name}, read by ${[...r.by].join(', ')}: made${have ? ` with ${have.size} key(s)` : ' by nothing'}${missing.length ? ` -- missing ${missing.join(', ')}` : ''}`);
  }
  const unread = Object.keys(made_).filter((n) => !reads[n]);
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
  check((main_.envFrom ?? []).some((f) => f.secretRef?.name === 'setup-env') && made_['setup-env'] !== undefined, "with compose's environment for setup, from setup-env");
  const setupEnv = composeButCredentials('setup-env', eks.envOf(compose, 'setup'));
  check(setupEnv.ok && setupEnv.creds > 0, `setup-env is compose's resolved setup environment, with the cluster's ${setupEnv.creds} credential(s) where compose's sat and every other variable compose's (${setupEnv.n} variable(s))${setupEnv.off.length ? ` -- differs at ${setupEnv.off.join(', ')}` : ''}`);
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
  section('nothing of .claude, keys or env files goes into the image');
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
