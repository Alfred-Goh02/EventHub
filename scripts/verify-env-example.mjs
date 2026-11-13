/**
 * Asserts that a fresh clone can actually start each service.
 *
 * Every service ships a .env.example whose first line says "Copy to .env and
 * adjust". Nothing checked that the copy WORKS. CI sets its environment from
 * the workflow file instead, so .env.example is the one piece of configuration
 * in this repository that no job ever exercised — and it drifted exactly the
 * way an unexercised file does.
 *
 * Found by trying it: services/ticketing/.env.example omitted
 * PAYMENT_WEBHOOK_SECRET, and Ticketing REFUSES TO START without it, by design,
 * because a webhook route with no secret is an unauthenticated write path into
 * payment state. So the documented way to run this service produced:
 *
 *   Error: PAYMENT_WEBHOOK_SECRET is required — the webhook route is
 *          unauthenticated without it
 *
 * The refusal is correct. The instructions were wrong. Nothing could tell the
 * difference, because the instructions were never run.
 *
 * So this does the only thing that actually proves the claim: it boots each
 * service with ITS .env.example AND NOTHING ELSE, and waits for the service to
 * answer. A missing variable, a stale URL, a renamed key, a new mandatory
 * secret — all of them fail here, and none of them can be argued away.
 *
 * ONE EXEMPTION, printed rather than assumed. AUTH_PUBLIC_KEY_B64 is left empty
 * in the examples because it is GENERATED, not configured: it is the base64 of
 * services/auth/keys/public.pem, which is gitignored and created by the setup
 * step. Supplying it here is the same thing the setup instructions tell a human
 * to do. Every other variable comes from the example file untouched, but for the
 * two below.
 *
 * TWO MORE, ALSO PRINTED: THE CONSUMER GROUP AND WHERE IT STARTS. An example names
 * the group the deployed consumer uses (KAFKA_CONSUMER_GROUP=notification-dispatch),
 * so a service booted from it is, for the seconds it is up, a live member of the real
 * group: it takes partitions from the real consumers, resumes from their committed
 * offsets, works through whatever they had waiting, and is then killed mid-flight.
 * Measured with the rewrite removed: Community, Notification, Search and Analytics'
 * ingest joined all five real groups, and in the moment Notification was a member it
 * SENT 14 emails that earlier verifiers had left waiting on auth.email-requested and the
 * retry topics, writing sent-log rows into five of those earlier runs' tenants. A
 * delivery it had claimed and not finished when it was killed would have stayed
 * PENDING in another tenant, which is how this was found. So every key that names a
 * group is rewritten to one of this run's, and every key that says where a new group
 * starts is set to `latest`, because a new group at `earliest` replays everything.
 * Both are derived from the examples' own keys, and the groups are deleted at the end.
 * The groups a service names from one of those (Notification's -retry, -replies and
 * -replies-retry) inherit the rewrite, and the check that nothing joined a real group
 * reads every group the cluster lists, so a derived one that escaped it is seen too.
 * Each service is killed as soon as its groups have a member, so it consumes as little
 * as possible. A group key with no start setting is committed at the end of its topics
 * before the service boots instead (START_AT_END below): Notification's contact-card
 * consumer starts every new group at the beginning of auth.user-contact by design, and
 * has no setting to say otherwise. From there its replay upserted cards the table already
 * held, and parked the topic's forged cards on auth.user-contact.dlq again: measured, 37
 * of them in the second or so before the kill.
 *
 * Ticketing's projection was the one consumer this could not move: its group was a
 * constant in services/ticketing/src/consumer.ts, in no example, so there was no key to
 * rewrite and it joined ticketing-event-projection every time it was booted here. And
 * the check below never saw it, because it looked only at services whose example named
 * a group. Now the group is KAFKA_CONSUMER_GROUP with PROJECTION_START_FROM beside it,
 * and the check watches every service for a join to any group that is neither this
 * run's nor another verifier's, which is what a group written into the code looks like.
 *
 * AND THE OTHER DIRECTION, FOR AUTH: the example is a laptop's configuration, and
 * Auth run from it as production must refuse. Its APP_URL_TEMPLATE is localhost,
 * and a production Auth that kept it would email every password-reset link
 * pointing at the reader's own machine. See authInProduction below.
 *
 * Run: node scripts/verify-env-example.mjs
 */

import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { existsSync, readdirSync, readFileSync } from 'node:fs';
import { join } from 'node:path';
import { Kafka, logLevel } from 'kafkajs';
import { commitAtEnd, deleteGroupsWhenEmpty } from './lib/kafka-groups.mjs';
import { CONTACTS_TOPIC } from './lib/notification-groups.mjs';

const RUN = randomBytes(4).toString('hex');
/**
 * Every consumer group this run's services join starts with this. A prefix rather than
 * a suffix, so the groups a service DERIVES from its configured name carry it too:
 * Notification runs four more from KAFKA_CONSUMER_GROUP (`-retry`, `-replies`,
 * `-replies-retry`), and each is this run's without being named here.
 */
const RUN_PREFIX = `verify-env-${RUN}-`;

/** The example keys that name a consumer group, and the ones that say where a new group starts. */
const isGroupKey = (key) => /(^|_)GROUP(_|$)/.test(key);
const isStartKey = (key) => /(^|_)START_FROM$/.test(key);

/**
 * The group keys whose consumer has no start setting and begins every new group at the
 * start of its topics, and those topics. Each such group of this run's is committed at
 * the end of them before its service boots. Hand-written, so checked in main() against
 * the examples: a key here that no example sets any more is reported.
 */
const START_AT_END = { CONTACTS_CONSUMER_GROUP: [CONTACTS_TOPIC] };

/**
 * A deployed group, or one a service derives from a deployed group's name:
 * notification-dispatch, and notification-dispatch-retry, -replies and -replies-retry
 * with it, whatever suffixes a service adds next. A name carrying a run's hex tag is
 * another verifier's (community-feed-verify-1a2b3c4d), not a deployed one.
 *
 * AND ANY OTHER GROUP WITH NO RUN'S TAG IN ITS NAME, which no example has to name. A
 * group id written into a service's code, rather than read from its configuration, is in
 * no example, so a check that knew only the examples' groups could never see a booted
 * service join one. Ticketing's projection group was exactly that until it became
 * KAFKA_CONSUMER_GROUP.
 */
const isRealFamily = (groupId, realGroups) =>
  !groupId.startsWith(RUN_PREFIX) &&
  (!/[0-9a-f]{8}/.test(groupId) ||
    realGroups.some((r) => groupId === r || (groupId.startsWith(`${r}-`) && !/[0-9a-f]{8}/.test(groupId.slice(r.length)))));

/** Scratch ports, well away from anything a developer is likely to be running. */
const SERVICES = [
  { name: 'auth', portVar: 'PORT', port: 3901, path: '/health' },
  { name: 'event', portVar: 'PORT', port: 3902, path: '/health' },
  { name: 'ticketing', portVar: 'PORT', port: 3903, path: '/health' },
  { name: 'relay', portVar: 'RELAY_METRICS_PORT', port: 3904, path: '/metrics' },
  { name: 'live', portVar: 'PORT', port: 3905, path: '/health' },
  { name: 'community', portVar: 'PORT', port: 3910, path: '/ready' },
  // /ready: it answers 200 only once the example's APP_DATABASE_URL has reached
  // Postgres, and the process only listens after refusing a role that bypasses RLS.
  { name: 'notification', portVar: 'PORT', port: 3909, path: '/ready' },
  // /ready, not /health. Search listens whether or not the cluster is there, so
  // /health proves only that the process started. /ready answers 200 only once
  // the example's OPENSEARCH_NODE has actually been reached.
  { name: 'search', portVar: 'PORT', port: 3906, path: '/ready' },
  // /ready for the same reason as Search: it answers 200 only once ClickHouse has
  // been reached as the example's analytics_app user, not merely once listening.
  { name: 'analytics', portVar: 'PORT', port: 3907, path: '/ready' },
  // The same package's second process, from its own entrypoint and its own port
  // variable. /ready answers once it has connected to Kafka, subscribed, and
  // reached ClickHouse, all from the example's addresses and credentials.
  { name: 'analytics', label: 'analytics ingest', entry: 'dist/consumer.js', portVar: 'INGEST_PORT', port: 3908, path: '/ready' },
];

const KEY_PATH = join('services', 'auth', 'keys', 'public.pem');

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Parse a .env file the way `node --env-file` does for the cases these files
 * use: KEY=value, blank lines, and # comments. No quoting or interpolation,
 * because none of these files use any.
 */
function parseEnvFile(path) {
  const out = {};
  for (const raw of readFileSync(path, 'utf8').split(/\r?\n/)) {
    const line = raw.trim();
    if (!line || line.startsWith('#')) continue;
    const eq = line.indexOf('=');
    if (eq === -1) continue;
    out[line.slice(0, eq).trim()] = line.slice(eq + 1).trim();
  }
  return out;
}

async function waitForHttp(port, path, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const res = await fetch(`http://localhost:${port}${path}`);
      if (res.ok) return true;
    } catch {
      // not up yet
    }
    await sleep(400);
  }
  return false;
}

/**
 * Every group the cluster lists, with its current members, id to "clientId@host".
 *
 * All of them, not a list: the groups a service derives from its configured name are
 * not in any example, so the only way to see one being joined is to look at everything.
 */
async function allMembers(admin) {
  const ids = (await admin.listGroups()).groups.map((g) => g.groupId);
  if (ids.length === 0) return new Map();
  const { groups } = await admin.describeGroups(ids);
  return new Map(groups.map((g) => [g.groupId, new Map(g.members.map((m) => [m.memberId, `${m.clientId}@${m.clientHost}`]))]));
}

/** The members of group `g` in `now` that were not members in `before`. */
const newcomers = (before, now, g) => [...(now.get(g) ?? new Map())].filter(([id]) => !before.get(g)?.has(id)).map(([, who]) => who);

/** The groups among these that have gained a member since `before`. */
const gainedMember = (before, now, groupIds) => groupIds.filter((g) => newcomers(before, now, g).length > 0);

async function bootFromExample(svc, publicKeyB64, kafka) {
  const dir = join('services', svc.name);
  const example = join(dir, '.env.example');

  if (!existsSync(example)) {
    check(false, `${svc.name}: no .env.example at all`);
    return;
  }

  const parsed = parseEnvFile(example);

  /**
   * The environment is the example file, plus PATH so node can run at all, plus
   * the one generated secret. Deliberately NOT process.env: inheriting the
   * developer's shell is exactly how a missing variable stays invisible, since
   * their own .env is usually already exported somewhere.
   */
  const env = {
    PATH: process.env['PATH'],
    SystemRoot: process.env['SystemRoot'],
    ...parsed,
    [svc.portVar]: String(svc.port),
  };

  const suppliedKey = 'AUTH_PUBLIC_KEY_B64' in parsed && parsed['AUTH_PUBLIC_KEY_B64'] === '';
  if (suppliedKey) env['AUTH_PUBLIC_KEY_B64'] = publicKeyB64;

  /**
   * THE CONSUMER GROUPS ARE THIS RUN'S, NOT THE EXAMPLE'S. See the header: the example
   * names the deployed group, and a process joining it is a live consumer of the real
   * thing. Only keys the example SETS are rewritten, so a key it lacks is still missing.
   * A new group starts at the end of its topics, not at the example's `earliest`: a
   * fresh group from the start would replay every message ever written, which is worse.
   */
  const consumer = {};
  for (const key of Object.keys(parsed)) {
    if (isGroupKey(key)) consumer[key] = `${RUN_PREFIX}${parsed[key]}`;
    else if (isStartKey(key)) consumer[key] = 'latest';
  }
  Object.assign(env, consumer);

  const mine = Object.keys(parsed).filter(isGroupKey).map((k) => env[k]);
  for (const g of mine) kafka.configured.add(g);
  const atEnd = [];
  for (const [key, topics] of Object.entries(START_AT_END)) {
    if (!(key in consumer)) continue;
    await commitAtEnd(kafka.admin, consumer[key], topics);
    atEnd.push(`${key} committed at the end of ${topics.join(', ')}`);
  }
  const before = await allMembers(kafka.admin);

  const child = spawn(process.execPath, [svc.entry ?? 'dist/index.js'], {
    cwd: dir,
    env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  let output = '';
  child.stdout.on('data', (c) => (output += c));
  child.stderr.on('data', (c) => (output += c));

  let exited = null;
  child.on('exit', (code) => (exited = code));

  const up = await waitForHttp(svc.port, svc.path, 25_000);

  /**
   * Lead with the thrown message, not the tail of the output.
   *
   * A Node stack trace puts the useful line FIRST and then several frames of
   * module loader noise, so a plain tail shows the noise and hides the reason.
   * The whole point of this check is to name the missing variable.
   */
  const lines = output.trim().split(/\r?\n/);
  const reason = lines.find((l) => /^(Error|[A-Za-z]*Error):/.test(l.trim()));
  const detail = reason ? [reason, ...lines.slice(0, 3)] : lines.slice(0, 8);

  check(
    up,
    up
      ? `${svc.label ?? svc.name} booted from .env.example alone` +
        (suppliedKey ? ' (AUTH_PUBLIC_KEY_B64 supplied from the generated keypair)' : '')
      : `${svc.label ?? svc.name} did NOT come up on port ${svc.port}` +
        (exited !== null ? ` — it exited with code ${exited}` : ' — still running but not answering') +
        `\n          ${detail.join('\n          ')}`,
  );

  /**
   * NOT A MEMBER OF ANY REAL GROUP, checked while the process is still alive.
   *
   * First it is given until each group it is configured with has gained a member, and
   * then until the set of groups gaining members stops growing, so by the time the real
   * ones are read its consumers have joined SOMEWHERE, derived groups included: a check
   * made before the join would pass on a service that was about to join a real group.
   * A process with no consumer (Analytics' HTTP side shares the ingest's example) just
   * waits the time out.
   *
   * EVERY SERVICE, not only those whose example names a group: a service that joins a
   * group its configuration does not name has no example key to rewrite, and this check
   * skipping it is how Ticketing's projection consumer went on joining the real group.
   * Those are watched for three seconds at least, past the moment they answer.
   *
   * Real means the whole family: a deployed group, or any group named from one, since a
   * derived group that escaped this run's prefix is as live as the one it came from. And
   * any other group with no run's tag in its name, which is what a group written into
   * the code looks like.
   */
  if (up) {
    const started = Date.now();
    const deadline = started + 15_000;
    let now = before;
    let settled = '';
    while (Date.now() < deadline) {
      now = await allMembers(kafka.admin);
      const moving = gainedMember(before, now, [...now.keys()]).sort().join(',');
      if (gainedMember(before, now, mine).length === mine.length && moving === settled && Date.now() - started >= 3_000) break;
      settled = moving;
      await sleep(1_000);
    }
    const joined = gainedMember(before, now, [...now.keys()].filter((g) => g.startsWith(RUN_PREFIX))).sort();
    for (const g of joined) kafka.joined.add(g);
    const real = gainedMember(before, now, [...now.keys()].filter((g) => isRealFamily(g, kafka.realGroups))).sort();
    const starts = [...Object.keys(consumer).filter(isStartKey).map((k) => `${k}=${consumer[k]}`), ...atEnd];
    check(
      real.length === 0,
      real.length === 0
        ? `${svc.label ?? svc.name} joined no real consumer group ` +
            `(${joined.length ? `it consumed as this run's ${joined.map((g) => g.slice(RUN_PREFIX.length)).join(', ')}` : 'it joined no group at all'}` +
            `${starts.length ? `; ${starts.join(', ')}` : ''})`
        : `${svc.label ?? svc.name} JOINED ${real.map((g) => `${g} (as ${newcomers(before, now, g).join(', ')})`).join(', ')}: ` +
            "for as long as it runs it takes partitions, and messages, from that group's real consumers, and it is killed mid-flight",
    );
  }

  child.kill();
  // Give the port back before the next service starts, so a failure to bind is
  // never mistaken for a configuration problem.
  await sleep(500);
}

/** Away from the scratch ports above, so a straggler there cannot answer for it. */
const AUTH_PRODUCTION_PORT = 3911;

/** Auth from its .env.example and nothing else, as production, plus `overrides`. */
function startAuthInProduction(overrides) {
  const dir = join('services', 'auth');
  const child = spawn(process.execPath, ['dist/index.js'], {
    cwd: dir,
    env: {
      PATH: process.env['PATH'],
      SystemRoot: process.env['SystemRoot'],
      ...parseEnvFile(join(dir, '.env.example')),
      NODE_ENV: 'production',
      PORT: String(AUTH_PRODUCTION_PORT),
      ...overrides,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let output = '';
  child.stdout.on('data', (c) => (output += c));
  child.stderr.on('data', (c) => (output += c));
  const proc = { child, code: undefined, log: () => output };
  // 'close', not 'exit': it fires once the output has been read to the end, so the
  // refusal is in log() by the time anything looks for it.
  proc.exited = new Promise((resolve) =>
    child.on('close', (code) => {
      proc.code = code;
      resolve(code);
    }),
  );
  return proc;
}

/** Whichever comes first: the process exits, or it answers /health. */
async function exitOrListen(proc, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (proc.code !== undefined) return { exit: proc.code };
    try {
      if ((await fetch(`http://localhost:${AUTH_PRODUCTION_PORT}/health`)).ok) return { listening: true };
    } catch {
      // not listening
    }
    await sleep(250);
  }
  return {};
}

/**
 * Auth, as production, from the same example: it must refuse the example's origin.
 *
 * APP_URL_TEMPLATE is where every invite and password-reset link points, and the
 * example's is http://{tenant}.eventhub.localhost:5173 -- right on a laptop. A
 * production Auth that kept it would start, seal every reset link to the reader's own
 * machine, and email it; password reset would be broken for everyone, and nothing
 * would say so. So Auth refuses to start under NODE_ENV=production unless it is https,
 * and this boots it exactly the way a forgetful deployment would: the example, and
 * NODE_ENV=production, and nothing else.
 *
 * The control is the same process given an https origin, which must start. Without
 * it, a guard that refused every production boot, for any reason, would pass.
 */
async function authInProduction() {
  const example = parseEnvFile(join('services', 'auth', '.env.example'));

  /**
   * The control below is "something answered /health on this port", so first make
   * sure nothing already does. A leftover process here would make a refusal look
   * like a start, and a broken start look like a working control.
   */
  const taken = await fetch(`http://localhost:${AUTH_PRODUCTION_PORT}/health`).then(() => true, () => false);
  check(!taken, taken ? `something is already listening on :${AUTH_PRODUCTION_PORT} -- stop it, then rerun` : `:${AUTH_PRODUCTION_PORT} is free`);
  if (taken) return;

  const refused = startAuthInProduction({});
  try {
    const outcome = await exitOrListen(refused, 30_000);
    // Anchored on "Error:". The stack trace also prints the throwing line of source,
    // and a looser pattern would match that instead of the message.
    const named = refused.log().match(/Error: refusing to start: (.+?) must be https/)?.[1] ?? '';
    check(
      outcome.exit !== undefined && outcome.exit !== 0 && named.includes('APP_URL_TEMPLATE'),
      `with the example's APP_URL_TEMPLATE (${example['APP_URL_TEMPLATE'] ?? 'absent'}), a production auth refuses to ` +
        `start and names it (${outcome.listening ? 'it STARTED' : `exit ${outcome.exit ?? 'none'}`}: ${named || 'no such message'})`,
    );
  } finally {
    refused.child.kill();
    await refused.exited;
  }

  const control = startAuthInProduction({ APP_URL_TEMPLATE: 'https://{tenant}.eventhub.example' });
  try {
    const outcome = await exitOrListen(control, 30_000);
    // Still ours: it answered, and it is the process this started, still running.
    const ours = outcome.listening === true && control.code === undefined;
    const lines = control.log().trim().split(/\r?\n/);
    const reason = lines.find((l) => /^(Error|[A-Za-z]*Error):/.test(l.trim())) ?? lines.slice(0, 3).join(' | ');
    check(
      ours,
      ours
        ? 'CONTROL: with an https APP_URL_TEMPLATE, the same production process starts'
        : `CONTROL: with an https APP_URL_TEMPLATE, the production process did NOT start (exit ${outcome.exit ?? 'none'}): ${reason}`,
    );
  } finally {
    control.child.kill();
    await control.exited;
  }
}

async function main() {
  console.log('  booting every service from its .env.example and nothing else');
  console.log('');

  if (!existsSync(KEY_PATH)) {
    console.error(`FAIL: ${KEY_PATH} is missing — generate the keypair first`);
    process.exit(1);
  }
  const publicKeyB64 = readFileSync(KEY_PATH).toString('base64');

  /**
   * THE LIST ABOVE IS HAND-WRITTEN, SO IT IS CHECKED AGAINST THE DIRECTORY.
   *
   * It was written with four services and stayed at four while a fifth and a
   * sixth arrived. Chapter 5 added Live with its own .env.example and eleven
   * variables, and this file went on printing "every service starts from the
   * configuration its .env.example documents" — true of every service it had
   * heard of. A check whose coverage is a literal is a check that silently
   * stops covering, which is the same trap verify-dockerfile-workspaces.mjs
   * exists for in the image.
   *
   * Asserted BEFORE anything boots, so a missing service is the first thing
   * reported rather than a line after a run that looked clean.
   */
  const onDisk = readdirSync('services', { withFileTypes: true })
    .filter((d) => d.isDirectory() && existsSync(join('services', d.name, 'package.json')))
    .map((d) => d.name)
    .sort();
  const listed = new Set(SERVICES.map((s) => s.name));
  const unlisted = onDisk.filter((n) => !listed.has(n));
  check(onDisk.length > 0, `services/ holds ${onDisk.length} service(s): ${onDisk.join(', ')}`);
  check(
    unlisted.length === 0,
    unlisted.length === 0
      ? 'and every one of them is booted below'
      : `${unlisted.join(', ')} ${unlisted.length === 1 ? 'is' : 'are'} in services/ and never booted ` +
        'here, so a broken .env.example there would pass this check',
  );
  console.log('');

  /**
   * THE REAL GROUPS, from the examples themselves: every value of a key that names a
   * consumer group. These are the groups the deployed consumers use, so a booted
   * service must never join one.
   */
  const examples = [...new Set(SERVICES.map((s) => s.name))]
    .map((name) => join('services', name, '.env.example'))
    .filter((p) => existsSync(p))
    .map(parseEnvFile);
  const realGroups = [...new Set(examples.flatMap((e) => Object.keys(e).filter(isGroupKey).map((k) => e[k])))].sort();
  const brokers = examples.find((e) => Object.keys(e).some(isGroupKey))?.['KAFKA_BROKERS'];
  check(realGroups.length > 0 && Boolean(brokers), `the examples name ${realGroups.length} consumer group(s) (${realGroups.join(', ')}) on ${brokers}`);
  const stale = Object.keys(START_AT_END).filter((k) => !examples.some((e) => k in e));
  check(
    stale.length === 0,
    stale.length === 0
      ? `and the group key(s) committed at the end of their topics before boot are all set by an example (${Object.keys(START_AT_END).join(', ')})`
      : `${stale.join(', ')} ${stale.length === 1 ? 'is' : 'are'} in START_AT_END but in no example: renamed, the group is no longer ` +
        'committed at the end, and starts at the beginning of its topics',
  );
  console.log('');

  const admin = new Kafka({ clientId: `verify-env-example-${RUN}`, brokers: (brokers ?? '').split(','), logLevel: logLevel.NOTHING }).admin();
  await admin.connect();
  const kafka = { admin, realGroups, configured: new Set(), joined: new Set() };

  try {
    for (const svc of SERVICES) {
      await bootFromExample(svc, publicKeyB64, kafka);
    }

    /**
     * The control for every "joined no real consumer group" above: each group the
     * services were configured with gained a member while its service ran. Without it,
     * a describe that saw no members at all would pass every one of those checks.
     */
    const configured = [...kafka.configured];
    const unseen = configured.filter((g) => !kafka.joined.has(g));
    const derived = [...kafka.joined].filter((g) => !kafka.configured.has(g));
    check(
      configured.length > 0 && unseen.length === 0,
      `CONTROL: every group the services were configured with gained a member while they ran (${configured.length - unseen.length} of ${configured.length}` +
        `${unseen.length ? `; never joined: ${unseen.join(', ')}` : ''}), and ${derived.length} more they derived from those names, ` +
        'so a join to a real group would have been seen',
    );

    console.log('');
    console.log("  and auth, as production, refuses the example's localhost origin");
    console.log('');
    await authInProduction();
  } finally {
    /**
     * Deleted, and reported by what was DELETED: "0 left" is also what a cleanup that
     * found nothing to delete prints. A killed consumer stays a member until its session
     * times out, so each group is retried until it can go.
     */
    const ours = (await admin.listGroups()).groups.map((g) => g.groupId).filter((g) => g.startsWith(RUN_PREFIX)).sort();
    const left = await deleteGroupsWhenEmpty(admin, ours, { timeoutMs: 60_000 });
    const deleted = ours.filter((g) => !left.includes(g));
    console.log('');
    check(
      deleted.length > 0 && left.length === 0,
      `cleanup: deleted this run's ${deleted.length} consumer group(s)${deleted.length ? ` (${deleted.join(', ')})` : ''}, ${left.length} left` +
        `${left.length ? ` (${left.join(', ')})` : ''}`,
    );
    await admin.disconnect().catch(() => {});
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exit(1);
  }
  console.log('every service starts from the configuration its .env.example documents,');
  console.log("and auth will not run in production on the example's origin");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
