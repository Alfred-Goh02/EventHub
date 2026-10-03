/**
 * Asserts what a user can turn off, what they cannot, and that the unsubscribe link in
 * an email does exactly one thing for exactly one person.
 *
 * EACH PROPERTY, AND WHAT WOULD LOOK THE SAME WITHOUT IT:
 *   - /preferences needs a signed-in user and reads their choices only: another tenant
 *     under the same user id keeps its own default, and a sealed-request token (aud
 *     eventhub-notification) is not an access token.
 *   - A password reset cannot be turned off: the PATCH is refused and writes no row,
 *     and a row planted to say otherwise is ignored when the page is built.
 *   - The categories are a closed set: a near-miss name is refused, not recorded as an
 *     opt-out of something that does not exist while the real one keeps sending.
 *   - Following an unsubscribe link changes NOTHING; POSTing it does. A link scanner
 *     fetching every URL in a message would otherwise unsubscribe people.
 *   - A tampered token is refused and changes nothing — another user's id under the
 *     original signature, a changed signature, a correctly signed token for a password
 *     reset, and garbage. The control is the untampered token working.
 *   - A production process refuses to start with the public development secret, and
 *     with either public origin not https: one left unset, so the localhost default, and
 *     one on plain http at a real host name. Each time its message names that one only,
 *     and the control is the same process starting once both are https and the secret is
 *     its own.
 *
 * Tokens are signed here with the working tree's keys and the example secret, the
 * same way Auth and Notification make them.
 *
 * Scoped to tenants created for this run; their preference rows are removed as the
 * owner, in each tenant's context, and the count deleted is reported.
 *
 * Run (with Postgres, Kafka and the Schema Registry up; Notification built; both
 * keypairs generated): node scripts/verify-notification-preferences.mjs
 */

import { spawn } from 'node:child_process';
import { createHmac, createPrivateKey, randomBytes, sign } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { Kafka, logLevel } from 'kafkajs';
import { serviceEnv } from './lib/service-env.mjs';
import { deleteGroupsWhenEmpty } from './lib/kafka-groups.mjs';
import { contactsFromTheEnd, notificationGroups } from './lib/notification-groups.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const key = (dir, file) => readFileSync(join(ROOT, 'services', dir, 'keys', file), 'utf8');

const RUN = randomBytes(4).toString('hex');
const TENANT = `tenant_prefs_${RUN}`;
const OTHER = `tenant_prefs_other_${RUN}`;
const U = `user_prefs_u_${RUN}`;
const V = `user_prefs_v_${RUN}`;
const W = `user_prefs_w_${RUN}`;
const GROUPS = notificationGroups(`verify-notification-preferences-${RUN}`);
const PORT = 3984;

const notificationEnv = serviceEnv(ROOT, 'notification', {
  PORT: String(PORT),
  // Both of its groups are this run's: see scripts/lib/notification-groups.mjs. Every
  // process below, the production ones included, starts its consumers on them.
  ...GROUPS.env,
  DISPATCH_START_FROM: 'latest',
  AUTH_PUBLIC_KEY_B64: Buffer.from(key('auth', 'public.pem')).toString('base64'),
});
const SECRET = notificationEnv.UNSUBSCRIBE_SECRET;

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const excerpt = (log) => (log.length <= 3000 ? log : `${log.slice(0, 2000)}\n  [...]\n${log.slice(-1000)}`);

async function waitFor(fn, timeoutMs = 30_000, stepMs = 250) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await fn().catch(() => null);
    if (value) return value;
    await sleep(stepMs);
  }
  return null;
}

function start(env) {
  const child = spawn(process.execPath, ['dist/index.js'], { cwd: join(ROOT, 'services', 'notification'), env, stdio: ['ignore', 'pipe', 'pipe'] });
  let log = '';
  child.stdout.on('data', (d) => (log += d));
  child.stderr.on('data', (d) => (log += d));
  const exited = new Promise((done) => child.on('exit', (code) => done(code)));
  return { child, log: () => log, exited };
}

const b64url = (v) => Buffer.from(v).toString('base64url');
const signingKey = createPrivateKey(key('auth', 'private.pem'));
function jwt(claims, header = { alg: 'RS256', typ: 'JWT' }) {
  const h = b64url(JSON.stringify(header));
  const p = b64url(JSON.stringify(claims));
  return `${h}.${p}.${b64url(sign('sha256', Buffer.from(`${h}.${p}`), signingKey))}`;
}
/** An access token as Auth mints one. */
function token(tenantId, userId, audience = notificationEnv.JWT_AUDIENCE) {
  const now = Math.floor(Date.now() / 1000);
  return jwt({ sub: userId, tenantId, role: 'ATTENDEE', iss: notificationEnv.JWT_ISSUER, aud: audience, iat: now, exp: now + 900 });
}

/** An unsubscribe token in the service's format, signed with whatever each case needs. */
function unsubscribeToken({ tenantId, userId, category }, secret = SECRET) {
  const body = Buffer.from(JSON.stringify({ t: tenantId, u: userId, c: category }), 'utf8').toString('base64url');
  const mac = createHmac('sha256', secret).update(`v1.${body}`).digest().toString('base64url');
  return `v1.${body}.${mac}`;
}

async function api(path, { method = 'GET', bearer, body, raw } = {}) {
  const res = await fetch(`http://127.0.0.1:${PORT}${path}`, {
    method,
    headers: { ...(bearer ? { Authorization: `Bearer ${bearer}` } : {}), ...(body ? { 'Content-Type': 'application/json' } : {}), ...(raw ? { 'Content-Type': 'application/x-www-form-urlencoded' } : {}) },
    ...(body ? { body: JSON.stringify(body) } : raw ? { body: raw } : {}),
  });
  const text = await res.text();
  let parsed = null;
  try {
    parsed = JSON.parse(text);
  } catch {
    parsed = null;
  }
  return { status: res.status, body: parsed, text, headers: res.headers };
}

/**
 * The headers every unsubscribe page must carry: its URL and its form hold a token that
 * never expires. Which of them a page lacks, or the empty list.
 */
const PAGE_HEADERS = {
  'referrer-policy': (v) => v === 'no-referrer',
  'cache-control': (v) => /\bno-store\b/.test(v),
  'content-security-policy': (v) => /frame-ancestors 'none'/.test(v) && /form-action 'self'/.test(v) && /default-src 'none'/.test(v),
  'x-frame-options': (v) => v === 'DENY',
  'x-content-type-options': (v) => v === 'nosniff',
};
const missingPageHeaders = (res) => Object.entries(PAGE_HEADERS).filter(([name, ok]) => !ok(res.headers.get(name) ?? '')).map(([name]) => name);

async function inTenant(client, tenant, sql, params = []) {
  await client.query('BEGIN');
  try {
    await client.query("SELECT set_config('app.tenant_id', $1, true)", [tenant]);
    const result = await client.query(sql, params);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  }
}

async function main() {
  const taken = await fetch(`http://127.0.0.1:${PORT}/health`).then(
    () => true,
    () => false,
  );
  if (taken) {
    console.error(`FAIL: something already answers on :${PORT}. Stop it before running this.`);
    process.exitCode = 1;
    return;
  }

  const owner = new pg.Client({ connectionString: notificationEnv.DATABASE_URL });
  await owner.connect();
  const kafka = new Kafka({ clientId: `verify-prefs-${RUN}`, brokers: notificationEnv.KAFKA_BROKERS.split(','), logLevel: logLevel.NOTHING });
  const admin = kafka.admin();
  await admin.connect();

  const rows = (tenant, user) =>
    inTenant(owner, tenant, `SELECT category, channel, enabled FROM "NotificationPreference" WHERE "tenantId" = $1 AND "userId" = $2 ORDER BY category`, [tenant, user]).then((r) => r.rows);
  const replyOn = async (tenant, user, t = token(tenant, user)) =>
    ((await api('/preferences', { bearer: t })).body?.preferences ?? []).find((p) => p.category === 'COMMUNITY_REPLY')?.enabled;

  // The service starts inside the try, once its contact-card group is committed, so a
  // failure there is still cleaned up. Until then this stands in for it.
  let service = { child: { kill: () => {} }, log: () => '', exited: Promise.resolve(null) };
  try {
    const contacts = await contactsFromTheEnd(admin, ROOT, GROUPS);
    service = start(notificationEnv);
    const ready = await waitFor(async () => (await fetch(`http://127.0.0.1:${PORT}/ready`)).ok, 60_000, 500);
    check(Boolean(ready), ready ? `CONTROL: a private Notification is ready on :${PORT}` : `Notification never became ready:\n${excerpt(service.log())}`);
    if (!ready) return;
    const own = await contacts.joined();
    check(own.ok, own.message);

    console.log('');
    console.log('  /preferences is the signed-in user\'s own, and nobody else\'s');
    const anonymous = await api('/preferences');
    const sealedAudience = await api('/preferences', { bearer: token(TENANT, U, 'eventhub-notification') });
    check(anonymous.status === 401 && sealedAudience.status === 401, `no token, and a token for the sealed-request audience, are both refused (${anonymous.status}, ${sealedAudience.status})`);
    const initial = await api('/preferences', { bearer: token(TENANT, U) });
    const byCategory = Object.fromEntries((initial.body?.preferences ?? []).map((p) => [p.category, p]));
    check(
      initial.status === 200 && Object.keys(byCategory).sort().join() === 'COMMUNITY_REPLY,PASSWORD_RESET',
      `a user sees exactly the two categories (${Object.keys(byCategory).join(', ')})`,
    );
    check(
      byCategory.COMMUNITY_REPLY?.enabled === true && byCategory.COMMUNITY_REPLY?.canTurnOff === true &&
        byCategory.PASSWORD_RESET?.enabled === true && byCategory.PASSWORD_RESET?.canTurnOff === false,
      'replies are on by default and can be turned off; password resets are on and cannot',
    );

    const off = await api('/preferences', { method: 'PATCH', bearer: token(TENANT, U), body: { category: 'COMMUNITY_REPLY', enabled: false } });
    const offAgain = await api('/preferences', { method: 'PATCH', bearer: token(TENANT, U), body: { category: 'COMMUNITY_REPLY', enabled: false } });
    const stored = await rows(TENANT, U);
    check(off.status === 200 && off.body?.changed === true && offAgain.body?.changed === false, `turning replies off changes it once; again changes nothing (${off.body?.changed}, ${offAgain.body?.changed})`);
    check(stored.length === 1 && stored[0].category === 'COMMUNITY_REPLY' && stored[0].enabled === false && (await replyOn(TENANT, U)) === false, 'the choice is stored, and read back as off');
    check((await replyOn(OTHER, U)) === true, 'the same user id in another tenant still has replies on: the choice is per tenant');

    console.log('');
    console.log('  a password reset cannot be turned off');
    const resetOff = await api('/preferences', { method: 'PATCH', bearer: token(TENANT, U), body: { category: 'PASSWORD_RESET', enabled: false } });
    check(resetOff.status === 409 && !(await rows(TENANT, U)).some((r) => r.category === 'PASSWORD_RESET'), `refused, and no row written (${resetOff.status})`);
    await inTenant(owner, TENANT, `INSERT INTO "NotificationPreference" ("tenantId", "userId", category, channel, enabled) VALUES ($1, $2, 'PASSWORD_RESET', 'EMAIL', false)`, [TENANT, W]);
    const planted = ((await api('/preferences', { bearer: token(TENANT, W) })).body?.preferences ?? []).find((p) => p.category === 'PASSWORD_RESET');
    check(planted?.enabled === true, 'a row planted to say it is off is ignored: password resets still read as on');

    console.log('');
    console.log('  the categories are a closed set');
    const nearMiss = await api('/preferences', { method: 'PATCH', bearer: token(TENANT, V), body: { category: 'COMMUNITY_REPLIES', enabled: false } });
    check(nearMiss.status === 400 && (await rows(TENANT, V)).length === 0, `a near-miss category is refused, and records nothing (${nearMiss.status})`);

    console.log('');
    console.log('  the unsubscribe link');
    const good = unsubscribeToken({ tenantId: TENANT, userId: V, category: 'COMMUNITY_REPLY' });
    const shown = await api(`/unsubscribe?token=${encodeURIComponent(good)}`);
    // "Changed nothing" is read after a pause: a write the handler started and did not
    // wait for lands after the response. The first version read at once and stayed
    // green with exactly that bug — an unsubscribe fired from GET and not awaited.
    await sleep(1_500);
    check(shown.status === 200 && shown.text.includes('<form method="post"') && (await rows(TENANT, V)).length === 0, `following it shows a button and changes nothing (${shown.status})`);
    const oneClick = await api(`/unsubscribe?token=${encodeURIComponent(good)}`, { method: 'POST', raw: 'List-Unsubscribe=One-Click' });
    check(oneClick.status === 200 && (await replyOn(TENANT, V)) === false, `the one-click POST unsubscribes (${oneClick.status})`);
    const twice = await api(`/unsubscribe?token=${encodeURIComponent(good)}`, { method: 'POST', raw: 'List-Unsubscribe=One-Click' });
    check(twice.status === 200 && (await rows(TENANT, V)).length === 1, 'and a second POST is still fine, and still one row');
    check((await replyOn(OTHER, V)) === true, 'the same user id in another tenant is untouched');

    // Every page carries the headers a page holding a never-expiring credential needs:
    // no Referer (the button's own POST sent the token as one), no caching, nothing
    // loaded, no framing of the one-click button, no sniffing.
    const invalidPage = await api('/unsubscribe?token=not-a-token');
    for (const [what, res] of [['the page with the button', shown], ['the page after unsubscribing', oneClick], ['the page for a bad link', invalidPage]]) {
      const missing = missingPageHeaders(res);
      check(
        /text\/html/.test(res.headers.get('content-type') ?? '') && missing.length === 0,
        `${what} (${res.status}) carries Referrer-Policy, Cache-Control, CSP, X-Frame-Options and nosniff (${missing.length ? `missing or wrong: ${missing.join(', ')}` : 'all'})`,
      );
    }

    // A genuine body and signature, with another user's id pasted into the body.
    const [, goodBody, goodMac] = good.split('.');
    const swappedBody = Buffer.from(JSON.stringify({ t: TENANT, u: W, c: 'COMMUNITY_REPLY' })).toString('base64url');
    const tampered = [
      ['another user under the original signature', `v1.${swappedBody}.${goodMac}`],
      ['a changed signature', `v1.${goodBody}.${Buffer.from(randomBytes(32)).toString('base64url')}`],
      ['a correctly signed token for a password reset', unsubscribeToken({ tenantId: TENANT, userId: W, category: 'PASSWORD_RESET' })],
      ['a token signed with a different secret', unsubscribeToken({ tenantId: TENANT, userId: W, category: 'COMMUNITY_REPLY' }, `${SECRET}-not-it`)],
      ['garbage', 'not-a-token'],
    ];
    const answers = [];
    for (const [, t] of tampered) answers.push((await api(`/unsubscribe?token=${encodeURIComponent(t)}`, { method: 'POST', raw: 'List-Unsubscribe=One-Click' })).status);
    const wRows = await rows(TENANT, W);
    check(
      answers.every((s) => s === 400) && wRows.length === 1 && wRows[0].category === 'PASSWORD_RESET',
      `each tampered token is refused and changes nothing (${tampered.map(([what], i) => `${what}: ${answers[i]}`).join('; ')})`,
    );
    check((await replyOn(TENANT, W)) === true, "and the user they were aimed at still gets replies");
  } catch (err) {
    check(false, `the run stopped early: ${err instanceof Error ? err.message : String(err)}`);
    console.error(`  Notification's output:\n${excerpt(service.log())}`);
  } finally {
    service.child.kill();
    await service.exited;
  }

  console.log('');
  console.log('  a production process will not sign links with the public development secret');
  const production = start({ ...notificationEnv, NODE_ENV: 'production', PORT: String(PORT + 1) });
  const exitCode = await Promise.race([production.exited, sleep(30_000).then(() => 'still running')]);
  if (exitCode === 'still running') production.child.kill();
  check(
    exitCode !== 'still running' && exitCode !== 0 && production.log().includes('UNSUBSCRIBE_SECRET is the development value'),
    `it refuses to start, and says why (exit ${exitCode})`,
  );
  const ownSecret = randomBytes(32).toString('hex');

  /**
   * A plain-http origin in production: every unsubscribe link would point where a reader
   * cannot follow it, in mail that is on by default. Each case has ONE origin wrong, so
   * a guard that refused only when both were wrong would start. One is left unset, which
   * is the deployment that forgot it and got the localhost default; the other is plain
   * http on a real host name, so a guard that looked for "localhost" instead of https
   * would start.
   */
  console.log('  nor with a public origin a reader cannot reach');
  async function refusalFor(overrides, unset = []) {
    const env = { ...notificationEnv, NODE_ENV: 'production', PORT: String(PORT + 1), UNSUBSCRIBE_SECRET: ownSecret, ...overrides };
    for (const name of unset) delete env[name];
    const child = start(env);
    const exit = await Promise.race([child.exited, sleep(30_000).then(() => 'still running')]);
    if (exit === 'still running') {
      child.child.kill();
      await child.exited;
    }
    // Anchored on "Error:": the stack trace also prints the throwing line of source, whose
    // template text would match a looser pattern and name nothing.
    return { exit, named: child.log().match(/Error: refusing to start: (.+?) must be https/)?.[1] ?? '' };
  }
  const unset = await refusalFor({ PUBLIC_APP_URL: 'https://app.example' }, ['NOTIFICATION_PUBLIC_URL']);
  check(
    unset.exit !== 'still running' && unset.exit !== 0 && unset.named === 'NOTIFICATION_PUBLIC_URL',
    `NOTIFICATION_PUBLIC_URL left unset, so localhost: it refuses, naming only that one (exit ${unset.exit}: ${unset.named || 'no such message'})`,
  );
  const plainApp = await refusalFor({ NOTIFICATION_PUBLIC_URL: 'https://notify.example', PUBLIC_APP_URL: 'http://app.example' });
  check(
    plainApp.exit !== 'still running' && plainApp.exit !== 0 && plainApp.named === 'PUBLIC_APP_URL',
    `PUBLIC_APP_URL on plain http at a real host: it refuses, naming only that one (exit ${plainApp.exit}: ${plainApp.named || 'no such message'})`,
  );

  const control = start({
    ...notificationEnv,
    NODE_ENV: 'production',
    PORT: String(PORT + 1),
    UNSUBSCRIBE_SECRET: ownSecret,
    NOTIFICATION_PUBLIC_URL: 'https://notify.example',
    PUBLIC_APP_URL: 'https://app.example',
  });
  const controlUp = await waitFor(async () => (await fetch(`http://127.0.0.1:${PORT + 1}/health`)).ok, 60_000, 500);
  control.child.kill();
  await control.exited;
  check(Boolean(controlUp), 'CONTROL: with a secret of its own and https origins, the same production process starts');

  let deleted = 0;
  for (const tenant of [TENANT, OTHER]) {
    deleted += (await inTenant(owner, tenant, `DELETE FROM "NotificationPreference" WHERE "tenantId" = $1`, [tenant]).catch(() => ({ rowCount: 0 }))).rowCount ?? 0;
  }
  const left = (await inTenant(owner, TENANT, `SELECT count(*)::int AS n FROM "NotificationPreference" WHERE "tenantId" = $1`, [TENANT])).rows[0].n;
  check(deleted > 0 && left === 0, `cleanup: this run's preference rows are gone (${deleted} deleted, ${left} left)`);
  const stuck = await deleteGroupsWhenEmpty(admin, GROUPS.all);
  if (stuck.length > 0) console.log(`    note  consumer group(s) left to expire: ${stuck.join(', ')}`);
  await Promise.allSettled([admin.disconnect(), owner.end()]);

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('a user turns off only what may be turned off, for themselves only, and an unsubscribe link does one thing for one person');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
