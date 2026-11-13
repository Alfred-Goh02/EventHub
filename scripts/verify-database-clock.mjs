/**
 * Asserts that what Auth expires, and what Community orders a thread by, is stamped and
 * judged on the database's clock: the one every reader of those values compares them with.
 *
 * THEY WERE NOT. Auth stamped a reset token's expiry as `new Date(Date.now() + 30 min)`
 * and judged it with `record.expiresAt < new Date()`, both on Auth's clock; a refresh
 * token's the same way, thirty days. Auth replicas whose clocks disagree with each other,
 * or with Postgres, gave one link a different life. Measured with Auth an hour fast (the
 * numbers are in the PR that added this file): a thirty-minute link was stored to expire
 * 5400 s after the database's now(), and a link the database had stamped thirty minutes
 * ahead was refused as expired, because to Auth it was half an hour gone.
 *
 * AND THE GUARDED CLAIMS DID NOT RE-CHECK EXPIRY. POST /reset-password and POST /refresh
 * each read the token, then spent it with `UPDATE ... WHERE usedAt IS NULL AND revokedAt
 * IS NULL`. lookupResetToken's comment said that UPDATE "re-checks the same conditions";
 * it re-checked two of three. A token that expired between the read and the claim -- an
 * argon2 hash sits between them on the reset path -- was still spent. The invitation
 * claim in POST /register already carried its expiry, in SQL, and is the model.
 *
 * AND A COMMENT'S PLACE IN ITS THREAD WAS COMMUNITY'S CLOCK. A comment's id starts with
 * the milliseconds it was written at, and the thread is ORDER BY path, so siblings sort
 * by that prefix -- taken from `Date.now()`. With two replicas whose clocks disagree, a
 * reply written first sorted last, under a createdAt from the database that said
 * otherwise. The prefix now comes from the same now() the row's createdAt is stamped from.
 *
 * THE FIXTURE. Every service started here runs from dist, on a port of this run's, and
 * the ones under test an HOUR FAST (scripts/lib/skewed-clock.mjs): on a machine whose
 * clocks agree, a stamp from the process lands on the database's millisecond and no check
 * can tell the two apart. Every comparison is made in SQL, against
 * `now() AT TIME ZONE 'UTC'`; a time read into JS would be compared on a third clock.
 *
 *   Auth, an hour fast, in a tenant and with a user made for this run:
 *     - a refresh token from /login, and its successor from /refresh, expire 30 days
 *       after now(), not 30 days and an hour;
 *     - one whose expiresAt SQL set to now() + 30 min still rotates (an hour-fast JS
 *       comparison refuses it), and one at now() - 1 s is refused, as a control;
 *     - a reset from /forgot-password expires 1800 s after now() -- the row, and the
 *       expiresAt the email request tells Notification -- not 5400;
 *     - a reset token SQL set to now() + 30 min is redeemable, and the password really
 *       changes; one at now() - 1 s is refused, and it does not;
 *     - for both kinds of token, one that expires BETWEEN the read and the claim is
 *       refused by the claim. The interleaving is forced, not hoped for: this run holds
 *       the row's lock, sends the request, waits until Postgres shows Auth's claim
 *       blocked on that lock, moves expiresAt into the past, and lets go. The token is
 *       planted valid by both clocks, so only the claim can refuse it;
 *     - every revokedAt and usedAt Auth writes is now(), not an hour ahead. None is
 *       compared with a time -- each is only tested for NULL -- but each was moved into
 *       the statement that sets it, and each is asserted so the move is not a claim.
 *   Community, one replica an hour fast and one not:
 *     - a reply written first, on the fast replica, comes first in the thread;
 *     - each comment's id carries its own createdAt, to the millisecond.
 *
 * WHAT IT CANNOT DELETE, AND WHAT IT LEAVES. The app role has no DELETE on
 * PasswordResetToken; this run's tokens go with this run's user, by the schema's own
 * ON DELETE CASCADE, counted before and after. Auth's outbox row is deleted as the relay
 * role, which is how every email verifier cleans up; compose's relay-auth may already
 * have published it, so locally one reset email to this run's invented address may reach
 * Mailpit. Community's rows go as the owner, inside this run's tenant.
 *
 * Needs Postgres and Redis up, and Auth and Community built:
 *   node scripts/verify-database-clock.mjs
 */

import { spawn } from 'node:child_process';
import { createHash, createPrivateKey, randomBytes, sign } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import pg from 'pg';
import { serviceEnv } from './lib/service-env.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const RUN = randomBytes(4).toString('hex');
const TENANT = `tenant_dbclock_${RUN}`;
const SLUG = `dbclock-${RUN}`;

const AUTH_PORT = 3951;
const FAST_COMMUNITY_PORT = 3953;
const COMMUNITY_PORT = 3954;

const SKEW = `--import=${pathToFileURL(join(ROOT, 'scripts', 'lib', 'skewed-clock.mjs')).href}`;
const SKEW_LINE = 'clock skewed by +3600s for this process';

/** How far behind the database's now() a stamp may be by the time it is read. The skew is 3600. */
const NEAR_S = 10;
const RESET_TTL_S = 30 * 60;
const REFRESH_TTL_S = 30 * 24 * 60 * 60;
const GENERIC_RESET_ERROR = 'invalid or expired reset token';

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const fmt = (s) => (typeof s === 'number' ? `${s >= 0 ? '+' : ''}${s.toFixed(3)} s` : String(s));
/** A stamp written just now, on the database's clock. From an hour-fast process it is +3600. */
const near = (s) => typeof s === 'number' && s > -NEAR_S && s < 1;
/** An expiry `ttl` seconds after the database's now(). From an hour-fast process it is ttl + 3600. */
const after = (s, ttl) => typeof s === 'number' && s > ttl - NEAR_S && s <= ttl + 1;
const sha256 = (t) => createHash('sha256').update(t).digest('hex');
const secret = () => randomBytes(32).toString('base64url');
const excerpt = (log) => (log.length <= 2000 ? log : `${log.slice(0, 1200)}\n  [...]\n${log.slice(-800)}`);

/** A transaction with app.tenant_id set, committed. The context Auth and Community write in. */
async function inTenant(db, tenant, fn) {
  await db.query('BEGIN');
  try {
    await db.query(`SELECT set_config('app.tenant_id', $1, true)`, [tenant]);
    const out = await fn();
    await db.query('COMMIT');
    return out;
  } catch (err) {
    await db.query('ROLLBACK').catch(() => {});
    throw err;
  }
}

/** One of this run's services, from dist, on a port nothing else answers on. */
async function start(service, port, env, ready = '/health') {
  const taken = await fetch(`http://127.0.0.1:${port}/health`).then(() => true, () => false);
  if (taken) {
    check(false, `something already answers on :${port}; stop it before running this`);
    return { up: false, log: () => '', stop: async () => {} };
  }
  const child = spawn(process.execPath, ['dist/index.js'], { cwd: join(ROOT, 'services', service), env, stdio: ['ignore', 'pipe', 'pipe'] });
  let log = '';
  child.stdout.on('data', (d) => (log += d));
  child.stderr.on('data', (d) => (log += d));
  let up = false;
  for (let i = 0; i < 120 && !up && child.exitCode === null; i += 1) {
    up = await fetch(`http://127.0.0.1:${port}${ready}`).then((r) => r.ok, () => false);
    if (!up) await sleep(500);
  }
  const stop = async () => {
    if (child.exitCode === null) {
      child.kill();
      await new Promise((r) => (child.exitCode !== null ? r() : child.once('exit', r)));
    }
  };
  return { up, log: () => log, stop };
}

async function api(port, path, { method = 'POST', body, headers = {}, bearer } = {}) {
  const res = await fetch(`http://127.0.0.1:${port}${path}`, {
    method,
    headers: {
      ...(body ? { 'Content-Type': 'application/json' } : {}),
      ...(bearer ? { Authorization: `Bearer ${bearer}` } : {}),
      ...headers,
    },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  const text = await res.text();
  let json = null;
  try {
    json = text ? JSON.parse(text) : null;
  } catch {}
  return { status: res.status, json, text };
}

/**
 * Holds `id`'s row lock, sends `request`, waits until Postgres shows another backend
 * blocked on that lock, runs `meanwhile` in the holding transaction, and commits.
 *
 * This is how "the token expires between the read and the claim" is made to happen
 * rather than waited for. A plain SELECT does not wait on FOR UPDATE, so the service's
 * read goes through and sees the token live; its claim is an UPDATE, which does wait.
 * Under READ COMMITTED the claim then re-evaluates its WHERE against the row as this
 * transaction left it.
 */
async function whileClaimWaits(url, table, id, request, meanwhile) {
  const holder = new pg.Client({ connectionString: url });
  const watcher = new pg.Client({ connectionString: url });
  await holder.connect();
  await watcher.connect();
  try {
    await holder.query('BEGIN');
    const locked = (await holder.query(`SELECT id FROM "${table}" WHERE id = $1 FOR UPDATE`, [id])).rowCount;
    const pid = (await holder.query('SELECT pg_backend_pid() AS pid')).rows[0].pid;
    const pending = request();
    let waiter = null;
    for (let i = 0; i < 100 && !waiter; i += 1) {
      const r = await watcher.query(
        `SELECT left(query, 60) AS query FROM pg_stat_activity
          WHERE $1 = ANY(pg_blocking_pids(pid)) AND wait_event_type = 'Lock'`,
        [pid],
      );
      waiter = r.rows[0] ?? null;
      if (!waiter) await sleep(50);
    }
    const changed = await meanwhile(holder);
    await holder.query('COMMIT');
    return { locked, waiter, changed, response: await pending };
  } catch (err) {
    await holder.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    await holder.end().catch(() => {});
    await watcher.end().catch(() => {});
  }
}

// ---------------------------------------------------------------------------
// AUTH
// ---------------------------------------------------------------------------

async function authCheck() {
  const env = serviceEnv(ROOT, 'auth', { PORT: String(AUTH_PORT), NODE_OPTIONS: SKEW });
  const relayUrl =
    process.env['AUTH_RELAY_DATABASE_URL'] ?? 'postgresql://eventhub_auth_relay:eventhub_auth_relay@localhost:5432/eventhub_auth';
  const USER_ID = `user_dbclock_${RUN}`;
  const EMAIL = `clock-${RUN}@dbclock.example`;
  const passwords = { start: `start-${RUN}`, reset: `reset-${RUN}`, refused: `refused-${RUN}`, raced: `raced-${RUN}` };

  const app = new pg.Client({ connectionString: env.APP_DATABASE_URL });
  const relayDb = new pg.Client({ connectionString: relayUrl });
  await app.connect();
  await relayDb.connect();
  let svc = null;

  /** Seconds from the database's now() to a column of one row, computed by Postgres. */
  const offset = async (table, column, id) =>
    (
      await app.query(
        `SELECT extract(epoch FROM "${column}" - (now() AT TIME ZONE 'UTC'))::float8 AS s FROM "${table}" WHERE id = $1`,
        [id],
      )
    ).rows[0]?.s ?? null;
  const refreshRow = async (token) =>
    (await app.query(`SELECT id, "familyId", "usedAt" IS NULL AS unused FROM "RefreshToken" WHERE "tokenHash" = $1`, [sha256(token)])).rows[0] ?? null;
  const familySize = async (familyId) =>
    (await app.query(`SELECT count(*)::int AS n FROM "RefreshToken" WHERE "familyId" = $1`, [familyId])).rows[0].n;
  /** A reset token written by SQL, expiring at now() + `interval` on the database's clock. */
  const plantReset = async (interval) => {
    const token = secret();
    const id = `prt_dbclock_${RUN}_${randomBytes(4).toString('hex')}`;
    await app.query(
      `INSERT INTO "PasswordResetToken" (id, "userId", "tenantId", "tokenHash", "expiresAt")
       VALUES ($1, $2, $3, $4, (now() AT TIME ZONE 'UTC') + $5::interval)`,
      [id, USER_ID, TENANT, sha256(token), interval],
    );
    return { id, token };
  };
  const login = (password) => api(AUTH_PORT, '/login', { body: { email: EMAIL, password }, headers: { 'X-Tenant-Slug': SLUG } });
  const usedAtIsNull = async (id) => (await app.query(`SELECT "usedAt" IS NULL AS unused FROM "PasswordResetToken" WHERE id = $1`, [id])).rows[0]?.unused;

  try {
    console.log('');
    console.log("  Auth, an hour fast: the fixture");
    // The hash is computed with Auth's own argon2, so the fixture does not depend on the
    // reset path it is about to test.
    const argon2 = createRequire(join(ROOT, 'services', 'auth', 'package.json'))('@node-rs/argon2');
    const startHash = await argon2.hash(passwords.start);
    await app.query(`INSERT INTO "Tenant" (id, name, slug, "updatedAt") VALUES ($1, $2, $3, now())`, [TENANT, `Clock ${RUN}`, SLUG]);
    await inTenant(app, TENANT, () =>
      app.query(
        `INSERT INTO "User" (id, "tenantId", email, "passwordHash", role, "updatedAt") VALUES ($1, $2, $3, $4, 'ADMIN', now())`,
        [USER_ID, TENANT, EMAIL, startHash],
      ),
    );

    svc = await start('auth', AUTH_PORT, env);
    check(svc.up, svc.up ? `CONTROL: this run's own Auth answers on :${AUTH_PORT}` : `Auth never came up:\n${excerpt(svc.log())}`);
    if (!svc.up) return;
    check(svc.log().includes(SKEW_LINE), 'CONTROL: its clock runs an hour fast, so a stamp from it cannot pass for the database\'s');

    const first = await login(passwords.start);
    check(first.status === 200 && Boolean(first.json?.refreshToken), `CONTROL: this run's user signs in (${first.status})`);
    if (!first.json?.refreshToken) return;
    // Auth's own clock, read off the token it just signed, placed by Postgres.
    const iat = JSON.parse(Buffer.from(first.json.accessToken.split('.')[1], 'base64url').toString()).iat;
    const authClock = (await app.query(`SELECT ($1::float8 - extract(epoch FROM now()))::float8 AS s`, [iat])).rows[0].s;
    check(
      authClock > 3590 && authClock < 3610,
      `CONTROL: and Postgres puts that Auth's clock an hour ahead of now() (its token's iat is ${fmt(authClock)}), so the fixture can show the failure`,
    );
    const admin = first.json.accessToken;

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  Auth: refresh tokens');
    const a = await refreshRow(first.json.refreshToken);
    const aExpires = await offset('RefreshToken', 'expiresAt', a.id);
    check(after(aExpires, REFRESH_TTL_S), `a refresh token from /login expires 30 days after the database's now() (${fmt(aExpires)}; from Auth's clock it would be ${REFRESH_TTL_S + 3600})`);

    await app.query(`UPDATE "RefreshToken" SET "expiresAt" = (now() AT TIME ZONE 'UTC') + interval '30 minutes' WHERE id = $1`, [a.id]);
    const rotated = await api(AUTH_PORT, '/refresh', { body: { refreshToken: first.json.refreshToken } });
    check(
      rotated.status === 200 && Boolean(rotated.json?.refreshToken),
      `one SQL set to expire 30 minutes after now() still rotates (${rotated.status}; to a JS comparison an hour fast it ran out 30 minutes ago)`,
    );
    const aUsed = await offset('RefreshToken', 'usedAt', a.id);
    check(near(aUsed), `the spent token's usedAt is the database's now() (${fmt(aUsed)})`);
    if (rotated.json?.refreshToken) {
      const b = await refreshRow(rotated.json.refreshToken);
      const bExpires = await offset('RefreshToken', 'expiresAt', b.id);
      check(after(bExpires, REFRESH_TTL_S), `its successor expires 30 days after now() too (${fmt(bExpires)})`);

      await app.query(`UPDATE "RefreshToken" SET "expiresAt" = (now() AT TIME ZONE 'UTC') - interval '1 second' WHERE id = $1`, [b.id]);
      const before = await familySize(b.familyId);
      const refused = await api(AUTH_PORT, '/refresh', { body: { refreshToken: rotated.json.refreshToken } });
      const sizeAfter = await familySize(b.familyId);
      const still = await refreshRow(rotated.json.refreshToken);
      check(
        refused.status === 401 && sizeAfter === before && still?.unused === true,
        `CONTROL: one SQL set to 1 s before now() is refused, unspent, with no successor (${refused.status}; family ${before} -> ${sizeAfter}, unused ${still?.unused})`,
      );
    }

    const raced = await login(passwords.start);
    const c = raced.json?.refreshToken ? await refreshRow(raced.json.refreshToken) : null;
    if (c) {
      const out = await whileClaimWaits(
        env.APP_DATABASE_URL,
        'RefreshToken',
        c.id,
        () => api(AUTH_PORT, '/refresh', { body: { refreshToken: raced.json.refreshToken } }),
        async (holder) =>
          (await holder.query(`UPDATE "RefreshToken" SET "expiresAt" = (now() AT TIME ZONE 'UTC') - interval '1 second' WHERE id = $1`, [c.id])).rowCount,
      );
      check(
        out.locked === 1 && out.waiter !== null && out.changed === 1,
        `CONTROL: /refresh read the token live and its claim waited on the row this run held, while it expired (${out.waiter ? JSON.stringify(out.waiter.query) : 'nothing waited'})`,
      );
      const sizeAfter = await familySize(c.familyId);
      const still = await refreshRow(raced.json.refreshToken);
      check(
        out.response.status === 401 && sizeAfter === 1 && still?.unused === true,
        `a refresh token that expires between the read and the claim is refused by the claim: unspent, no successor (${out.response.status}; family of ${sizeAfter}, unused ${still?.unused})`,
      );
    } else {
      check(false, `CONTROL: a second sign-in for the claim race (${raced.status})`);
    }

    const leaving = await login(passwords.start);
    const d = leaving.json?.refreshToken ? await refreshRow(leaving.json.refreshToken) : null;
    const out = d ? await api(AUTH_PORT, '/logout', { body: { refreshToken: leaving.json.refreshToken } }) : { status: null };
    const dRevoked = d ? await offset('RefreshToken', 'revokedAt', d.id) : null;
    check(out.status === 204 && near(dRevoked), `/logout's revokedAt is the database's now() (${out.status}, ${fmt(dRevoked)})`);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  Auth: password resets');
    const session = await login(passwords.start);
    const e = session.json?.refreshToken ? await refreshRow(session.json.refreshToken) : null;
    check(Boolean(e), `CONTROL: a session for the reset to end (${session.status})`);

    const superseded = await plantReset('30 minutes');
    const asked = await api(AUTH_PORT, '/forgot-password', { body: { email: EMAIL }, headers: { 'X-Tenant-Slug': SLUG } });
    const issued = (
      await app.query(
        `SELECT id, extract(epoch FROM "expiresAt" - (now() AT TIME ZONE 'UTC'))::float8 AS s
           FROM "PasswordResetToken" WHERE "userId" = $1 AND "usedAt" IS NULL AND "revokedAt" IS NULL`,
        [USER_ID],
      )
    ).rows;
    check(asked.status === 202 && issued.length === 1, `CONTROL: /forgot-password issued one live reset, and only one (${asked.status}, ${issued.length} live)`);
    check(after(issued[0]?.s, RESET_TTL_S), `a reset issued through the API expires 1800 s after the database's now() (${fmt(issued[0]?.s)}; from Auth's clock it would be ${RESET_TTL_S + 3600})`);
    const envelope = (
      await relayDb.query(
        `SELECT extract(epoch FROM (payload->>'expiresAt')::timestamptz - now())::float8 AS s
           FROM "OutboxMessage" WHERE "tenantId" = $1 AND "eventType" = 'PasswordResetRequested'`,
        [TENANT],
      )
    ).rows;
    check(
      envelope.length === 1 && after(envelope[0].s, RESET_TTL_S),
      `and the expiry its email request tells Notification is the same 1800 s (${envelope.length} request(s), ${fmt(envelope[0]?.s)})`,
    );
    const supersededAt = await offset('PasswordResetToken', 'revokedAt', superseded.id);
    check(near(supersededAt), `the earlier reset it superseded was revoked at the database's now() (${fmt(supersededAt)})`);

    const redeemed = await plantReset('30 minutes');
    const reset = await api(AUTH_PORT, '/reset-password', { body: { token: redeemed.token, password: passwords.reset } });
    const signsIn = await login(passwords.reset);
    check(
      reset.status === 200 && signsIn.status === 200,
      `a reset SQL set to expire 30 minutes after now() is redeemable, and the new password signs in (${reset.status}, ${signsIn.status}; to a JS comparison an hour fast it ran out 30 minutes ago)`,
    );
    const usedAt = await offset('PasswordResetToken', 'usedAt', redeemed.id);
    check(near(usedAt), `the redeemed reset's usedAt is the database's now() (${fmt(usedAt)})`);
    const siblingAt = issued[0] ? await offset('PasswordResetToken', 'revokedAt', issued[0].id) : null;
    check(near(siblingAt), `the live reset it revoked was revoked at the database's now() (${fmt(siblingAt)})`);
    const sessionAt = e ? await offset('RefreshToken', 'revokedAt', e.id) : null;
    check(near(sessionAt), `and the session it ended was ended at the database's now() (${fmt(sessionAt)})`);

    const expired = await plantReset('-1 second');
    const late = await api(AUTH_PORT, '/reset-password', { body: { token: expired.token, password: passwords.refused } });
    const lateUnused = await usedAtIsNull(expired.id);
    const lateSignIn = await login(passwords.refused);
    check(
      late.status === 400 && late.json?.error === GENERIC_RESET_ERROR && lateUnused === true && lateSignIn.status === 401,
      `CONTROL: a reset SQL set to 1 s before now() is refused with the one generic error, unspent, and its password does not sign in (${late.status} ${late.text}, unused ${lateUnused}, ${lateSignIn.status})`,
    );

    // Valid by both clocks when Auth reads it, so nothing but the claim can refuse it.
    const racing = await plantReset('2 hours');
    const outcome = await whileClaimWaits(
      env.APP_DATABASE_URL,
      'PasswordResetToken',
      racing.id,
      () => api(AUTH_PORT, '/reset-password', { body: { token: racing.token, password: passwords.raced } }),
      async (holder) =>
        (await holder.query(`UPDATE "PasswordResetToken" SET "expiresAt" = (now() AT TIME ZONE 'UTC') - interval '1 second' WHERE id = $1`, [racing.id])).rowCount,
    );
    check(
      outcome.locked === 1 && outcome.waiter !== null && outcome.changed === 1,
      `CONTROL: /reset-password read the token live and its claim waited on the row this run held, while it expired (${outcome.waiter ? JSON.stringify(outcome.waiter.query) : 'nothing waited'})`,
    );
    const racedUnused = await usedAtIsNull(racing.id);
    const racedSignIn = await login(passwords.raced);
    const keptSignIn = await login(passwords.reset);
    check(
      outcome.response.status === 400 && outcome.response.json?.error === GENERIC_RESET_ERROR && racedUnused === true && racedSignIn.status === 401 && keptSignIn.status === 200,
      `a reset that expires between the read and the claim is refused by the claim, with the same error: unspent, and the password unchanged (${outcome.response.status}, unused ${racedUnused}, new ${racedSignIn.status}, kept ${keptSignIn.status})`,
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  Auth: an invitation withdrawn');
    const inviteId = `inv_dbclock_${RUN}`;
    const invitee = `invitee-${RUN}@dbclock.example`;
    await app.query(
      `INSERT INTO "Invite" (id, "tenantId", email, role, "tokenHash", "invitedBy", "expiresAt")
       VALUES ($1, $2, $3, 'ATTENDEE', $4, $5, (now() AT TIME ZONE 'UTC') + interval '1 day')`,
      [inviteId, TENANT, invitee, sha256(secret()), USER_ID],
    );
    const withdrawn = await api(AUTH_PORT, '/invites/revoke', { body: { email: invitee }, bearer: admin });
    const inviteAt = await offset('Invite', 'revokedAt', inviteId);
    check(
      withdrawn.status === 200 && withdrawn.json?.revoked === 1 && near(inviteAt),
      `/invites/revoke's revokedAt is the database's now() (${withdrawn.status} ${withdrawn.text}, ${fmt(inviteAt)})`,
    );
  } catch (err) {
    check(false, `Auth's section stopped early: ${err instanceof Error ? err.stack : String(err)}`);
    if (svc) console.error(excerpt(svc.log()));
  } finally {
    if (svc) await svc.stop();
    /**
     * As the roles and in the context each row was written in, and counted. The app role
     * has no DELETE on PasswordResetToken, so the tokens go with the user, by the schema's
     * ON DELETE CASCADE: counted before, and after.
     */
    const count = async (table) => (await app.query(`SELECT count(*)::int AS n FROM "${table}" WHERE "userId" = $1`, [USER_ID])).rows[0].n;
    const result = {};
    try {
      result.resetsBefore = await count('PasswordResetToken');
      result.refreshBefore = await count('RefreshToken');
      result.invites = (await app.query(`DELETE FROM "Invite" WHERE "tenantId" = $1`, [TENANT])).rowCount;
      result.outbox = (await relayDb.query(`DELETE FROM "OutboxMessage" WHERE "tenantId" = $1`, [TENANT])).rowCount;
      result.users = await inTenant(app, TENANT, async () => (await app.query(`DELETE FROM "User" WHERE id = $1`, [USER_ID])).rowCount);
      result.tenants = (await app.query(`DELETE FROM "Tenant" WHERE id = $1`, [TENANT])).rowCount;
      result.resetsAfter = await count('PasswordResetToken');
      result.refreshAfter = await count('RefreshToken');
      result.left =
        (await app.query(`SELECT (SELECT count(*) FROM "Tenant" WHERE id = $1)::int + (SELECT count(*) FROM "Invite" WHERE "tenantId" = $1)::int AS n`, [TENANT])).rows[0].n +
        (await relayDb.query(`SELECT count(*)::int AS n FROM "OutboxMessage" WHERE "tenantId" = $1`, [TENANT])).rows[0].n +
        (await inTenant(app, TENANT, () => app.query(`SELECT count(*)::int AS n FROM "User" WHERE "tenantId" = $1`, [TENANT]))).rows[0].n;
    } catch (err) {
      result.error = err.message;
    }
    check(
      !result.error && result.users === 1 && result.tenants === 1 && result.resetsBefore > 0 && result.resetsAfter === 0 && result.refreshAfter === 0 && result.left === 0,
      `cleanup: deleted ${result.error ?? `${result.users} user (with it ${result.resetsBefore} reset and ${result.refreshBefore} refresh token(s); ${result.resetsAfter} and ${result.refreshAfter} left), ${result.invites} invite, ${result.outbox} outbox row and ${result.tenants} tenant; ${result.left} left`}`,
    );
    await app.end().catch(() => {});
    await relayDb.end().catch(() => {});
  }
}

// ---------------------------------------------------------------------------
// COMMUNITY
// ---------------------------------------------------------------------------

async function communityCheck() {
  console.log('');
  console.log("  Community: a thread's order, one replica an hour fast and one not");
  const publicKey = readFileSync(join(ROOT, 'services', 'auth', 'keys', 'public.pem')).toString('base64');
  const base = {
    AUTH_PUBLIC_KEY_B64: publicKey,
    // No fan-out consumer: it would join the shared group. See verify-community-posts.mjs.
    FEED_CONSUMER_ENABLED: 'false',
  };
  const fastEnv = serviceEnv(ROOT, 'community', { ...base, PORT: String(FAST_COMMUNITY_PORT), NODE_OPTIONS: SKEW });
  const plainEnv = serviceEnv(ROOT, 'community', { ...base, PORT: String(COMMUNITY_PORT) });
  const owner = new pg.Client({ connectionString: plainEnv.DATABASE_URL });
  await owner.connect();
  const signing = createPrivateKey(readFileSync(join(ROOT, 'services', 'auth', 'keys', 'private.pem')));
  const b64url = (v) => Buffer.from(v).toString('base64url');
  const AUTHOR = `user_dbclock_author_${RUN}`;
  /** Good for two hours: the fast replica checks exp on ITS clock, an hour ahead. */
  const now = Math.floor(Date.now() / 1000);
  const head = b64url(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
  const body = b64url(JSON.stringify({ sub: AUTHOR, tenantId: TENANT, role: 'ATTENDEE', iss: plainEnv.JWT_ISSUER, aud: plainEnv.JWT_AUDIENCE, iat: now, exp: now + 7200 }));
  const bearer = `${head}.${body}.${b64url(sign('sha256', Buffer.from(`${head}.${body}`), signing))}`;
  let fast = null;
  let plain = null;
  try {
    [fast, plain] = await Promise.all([
      start('community', FAST_COMMUNITY_PORT, fastEnv, '/ready'),
      start('community', COMMUNITY_PORT, plainEnv, '/ready'),
    ]);
    check(fast.up && plain.up, `CONTROL: two Communities of this run's are ready, on :${FAST_COMMUNITY_PORT} and :${COMMUNITY_PORT}${fast.up && plain.up ? '' : `:\n${excerpt(fast.log() + plain.log())}`}`);
    if (!fast.up || !plain.up) return;
    check(
      fast.log().includes(SKEW_LINE) && !plain.log().includes(SKEW_LINE),
      `CONTROL: the first runs an hour fast and the second does not`,
    );

    const space = await api(COMMUNITY_PORT, '/spaces', { bearer, body: { slug: `clock-${RUN}`, name: `Clock ${RUN}` } });
    const post = space.json?.id ? await api(COMMUNITY_PORT, `/spaces/${space.json.id}/posts`, { bearer, body: { body: `what time is it? ${RUN}` } }) : null;
    const postId = post?.json?.id;
    check(space.status === 201 && post?.status === 201 && Boolean(postId), `CONTROL: a space and a post to reply under (${space.status}, ${post?.status})`);
    if (!postId) return;

    const early = await api(FAST_COMMUNITY_PORT, `/posts/${postId}/comments`, { bearer, body: { body: 'first, through the fast replica' } });
    await sleep(50);
    const later = await api(COMMUNITY_PORT, `/posts/${postId}/comments`, { bearer, body: { body: 'second, through the other' } });
    const ids = [early.json?.id, later.json?.id];
    check(early.status === 201 && later.status === 201 && ids.every(Boolean), `CONTROL: two replies, one through each (${early.status}, ${later.status})`);
    if (!ids.every(Boolean)) return;

    /** The id's time prefix: base36 milliseconds, nine characters after the leading 'c'. */
    const prefixMs = (id) => parseInt(id.slice(1, 10), 36);
    const rows = await inTenant(owner, TENANT, () =>
      owner.query(
        `SELECT c.id,
                (extract(epoch FROM c."createdAt") * 1000)::bigint AS "createdMs",
                ($2::bigint - (extract(epoch FROM c."createdAt") * 1000)::bigint)::int AS "earlyApart",
                ($3::bigint - (extract(epoch FROM c."createdAt") * 1000)::bigint)::int AS "laterApart"
           FROM "Comment" c WHERE c.id = ANY($1::text[])`,
        [ids, prefixMs(ids[0]), prefixMs(ids[1])],
      ),
    );
    const byId = new Map(rows.rows.map((r) => [r.id, r]));
    const e = byId.get(ids[0]);
    const l = byId.get(ids[1]);
    check(
      Boolean(e && l) && BigInt(e.createdMs) < BigInt(l.createdMs),
      `CONTROL: by the database's clock the reply through the fast replica was written first (createdAt ${e?.createdMs} < ${l?.createdMs} ms)`,
    );

    const thread = await api(COMMUNITY_PORT, `/posts/${postId}/comments`, { method: 'GET', bearer });
    const order = (thread.json?.comments ?? []).map((c) => c.id);
    check(
      thread.status === 200 && order.length === 2 && order[0] === ids[0] && order[1] === ids[1],
      `and it comes first in the thread (${order.map((id) => (id === ids[0] ? 'fast' : id === ids[1] ? 'other' : id)).join(', ') || thread.status}; ordered by an hour-fast prefix it would come last)`,
    );
    check(
      e?.earlyApart === 0 && l?.laterApart === 0,
      `each reply's id carries its own createdAt, to the millisecond (${e?.earlyApart} ms and ${l?.laterApart} ms apart; from the fast replica's clock the first would be +3600000)`,
    );
  } catch (err) {
    check(false, `Community's section stopped early: ${err instanceof Error ? err.stack : String(err)}`);
  } finally {
    await Promise.all([fast?.stop(), plain?.stop()]);
    /**
     * As the owner, INSIDE this run's tenant: FORCE row security binds the owner too, so
     * outside it the deletes would match nothing and the count would see nothing. The
     * deleted count is the control; "0 deleted, 0 left" is the bug.
     */
    const tables = ['FeedEntry', 'Reaction', 'Comment', 'Post', 'OutboxMessage', 'Membership', 'CommunitySpace'];
    const deleted = {};
    let left = 0;
    try {
      for (const table of tables) {
        deleted[table] = await inTenant(owner, TENANT, async () => (await owner.query(`DELETE FROM "${table}" WHERE "tenantId" = $1`, [TENANT])).rowCount);
      }
      for (const table of tables) {
        left += (await inTenant(owner, TENANT, () => owner.query(`SELECT count(*)::int AS n FROM "${table}" WHERE "tenantId" = $1`, [TENANT]))).rows[0].n;
      }
    } catch (err) {
      deleted.error = err.message;
    }
    if (deleted.CommunitySpace !== undefined || deleted.error) {
      check(
        !deleted.error && deleted.CommunitySpace === 1 && left === 0,
        `cleanup: deleted ${deleted.error ?? Object.entries(deleted).map(([t, n]) => `${n} ${t}`).join(', ')}; ${left} left`,
      );
    }
    await owner.end().catch(() => {});
  }
}

async function main() {
  console.log(`  run ${RUN}, tenant ${TENANT}`);
  for (const section of [authCheck, communityCheck]) {
    try {
      await section();
    } catch (err) {
      check(false, `a section stopped early: ${err instanceof Error ? err.stack : String(err)}`);
    }
  }
  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    return;
  }
  console.log("every expiry, claim and stamp Auth writes, and a thread's order in Community, is the database's clock, whatever the app server's says");
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
