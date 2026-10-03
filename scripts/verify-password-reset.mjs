/**
 * Asserts the password reset flow, including the failure modes that return
 * success while doing nothing.
 *
 * The highest-value assertion here is the dullest one: that the password
 * ACTUALLY CHANGES. "User" is the one table in this database under FORCE ROW
 * LEVEL SECURITY, so a reset written outside withTenant matches zero rows —
 * and with a guarded updateMany that is not an error, it is a count of 0. The
 * endpoint would return 200, the token would be spent, every session would be
 * revoked, and the password would be untouched: the user is signed out
 * everywhere and locked out of an account whose reset link is now used.
 *
 * Nothing about that is visible from the response. Only logging in afterwards
 * with the new password proves it happened.
 *
 * Run: node scripts/verify-password-reset.mjs
 */

import pg from 'pg';

const AUTH = process.env['AUTH_URL'] ?? 'http://localhost:3001';
const AUTH_APP_URL =
  process.env['APP_DATABASE_URL'] ??
  'postgresql://eventhub_auth_app:eventhub_auth_app@localhost:5432/eventhub_auth';

const TENANT = 'tenant_acme';
const OTHER_TENANT = 'tenant_globex';
/** Their subdomains: Auth takes the tenant from the gateway's X-Tenant-Slug. */
const SLUG = { [TENANT]: 'acme', [OTHER_TENANT]: 'globex' };
const ADMIN = { email: 'alice@acme.com', password: 'hunter2222' };

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}
function fail(msg) {
  console.error(`FAIL: ${msg}`);
  process.exit(1);
}

const db = new pg.Client({ connectionString: AUTH_APP_URL });

const post = (path, body, headers = {}) =>
  fetch(`${AUTH}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...headers },
    body: JSON.stringify(body),
  }).then(async (r) => ({ status: r.status, body: await r.json().catch(() => null) }));

const login = (email, password, tenantId = TENANT) =>
  post('/login', { email, password }, { 'X-Tenant-Slug': SLUG[tenantId] });

/**
 * Reads the freshly-issued reset token straight from the database.
 *
 * Only the SHA-256 hash is stored, so the raw token cannot be recovered — this
 * mints its own known token by writing the hash directly, which is exactly what
 * /forgot-password does one layer up. Going through the log sink instead would
 * make this test depend on log parsing and on the sink being enabled.
 */
import { createHash, randomBytes } from 'node:crypto';
const sha256 = (t) => createHash('sha256').update(t).digest('hex');

async function plantToken(userId, tenantId, { minutesFromNow = 30 } = {}) {
  const token = randomBytes(32).toString('base64url');
  await db.query(
    `INSERT INTO "PasswordResetToken" (id,"userId","tenantId","tokenHash","expiresAt","createdAt")
     VALUES ($1,$2,$3,$4, now() + ($5 || ' minutes')::interval, now())`,
    [`prt_${randomBytes(8).toString('hex')}`, userId, tenantId, sha256(token), String(minutesFromNow)],
  );
  return token;
}

const userIdFor = async (email, tenantId) => {
  await db.query('BEGIN');
  await db.query(`SELECT set_config('app.tenant_id', $1, true)`, [tenantId]);
  const { rows } = await db.query(`SELECT id FROM "User" WHERE email = $1`, [email]);
  await db.query('COMMIT');
  if (!rows[0]) fail(`no user ${email} in ${tenantId}`);
  return rows[0].id;
};

async function main() {
  await db.connect();
  const userId = await userIdFor(ADMIN.email, TENANT);

  // -------------------------------------------------------------------------
  console.log('\n1. /forgot-password is not an existence oracle');
  // -------------------------------------------------------------------------
  const known = await post('/forgot-password', { email: ADMIN.email }, { 'X-Tenant-Slug': SLUG[TENANT] });
  const unknownEmail = await post('/forgot-password', { email: 'nobody@nowhere.test' }, { 'X-Tenant-Slug': SLUG[TENANT] });
  const unknownTenant = await post('/forgot-password', { email: ADMIN.email }, { 'X-Tenant-Slug': 'no-such-tenant' });
  // Not a subdomain at all. The gateway would never pass one on; a caller that reaches
  // Auth directly can, and it is answered like any other unknown tenant.
  const malformed = await post('/forgot-password', { email: ADMIN.email }, { 'X-Tenant-Slug': 'Not_A.Slug' });

  check(known.status === 202, `known address -> 202, got ${known.status}`);
  check(unknownEmail.status === 202, `unknown address -> 202, got ${unknownEmail.status}`);
  check(unknownTenant.status === 202, `unknown tenant -> 202, got ${unknownTenant.status}`);
  check(malformed.status === 202, `a slug that is not a subdomain -> 202, got ${malformed.status}`);

  const bodies = [known, unknownEmail, unknownTenant, malformed].map((r) => JSON.stringify(r.body));
  check(new Set(bodies).size === 1, `all four bodies identical, got ${[...new Set(bodies)].join(' vs ')}`);
  check(!bodies[0].includes('token'), `no token-shaped field in the response, got ${bodies[0]}`);

  const missingHeader = await post('/forgot-password', { email: ADMIN.email });
  check(missingHeader.status === 400, `missing X-Tenant-Slug -> 400, got ${missingHeader.status}`);

  // The header this replaced. Through the gateway it never arrives, and Auth must not
  // fall back to it when it does.
  const oldHeader = await post('/forgot-password', { email: ADMIN.email }, { 'X-Tenant-ID': TENANT });
  check(oldHeader.status === 400, `X-Tenant-ID alone is no tenant -> 400, got ${oldHeader.status}`);

  // -------------------------------------------------------------------------
  console.log('\n2. a reset actually changes the password');
  // -------------------------------------------------------------------------
  const NEW_PASSWORD = `reset-${Date.now()}`;
  const token = await plantToken(userId, TENANT);

  const reset = await post('/reset-password', { token, password: NEW_PASSWORD });
  check(reset.status === 200, `200, got ${reset.status} ${JSON.stringify(reset.body)}`);

  const withNew = await login(ADMIN.email, NEW_PASSWORD);
  const withOld = await login(ADMIN.email, ADMIN.password);
  check(withNew.status === 200, `the NEW password works, got ${withNew.status}`);
  check(withOld.status === 401, `the OLD password is dead, got ${withOld.status}`);

  // -------------------------------------------------------------------------
  console.log('\n3. a spent token cannot be spent again');
  // -------------------------------------------------------------------------
  const replay = await post('/reset-password', { token, password: 'another-password-1' });
  check(replay.status === 400, `replay -> 400, got ${replay.status}`);
  check(
    replay.body?.error === 'invalid or expired reset token',
    `one generic error, got ${JSON.stringify(replay.body)}`,
  );

  // The generic error must be the SAME for every distinct failure, or it tells a
  // token-prober which guesses were close.
  const garbage = await post('/reset-password', { token: 'not-a-real-token', password: 'another-password-1' });
  const expired = await post('/reset-password', {
    token: await plantToken(userId, TENANT, { minutesFromNow: -5 }),
    password: 'another-password-1',
  });
  check(garbage.status === 400 && expired.status === 400, `unknown and expired both 400, got ${garbage.status}/${expired.status}`);
  check(
    JSON.stringify(garbage.body) === JSON.stringify(replay.body) &&
      JSON.stringify(expired.body) === JSON.stringify(replay.body),
    'used, unknown and expired are indistinguishable',
  );

  // -------------------------------------------------------------------------
  console.log('\n4. one token, one reset, even under a race');
  // -------------------------------------------------------------------------
  // Two racers with DIFFERENT passwords. Read-then-write lets both through and
  // the account ends up holding whichever committed last -- so the victim
  // believes they set their password while the attacker's is the live one.
  let racesWithTwoWinners = 0;
  const ROUNDS = 10;

  for (let i = 0; i < ROUNDS; i += 1) {
    const t = await plantToken(userId, TENANT);
    const a = `race-a-${i}-${Date.now()}`;
    const b = `race-b-${i}-${Date.now()}`;

    const [ra, rb] = await Promise.all([
      post('/reset-password', { token: t, password: a }),
      post('/reset-password', { token: t, password: b }),
    ]);

    const winners = [ra, rb].filter((r) => r.status === 200).length;
    if (winners !== 1) racesWithTwoWinners += 1;

    // Exactly one of the two passwords must work.
    const worksA = (await login(ADMIN.email, a)).status === 200;
    const worksB = (await login(ADMIN.email, b)).status === 200;
    if (worksA && worksB) fail('both racers’ passwords work, which is impossible unless the row was written twice');
    if (!worksA && !worksB) fail('neither password works after a successful reset');
  }

  check(racesWithTwoWinners === 0, `every race had exactly one winner (${racesWithTwoWinners}/${ROUNDS} did not)`);

  // -------------------------------------------------------------------------
  console.log('\n5. a completed reset invalidates tokens issued before it');
  // -------------------------------------------------------------------------
  // The attack this closes: a link harvested from a mailbox at 10:00 must not
  // still work at 10:20, after the victim has noticed and reset it themselves.
  const harvested = await plantToken(userId, TENANT);
  const victims = await plantToken(userId, TENANT);

  const victimReset = await post('/reset-password', { token: victims, password: `recovered-${Date.now()}` });
  check(victimReset.status === 200, `the victim's own reset succeeds, got ${victimReset.status}`);

  const attacker = await post('/reset-password', { token: harvested, password: 'attacker-owns-you' });
  check(attacker.status === 400, `the earlier harvested token is dead, got ${attacker.status}`);

  // -------------------------------------------------------------------------
  console.log('\n6. issuing a new reset supersedes the previous one');
  // -------------------------------------------------------------------------
  const superseded = await plantToken(userId, TENANT);
  await post('/forgot-password', { email: ADMIN.email }, { 'X-Tenant-Slug': SLUG[TENANT] });
  const stale = await post('/reset-password', { token: superseded, password: 'should-not-work-1' });
  check(stale.status === 400, `a token superseded by a new request is dead, got ${stale.status}`);

  // -------------------------------------------------------------------------
  console.log('\n7. a completed reset kills existing sessions');
  // -------------------------------------------------------------------------
  const CURRENT = `recovered-${Date.now()}`;
  const reset7 = await post('/reset-password', {
    token: await plantToken(userId, TENANT),
    password: CURRENT,
  });
  check(reset7.status === 200, `reset succeeded, got ${reset7.status}`);

  const session = await login(ADMIN.email, CURRENT);
  check(session.status === 200, `logged in with the current password, got ${session.status}`);
  const refreshToken = session.body.refreshToken;

  const FINAL = `recovered-final-${Date.now()}`;
  await post('/reset-password', { token: await plantToken(userId, TENANT), password: FINAL });

  const afterReset = await post('/refresh', { refreshToken });
  check(afterReset.status === 401, `a refresh token from before the reset is dead, got ${afterReset.status}`);

  // -------------------------------------------------------------------------
  console.log('\n8. a token cannot be redeemed against the wrong tenant');
  // -------------------------------------------------------------------------
  // The token row carries its own tenantId and /reset-password takes no header,
  // so there is nothing for a caller to state. This asserts the other tenant's
  // account with the SAME email address is untouched -- the seed puts
  // alice@acme.com in both on purpose.
  const otherStillWorks = await login(ADMIN.email, ADMIN.password, OTHER_TENANT);
  check(
    otherStillWorks.status === 200,
    `the same address in ${OTHER_TENANT} still has its original password, got ${otherStillWorks.status}`,
  );

  // -------------------------------------------------------------------------
  // Leave the seeded account as we found it, so re-running this and every other
  // verifier that logs in as alice keeps working.
  // -------------------------------------------------------------------------
  const restore = await post('/reset-password', {
    token: await plantToken(userId, TENANT),
    password: ADMIN.password,
  });
  check(restore.status === 200, 'restored the seeded password');
  const restored = await login(ADMIN.email, ADMIN.password);
  check(restored.status === 200, `the seeded password works again, got ${restored.status}`);

  await db.end();

  console.log('');
  if (failures > 0) {
    console.error(`${failures} assertion(s) failed`);
    process.exit(1);
  }
  console.log('password reset verified: the password actually changes, the token spends once');
  console.log('even under a race, and recovery invalidates every other live token and session');
}

main().catch(async (err) => {
  console.error(err);
  await db.end().catch(() => {});
  process.exit(1);
});
