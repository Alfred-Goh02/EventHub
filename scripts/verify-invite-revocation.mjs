/**
 * Asserts that an invitation can be withdrawn, and that withdrawing one cannot
 * reach across tenants.
 *
 * `Invite.revokedAt` was read in two places and written in none, so the column
 * was decorative: a mis-sent invitation stayed redeemable for its full seven
 * days and whoever received it could join the tenant at the role it named.
 *
 * The cross-tenant assertion is the one that needs a test rather than a reading.
 * Invite is deliberately RLS-EXEMPT -- a tenant policy would filter out the very
 * row that tells /register which tenant to use -- so unlike almost every other
 * write in this service there is no policy underneath to catch a missing scope.
 * The tenantId filter in the query is the only thing standing between an admin
 * and another tenant's invitations, and nothing but this checks it is there.
 *
 * Run: node scripts/verify-invite-revocation.mjs
 */

import { createHash, randomBytes } from 'node:crypto';
import pg from 'pg';

const AUTH = process.env['AUTH_URL'] ?? 'http://localhost:3001';
const AUTH_APP_URL =
  process.env['APP_DATABASE_URL'] ??
  'postgresql://eventhub_auth_app:eventhub_auth_app@localhost:5432/eventhub_auth';

const TENANT = 'tenant_acme';
const OTHER_TENANT = 'tenant_globex';
/** Their subdomains: Auth takes the tenant from the gateway's X-Tenant-Slug. */
const TENANT_SLUG = 'acme';
const OTHER_TENANT_SLUG = 'globex';
const ADMIN = { email: 'alice@acme.com', password: 'hunter2222' };

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}
function fail(msg) {
  console.error(`FAIL: ${msg}`);
  process.exit(1);
}

const db = new pg.Client({ connectionString: AUTH_APP_URL });
const sha256 = (t) => createHash('sha256').update(t).digest('hex');

/**
 * Writes an invite directly, returning the raw token.
 *
 * Only the hash is stored, so a token cannot be read back out -- this mints a
 * known one the same way issueInvite does. Going through POST /invites instead
 * would mean parsing the dev sink's log, which is off by default.
 */
async function plantInvite(tenantId, email) {
  const token = randomBytes(32).toString('base64url');
  await db.query(
    `INSERT INTO "Invite" (id,"tenantId",email,role,"tokenHash","invitedBy","expiresAt","createdAt")
     VALUES ($1,$2,$3,'ATTENDEE',$4,'seed', now() + interval '7 days', now())`,
    [`inv_${randomBytes(8).toString('hex')}`, tenantId, email.toLowerCase(), sha256(token)],
  );
  return token;
}

const inviteState = async (tenantId, email) => {
  const { rows } = await db.query(
    `SELECT "revokedAt" IS NOT NULL AS revoked, "acceptedAt" IS NOT NULL AS accepted
       FROM "Invite" WHERE "tenantId" = $1 AND email = $2`,
    [tenantId, email.toLowerCase()],
  );
  return rows;
};

const post = (path, body, headers = {}) =>
  fetch(`${AUTH}${path}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...headers },
    body: JSON.stringify(body),
  }).then(async (r) => ({ status: r.status, body: await r.json().catch(() => null) }));

async function main() {
  await db.connect();

  const login = await post('/login', ADMIN, { 'X-Tenant-Slug': TENANT_SLUG });
  if (login.status !== 200) fail(`could not log in as admin (${login.status})`);
  const auth = { Authorization: `Bearer ${login.body.accessToken}` };

  // The SAME address invited into two tenants. The seed already proves this is a
  // real shape: alice@acme.com exists in both.
  const email = `revoke.test.${Date.now()}@example.com`;
  const mine = await plantInvite(TENANT, email);
  const theirs = await plantInvite(OTHER_TENANT, email);

  // -------------------------------------------------------------------------
  console.log('\n1. an admin can withdraw an invitation');
  // -------------------------------------------------------------------------
  const revoked = await post('/invites/revoke', { email }, auth);
  check(revoked.status === 200, `200, got ${revoked.status}`);
  check(revoked.body?.revoked === 1, `revoked exactly 1, got ${revoked.body?.revoked}`);

  const after = await inviteState(TENANT, email);
  check(after.length === 1 && after[0].revoked === true, 'the invite is marked revoked');

  // -------------------------------------------------------------------------
  console.log('\n2. a revoked invitation cannot be redeemed');
  // -------------------------------------------------------------------------
  const used = await post('/register', { token: mine, password: 'hunter2222' });
  check(used.status === 400, `register with a revoked token -> 400, got ${used.status}`);
  check(
    used.body?.error === 'invalid or expired invite',
    `the same generic error as every other failure, got ${JSON.stringify(used.body)}`,
  );

  // -------------------------------------------------------------------------
  console.log('\n3. revoking does not reach into another tenant');
  // -------------------------------------------------------------------------
  // Invite is RLS-exempt, so the tenantId filter in the query is the ONLY thing
  // scoping this. Without it, one admin revokes every tenant's invitations for
  // an address.
  const otherAfter = await inviteState(OTHER_TENANT, email);
  check(
    otherAfter.length === 1 && otherAfter[0].revoked === false,
    `the other tenant's invite is untouched, got revoked=${otherAfter[0]?.revoked}`,
  );

  const stillWorks = await post('/register', { token: theirs, password: 'hunter2222' });
  check(stillWorks.status === 201, `the other tenant's invite still redeems, got ${stillWorks.status}`);

  // -------------------------------------------------------------------------
  console.log('\n4. revoking nothing is not an error');
  // -------------------------------------------------------------------------
  // "No live invite for that address" is the state the caller asked for, and
  // answering 404 would tell an admin whether an address they do not control has
  // a pending invitation in their tenant.
  const again = await post('/invites/revoke', { email }, auth);
  check(again.status === 200, `revoking an already-revoked address -> 200, got ${again.status}`);
  check(again.body?.revoked === 0, `revoked 0 the second time, got ${again.body?.revoked}`);

  const nobody = await post('/invites/revoke', { email: 'nobody@nowhere.test' }, auth);
  check(nobody.status === 200, `unknown address -> 200, got ${nobody.status}`);
  check(nobody.body?.revoked === 0, `revoked 0, got ${nobody.body?.revoked}`);

  // -------------------------------------------------------------------------
  console.log('\n5. revoking requires ADMIN');
  // -------------------------------------------------------------------------
  // The user registered in step 3 is an ATTENDEE in the other tenant.
  const attendee = await post('/login', { email, password: 'hunter2222' }, { 'X-Tenant-Slug': OTHER_TENANT_SLUG });
  if (attendee.status !== 200) fail(`could not log in as the new attendee (${attendee.status})`);

  const forbidden = await post(
    '/invites/revoke',
    { email: 'someone@example.com' },
    { Authorization: `Bearer ${attendee.body.accessToken}` },
  );
  check(forbidden.status === 403, `an ATTENDEE is refused, got ${forbidden.status}`);

  await db.end();

  console.log('');
  if (failures > 0) {
    console.error(`${failures} assertion(s) failed`);
    process.exit(1);
  }
  console.log('invite revocation verified: a withdrawn invite is dead, and revoking one');
  console.log('tenant’s invitation cannot touch another’s');
}

main().catch(async (err) => {
  console.error(err);
  await db.end().catch(() => {});
  process.exit(1);
});
