/**
 * Asserts that the tenant a new user joins comes from the INVITE, and that a
 * caller cannot steer it.
 *
 * REPLACES A CHECK THAT COULD NOT FAIL. The CI step for this used to send:
 *
 *   POST /register  X-Tenant-ID: tenant_globex
 *   {"email":"attacker@evil.com","password":"hunter2222"}
 *
 * and assert 400, under a comment saying "the tenant hole is open". The body
 * has no `token` field, and registerSchema requires one — so the 400 came from
 * Zod, before lookupInvite() was ever called. Measured:
 *
 *   {"error":{"formErrors":[],"fieldErrors":{"token":["Required"]}}}
 *
 * Delete invite verification entirely and that check still passes. It asserted
 * that a schema has a required field.
 *
 * The property it was reaching for is real and was tested nowhere: /register
 * takes no X-Tenant-ID at all, deliberately, because the tenant comes from an
 * invite created by someone already inside that tenant — so the registering
 * user never gets to say which tenant they are joining. Nothing checked that
 * the header is actually ignored rather than merely undocumented.
 *
 * The membership assertion is made THROUGH row-level security rather than by
 * reading a tenantId column: the new row must be visible under the invite's
 * tenant context and invisible under the other one. That is the isolation
 * property itself, not a proxy for it — and it is checked on the app role,
 * which is the one the policies actually apply to.
 *
 * Run: node scripts/verify-invite-tenant-binding.mjs
 */

import { randomBytes, createHash } from 'node:crypto';
import pg from 'pg';

const OWNER_URL =
  process.env['DATABASE_URL'] ??
  'postgresql://eventhub_auth_owner:eventhub_auth_owner@localhost:5432/eventhub_auth';
const APP_URL =
  process.env['APP_DATABASE_URL'] ??
  'postgresql://eventhub_auth_app:eventhub_auth_app@localhost:5432/eventhub_auth';
const AUTH = process.env['AUTH_URL'] ?? 'http://localhost:3001';

const INVITE_TENANT = 'tenant_acme';
const CLAIMED_TENANT = 'tenant_globex';

const run = `itb_${Date.now()}`;

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}

const sha256 = (s) => createHash('sha256').update(s).digest('hex');

const owner = new pg.Client({ connectionString: OWNER_URL, application_name: 'verify-invite-binding' });
const appRole = new pg.Client({ connectionString: APP_URL, application_name: 'verify-invite-binding' });
owner.on('error', () => {});
appRole.on('error', () => {});

/**
 * Mint an invite directly, returning the raw token.
 *
 * Only the hash is stored, so a token cannot be read back out. This writes one
 * the same way issueInvite does — going through POST /invites instead would
 * make this test depend on that endpoint's behaviour as well as on the one it
 * is about.
 */
async function mintInvite(tenantId, email) {
  const token = randomBytes(32).toString('base64url');
  await owner.query(
    `INSERT INTO "Invite" (id,"tenantId",email,role,"tokenHash","invitedBy","expiresAt","createdAt")
     VALUES ($1,$2,$3,'ATTENDEE',$4,'seed', now() + interval '7 days', now())`,
    [`inv_${randomBytes(8).toString('hex')}`, tenantId, email.toLowerCase(), sha256(token)],
  );
  return token;
}

const register = (body, headers = {}) =>
  fetch(`${AUTH}/register`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', ...headers },
    body: JSON.stringify(body),
  }).then(async (r) => ({ status: r.status, body: await r.json() }));

/** Is this user visible to a session scoped to `tenantId`? */
async function visibleUnder(tenantId, email) {
  await appRole.query('BEGIN');
  await appRole.query(`SELECT set_config('app.tenant_id', $1, true)`, [tenantId]);
  const { rows } = await appRole.query(`SELECT id FROM "User" WHERE email = $1`, [email]);
  await appRole.query('COMMIT');
  return rows.length > 0;
}

async function main() {
  await Promise.all([owner.connect(), appRole.connect()]);

  // -------------------------------------------------------------------------
  console.log('  the positive control: a real invite must register');
  // -------------------------------------------------------------------------
  /**
   * Without this, an endpoint that rejected EVERY registration would satisfy
   * every other assertion in this file.
   */
  const controlEmail = `${run}_control@acme.com`;
  const controlToken = await mintInvite(INVITE_TENANT, controlEmail);
  const control = await register({ token: controlToken, password: 'hunter2222' });
  check(control.status === 201, `a valid invite registers, got ${control.status}`);

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  the header must not steer which tenant the user joins');
  // -------------------------------------------------------------------------
  const email = `${run}_steered@acme.com`;
  const token = await mintInvite(INVITE_TENANT, email);

  const steered = await register(
    { token, password: 'hunter2222' },
    { 'X-Tenant-ID': CLAIMED_TENANT },
  );
  check(
    steered.status === 201,
    `the invite still redeems while claiming another tenant, got ${steered.status}`,
  );

  const inInviteTenant = await visibleUnder(INVITE_TENANT, email.toLowerCase());
  const inClaimedTenant = await visibleUnder(CLAIMED_TENANT, email.toLowerCase());

  check(
    inInviteTenant,
    `the user is visible under ${INVITE_TENANT} — the tenant came from the invite`,
  );
  check(
    !inClaimedTenant,
    inClaimedTenant
      ? `the user is ALSO visible under ${CLAIMED_TENANT} — the header steered the registration`
      : `the user is not visible under ${CLAIMED_TENANT}, which the request asked for`,
  );

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  a token that was never issued must be refused by the invite logic');
  // -------------------------------------------------------------------------
  /**
   * Well-formed, so it passes the schema and reaches lookupInvite(). This is
   * the case the old check could not reach, and the message proves where the
   * refusal came from: a Zod failure returns a fieldErrors object, not this.
   */
  const bogus = await register({
    token: randomBytes(32).toString('base64url'),
    password: 'hunter2222',
  });
  check(bogus.status === 400, `a never-issued token is refused, got ${bogus.status}`);
  check(
    bogus.body?.error === 'invalid or expired invite',
    `refused by the invite logic, not by the schema: ${JSON.stringify(bogus.body?.error)}`,
  );

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  and the shape check, labelled as what it is');
  // -------------------------------------------------------------------------
  const shapeless = await register({ email: 'attacker@evil.com', password: 'hunter2222' });
  check(shapeless.status === 400, `a body with no token is refused, got ${shapeless.status}`);
  check(
    shapeless.body?.error?.fieldErrors?.token !== undefined,
    'this one IS the schema talking — which is why it cannot stand in for the checks above',
  );

  await Promise.all([owner.end(), appRole.end()]);

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    /**
     * exitCode rather than process.exit(). fetch() leaves a keep-alive agent
     * holding a socket, and exiting on top of it aborts libuv on Windows with
     * "Assertion failed: !(handle->flags & UV_HANDLE_CLOSING)" — which replaces
     * a clean exit 1 with a confusing 127. Setting the code and returning lets
     * Node close its handles and exit on its own.
     */
    process.exitCode = 1;
    return;
  }
  console.log('the tenant a user joins is decided by the invite, and the request cannot move it');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
