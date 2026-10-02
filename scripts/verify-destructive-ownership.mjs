/**
 * Asserts that cancelling or deleting an event needs more than a job title.
 *
 * Every write route requires ORGANIZER or ADMIN, and until now that was the
 * whole check — so any organiser could cancel any other organiser's event.
 * organiserId was set from the token at creation and carried into the Kafka
 * snapshot, and no route ever read it back.
 *
 * That was nobody's decision. The security boundary in this system is the
 * TENANT: every RLS policy reads app.tenant_id and nothing else, so once tenant
 * scoping was right the routes felt finished. Ticketing later drew the line
 * differently — /pay filters on userId, with a comment saying RLS scopes to a
 * tenant and not to a user. The Event service never did.
 *
 * THE SPLIT THIS ASSERTS, and the second half is as deliberate as the first:
 *
 *   destructive   cancel and delete need the creator, or an ADMIN. Both
 *                 propagate to Ticketing and stop sales for people already
 *                 holding tickets, and neither has an undo.
 *   collaborative patch and publish stay open to any organiser in the tenant.
 *                 Editing a colleague's event is what a team account is FOR,
 *                 and locking it to the creator buys no safety.
 *
 * The open half is asserted too. Without it, someone reading this file later
 * cannot tell whether editing is open by choice or by the same oversight that
 * left cancel open — which is exactly the ambiguity being fixed.
 *
 * 403 rather than 404 for a non-owner: the caller can already GET the event, so
 * hiding it would deny something they can plainly observe. Another tenant's row
 * still 404s, because there confirming existence is itself the leak.
 *
 * Run: node scripts/verify-destructive-ownership.mjs
 */

import { randomBytes, createHash } from 'node:crypto';
import pg from 'pg';

const AUTH_OWNER_URL =
  process.env['DATABASE_URL'] ??
  'postgresql://eventhub_auth_owner:eventhub_auth_owner@localhost:5432/eventhub_auth';
const AUTH = process.env['AUTH_URL'] ?? 'http://localhost:3001';
const EVENT = process.env['EVENT_URL'] ?? 'http://localhost:3002';

const TENANT = 'tenant_acme';
/** The seeded tenant's subdomain: Auth takes the tenant from the gateway's X-Tenant-Slug. */
const TENANT_SLUG = 'acme';
const run = `own_${Date.now()}`;

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}
function fail(msg) {
  console.error(`FAIL: ${msg}`);
  process.exitCode = 1;
  throw new Error(msg);
}

const sha256 = (s) => createHash('sha256').update(s).digest('hex');
const db = new pg.Client({ connectionString: AUTH_OWNER_URL, application_name: 'verify-ownership' });
db.on('error', () => {});

async function makeUser(label, role) {
  const email = `${run}_${label}@acme.com`;
  const token = randomBytes(32).toString('base64url');
  await db.query(
    `INSERT INTO "Invite" (id,"tenantId",email,role,"tokenHash","invitedBy","expiresAt","createdAt")
     VALUES ($1,$2,$3,$4::"Role",$5,'seed', now() + interval '7 days', now())`,
    [`inv_${randomBytes(8).toString('hex')}`, TENANT, email, role, sha256(token)],
  );
  const reg = await fetch(`${AUTH}/register`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ token, password: 'hunter2222' }),
  });
  if (reg.status !== 201) fail(`could not register ${label}: ${reg.status}`);

  const login = await fetch(`${AUTH}/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Tenant-Slug': TENANT_SLUG },
    body: JSON.stringify({ email, password: 'hunter2222' }),
  });
  const { accessToken } = await login.json();
  if (!accessToken) fail(`could not log in as ${label}`);
  return accessToken;
}

const roleOf = (t) => JSON.parse(Buffer.from(t.split('.')[1], 'base64').toString()).role;

const call = (method, path, token, body) =>
  fetch(`${EVENT}${path}`, {
    method,
    headers: {
      Authorization: `Bearer ${token}`,
      ...(body ? { 'Content-Type': 'application/json' } : {}),
      ...(body?.__ifMatch ? { 'If-Match': `"${body.__ifMatch}"` } : {}),
    },
    ...(body ? { body: JSON.stringify({ ...body, __ifMatch: undefined }) } : {}),
  }).then(async (r) => ({
    status: r.status,
    body: r.status === 204 ? null : await r.json().catch(() => null),
  }));

async function newEvent(token, title) {
  const base = Date.now();
  const res = await call('POST', '/events', token, {
    title,
    capacity: 20,
    startsAt: new Date(base + 86_400_000).toISOString(),
    endsAt: new Date(base + 90_000_000).toISOString(),
  });
  if (res.status !== 201) fail(`could not create an event: ${res.status}`);
  return res.body;
}

async function main() {
  await db.connect();

  const alice = await makeUser('alice', 'ORGANIZER');
  const bob = await makeUser('bob', 'ORGANIZER');
  const admin = await makeUser('admin', 'ADMIN');

  check(roleOf(alice) === 'ORGANIZER' && roleOf(bob) === 'ORGANIZER', 'two ORGANIZERs in one tenant');
  check(roleOf(admin) === 'ADMIN', 'and an ADMIN');

  // -------------------------------------------------------------------------
  console.log('');
  console.log("  a colleague must not cancel or delete another organiser's event");
  // -------------------------------------------------------------------------
  const toCancel = await newEvent(alice, `Alice cancel ${run}`);
  const bobCancels = await call('POST', `/events/${toCancel.id}/cancel`, bob);
  check(
    bobCancels.status === 403,
    bobCancels.status === 403
      ? `cancel by a colleague -> 403 ${JSON.stringify(bobCancels.body?.error)}`
      : `cancel by a colleague -> ${bobCancels.status}; it went through`,
  );

  const toDelete = await newEvent(alice, `Alice delete ${run}`);
  const bobDeletes = await call('DELETE', `/events/${toDelete.id}`, bob);
  check(
    bobDeletes.status === 403,
    bobDeletes.status === 403
      ? `delete by a colleague -> 403 ${JSON.stringify(bobDeletes.body?.error)}`
      : `delete by a colleague -> ${bobDeletes.status}; it went through`,
  );

  const stillThere = await call('GET', `/events/${toCancel.id}`, alice);
  check(
    stillThere.status === 200 && stillThere.body?.status === 'DRAFT',
    `and nothing moved: the event is ${stillThere.body?.status}, version ${stillThere.body?.version}`,
  );
  check(
    stillThere.body?.version === toCancel.version,
    `the refused calls did not bump the version (${stillThere.body?.version}, expected ${toCancel.version})`,
  );

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  but the owner and an ADMIN still can');
  // -------------------------------------------------------------------------
  /**
   * Without these two, a route that refused EVERY cancel would satisfy
   * everything above — and refusing everything is a worse bug than the one
   * being fixed.
   */
  const aliceCancels = await call('POST', `/events/${toCancel.id}/cancel`, alice);
  check(aliceCancels.status === 200, `the owner cancels her own event, got ${aliceCancels.status}`);

  const adminDeletes = await call('DELETE', `/events/${toDelete.id}`, admin);
  check(
    adminDeletes.status === 204,
    `an ADMIN deletes someone else's event, got ${adminDeletes.status} — the override has to ` +
      'survive, or an organiser who leaves takes their events with them',
  );

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  and editing stays open on purpose');
  // -------------------------------------------------------------------------
  /**
   * The deliberate half. Asserted so a later reader can tell that patch and
   * publish are open BY CHOICE, rather than by the same oversight that left
   * cancel open.
   */
  const shared = await newEvent(alice, `Shared ${run}`);

  const bobPatches = await fetch(`${EVENT}/events/${shared.id}`, {
    method: 'PATCH',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${bob}`,
      'If-Match': `"${shared.version}"`,
    },
    body: JSON.stringify({ title: `Retitled by a colleague ${run}` }),
  }).then(async (r) => ({ status: r.status, body: await r.json() }));

  check(
    bobPatches.status === 200,
    `a colleague can still edit the title, got ${bobPatches.status} — editing is collaborative ` +
      'by design and this is what says so',
  );

  const bobPublishes = await call('POST', `/events/${shared.id}/publish`, bob);
  check(bobPublishes.status === 200, `and still publish it, got ${bobPublishes.status}`);

  await db.end();

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('destructive actions need the creator or an admin; collaborative ones stay shared');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
