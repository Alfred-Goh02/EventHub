/**
 * Asserts that an unpublished event is not visible to someone who cannot
 * publish one.
 *
 * ATTENDEE is the DEFAULT role, and the schema calls it "buys tickets, joins
 * the community" — so attendees are customers living in the same tenant as the
 * organisers. Neither read route filtered on status, so any of them could fetch
 * a DRAFT by id and list every draft in the tenant. Reproduced before the fix:
 *
 *   attendee GET /events/<id>        -> 200 "Secret Launch" (DRAFT)
 *   attendee GET /events?status=DRAFT -> 1 draft(s) visible
 *
 * That contradicts the meaning the rest of the system gives this field.
 * Ticketing refuses to sell anything not PUBLISHED, and /publish exists
 * precisely to mark the moment an event becomes public. An organiser drafting
 * next quarter's programme should not be doing it in front of the customers.
 *
 * FOUR THINGS ARE ASSERTED, and three of them are there to stop this passing
 * for the wrong reason:
 *
 *   the leak        an attendee gets 404 for a draft and sees none in a list
 *   ?status=DRAFT   asking for drafts by name is not a way around it
 *   still visible   an attendee CAN see a PUBLISHED event — otherwise a route
 *                   that returned nothing at all would pass
 *   the organiser   an ORGANIZER/ADMIN CAN still see their own draft —
 *                   otherwise hiding drafts from everyone would pass
 *
 * The ?status=DRAFT case is not hypothetical. The first version of the fix
 * spread the filter alongside the caller's own status filter, and whichever
 * came last won: one ordering let ?status=DRAFT walk straight past it.
 *
 * Run: node scripts/verify-draft-visibility.mjs
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
const run = `dv_${Date.now()}`;

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
  process.exitCode = 1;
  throw new Error(msg);
}

const sha256 = (s) => createHash('sha256').update(s).digest('hex');

const db = new pg.Client({ connectionString: AUTH_OWNER_URL, application_name: 'verify-draft' });
db.on('error', () => {});

/** An ATTENDEE in the same tenant as the organiser — the default role. */
async function makeAttendee() {
  const email = `${run}_attendee@acme.com`;
  const token = randomBytes(32).toString('base64url');
  await db.query(
    `INSERT INTO "Invite" (id,"tenantId",email,role,"tokenHash","invitedBy","expiresAt","createdAt")
     VALUES ($1,$2,$3,'ATTENDEE',$4,'seed', now() + interval '7 days', now())`,
    [`inv_${randomBytes(8).toString('hex')}`, TENANT, email, sha256(token)],
  );
  const res = await fetch(`${AUTH}/register`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ token, password: 'hunter2222' }),
  });
  if (res.status !== 201) fail(`could not register the attendee, got ${res.status}`);
  return email;
}

async function login(email) {
  const res = await fetch(`${AUTH}/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Tenant-Slug': TENANT_SLUG },
    body: JSON.stringify({ email, password: 'hunter2222' }),
  });
  const body = await res.json();
  if (!body.accessToken) fail(`could not log in as ${email}, got ${res.status}`);
  return body.accessToken;
}

const roleOf = (token) =>
  JSON.parse(Buffer.from(token.split('.')[1], 'base64').toString()).role;

const get = (path, token) =>
  fetch(`${EVENT}${path}`, { headers: { Authorization: `Bearer ${token}` } }).then(async (r) => ({
    status: r.status,
    body: await r.json(),
  }));

async function createEvent(token, title) {
  const res = await fetch(`${EVENT}/events`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${token}` },
    body: JSON.stringify({
      title,
      description: 'not announced yet',
      capacity: 100,
      startsAt: new Date(Date.now() + 86_400_000).toISOString(),
      endsAt: new Date(Date.now() + 90_000_000).toISOString(),
    }),
  });
  const body = await res.json();
  if (res.status !== 201) fail(`could not create an event, got ${res.status}`);
  return body;
}

const listed = (body) => body.events ?? body.items ?? body.data ?? [];

async function main() {
  await db.connect();

  const attendeeEmail = await makeAttendee();
  const organiserToken = await login('alice@acme.com');
  const attendeeToken = await login(attendeeEmail);

  const organiserRole = roleOf(organiserToken);
  const attendeeRole = roleOf(attendeeToken);
  console.log(`  organiser is ${organiserRole}; the other caller is ${attendeeRole}`);
  check(attendeeRole === 'ATTENDEE', `the second caller really is an ATTENDEE, got ${attendeeRole}`);

  const draft = await createEvent(organiserToken, `Secret Launch ${run}`);
  check(draft.status === 'DRAFT', `the new event is DRAFT, got ${draft.status}`);

  const published = await createEvent(organiserToken, `Public Show ${run}`);
  const pub = await fetch(`${EVENT}/events/${published.id}/publish`, {
    method: 'POST',
    headers: { Authorization: `Bearer ${organiserToken}` },
  });
  if (pub.status !== 200) fail(`could not publish the control event, got ${pub.status}`);

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  the attendee must not see the draft');
  // -------------------------------------------------------------------------
  const byId = await get(`/events/${draft.id}`, attendeeToken);
  check(
    byId.status === 404,
    byId.status === 404
      ? 'by id -> 404, and 404 rather than 403 so the id is not confirmed'
      : `by id -> ${byId.status} "${byId.body?.title}" (${byId.body?.status}) — the draft is readable`,
  );

  const plainList = await get('/events', attendeeToken);
  const draftsInList = listed(plainList.body).filter((e) => e.status === 'DRAFT');
  check(
    draftsInList.length === 0,
    `no drafts in the default list, saw ${draftsInList.length}`,
  );

  const asked = await get('/events?status=DRAFT', attendeeToken);
  const askedDrafts = listed(asked.body);
  check(
    askedDrafts.length === 0,
    `?status=DRAFT returns nothing, saw ${askedDrafts.length} — asking by name is not a bypass`,
  );

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  and the two controls, without which the above proves nothing');
  // -------------------------------------------------------------------------
  const attendeeSeesPublished = await get(`/events/${published.id}`, attendeeToken);
  check(
    attendeeSeesPublished.status === 200,
    `the attendee can still read a PUBLISHED event, got ${attendeeSeesPublished.status} — ` +
      'a route that returned nothing at all would satisfy every assertion above',
  );

  const organiserSeesDraft = await get(`/events/${draft.id}`, organiserToken);
  check(
    organiserSeesDraft.status === 200 && organiserSeesDraft.body?.status === 'DRAFT',
    `the organiser can still read their own draft, got ${organiserSeesDraft.status} — ` +
      'hiding drafts from everyone is not the fix',
  );

  const organiserList = await get('/events?status=DRAFT', organiserToken);
  check(
    listed(organiserList.body).some((e) => e.id === draft.id),
    'the organiser can still list drafts',
  );

  await db.end();

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('an unannounced event is visible to the people who can announce it, and nobody else');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
