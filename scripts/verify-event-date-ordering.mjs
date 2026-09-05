/**
 * Asserts that an event cannot be made to end before it starts.
 *
 * POST /events has always refused a backwards pair with a Zod `.refine()`.
 * PATCH never checked, and there was no constraint underneath either — so an
 * event created correctly could be patched into nonsense two different ways,
 * both measured returning 200:
 *
 *   PATCH { startsAt: <later>, endsAt: <earlier> }  -> 200
 *   PATCH { startsAt: <past the stored endsAt> }    -> 200
 *
 * The second is the one a refinement on the request body cannot catch on its
 * own. PATCH is PARTIAL, so half the comparison lives in the database, and a
 * schema that only sees the request has nothing to compare against.
 *
 * TWO LAYERS, ASSERTED SEPARATELY, because they fail for different reasons and
 * one of them is the guarantee while the other is only the error message:
 *
 *   the route      reads the current row, merges the patch, and answers 400.
 *                  This is what makes the ordinary case legible.
 *   the constraint CHECK ("endsAt" > "startsAt"). This is what makes it true.
 *                  Two concurrent patches could each merge to something valid
 *                  and still land on something invalid, and an If-Match of `*`
 *                  or none does not stop them.
 *
 * The constraint is asserted by writing DIRECTLY to the database, going around
 * the route entirely — because a test that only ever goes through the route
 * cannot tell the two layers apart, and would keep passing if the constraint
 * were dropped.
 *
 * Run: node scripts/verify-event-date-ordering.mjs
 */

import pg from 'pg';

const EVENT_APP_URL =
  process.env['EVENT_APP_DATABASE_URL'] ??
  'postgresql://eventhub_event_app:eventhub_event_app@localhost:5432/eventhub_event';
const AUTH = process.env['AUTH_URL'] ?? 'http://localhost:3001';
const EVENT = process.env['EVENT_URL'] ?? 'http://localhost:3002';

const TENANT = 'tenant_acme';
const run = `edo_${Date.now()}`;

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

const DAY = 86_400_000;
const base = Date.now();
const at = (days) => new Date(base + days * DAY).toISOString();

const db = new pg.Client({ connectionString: EVENT_APP_URL, application_name: 'verify-dates' });
db.on('error', () => {});

async function tx(fn) {
  await db.query('BEGIN');
  await db.query(`SELECT set_config('app.tenant_id', $1, true)`, [TENANT]);
  try {
    const out = await fn();
    await db.query('COMMIT');
    return out;
  } catch (err) {
    await db.query('ROLLBACK').catch(() => {});
    throw err;
  }
}

let token;
const H = () => ({ 'Content-Type': 'application/json', Authorization: `Bearer ${token}` });

const patch = (id, version, body) =>
  fetch(`${EVENT}/events/${id}`, {
    method: 'PATCH',
    headers: { ...H(), 'If-Match': `"${version}"` },
    body: JSON.stringify(body),
  }).then(async (r) => ({ status: r.status, body: await r.json() }));

async function main() {
  await db.connect();

  const login = await fetch(`${AUTH}/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Tenant-ID': TENANT },
    body: JSON.stringify({ email: 'alice@acme.com', password: 'hunter2222' }),
  });
  ({ accessToken: token } = await login.json());
  if (!token) fail('could not log in');

  // -------------------------------------------------------------------------
  console.log('  the positive control: an ordinary event and an ordinary patch');
  // -------------------------------------------------------------------------
  /**
   * Without this, a route that rejected EVERY patch would satisfy every other
   * assertion here.
   */
  const created = await fetch(`${EVENT}/events`, {
    method: 'POST',
    headers: H(),
    body: JSON.stringify({ title: `Dates ${run}`, capacity: 10, startsAt: at(10), endsAt: at(11) }),
  }).then(async (r) => ({ status: r.status, body: await r.json() }));

  check(created.status === 201, `a well-ordered event is created, got ${created.status}`);
  const id = created.body.id;

  const moved = await patch(id, created.body.version, { endsAt: at(12) });
  check(moved.status === 200, `moving endsAt later is accepted, got ${moved.status}`);

  const backwardsCreate = await fetch(`${EVENT}/events`, {
    method: 'POST',
    headers: H(),
    body: JSON.stringify({ title: 'Backwards', capacity: 10, startsAt: at(11), endsAt: at(10) }),
  });
  check(backwardsCreate.status === 400, `POST still refuses a backwards pair, got ${backwardsCreate.status}`);

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  the route must refuse both shapes of backwards patch');
  // -------------------------------------------------------------------------
  const version = moved.body.version;

  const both = await patch(id, version, { startsAt: at(20), endsAt: at(19) });
  check(both.status === 400, `PATCH with both fields backwards -> ${both.status}, expected 400`);
  check(
    both.body?.error === 'endsAt must be after startsAt',
    `and it says why: ${JSON.stringify(both.body?.error)}`,
  );

  /**
   * The partial case. Only startsAt is sent, and it moves past the STORED
   * endsAt — so the request on its own looks perfectly reasonable and the
   * comparison can only be made against the database.
   */
  const partial = await patch(id, version, { startsAt: at(30) });
  check(
    partial.status === 400,
    `PATCH moving startsAt past the stored endsAt -> ${partial.status}, expected 400`,
  );

  const after = await tx(async () => {
    const { rows } = await db.query(
      `SELECT "startsAt", "endsAt", version FROM "Event" WHERE id = $1`,
      [id],
    );
    return rows[0];
  });
  check(
    after.endsAt > after.startsAt,
    `the stored row is still well-ordered (${after.startsAt.toISOString()} -> ${after.endsAt.toISOString()})`,
  );
  check(
    after.version === version,
    `and the refused patches did not bump the version (${after.version}, expected ${version})`,
  );

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  and the constraint underneath, reached around the route');
  // -------------------------------------------------------------------------
  /**
   * Written straight to the table. A test that only ever goes through the route
   * cannot tell the route's check from the constraint, and would keep passing
   * if the constraint were dropped — which is the layer that actually holds
   * under a concurrent patch.
   */
  let violation = null;
  try {
    await tx(async () => {
      await db.query(`UPDATE "Event" SET "endsAt" = "startsAt" - interval '1 hour' WHERE id = $1`, [
        id,
      ]);
    });
  } catch (err) {
    violation = err;
  }

  check(
    violation !== null,
    violation
      ? `the database refused it: ${violation.code} ${String(violation.message).slice(0, 60)}`
      : 'the database ACCEPTED a backwards pair written directly — there is no constraint',
  );
  check(
    violation?.code === '23514',
    `and refused it as a CHECK violation (SQLSTATE ${violation?.code}), not by accident`,
  );

  const final = await tx(async () => {
    const { rows } = await db.query(`SELECT "startsAt", "endsAt" FROM "Event" WHERE id = $1`, [id]);
    return rows[0];
  });
  check(final.endsAt > final.startsAt, 'the row survived the attempt unchanged');

  await db.end();

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('an event cannot be made to end before it starts, by either route or by neither');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
