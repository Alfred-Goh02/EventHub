/**
 * Asserts that a request which did NOTHING does not consume its
 * Idempotency-Key.
 *
 * POST /reservations claims the key in step 1, before anything contended, and
 * the claim is a row written inside the transaction. So whether a guard RETURNS
 * or THROWS decides whether that claim survives: returning commits it, throwing
 * rolls it back with everything else.
 *
 * The endpoint got this right for its inventory guards and wrong for its two
 * projection guards. The unknown-event branch is where it did real damage,
 * because that branch can answer 503 with Retry-After: 2 when the projection
 * consumer is not current -- and the committed claim holds a SIXTY-second
 * lease. The service asks the client to retry in two seconds and then spends the
 * next fifty-eight refusing that retry with "a request with this
 * Idempotency-Key is in progress", about a request that finished long ago.
 *
 * Measured against the unfixed code: 404, then 409, then 409.
 *
 * Two assertions per case, and the second is the one that matters:
 *
 *   ANSWERED AGAIN  the retry gets the same answer as the first attempt, not
 *                   409 in-flight.
 *   NO ROW          there is no IdempotencyKey row at all. This is the actual
 *                   invariant; the response code is only how it shows.
 *
 * The two POSITIVE CONTROLS are what stop this from being a test that merely
 * rewards deleting the idempotency mechanism:
 *
 *   sold-out        a guard that already threw. It must keep behaving, which
 *                   proves the assertions are reading a real property rather
 *                   than a change made to this file.
 *   a real hold     201, then the same key replayed must return the SAME
 *                   reservation, and the row must be 'completed'. A "fix" that
 *                   simply stopped claiming keys would pass everything above
 *                   and fail here -- and would have broken the one guarantee
 *                   the header exists to give.
 *
 * Run: node scripts/verify-idempotency-rollback.mjs
 */

import pg from 'pg';

const APP_URL =
  process.env['TICKETING_APP_DATABASE_URL'] ??
  'postgresql://eventhub_ticketing_app:eventhub_ticketing_app@localhost:5432/eventhub_ticketing';
const AUTH = process.env['AUTH_URL'] ?? 'http://localhost:3001';
const TICKETING = process.env['TICKETING_URL'] ?? 'http://localhost:3003';

const TENANT = 'tenant_acme';
/** The seeded tenant's subdomain: Auth takes the tenant from the gateway's X-Tenant-Slug. */
const TENANT_SLUG = 'acme';
const PRICE = 1000;

const run = `ir_${Date.now()}`;
const liveEvent = `${run}_evt_live`;
const deadEvent = `${run}_evt_dead`;
const ttLive = `${run}_tt_live`;
const ttDead = `${run}_tt_dead`;
const ttScarce = `${run}_tt_scarce`;

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}
function fail(msg) {
  console.error(`FAIL: ${msg}`);
  process.exit(1);
}

const db = new pg.Client({ connectionString: APP_URL });

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

/** What the key table says about this key, if anything. */
const keyRow = (key) =>
  tx(async () => {
    const { rows } = await db.query(
      `SELECT state, "responseStatus" FROM "IdempotencyKey"
        WHERE "tenantId" = $1 AND endpoint = 'POST /reservations' AND key = $2`,
      [TENANT, key],
    );
    return rows[0] ?? null;
  });

let accessToken;

async function post(key, body) {
  const res = await fetch(`${TICKETING}/reservations`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${accessToken}`,
      'Idempotency-Key': key,
    },
    body: JSON.stringify(body),
  });
  return { status: res.status, body: await res.json() };
}

async function main() {
  await db.connect();

  await tx(async () => {
    for (const [id, status, deleted] of [
      [liveEvent, 'PUBLISHED', false],
      [deadEvent, 'CANCELLED', false],
    ]) {
      await db.query(
        `INSERT INTO "EventProjection"
           (id,"tenantId",title,slug,status,capacity,"startsAt","endsAt","organiserId",deleted,"sourceVersion","updatedAt")
         VALUES ($1,$2,'Idempotency Rollback',$3,$4,100,now(),now(),'u_test',$5,1,now())`,
        [id, TENANT, `idem-rollback-${id}`, status, deleted],
      );
    }
    for (const [id, eventId, total] of [
      [ttLive, liveEvent, 10],
      [ttScarce, liveEvent, 1],
      [ttDead, deadEvent, 10],
    ]) {
      await db.query(
        `INSERT INTO "TicketType"
           (id,"tenantId","eventId",name,"priceCents",total,remaining,held,sold,"updatedAt")
         VALUES ($1,$2,$3,'General',$4,$5,$5,0,0,now())`,
        [id, TENANT, eventId, PRICE, total],
      );
    }
  });
  console.log('  seeded a published event, a cancelled event, and a 1-seat ticket type');

  const login = await fetch(`${AUTH}/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Tenant-Slug': TENANT_SLUG },
    body: JSON.stringify({ email: 'alice@acme.com', password: 'hunter2222' }),
  });
  ({ accessToken } = await login.json());
  if (!accessToken) fail('could not log in');

  /**
   * Every case sends the SAME body twice under the SAME key, which is exactly
   * what a client following a Retry-After header does.
   */
  const CASES = [
    {
      label: 'unknown event (the 503/Retry-After case)',
      body: { eventId: `${run}_evt_missing`, items: [{ ticketTypeId: ttLive, quantity: 1 }] },
      // 404 if the projection consumer is current, 503 if it is not. Which one
      // depends on the consumer's state at this instant, and this verifier is
      // not about that distinction -- verify-projection-freshness.mjs is. What
      // is asserted here holds for both: the claim rolls back and the retry gets
      // the SAME answer. Pinning this to 404 would make the test fail whenever
      // the consumer happened to be mid-replay, which is a flake, not a defect.
      expect: [404, 503],
    },
    {
      label: 'cancelled event',
      body: { eventId: deadEvent, items: [{ ticketTypeId: ttDead, quantity: 1 }] },
      expect: [409],
    },
    {
      label: 'sold out (positive control: already threw)',
      body: { eventId: liveEvent, items: [{ ticketTypeId: ttScarce, quantity: 5 }] },
      expect: [409],
    },
  ];

  for (const c of CASES) {
    console.log('');
    console.log(`  ${c.label}`);

    const key = `${run}_${CASES.indexOf(c)}`;

    const first = await post(key, c.body);
    check(
      c.expect.includes(first.status),
      `first attempt -> ${first.status}, expected ${c.expect.join(' or ')}`,
    );

    const row = await keyRow(key);
    check(
      row === null,
      row === null
        ? 'no IdempotencyKey row: the claim rolled back with the transaction'
        : `an IdempotencyKey row survived in state '${row.state}' -- the retry below is doomed`,
    );

    const second = await post(key, c.body);
    check(
      second.status === first.status,
      `retry with the same key -> ${second.status}, expected ${first.status}` +
        (second.status === first.status ? '' : ` -- ${JSON.stringify(second.body)}`),
    );
    check(
      JSON.stringify(second.body) === JSON.stringify(first.body),
      'the retry got the same answer, not "a request with this key is in progress"',
    );
  }

  /**
   * THE CONTROL THAT MATTERS.
   *
   * Everything above is satisfied by an endpoint that never claims a key at
   * all. This is what says the mechanism is still there: a successful hold must
   * mark the key 'completed', and replaying it must return the SAME
   * reservation rather than taking a second set of seats.
   */
  console.log('');
  console.log('  a successful hold (positive control: the key must still work)');

  const liveKey = `${run}_live`;
  const liveBody = { eventId: liveEvent, items: [{ ticketTypeId: ttLive, quantity: 2 }] };

  const created = await post(liveKey, liveBody);
  check(created.status === 201, `first attempt -> ${created.status}, expected 201`);

  const completedRow = await keyRow(liveKey);
  check(
    completedRow?.state === 'completed',
    `IdempotencyKey row is '${completedRow?.state ?? 'absent'}', expected 'completed'`,
  );

  const replayed = await post(liveKey, liveBody);
  check(replayed.status === 201, `replay -> ${replayed.status}, expected 201`);
  check(
    replayed.body?.id === created.body?.id,
    `replay returned reservation ${replayed.body?.id}, expected the original ${created.body?.id}`,
  );

  const inv = await tx(async () => {
    const { rows } = await db.query(`SELECT held, remaining FROM "TicketType" WHERE id = $1`, [ttLive]);
    return rows[0];
  });
  check(
    inv.held === 2 && inv.remaining === 8,
    `the replay took no extra seats (held ${inv.held}, remaining ${inv.remaining})`,
  );

  await db.end();

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exit(1);
  }
  console.log('a request that did nothing leaves its Idempotency-Key unspent, and a request that');
  console.log('did something still spends it exactly once');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
