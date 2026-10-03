/**
 * Asserts that an Idempotency-Key can never be taken over and run a second time.
 *
 * The claim used ON CONFLICT ... DO UPDATE to take a key over once a 60-second
 * lease expired — which means it ran the work again. For POST /reservations,
 * "run the work again" is a SECOND HOLD: the exact thing an Idempotency-Key
 * exists to prevent. Reproduced by planting such a row:
 *
 *   1. real request                      -> 201, reservation A, held 1
 *   2. same key, expired in_progress     -> 201, reservation B, held 2
 *
 * One key, two reservations, and on a scarce ticket type that is a false
 * sold-out for a real buyer.
 *
 * The lease existed for a crash between claiming and completing. That cannot
 * happen — the claim and the work commit in ONE transaction, so a crash rolls
 * both back. pay.ts already made the same argument for the payment route: "an
 * expiring lease would authorise exactly the double charge it exists to
 * prevent. Where a lease can expire, a status cannot."
 *
 * WHAT THIS ASSERTS, and the third one is the reason the other two are safe to
 * want:
 *
 *   no takeover  a planted in_progress row does NOT produce a second hold
 *   recoverable  it answers 409 instead, so an operator can clear one row
 *   still works  an ordinary request still holds seats, and a completed key
 *                still REPLAYS to the same reservation without taking more
 *
 * Without the third, an endpoint that refused everything would pass the first
 * two — and refusing everything is a worse bug than the one being fixed.
 *
 * IT DELETES ITS OWN KEYS, and the planted one is why. This file had no cleanup,
 * so every run left its planted key in_progress for good. Measured as the
 * superuser: 0 in_progress keys in tenant_acme before one run, 1 after, and the
 * run exited 0. No code path produces such a row any more, so each one was a key
 * answering 409 forever and a false positive for anyone counting them: three runs
 * of this file were among the 52 that 20260925100000_rerun_in_progress_key_delete
 * found, and were the three created after the migration meant to remove them.
 *
 * Run: node scripts/verify-no-key-takeover.mjs
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
const run = `kt_${Date.now()}`;

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

const db = new pg.Client({ connectionString: APP_URL, application_name: 'verify-takeover' });
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

const ids = { event: `${run}_evt`, ticketType: `${run}_tt` };

/** The key rewound to in_progress, and the one used as ordinary traffic. */
const key = `${run}_key`;
const freshKey = `${run}_fresh`;
let planted = false;

const inventory = () =>
  tx(async () => {
    const { rows } = await db.query(
      `SELECT remaining, held, sold FROM "TicketType" WHERE id = $1`,
      [ids.ticketType],
    );
    return rows[0];
  });

const keyRow = (key) =>
  tx(async () => {
    const { rows } = await db.query(
      `SELECT state FROM "IdempotencyKey"
        WHERE "tenantId" = $1 AND endpoint = 'POST /reservations' AND key = $2`,
      [TENANT, key],
    );
    return rows[0] ?? null;
  });

/**
 * Deletes the keys this run created, and reports what it DELETED.
 *
 * Through tx(): the same role and the same app.tenant_id the writes used.
 * IdempotencyKey is under FORCE row security, so a DELETE made without the tenant
 * set matches no rows and succeeds, and a count asked the same way agrees there is
 * nothing left. "0 deleted, 0 left" is the bug this is written against, so the
 * planted row among the deleted ones is the control, and what is left is asked
 * inside the tenant the rows were written in.
 *
 * The fixture's reservations stay, as every ticketing verifier's do: they were made
 * by the service, and the sweeper expires them and returns their seats. The
 * planted key is the one row here that nothing real would ever have written.
 */
async function cleanup() {
  const keys = [key, freshKey];
  const deleted = await tx(async () => {
    const { rows } = await db.query(
      `DELETE FROM "IdempotencyKey"
        WHERE "tenantId" = $1 AND endpoint = 'POST /reservations' AND key = ANY($2::text[])
        RETURNING state`,
      [TENANT, keys],
    );
    return rows;
  });
  const left = await tx(async () => {
    const { rows } = await db.query(
      `SELECT count(*)::int AS n FROM "IdempotencyKey"
        WHERE "tenantId" = $1 AND endpoint = 'POST /reservations' AND key = ANY($2::text[])`,
      [TENANT, keys],
    );
    return rows[0].n;
  });
  const inProgress = deleted.filter((r) => r.state === 'in_progress').length;

  console.log(`  cleanup: ${deleted.length} key(s) deleted, ${inProgress} of them in_progress; ${left} left`);
  check(
    inProgress === (planted ? 1 : 0),
    `CONTROL: the cleanup deleted the key this run planted in_progress (${inProgress} deleted, planted: ${planted})`,
  );
  check(left === 0, `no key of this run is left behind (${left})`);
}

async function main() {
  await db.connect();
  try {
    await exercise();
  } finally {
    console.log('');
    try {
      await cleanup();
    } finally {
      await db.end();
    }
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('one key means one hold, and a stuck key stays stuck rather than booking again');
}

async function exercise() {
  await tx(async () => {
    await db.query(
      `INSERT INTO "EventProjection"
         (id,"tenantId",title,slug,status,capacity,"startsAt","endsAt","organiserId","sourceVersion","updatedAt")
       VALUES ($1,$2,'Key Takeover',$3,'PUBLISHED',100,now(),now(),'u_test',1,now())`,
      [ids.event, TENANT, `key-takeover-${run}`],
    );
    await db.query(
      `INSERT INTO "TicketType"
         (id,"tenantId","eventId",name,"priceCents",total,remaining,held,sold,"updatedAt")
       VALUES ($1,$2,$3,'General',1000,20,20,0,0,now())`,
      [ids.ticketType, TENANT, ids.event],
    );
  });

  const login = await fetch(`${AUTH}/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Tenant-Slug': TENANT_SLUG },
    body: JSON.stringify({ email: 'alice@acme.com', password: 'hunter2222' }),
  });
  const { accessToken } = await login.json();
  if (!accessToken) fail('could not log in');

  const userId = JSON.parse(Buffer.from(accessToken.split('.')[1], 'base64').toString()).sub;
  const body = JSON.stringify({
    eventId: ids.event,
    items: [{ ticketTypeId: ids.ticketType, quantity: 1 }],
  });

  const post = (key) =>
    fetch(`${TICKETING}/reservations`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${accessToken}`,
        'Idempotency-Key': key,
      },
      body,
    }).then(async (r) => ({ status: r.status, body: await r.json() }));

  // -------------------------------------------------------------------------
  console.log('  a real hold, then the same key rewound to in_progress');
  // -------------------------------------------------------------------------
  const first = await post(key);
  check(first.status === 201, `the first request holds a seat, got ${first.status}`);
  const held1 = await inventory();
  check(held1.held === 1, `one seat held, got ${held1.held}`);

  /**
   * The state the takeover was written to act on: a key claimed but never
   * completed, whose lease has run out.
   *
   * It is planted rather than provoked, because no code path produces one any
   * more — that is the point. What is being asserted is what happens IF one
   * exists, from old code, a restored backup, or a future refactor that splits
   * the claim out of the transaction.
   */
  await tx(async () => {
    const { rowCount } = await db.query(
      `UPDATE "IdempotencyKey"
          SET state = 'in_progress', "responseStatus" = NULL, "responseBody" = NULL,
              "updatedAt" = now() - interval '5 minutes'
        WHERE "tenantId" = $1 AND "userId" = $2
          AND endpoint = 'POST /reservations' AND key = $3`,
      [TENANT, userId, key],
    );
    if (rowCount !== 1) fail(`could not plant the in_progress row (${rowCount} rows)`);
  });
  planted = true;
  console.log('    planted: state=in_progress, older than any lease would have been');

  const second = await post(key);
  const held2 = await inventory();

  check(
    second.status !== 201 || second.body?.id === first.body?.id,
    second.status === 201 && second.body?.id !== first.body?.id
      ? `it created a SECOND reservation (${second.body?.id}) from one key`
      : `it did not create a second reservation (got ${second.status})`,
  );
  check(
    held2.held === 1,
    `still one seat held, got ${held2.held}${held2.held > 1 ? ' — the key was taken over' : ''}`,
  );
  check(
    second.status === 409,
    `and it says the key is in progress: ${second.status} ${JSON.stringify(second.body?.error ?? '')}`,
  );

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  the controls: ordinary use must be untouched');
  // -------------------------------------------------------------------------
  /**
   * Without these, an endpoint that refused every request would satisfy
   * everything above — and refusing everything is a worse bug than the one
   * being fixed.
   */
  const fresh = await post(freshKey);
  check(fresh.status === 201, `a new key still holds seats, got ${fresh.status}`);

  const replay = await post(freshKey);
  check(replay.status === 201, `replaying a completed key still returns 201, got ${replay.status}`);
  check(
    replay.body?.id === fresh.body?.id,
    `and returns the SAME reservation (${replay.body?.id} vs ${fresh.body?.id})`,
  );

  const held3 = await inventory();
  check(
    held3.held === 2,
    `the replay took no extra seats — two holds from two keys, got held ${held3.held}`,
  );

  const completed = await keyRow(freshKey);
  check(completed?.state === 'completed', `the key is '${completed?.state}', expected 'completed'`);
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
