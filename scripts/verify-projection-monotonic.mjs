/**
 * Asserts that the projection cannot move BACKWARDS under concurrent writers.
 *
 * The monotonic guard — apply a snapshot only when its version is higher than
 * the stored one — is what makes the projection safe under at-least-once
 * redelivery and out-of-order arrival. It used to be a read and then a write:
 *
 *   const existing = await tx.eventProjection.findFirst({ ... });
 *   if (existing && existing.sourceVersion >= version) skip;
 *   else await tx.eventProjection.upsert({ ... });
 *
 * Two writers both read 5, one carrying 6 and one carrying 7. Both pass the
 * check. Both write. Whichever commits last wins — and if that is the 6, the
 * projection has gone backwards: a stale capacity, or a CANCELLED event
 * returning to PUBLISHED and becoming sellable again.
 *
 * WHAT WAS ACTUALLY HOLDING IT UP: messages are keyed by aggregate id, so all
 * messages for one event share a partition, and a partition is consumed by one
 * member of the group, sequentially. That is a property of the DEPLOYMENT, not
 * of the code, it is invisible from the consumer, and it stops holding during a
 * rebalance or the day somebody raises the partition count or changes the key.
 * Nothing would fail at that moment. The projection would just be wrong
 * sometimes.
 *
 * This runs the SAME interleaving against both shapes:
 *
 *   1. both writers observe version 5
 *   2. the HIGH writer (7) writes and commits
 *   3. the LOW writer (6) writes and commits
 *
 * The statement under test is imported from the built consumer module, not
 * copied — so the assertion cannot drift away from the code it guards.
 *
 * The read-then-write shape is reproduced here deliberately, and its result is
 * asserted too. If it ever came out at 7, this test would not be reproducing
 * the race at all and every "ok" below would be meaningless — so the control
 * being RED is itself an assertion.
 *
 * Run: node scripts/verify-projection-monotonic.mjs
 */

import { readFileSync } from 'node:fs';
import pg from 'pg';
import { PROJECTION_UPSERT_SQL } from '../services/ticketing/dist/projection-upsert.js';

const APP_URL =
  process.env['TICKETING_APP_DATABASE_URL'] ??
  'postgresql://eventhub_ticketing_app:eventhub_ticketing_app@localhost:5432/eventhub_ticketing';

const TENANT = 'tenant_acme';
const run = `pm_${Date.now()}`;

const START = 5;
const HIGH = 7;
const LOW = 6;

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}

function client() {
  const c = new pg.Client({ connectionString: APP_URL, application_name: 'verify-monotonic' });
  // This script deliberately runs transactions that block on each other; a
  // Client is an EventEmitter and an unhandled 'error' would exit the process.
  c.on('error', () => {});
  return c;
}

async function begin(c) {
  await c.query('BEGIN');
  await c.query(`SELECT set_config('app.tenant_id', $1, true)`, [TENANT]);
}

const params = (id, version) => [
  id,
  TENANT,
  'Monotonic',
  `monotonic-${id}-v${version}`,
  version === LOW ? 'CANCELLED' : 'PUBLISHED',
  version * 10,
  new Date(),
  new Date(),
  'u_test',
  false,
  version,
];

/** Seed one projection row at START, on its own connection. */
async function seed(admin, id) {
  await begin(admin);
  await admin.query(
    `INSERT INTO "EventProjection"
       (id,"tenantId",title,slug,status,capacity,"startsAt","endsAt","organiserId",deleted,"sourceVersion","updatedAt")
     VALUES ($1,$2,'Monotonic',$3,'PUBLISHED',50,now(),now(),'u_test',false,$4,now())`,
    [id, TENANT, `monotonic-seed-${id}`, START],
  );
  await admin.query('COMMIT');
}

async function finalVersion(admin, id) {
  await begin(admin);
  const { rows } = await admin.query(
    `SELECT "sourceVersion", status FROM "EventProjection" WHERE id = $1`,
    [id],
  );
  await admin.query('COMMIT');
  return rows[0];
}

/**
 * The shape in the code today: one statement, the guard in its WHERE clause.
 * Postgres evaluates it under the row lock ON CONFLICT already takes, so the
 * second writer re-evaluates against what the first committed.
 */
async function raceAtomic(admin, hi, lo, id) {
  await begin(hi);
  await begin(lo);

  const appliedHigh = await hi.query(PROJECTION_UPSERT_SQL, params(id, HIGH));
  await hi.query('COMMIT');

  const appliedLow = await lo.query(PROJECTION_UPSERT_SQL, params(id, LOW));
  await lo.query('COMMIT');

  return { high: appliedHigh.rowCount, low: appliedLow.rowCount };
}

/**
 * The shape this replaced, reproduced exactly: BOTH writers read first, then
 * write. Present as a control — see the header.
 */
async function raceReadThenWrite(admin, hi, lo, id) {
  await begin(hi);
  await begin(lo);

  // 1. Both observe the same stored version. This is the window.
  const seenHigh = (await hi.query(`SELECT "sourceVersion" FROM "EventProjection" WHERE id = $1`, [id]))
    .rows[0].sourceVersion;
  const seenLow = (await lo.query(`SELECT "sourceVersion" FROM "EventProjection" WHERE id = $1`, [id]))
    .rows[0].sourceVersion;

  // 2. Each decides, in its own process, that its version is newer. Both are
  //    right about what they read, and one of them is about to be wrong.
  const highWrites = HIGH > seenHigh;
  const lowWrites = LOW > seenLow;

  if (highWrites) {
    await hi.query(
      `UPDATE "EventProjection"
          SET "sourceVersion" = $2, status = 'PUBLISHED', capacity = $3, "updatedAt" = now()
        WHERE id = $1`,
      [id, HIGH, HIGH * 10],
    );
  }
  await hi.query('COMMIT');

  if (lowWrites) {
    await lo.query(
      `UPDATE "EventProjection"
          SET "sourceVersion" = $2, status = 'CANCELLED', capacity = $3, "updatedAt" = now()
        WHERE id = $1`,
      [id, LOW, LOW * 10],
    );
  }
  await lo.query('COMMIT');

  return { seenHigh, seenLow };
}

async function main() {
  const admin = client();
  const hi = client();
  const lo = client();
  await Promise.all([admin.connect(), hi.connect(), lo.connect()]);

  console.log(`  stored version ${START}; a writer carrying ${HIGH} and a writer carrying ${LOW}`);
  console.log(`  both observe ${START}, then ${HIGH} commits, then ${LOW} commits`);

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  the control: the read-then-write shape this replaced');

  const controlId = `${run}_control`;
  await seed(admin, controlId);
  await raceReadThenWrite(admin, hi, lo, controlId);
  const control = await finalVersion(admin, controlId);

  check(
    control.sourceVersion === LOW,
    control.sourceVersion === LOW
      ? `it went BACKWARDS to ${control.sourceVersion} (status ${control.status}) — the race reproduces`
      : `it ended at ${control.sourceVersion}, not ${LOW}. This test is NOT reproducing the race, ` +
        'so nothing below proves anything',
  );

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  the statement in the consumer today');

  const atomicId = `${run}_atomic`;
  await seed(admin, atomicId);
  const counts = await raceAtomic(admin, hi, lo, atomicId);
  const atomic = await finalVersion(admin, atomicId);

  check(counts.high === 1, `the newer snapshot applied (${counts.high} row)`);
  check(counts.low === 0, `the older snapshot changed nothing (${counts.low} rows)`);
  check(
    atomic.sourceVersion === HIGH,
    `version is ${atomic.sourceVersion}, expected ${HIGH}`,
  );
  check(
    atomic.status === 'PUBLISHED',
    `status is ${atomic.status}, expected PUBLISHED — a reverted projection would read CANCELLED`,
  );

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  and the reverse order, which must reach the same answer');

  const reverseId = `${run}_reverse`;
  await seed(admin, reverseId);
  await begin(hi);
  await begin(lo);
  const lowFirst = await lo.query(PROJECTION_UPSERT_SQL, params(reverseId, LOW));
  await lo.query('COMMIT');
  const highSecond = await hi.query(PROJECTION_UPSERT_SQL, params(reverseId, HIGH));
  await hi.query('COMMIT');
  const reverse = await finalVersion(admin, reverseId);

  check(lowFirst.rowCount === 1, `${LOW} applied over ${START} (${lowFirst.rowCount} row)`);
  check(highSecond.rowCount === 1, `${HIGH} then applied over ${LOW} (${highSecond.rowCount} row)`);
  check(
    reverse.sourceVersion === HIGH,
    `version is ${reverse.sourceVersion}, expected ${HIGH} regardless of arrival order`,
  );

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  and the consumer uses this statement, not another one');

  const consumerSrc = readFileSync('services/ticketing/dist/consumer.js', 'utf8');
  check(
    consumerSrc.includes('PROJECTION_UPSERT_SQL'),
    'the built consumer references PROJECTION_UPSERT_SQL',
  );
  check(
    !/eventProjection\.upsert/.test(consumerSrc) && !/eventProjection\.findFirst/.test(consumerSrc),
    'the built consumer has no read-then-write left on EventProjection',
  );

  await Promise.all([admin.end(), hi.end(), lo.end()]);

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exit(1);
  }
  console.log('the projection cannot be moved backwards by a concurrent older snapshot');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
