/**
 * Asserts that a payment the provider cannot be asked about stops occupying a
 * slot in every pass.
 *
 * #43 fixed the urgent half: a thrown lookup used to escape the loop and leave
 * every payment behind it unexamined. What it could not fix without a migration
 * is that the row is still SELECTED first next time — the candidate query is
 * ORDER BY "startedAt" with no cursor and no record of what has already failed.
 *
 * So a handful of poison rows shrink the effective batch, and at
 * RECONCILE_BATCH_SIZE of them the job examines nothing else at all. The
 * reconciler is what decides whether customers who may already have been charged
 * get their seats or their money back, so "it quietly stops looking at anyone
 * else" is the wrong way to fail.
 *
 * THREE PASSES, and the shape is the assertion:
 *
 *   pass 1  the poison row is asked about, throws, and is backed off
 *   pass 2  it is NOT asked about again, and the payments behind it are
 *   pass 3  after the backoff is forced to expire, it IS asked again — because
 *           there is money on the other side of this row, so it is deferred and
 *           never abandoned
 *
 * Pass 3 is the one that stops the fix being "ignore it forever", which would be
 * a worse bug wearing the same clothes.
 *
 * Run: node scripts/verify-reconciler-backoff.mjs
 */

import pg from 'pg';
import { reconcileOnce } from '../services/ticketing/dist/reconciler/reconcile.js';

const SWEEPER_URL =
  process.env['SWEEPER_DATABASE_URL'] ??
  'postgresql://eventhub_ticketing_sweeper:eventhub_ticketing_sweeper@localhost:5432/eventhub_ticketing';
const APP_URL =
  process.env['TICKETING_APP_DATABASE_URL'] ??
  'postgresql://eventhub_ticketing_app:eventhub_ticketing_app@localhost:5432/eventhub_ticketing';

const TENANT = 'tenant_acme';
const run = `bo_${Date.now()}`;

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}

const logger = { info: () => {}, warn: () => {}, error: () => {}, debug: () => {} };

const app = new pg.Client({ connectionString: APP_URL, application_name: 'verify-backoff' });
app.on('error', () => {});

async function tx(fn) {
  await app.query('BEGIN');
  await app.query(`SELECT set_config('app.tenant_id', $1, true)`, [TENANT]);
  try {
    const out = await fn();
    await app.query('COMMIT');
    return out;
  } catch (err) {
    await app.query('ROLLBACK').catch(() => {});
    throw err;
  }
}

const ids = { event: `${run}_evt`, ticketType: `${run}_tt` };
const LABELS = ['poison', 'good1', 'good2'];

async function seed() {
  await tx(async () => {
    await app.query(
      `INSERT INTO "EventProjection"
         (id,"tenantId",title,slug,status,capacity,"startsAt","endsAt","organiserId","sourceVersion","updatedAt")
       VALUES ($1,$2,'Backoff',$3,'PUBLISHED',100,now(),now(),'u_test',1,now())`,
      [ids.event, TENANT, `backoff-${run}`],
    );
    await app.query(
      `INSERT INTO "TicketType"
         (id,"tenantId","eventId",name,"priceCents",total,remaining,held,sold,"updatedAt")
       VALUES ($1,$2,$3,'General',1000,30,27,3,0,now())`,
      [ids.ticketType, TENANT, ids.event],
    );

    // Oldest first, so the poison row heads the batch — which is the whole
    // problem being fixed.
    for (const [i, label] of LABELS.entries()) {
      const reservationId = `${run}_res_${label}`;
      await app.query(
        `INSERT INTO "Reservation"
           (id,"tenantId","eventId","userId",status,"expiresAt","paymentStartedAt",version,"createdAt","updatedAt")
         VALUES ($1,$2,$3,'u_test','PENDING_PAYMENT', now() + interval '1 hour', now(), 1, now(), now())`,
        [reservationId, TENANT, ids.event],
      );
      await app.query(
        `INSERT INTO "ReservationItem"
           (id,"tenantId","reservationId","ticketTypeId",quantity,"unitPriceCents")
         VALUES ($1,$2,$3,$4,1,1000)`,
        [`${run}_item_${label}`, TENANT, reservationId, ids.ticketType],
      );
      await app.query(
        `INSERT INTO "Payment"
           (id,"tenantId","reservationId","userId","amountCents",currency,status,"startedAt","updatedAt")
         VALUES ($1,$2,$3,'u_test',1000,'SGD','UNKNOWN', now() - ($4::int * interval '1 hour'), now())`,
        [`${run}_pay_${label}`, TENANT, reservationId, 10 - i],
      );
    }
  });
}

/**
 * Ask the DATABASE how far away nextLookupAt is, rather than comparing it to a
 * JS Date.
 *
 * Every timestamp in this schema is TIMESTAMP(3) WITHOUT TIME ZONE, so pg hands
 * it back as a Date interpreted in the host's zone while now() wrote it in the
 * database's. The first version of this file compared the two directly and made
 * a 60-second backoff look like it was eight hours in the past.
 *
 * The production code never has that problem, because its comparison
 * (nextLookupAt IS NULL OR nextLookupAt <= now()) happens entirely inside SQL.
 * The test should assert in the same frame the code decides in.
 */
const paymentRow = (label) =>
  tx(async () => {
    const { rows } = await app.query(
      `SELECT status,
              "lookupAttempts",
              "nextLookupAt" IS NULL                                   AS "neverScheduled",
              "nextLookupAt" > now()                                   AS "inFuture",
              extract(epoch from ("nextLookupAt" - now()))::int         AS "secondsAway"
         FROM "Payment" WHERE id = $1`,
      [`${run}_pay_${label}`],
    );
    return rows[0];
  });

async function main() {
  await app.connect();
  await seed();
  console.log('  3 unsettled attempts; the one whose lookup throws is the OLDEST');

  const poisonId = `${run}_pay_poison`;
  let asked = [];

  const provider = {
    async charge() {
      throw new Error('charge is not used by reconcileOnce');
    },
    async lookup(req) {
      // Anything this run did not seed answers `unknown`, which leaves the row
      // exactly as it was — reconcileOnce selects every unsettled Payment, not
      // just these, and settling another verifier's fixtures as a side effect
      // has produced a red build pointing at the wrong file before.
      if (!req.idempotencyKey.startsWith(`${run}_pay_`)) {
        return { status: 'unknown', reason: 'not_this_test' };
      }
      asked.push(req.idempotencyKey);
      if (req.idempotencyKey === poisonId) {
        throw Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' });
      }
      return { status: 'succeeded', providerRef: `fake_ref_${req.idempotencyKey}` };
    },
  };

  const pool = new pg.Pool({ connectionString: SWEEPER_URL, max: 2 });
  pool.on('error', () => {});
  const pass = () =>
    reconcileOnce(pool, provider, logger, { batchSize: 50, graceMs: 1000 });

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  pass 1: it is asked, it throws, and it is backed off');
  // -------------------------------------------------------------------------
  asked = [];
  const first = await pass();
  check(asked.includes(poisonId), 'the poison attempt was asked about');
  check(first.lookupFailed === 1, `lookupFailed is ${first.lookupFailed}, expected 1`);
  check(first.confirmed >= 2, `and the two behind it settled (${first.confirmed})`);

  const afterFirst = await paymentRow('poison');
  check(
    afterFirst.lookupAttempts === 1,
    `lookupAttempts is ${afterFirst.lookupAttempts}, expected 1`,
  );
  check(
    !afterFirst.neverScheduled && afterFirst.inFuture,
    `nextLookupAt is ${afterFirst.secondsAway}s in the future`,
  );
  check(afterFirst.status === 'UNKNOWN', `and it is still unsettled (${afterFirst.status})`);

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  pass 2: it is not asked again, and it is not holding a slot');
  // -------------------------------------------------------------------------
  asked = [];
  const second = await pass();
  check(
    !asked.includes(poisonId),
    asked.includes(poisonId)
      ? 'it was asked about again immediately — the backoff is not being read'
      : 'it was skipped',
  );
  check(
    second.lookupFailed === 0,
    `and the pass reported no lookup failures (${second.lookupFailed})`,
  );

  const afterSecond = await paymentRow('poison');
  check(
    afterSecond.lookupAttempts === 1,
    `attempts did not move (${afterSecond.lookupAttempts}) — it was genuinely not tried`,
  );

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  pass 3: once the backoff expires it is asked again — never abandoned');
  // -------------------------------------------------------------------------
  /**
   * The control that stops the fix being "ignore it forever". There is money on
   * the other side of an unsettled payment: this defers a row, it does not give
   * up on one. Without this assertion, a change that simply excluded failed
   * lookups permanently would pass everything above.
   */
  await tx(() =>
    app.query(`UPDATE "Payment" SET "nextLookupAt" = now() - interval '1 minute' WHERE id = $1`, [
      poisonId,
    ]),
  );

  asked = [];
  const third = await pass();
  check(asked.includes(poisonId), 'it came back round once its backoff was due');
  check(third.lookupFailed === 1, `and failed again (${third.lookupFailed})`);

  const afterThird = await paymentRow('poison');
  check(
    afterThird.lookupAttempts === 2,
    `attempts climbed to ${afterThird.lookupAttempts}, expected 2`,
  );

  /**
   * The delay must GROW. A fixed retry interval would satisfy every assertion
   * above while still hammering a provider that is down.
   */
  check(
    afterThird.secondsAway > 60,
    `and the next wait is longer than the first (${afterThird.secondsAway}s, first was 60s) — ` +
      'a fixed interval would pass everything above',
  );

  await pool.end();
  await app.end();

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('an unanswerable payment steps aside for the ones that can be settled, and comes back');
}

main().catch(async (err) => {
  console.error(err);
  process.exitCode = 1;
  await app.end().catch(() => {});
});
