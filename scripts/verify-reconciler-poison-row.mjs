/**
 * Asserts that one payment the provider cannot be asked about does not stop the
 * reconciler examining every other one.
 *
 * The loop's own catch says it: "One bad row must not stop the batch. It stays
 * unsettled and is picked up next pass, which is the safe direction to fail."
 * That catch covered only the DATABASE settlement. The `provider.lookup()` call
 * three lines above it was outside, so a lookup that REJECTED escaped the loop
 * and abandoned the rest of the batch.
 *
 * A timeout is not the hazard — withTimeout() resolves to `unknown`, so a slow
 * provider was already handled. A rejection is different: a DNS failure, a
 * connection reset, a 500 the client surfaces as a throw.
 *
 * And it does not merely lose one pass. The candidate query is
 * `ORDER BY "startedAt" LIMIT n` with no cursor and no per-row failure marking,
 * so the SAME row is selected first next time. A payment whose lookup fails
 * deterministically sits at the head of every pass forever and every payment
 * behind it is never examined — in the job that decides whether customers who
 * may already have been charged get their seats or their money back.
 *
 * This imports reconcileOnce() itself and hands it a provider that throws for
 * one specific attempt. reconcile.ts imports nothing but types and
 * settleAttempt, so it loads with no environment at all — the test drives the
 * real function rather than a copy of its logic.
 *
 * Run: node scripts/verify-reconciler-poison-row.mjs
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
const run = `pr_${Date.now()}`;

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

/** Quiet unless something interesting happens — the reconciler logs per row. */
const logger = {
  info: () => {},
  warn: () => {},
  error: () => {},
  debug: () => {},
};

const app = new pg.Client({ connectionString: APP_URL, application_name: 'verify-poison' });
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

/**
 * Three unsettled attempts, seeded oldest-first so the poison one is at the
 * HEAD of the batch. That ordering is the whole point: the query is
 * ORDER BY "startedAt", so a row that throws first is a row that blocks
 * everything behind it.
 *
 * SECONDS OLD, NOT HOURS. This pass takes anything over 1000 ms; any deployed
 * reconciler waits more than twice the provider timeout, over 16 s by default.
 * Seeded ten hours old, the rows were every reconciler's the moment they
 * committed, and compose runs one now: its fake provider answers "succeeded"
 * for 1000, so a pass landing before this one settled all three, the poison row
 * included. Measured with the window widened to 6 s, one compose interval: 5
 * FAIL, "asked the provider about 0 attempt(s)" -- and "the attempts behind it
 * settled" green beside them, for the compose reconciler's work. Two to four
 * seconds old is past this pass's grace and inside every deployed one's.
 */
async function seed() {
  const eventId = `${run}_evt`;
  const ticketTypeId = `${run}_tt`;

  await tx(async () => {
    await app.query(
      `INSERT INTO "EventProjection"
         (id,"tenantId",title,slug,status,capacity,"startsAt","endsAt","organiserId","sourceVersion","updatedAt")
       VALUES ($1,$2,'Poison Row',$3,'PUBLISHED',100,now(),now(),'u_test',1,now())`,
      [eventId, TENANT, `poison-${run}`],
    );
    await app.query(
      `INSERT INTO "TicketType"
         (id,"tenantId","eventId",name,"priceCents",total,remaining,held,sold,"updatedAt")
       VALUES ($1,$2,$3,'General',1000,30,27,3,0,now())`,
      [ticketTypeId, TENANT, eventId],
    );

    for (const [i, label] of ['poison', 'good1', 'good2'].entries()) {
      const reservationId = `${run}_res_${label}`;
      await app.query(
        `INSERT INTO "Reservation"
           (id,"tenantId","eventId","userId",status,"expiresAt","paymentStartedAt","version","createdAt","updatedAt")
         VALUES ($1,$2,$3,'u_test','PENDING_PAYMENT', now() + interval '1 hour', now(), 1, now(), now())`,
        [reservationId, TENANT, eventId],
      );
      await app.query(
        `INSERT INTO "ReservationItem"
           (id,"tenantId","reservationId","ticketTypeId",quantity,"unitPriceCents")
         VALUES ($1,$2,$3,$4,1,1000)`,
        [`${run}_item_${label}`, TENANT, reservationId, ticketTypeId],
      );
      await app.query(
        `INSERT INTO "Payment"
           (id,"tenantId","reservationId","userId","amountCents",currency,status,"startedAt","updatedAt")
         VALUES ($1,$2,$3,'u_test',1000,'SGD','UNKNOWN', now() - ($4::int * interval '1 second'), now())`,
        [`${run}_pay_${label}`, TENANT, reservationId, 4 - i],
      );
    }
  });

  return { eventId, ticketTypeId };
}

async function main() {
  await app.connect();
  const { ticketTypeId } = await seed();
  console.log('  3 unsettled attempts; the one whose lookup throws is the OLDEST');

  const poisonId = `${run}_pay_poison`;
  let asked = 0;

  /**
   * Succeeds for everything except the head of the batch, which rejects the way
   * a real client rejects on a reset connection.
   *
   * ANYTHING NOT SEEDED BY THIS RUN ANSWERS `unknown`, which leaves the row
   * exactly as it was. reconcileOnce() selects every unsettled Payment older
   * than the grace window, not just this run's, so a provider that answered
   * "succeeded" to all of them would SETTLE other verifiers' fixtures as a side
   * effect — and the red build would point at whichever test ran next. That
   * mistake has already been made once in this repository; this is the shape
   * that prevents it.
   */
  const mine = (id) => id.startsWith(`${run}_pay_`);

  const provider = {
    async charge() {
      throw new Error('charge is not used by reconcileOnce');
    },
    async lookup(req) {
      if (!mine(req.idempotencyKey)) {
        return { status: 'unknown', reason: 'not_this_test' };
      }
      asked += 1;
      if (req.idempotencyKey === poisonId) {
        throw Object.assign(new Error('socket hang up'), { code: 'ECONNRESET' });
      }
      return { status: 'succeeded', providerRef: `fake_ref_${req.idempotencyKey}` };
    },
  };

  const pool = new pg.Pool({ connectionString: SWEEPER_URL, max: 2 });
  pool.on('error', () => {});

  let threw = null;
  let result = null;
  try {
    result = await reconcileOnce(pool, provider, logger, { batchSize: 50, graceMs: 1000 });
  } catch (err) {
    threw = err;
  }

  check(
    threw === null,
    threw === null
      ? 'reconcileOnce returned rather than throwing'
      : `reconcileOnce THREW (${threw.message}) — the rest of the batch was abandoned`,
  );

  check(
    asked >= 3,
    `it asked the provider about ${asked} attempt(s) — it must not stop at the first failure`,
  );

  if (result) {
    check(result.lookupFailed === 1, `lookupFailed is ${result.lookupFailed}, expected 1`);
    check(
      result.confirmed >= 2,
      `it settled ${result.confirmed} attempt(s) behind the poison row, expected at least 2`,
    );
  }

  /**
   * The state assertion, which is the one that matters to a customer: the two
   * good attempts are resolved, and the poison one is untouched so the next
   * pass can try again.
   */
  const rows = await tx(async () => {
    const { rows } = await app.query(
      `SELECT id, status, "settledAt" IS NOT NULL AS settled FROM "Payment"
        WHERE id LIKE $1 ORDER BY id`,
      [`${run}_pay_%`],
    );
    return rows;
  });

  const byLabel = Object.fromEntries(rows.map((r) => [r.id.replace(`${run}_pay_`, ''), r]));

  check(
    byLabel.good1?.status === 'SUCCEEDED' && byLabel.good2?.status === 'SUCCEEDED',
    `the attempts behind it settled (good1=${byLabel.good1?.status}, good2=${byLabel.good2?.status})`,
  );
  check(
    byLabel.poison?.status === 'UNKNOWN' && byLabel.poison?.settled === false,
    `the poison attempt is untouched and still unsettled (${byLabel.poison?.status}), so the ` +
      'next pass will ask again',
  );

  const inv = await tx(async () => {
    const { rows } = await app.query(
      `SELECT remaining, held, sold FROM "TicketType" WHERE id = $1`,
      [ticketTypeId],
    );
    return rows[0];
  });
  check(
    inv.sold === 2 && inv.held === 1,
    `inventory moved only for the settled attempts (held ${inv.held}, sold ${inv.sold})`,
  );

  await pool.end();
  await app.end();

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exit(1);
  }
  console.log('a payment the provider cannot be asked about costs one row, not the whole batch');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
