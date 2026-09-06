/**
 * Asserts that the two retention-critical queries use their index to ORDER, not
 * merely to filter.
 *
 * An index that is "used" is not the same as an index that helps. Both of these
 * queries end in `ORDER BY ... LIMIT n`, and there are two shapes the planner
 * can produce:
 *
 *   Index Scan            walks the index in order and stops at n. Work is
 *                         bounded by the LIMIT.
 *   Bitmap Scan + Sort    collects every matching row, sorts all of them, then
 *                         takes n. Work is bounded by the TABLE.
 *
 * The second still says "Index Scan" in the plan, still looks like the index is
 * doing its job, and grows without limit as the table does. The reconciler's
 * query was exactly that until `nextLookupAt` stopped being nullable — the
 * `IS NULL OR <= now()` forced a bitmap scan:
 *
 *   Sort  (Sort Key: "nextLookupAt" NULLS FIRST, "startedAt")
 *     -> Bitmap Heap Scan on "Payment"
 *          -> Bitmap Index Scan on "Payment_reconcilable_idx"
 *
 * Both roles carry statement_timeout=15s, so this is not a latency question. A
 * plan whose cost tracks the table rather than the page eventually stops
 * completing, and it stops in the direction where the table keeps growing.
 *
 * ASSERTED ON THE PLAN, not on a timing. A benchmark on a near-empty CI database
 * proves nothing about the shape of the work; the plan says what the work IS.
 *
 * AND ASSERTED AS A CAPABILITY, not as the plan the planner happens to pick.
 * On a table with a few dozen rows a bitmap scan and a sort really is cheaper,
 * and the planner is right to choose it — asserting "no Sort" outright would be
 * asserting a decision that only goes the other way once the table is big, which
 * is the moment CI cannot reach. So seqscan and bitmapscan are both disabled and
 * the question becomes: WHEN an ordered scan is needed, can this index provide
 * one without sorting? That answer does not move with table size.
 *
 * The control is at the bottom: the same query written the way it used to be,
 * with the OR that the nullable column forced. It cannot produce an ordered scan
 * however the planner is nudged — which is what makes the assertion above mean
 * something rather than being true of any index at all.
 *
 * Run: node scripts/verify-query-plans.mjs
 */

import pg from 'pg';

/**
 * THE ROLE MATTERS, and it is not a detail — it changes the plan.
 *
 * Both tables are FORCE ROW LEVEL SECURITY. A role with no matching policy does
 * not merely see nothing; Postgres injects a constant-false qual, and that
 * changes the shape of the plan outright. Explaining the reconciler's query as
 * the table OWNER produces:
 *
 *   Filter: (false AND ("nextLookupAt" <= now()) AND ...)
 *     -> Bitmap Index Scan on "Payment_reconcilable_idx"
 *
 * — a bitmap scan and a Sort, for a query that is a clean ordered Index Scan
 * when run by the credential that actually runs it. The first version of this
 * file used the owner for both and reported a regression that did not exist.
 *
 * So each query is explained as the role whose code issues it: the sweeper
 * credential for the reconciler, the relay credential for the outbox prune.
 */
const SWEEPER =
  process.env['SWEEPER_DATABASE_URL'] ??
  'postgresql://eventhub_ticketing_sweeper:eventhub_ticketing_sweeper@localhost:5432/eventhub_ticketing';
const RELAY =
  process.env['RELAY_DATABASE_URL'] ??
  'postgresql://eventhub_event_relay:eventhub_event_relay@localhost:5432/eventhub_event';

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}

/**
 * The queries, written to match what the code actually runs.
 *
 * Kept verbatim rather than imported, because these are the only two statements
 * in the repository whose PLAN is part of the contract — and a copy that drifts
 * would show up as this file passing while the real query regressed. The
 * `mustMention` field is what pins them together: if the code stops using the
 * index this names, the assertion fails even though the plan still looks fine.
 */
const PLANS = [
  {
    label: 'the reconciler picking its next candidates',
    url: SWEEPER,
    role: 'eventhub_ticketing_sweeper',
    index: 'Payment_reconcilable_idx',
    sql: `SELECT id FROM "Payment"
           WHERE "settledAt" IS NULL
             AND "startedAt" < now() - interval '1 second'
             AND "nextLookupAt" <= now()
           ORDER BY "nextLookupAt", "startedAt"
           LIMIT 50`,
  },
  {
    label: 'the relay pruning delivered outbox rows',
    url: RELAY,
    role: 'eventhub_event_relay',
    index: 'OutboxMessage_prunable_idx',
    sql: `SELECT id FROM "OutboxMessage"
           WHERE "publishedAt" IS NOT NULL
             AND "deadLetteredAt" IS NULL
             AND "publishedAt" < now() - interval '365 days'
           ORDER BY "publishedAt"
           LIMIT 1000`,
  },
];

async function main() {
  for (const plan of PLANS) {
    const db = new pg.Client({ connectionString: plan.url, application_name: 'verify-plans' });
    db.on('error', () => {});
    await db.connect();

    console.log(`  ${plan.label}  [as ${plan.role}]`);

    /**
     * Both off for this session only, so the planner has to answer the question
     * this file is actually asking. See the header: on a small table it would
     * otherwise pick a bitmap scan and a sort, correctly, and the assertion
     * would be measuring the size of the CI database rather than the index.
     */
    await db.query('SET enable_seqscan = off');
    await db.query('SET enable_bitmapscan = off');

    const { rows } = await db.query(`EXPLAIN ${plan.sql}`);
    const text = rows.map((r) => r['QUERY PLAN']).join('\n');

    /**
     * If row security has filtered this role out entirely the plan carries a
     * constant-false qual, and everything below it describes a query over no
     * rows. That is not a plan worth asserting on, so say so plainly.
     */
    check(
      !/false/.test(text),
      /false/.test(text)
        ? `row security excluded ${plan.role} from this table — the plan is over nothing, so ` +
          'the assertions below would prove nothing'
        : `${plan.role} can see the table, so this plan describes real work`,
    );

    check(
      text.includes(plan.index),
      text.includes(plan.index)
        ? `uses ${plan.index}`
        : `does NOT use ${plan.index} — the index is not serving this query at all:\n` +
          text.split('\n').map((l) => `          ${l}`).join('\n'),
    );

    check(
      !/\bSort\b/.test(text),
      /\bSort\b/.test(text)
        ? 'the plan contains a Sort — the index filters but does not order, so every ' +
          'matching row is collected and sorted before the LIMIT takes a page'
        : 'no Sort: the index orders as well as filters, so the LIMIT bounds the scan',
    );

    check(
      /Index Scan/.test(text) && !/Bitmap/.test(text),
      /Bitmap/.test(text)
        ? 'it is a Bitmap scan, which is unordered by construction'
        : 'and it is a plain ordered Index Scan',
    );

    await db.end();
    console.log('');
  }

  // -------------------------------------------------------------------------
  console.log('  the control: the same shape with a nullable column');
  // -------------------------------------------------------------------------
  /**
   * On a TEMP table, not on Payment, and that is the point.
   *
   * The obvious control — run the old `IS NULL OR <= now()` against Payment —
   * does not work any more, and the reason is the fix itself: the column is NOT
   * NULL, so the planner folds `IS NULL` to false, the OR disappears, and it
   * produces the same ordered scan. I wrote that control first and it passed,
   * which told me it was measuring nothing.
   *
   * So the control reproduces the SHAPE on a table where the column really is
   * nullable: same partial index, same ORDER BY, same planner settings. If the
   * OR form sorted here and the real queries did not, the difference is the
   * shape rather than the settings — which is what the assertions above claim.
   *
   * No RLS on a temp table, so the role does not matter here the way it does
   * above; this runs on whichever connection is to hand.
   */
  const control = new pg.Client({ connectionString: SWEEPER, application_name: 'verify-plans' });
  control.on('error', () => {});
  await control.connect();
  await control.query('SET enable_seqscan = off');
  await control.query('SET enable_bitmapscan = off');

  let controlText = '';
  let controlErr = null;
  try {
    await control.query(
      `CREATE TEMP TABLE plan_shape (
         id text PRIMARY KEY,
         settled_at timestamp(3),
         due_at timestamp(3),
         started_at timestamp(3) NOT NULL
       )`,
    );
    await control.query(
      `CREATE INDEX plan_shape_idx ON plan_shape (due_at, started_at) WHERE settled_at IS NULL`,
    );
    // Enough rows that an ordered scan is a plan the planner would consider.
    await control.query(
      `INSERT INTO plan_shape
         SELECT g::text, NULL, now() - (g || ' minutes')::interval, now()
           FROM generate_series(1, 500) g`,
    );
    await control.query('ANALYZE plan_shape');

    const { rows } = await control.query(
      `EXPLAIN SELECT id FROM plan_shape
          WHERE settled_at IS NULL
            AND (due_at IS NULL OR due_at <= now())
          ORDER BY due_at NULLS FIRST, started_at
          LIMIT 50`,
    );
    controlText = rows.map((r) => r['QUERY PLAN']).join('\n');
  } catch (err) {
    controlErr = err;
  } finally {
    await control.end();
  }

  check(
    controlErr === null,
    controlErr
      ? `the control could not run: ${String(controlErr.message).slice(0, 70)}`
      : 'the control table was built',
  );

  check(
    controlErr === null && /\bSort\b/.test(controlText),
    controlErr
      ? 'skipped'
      : /\bSort\b/.test(controlText)
        ? 'the nullable OR form sorts even with both scan types disabled — so the assertions ' +
          'above are about the query shape, not about the planner settings'
        : 'the OR form ALSO avoided a sort, which would mean nothing above was being tested',
  );

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('both retention-critical queries can stop at their LIMIT rather than sorting the table');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
