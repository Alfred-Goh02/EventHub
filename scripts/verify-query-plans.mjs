/**
 * Asserts that each plan-critical query uses its index to ORDER, not merely to
 * filter -- the relay's claim, the relay's prune, the reconciler's candidate scan,
 * and the hold sweeper's claim.
 *
 * An index that is "used" is not the same as an index that helps. Each of these
 * queries ends in `ORDER BY ... LIMIT n`, and there are two shapes the planner
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
import { CLAIM_SQL } from '../services/relay/dist/claim-sql.js';
import { SWEEP_CLAIM_SQL } from '../services/ticketing/dist/sweeper/claim-sql.js';

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
 * The queries whose PLAN is part of the contract.
 *
 * THE CLAIM IS IMPORTED, NOT COPIED, AND THAT CHANGED BECAUSE A COPY FAILED.
 * This block used to justify hand-written copies on the grounds that "a copy
 * that drifts would show up as this file passing while the real query
 * regressed". That sentence describes the failure rather than preventing it, and
 * it duly happened: the copy of the claim selected a bare `id` out of the CTE,
 * so it never carried the outer `seq::text AS "seq"` alias — and that alias is
 * the entirety of #64, where a bare `ORDER BY seq` resolved to the text output
 * column and published '10' before '9'. This file could not have caught it,
 * because the query it explained was not the query anyone runs.
 *
 * So the claim now comes from services/relay/src/claim-sql.ts, the same text
 * drain.ts executes. The two prune/reconcile queries below are still written out
 * here, and that is a weaker position than the claim's: see the note on each.
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
  {
    label: 'the relay claiming a batch to publish',
    url: RELAY,
    role: 'eventhub_event_relay',
    index: 'OutboxMessage_undelivered_idx',
    /**
     * The claim is the relay's hot path, and the query most at risk from the
     * ordering guards built around it: a NOT EXISTS for rows stuck behind a
     * backoff, and a per-aggregate advisory lock.
     *
     * The advisory lock is VOLATILE, and putting it in the inner WHERE cost
     * exactly what this file exists to catch. The planner stopped using the
     * partial index to supply the ordering and produced a bitmap scan with a
     * Sort on top -- the shape OutboxMessage_undelivered_idx was created to
     * avoid. It also meant the lock was evaluated for every pending row
     * BEFORE the LIMIT, so one drain would hold an advisory lock per
     * aggregate in the entire backlog, and max_locks_per_transaction is
     * finite. Both are why the claim is a CTE.
     */
    outerSort: 'the CTE bounds it to batchSize rows before the advisory-lock filter sees them',
    /**
     * The real text, imported. See the block above this array for what a copy of
     * it cost, and claim-sql.ts for why that module exists.
     */
    sql: CLAIM_SQL,
    /** The batch size. EXPLAIN takes parameters; it just never runs the query. */
    params: [100],
    /**
     * The claim reads "OutboxMessage" TWICE, and only one of the two reads was
     * ever asserted.
     *
     * The NOT EXISTS that looks for an older row stuck behind a backoff is a
     * correlated subquery run for candidate rows, and it has its own partial
     * index. Nothing checked that the planner used it. Unindexed, that subquery
     * is a scan of the pending backlog per candidate row — the ordering guard
     * added in #61 would quietly become the most expensive thing the relay does,
     * and every assertion in this file would still have passed, because they all
     * describe the OUTER scan.
     */
    alsoUses: ['OutboxMessage_backoff_idx'],
  },
  {
    label: 'the sweeper claiming expired seat holds',
    url: SWEEPER,
    role: 'eventhub_ticketing_sweeper',
    index: 'Reservation_sweep_idx',
    /**
     * The fourth query with this shape, and the one nothing was checking.
     *
     * Reservation_sweep_idx is partial -- ON "Reservation" ("expiresAt") WHERE
     * status = 'HELD' -- and it has to supply the ordering for the LIMIT to bound
     * the work. Dropping the index, or ordering by anything it cannot supply,
     * turns the sweeper into a scan-and-sort of every reservation ever made,
     * which is the table that grows fastest here.
     *
     * WHAT THIS DOES NOT COVER, stated because the obvious assumption is wrong.
     * sweep.ts writes `status = 'HELD'` as a literal rather than binding it,
     * because a partial index is only usable when the planner can prove the query
     * implies its predicate, and as a parameter the value arrives as
     * ('HELD'::cstring)::"ReservationStatus", which it could not fold -- measured,
     * at the time, as a Seq Scan. It is tempting to assume this entry guards that.
     * It does not, and cannot: the header explains that seqscan and bitmapscan are
     * both disabled so the question becomes a capability rather than a preference,
     * and with them off there is no Seq Scan left to fall back TO. Checked rather
     * than assumed -- binding the status changes nothing about this plan.
     *
     * So the literal is still load-bearing and still unguarded. It is guarded by
     * the comment on it in claim-sql.ts and by nothing else.
     */
    sql: SWEEP_CLAIM_SQL,
    params: [100],
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

    const { rows } = await db.query(`EXPLAIN ${plan.sql}`, plan.params ?? []);
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

    /**
     * A Sort is the failure this file is named for: the index filters but
     * does not order, so every matching row is collected and sorted before
     * the LIMIT takes a page, and the query degrades as the backlog grows.
     *
     * A case may declare `outerSort` when a sort is over a set something else
     * has already bounded. That TIGHTENS the rule rather than relaxing it:
     * the sort must be OUTSIDE the scan that reads the table, so the scan is
     * still ordered by the index. A Sort under the CTE still fails.
     */
    const scan = plan.outerSort ? text.slice(text.indexOf('CTE ')) : text;

    check(
      !/\bSort\b/.test(scan),
      /\bSort\b/.test(scan)
        ? 'the plan contains a Sort — the index filters but does not order, so every ' +
          'matching row is collected and sorted before the LIMIT takes a page'
        : plan.outerSort
          ? `no Sort in the scan itself; the one above it is fine because ${plan.outerSort}`
          : 'no Sort: the index orders as well as filters, so the LIMIT bounds the scan',
    );

    check(
      /Index Scan/.test(scan) && !/Bitmap/.test(scan),
      /Bitmap/.test(scan)
        ? 'it is a Bitmap scan, which is unordered by construction'
        : 'and it is a plain ordered Index Scan',
    );

    for (const also of plan.alsoUses ?? []) {
      check(
        text.includes(also),
        text.includes(also)
          ? `and ${also} serves the subquery's read of the same table`
          : `does NOT use ${also}. This query reads the table more than once, and the read ` +
            'this index serves is unasserted by every other check here, all of which ' +
            'describe the outer scan:\n' +
            text.split('\n').map((l) => `          ${l}`).join('\n'),
      );
    }

    /**
     * WHAT the plan sorts by, not merely whether it sorts.
     *
     * Importing the real claim text was necessary to catch #64 and not
     * sufficient, which is worth recording because it was a surprise. The bug
     * survives every assertion above: a bare `ORDER BY seq` resolving to the
     * `seq::text` output column still uses the index, still produces no Sort
     * inside the scan, and still reports a plain ordered Index Scan. Verified by
     * reintroducing it -- the run came back entirely green.
     *
     * The plans differ in one character sequence:
     *
     *   correct   Sort Key: c.seq
     *   bug #64   Sort Key: ((c.seq)::text)
     *
     * and a text sort puts '10' before '9', so any batch straddling a power of
     * ten published out of order onto a COMPACTED topic -- where ordering
     * decides which version survives a rebuild.
     *
     * Stated as "no cast in the sort key" rather than "the sort key is exactly
     * c.seq" on purpose. The first is the defect and cannot fire on an unrelated
     * improvement to the plan; the second would turn any legitimate change to
     * the ordering into a red build for no reason.
     */
    const sortKeys = text
      .split('\n')
      .map((l) => l.trim())
      .filter((l) => l.startsWith('Sort Key:'))
      .map((l) => l.replace(/^Sort Key:\s*/, ''));

    const cast = sortKeys.filter((k) => k.includes('::'));
    check(
      cast.length === 0,
      cast.length === 0
        ? sortKeys.length > 0
          ? `and it orders by the column itself, uncast: ${sortKeys.join(' | ')}`
          : 'and it does not sort at all, so there is no sort key to get wrong'
        : `the sort key is a CAST: ${cast.join(' | ')}. Ordering by a text rendering of a ` +
          "number sorts '10' before '9' — this is #64, versions published out of order onto a " +
          'compacted topic. Qualify the column (c.seq), so the name resolves to the bigint ' +
          'input column rather than to the cast output column that shares its name.',
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
  console.log(
    'every plan-critical query stops at its LIMIT rather than sorting the table, and the ' +
      'claim orders by the bigint it means to',
  );
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
