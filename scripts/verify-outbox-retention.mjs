/**
 * Asserts that delivered outbox rows are pruned, that the rows which must
 * survive do, and that a prune which deletes nothing cannot report success.
 *
 * Nothing pruned this table in either database. The relay was granted DELETE
 * precisely so that something could — the migration comment says "DELETE is
 * granted because nothing else can prune this table" — and the code was never
 * written. The grant existed; the job did not.
 *
 * THE THIRD ASSERTION IS THE REASON THIS FILE IS INTERESTING.
 *
 * A prune that deletes nothing looks exactly like a prune with nothing to do.
 * Row-level security makes that silent: a policy that does not match the caller
 * does not raise, it filters. Measured on this repository before any pruning
 * existed — as the table OWNER, with no app.tenant_id set, `SELECT count(*)` on
 * ProcessedMessage returned 0 of 29 real rows and a DELETE removed 0, exit code
 * 0, nothing logged. A maintenance job written the obvious way would have said
 * "nothing to prune" forever while the table grew without bound.
 *
 * So pruneOnce compares what it can SEE against what it can DELETE, in one
 * transaction, and throws when they disagree. SELECT and DELETE are evaluated
 * against different policy sets in Postgres, so a role can be perfectly able to
 * read a row and unable to remove it — and a control that only proved the rows
 * were visible would pass in precisely that case.
 *
 * This test provokes it for real by revoking DELETE from the relay role, which
 * is the closest thing to the production failure that can be arranged on
 * purpose. The grant is restored afterwards.
 *
 * Run: node scripts/verify-outbox-retention.mjs
 */

import pg from 'pg';
import { pruneOnce, PruneVisibilityError } from '../services/relay/dist/prune.js';

const RELAY_URL =
  process.env['RELAY_DATABASE_URL'] ??
  'postgresql://eventhub_event_relay:eventhub_event_relay@localhost:5432/eventhub_event';
const OWNER_URL =
  process.env['EVENT_DATABASE_URL'] ??
  'postgresql://eventhub_event_owner:eventhub_event_owner@localhost:5432/eventhub_event';
const APP_URL =
  process.env['EVENT_APP_DATABASE_URL'] ??
  'postgresql://eventhub_event_app:eventhub_event_app@localhost:5432/eventhub_event';

const TENANT = 'tenant_acme';
const run = `ret_${Date.now()}`;
const RETENTION_DAYS = 365;

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

const logger = { info: () => {}, warn: () => {}, error: () => {}, debug: () => {} };

/**
 * TWO CREDENTIALS, and the split is not incidental.
 *
 * The OWNER can run GRANT and REVOKE but cannot touch a row: OutboxMessage is
 * FORCE ROW LEVEL SECURITY and the only policies name eventhub_event_app and
 * eventhub_event_relay, so the owner has no policy at all and every insert
 * comes back 42501. The first version of this file seeded as the owner and got
 * exactly that.
 *
 * So fixtures go in on the APP role with app.tenant_id set, which is the
 * credential the service itself writes outbox rows with; the owner is used only
 * for the DDL that has no policy to satisfy.
 */
const owner = new pg.Client({ connectionString: OWNER_URL, application_name: 'verify-retention' });
const app = new pg.Client({ connectionString: APP_URL, application_name: 'verify-retention' });
owner.on('error', () => {});
app.on('error', () => {});

/** Every row read or written through the app role needs its tenant context. */
async function asApp(fn) {
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
 * Four rows, one per rule. Ages are set explicitly rather than waited for —
 * the retention window is a year.
 */
const ROWS = [
  { label: 'old, delivered', ageDays: 400, published: true, deadLettered: false, survives: false },
  { label: 'recent, delivered', ageDays: 10, published: true, deadLettered: false, survives: true },
  { label: 'old, NOT delivered', ageDays: 400, published: false, deadLettered: false, survives: true },
  { label: 'old, dead-lettered', ageDays: 400, published: false, deadLettered: true, survives: true },
];

async function seed() {
  for (const [i, r] of ROWS.entries()) {
    r.id = `${run}_${i}`;
    await asApp(() =>
      app.query(
      `INSERT INTO "OutboxMessage"
         ("id","tenantId","topic","eventType","schemaVersion","aggregateId","payload",
          "attempts","createdAt","publishedAt","deadLetteredAt")
       VALUES ($1,$2,'events.activity','event.updated',1,$1,'{}'::jsonb,0,
               now() - ($3::int * interval '1 day'),
               CASE WHEN $4 THEN now() - ($3::int * interval '1 day') ELSE NULL END,
               CASE WHEN $5 THEN now() - ($3::int * interval '1 day') ELSE NULL END)`,
      [r.id, TENANT, r.ageDays, r.published, r.deadLettered],
      ),
    );
  }
}

const survivors = async () => {
  const rows = await asApp(async () => {
    const r = await app.query(`SELECT id FROM "OutboxMessage" WHERE id LIKE $1`, [`${run}_%`]);
    return r.rows;
  });
  return new Set(rows.map((x) => x.id));
};

async function main() {
  await Promise.all([owner.connect(), app.connect()]);
  await seed();
  console.log(`  seeded ${ROWS.length} rows: one prunable, three that must survive`);

  const pool = new pg.Pool({ connectionString: RELAY_URL, max: 2 });
  pool.on('error', () => {});

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  the prune takes delivered rows past the window, and only those');
  // -------------------------------------------------------------------------
  const result = await pruneOnce(pool, logger, {
    olderThanDays: RETENTION_DAYS,
    batchSize: 500,
    maxBatches: 5,
  });

  const left = await survivors();

  for (const r of ROWS) {
    const alive = left.has(r.id);
    check(
      alive === r.survives,
      r.survives
        ? `${r.label} survived`
        : `${r.label} was pruned` + (alive ? ' — it is still there' : ''),
    );
  }

  /**
   * The positive control for the prune itself: it must have deleted something.
   * A prune that removed nothing would satisfy all three "survived" assertions
   * above and prove only that it is inert.
   */
  check(
    result.deleted >= 1,
    `it reported deleting ${result.deleted} row(s) — a prune that removes nothing would ` +
      'satisfy every "survived" assertion above',
  );

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  and a prune that cannot delete must not report success');
  // -------------------------------------------------------------------------
  const blocked = { id: `${run}_blocked`, ageDays: 400 };
  await asApp(() =>
    app.query(
      `INSERT INTO "OutboxMessage"
         ("id","tenantId","topic","eventType","schemaVersion","aggregateId","payload",
          "attempts","createdAt","publishedAt")
       VALUES ($1,$2,'events.activity','event.updated',1,$1,'{}'::jsonb,0,
               now() - ($3::int * interval '1 day'), now() - ($3::int * interval '1 day'))`,
      [blocked.id, TENANT, blocked.ageDays],
    ),
  );

  /**
   * TWO WAYS TO FAIL, and only one of them is quiet.
   *
   * A: the DELETE GRANT is missing. Postgres checks table privileges before row
   * security, so this raises 42501 outright. Loud, and the loop's own error
   * handling takes it — nothing here needs to catch it specially.
   *
   * B: the grant is there and the ROW-LEVEL POLICY does not cover DELETE. Then
   * nothing is raised: the command simply matches no rows. That is the silent
   * no-op — the job finds candidates, removes none, and without the comparison
   * inside pruneOnce it would log "nothing to prune" and exit 0, forever, while
   * the table grew.
   *
   * B is the one this design exists for, and it is reproduced by narrowing the
   * relay's policy to FOR SELECT for the length of one call. Both are asserted,
   * because "it errors" and "it lies" need different answers and only one of
   * them can be caught by application code.
   */
  console.log('    A: the DELETE grant is missing — Postgres raises');
  await owner.query('REVOKE DELETE ON "OutboxMessage" FROM eventhub_event_relay');
  let grantErr = null;
  try {
    await pruneOnce(pool, logger, { olderThanDays: RETENTION_DAYS, batchSize: 500, maxBatches: 5 });
  } catch (err) {
    grantErr = err;
  } finally {
    await owner.query('GRANT DELETE ON "OutboxMessage" TO eventhub_event_relay');
  }
  check(
    grantErr !== null && /permission denied/i.test(String(grantErr.message)),
    grantErr
      ? `it failed loudly: ${String(grantErr.message).slice(0, 60)}`
      : 'a missing DELETE grant did not raise at all',
  );

  console.log('    B: the grant is there and the policy does not cover DELETE — silent');
  /**
   * Narrowed for the length of one call and put straight back. The relay may be
   * running against this database; a policy that is briefly SELECT-only makes
   * its prune a no-op for that moment and touches nothing else, because the
   * drain only ever reads and updates rows this policy still covers.
   */
  await owner.query('DROP POLICY outbox_relay ON "OutboxMessage"');
  await owner.query(
    'CREATE POLICY outbox_relay ON "OutboxMessage" FOR SELECT TO eventhub_event_relay USING (true)',
  );

  let silentErr = null;
  try {
    await pruneOnce(pool, logger, { olderThanDays: RETENTION_DAYS, batchSize: 500, maxBatches: 5 });
  } catch (err) {
    silentErr = err;
  } finally {
    await owner.query('DROP POLICY outbox_relay ON "OutboxMessage"');
    await owner.query(
      'CREATE POLICY outbox_relay ON "OutboxMessage" TO eventhub_event_relay USING (true)',
    );
  }

  check(
    silentErr instanceof PruneVisibilityError,
    silentErr
      ? `it refused to call that success: ${silentErr.name}`
      : 'it returned normally — the prune saw rows, deleted none, and reported success. That is ' +
        'the failure this whole design exists to prevent',
  );

  const stillBlocked = await asApp(() =>
    app.query(`SELECT id FROM "OutboxMessage" WHERE id = $1`, [blocked.id]),
  );
  check(stillBlocked.rowCount === 1, 'and the row it could not delete is still there');

  // Clean up this run's fixtures, including the ones that were meant to survive.
  await asApp(() => app.query(`DELETE FROM "OutboxMessage" WHERE id LIKE $1`, [`${run}_%`]));

  const restored = await owner.query(
    `SELECT count(*)::int AS n FROM information_schema.role_table_grants
      WHERE grantee = 'eventhub_event_relay' AND table_name = 'OutboxMessage'
        AND privilege_type = 'DELETE'`,
  );
  check(restored.rows[0].n === 1, 'the DELETE grant was put back');

  const policy = await owner.query(
    `SELECT pg_get_expr(polqual, polrelid) AS q, polcmd::text AS cmd FROM pg_policy
      WHERE polrelid = '"OutboxMessage"'::regclass AND polname = 'outbox_relay'`,
  );
  check(
    policy.rows[0]?.cmd === '*' && policy.rows[0]?.q === 'true',
    `and the relay policy is back to FOR ALL USING(true) (cmd=${policy.rows[0]?.cmd})`,
  );

  await pool.end();
  await Promise.all([owner.end(), app.end()]);

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('delivered rows age out, evidence is kept, and a prune that deletes nothing shouts');
}

main().catch(async (err) => {
  console.error(err);
  process.exitCode = 1;
  // Close the connections, or a failure before the happy-path cleanup leaves
  // node holding two open sockets and the run hangs instead of reporting.
  await owner.end().catch(() => {});
  await app.end().catch(() => {});
});
