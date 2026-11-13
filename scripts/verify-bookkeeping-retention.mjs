/**
 * Asserts that the two tables which grow forever are pruned, that a prune which
 * deletes nothing cannot report success, and that the credential doing it
 * cannot read more than it needs.
 *
 * NOTHING PRUNED ProcessedMessage OR IdempotencyKey, AND NO ROLE COULD. Both
 * carry a tenant_isolation policy with no TO clause, so it applies to PUBLIC,
 * and FORCE ROW LEVEL SECURITY binds even the table owner. Measured before this
 * existed — as eventhub_ticketing_owner with no app.tenant_id set,
 * `SELECT count(*)` on ProcessedMessage returned 0 of 29 real rows and a DELETE
 * removed 0. Exit code 0, no error, nothing logged.
 *
 * A retention sweep has no tenant: "what is older than a year" is a question
 * with no tenant in it, and there is no Tenant table here to loop over. So the
 * sweeper gets a role-targeted policy, the way its other cross-tenant work
 * does, and its reach is bounded by GRANTS instead.
 *
 * FOUR THINGS, and the last two are the ones worth writing:
 *
 *   it prunes      rows past the window go, recent ones stay
 *   it is bounded  the credential can count and delete but cannot read
 *                  IdempotencyKey.responseBody — a cached customer response —
 *                  or ProcessedMessage.messageId
 *   loud failure   a missing DELETE grant raises, because Postgres checks table
 *                  privileges before row security
 *   quiet failure  a policy that does not cover DELETE raises NOTHING and
 *                  matches no rows. That is the silent no-op, and it is the
 *                  case the SEEN-versus-DELETED comparison exists for
 *
 * Run: node scripts/verify-bookkeeping-retention.mjs
 */

import pg from 'pg';
import { pruneOnce, PruneVisibilityError } from '../services/ticketing/dist/sweeper/prune.js';

const SWEEPER_URL =
  process.env['SWEEPER_DATABASE_URL'] ??
  'postgresql://eventhub_ticketing_sweeper:eventhub_ticketing_sweeper@localhost:5432/eventhub_ticketing';
const OWNER_URL =
  process.env['TICKETING_DATABASE_URL'] ??
  'postgresql://eventhub_ticketing_owner:eventhub_ticketing_owner@localhost:5432/eventhub_ticketing';
const APP_URL =
  process.env['TICKETING_APP_DATABASE_URL'] ??
  'postgresql://eventhub_ticketing_app:eventhub_ticketing_app@localhost:5432/eventhub_ticketing';

const TENANT = 'tenant_acme';
const run = `bk_${Date.now()}`;
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
 * The owner runs GRANT/REVOKE and CREATE/DROP POLICY, which need no policy to
 * satisfy. It cannot touch a ROW — these tables are FORCE RLS and its only
 * policy is tenant-scoped — so fixtures go in on the app role with tenant
 * context, which is the credential the service writes them with.
 */
const owner = new pg.Client({ connectionString: OWNER_URL, application_name: 'verify-bookkeeping' });
const app = new pg.Client({ connectionString: APP_URL, application_name: 'verify-bookkeeping' });
const sweeper = new pg.Client({ connectionString: SWEEPER_URL, application_name: 'verify-bookkeeping' });
for (const c of [owner, app, sweeper]) c.on('error', () => {});

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

const FIXTURES = [
  { table: 'ProcessedMessage', label: 'old dedup record', ageDays: 400, survives: false },
  { table: 'ProcessedMessage', label: 'recent dedup record', ageDays: 5, survives: true },
  { table: 'IdempotencyKey', label: 'old key', ageDays: 400, survives: false },
  { table: 'IdempotencyKey', label: 'recent key', ageDays: 5, survives: true },
];

async function seed() {
  for (const [i, f] of FIXTURES.entries()) {
    f.id = `${run}_${i}`;
    if (f.table === 'ProcessedMessage') {
      await asApp(() =>
        app.query(
          `INSERT INTO "ProcessedMessage" (id,"tenantId","consumerGroup","messageId","processedAt")
           VALUES ($1,$2,'verify-bookkeeping',$1, now() - ($3::int * interval '1 day'))`,
          [f.id, TENANT, f.ageDays],
        ),
      );
    } else {
      await asApp(() =>
        app.query(
          `INSERT INTO "IdempotencyKey"
             (id,"tenantId","userId",key,endpoint,"requestHash",state,"responseStatus",
              "responseBody","createdAt","updatedAt")
           VALUES ($1,$2,'u_probe',$1,'POST /reservations','hash','completed',201,
                   '{"secret":"a cached customer response"}'::jsonb,
                   now() - ($3::int * interval '1 day'), now())`,
          [f.id, TENANT, f.ageDays],
        ),
      );
    }
  }
}

const alive = async (table, id) => {
  const r = await asApp(() =>
    app.query(`SELECT 1 FROM "${table}" WHERE id = $1`, [id]),
  );
  return r.rowCount === 1;
};

async function main() {
  await Promise.all([owner.connect(), app.connect(), sweeper.connect()]);
  await seed();
  console.log(`  seeded ${FIXTURES.length} rows across both tables: two stale, two recent`);

  const pool = new pg.Pool({ connectionString: SWEEPER_URL, max: 2 });
  pool.on('error', () => {});

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  the prune takes what is past the window, and only that');
  // -------------------------------------------------------------------------
  const result = await pruneOnce(pool, logger, {
    olderThanDays: RETENTION_DAYS,
    batchSize: 500,
    maxBatches: 5,
  });

  for (const f of FIXTURES) {
    const still = await alive(f.table, f.id);
    check(
      still === f.survives,
      f.survives ? `${f.label} survived` : `${f.label} was pruned${still ? ' — it is still there' : ''}`,
    );
  }

  const total = Object.values(result.deleted).reduce((a, b) => a + b, 0);
  check(
    total >= 2,
    `it reported deleting ${JSON.stringify(result.deleted)} — a prune that removes nothing ` +
      'would satisfy every "survived" assertion above',
  );

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  the pruning credential must not be able to read the payloads');
  // -------------------------------------------------------------------------
  /**
   * The column-level grant. IdempotencyKey.responseBody is a cached customer
   * response and this role can now see every tenant's rows, so being unable to
   * read that column is what keeps the widening honest.
   */
  let readErr = null;
  try {
    await sweeper.query(`SELECT "responseBody" FROM "IdempotencyKey" LIMIT 1`);
  } catch (err) {
    readErr = err;
  }
  check(
    readErr !== null && readErr.code === '42501',
    readErr
      ? `responseBody is refused (${readErr.code})`
      : 'the sweeper can read cached customer responses across every tenant',
  );

  let countOk = null;
  try {
    const r = await sweeper.query(`SELECT count(*)::int AS n FROM "IdempotencyKey"`);
    countOk = r.rows[0].n;
  } catch {
    countOk = null;
  }
  check(
    typeof countOk === 'number',
    countOk === null
      ? 'but it also cannot COUNT them, so it cannot do its job either'
      : `while still counting them across tenants (${countOk}) — which is what it needs`,
  );

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  and a prune that cannot delete must not report success');
  // -------------------------------------------------------------------------
  const blocked = `${run}_blocked`;
  await asApp(() =>
    app.query(
      `INSERT INTO "ProcessedMessage" (id,"tenantId","consumerGroup","messageId","processedAt")
       VALUES ($1,$2,'verify-bookkeeping',$1, now() - interval '400 days')`,
      [blocked, TENANT],
    ),
  );

  console.log('    A: the DELETE grant is missing — Postgres raises');
  await owner.query('REVOKE DELETE ON "ProcessedMessage" FROM eventhub_ticketing_sweeper');
  let grantErr = null;
  try {
    await pruneOnce(pool, logger, { olderThanDays: RETENTION_DAYS, batchSize: 500, maxBatches: 5 });
  } catch (err) {
    grantErr = err;
  } finally {
    await owner.query('GRANT DELETE ON "ProcessedMessage" TO eventhub_ticketing_sweeper');
  }
  check(
    grantErr !== null && /permission denied/i.test(String(grantErr.message)),
    grantErr ? `it failed loudly: ${String(grantErr.message).slice(0, 58)}` : 'nothing raised',
  );

  console.log('    B: the grant is there and the policy does not cover DELETE — silent');
  await owner.query('DROP POLICY processed_message_pruner ON "ProcessedMessage"');
  await owner.query(
    'CREATE POLICY processed_message_pruner ON "ProcessedMessage" FOR SELECT ' +
      'TO eventhub_ticketing_sweeper USING (true)',
  );

  let silentErr = null;
  try {
    await pruneOnce(pool, logger, { olderThanDays: RETENTION_DAYS, batchSize: 500, maxBatches: 5 });
  } catch (err) {
    silentErr = err;
  } finally {
    await owner.query('DROP POLICY processed_message_pruner ON "ProcessedMessage"');
    await owner.query(
      'CREATE POLICY processed_message_pruner ON "ProcessedMessage" ' +
        'TO eventhub_ticketing_sweeper USING (true)',
    );
  }

  check(
    silentErr instanceof PruneVisibilityError,
    silentErr
      ? `it refused to call that success: ${silentErr.name}`
      : 'it returned normally — the prune saw rows, deleted none, and reported success. That is ' +
        'the failure this design exists to prevent',
  );

  check(await alive('ProcessedMessage', blocked), 'and the row it could not delete is still there');

  await asApp(() => app.query(`DELETE FROM "ProcessedMessage" WHERE id LIKE $1`, [`${run}_%`]));
  await asApp(() => app.query(`DELETE FROM "IdempotencyKey" WHERE id LIKE $1`, [`${run}_%`]));

  const policy = await owner.query(
    `SELECT polcmd::text AS cmd FROM pg_policy
      WHERE polrelid = '"ProcessedMessage"'::regclass AND polname = 'processed_message_pruner'`,
  );
  check(policy.rows[0]?.cmd === '*', `the pruner policy is back to FOR ALL (cmd=${policy.rows[0]?.cmd})`);

  await pool.end();
  await Promise.all([owner.end(), app.end(), sweeper.end()]);

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('the bookkeeping tables age out, on a credential that can count and delete but not read');
}

main().catch(async (err) => {
  console.error(err);
  process.exitCode = 1;
  for (const c of [owner, app, sweeper]) await c.end().catch(() => {});
});
