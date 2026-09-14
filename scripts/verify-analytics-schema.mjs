/**
 * Asserts the four properties Analytics' storage rests on, against a real
 * ClickHouse, before any code writes to it.
 *
 *   LEAST PRIVILEGE   the service's user can SELECT and INSERT and is refused
 *                     CREATE, DROP, TRUNCATE, ALTER DELETE and DELETE
 *   MIGRATIONS        apply once, re-run as a no-op, and refuse a migration whose
 *                     recorded checksum no longer matches its file
 *   ONE RETRY, ONE COUNT  a batch inserted twice with the same dedup token lands
 *                     once in the raw table AND once in every rollup
 *   PRUNING           the sparse index skips other tenants' granules — and skips
 *                     nothing when a query leaves the tenant out
 *
 * THE DEDUP CASE HAS TWO CONTROLS, because it failed silently once already. A
 * different token must double both tables (so "counted once" is dedup, not a
 * second insert that never arrived). And the same token WITHOUT
 * deduplicate_blocks_in_dependent_materialized_views must leave the raw table
 * deduplicated and the rollup doubled — the behaviour measured on ClickHouse 25.8
 * that migration 002 warns about. If a future ClickHouse changes it, this fails
 * and the warning gets rewritten rather than quietly going stale.
 *
 * A SCRATCH DATABASE PER RUN, created and dropped here, so migrations, fixtures
 * and the checksum tampering never touch `analytics`. The one exception is the
 * privilege check, which must test the real grants: every statement that should
 * be refused targets a victim table this run creates, so a grant that is wrong
 * can only ever destroy the victim.
 *
 * WATCHED FAIL, four guards broken in one run, seven assertions red:
 *
 *   analytics_app granted ALTER DELETE       refused-ALTER-DELETE red, and the
 *                                            victim's row really was deleted
 *   rollup dedup window removed              the retried batch doubled in the
 *                                            rollup: 14 tickets, not 7
 *   event_id ahead of tenant_id in the key   a tenant-only query read 50 of 50
 *                                            granules, event_id alone read 8
 *   the runner's checksum check disabled     the tampered migration ran, exit 0
 *
 * Getting here found three faults in this file first: a rollup-row count that a
 * background merge raced (merges are now stopped around it), an EXPLAIN parser
 * written for a format 25.8 does not print, and a pruning query that filtered on
 * tenant and event together — which a key led by event_id would also have pruned.
 *
 * Run (after pnpm build, with ClickHouse up): node scripts/verify-analytics-schema.mjs
 */

import { randomUUID } from 'node:crypto';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  ADMIN,
  APP,
  CH_URL,
  MIGRATOR,
  ch,
  createScratchDatabase,
  denied,
  dropScratchDatabase,
  json,
  migrate,
  toChTime,
} from './lib/clickhouse.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

const run = randomUUID().replace(/-/g, '').slice(0, 8);
const DB = `analytics_verify_${run}`;
const VICTIM = `analytics.verify_victim_${run}`;

function row(tenant, activity, over = {}) {
  return {
    tenant_id: tenant,
    activity,
    occurred_at: toChTime(),
    message_id: randomUUID(),
    line: 0,
    event_id: 'evt_1',
    ticket_type_id: 'tt_ga',
    user_id: 'u1',
    quantity: 0,
    amount_cents: 0,
    currency: '',
    source_topic: 'verify',
    kafka_partition: 0,
    kafka_offset: 0,
    props: {},
    ...over,
  };
}

async function main() {
  const ping = await fetch(`${CH_URL}/ping`).then((r) => r.text()).catch(() => '');
  check(ping.trim() === 'Ok.', `ClickHouse answers at ${CH_URL}`);
  if (failures > 0) return;

  try {
    // -----------------------------------------------------------------------
    console.log('');
    console.log("  least privilege: the service's user reads and writes, and nothing else");
    // -----------------------------------------------------------------------
    const created = await ch(MIGRATOR, `CREATE TABLE ${VICTIM} (x UInt8) ENGINE = MergeTree ORDER BY x`);
    check(created.ok, `CONTROL: the migrator can create a table (${VICTIM})`);
    const inserted = await ch(APP, `INSERT INTO ${VICTIM} VALUES (1)`);
    const selected = await ch(APP, `SELECT count() FROM ${VICTIM}`);
    check(inserted.ok && selected.ok && selected.text.trim() === '1', 'CONTROL: analytics_app can INSERT and SELECT');

    for (const [label, sql] of [
      ['CREATE TABLE', `CREATE TABLE analytics.verify_nope_${run} (x UInt8) ENGINE = MergeTree ORDER BY x`],
      ['DROP TABLE', `DROP TABLE ${VICTIM}`],
      ['TRUNCATE', `TRUNCATE TABLE ${VICTIM}`],
      ['ALTER ... DELETE', `ALTER TABLE ${VICTIM} DELETE WHERE 1`],
      ['lightweight DELETE', `DELETE FROM ${VICTIM} WHERE 1`],
    ]) {
      const r = await ch(APP, sql);
      check(denied(r), `analytics_app is refused ${label}${denied(r) ? '' : `: ${r.text.slice(0, 160)}`}`);
    }
    const survived = await ch(ADMIN, `SELECT count() FROM ${VICTIM}`);
    check(survived.ok && survived.text.trim() === '1', 'and the victim table is intact, with its row');

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  migrations apply once, re-run as a no-op, into a database given as a parameter');
    // -----------------------------------------------------------------------
    await createScratchDatabase(DB);
    const first = migrate(ROOT, DB);
    check(first.code === 0 && first.applied === 2, `first run into ${DB}: exit ${first.code}, applied ${first.applied}`);
    const second = migrate(ROOT, DB);
    check(second.code === 0 && second.applied === 0, `second run: exit ${second.code}, applied ${second.applied}`);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  a retried batch lands once, in the raw table and in the rollup');
    // -----------------------------------------------------------------------
    const T1 = `t_dedup_${run}`;
    const T2 = `t_nodedup_${run}`;
    const batch = (tenant) => [
      row(tenant, 'ticket_purchased', { user_id: 'u1', quantity: 2, amount_cents: 9000, currency: 'USD', kafka_offset: 1 }),
      row(tenant, 'ticket_purchased', { user_id: 'u2', quantity: 1, amount_cents: 4500, currency: 'USD', kafka_offset: 2 }),
      row(tenant, 'ticket_purchased', { user_id: 'u3', quantity: 4, amount_cents: 18000, currency: 'USD', kafka_offset: 3 }),
    ];
    const totals = async (tenant) => {
      const [raw] = await json(ADMIN, `SELECT count() AS n FROM activity_events WHERE tenant_id = '${tenant}'`, DB);
      const [roll] = await json(ADMIN, `SELECT sum(tickets_sold) AS t, sum(gross_cents) AS g FROM ticket_funnel_daily WHERE tenant_id = '${tenant}'`, DB);
      return { raw: Number(raw.n), tickets: Number(roll.t), gross: Number(roll.g) };
    };
    const insert = (rows, settings) => ch(APP, 'INSERT INTO activity_events', { database: DB, rows, settings });

    const withDedup = { insert_deduplication_token: `topic-0-1-3-${run}`, deduplicate_blocks_in_dependent_materialized_views: 1 };
    const b1 = batch(T1);
    const i1 = await insert(b1, withDedup);
    const i2 = await insert(b1, withDedup);
    check(i1.ok && i2.ok, 'the same block inserted twice with one token, both accepted');
    const once = await totals(T1);
    check(
      once.raw === 3 && once.tickets === 7 && once.gross === 31500,
      `counted once: raw ${once.raw} rows, rollup ${once.tickets} tickets / ${once.gross} cents (want 3, 7, 31500)`,
    );

    await insert(b1, { ...withDedup, insert_deduplication_token: `topic-0-4-6-${run}` });
    const twice = await totals(T1);
    check(
      twice.raw === 6 && twice.tickets === 14 && twice.gross === 63000,
      `CONTROL: a different token doubles both — raw ${twice.raw}, rollup ${twice.tickets} / ${twice.gross} — so "once" above was dedup`,
    );

    const b2 = batch(T2);
    const noDependent = { insert_deduplication_token: `topic-1-1-3-${run}` };
    await insert(b2, noDependent);
    await insert(b2, noDependent);
    const leaky = await totals(T2);
    check(
      leaky.raw === 3 && leaky.tickets === 14 && leaky.gross === 63000,
      `still true of this ClickHouse: without deduplicate_blocks_in_dependent_materialized_views the raw table dedups (${leaky.raw} rows) and the rollup doubles (${leaky.tickets} / ${leaky.gross})`,
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  the rollups agree with the raw table they are derived from');
    // -----------------------------------------------------------------------
    const T3 = `t_rollup_${run}`;
    const mixed = [
      row(T3, 'ticket_reserved', { user_id: 'a', quantity: 2 }),
      row(T3, 'ticket_reserved', { user_id: 'b', quantity: 1, ticket_type_id: 'tt_vip' }),
      row(T3, 'reservation_released', { user_id: 'b', quantity: 1, ticket_type_id: 'tt_vip' }),
      row(T3, 'reservation_expired', { user_id: 'c', quantity: 3 }),
      row(T3, 'ticket_purchased', { user_id: 'a', quantity: 2, amount_cents: 9000, currency: 'USD' }),
      row(T3, 'ticket_purchased', { user_id: 'd', quantity: 1, amount_cents: 7000, currency: 'EUR', ticket_type_id: 'tt_vip' }),
      row(T3, 'question_asked', { user_id: 'a' }),
      row(T3, 'question_upvoted', { user_id: 'b' }),
      row(T3, 'question_upvoted', { user_id: 'c' }),
    ];
    /**
     * Three separate inserts, so the rollup holds several unmerged rows per key —
     * WITH MERGES STOPPED. The first version left them running, and a background
     * merge collapsed the duplicates before the count, so "more rows than keys"
     * failed on a correct table. Merges are eventual, which is the whole point
     * being demonstrated; a check that races one proves nothing either way.
     */
    await ch(ADMIN, 'SYSTEM STOP MERGES ticket_funnel_daily', { database: DB });
    for (let i = 0; i < mixed.length; i += 3) {
      await insert(mixed.slice(i, i + 3), { insert_deduplication_token: `mixed-${i}-${run}`, deduplicate_blocks_in_dependent_materialized_views: 1 });
    }
    const [fromRollup] = await json(
      ADMIN,
      `SELECT uniqMerge(reservers) AS reservers, uniqMerge(purchasers) AS purchasers, sum(seats_reserved) AS reserved,
              sum(seats_released) AS released, sum(seats_expired) AS expired, sum(tickets_sold) AS sold
         FROM ticket_funnel_daily WHERE tenant_id = '${T3}'`,
      DB,
    );
    const [fromRaw] = await json(
      ADMIN,
      `SELECT uniqIf(user_id, activity = 'ticket_reserved') AS reservers, uniqIf(user_id, activity = 'ticket_purchased') AS purchasers,
              sumIf(quantity, activity = 'ticket_reserved') AS reserved, sumIf(quantity, activity = 'reservation_released') AS released,
              sumIf(quantity, activity = 'reservation_expired') AS expired, sumIf(quantity, activity = 'ticket_purchased') AS sold
         FROM activity_events WHERE tenant_id = '${T3}'`,
      DB,
    );
    check(
      JSON.stringify(Object.values(fromRollup).map(Number)) === JSON.stringify(Object.values(fromRaw).map(Number)),
      `funnel rollup equals raw: ${JSON.stringify(fromRollup)}`,
    );
    const byCurrency = await json(ADMIN, `SELECT currency, sum(gross_cents) AS g FROM ticket_funnel_daily WHERE tenant_id = '${T3}' AND gross_cents > 0 GROUP BY currency ORDER BY currency`, DB);
    check(
      JSON.stringify(byCurrency.map((r) => [r.currency, Number(r.g)])) === JSON.stringify([['EUR', 7000], ['USD', 9000]]),
      `money stays per currency: ${JSON.stringify(byCurrency)}`,
    );
    const [eng] = await json(ADMIN, `SELECT sum(questions_asked) AS q, sum(upvotes) AS u, uniqMerge(active_users) AS users FROM engagement_hourly WHERE tenant_id = '${T3}'`, DB);
    check(Number(eng.q) === 1 && Number(eng.u) === 2 && Number(eng.users) === 3, `engagement rollup: ${JSON.stringify(eng)} (want 1 question, 2 upvotes, 3 users)`);
    const [physical] = await json(ADMIN, `SELECT count() AS rows, uniqExact(tuple(event_id, day, ticket_type_id, currency)) AS keys FROM ticket_funnel_daily WHERE tenant_id = '${T3}'`, DB);
    check(
      Number(physical.rows) > Number(physical.keys),
      `the rollup holds ${physical.rows} physical rows for ${physical.keys} keys — which is why every read must GROUP BY and merge`,
    );
    await ch(ADMIN, 'SYSTEM START MERGES ticket_funnel_daily', { database: DB });

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  the sparse index prunes by tenant, and only when the tenant is in the query');
    // -----------------------------------------------------------------------
    const volume = await ch(
      ADMIN,
      `INSERT INTO activity_events (tenant_id, activity, occurred_at, message_id, event_id, source_topic, kafka_partition, kafka_offset, props)
       SELECT concat('tv_', toString(number % 40)), 'question_asked', now64(3), toString(number), concat('e', toString(number % 7)), 'verify', 0, number, map()
       FROM numbers(400000)`,
      { database: DB },
    );
    check(volume.ok, 'CONTROL: 400000 rows across 40 tenants inserted');
    await ch(ADMIN, 'OPTIMIZE TABLE activity_events FINAL', { database: DB });

    const granules = async (where) => {
      const r = await ch(ADMIN, `EXPLAIN indexes = 1 SELECT count() FROM activity_events WHERE ${where}`, { database: DB });
      const lines = r.text.split('\n');
      // 25.8 prints the index step as a bare "PrimaryKey" line, then Keys,
      // Condition, Parts and Granules beneath it. The first version looked for
      // "Type: PrimaryKey", found nothing, and reported undefined for every count.
      const at = lines.findIndex((l) => /^\s*PrimaryKey\s*$/.test(l));
      const g = at === -1 ? null : lines.slice(at).join('\n').match(/Granules: (\d+)\/(\d+)/);
      return g ? { selected: Number(g[1]), total: Number(g[2]) } : null;
    };
    // The tenant ALONE. Filtering on tenant and event together would still prune
    // if event_id led the sort key, and prove nothing about tenant_id leading it.
    const withTenant = await granules(`tenant_id = 'tv_7'`);
    const withoutTenant = await granules(`event_id = 'e3'`);
    check(Boolean(withTenant) && withTenant.total >= 10, `CONTROL: EXPLAIN reports a primary-key step over ${withTenant?.total} granules`);
    check(
      Boolean(withTenant) && withTenant.selected < withTenant.total / 4,
      `with the tenant: ${withTenant?.selected} of ${withTenant?.total} granules read`,
    );
    check(
      Boolean(withoutTenant) && withoutTenant.selected === withoutTenant.total,
      `without it, event_id alone prunes nothing: ${withoutTenant?.selected} of ${withoutTenant?.total} — the prefix rule`,
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  and a migration edited after it was applied is refused');
    // -----------------------------------------------------------------------
    const tamper = await ch(
      ADMIN,
      `ALTER TABLE schema_migrations UPDATE checksum = 'tampered' WHERE version = '002_rollups.sql' SETTINGS mutations_sync = 2`,
      { database: DB },
    );
    check(tamper.ok, 'CONTROL: the recorded checksum for 002 was changed');
    const third = migrate(ROOT, DB);
    check(
      third.code !== 0 && /was changed after it was applied/.test(third.text),
      `the runner refuses: exit ${third.code}${/was changed after it was applied/.test(third.text) ? ', "002_rollups.sql was changed after it was applied"' : ''}`,
    );
  } finally {
    const gone = await dropScratchDatabase(DB);
    await ch(ADMIN, `DROP TABLE IF EXISTS ${VICTIM} SYNC`);
    await ch(ADMIN, `DROP TABLE IF EXISTS analytics.verify_nope_${run} SYNC`);
    console.log(`\n    cleanup: scratch database ${gone ? 'dropped' : 'STILL PRESENT'}, victim table dropped`);
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('the service cannot change the schema, a retry counts once everywhere, and the index prunes by tenant');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
