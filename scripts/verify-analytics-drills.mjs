/**
 * Chapter 7's failure drills, run rather than described: what the analytics
 * pipeline does when it is pushed to where it breaks, and what an operator sees.
 *
 *   TOO MANY PARTS   one insert per message makes one data part per message; one
 *                    insert per batch makes one. /metrics reports each table's
 *                    worst partition beside the server's own thresholds, so the
 *                    drift is visible before inserts are delayed or refused
 *   REBUILD          a rollup deliberately corrupted is rebuilt from the raw table
 *                    by `pnpm run rebuild-rollups`, and comes back equal to the raw
 *                    table rather than to the corruption
 *   CLICKHOUSE DOWN  the consumer commits nothing, lag in offsets climbs by exactly
 *                    what was produced, and /ready says so; with ClickHouse back,
 *                    the same group drains to zero with every message landed once
 *
 * "Down" is a consumer pointed at a port nothing listens on, not a stopped
 * container. The container is shared with every other service and verifier on this
 * machine and in CI, and stopping it would test all of them at once.
 *
 * A SCRATCH DATABASE PER RUN, as the other analytics verifiers use. The one thing
 * this run cannot keep to itself is Kafka: what it produces, a running compose
 * ingest copies into the real analytics database, and the run removes those copies.
 *
 * WATCHED FAIL: one run with four guards broken, four red, and each red names
 * exactly one break.
 *
 *   the worst partition reported as the total   52 reported where system.parts
 *                                               says 51
 *   the delay and refuse thresholds swapped     delayed at 3000, refused at 1000
 *   the rebuild's TRUNCATE skipped              1017 tickets where the raw table
 *                                               has 9: the corruption kept, plus a
 *                                               second copy of everything
 *   lag ignoring the committed offset           it never drained, stuck at 76
 *
 * The first of those needed the fixture changed before it could fail. With every
 * row in one month, a table's worst partition and its total are the same number,
 * so a metric reporting the total could not have been told apart. The second
 * partition was added for that, with a control that the two numbers differ.
 *
 * Run (after pnpm build and topics, with ClickHouse and Kafka up):
 *   node scripts/verify-analytics-drills.mjs
 */

import { spawn, spawnSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { Kafka, logLevel } from 'kafkajs';
import { ADMIN, APP, CH_URL, MIGRATOR, ch, createScratchDatabase, dropScratchDatabase, json, migrate } from './lib/clickhouse.mjs';
import { serviceEnv } from './lib/service-env.mjs';
import { cleanRealAnalytics } from './lib/real-analytics.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');

process.env['KAFKA_BROKERS'] ??= '127.0.0.1:9092';
process.env['SCHEMA_REGISTRY_URL'] ??= 'http://127.0.0.1:8081';
const BROKERS = process.env['KAFKA_BROKERS'].split(',').map((b) => b.trim());
const PORT = 3921;

const mod = (rel) => import(pathToFileURL(join(ROOT, 'services', 'analytics', 'dist', rel)).href);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

async function waitFor(fn, timeoutMs = 20_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await fn();
    if (value) return value;
    await sleep(250);
  }
  return null;
}

const run = randomUUID().replace(/-/g, '').slice(0, 8);
const DB = `analytics_drills_${run}`;
const GROUP = `verify_analytics_drills_${run}`;
/** The only tenant this run puts on the shared topics. */
const TD = `t_drill_down_${run}`;
const AT = new Date(Date.now() - 60_000).toISOString();

const upvote = (tenantId, eventId, n) => ({
  kind: 'question.upvoted',
  tenantId,
  eventId,
  userId: `u_${run}_${n}`,
  questionId: `q_${run}`,
  occurredAt: AT,
});

const purchase = (tenantId, n, over = {}) => ({
  messageId: `drill_pur_${run}_${tenantId}_${n}`,
  occurredAt: AT,
  paymentId: `pay_${run}_${n}`,
  reservationId: `res_${run}_${n}`,
  tenantId,
  eventId: `evt_drill_${run}`,
  userId: `buyer_${n}`,
  amountCents: 7000,
  currency: 'USD',
  items: [
    { ticketTypeId: 'tt_ga', quantity: 2, unitPriceCents: 2000 },
    { ticketTypeId: 'tt_vip', quantity: 1, unitPriceCents: 3000 },
  ],
  providerRef: null,
  ...over,
});

async function main() {
  const ping = await fetch(`${CH_URL}/ping`).then((r) => r.text()).catch(() => '');
  check(ping.trim() === 'Ok.', `ClickHouse answers at ${CH_URL}`);
  if (failures > 0) return;

  const { createClickHouse } = await mod('clickhouse.js');
  const { writeActivity } = await mod('ingest/writer.js');
  const { toRows, INGEST_TOPICS } = await mod('ingest/mappers.js');
  const { partCounts } = await mod('ingest/parts.js');
  const TOPICS = Object.keys(INGEST_TOPICS);

  const kafka = new Kafka({ clientId: `verify-analytics-drills-${run}`, brokers: BROKERS, logLevel: logLevel.NOTHING });
  const producer = kafka.producer();
  const admin = kafka.admin();
  const produced = [];
  let app = null;
  let child = null;

  const stopConsumer = async () => {
    if (!child) return;
    const c = child;
    child = null;
    c.kill('SIGTERM');
    await new Promise((r) => (c.exitCode !== null ? r() : c.once('exit', r)));
  };

  try {
    await createScratchDatabase(DB);
    const migrated = migrate(ROOT, DB);
    check(migrated.code === 0 && migrated.applied > 0, `CONTROL: ${DB} migrated by the real runner: exit ${migrated.code}, applied ${migrated.applied}`);
    if (failures > 0) return;
    app = createClickHouse({ url: CH_URL, database: DB, username: APP.user, password: APP.password });

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  too many parts: a part per insert, and /metrics shows the worst partition beside the limits');
    // -----------------------------------------------------------------------
    // Merges stopped for the count, or a background merge would shrink it mid-drill
    // and make "one part per insert" untrue for reasons that have nothing to do
    // with how the rows were inserted.
    await ch(ADMIN, 'SYSTEM STOP MERGES activity_events', { database: DB });
    const activeParts = async () =>
      Number((await json(ADMIN, `SELECT count() AS n FROM system.parts WHERE database = '${DB}' AND table = 'activity_events' AND active`, DB))[0].n);
    const liveRows = (tenant, from, to) =>
      Array.from({ length: to - from + 1 }, (_, i) =>
        toRows({ topic: 'live.interactions', partition: 0, offset: String(from + i) }, upvote(tenant, `evt_parts_${run}`, from + i))[0],
      );

    const TP = `t_drill_parts_${run}`;
    const beforeSingles = await activeParts();
    for (const row of liveRows(TP, 0, 49)) await writeActivity(app, [row]);
    const singles = (await activeParts()) - beforeSingles;
    check(singles === 50, `one insert per message: 50 messages made ${singles} parts`);

    const beforeBatch = await activeParts();
    await writeActivity(app, liveRows(TP, 100, 149));
    const batched = (await activeParts()) - beforeBatch;
    check(batched === 1, `one insert per batch, as the consumer writes: 50 messages made ${batched} part`);

    // A second partition, so a table's worst partition and its total are different
    // numbers. With every row in one month they are the same, and a metric that
    // reported the whole table would pass for one that found the worst partition.
    const earlierMonth = new Date(Date.now() - 40 * 86_400_000).toISOString();
    await writeActivity(app, [
      toRows({ topic: 'live.interactions', partition: 0, offset: '999' }, { ...upvote(TP, `evt_parts_${run}`, 999), occurredAt: earlierMonth })[0],
    ]);

    const reported = await partCounts(app, DB);
    const eventsParts = reported.tables.find((t) => t.table === 'activity_events');
    const [{ worst, partitions, total }] = await json(
      ADMIN,
      `SELECT max(c) AS worst, count() AS partitions, sum(c) AS total FROM (SELECT partition, count() AS c FROM system.parts
         WHERE database = '${DB}' AND table = 'activity_events' AND active GROUP BY partition)`,
      DB,
    );
    check(
      Number(partitions) >= 2 && Number(total) > Number(worst),
      `CONTROL: activity_events spans ${partitions} partitions, so its worst partition (${worst}) and its total (${total}) differ`,
    );
    check(
      eventsParts?.worstPartitionParts === Number(worst) && eventsParts.activeParts === Number(total),
      `as analytics_app, the parts metric reports activity_events' worst partition at ${eventsParts?.worstPartitionParts} and ${eventsParts?.activeParts} in all (system.parts says ${worst} and ${total})`,
    );
    const limits = Object.fromEntries(
      (await json(ADMIN, "SELECT name, value FROM system.merge_tree_settings WHERE name IN ('parts_to_delay_insert', 'parts_to_throw_insert')", DB)).map(
        (r) => [r.name, Number(r.value)],
      ),
    );
    check(
      reported.delayInsertAt === limits['parts_to_delay_insert'] && reported.throwInsertAt === limits['parts_to_throw_insert'],
      `beside the server's own limits: delayed at ${reported.delayInsertAt}, refused at ${reported.throwInsertAt} (the server says ${limits['parts_to_delay_insert']} and ${limits['parts_to_throw_insert']})`,
    );
    await ch(ADMIN, 'SYSTEM START MERGES activity_events', { database: DB });

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  a corrupted rollup is rebuilt from the raw table');
    // -----------------------------------------------------------------------
    const TR = `t_drill_rebuild_${run}`;
    const purchaseRows = [0, 1, 2].flatMap((n) => toRows({ topic: 'tickets.purchased', partition: 0, offset: String(n) }, purchase(TR, n)));
    const questionRows = [0, 1].map((n) => toRows({ topic: 'live.interactions', partition: 0, offset: String(n) }, { ...upvote(TR, `evt_drill_${run}`, n), kind: 'question.created', questionId: `q_${run}_${n}` })[0]);
    await writeActivity(app, [...purchaseRows, ...questionRows]);

    const rollupTotals = async () => {
      const [f] = await json(ADMIN, `SELECT sum(tickets_sold) AS sold, sum(gross_cents) AS gross, uniqMerge(purchasers) AS buyers FROM ticket_funnel_daily WHERE tenant_id = '${TR}'`, DB);
      const [e] = await json(ADMIN, `SELECT sum(questions_asked) AS asked FROM engagement_hourly WHERE tenant_id = '${TR}'`, DB);
      return [Number(f.sold), Number(f.gross), Number(f.buyers), Number(e.asked)];
    };
    const [rawTotals] = await json(
      ADMIN,
      `SELECT sumIf(quantity, activity = 'ticket_purchased') AS sold, sumIf(amount_cents, activity = 'ticket_purchased') AS gross,
              uniqExactIf(user_id, activity = 'ticket_purchased') AS buyers, countIf(activity = 'question_asked') AS asked
         FROM activity_events WHERE tenant_id = '${TR}'`,
      DB,
    );
    const truth = [Number(rawTotals.sold), Number(rawTotals.gross), Number(rawTotals.buyers), Number(rawTotals.asked)];
    check(JSON.stringify(await rollupTotals()) === JSON.stringify(truth), `CONTROL: before any damage the rollups agree with the raw table: ${JSON.stringify(truth)}`);

    // The damage: a rollup row no raw row stands behind. Something a rebuild that
    // did not really empty and refill the table would leave in place.
    const corrupt = await ch(
      APP,
      `INSERT INTO ticket_funnel_daily SELECT '${TR}', 'evt_corrupt', toDate(now()), 'tt_ga', 'USD',
              uniqState('ghost'), uniqState('ghost'), toUInt64(0), toUInt64(0), toUInt64(0), toUInt64(999), toUInt64(99900)`,
      { database: DB },
    );
    const damaged = await rollupTotals();
    check(corrupt.ok && damaged[0] === truth[0] + 999, `CONTROL: the corruption shows: ${damaged[0]} tickets sold where the raw table has ${truth[0]}`);

    const rebuilt = spawnSync(process.execPath, ['dist/scripts/rebuild-rollups.js'], {
      cwd: join(ROOT, 'services', 'analytics'),
      env: {
        PATH: process.env['PATH'],
        SystemRoot: process.env['SystemRoot'],
        LOG_LEVEL: 'info',
        CLICKHOUSE_URL: CH_URL,
        CLICKHOUSE_DATABASE: DB,
        CLICKHOUSE_MIGRATOR_USER: MIGRATOR.user,
        CLICKHOUSE_MIGRATOR_PASSWORD: MIGRATOR.password,
      },
      encoding: 'utf8',
    });
    const rebuildLog = `${rebuilt.stdout}${rebuilt.stderr}`;
    const views = (rebuildLog.match(/rollup rebuilt from the raw table/g) ?? []).length;
    check(rebuilt.status === 0 && views === 2, `pnpm run rebuild-rollups exits ${rebuilt.status} after rebuilding ${views} rollups, derived from the views themselves`);
    const restored = await rollupTotals();
    check(JSON.stringify(restored) === JSON.stringify(truth), `and the rollups equal the raw table again, not the corruption: ${JSON.stringify(restored)} (want ${JSON.stringify(truth)})`);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  ClickHouse down: nothing committed, lag climbs by what was produced, then drains with every row landed');
    // -----------------------------------------------------------------------
    await producer.connect();
    await admin.connect();
    const send = async (payload) => {
      const [meta] = await producer.send({ topic: 'live.interactions', messages: [{ key: payload.eventId, value: JSON.stringify(payload) }] });
      produced.push({ topic: 'live.interactions', partition: meta.partition, offset: meta.baseOffset });
      return { partition: meta.partition, offset: meta.baseOffset };
    };
    const startConsumer = (overrides = {}) => {
      child = spawn(process.execPath, ['dist/consumer.js'], {
        cwd: join(ROOT, 'services', 'analytics'),
        env: serviceEnv(ROOT, 'analytics', {
          LOG_LEVEL: 'fatal',
          INGEST_PORT: String(PORT),
          CLICKHOUSE_URL: CH_URL,
          CLICKHOUSE_DATABASE: DB,
          KAFKA_BROKERS: process.env['KAFKA_BROKERS'],
          SCHEMA_REGISTRY_URL: process.env['SCHEMA_REGISTRY_URL'],
          KAFKA_CONSUMER_GROUP: GROUP,
          INGEST_START_FROM: 'latest',
          ...overrides,
        }),
        stdio: ['ignore', 'ignore', 'ignore'],
      });
    };
    const probe = async (path) => {
      try {
        const res = await fetch(`http://127.0.0.1:${PORT}${path}`);
        return { status: res.status, body: await res.json().catch(() => null) };
      } catch {
        return null;
      }
    };
    const metrics = async () => (await probe('/metrics'))?.body?.ingest ?? null;
    const committed = async (partition) => {
      const offsets = await admin.fetchOffsets({ groupId: GROUP, topics: ['live.interactions'] });
      const o = offsets[0]?.partitions.find((p) => p.partition === partition)?.offset;
      return o === undefined || o === '-1' ? null : BigInt(o);
    };
    const rowsFor = async (userPrefix) =>
      Number((await json(ADMIN, `SELECT count() AS n FROM activity_events WHERE tenant_id = '${TD}' AND startsWith(user_id, '${userPrefix}')`, DB))[0].n);
    const topicMeta = await admin.fetchTopicMetadata({ topics: TOPICS });
    const partitionTotal = topicMeta.topics.reduce((n, t) => n + t.partitions.length, 0);
    const EVD = `evt_down_${run}`;

    // A healthy consumer first, so the group has a committed position on the
    // partition the drill uses. Under 'latest' with nothing committed, a restarted
    // consumer would skip what was produced while it was away.
    startConsumer();
    check(Boolean(await waitFor(async () => (await metrics())?.assignedPartitions === partitionTotal, 60_000)), 'CONTROL: a healthy consumer joined the group');
    let canary = null;
    for (let attempt = 0; attempt < 10 && !canary; attempt++) {
      const at = await send(upvote(TD, EVD, `canary_${attempt}`));
      if (await waitFor(async () => (await rowsFor(`u_${run}_canary_${attempt}`)) === 1, 4_000)) canary = at;
    }
    check(Boolean(canary), 'CONTROL: a canary became a row');
    if (!canary) return;
    const positioned = await waitFor(async () => {
      const c = await committed(canary.partition);
      return c !== null && c > BigInt(canary.offset);
    }, 15_000);
    check(Boolean(positioned), `CONTROL: the group committed past the canary on partition ${canary.partition}`);
    await stopConsumer();

    startConsumer({ CLICKHOUSE_URL: 'http://127.0.0.1:1' });
    check(
      Boolean(await waitFor(async () => (await metrics())?.assignedPartitions === partitionTotal, 90_000)),
      'CONTROL: a consumer whose ClickHouse is unreachable joined the same group',
    );
    const frozenAt = await committed(canary.partition);
    const during = [];
    for (let n = 0; n < 5; n++) during.push(await send(upvote(TD, EVD, `down_${n}`)));
    const lagOf = (m) => m?.lag?.partitions?.find((p) => p.topic === 'live.interactions' && p.partition === canary.partition)?.lag;
    const climbed = await waitFor(async () => (lagOf(await metrics()) ?? -1) >= 5, 45_000);
    check(Boolean(climbed), `/metrics shows lag of ${lagOf(await metrics())} messages on partition ${canary.partition} while ClickHouse is down (want at least 5)`);
    const notReady = await probe('/ready');
    check(notReady?.status === 503 && notReady.body?.clickhouse === false, `/ready is 503 with clickhouse false (${notReady?.status} ${JSON.stringify(notReady?.body)})`);
    check(
      (await committed(canary.partition)) === frozenAt && (await rowsFor(`u_${run}_down_`)) === 0,
      `nothing was written and the committed offset did not move (${frozenAt} -> ${await committed(canary.partition)})`,
    );
    await stopConsumer();

    startConsumer();
    const drained = await waitFor(async () => (await rowsFor(`u_${run}_down_`)) === 5, 90_000);
    check(Boolean(drained), `with ClickHouse back, the same group delivers all 5 messages (${await rowsFor(`u_${run}_down_`)} rows)`);
    const last = during[during.length - 1];
    const zero = await waitFor(async () => lagOf(await metrics()) === 0, 30_000);
    check(Boolean(zero), `and lag drains to ${lagOf(await metrics())}`);
    const past = await committed(canary.partition);
    check(past !== null && past > BigInt(last.offset), `committed ${past}, past the last message at ${last.offset}`);
    const parts = (await metrics())?.parts;
    check(
      Array.isArray(parts?.tables) && parts.tables.some((t) => t.table === 'activity_events') && typeof parts.throwInsertAt === 'number',
      `/metrics carries part counts too: ${parts?.tables?.length} tables, refused at ${parts?.throwInsertAt}`,
    );
    await stopConsumer();
  } finally {
    await stopConsumer();
    if (produced.length > 0) {
      const realCleanup = await cleanRealAnalytics(admin, produced, TD).catch((err) => `failed: ${err.message}`);
      console.log(`\n    the real analytics database: this run's copies removed; ${realCleanup}`);
    }
    await producer.disconnect().catch(() => {});
    await admin.disconnect().catch(() => {});
    await app?.close().catch(() => {});
    const gone = await dropScratchDatabase(DB);
    console.log(`    cleanup: scratch database ${gone ? 'dropped' : 'STILL PRESENT'}`);
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('parts are visible before they bite, a rollup rebuilds from raw, and an outage is lag that drains with nothing lost');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
