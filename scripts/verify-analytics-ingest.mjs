/**
 * Asserts that Analytics ingest turns the ticket and live topics into rows and
 * rollups that count each message once, under the delivery Kafka provides.
 *
 *   ONE MESSAGE, ONE COUNT     a redelivery that ends later than the first attempt,
 *                              a republish at a new offset, a duplicate inside one
 *                              batch, and a retry that raced its own lookup each
 *                              leave the raw table AND the funnel rollup unchanged
 *   AN INDEXED LOOKUP          the duplicate check uses tenant_id from the key
 *   FAITHFUL ROWS              one row per line, with that line's quantity and
 *                              money, at the UTC instant the producer stamped,
 *                              carrying the Kafka coordinates it was read from
 *   NOT BLOCKED                a message that can never be a row — refused by the
 *                              mapper, or by ClickHouse — is kept in ingest_dlq
 *                              with its coordinates and bytes, and its partition
 *                              moves past it; a failure that is not the message's
 *                              fault is thrown for redelivery instead
 *   REFUSED BEFORE CHANGED     what ClickHouse would silently store as something
 *                              else never reaches it
 *   COMMITTED AFTER THE WRITE  the group's committed offset reaches past what was
 *                              written, so a restart does not replay it
 *
 * THE WRITER IS DRIVEN DIRECTLY for the duplicate cases, not through Kafka,
 * because each needs an exact interleaving — a crash between insert and commit, a
 * retry that passed the lookup before the first insert landed — that a live
 * consumer produces only by luck. The mapper is the real one, so the rows are the
 * rows Kafka would produce.
 *
 * A SCRATCH DATABASE PER RUN, migrated by the real runner and granted to
 * analytics_app exactly as analytics is. The consumer is spawned against it under
 * a fresh group starting at 'latest', which skips anything produced before the
 * group has resolved its position — so a canary goes first, and nothing is
 * asserted before it lands.
 *
 * WATCHED FAIL: a real bug first, then six guards broken across two runs.
 *
 *   the consumer as first written      committed offsets -1 on all three topics
 *                                      after every message had been written
 *   run 1, 14 red:
 *     writer lookup bypassed           a redelivery inserted 20 and skipped 0;
 *                                      a republish inserted 4
 *     lookup as a tuple IN             the primary-key step used [event_id] only
 *     line money = the charge total    purchase lines of 7000 and 7000, and a
 *                                      rollup gross of 14000
 *     ZodError message kept raw        the DLQ error was multi-line JSON
 *   run 2, 10 red:
 *     dedup token emptied              the raced retry landed: 12 rows, not 6 —
 *                                      ClickHouse's own block hash did not catch it
 *     poison thrown, not kept          0 of 5 rows behind it, nothing in the DLQ,
 *                                      committed offsets stopped at the poison
 *
 * And this file was wrong once: it wanted four rows behind the poison where the
 * fixture sends five, and failed on a correct consumer.
 *
 * The cross-tenant checks came later, from the real ingest container storing this
 * file's reused canary id under three tenants. Against the writer as it was then,
 * one id under two tenants in a batch kept only the first tenant's rows, and a
 * second tenant's copy written on its own was stored as nothing at all: its token,
 * hashed from ids without the tenant, matched the first tenant's block. The check
 * that leans on that copy passed anyway — so its precondition is now a control
 * read from the table, which went red.
 *
 * The refusal checks came from review. A lone surrogate the mapper accepted made
 * ClickHouse refuse the whole insert, and the batch was redelivered forever:
 * against that consumer this file went 5 red — the partition stalled at one
 * offset while kafkajs crash-looped, and the truncated JSON beside the surrogate
 * never reached the DLQ either. Then two runs against broken guards:
 *
 *   run 3, 6 red   the mapper's four refusals    all five cases mapped; the
 *                  loosened                      surrogate reached ClickHouse and
 *                                                was kept by the writer instead —
 *                                                and its partition still moved
 *   run 4, 4 red   the writer's per-message pass the refused batch stored
 *                  emptied, and any ClickHouse   nothing; a missing database was
 *                  error called a refusal        kept aside rather than thrown
 *
 * One run between those proved nothing and is not counted: ClickHouse refused a
 * plain count() with MEMORY_LIMIT_EXCEEDED while the stuck compose ingest
 * container retried beside it.
 *
 * A second review round added the rest, each seen red first. Against the consumer
 * as it was, 9 red: a poison message whose own zod error echoed a lone surrogate
 * stalled its partition all over again, because ClickHouse refused the dead-letter
 * row; a registry outage left /ready at 200 with no failure or crash counted; and
 * a wrong ClickHouse password answered ready. Then the fixes were broken, in
 * batches kept apart so that each red names one guard:
 *
 *   batch 1, 5 red   the token without the tenant    tenant B's copy stored as nothing
 *                    any ClickHouse error in the     4 inserts, nothing thrown
 *                    per-message pass a refusal
 *                    a commit ahead of the write     0 of 2 rows stored at commit
 *                    ping() without select           ready on a wrong password
 *   batch 2, 5 red   dead-letter text not made       the stall, reproduced
 *                    well-formed
 *   batch 3, 3 red   a write error swallowed         committed with ClickHouse down
 *                    health tracking off             /ready 200 through an outage,
 *                                                    crashesTotal 0
 *   batch 4, 7 red   a transient decode failure      kept aside and committed past;
 *                    parked like poison              with the registry back, the
 *                                                    purchase never came back
 *
 * Then a measurement no test had made: a backlog-sized batch. Against the writer
 * as it was, 2 red. 12000 rows whose ids take 216 KiB failed their duplicate
 * lookup with "Field value too long", because each query parameter is one HTTP
 * form field and ClickHouse caps those at 128 KiB — not a refusal, so the batch
 * would have been rethrown forever. And a 257-character message id mapped, which
 * no lookup budget could have held.
 *
 * Run (after pnpm build, topics and schemas, with ClickHouse up):
 *   node scripts/verify-analytics-ingest.mjs
 */

import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { readdirSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { Kafka, logLevel } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { ADMIN, APP, CH_URL, ch, createScratchDatabase, dropScratchDatabase, json, migrate } from './lib/clickhouse.mjs';
import { serviceEnv } from './lib/service-env.mjs';
import { cleanRealAnalytics } from './lib/real-analytics.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');

process.env['KAFKA_BROKERS'] ??= '127.0.0.1:9092';
process.env['SCHEMA_REGISTRY_URL'] ??= 'http://127.0.0.1:8081';

const BROKERS = process.env['KAFKA_BROKERS'].split(',').map((b) => b.trim());
const REGISTRY = process.env['SCHEMA_REGISTRY_URL'];
const PORT = 3916;

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
const DB = `analytics_ingest_${run}`;
const GROUP = `verify_analytics_ingest_${run}`;
/** The only tenant this run puts on the shared topics. */
const TE = `t_ing_e2e_${run}`;

// A minute ago, fixed for the run: every row written from these payloads must
// carry exactly this instant.
const AT = new Date(Date.now() - 60_000).toISOString();
const AT_CH = AT.replace('T', ' ').replace('Z', '');

/** A tickets.purchased payload: two lines, 3 tickets, 7000 cents. */
function purchase(tenantId, eventId, n, over = {}) {
  return {
    messageId: `pur_${run}_${tenantId}_${n}`,
    occurredAt: AT,
    paymentId: `pay_${run}_${n}`,
    reservationId: `res_${run}_${n}`,
    tenantId,
    eventId,
    userId: `u_${n}`,
    amountCents: 7000,
    currency: 'USD',
    items: [
      { ticketTypeId: 'tt_ga', quantity: 2, unitPriceCents: 2000 },
      { ticketTypeId: 'tt_vip', quantity: 1, unitPriceCents: 3000 },
    ],
    providerRef: null,
    ...over,
  };
}

async function main() {
  const ping = await fetch(`${CH_URL}/ping`).then((r) => r.text()).catch(() => '');
  check(ping.trim() === 'Ok.', `ClickHouse answers at ${CH_URL}`);
  if (failures > 0) return;

  const { createClickHouse } = await mod('clickhouse.js');
  const { writeActivity, insertRows, writeMessages, refusedByClickHouse, EXISTING_ROWS_QUERY } = await mod('ingest/writer.js');
  const { toRows, INGEST_TOPICS } = await mod('ingest/mappers.js');
  const TOPICS = Object.keys(INGEST_TOPICS);

  const kafka = new Kafka({ clientId: `verify-analytics-ingest-${run}`, brokers: BROKERS, logLevel: logLevel.NOTHING });
  const producer = kafka.producer();
  const admin = kafka.admin();
  const registry = new SchemaRegistry({ host: REGISTRY });
  let app = null;
  let child = null;
  const produced = [];

  const stopConsumer = async () => {
    if (!child) return;
    const c = child;
    child = null;
    c.kill('SIGTERM');
    await new Promise((r) => (c.exitCode !== null ? r() : c.once('exit', r)));
  };

  try {
    await createScratchDatabase(DB);
    const migrations = readdirSync(join(ROOT, 'services', 'analytics', 'migrations')).filter((f) => f.endsWith('.sql')).length;
    const migrated = migrate(ROOT, DB);
    check(
      migrated.code === 0 && migrated.applied === migrations,
      `CONTROL: ${DB} migrated by the real runner: exit ${migrated.code}, applied ${migrated.applied} of ${migrations}`,
    );
    if (failures > 0) return;

    app = createClickHouse({ url: CH_URL, database: DB, username: APP.user, password: APP.password });
    const EV = `evt_ing_${run}`;

    const rowsFor = (tenant, from, to, offsetBase = 0) => {
      const out = [];
      for (let n = from; n <= to; n++) {
        out.push(...toRows({ topic: 'tickets.purchased', partition: 0, offset: String(offsetBase + n) }, purchase(tenant, EV, n)));
      }
      return out;
    };
    const totals = async (tenant) => {
      const [raw] = await json(ADMIN, `SELECT count() AS n FROM activity_events WHERE tenant_id = '${tenant}'`, DB);
      const [roll] = await json(ADMIN, `SELECT sum(tickets_sold) AS t, sum(gross_cents) AS g FROM ticket_funnel_daily WHERE tenant_id = '${tenant}'`, DB);
      return { raw: Number(raw.n), tickets: Number(roll.t), gross: Number(roll.g) };
    };
    const same = (a, b) => a.raw === b.raw && a.tickets === b.tickets && a.gross === b.gross;
    const fmt = (t) => `${t.raw} rows, ${t.tickets} tickets, ${t.gross} cents`;
    const rawInsert = async (rows, token) => {
      const r = await ch(APP, 'INSERT INTO activity_events', {
        database: DB,
        rows,
        settings: { insert_deduplication_token: token, deduplicate_blocks_in_dependent_materialized_views: 1 },
      });
      if (!r.ok) throw new Error(`insert failed: ${r.text}`);
    };

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  a redelivered batch that ends later than the first attempt counts each message once');
    // -----------------------------------------------------------------------
    const TA = `t_ing_a_${run}`;
    const w1 = await writeActivity(app, rowsFor(TA, 0, 4));
    check(w1.inserted === 10 && w1.duplicates === 0, `first attempt, messages 0-4: inserted ${w1.inserted}, duplicates ${w1.duplicates}`);
    const w2 = await writeActivity(app, rowsFor(TA, 0, 9));
    check(w2.inserted === 10 && w2.duplicates === 10, `redelivery after a crash before commit, messages 0-9: inserted ${w2.inserted}, skipped ${w2.duplicates}`);
    const a = await totals(TA);
    check(same(a, { raw: 20, tickets: 30, gross: 70000 }), `ten messages counted once: ${fmt(a)} (want 20 rows, 30 tickets, 70000 cents)`);

    const TOLD = `t_ing_old_${run}`;
    await rawInsert(rowsFor(TOLD, 0, 4), 'tickets.purchased-0-0-4');
    await rawInsert(rowsFor(TOLD, 0, 9), 'tickets.purchased-0-0-9');
    const old = await totals(TOLD);
    check(
      same(old, { raw: 30, tickets: 45, gross: 105000 }),
      `CONTROL: the offset-range token this replaced counts messages 0-4 twice in raw AND rollup: ${fmt(old)}`,
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  a message republished at a new offset is recognised by its id');
    // -----------------------------------------------------------------------
    const w3 = await writeActivity(app, rowsFor(TA, 3, 4, 1000));
    check(w3.inserted === 0 && w3.duplicates === 4, `messages 3-4 again at offsets 1003-1004: inserted ${w3.inserted}, skipped ${w3.duplicates}`);
    const a2 = await totals(TA);
    check(same(a2, a), `and nothing moved: ${fmt(a2)}`);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  a message twice inside one batch, and a retry that got past the lookup');
    // -----------------------------------------------------------------------
    const TB = `t_ing_b_${run}`;
    const one = rowsFor(TB, 0, 0);
    const w4 = await writeActivity(app, [...one, ...one]);
    check(w4.inserted === 2 && w4.duplicates === 2, `the same message twice in a batch: inserted ${w4.inserted}, skipped ${w4.duplicates}`);
    const b = await totals(TB);
    check(same(b, { raw: 2, tickets: 3, gross: 7000 }), `counted once: ${fmt(b)}`);

    const TC = `t_ing_c_${run}`;
    const raced = rowsFor(TC, 0, 2);
    const w5 = await writeActivity(app, raced);
    // The retry whose lookup ran before the first insert had landed: it reaches
    // the insert with every row still "fresh".
    await insertRows(app, raced);
    const c = await totals(TC);
    check(
      w5.inserted === 6 && same(c, { raw: 6, tickets: 9, gross: 21000 }),
      `the retry is absorbed by the token: ${fmt(c)} (want 6 rows, 9 tickets, 21000 cents)`,
    );
    await rawInsert(raced, `any-other-token-${run}`);
    const c2 = await totals(TC);
    check(
      same(c2, { raw: 12, tickets: 18, gross: 42000 }),
      `CONTROL: the same rows under a different token land again — ${fmt(c2)} — so the absorption was the token`,
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log("  one tenant's message id cannot suppress another tenant's message");
    // -----------------------------------------------------------------------
    // Kafka is not tenant-scoped, and a message id is only as unique as its
    // producer makes it. This file's own canary once reused one id across runs,
    // under a new tenant each time, and the real ingest stored it three times —
    // correctly, but only because each copy arrived in a different batch.
    const shared = (tenant, n) =>
      toRows({ topic: 'tickets.purchased', partition: 0, offset: String(n) }, purchase(tenant, EV, n, { messageId: `shared_${run}` }));
    const TX = `t_ing_x_${run}`;
    const TY = `t_ing_y_${run}`;
    const w6 = await writeActivity(app, [...shared(TX, 0), ...shared(TY, 1)]);
    check(
      w6.inserted === 4 && w6.duplicates === 0,
      `one id under two tenants in the same batch: inserted ${w6.inserted}, skipped ${w6.duplicates} (want 4, 0)`,
    );
    const TZ = `t_ing_z_${run}`;
    const TW = `t_ing_w_${run}`;
    await writeActivity(app, shared(TW, 2));
    // The precondition, checked in the TABLE rather than trusted from the writer's
    // count. Without it the next check passed on the broken writer: its token was
    // hashed from ids alone, so this copy reused an earlier tenant's token and
    // ClickHouse discarded it as a duplicate block — leaving nothing to collide with.
    const tw = await totals(TW);
    check(
      same(tw, { raw: 2, tickets: 3, gross: 7000 }),
      `CONTROL: the other tenant's copy is really stored before the batch that shares its id: ${fmt(tw)}`,
    );
    const w7 = await writeActivity(app, [...shared(TZ, 3), ...rowsFor(TW, 7, 7)]);
    check(
      w7.inserted === 4 && w7.duplicates === 0,
      `an id already stored for another tenant that shares the batch: inserted ${w7.inserted}, skipped ${w7.duplicates} (want 4, 0)`,
    );
    const perTenant = await Promise.all([TX, TY, TZ].map(totals));
    const twAfter = await totals(TW);
    check(
      perTenant.every((t) => same(t, { raw: 2, tickets: 3, gross: 7000 })) && same(twAfter, { raw: 4, tickets: 6, gross: 14000 }),
      `and each tenant holds exactly its own purchases: ${[...perTenant, twAfter].map(fmt).join(' | ')} (want 2 rows each, 4 for the last)`,
    );

    // Each tenant's copy ALONE in its own batch. The cases above always put an id
    // beside other rows, and review showed a token hashed WITHOUT the tenant passes
    // them all: no two of those batches hold identical ids. These two do, so only
    // the tenant can tell their tokens apart.
    const TP = `t_ing_p_${run}`;
    const TQ = `t_ing_q_${run}`;
    const solo = (tenant) =>
      toRows({ topic: 'tickets.purchased', partition: 0, offset: '0' }, purchase(tenant, EV, 0, { messageId: `solo_${run}` }));
    await writeActivity(app, solo(TP));
    await writeActivity(app, solo(TQ));
    const [tp, tq] = await Promise.all([totals(TP), totals(TQ)]);
    check(
      same(tp, { raw: 2, tickets: 3, gross: 7000 }) && same(tq, { raw: 2, tickets: 3, gross: 7000 }),
      `one id written by two tenants in separate batches is stored for both: ${fmt(tp)} | ${fmt(tq)}`,
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log("  the duplicate lookup is narrowed by tenant, the key's first column");
    // -----------------------------------------------------------------------
    const explain = await ch(ADMIN, `EXPLAIN indexes = 1 ${EXISTING_ROWS_QUERY}`, {
      database: DB,
      params: { tenants: `['${TA}']`, events: `['${EV}']`, ids: "['x']", from: AT_CH, to: AT_CH },
    });
    const lines = explain.text.split('\n');
    const pk = lines.findIndex((l) => /^\s*PrimaryKey\s*$/.test(l));
    const keys = [];
    if (pk !== -1) {
      for (const l of lines.slice(pk + 1)) {
        if (/^\s*Keys:\s*$/.test(l)) continue;
        if (/^\s*[A-Za-z ]+:/.test(l)) break;
        keys.push(l.trim());
      }
    }
    check(explain.ok && pk !== -1, `CONTROL: EXPLAIN reports a primary-key step for the lookup${explain.ok ? '' : `: ${explain.text.slice(0, 160)}`}`);
    check(keys.includes('tenant_id') && keys.includes('event_id'), `it uses key columns [${keys.join(', ')}] (want tenant_id and event_id)`);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  a batch too big for one lookup is looked up in parts');
    // -----------------------------------------------------------------------
    // Every lookup parameter travels as one HTTP form field, which ClickHouse caps at
    // http_max_field_value_size. Found by measuring, not by any test: 10000 ids in
    // one lookup failed with "Field value too long", which is not a refusal, so a
    // consumer catching up on a backlog would have rethrown the same batch forever.
    const [{ value: fieldLimit }] = await json(ADMIN, "SELECT value FROM system.settings WHERE name = 'http_max_field_value_size'", DB);
    const TL = `t_ing_l_${run}`;
    const large = rowsFor(TL, 0, 5_999);
    const idBytes = [...new Set(large.map((r) => r.message_id))].reduce((n, id) => n + Buffer.byteLength(id) + 3, 0);
    check(
      idBytes > Number(fieldLimit),
      `CONTROL: the batch's ids alone take ${Math.round(idBytes / 1024)} KiB, past the ${Math.round(Number(fieldLimit) / 1024)} KiB one parameter may hold`,
    );
    let largeFirst = null;
    let largeReplay = null;
    let largeError = null;
    try {
      largeFirst = await writeActivity(app, large);
      largeReplay = await writeActivity(app, large);
    } catch (err) {
      largeError = err;
    }
    check(
      largeFirst?.inserted === large.length && largeReplay?.duplicates === large.length,
      `and it is written, then recognised on replay: inserted ${largeFirst?.inserted}, skipped ${largeReplay?.duplicates} of ${large.length}` +
        (largeError ? ` — threw ${largeError.type ?? ''} ${String(largeError.message).slice(0, 80)}` : ''),
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  what ClickHouse would refuse, or silently store as something else, is refused first');
    // -----------------------------------------------------------------------
    // Each case was measured against ClickHouse 25.8 before a rule was written:
    // the surrogate refused a whole insert, and the rest were STORED, changed —
    // 2 March for 30 February, a local time read as UTC, a wrapped integer.
    const TM = `t_ing_m_${run}`;
    const mapError = (payload) => {
      try {
        toRows({ topic: 'tickets.purchased', partition: 0, offset: '0' }, payload);
        return null;
      } catch (err) {
        return err instanceof Error ? err.message.replace(/\s+/g, ' ') : String(err);
      }
    };
    check(mapError(purchase(TM, EV, 0)) === null, 'CONTROL: the unaltered purchase maps');
    for (const [label, over, pattern] of [
      ['a lone surrogate in a ticket type id', { items: [{ ticketTypeId: 'tt_\ud800', quantity: 1, unitPriceCents: 100 }] }, /well-formed/],
      ['a message id longer than 256 characters, which no lookup budget could hold', { messageId: 'm'.repeat(257) }, /256/],
      ['a day that does not exist', { occurredAt: '2026-02-30T00:00:00.000Z' }, /UTC instant/],
      ['a time with no zone, which Date reads as local', { occurredAt: '2026-09-14 01:00:00' }, /UTC instant/],
      ['a line amount past 2^53 from in-range factors', { items: [{ ticketTypeId: 'tt_ga', quantity: 4, unitPriceCents: 2 ** 52 }] }, /2\^53/],
      [
        '65537 lines, one past what a UInt16 line index holds',
        { items: Array.from({ length: 65_537 }, () => ({ ticketTypeId: 'tt_ga', quantity: 1, unitPriceCents: 1 })) },
        /65536/,
      ],
    ]) {
      const message = mapError(purchase(TM, EV, 0, over));
      check(message !== null && pattern.test(message), `${label} is refused${message ? `: ${message.slice(0, 110)}` : ' — it mapped'}`);
    }

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  a message ClickHouse refuses is found and kept aside, and the rest of its batch is written');
    // -----------------------------------------------------------------------
    // Bypasses the mapper on purpose: this is the net under it, for whatever a
    // future mapper lets through. A negative quantity is one ClickHouse refuses.
    const TN = `t_ing_n_${run}`;
    const messageOf = (n, change = (r) => r) => ({
      offset: String(n),
      payload: Buffer.from(`message ${n} ${run}`),
      rows: rowsFor(TN, n, n).map(change),
    });
    const netBatch = [messageOf(0), messageOf(1, (r) => ({ ...r, quantity: -1 })), messageOf(2)];

    let whole = null;
    try {
      await writeActivity(app, netBatch.flatMap((m) => m.rows));
    } catch (err) {
      whole = err;
    }
    check(
      refusedByClickHouse(whole),
      `CONTROL: as one insert, ClickHouse refuses the whole batch${whole ? ` (${whole.type ?? whole.message})` : ' — it did not'}`,
    );
    check((await totals(TN)).raw === 0, 'CONTROL: and stores none of it, the two good messages included');

    const net = await writeMessages(app, netBatch);
    check(
      net.inserted === 4 && net.refused.length === 1 && net.refused[0].offset === '1' && /CANNOT_PARSE_NUMBER/.test(net.refused[0].error),
      `the refused message is found: inserted ${net.inserted}, refused ${JSON.stringify(net.refused.map((r) => [r.offset, r.error.slice(0, 50)]))}`,
    );
    const tn = await totals(TN);
    check(same(tn, { raw: 4, tickets: 6, gross: 14000 }), `and the good messages around it are stored whole: ${fmt(tn)}`);
    check(
      Boolean(net.refused[0]?.payload?.equals(Buffer.from(`message 1 ${run}`))),
      'and the refused message keeps its bytes for the dead-letter table',
    );

    const elsewhere = createClickHouse({ url: CH_URL, database: `analytics_missing_${run}`, username: APP.user, password: APP.password });
    let outage = null;
    try {
      await writeMessages(elsewhere, [messageOf(3)]);
    } catch (err) {
      outage = err;
    }
    await elsewhere.close();
    check(
      outage !== null && !refusedByClickHouse(outage),
      `a failure that is not the rows' fault is thrown for redelivery, never kept aside: ${outage ? (outage.type ?? outage.message).slice(0, 80) : 'nothing was thrown'}`,
    );

    // The same, but PARTWAY THROUGH the per-message pass. The check above fails at
    // the first lookup, before that pass runs, so review found its own rethrow
    // could be deleted with every check still green — and then an outage mid-pass
    // would dead-letter each good message after it and commit past them. A client
    // whose third insert fails as ClickHouse once did mid-run: insert 1 is the
    // whole batch, refused for real; 2 is the bad message alone; 3 is the outage.
    const requireFromAnalytics = createRequire(join(ROOT, 'services', 'analytics', 'package.json'));
    const { ClickHouseError } = await import(pathToFileURL(requireFromAnalytics.resolve('@clickhouse/client')).href);
    let inserts = 0;
    const flaky = {
      query: (params) => app.query(params),
      insert: async (params) => {
        inserts += 1;
        if (inserts === 3) {
          throw new ClickHouseError({ message: 'simulated: memory limit exceeded', code: '241', type: 'MEMORY_LIMIT_EXCEEDED' });
        }
        return app.insert(params);
      },
    };
    const TR = `t_ing_r_${run}`;
    const messageFor = (tenant, n, change = (r) => r) => ({
      offset: String(n),
      payload: Buffer.from(`message ${n} ${run}`),
      rows: rowsFor(tenant, n, n).map(change),
    });
    let midway = null;
    try {
      await writeMessages(flaky, [messageFor(TR, 0, (r) => ({ ...r, quantity: -1 })), messageFor(TR, 1), messageFor(TR, 2)]);
    } catch (err) {
      midway = err;
    }
    check(
      inserts === 3 && midway?.type === 'MEMORY_LIMIT_EXCEEDED',
      `an outage partway through the per-message pass is thrown, not taken for a refusal: ${inserts} inserts, ${midway ? (midway.type ?? midway.message) : 'nothing thrown'}`,
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  a batch commits only what it wrote, and commits nothing when a dependency fails');
    // -----------------------------------------------------------------------
    // The batch step itself, handed batches whose dependencies fail on cue. Review
    // found three changes that passed every check through Kafka: the commit moved
    // ahead of the write, a write error swallowed, the transient-decode rethrow
    // deleted. Each loses data only when something fails at the wrong moment.
    const { createBatchProcessor } = await mod('ingest/consumer.js');
    const quiet = { info() {}, warn() {}, error() {}, fatal() {}, debug() {} };
    const batchFor = (offset, onCommit = async () => {}) => {
      const calls = { resolved: [], committed: [] };
      return {
        calls,
        payload: {
          batch: { topic: 'tickets.purchased', partition: 0, messages: [{ offset, value: Buffer.from(`batch ${offset} ${run}`) }] },
          resolveOffset: (o) => calls.resolved.push(o),
          heartbeat: async () => {},
          isRunning: () => true,
          isStale: () => false,
          commitOffsetsIfNecessary: async (offsets) => {
            calls.committed.push({ offsets, seen: await onCommit() });
          },
        },
      };
    };
    const outcome = async (promise) => promise.then(() => 'resolved', (err) => `threw ${err?.type ?? err?.code ?? err?.message}`);

    const TS = `t_ing_s_${run}`;
    const ordered = batchFor('41', async () => (await totals(TS)).raw);
    const writes = createBatchProcessor({ ch: app, decode: async () => purchase(TS, EV, 0, { messageId: `batch_${run}` }), logger: quiet });
    const orderedResult = await outcome(writes(ordered.payload));
    const commit = ordered.calls.committed[0];
    check(
      orderedResult === 'resolved' && ordered.calls.committed.length === 1 && commit.seen === 2 && commit.offsets.topics[0].partitions[0].offset === '42',
      `the commit comes after the write: at commit time ${commit?.seen} of 2 rows were stored, committing offset ${commit?.offsets?.topics?.[0]?.partitions?.[0]?.offset} (want 42)`,
    );

    const unreachable = createClickHouse({ url: 'http://127.0.0.1:1', database: DB, username: APP.user, password: APP.password });
    const storeDown = batchFor('43');
    const storeDownResult = await outcome(
      createBatchProcessor({ ch: unreachable, decode: async () => purchase(TS, EV, 1, { messageId: `down_${run}` }), logger: quiet })(storeDown.payload),
    );
    await unreachable.close();
    check(
      storeDownResult.startsWith('threw') && storeDown.calls.committed.length === 0 && storeDown.calls.resolved.length === 0,
      `with ClickHouse unreachable the batch throws and commits nothing: ${storeDownResult}, ${storeDown.calls.committed.length} commit(s)`,
    );

    const dlqRows = async () => Number((await json(ADMIN, 'SELECT count() AS n FROM ingest_dlq', DB))[0].n);
    const dlqBeforeDecode = await dlqRows();
    const registryDown = batchFor('44');
    const registryDownResult = await outcome(
      createBatchProcessor({
        ch: app,
        decode: async () => {
          throw Object.assign(new Error('connect ECONNREFUSED 127.0.0.1:1'), { status: 400 });
        },
        logger: quiet,
      })(registryDown.payload),
    );
    check(
      registryDownResult.startsWith('threw') && registryDown.calls.committed.length === 0 && (await dlqRows()) === dlqBeforeDecode,
      `a registry that cannot be reached is thrown for redelivery, not kept aside as poison: ${registryDownResult}, ${registryDown.calls.committed.length} commit(s), DLQ ${dlqBeforeDecode} -> ${await dlqRows()}`,
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  the consumer, end to end, under a fresh group');
    // -----------------------------------------------------------------------
    await producer.connect();
    await admin.connect();
    const schemaIds = {
      'tickets.reserved': await registry.getLatestSchemaId('tickets.reserved-value'),
      'tickets.purchased': await registry.getLatestSchemaId('tickets.purchased-value'),
    };
    const send = async (topic, key, value) => {
      const [meta] = await producer.send({ topic, messages: [{ key, value }] });
      produced.push({ topic, partition: meta.partition, offset: meta.baseOffset });
      return { partition: meta.partition, offset: meta.baseOffset };
    };
    const sendAvro = async (topic, payload) => send(topic, payload.eventId, await registry.encode(schemaIds[topic], payload));
    const sendJson = async (payload) => send('live.interactions', payload.eventId, JSON.stringify(payload));
    const rawCount = async (where) => Number((await json(ADMIN, `SELECT count() AS n FROM activity_events WHERE ${where}`, DB))[0].n);

    const startConsumer = (overrides = {}) => {
      child = spawn(process.execPath, ['dist/consumer.js'], {
        cwd: join(ROOT, 'services', 'analytics'),
        // Analytics' own .env.example plus these. See scripts/lib/service-env.mjs.
        env: serviceEnv(ROOT, 'analytics', {
          LOG_LEVEL: 'fatal',
          INGEST_PORT: String(PORT),
          CLICKHOUSE_URL: CH_URL,
          CLICKHOUSE_DATABASE: DB,
          KAFKA_BROKERS: process.env['KAFKA_BROKERS'],
          SCHEMA_REGISTRY_URL: REGISTRY,
          KAFKA_CONSUMER_GROUP: GROUP,
          INGEST_START_FROM: 'latest',
          ...overrides,
        }),
        stdio: ['ignore', 'inherit', 'inherit'],
      });
    };
    startConsumer();

    const probe = async (path) => {
      try {
        const res = await fetch(`http://127.0.0.1:${PORT}${path}`);
        return { status: res.status, body: await res.json().catch(() => null) };
      } catch {
        return null;
      }
    };
    const metrics = async () => {
      const r = await probe('/metrics');
      return r?.status === 200 ? r.body.ingest : null;
    };
    const committedOffset = async (topic, partition) => {
      const offsets = await admin.fetchOffsets({ groupId: GROUP, topics: [topic] });
      const o = offsets.find((t) => t.topic === topic)?.partitions.find((p) => p.partition === partition)?.offset;
      return o === undefined || o === '-1' ? null : o;
    };

    const topicMeta = await admin.fetchTopicMetadata({ topics: TOPICS });
    const partitionTotal = topicMeta.topics.reduce((n, t) => n + t.partitions.length, 0);
    const joined = await waitFor(async () => (await metrics())?.assignedPartitions === partitionTotal, 60_000);
    check(Boolean(joined), `the consumer joined its group and holds all ${partitionTotal} partitions of ${TOPICS.join(', ')}`);

    const EV2 = `evt_e2e_${run}`;
    const CANARY_EV = `evt_canary_${run}`;
    let canary = false;
    for (let attempt = 1; attempt <= 10 && !canary; attempt++) {
      await sendJson({ kind: 'question.created', tenantId: TE, eventId: CANARY_EV, userId: 'u_canary', questionId: `q_canary_${run}_${attempt}`, occurredAt: new Date().toISOString() });
      canary = Boolean(await waitFor(async () => (await rawCount(`tenant_id = '${TE}' AND event_id = '${CANARY_EV}'`)) > 0, 4_000));
    }
    check(canary, 'CONTROL: a canary produced after the group settled became a row');
    if (!canary) return;

    const reserved = {
      messageId: `rsv_${run}`,
      occurredAt: AT,
      reservationId: `res_e2e_${run}`,
      tenantId: TE,
      eventId: EV2,
      userId: 'u_buyer',
      state: 'RESERVED',
      items: [
        { ticketTypeId: 'tt_ga', quantity: 2 },
        { ticketTypeId: 'tt_vip', quantity: 1 },
      ],
      expiresAt: new Date(Date.now() + 600_000).toISOString(),
    };
    const purchased = purchase(TE, EV2, 0, { messageId: `pur_e2e_${run}`, userId: 'u_buyer' });
    const asked = { kind: 'question.created', tenantId: TE, eventId: EV2, userId: 'u_asker', questionId: `q_${run}`, occurredAt: AT };

    await sendAvro('tickets.reserved', reserved);
    const atPurchased = await sendAvro('tickets.purchased', purchased);
    await sendJson(asked);
    await sendJson({ ...asked, kind: 'question.upvoted', userId: 'u_voter1' });
    await sendJson({ ...asked, kind: 'question.upvoted', userId: 'u_voter2' });

    const landed = await waitFor(async () => (await rawCount(`tenant_id = '${TE}' AND event_id = '${EV2}'`)) === 7, 30_000);
    check(Boolean(landed), 'a reservation (2 lines), a purchase (2 lines), a question and 2 upvotes became 7 rows');

    const rows = await json(
      ADMIN,
      `SELECT activity, ticket_type_id, quantity, amount_cents, currency, line, toString(occurred_at) AS at, kafka_partition, kafka_offset
         FROM activity_events WHERE tenant_id = '${TE}' AND event_id = '${EV2}' ORDER BY activity, line, user_id`,
      DB,
    );
    const got = rows.map((r) => [r.activity, r.ticket_type_id, Number(r.quantity), Number(r.amount_cents), r.currency, Number(r.line)]);
    const want = [
      ['question_asked', '', 0, 0, '', 0],
      ['question_upvoted', '', 0, 0, '', 0],
      ['question_upvoted', '', 0, 0, '', 0],
      ['ticket_purchased', 'tt_ga', 2, 4000, 'USD', 0],
      ['ticket_purchased', 'tt_vip', 1, 3000, 'USD', 1],
      ['ticket_reserved', 'tt_ga', 2, 0, '', 0],
      ['ticket_reserved', 'tt_vip', 1, 0, '', 1],
    ];
    check(JSON.stringify(got) === JSON.stringify(want), `one row per line, each with its own quantity and money: ${JSON.stringify(got)}`);
    const instants = [...new Set(rows.map((r) => r.at))];
    check(instants.length === 1 && instants[0] === AT_CH, `every row is at the producer's instant in UTC, ${AT_CH} (stored: ${instants.join(', ')})`);
    const purchaseRows = rows.filter((r) => r.activity === 'ticket_purchased');
    check(
      purchaseRows.length === 2 &&
        purchaseRows.every((r) => Number(r.kafka_partition) === atPurchased.partition && String(r.kafka_offset) === atPurchased.offset),
      `the purchase rows carry the partition and offset they were read from (${atPurchased.partition}/${atPurchased.offset})`,
    );

    const [funnel] = await json(
      ADMIN,
      `SELECT sum(seats_reserved) AS reserved, sum(tickets_sold) AS sold, sum(gross_cents) AS gross, uniqMerge(purchasers) AS purchasers
         FROM ticket_funnel_daily WHERE tenant_id = '${TE}' AND event_id = '${EV2}'`,
      DB,
    );
    check(
      Number(funnel.reserved) === 3 && Number(funnel.sold) === 3 && Number(funnel.gross) === 7000 && Number(funnel.purchasers) === 1,
      `the funnel rollup: ${JSON.stringify(funnel)} (want 3 reserved, 3 sold, 7000 cents, 1 purchaser)`,
    );
    const [eng] = await json(
      ADMIN,
      `SELECT sum(questions_asked) AS q, sum(upvotes) AS u, uniqMerge(active_users) AS users
         FROM engagement_hourly WHERE tenant_id = '${TE}' AND event_id = '${EV2}'`,
      DB,
    );
    check(Number(eng.q) === 1 && Number(eng.u) === 2 && Number(eng.users) === 3, `the engagement rollup: ${JSON.stringify(eng)} (want 1 question, 2 upvotes, 3 users)`);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  the relay republishing a purchase changes nothing');
    // -----------------------------------------------------------------------
    const dupBefore = (await metrics()).duplicatesSkippedTotal;
    const atRepublished = await sendAvro('tickets.purchased', purchased);
    const skipped = await waitFor(async () => (await metrics())?.duplicatesSkippedTotal === dupBefore + 2, 30_000);
    check(Boolean(skipped), `CONTROL: the copy at offset ${atRepublished.offset} was consumed and both its lines skipped (${dupBefore} -> ${dupBefore + 2})`);
    const [after] = await json(ADMIN, `SELECT sum(tickets_sold) AS sold, sum(gross_cents) AS gross FROM ticket_funnel_daily WHERE tenant_id = '${TE}' AND event_id = '${EV2}'`, DB);
    check(Number(after.sold) === 3 && Number(after.gross) === 7000, `and the rollup still says 3 sold for 7000 cents (${after.sold}, ${after.gross})`);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  a message that can never be a row is kept, and does not block its partition');
    // -----------------------------------------------------------------------
    const parkedBefore = (await metrics()).deadLetteredTotal;

    // Each poison message is keyed like the good ones, so it shares their
    // partition, and each is followed by a good message that must still land.
    const garbage = Buffer.from(`not a Confluent frame ${run}`);
    const atGarbage = await send('tickets.purchased', EV2, garbage);
    const atBadTenant = await sendAvro('tickets.reserved', { ...reserved, messageId: `rsv_bad_${run}`, tenantId: 'Not A Tenant' });
    const atBadJson = await send('live.interactions', EV2, `{"kind":"question.created","tenantId":"${TE}"`);
    // A message the mapper once ACCEPTED and ClickHouse refuses: a lone UTF-16
    // surrogate, which any Live client can put in an eventId, and which
    // JSON.stringify carries to Kafka as the escape \ud800. ClickHouse 25.8 rejects
    // the whole insert over it (CANNOT_PARSE_ESCAPE_SEQUENCE), so it stalled its
    // partition for good, and every good message behind it with it.
    const atSurrogate = await send(
      'live.interactions',
      EV2,
      JSON.stringify({ ...asked, kind: 'question.upvoted', userId: 'u_surrogate', eventId: '\ud800' }),
    );
    // Refused by the mapper, and the REFUSAL carried the poison: zod echoes the
    // value it received, so this kind's error text held a lone surrogate, and
    // ClickHouse refused the dead-letter row itself — the same stall, one table
    // over. Found by review, measured by two reviewers independently.
    const atBadKind = await send('live.interactions', EV2, JSON.stringify({ ...asked, kind: '\ud800', userId: 'u_badkind' }));

    const behind = {
      'tickets.purchased': await sendAvro('tickets.purchased', purchase(TE, EV2, 1, { messageId: `behind_pur_${run}`, userId: 'u_second' })),
      'tickets.reserved': await sendAvro('tickets.reserved', { ...reserved, messageId: `behind_rsv_${run}`, state: 'RELEASED', expiresAt: null }),
      'live.interactions': await sendJson({ ...asked, kind: 'question.upvoted', userId: 'u_voter3' }),
    };
    // Two lines, two lines and one upvote: five rows. (The first version of this
    // check wanted four, and failed on a correct consumer.)
    const behindRows = `tenant_id = '${TE}' AND (message_id IN ('behind_pur_${run}', 'behind_rsv_${run}') OR user_id = 'u_voter3')`;
    const through = await waitFor(async () => (await rawCount(behindRows)) === 5, 30_000);
    check(
      Boolean(through),
      `the good messages behind the poison on all three topics became rows (${await rawCount(behindRows)} of 5) — no partition is stuck`,
    );
    const parked = await waitFor(async () => (await metrics())?.deadLetteredTotal === parkedBefore + 5, 30_000);
    check(Boolean(parked), `deadLetteredTotal rose by exactly 5 (${parkedBefore} -> ${(await metrics())?.deadLetteredTotal})`);

    const dlq = await json(ADMIN, 'SELECT source_topic, kafka_partition, kafka_offset, error, payload FROM ingest_dlq', DB);
    const kept = (topic, at) =>
      dlq.find((r) => r.source_topic === topic && Number(r.kafka_partition) === at.partition && String(r.kafka_offset) === at.offset);
    const g = kept('tickets.purchased', atGarbage);
    check(
      Boolean(g) && Buffer.from(g.payload, 'base64').equals(garbage),
      `undecodable bytes on tickets.purchased kept at ${atGarbage.partition}/${atGarbage.offset}, bytes intact${g ? `: ${g.error.slice(0, 70)}` : ''}`,
    );
    const t = kept('tickets.reserved', atBadTenant);
    check(
      Boolean(t) && /^contract violation: tenantId: not a valid tenant id$/.test(t.error),
      `a tenant id outside the pattern is refused, in one line naming the field${t ? `: ${JSON.stringify(t.error.slice(0, 90))}` : ''}`,
    );
    check((await rawCount("tenant_id = 'Not A Tenant'")) === 0, 'and nothing was stored under a tenant no query could name');
    const j = kept('live.interactions', atBadJson);
    check(Boolean(j) && /JSON/i.test(j.error), `truncated JSON on live.interactions kept${j ? `: ${j.error.slice(0, 70)}` : ''}`);
    const s = kept('live.interactions', atSurrogate);
    check(
      Boolean(s) && /well-formed/i.test(s.error),
      `a lone surrogate in an eventId is refused before ClickHouse sees it${s ? `: ${JSON.stringify(s.error.slice(0, 90))}` : ''}`,
    );
    const k = kept('live.interactions', atBadKind);
    check(
      Boolean(k) && k.error.isWellFormed() && k.error.includes('�'),
      `a message whose own error text carries a lone surrogate is still kept, the surrogate replaced${k ? `: ${JSON.stringify(k.error.slice(0, 110))}` : ''}`,
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  offsets are committed after the write, so a restart does not replay it');
    // -----------------------------------------------------------------------
    // Waited for, not read straight after the kill: the DLQ counter the wait above
    // watched rises before the commit's round trip, and on Windows the kill is
    // immediate — review showed a correct consumer could go red on that race.
    await waitFor(async () => {
      for (const topic of TOPICS) {
        const o = await committedOffset(topic, behind[topic].partition);
        if (o === null || BigInt(o) <= BigInt(behind[topic].offset)) return false;
      }
      return true;
    }, 15_000);
    await stopConsumer();
    const committed = await admin.fetchOffsets({ groupId: GROUP, topics: TOPICS });
    for (const topic of TOPICS) {
      const at = behind[topic];
      const offset = committed.find((x) => x.topic === topic)?.partitions.find((p) => p.partition === at.partition)?.offset;
      check(
        offset !== undefined && BigInt(offset) > BigInt(at.offset),
        `${topic} partition ${at.partition}: committed ${offset}, past the last written offset ${at.offset}`,
      );
    }

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  an unreachable schema registry is an outage: nothing kept aside, nothing committed, not ready');
    // -----------------------------------------------------------------------
    // Through the real consumer, because review found that nothing had ever made
    // a dependency fail under it: the rethrow for a transient decode failure could
    // be deleted, or the commit moved ahead of the write, with every check green.
    const dlqCount = async () => Number((await json(ADMIN, 'SELECT count() AS n FROM ingest_dlq', DB))[0].n);
    const dlqBefore = await dlqCount();
    startConsumer({ SCHEMA_REGISTRY_URL: 'http://127.0.0.1:1' });
    // Up to 90s: the member killed above is still in the group until its session times out.
    const outageJoined = await waitFor(async () => (await metrics())?.assignedPartitions === partitionTotal, 90_000);
    check(Boolean(outageJoined), 'CONTROL: a consumer whose registry is unreachable joined the same group');
    const atOutage = await sendAvro('tickets.purchased', purchase(TE, EV2, 5, { messageId: `outage_${run}`, userId: 'u_outage' }));
    const failing = await waitFor(async () => (await metrics())?.batchFailuresTotal > 0, 60_000);
    check(Boolean(failing), `CONTROL: it read the purchase and failed on it (batchFailuresTotal ${(await metrics())?.batchFailuresTotal})`);
    const notReady = await waitFor(async () => (await probe('/ready'))?.status === 503, 30_000);
    check(Boolean(notReady), `/ready says 503 while it cannot make progress (${(await probe('/ready'))?.status})`);
    const crashed = await waitFor(async () => (await metrics())?.crashesTotal > 0, 60_000);
    check(Boolean(crashed), `the crash kafkajs restarts it from is counted (crashesTotal ${(await metrics())?.crashesTotal})`);
    check(
      (await rawCount(`message_id = 'outage_${run}'`)) === 0 && (await dlqCount()) === dlqBefore,
      'nothing was written, and nothing was kept aside as if the message were at fault',
    );
    const frozen = await committedOffset('tickets.purchased', atOutage.partition);
    check(
      frozen !== null && BigInt(frozen) <= BigInt(atOutage.offset),
      `the group did not commit past it (committed ${frozen}, the purchase is at ${atOutage.offset})`,
    );
    await stopConsumer();

    startConsumer();
    const recovered = await waitFor(async () => (await rawCount(`message_id = 'outage_${run}'`)) === 2, 90_000);
    check(Boolean(recovered), 'with the registry back, the same group redelivers the purchase and both its lines land');
    const moved = await waitFor(async () => {
      const o = await committedOffset('tickets.purchased', atOutage.partition);
      return o !== null && BigInt(o) > BigInt(atOutage.offset);
    }, 30_000);
    check(Boolean(moved), 'and commits past it only then');
    await stopConsumer();

    // -----------------------------------------------------------------------
    console.log('');
    console.log("  readiness asks ClickHouse as this process's user, not merely whether it answers");
    // -----------------------------------------------------------------------
    // ClickHouse answers GET /ping without authenticating, so a readiness check
    // built on it said ready with a wrong password (found by review).
    startConsumer({ CLICKHOUSE_PASSWORD: 'not-the-password', KAFKA_CONSUMER_GROUP: `${GROUP}_badpass` });
    const subscribed = await waitFor(async () => (await probe('/ready'))?.body?.consumer === true, 60_000);
    check(Boolean(subscribed), 'CONTROL: a consumer given a wrong ClickHouse password connected to Kafka and subscribed');
    const badPass = await probe('/ready');
    check(
      badPass?.status === 503 && badPass.body?.clickhouse === false,
      `and /ready is 503 with clickhouse false (${badPass?.status} ${JSON.stringify(badPass?.body)})`,
    );
    await stopConsumer();
  } finally {
    await stopConsumer();
    const realCleanup = await cleanRealAnalytics(admin, produced, TE).catch((err) => `failed: ${err.message}`);
    console.log(`\n    the real analytics database: this run's copies removed; ${realCleanup}`);
    await producer.disconnect().catch(() => {});
    await admin.disconnect().catch(() => {});
    await app?.close().catch(() => {});
    const gone = await dropScratchDatabase(DB);
    console.log(`\n    cleanup: scratch database ${gone ? 'dropped' : 'STILL PRESENT'}`);
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('each message counts once in rows and rollups, poison is kept without blocking, and offsets follow the write');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
