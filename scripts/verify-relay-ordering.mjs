/**
 * Asserts that one aggregate's messages reach Kafka in the order they were
 * written, even when a publish fails.
 *
 * They did not. CI caught it on a fresh broker: the relay published
 *
 *   key cmtp8xq03000bfw4x8fxz3rv8: v3 at p1/0 (relay) then v1 at p1/8 (relay)
 *
 * -- a newer snapshot of an event, then an older one, from the relay itself.
 *
 * THE MECHANISM. A publish that fails transiently sets nextAttemptAt to a
 * backoff in the future, and the row stays unpublished. The claim query then
 * skipped it (nextAttemptAt <= now()) and nothing stopped the SAME aggregate's
 * later rows going out ahead of it, so seq 6 and 7 published while seq 5 waited.
 * Inside a single drain the same thing happened one level down: the publish loop
 * caught the failure on seq 5 and carried straight on to 6.
 *
 * WHY IT MATTERS MORE THAN "MESSAGES ARRIVED ODDLY". events.activity is
 * COMPACTED, and compaction keeps the last message per key. If the last message
 * for an event is a stale version, the topic's stated purpose -- that a consumer
 * can rebuild its whole projection by replaying from the beginning -- yields a
 * projection built from stale snapshots. Permanently, and with nothing to see:
 * the live projection is fine, because its monotonic version guard discards the
 * late arrival. Only a rebuild is wrong, and only later.
 *
 * That guard is what makes the live path safe. It is not a licence for the relay
 * to emit garbage; a guard doing load-bearing work in normal operation has no
 * margin left for a real bug.
 *
 * Run: node scripts/verify-relay-ordering.mjs
 */

import pg from 'pg';
import { drainOnce } from '../services/relay/dist/drain.js';

const APP_URL =
  process.env['EVENT_APP_DATABASE_URL'] ??
  'postgresql://eventhub_event_app:eventhub_event_app@localhost:5432/eventhub_event';
const RELAY_URL =
  process.env['RELAY_DATABASE_URL'] ??
  'postgresql://eventhub_event_relay:eventhub_event_relay@localhost:5432/eventhub_event';

const tenantId = 'tenant_acme';
const run = `ord_${Date.now()}`;
const TOPIC = 'events.activity';

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}

/** Silent, so a deliberate failure does not print a stack in a passing run. */
const logger = {
  debug() {},
  info() {},
  warn() {},
  error() {},
  child() {
    return logger;
  },
};

/**
 * A publisher that records what it was asked to send and fails on demand.
 *
 * The real one needs a broker and a registry; this needs neither, and it can
 * fail exactly one row transiently, which is the case under test and awkward to
 * provoke against a real broker. The DRAIN is the shipped one, imported from
 * dist rather than reimplemented, so the ordering logic under test is the
 * ordering logic that runs in production.
 */
function stubPublisher(failVersions = new Set()) {
  const sent = [];
  return {
    sent,
    publisher: {
      async publish(row) {
        if (failVersions.has(row.payload.version)) {
          throw new Error('broker unreachable (simulated transient failure)');
        }
        sent.push(row.payload.version);
      },
    },
  };
}

async function seed(app, aggregateId, versions) {
  await app.query('BEGIN');
  await app.query(`SELECT set_config('app.tenant_id', $1, true)`, [tenantId]);
  for (const version of versions) {
    await app.query(
      `INSERT INTO "OutboxMessage"
         (id, "tenantId", topic, "eventType", "schemaVersion", "aggregateId", payload)
       VALUES ($1, $2, $3, 'event.snapshot', 1, $4, $5::jsonb)`,
      [
        `${aggregateId}_v${version}`,
        tenantId,
        TOPIC,
        aggregateId,
        JSON.stringify({ eventId: aggregateId, version }),
      ],
    );
  }
  await app.query('COMMIT');
}

/** Removes only the rows this run created. */
async function cleanup(app, prefix) {
  await app.query('BEGIN');
  await app.query(`SELECT set_config('app.tenant_id', $1, true)`, [tenantId]);
  await app.query(`DELETE FROM "OutboxMessage" WHERE "aggregateId" LIKE $1`, [`${prefix}%`]);
  await app.query('COMMIT');
}

async function setRow(app, id, sql) {
  await app.query('BEGIN');
  await app.query(`SELECT set_config('app.tenant_id', $1, true)`, [tenantId]);
  await app.query(`UPDATE "OutboxMessage" SET ${sql} WHERE id = $1`, [id]);
  await app.query('COMMIT');
}

async function main() {
  const app = new pg.Client({ connectionString: APP_URL });
  await app.connect();
  const pool = new pg.Pool({ connectionString: RELAY_URL, max: 2 });
  const opts = { batchSize: 100, maxAttempts: 5 };

  try {
    // -----------------------------------------------------------------------
    console.log('  a clean run publishes every version, in order (positive control)');
    // -----------------------------------------------------------------------
    /**
     * Without this, every assertion below is satisfied by a relay that publishes
     * NOTHING -- and "correctly held back" and "completely broken" look
     * identical from the outside.
     */
    const clean = `${run}_clean`;
    await seed(app, clean, [1, 2, 3]);
    const okRun = stubPublisher();
    await drainOnce(pool, okRun.publisher, logger, opts);
    check(
      JSON.stringify(okRun.sent) === JSON.stringify([1, 2, 3]),
      `published ${JSON.stringify(okRun.sent)} — expected [1,2,3]`,
    );
    await cleanup(app, clean);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  a failure inside a batch stops that aggregate, not the whole drain');
    // -----------------------------------------------------------------------
    const inBatch = `${run}_inbatch`;
    const other = `${run}_other`;
    await seed(app, inBatch, [1, 2, 3]);
    await seed(app, other, [4, 5]);

    const batchRun = stubPublisher(new Set([1]));
    await drainOnce(pool, batchRun.publisher, logger, opts);

    check(
      !batchRun.sent.includes(2) && !batchRun.sent.includes(3),
      `v1 failed, so v2 and v3 must not have gone ahead of it — sent ${JSON.stringify(batchRun.sent)}`,
    );
    check(
      JSON.stringify(batchRun.sent) === JSON.stringify([4, 5]),
      `the UNRELATED aggregate still published: ${JSON.stringify(batchRun.sent)} — expected [4,5]. ` +
        'A drain that stops entirely would be a different bug: one failing aggregate ' +
        'must not hold up the queue.',
    );
    await cleanup(app, inBatch);
    await cleanup(app, other);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  and a later drain cannot overtake a row that is backing off');
    // -----------------------------------------------------------------------
    /**
     * nextAttemptAt in the future is exactly the state a transient failure
     * leaves behind, so this sets up the precondition rather than faking the bug.
     */
    const backoff = `${run}_backoff`;
    await seed(app, backoff, [1, 2, 3]);
    await setRow(app, `${backoff}_v1`, `attempts = 1, "nextAttemptAt" = now() + interval '1 hour'`);

    const held = stubPublisher();
    await drainOnce(pool, held.publisher, logger, opts);
    check(
      held.sent.length === 0,
      held.sent.length === 0
        ? 'v1 is backing off, so nothing for that aggregate published'
        : `published ${JSON.stringify(held.sent)} while v1 was still waiting — this is the bug`,
    );

    await setRow(app, `${backoff}_v1`, `"nextAttemptAt" = NULL`);

    const after = stubPublisher();
    await drainOnce(pool, after.publisher, logger, opts);
    check(
      JSON.stringify(after.sent) === JSON.stringify([1, 2, 3]),
      `once the backoff expired: ${JSON.stringify(after.sent)} — expected [1,2,3]`,
    );
    await cleanup(app, backoff);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  but a dead-lettered row does not block its aggregate for ever');
    // -----------------------------------------------------------------------
    /**
     * A poison row is never going to publish. Blocking on it would trade a
     * reordering bug for a queue that stops, which is worse -- and the operator
     * already has an error log naming the dead letter. The ordering gap this
     * leaves is real, and it is the deliberate choice.
     */
    const dead = `${run}_dead`;
    await seed(app, dead, [1, 2, 3]);
    await setRow(app, `${dead}_v1`, `"deadLetteredAt" = now()`);

    const past = stubPublisher();
    await drainOnce(pool, past.publisher, logger, opts);
    check(
      JSON.stringify(past.sent) === JSON.stringify([2, 3]),
      `published ${JSON.stringify(past.sent)} — expected [2,3], past the dead letter`,
    );
    await cleanup(app, dead);
  } finally {
    await cleanup(app, run).catch(() => {});
    await app.end();
    await pool.end();
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('one aggregate reaches Kafka in the order it was written, failures included');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
