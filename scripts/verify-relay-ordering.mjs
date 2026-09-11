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
    /**
     * Only what this run put there.
     *
     * The drain takes whatever is pending, and a developer outbox holds rows
     * from earlier runs and from other verifiers. Asserting on everything the
     * drain published made these checks depend on how clean the database
     * happened to be -- which surfaced as one run of this file failing its own
     * positive control on rows a previous run had left behind.
     */
    of: (aggregateId) => sent.filter((m) => m.aggregateId === aggregateId).map((m) => m.version),
    sent,
    publisher: {
      async publish(row) {
        if (failVersions.has(row.payload.version)) {
          throw new Error('broker unreachable (simulated transient failure)');
        }
        sent.push({ aggregateId: row.aggregateId, version: row.payload.version });
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

  /**
   * REFUSE TO RUN ON A NON-EMPTY OUTBOX.
   *
   * Every drain in this file claims whatever is pending -- not just this run's
   * rows -- and hands it to a stub publisher that records it and reports
   * success. drainOnce then marks it published. So a real pending message,
   * left by the Event service or an earlier CI step, would be marked
   * delivered without ever reaching Kafka: silently lost, by a test.
   *
   * The stub cannot simply decline foreign rows: a thrown error is a failed
   * publish, which increments attempts, sets a backoff, and -- since #61 --
   * holds that aggregate back. Not running is the only harmless option.
   *
   * The relay role sees every tenant, so this count is the whole outbox.
   */
  const {
    rows: [pending],
  } = await pool.query(
    `SELECT count(*)::int AS n FROM "OutboxMessage" WHERE "publishedAt" IS NULL AND "deadLetteredAt" IS NULL`,
  );
  if (pending.n > 0) {
    console.error(
      `REFUSING TO RUN: ${pending.n} undelivered outbox row(s) already pending. This file ` +
        'drains with a stub publisher, so it would mark them published without sending them. ' +
        'Run the real relay (node services/relay/dist/index.js --once) first, then re-run this.',
    );
    await app.end();
    await pool.end();
    process.exitCode = 1;
    return;
  }

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
      JSON.stringify(okRun.of(clean)) === JSON.stringify([1, 2, 3]),
      `published ${JSON.stringify(okRun.of(clean))} — expected [1,2,3]`,
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
      !batchRun.of(inBatch).includes(2) && !batchRun.of(inBatch).includes(3),
      `v1 failed, so v2 and v3 must not have gone ahead of it — sent ${JSON.stringify(batchRun.of(inBatch))}`,
    );
    check(
      JSON.stringify(batchRun.of(other)) === JSON.stringify([4, 5]),
      `the UNRELATED aggregate still published: ${JSON.stringify(batchRun.of(other))} — expected [4,5]. ` +
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
      held.of(backoff).length === 0,
      held.of(backoff).length === 0
        ? 'v1 is backing off, so nothing for that aggregate published'
        : `published ${JSON.stringify(held.of(backoff))} while v1 was still waiting — this is the bug`,
    );

    await setRow(app, `${backoff}_v1`, `"nextAttemptAt" = NULL`);

    const after = stubPublisher();
    await drainOnce(pool, after.publisher, logger, opts);
    check(
      JSON.stringify(after.of(backoff)) === JSON.stringify([1, 2, 3]),
      `once the backoff expired: ${JSON.stringify(after.of(backoff))} — expected [1,2,3]`,
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
      JSON.stringify(past.of(dead)) === JSON.stringify([2, 3]),
      `published ${JSON.stringify(past.of(dead))} — expected [2,3], past the dead letter`,
    );
    await cleanup(app, dead);
    // -----------------------------------------------------------------------
    console.log('');
    console.log('  and a second relay cannot slip past one that is mid-publish');
    // -----------------------------------------------------------------------
    /**
     * The case SKIP LOCKED creates rather than solves.
     *
     * SKIP LOCKED hides a row another relay is holding, which is the point --
     * but it hides it from the ORDER BY too. Relay A claims seq 1 and stalls
     * inside the Kafka round-trip that drainOnce deliberately holds its
     * transaction across. Seq 2 and 3 commit meanwhile. Relay B drains, cannot
     * see seq 1, and publishes 2 and 3 ahead of it.
     *
     * Measured against this schema before the fix: [2,3,1].
     *
     * The drain used to call this out and accept it -- "running one relay is
     * still the requirement". But the thing that causes it is the forgotten
     * `tsx watch` its own comment names, and the consequence on a compacted
     * topic is a projection that rebuilds to stale state. A configuration
     * mistake should not silently corrupt data.
     *
     * NO TIMERS. A gate the test resolves by hand, so the race is arranged
     * rather than hoped for -- a sleep long enough to be reliable in CI is a
     * sleep, and a sleep short enough to be quick is a flake.
     */
    const race = `${run}_race`;
    await seed(app, race, [1]);

    let release;
    const gate = new Promise((r) => { release = r; });
    let entered;
    const insidePublish = new Promise((r) => { entered = r; });

    const relayA = [];
    const slow = {
      async publish(row) {
        if (row.aggregateId === race) {
          entered();
          await gate;
        }
        relayA.push({ aggregateId: row.aggregateId, version: row.payload.version });
      },
    };

    // Separate pools, so the two drains are separate SESSIONS. Advisory locks
    // are held per session, and one session re-taking its own lock succeeds --
    // which would make this test pass against the broken code.
    const poolA = new pg.Pool({ connectionString: RELAY_URL, max: 2 });
    const poolB = new pg.Pool({ connectionString: RELAY_URL, max: 2 });

    try {
      const aDrain = drainOnce(poolA, slow, logger, opts);
      await insidePublish;

      // Now the later versions land, while A still holds seq 1.
      await seed(app, race, [2, 3]);

      // And an unrelated aggregate, so "B published nothing of ours" can be
      // told apart from "B published nothing at all".
      const bystander = `${run}_bystander`;
      await seed(app, bystander, [1, 2]);

      const relayB = [];
      const fast = {
        async publish(row) {
          relayB.push({ aggregateId: row.aggregateId, version: row.payload.version });
        },
      };
      await drainOnce(poolB, fast, logger, opts);

      const bTookOurs = relayB.filter((m) => m.aggregateId === race).map((m) => m.version);
      check(
        bTookOurs.length === 0,
        bTookOurs.length === 0
          ? 'the second relay published nothing for an aggregate the first was holding'
          : `the second relay published ${JSON.stringify(bTookOurs)} while the first still had ` +
            'v1 in flight — those arrive on the topic before v1 does',
      );

      /**
       * And it did not simply stop.
       *
       * Blocking one aggregate must not block the queue, or the fix trades a
       * silent corruption for a stalled relay -- the same trade the dead-letter
       * case above refuses.
       *
       * This needs a SEPARATE aggregate to be meaningful. The first version
       * asserted `relayB.length >= 0 && bTookOurs.length === 0`, which is the
       * assertion above with a tautology bolted on: it printed
       * "0 message(s) from other aggregates" and called that a pass.
       */
      const bystanderSent = relayB.filter((m) => m.aggregateId === bystander).map((m) => m.version);
      check(
        JSON.stringify(bystanderSent) === JSON.stringify([1, 2]),
        `an unrelated aggregate still drained in the same pass: ${JSON.stringify(bystanderSent)} ` +
          '— expected [1,2]. One aggregate being held must not stop the queue.',
      );

      release();
      await aDrain;

      // Everything held back goes out now, behind what preceded it.
      const after = [];
      await drainOnce(poolB, {
        async publish(row) {
          after.push({ aggregateId: row.aggregateId, version: row.payload.version });
        },
      }, logger, opts);

      const order = [
        ...relayA.filter((m) => m.aggregateId === race).map((m) => m.version),
        ...after.filter((m) => m.aggregateId === race).map((m) => m.version),
      ];
      check(
        JSON.stringify(order) === JSON.stringify([1, 2, 3]),
        `the aggregate reached Kafka as ${JSON.stringify(order)} — expected [1,2,3]`,
      );
    } finally {
      await poolA.end();
      await poolB.end();
    }
    await cleanup(app, race);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  and a batch goes out in NUMERIC seq order, even across a power of ten');
    // -----------------------------------------------------------------------
    /**
     * The claim returns seq as text (seq::text AS "seq", since a bigint does
     * not fit a JS number safely), and the CTE's outer query ended in a bare
     * ORDER BY seq. Postgres resolves a bare name in ORDER BY to the OUTPUT
     * column first -- so that sorted the text, and '10' comes before '9'.
     *
     * CI caught it through verify-projection-rebuild.mjs on a fresh broker:
     *   compaction keeps v2 (p0/7) and deletes v3 (p0/1)
     * Every other assertion here missed it. On a warm database the seq values
     * one test seeds are consecutive and almost never straddle a digit
     * boundary, so text order and numeric order agree by luck.
     *
     * EXPLICIT seq values, not a moved sequence. The first version of this case
     * pushed the outbox sequence up to just below the next power of ten. That
     * works once and is a landmine after: every run multiplies seq by ten, so
     * about sixteen local runs would exhaust the bigint and break every outbox
     * insert. Nothing is unique on seq and the claim only reads unpublished
     * rows, so rows at 9, 10 and 11 sort ahead of anything real, always land
     * inside the batch, and leave the sequence alone.
     */
    const straddle = `${run}_straddle`;
    await app.query('BEGIN');
    await app.query(`SELECT set_config('app.tenant_id', $1, true)`, [tenantId]);
    for (const [version, seq] of [[1, 9], [2, 10], [3, 11]]) {
      await app.query(
        `INSERT INTO "OutboxMessage"
           (id, "tenantId", topic, "eventType", "schemaVersion", "aggregateId", payload, seq)
         VALUES ($1, $2, $3, 'event.snapshot', 1, $4, $5::jsonb, $6)`,
        [`${straddle}_v${version}`, tenantId, TOPIC, straddle, JSON.stringify({ eventId: straddle, version }), seq],
      );
    }
    const { rows: seeded } = await app.query(
      `SELECT seq::text AS s FROM "OutboxMessage" WHERE "aggregateId" = $1`,
      [straddle],
    );
    await app.query('COMMIT');

    /**
     * The positive control: the rows really sit on seqs of different lengths.
     * If the explicit values were ignored -- a column default that won, an
     * identity column someone introduces later -- the three would be
     * consecutive same-length numbers, text and numeric order would agree, and
     * the assertion below would pass for exactly the reason it exists to catch.
     */
    const lengths = new Set(seeded.map((r) => r.s.length));
    check(
      seeded.length === 3 && lengths.size > 1,
      `the three versions sit on seq ${seeded.map((r) => r.s).sort((a, b) => Number(a) - Number(b)).join(', ')}` +
        (lengths.size > 1 ? ', across a power of ten' : ' -- NOT across a power of ten, so this proves nothing'),
    );

    const numeric = stubPublisher();
    await drainOnce(pool, numeric.publisher, logger, opts);
    check(
      JSON.stringify(numeric.of(straddle)) === JSON.stringify([1, 2, 3]),
      `published ${JSON.stringify(numeric.of(straddle))} — expected [1,2,3]; sorting seq as text gives [2,3,1]`,
    );
    await cleanup(app, straddle);
    await cleanup(app, `${run}_bystander`);

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
