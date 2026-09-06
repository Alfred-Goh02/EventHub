/**
 * Asserts that events.activity actually contains what a rebuild would need.
 *
 * Four places in this repository claim it does:
 *
 *   create-topics.mjs        "a consumer can rebuild its entire projection by
 *                             replaying from the beginning — no HTTP bootstrap
 *                             path and no cold-start hole"
 *   register-schemas.mjs     "Ticketing rebuilds its projection by replaying
 *                             from offset 0"
 *   consumer.ts              "a new deployment or a wiped projection rebuilds
 *                             the whole projection"
 *   consume-events-activity  the same, in passing
 *
 * Nothing checked it. That claim is the entire justification for
 * cleanup.policy=compact, for the payload being a full SNAPSHOT rather than a
 * delta, and for there being no bootstrap endpoint anywhere in Ticketing. It is
 * also exactly what the relay ordering bug broke, and it broke it invisibly.
 *
 * WHY COMPACTION MAKES ORDER LOAD-BEARING. Compaction keeps the LAST message
 * per key and deletes the rest. So if v1 is published after v3 — which is what
 * a transient publish failure used to cause — then once the compactor runs, v3
 * is GONE and v1 is the only message left for that event. A rebuild then
 * produces v1 and no version guard can help, because the newer snapshot no
 * longer exists to be preferred. The live projection stays correct the whole
 * time, because its guard discarded the late v1 when it arrived. Only a rebuild
 * is wrong, and only later.
 *
 * That is the assertion below that matters most: for every key, the LAST
 * message must also be the HIGHEST-versioned one. Anything else is a projection
 * that will silently degrade the next time the compactor runs.
 *
 * DIRECTION MATTERS. This checks that every row the projection holds can be
 * reconstructed from the topic, not that every message on the topic reached the
 * projection. The second direction races the consumer: a message published
 * moments ago has not been applied yet, and that is normal rather than wrong.
 * The first direction cannot race — if the projection holds it, the message
 * that produced it must be on the topic.
 *
 * Run: node scripts/verify-projection-rebuild.mjs
 */

import pg from 'pg';
import { Kafka, logLevel } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';

const TICKETING_URL =
  process.env['TICKETING_APP_DATABASE_URL'] ??
  'postgresql://eventhub_ticketing_app:eventhub_ticketing_app@localhost:5432/eventhub_ticketing';
const BROKERS = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(',');
const REGISTRY = process.env['SCHEMA_REGISTRY_URL'] ?? 'http://localhost:8081';
const TOPIC = process.env['ACTIVITY_TOPIC'] ?? 'events.activity';

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}

/** Every message on the topic, in broker offset order, per partition. */
async function readTopic() {
  const kafka = new Kafka({
    clientId: 'verify-rebuild',
    brokers: BROKERS,
    logLevel: logLevel.ERROR,
  });
  const registry = new SchemaRegistry({ host: REGISTRY });
  const consumer = kafka.consumer({
    // A fresh group, so this always starts at offset 0 rather than resuming
    // someone else's committed position. Replaying from the beginning is the
    // thing under test.
    groupId: `rebuild-${process.pid}-${Math.random().toString(36).slice(2)}`,
  });

  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

  const messages = [];
  let undecodable = 0;

  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      let value;
      try {
        value = await registry.decode(message.value);
      } catch {
        // verify-dlq-malformed.mjs leaves a payload here that is not a Confluent
        // frame, deliberately. Without this catch it throws inside eachMessage,
        // kafkajs retries it with backoff, and the whole window goes on one
        // message that was published to be unreadable.
        undecodable += 1;
        return;
      }
      messages.push({
        partition,
        offset: Number(message.offset),
        key: message.key?.toString(),
        value,
      });
    },
  });

  await new Promise((r) => setTimeout(r, 7000));
  await consumer.disconnect();

  return { messages, undecodable };
}

async function main() {
  const { messages, undecodable } = await readTopic();

  // -------------------------------------------------------------------------
  console.log('  the replay has to have read something');
  // -------------------------------------------------------------------------
  /**
   * Every assertion below is of the form "for each projected row, the topic
   * must contain X". On an empty topic every one of them is vacuously true, and
   * this file would report that the rebuild claim holds while having replayed
   * nothing at all.
   */
  check(messages.length > 0, `replayed ${messages.length} decodable message(s) from ${TOPIC}`);
  if (messages.length === 0) {
    console.error('\nFAIL: nothing to rebuild from');
    process.exitCode = 1;
    return;
  }
  if (undecodable > 0) {
    console.log(
      `          (${undecodable} undecodable, which verify-dlq-malformed.mjs publishes on purpose)`,
    );
  }

  const byKey = new Map();
  for (const m of [...messages].sort((a, b) => a.offset - b.offset)) {
    const key = m.key ?? '(null key)';
    if (!byKey.has(key)) byKey.set(key, []);
    byKey.get(key).push(m);
  }

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  the message that survives compaction is the newest one');
  // -------------------------------------------------------------------------
  /**
   * THE ASSERTION THIS FILE EXISTS FOR.
   *
   * Compaction keeps the last message per key. If that is not also the highest
   * version, the compactor will delete the newer snapshot and leave the older
   * one as the event's permanent value — and a rebuild reads the older one. No
   * version guard can recover it, because by then the newer message is gone.
   */
  const wouldLose = [];
  for (const [key, msgs] of byKey) {
    const last = msgs[msgs.length - 1];
    const highest = msgs.reduce((a, b) => (b.value.version > a.value.version ? b : a));
    if (last.value.version < highest.value.version) {
      wouldLose.push(
        `${key}: compaction keeps v${last.value.version} (p${last.partition}/${last.offset}) ` +
          `and deletes v${highest.value.version} (p${highest.partition}/${highest.offset})`,
      );
    }
  }
  check(
    wouldLose.length === 0,
    wouldLose.length === 0
      ? `all ${byKey.size} key(s) end on their highest version`
      : `a rebuild would read stale state for ${wouldLose.length} event(s):\n        ` +
        wouldLose.join('\n        '),
  );

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  and every row the projection holds is reconstructible from the topic');
  // -------------------------------------------------------------------------
  const db = new pg.Client({ connectionString: TICKETING_URL });
  await db.connect();

  /**
   * Read across tenants by asking for each tenant in turn. EventProjection is
   * FORCE RLS and its policy reads app.tenant_id, so a single query with no
   * tenant set returns zero rows — silently, which would make the loop below
   * pass without examining anything. The count assertion catches that.
   */
  const tenants = [...new Set(messages.map((m) => m.value.tenantId).filter(Boolean))];
  const projected = [];

  for (const tenantId of tenants) {
    await db.query('BEGIN');
    await db.query(`SELECT set_config('app.tenant_id', $1, true)`, [tenantId]);
    const { rows } = await db.query(
      `SELECT id, "tenantId", title, slug, status, capacity, "organiserId",
              deleted, "sourceVersion"
         FROM "EventProjection"`,
    );
    await db.query('COMMIT');
    projected.push(...rows);
  }
  await db.end();

  check(
    projected.length > 0,
    projected.length > 0
      ? `read ${projected.length} projected row(s) across ${tenants.length} tenant(s)`
      : 'the projection is empty, so the comparison below would examine nothing',
  );

  const mismatches = [];
  const backwards = [];
  const notFromKafka = [];
  let reconstructible = 0;
  let fieldCompared = 0;

  for (const row of projected) {
    const msgs = byKey.get(row.id);

    /**
     * Reported, not failed.
     *
     * Thirteen verifier scripts INSERT straight into EventProjection to set up
     * a fixture -- 175 of 230 rows on a developer's database. Those never went
     * through Kafka, so no message can exist for them and demanding one would
     * be asserting something about the test suite rather than the system.
     *
     * It stays visible because the count moving for another reason would be
     * worth seeing, and because a reader deserves to know this file examined
     * a quarter of the table rather than all of it.
     */
    if (!msgs) {
      notFromKafka.push(row.id);
      continue;
    }

    // What a rebuild ends up holding: the highest version available. A replaying
    // consumer applies snapshots in whatever order they arrive and keeps the
    // highest, so the outcome is order-independent -- which is exactly why the
    // compaction assertion above is the one that matters. Order decides what
    // still EXISTS to be applied.
    const rebuilt = msgs.reduce((a, b) => (b.value.version > a.value.version ? b : a)).value;
    reconstructible += 1;

    if (Number(rebuilt.version) < Number(row.sourceVersion)) {
      backwards.push(
        `${row.id}: the projection is at v${row.sourceVersion} and the topic only reaches ` +
          `v${rebuilt.version} — a rebuild would go BACKWARDS`,
      );
      continue;
    }
    if (Number(rebuilt.version) > Number(row.sourceVersion)) {
      /**
       * Not a failure, and NOT a comparison either.
       *
       * The topic being ahead is ordinary: a message applied after this read,
       * or one the consumer rejected as a duplicate messageId — which is what
       * verify-projection-e2e.mjs deliberately leaves on the topic at
       * version + 100. There is nothing to compare against, because the
       * projection is not claiming to be at that version.
       *
       * It is counted rather than ignored. The field assertion below is "for
       * each row at the topic's version, the fields agree", and every row that
       * lands here is a row it did not look at — so without `fieldCompared`,
       * a topic that had run ahead of every row would report agreement having
       * compared nothing. That is how the first draft of this file behaved, and
       * two deliberately corrupted rows failed to make it go red.
       */
      continue;
    }

    fieldCompared += 1;

    for (const [field, mine, theirs] of [
      ['title', row.title, rebuilt.title],
      ['slug', row.slug, rebuilt.slug],
      ['status', row.status, rebuilt.status],
      ['capacity', row.capacity, rebuilt.capacity],
      ['organiserId', row.organiserId, rebuilt.organiserId],
      ['deleted', row.deleted, rebuilt.deleted],
    ]) {
      if (String(mine) !== String(theirs)) {
        mismatches.push(
          `${row.id} v${row.sourceVersion}: ${field} is ${mine} here, ${theirs} on the topic`,
        );
      }
    }
  }

  /**
   * The positive control for the two assertions after it. Both are "for each
   * row that came from Kafka, ..." -- and on a projection made entirely of
   * directly-inserted fixtures there are no such rows and both pass having
   * compared nothing.
   */
  check(
    reconstructible > 0,
    `${reconstructible} of ${projected.length} projected row(s) came from the topic and were compared`,
  );
  if (notFromKafka.length > 0) {
    console.log(
      `          (${notFromKafka.length} inserted directly by other verifiers as fixtures, ` +
        'never published)',
    );
  }

  check(
    backwards.length === 0,
    backwards.length === 0
      ? 'the topic reaches at least the version the projection holds'
      : `${backwards.length} row(s) would rebuild to an older state:\n        ${backwards.join('\n        ')}`,
  );
  check(
    fieldCompared > 0,
    fieldCompared > 0
      ? `${fieldCompared} row(s) sit at the topic's newest version and had their fields compared`
      : 'no row sits at the version the topic reaches, so the field check below compared nothing',
  );
  check(
    mismatches.length === 0,
    mismatches.length === 0
      ? `and across those ${fieldCompared} row(s) every field agrees`
      : `${mismatches.length} field(s) disagree:\n        ${mismatches.join('\n        ')}`,
  );

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('the topic carries everything the projection would need to rebuild itself');
}

main()
  .then(() => process.exit(process.exitCode ?? 0))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
