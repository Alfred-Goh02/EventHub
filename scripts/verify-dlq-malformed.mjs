/**
 * Asserts that a message the consumer cannot use is KEPT, not dropped.
 *
 * There were two ways for a message to be unprocessable, and only one of them
 * left a trace.
 *
 *   UNDECODABLE      registry.decode() throws. Dead-lettered, offset committed,
 *                    partition unblocked. Correct.
 *
 *   NO IDENTITY      decodes cleanly and carries no messageId, tenantId or
 *                    eventId. Logged at ERROR and then simply RETURNED — while
 *                    the caller went on to commit the offset. The message was
 *                    gone, and the only evidence was a log line nobody greps.
 *
 * Those are the same situation. Both are permanently unprocessable, both would
 * block the partition forever if retried, and neither is the consumer's fault.
 * The difference was only in how visible the loss was.
 *
 * The second is the sharper case, because it cannot be recorded in
 * ProcessedMessage either: every table in this database is FORCE ROW LEVEL
 * SECURITY and tenant-scoped, and the whole problem is that the message has no
 * tenant. There is nowhere in Postgres to write down that it happened. The
 * dead-letter topic is the only place it can go.
 *
 * `eventId` is a plain Avro `string`, so an empty one is a perfectly VALID
 * message on the wire. That is what makes this reachable rather than
 * theoretical: nothing in the contract stops a producer emitting it, and it
 * decodes without complaint.
 *
 * The undecodable case is asserted alongside it as a positive control. If a
 * plain garbage payload did not reach the dead-letter topic either, this test
 * would be measuring a broken producer rather than the consumer's behaviour.
 *
 * Run: node scripts/verify-dlq-malformed.mjs
 */

import { Kafka, logLevel } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';

const BROKERS = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(',').map((b) => b.trim());
const REGISTRY = process.env['SCHEMA_REGISTRY_URL'] ?? 'http://localhost:8081';

const TOPIC = 'events.activity';
const DLQ = 'events.activity.dlq';

const run = `dlq_${Date.now()}`;

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * A snapshot that satisfies the Avro schema in every respect EXCEPT that its
 * identity fields are empty strings — which the schema permits, because they
 * are plain `string` rather than anything narrower.
 */
function snapshot(overrides) {
  const now = new Date().toISOString();
  return {
    messageId: `${run}_msg`,
    occurredAt: now,
    eventId: `${run}_evt`,
    tenantId: 'tenant_acme',
    title: 'Malformed identity probe',
    slug: `malformed-${run}`,
    description: null,
    status: 'PUBLISHED',
    capacity: 10,
    startsAt: now,
    endsAt: now,
    organiserId: 'u_test',
    version: 1,
    changeType: 'UPDATED',
    deleted: false,
    ...overrides,
  };
}

async function main() {
  const kafka = new Kafka({ clientId: 'verify-dlq', brokers: BROKERS, logLevel: logLevel.ERROR });
  const registry = new SchemaRegistry({ host: REGISTRY });
  const producer = kafka.producer();
  await producer.connect();

  const schemaId = await registry.getLatestSchemaId(`${TOPIC}-value`);

  /**
   * Read the dead-letter topic from the END before producing anything, so the
   * assertion is about what THIS run put there rather than about the accumulated
   * history of every previous run. The DLQ is deliberately not compacted.
   */
  const consumer = kafka.consumer({ groupId: `${run}_dlq_reader` });
  await consumer.connect();
  await consumer.subscribe({ topic: DLQ, fromBeginning: false });

  const seen = [];
  await consumer.run({
    eachMessage: async ({ message }) => {
      const headers = Object.fromEntries(
        Object.entries(message.headers ?? {}).map(([k, v]) => [k, v?.toString()]),
      );
      seen.push({ key: message.key?.toString(), headers });
    },
  });

  // kafkajs joins the group and assigns before it will deliver anything; without
  // this the first produce races the assignment and the message is missed.
  await sleep(4000);

  console.log('  a message that decodes cleanly but carries no eventId');

  const emptyIdentity = await registry.encode(schemaId, snapshot({ eventId: '' }));
  await producer.send({
    topic: TOPIC,
    messages: [{ key: `${run}_empty`, value: emptyIdentity }],
  });

  console.log('  and a payload that is not Avro at all (positive control)');

  await producer.send({
    topic: TOPIC,
    messages: [{ key: `${run}_garbage`, value: Buffer.from('this is not a Confluent frame') }],
  });

  // Give the projection consumer time to read both and produce to the DLQ.
  for (let i = 0; i < 30 && seen.length < 2; i += 1) await sleep(1000);

  await consumer.disconnect();
  await producer.disconnect();

  console.log('');

  const empty = seen.find((m) => m.key === `${run}_empty`);
  const garbage = seen.find((m) => m.key === `${run}_garbage`);

  check(
    garbage !== undefined,
    garbage
      ? 'the undecodable message reached the dead-letter topic (control)'
      : 'the undecodable message did NOT reach the dead-letter topic — the consumer is not ' +
        'running, or is not reading this topic, so nothing below proves anything',
  );

  check(
    empty !== undefined,
    empty
      ? 'the message with no eventId reached the dead-letter topic'
      : 'the message with no eventId was DROPPED — it decoded, failed the identity check, ' +
        'and vanished with the offset committed behind it',
  );

  if (empty) {
    check(
      /missing required identity fields/.test(empty.headers['dlq-reason'] ?? ''),
      `dlq-reason names the problem: ${JSON.stringify(empty.headers['dlq-reason'])}`,
    );
    check(
      empty.headers['dlq-source-topic'] === TOPIC && empty.headers['dlq-source-offset'] !== undefined,
      `it records where it came from (${empty.headers['dlq-source-topic']} offset ` +
        `${empty.headers['dlq-source-offset']})`,
    );
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exit(1);
  }
  console.log('a message the consumer cannot use is parked where an operator can find it');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
