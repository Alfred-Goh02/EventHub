/**
 * Reads events.activity back and asserts, on the wire, the properties the
 * Ticketing projection's safety rests on.
 *
 * This is the consumer side of the contract, and it proves something the
 * producer side cannot: that a message encoded by the relay is decodable by
 * someone who was not involved in encoding it. It resolves the writer's schema
 * from the id in the wire format, exactly as the Ticketing projection does.
 *
 * IT USED TO ONLY PRINT. Nothing invoked it, every line was informational, it
 * exited 0 whatever it found -- and one of its three checks could not fail at
 * all. The display loop above it called seen.sort(), which sorts IN PLACE, so
 * the ordering check was asking whether a list sorted by version was in version
 * order. It answered "versions arrived in order: yes" for two chapters, and
 * would have answered yes to 3, 1, 2.
 *
 * The other two were scoped to the whole topic rather than to a KEY. Kafka
 * orders within a PARTITION and routes by hashing the KEY, so "all messages on
 * one partition" is a property of a topic holding one event, not of a correct
 * system: it goes red on any topic carrying several. Both are per key now.
 *
 * Run: node scripts/consume-events-activity.mjs
 *
 * ACTIVITY_TOPIC overrides the topic, which is how the checks get watched fail:
 * events.activity is compacted and kept forever, so a deliberately broken
 * message published to prove an assertion works would poison every later run.
 */

import { Kafka, logLevel } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';

const BROKERS = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(',');
const REGISTRY = process.env['SCHEMA_REGISTRY_URL'] ?? 'http://localhost:8081';
/**
 * Overridable so the checks below can be WATCHED FAIL.
 *
 * events.activity is compacted with no retention limit, so a deliberately
 * broken message published to it to prove an assertion works would stay there
 * for good and fail every later run. The negative control points this at a
 * throwaway topic instead.
 */
const TOPIC = process.env['ACTIVITY_TOPIC'] ?? 'events.activity';

const kafka = new Kafka({ clientId: 'events-activity-reader', brokers: BROKERS, logLevel: logLevel.ERROR });
const registry = new SchemaRegistry({ host: REGISTRY });

async function main() {
  // A fresh group id, so this always replays from the beginning rather than
  // resuming someone else's committed offset. Replaying from earliest is
  // exactly what a projection rebuild does, and it only works because the topic
  // is compacted and every message is a full snapshot.
  const consumer = kafka.consumer({ groupId: `reader-${process.pid}-${Math.random().toString(36).slice(2)}` });
  await consumer.connect();
  await consumer.subscribe({ topic: TOPIC, fromBeginning: true });

  const seen = [];
  await consumer.run({
    eachMessage: async ({ partition, message }) => {
      // The first byte is 0x00 and the next four are the schema id. decode()
      // reads that id, fetches the WRITER's schema, and resolves it against the
      // reader's — which is why a plain avsc encode without the prefix would be
      // undecodable here.
      const value = await registry.decode(message.value);
      seen.push({
        partition,
        // The offset is what makes the ordering check below mean anything:
        // it is the position the broker assigned, not the order this array
        // happens to be in after something else has sorted it.
        offset: Number(message.offset),
        key: message.key?.toString(),
        headerId: message.headers?.['message-id']?.toString(),
        value,
      });
    },
  });

  await new Promise((r) => setTimeout(r, 6000));
  await consumer.disconnect();

  if (seen.length === 0) {
    console.error('no messages on events.activity');
    process.exit(1);
  }

  console.log(`decoded ${seen.length} message(s) from ${TOPIC}\n`);
  // A COPY. sort() sorts in place, and the ordering check below reads this
  // same array -- so sorting it here to make the log readable was quietly
  // sorting the evidence. See the comment on that check.
  const byVersion = [...seen].sort((a, b) => a.value.version - b.value.version);

  for (const m of byVersion) {
    console.log(
      `  p${m.partition}  ${String(m.value.changeType).padEnd(9)} v${m.value.version}  ` +
        `status=${String(m.value.status).padEnd(9)} capacity=${String(m.value.capacity).padEnd(4)} ` +
        `deleted=${m.value.deleted}  tenant=${m.value.tenantId}`,
    );
  }

  /**
   * Everything below is PER KEY, and both checks used to be scoped wider than
   * the guarantee they were testing.
   *
   * Kafka orders messages within a PARTITION, and routes by hashing the KEY, so
   * what the design actually rests on is: one aggregate keys to one partition,
   * therefore that aggregate's own updates cannot overtake each other. A topic
   * holds many keys, and one partition holds many keys, so any check that spans
   * keys is reading unrelated events as if they were a sequence.
   *
   * On a topic with a single event's updates on it -- which is how this was run
   * -- the two scopes coincide, which is why the wider scope looked right.
   */
  const byKey = new Map();
  for (const m of [...seen].sort((a, b) => a.offset - b.offset)) {
    const key = m.key ?? '(null key)';
    if (!byKey.has(key)) byKey.set(key, []);
    byKey.get(key).push(m);
  }

  /**
   * THE ORDERING CHECK USED TO BE UNABLE TO FAIL.
   *
   * It read seen.map(...), and the display loop above had already called
   * seen.sort() -- which sorts IN PLACE. So it was asking whether a list sorted
   * by version was in version order. Hand it 3, 1, 2 and it printed
   * "versions arrived in order: yes", which is what it printed for two chapters.
   *
   * Offset order, because that is the order the BROKER assigned. Arrival order
   * in `seen` is the order this process happened to decode them in, and
   * kafkajs runs partitions concurrently.
   */
  const outOfOrder = [];
  for (const [key, messages] of byKey) {
    for (let i = 1; i < messages.length; i += 1) {
      if (messages[i].value.version < messages[i - 1].value.version) {
        outOfOrder.push(
          `${key.slice(0, 8)}: v${messages[i - 1].value.version} then v${messages[i].value.version}`,
        );
      }
    }
  }

  /**
   * And the partition check. "All messages on one partition" was true only
   * because the topic held one event; it says nothing on a topic with several,
   * and would fail on a healthy one. The guarantee is that each KEY sticks to a
   * partition -- if one ever spanned two, its updates would be ordered against
   * each other by nothing at all.
   */
  const split = [];
  for (const [key, messages] of byKey) {
    const partitions = new Set(messages.map((m) => m.partition));
    if (partitions.size > 1) split.push(`${key.slice(0, 8)} on p${[...partitions].join(", p")}`);
  }

  console.log('');
  console.log(`  ${byKey.size} key(s) across ${new Set(seen.map((m) => m.partition)).size} partition(s)`);
  console.log('');

  /**
   * What each message claims to be, from the two places it says so.
   *
   * messageId lives in the PAYLOAD (a required field on the schema) and again in
   * a HEADER. The payload one is what services/ticketing/src/consumer.ts dedupes
   * on; the header exists so a consumer could dedupe without paying to
   * deserialise a message it is about to discard. No consumer takes that option
   * yet, which is exactly why the two can drift without anything noticing.
   */
  const payloadIds = seen.filter((m) => !m.value.messageId);
  const disagreeing = seen.filter((m) => m.headerId && m.headerId !== m.value.messageId);
  const headerless = seen.filter((m) => !m.headerId).length;

  /**
   * Each of these is a claim this script exists to make, so a failed one is a
   * failed run. They all used to print and exit 0 -- a demonstration that says
   * NO and then reports success is telling you the same nothing as one that
   * cannot say NO at all.
   */
  const properties = [
    {
      label: 'every message carries a payload messageId',
      ok: payloadIds.length === 0,
      detail: `${payloadIds.length} of ${seen.length} without one; the consumer drops those to the DLQ`,
    },
    {
      label: 'header messageId agrees with the payload',
      ok: disagreeing.length === 0,
      detail:
        `${disagreeing.length} disagree, e.g. header ${disagreeing[0]?.headerId} vs payload ` +
        `${disagreeing[0]?.value.messageId} -- a consumer deduping on the header and one deduping ` +
        'on the payload would reach different answers about the same message',
    },
    {
      label: 'each key stays on one partition',
      ok: split.length === 0,
      detail: `spans partitions: ${split.join('; ')} -- that aggregate's updates are ordered by nothing`,
    },
    {
      label: 'versions never go backwards within a key',
      ok: outOfOrder.length === 0,
      detail: `went backwards at ${outOfOrder.join('; ')} -- this reverts the projection`,
    },
  ];

  for (const prop of properties) {
    console.log(`  ${prop.label.padEnd(44)}${prop.ok ? 'yes' : 'NO'}`);
  }

  /**
   * Reported, not asserted. A message with no header is not wrong: the header is
   * an optimisation, and scripts/verify-projection-e2e.mjs deliberately publishes
   * without one because it is exercising the payload path. Failing on it would be
   * failing on correct traffic. But a sudden change in the count is worth seeing,
   * so it prints.
   */
  if (headerless > 0) {
    console.log('');
    console.log(`  (${headerless} of ${seen.length} carry no message-id header -- not a failure;`);
    console.log('   the header is an optimisation and the e2e replay omits it on purpose)');
  }

  const failed = properties.filter((prop) => !prop.ok);
  if (failed.length > 0) {
    console.error('');
    for (const prop of failed) console.error(`  FAIL  ${prop.label}\n        ${prop.detail}`);
    process.exitCode = 1;
  }
}

/**
   process.exit(0) used to be here, and it would have swallowed every failure
   above -- an explicit exit code beats whatever process.exitCode was set to.
   A check that reports a failure the process then discards is the same hollow
   assertion in a different place.

   The exit is still explicit because kafkajs keeps its sockets open long
   enough to hang the process after disconnect(), so waiting for the event
   loop to drain is not an option. It just has to carry the code through.
 */
main()
  .then(() => process.exit(process.exitCode ?? 0))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
