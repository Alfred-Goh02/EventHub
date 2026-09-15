/**
 * Asserts that the dead-letter topic keeps its dead letters, and that the setup
 * script can still say so on a cluster where the topics already exist.
 *
 * events.activity.dlq had no retention of its own, so it inherited the broker
 * default — log.retention.hours=168. The topic whose stated purpose is "keep
 * every failure for a human to look at" was deleting them after seven days. A
 * dead letter nobody happened to look at inside a week was simply gone, and
 * #42 had just finished routing MORE things into it.
 *
 * THE SECOND HALF IS WHY THE FIRST WENT UNNOTICED. create-topics.mjs only ever
 * created topics that were MISSING. On any cluster where they already existed
 * it printed "all topics already exist" and exited 0 — so every configEntry in
 * that file described what the topics were made with once, not what they are.
 * Adding retention.ms to the entry would have changed nothing, successfully.
 *
 * So this asserts the effective config on the BROKER, not the contents of the
 * script. A script that says the right thing and applies none of it is exactly
 * the failure being fixed, and reading the script back would reproduce it.
 *
 * Run: node scripts/verify-topic-configs.mjs
 */

import { Kafka } from 'kafkajs';
import kafkajs from 'kafkajs';

const TOPIC_RESOURCE = kafkajs.ConfigResourceTypes.TOPIC;
const BROKERS = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(',').map((b) => b.trim());

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}

/**
 * What each topic's shape has to be, and why it is not the default.
 *
 * Only the settings a deliberate decision was made about. Everything else is
 * the broker's business.
 */
const EXPECTED = [
  {
    topic: 'events.activity.dlq',
    name: 'retention.ms',
    value: '-1',
    why: 'a dead letter must outlive the week nobody looked at it',
  },
  {
    topic: 'events.activity',
    name: 'cleanup.policy',
    value: 'compact',
    why: 'the projection is rebuilt by replaying this topic from the beginning',
  },
  {
    topic: 'auth.email-requested',
    name: 'retention.ms',
    value: '86400000',
    why: 'a sealed link is dead thirty minutes after issue, so a day covers any outage worth waiting out',
  },
];

async function main() {
  const kafka = new Kafka({ clientId: 'verify-topic-configs', brokers: BROKERS });
  const admin = kafka.admin();
  await admin.connect();

  const topics = [...new Set(EXPECTED.map((e) => e.topic))];

  /**
   * THE VACUITY GUARD, and it has to come BEFORE the broker call.
   *
   * It is what the check further down was reaching for. That one compared
   * `described.resources.length` to `topics.length`, and those cannot differ: a
   * missing topic throws rather than coming back as a short list, and otherwise
   * describeConfigs returns one resource per resource asked about. It could not
   * fail, so it guarded nothing.
   *
   * The failure worth guarding is an EXPECTED list that is empty — then the loop
   * below runs zero times and the file reports success having asserted nothing.
   *
   * Placed above describeConfigs after watching the first version of this guard
   * fail to fire: with EXPECTED empty, `topics` is empty too, the broker call
   * throws "Resources array cannot be empty" first, and the catch below then
   * blamed a missing topic. A guard that runs after the thing it guards is not a
   * guard.
   */
  check(EXPECTED.length > 0, `${EXPECTED.length} config decision(s) to check`);
  if (EXPECTED.length === 0) {
    await admin.disconnect();
    console.error('\nFAIL: EXPECTED is empty, so there is nothing to assert about the broker');
    process.exitCode = 1;
    return;
  }

  /**
   * A missing topic THROWS here, and the error does not say which one.
   *
   * The comment that used to sit below this said describeConfigs returns an
   * empty resource for a topic that does not exist. It does not — measured
   * against this broker, asking about one real topic and one invented one threw
   *
   *   KafkaJSProtocolError: This server does not host this topic-partition
   *
   * naming neither the topic nor the thing that would create it. That is the
   * same unhelpful failure the README calls out for the relay's first publish,
   * and it is worth translating here rather than leaving a reader to guess.
   */
  let described;
  try {
    described = await admin.describeConfigs({
      includeSynonyms: false,
      resources: topics.map((name) => ({ type: TOPIC_RESOURCE, name })),
    });
  } catch (err) {
    await admin.disconnect();
    console.error(
      `\nFAIL: could not describe ${topics.join(', ')} — ${err.message}\n\n` +
        'That error means at least one of those topics does not exist on the broker, and it ' +
        'does not say which. The broker runs with KAFKA_AUTO_CREATE_TOPICS_ENABLE=false on ' +
        'purpose, so nothing creates them on demand: run `node scripts/create-topics.mjs`.',
    );
    process.exitCode = 1;
    return;
  }

  /**
   * And that the broker actually returned config, not merely a resource shell.
   * A topic described with no entries makes every assertion below read
   * "(absent)", which is a failure that says nothing about why.
   */
  const empty = described.resources.filter((r) => r.configEntries.length === 0);
  check(
    empty.length === 0,
    empty.length === 0
      ? `and the broker returned config for all ${described.resources.length} topic(s)`
      : `the broker returned NO config entries for ${empty
          .map((r) => r.resourceName)
          .join(', ')} — every assertion below would read (absent) and say nothing useful`,
  );

  for (const e of EXPECTED) {
    const resource = described.resources.find((r) => r.resourceName === e.topic);
    const entry = resource?.configEntries.find((c) => c.configName === e.name);

    check(
      entry?.configValue === e.value,
      entry?.configValue === e.value
        ? `${e.topic} ${e.name}=${e.value} — ${e.why}`
        : `${e.topic} ${e.name} is ${entry?.configValue ?? '(absent)'}, expected ${e.value}. ` +
          `${e.why}. Note the setup script reports success without applying config to ` +
          'topics that already exist, so "I ran create-topics" does not settle this.',
    );
  }

  /**
   * The DLQ's retention specifically must not be the broker default, and saying
   * "not the default" is a different assertion from "equals -1": it is the one
   * that would still catch the bug if somebody set retention.ms to a week
   * explicitly.
   */
  const dlq = described.resources.find((r) => r.resourceName === 'events.activity.dlq');
  const retention = dlq?.configEntries.find((c) => c.configName === 'retention.ms');
  check(
    retention?.configValue !== '604800000',
    retention?.configValue === '604800000'
      ? 'the DLQ is on the seven-day broker default — this is the original bug, exactly'
      : 'and it is not the seven-day default it used to inherit',
  );

  await admin.disconnect();

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('the topics on the broker match the decisions written down about them');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
