import { randomUUID } from 'node:crypto';
import { Kafka, logLevel, type KafkaMessage } from 'kafkajs';
import { z } from 'zod';
import { DLQ_HEADERS, DLQ_TOPIC, header, originalHeaders } from '../dispatch/retry.js';

/**
 * Replays one dead-lettered delivery onto the topic it came from, by message id.
 *
 *   pnpm --filter @eventhub/notification run dlq:replay -- <message-id>
 *
 * MANUAL AND DELIBERATE. Nothing replays the dead-letter topic automatically: the
 * usual reason a message is there is a bug that has to be fixed first, and an
 * automatic replay of a message that fails for a reason nobody fixed is a loop.
 *
 * SAFE TO REPEAT. The replayed message is the original, message id included, so
 * the sent-log claim sees a delivery already recorded SENT and sends nothing. An
 * expired reset or invitation is skipped as expired.
 *
 * SEALED REQUESTS ONLY: a password reset or an invitation. Only a sealed value is kept
 * in the dead-letter topic; a reply's copy has none, and this says so rather than
 * replaying an empty message. A replayed invitation that Auth has since revoked is
 * still sent if it has not expired -- Notification cannot see a revocation -- and its
 * link is refused by /register.
 *
 * Reads KAFKA_BROKERS and nothing else. It does not import the service's
 * configuration, which requires a database and an SMTP server this has no use for.
 */

const { KAFKA_BROKERS } = z.object({ KAFKA_BROKERS: z.string().min(1) }).parse(process.env);

async function main(): Promise<void> {
  // `pnpm run <script> -- <id>` hands this script '--' as its first argument in every
  // shell but PowerShell, where pnpm.ps1 swallows it. Without this the documented
  // command looks for a message whose id is "--" and reports it is not in the topic.
  const rest = process.argv.slice(2);
  if (rest[0] === '--') rest.shift();
  const messageId = rest[0];
  if (!messageId) {
    console.error('usage: dlq-replay <message-id>');
    process.exitCode = 2;
    return;
  }

  const kafka = new Kafka({ clientId: 'notification-dlq-replay', brokers: KAFKA_BROKERS.split(',').map((b) => b.trim()), logLevel: logLevel.NOTHING });
  const admin = kafka.admin();
  await admin.connect();

  // Read the whole topic as it stands now, and no further.
  const ends = new Map((await admin.fetchTopicOffsets(DLQ_TOPIC)).map((p) => [p.partition, BigInt(p.high)]));
  const remaining = new Set([...ends].filter(([, high]) => high > 0n).map(([partition]) => partition));

  const matches: KafkaMessage[] = [];
  const groupId = `notification-dlq-replay-${randomUUID()}`;
  if (remaining.size > 0) {
    const consumer = kafka.consumer({ groupId });
    await consumer.connect();
    await consumer.subscribe({ topic: DLQ_TOPIC, fromBeginning: true });
    const done = new Promise<void>((resolve) => {
      void consumer.run({
        eachMessage: async ({ partition, message }) => {
          if (header(message.headers, 'message-id') === messageId) matches.push(message);
          if (BigInt(message.offset) + 1n >= (ends.get(partition) ?? 0n)) {
            remaining.delete(partition);
            if (remaining.size === 0) resolve();
          }
        },
      });
    });
    await done;
    await consumer.disconnect();
    await admin.deleteGroups([groupId]).catch(() => {});
  }

  if (matches.length === 0) {
    console.error(`${messageId} is not in ${DLQ_TOPIC}`);
    process.exitCode = 1;
    await admin.disconnect();
    return;
  }

  const latest = matches[matches.length - 1]!;
  const topic = header(latest.headers, DLQ_HEADERS.originalTopic);
  if (!topic) {
    console.error(`${messageId} was dead-lettered without ${DLQ_HEADERS.originalTopic}, so there is nowhere to replay it`);
    process.exitCode = 1;
    await admin.disconnect();
    return;
  }

  // A reply's dead-letter copy keeps its headers and not its value (see forward() in
  // dispatch/consumer.ts). Replaying nothing would print "replayed" and send nothing, so
  // say what there is instead.
  if (latest.value === null || header(latest.headers, DLQ_HEADERS.valueWithheld) === 'true') {
    console.error(
      `${messageId} was dead-lettered without its value, which was not sealed: nothing here to replay. ` +
        `The original is on ${topic} under the same message id for as long as that topic keeps it ` +
        `(dead-lettered for: ${header(latest.headers, DLQ_HEADERS.reason) ?? 'no reason recorded'})`,
    );
    process.exitCode = 1;
    await admin.disconnect();
    return;
  }

  const producer = kafka.producer({ idempotent: true, maxInFlightRequests: 1 });
  await producer.connect();
  await producer.send({
    topic,
    messages: [{ key: latest.key, value: latest.value, headers: originalHeaders(latest.headers) }],
  });
  await producer.disconnect();
  await admin.disconnect();
  console.log(
    `replayed ${messageId} to ${topic} (dead-lettered for: ${header(latest.headers, DLQ_HEADERS.reason) ?? 'no reason recorded'})`,
  );
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
