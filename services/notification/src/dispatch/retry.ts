import type { IHeaders } from 'kafkajs';

/**
 * Where a delivery goes when it cannot be completed now: a delayed retry tier, or
 * the dead-letter topic.
 *
 * KAFKA HAS NO DELAYED DELIVERY, so a delay is a topic. A send that failed for a
 * reason that may pass moves to the next tier, stamped with the time it may be
 * tried again, and its offset on the topic it came from is committed. The partition
 * it was read from moves on, so one address that is having trouble does not hold up
 * every user whose messages share its partition.
 *
 * THE TIERS END INSIDE THE LINK'S LIFE. A reset link lives thirty minutes, so the
 * defaults (30 s, 5 min, 15 min) are spent before it dies; a tier past that could
 * only ever deliver a dead link.
 *
 * WHAT NEVER SUCCEEDS goes to the dead-letter topic on the first attempt: poison,
 * whose failure is deterministic, so retrying it only delays the signal. A transient
 * failure goes there once the last tier fails too. Nothing is replayed from it
 * automatically: the usual reason a message is there is a bug to fix first.
 */

export const DLQ_TOPIC = 'notification.dlq';
export const RETRY_TOPICS = ['notification.retry.1', 'notification.retry.2', 'notification.retry.3'] as const;

export interface RetryTier {
  topic: string;
  delayMs: number;
}

export function retryTiers(delaysMs: readonly number[]): RetryTier[] {
  if (delaysMs.length !== RETRY_TOPICS.length) {
    throw new Error(`expected ${RETRY_TOPICS.length} retry delays, one per tier topic, got ${delaysMs.length}`);
  }
  return RETRY_TOPICS.map((topic, i) => ({ topic, delayMs: delaysMs[i]! }));
}

/**
 * Equal jitter: half the delay, plus up to the other half at random.
 *
 * When SMTP has a bad few minutes, every in-flight email fails in the same second.
 * Retried after exactly the same delay, they would all hit the recovering server in
 * the same second again. The random half spreads them out; the fixed half keeps a
 * retry from coming back almost at once.
 */
export function jittered(delayMs: number, random: () => number = Math.random): number {
  return Math.floor(delayMs / 2 + random() * (delayMs / 2));
}

/** Bookkeeping on a message in a retry tier. The repository's DLQ headers carry no x- prefix, and neither do these. */
export const RETRY_HEADERS = {
  originalTopic: 'retry-original-topic',
  /** How many delivery attempts have failed so far. */
  attempt: 'retry-attempt',
  /** Milliseconds since the epoch; not to be tried before then. */
  notBefore: 'retry-not-before',
  firstFailedAt: 'retry-first-failed-at',
  lastError: 'retry-last-error',
} as const;

export const DLQ_HEADERS = {
  reason: 'dlq-reason',
  consumerGroup: 'dlq-consumer-group',
  originalTopic: 'dlq-original-topic',
  sourceTopic: 'dlq-source-topic',
  sourcePartition: 'dlq-source-partition',
  sourceOffset: 'dlq-source-offset',
  attempts: 'dlq-attempts',
  firstFailedAt: 'dlq-first-failed-at',
  /** 'true' when the value was left out on purpose: it was not sealed. See forward(). */
  valueWithheld: 'dlq-value-withheld',
} as const;

/**
 * One header's value as a string. A header can be present with a NULL value — kafkajs
 * encodes null as length -1 and decodes it back to null — and that is read as absent:
 * calling toString() on it threw a TypeError, which kafkajs treats as unrecoverable, so
 * one such header on a message crashed its consumer on every redelivery.
 */
export function header(headers: IHeaders | undefined, name: string): string | undefined {
  const value = headers?.[name];
  const one = Array.isArray(value) ? value[0] : value;
  return one == null ? undefined : one.toString();
}

/** A message's own headers, without the bookkeeping of any earlier retry or dead-letter. */
export function originalHeaders(headers: IHeaders | undefined): Record<string, string> {
  const out: Record<string, string> = {};
  for (const name of Object.keys(headers ?? {})) {
    if (name.startsWith('retry-') || name.startsWith('dlq-')) continue;
    const value = header(headers, name);
    if (value !== undefined) out[name] = value;
  }
  return out;
}
