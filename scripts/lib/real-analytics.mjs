import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { ADMIN, ch } from './clickhouse.mjs';
import { serviceEnv } from './service-env.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');

/** The group a running compose analytics-ingest consumes under, from the service's own example. */
const REAL_GROUP = serviceEnv(ROOT, 'analytics')['KAFKA_CONSUMER_GROUP'];

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * What a verifier left in the REAL analytics database, removed.
 *
 * The shared topics keep every message a verifier produces, and a running compose
 * analytics-ingest copies them into `analytics`: fake purchases under the run's
 * tenant, and poison in ingest_dlq that an operator cannot tell from the real
 * thing. Review found thirteen runs' worth there. Kafka cannot be un-produced, so
 * the copies are deleted once that ingest has caught up past them. A compose
 * ingest started later on a fresh store would replay them again — said here rather
 * than left to be found.
 *
 * `produced` is every { topic, partition, offset } the run sent; `tenant` is the
 * only tenant it put on the topics.
 */
export async function cleanRealAnalytics(admin, produced, tenant) {
  if (produced.length === 0) return 'this run produced nothing';
  let waited = 'no compose ingest was consuming';
  try {
    const { groups } = await admin.describeGroups([REAL_GROUP]);
    if (groups[0]?.state === 'Stable' && groups[0].members.length > 0) {
      const topics = [...new Set(produced.map((p) => p.topic))];
      const deadline = Date.now() + 30_000;
      let caughtUp = false;
      while (!caughtUp && Date.now() < deadline) {
        const offsets = await admin.fetchOffsets({ groupId: REAL_GROUP, topics });
        caughtUp = produced.every((p) => {
          const o = offsets.find((t) => t.topic === p.topic)?.partitions.find((x) => x.partition === p.partition)?.offset;
          return o !== undefined && BigInt(o) > BigInt(p.offset);
        });
        if (!caughtUp) await sleep(500);
      }
      waited = caughtUp ? 'the compose ingest had caught up' : 'the compose ingest had NOT caught up after 30s, so later copies may remain';
    }
  } catch (err) {
    waited = `the compose ingest group could not be read (${err.message})`;
  }
  const coordinates = produced.map((p) => `('${p.topic}', ${p.partition}, ${p.offset})`).join(', ');
  for (const sql of [
    `DELETE FROM analytics.activity_events WHERE tenant_id = '${tenant}'`,
    `ALTER TABLE analytics.ticket_funnel_daily DELETE WHERE tenant_id = '${tenant}' SETTINGS mutations_sync = 2`,
    `ALTER TABLE analytics.engagement_hourly DELETE WHERE tenant_id = '${tenant}' SETTINGS mutations_sync = 2`,
    `DELETE FROM analytics.ingest_dlq WHERE (source_topic, kafka_partition, kafka_offset) IN (${coordinates})`,
  ]) {
    await ch(ADMIN, sql);
  }
  return waited;
}
