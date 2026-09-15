/**
 * Asserts that Search's /metrics tells an operator the two things HTTP metrics
 * cannot: how stale search is, and whether redelivery is normal.
 *
 *   LAG RISES       with the indexer stopped, messages produced to events.activity
 *                   show up as lag, counted in messages
 *   LAG RECOVERS    with the indexer started again under the same group, lag
 *                   returns to 0 because it CAUGHT UP — the new process indexes
 *                   what it missed — not because the measure reset
 *   REPLAY IS SEEN  the same snapshots produced again raise the version-conflict
 *                   counter by exactly that many, and dead-letter nothing
 *   QUERIES COUNT   searches are counted by outcome and timed
 *   SLOWLOG IS ON   the live index carries the query slowlog threshold its
 *                   definition declares
 *
 * WHY LAG IS READ HERE BY THE SERVICE'S OWN FUNCTION WHILE IT IS DOWN. A stopped
 * service has no /metrics to ask. So the verifier imports the same consumerLag
 * the endpoint calls, for the reason #71 established, and reads the group's lag
 * itself; once the service is back, /metrics is asked directly.
 *
 * WATCHED FAIL, four guards broken in one run:
 *
 *   lag computed as 0 whatever the offsets   the rise while stopped was never seen
 *   a version conflict treated as rejected   the replay counted no conflicts and
 *                                            dead-lettered instead
 *   served searches not recorded             servedTotal stayed 0
 *   the slowlog threshold cleared live       the setting read back empty
 *
 * The first run, before any mutation, found something else: under 'latest' a
 * partition the group never committed on restarts at the end, and the messages
 * produced there while the indexer was stopped were never indexed. See the
 * per-partition canaries below, and the warning on SEARCH_INDEXER_START_FROM.
 *
 * Run (after pnpm build, bootstrap:index, topics and schemas):
 *   node scripts/verify-search-observability.mjs
 */

import { spawn } from 'node:child_process';
import { createPrivateKey, randomUUID, sign } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { Kafka, logLevel } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { searchServiceEnv } from './lib/search-service-env.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
process.env['OPENSEARCH_NODE'] ??= 'http://127.0.0.1:9200';
process.env['KAFKA_BROKERS'] ??= '127.0.0.1:9092';
process.env['SCHEMA_REGISTRY_URL'] ??= 'http://127.0.0.1:8081';

const TOPIC = 'events.activity';
const PORT = 3945;
const mod = (rel) => import(pathToFileURL(join(ROOT, 'services', 'search', 'dist', rel)).href);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    // Set at the failure, so an early return cannot exit 0 past it.
    process.exitCode = 1;
  }
}

const run = randomUUID().replace(/-/g, '').slice(0, 8);
const GROUP = `verify_search_obs_${run}`;
const TENANT = `t_obs_${run}`;
const MISSED = 7;

async function waitFor(fn, timeoutMs = 30_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const v = await fn().catch(() => null);
    if (v) return v;
    await sleep(300);
  }
  return null;
}

const ISSUER = process.env['JWT_ISSUER'] ?? 'https://auth.eventhub.local';
const AUDIENCE = process.env['JWT_AUDIENCE'] ?? 'eventhub-api';
function token(tenantId) {
  const b64url = (v) => Buffer.from(v).toString('base64url');
  const now = Math.floor(Date.now() / 1000);
  const header = b64url(JSON.stringify({ alg: 'RS256', typ: 'JWT' }));
  const payload = b64url(JSON.stringify({ sub: `u_obs_${run}`, tenantId, role: 'ATTENDEE', iss: ISSUER, aud: AUDIENCE, iat: now, exp: now + 600 }));
  const key = createPrivateKey(readFileSync(join(ROOT, 'services', 'auth', 'keys', 'private.pem')));
  return `${header}.${payload}.${b64url(sign('sha256', Buffer.from(`${header}.${payload}`), key))}`;
}

async function main() {
  const { os } = await mod('opensearch.js');
  // The same function the indexer's /metrics calls, now shared by every consumer.
  const { consumerLag } = await import(pathToFileURL(join(ROOT, 'packages', 'kafka', 'dist', 'index.js')).href);
  const { INDEX_NAME, tenantAlias } = await mod('index-definition.js');

  const kafka = new Kafka({ clientId: 'verify-search-observability', brokers: process.env['KAFKA_BROKERS'].split(','), logLevel: logLevel.NOTHING });
  const registry = new SchemaRegistry({ host: process.env['SCHEMA_REGISTRY_URL'] });
  const producer = kafka.producer();
  const admin = kafka.admin();
  const schemaId = await registry.getLatestSchemaId(`${TOPIC}-value`);

  const snapshot = (n, version) => ({
    messageId: randomUUID(),
    occurredAt: new Date().toISOString(),
    eventId: `obs_${run}_${n}`,
    tenantId: TENANT,
    title: `observed ${run} ${n}`,
    slug: `observed-${run}-${n}`,
    description: null,
    status: 'PUBLISHED',
    capacity: 10,
    startsAt: '2028-09-01T18:00:00.000Z',
    endsAt: '2028-09-01T22:00:00.000Z',
    organiserId: 'u_obs',
    version,
    changeType: 'UPDATED',
    deleted: false,
  });
  const sentCanaries = new Set();
  /** Produces a snapshot and returns the partition it landed on. */
  const send = async (n, version) => {
    const s = snapshot(n, version);
    const [meta] = await producer.send({ topic: TOPIC, messages: [{ key: s.eventId, value: await registry.encode(schemaId, s) }] });
    return meta.partition;
  };

  let child = null;
  const start = () => {
    const c = spawn(process.execPath, ['dist/index.js'], {
      cwd: join(ROOT, 'services', 'search'),
      env: searchServiceEnv(ROOT, {
        LOG_LEVEL: 'warn',
        PORT: String(PORT),
        OPENSEARCH_NODE: process.env['OPENSEARCH_NODE'],
        KAFKA_BROKERS: process.env['KAFKA_BROKERS'],
        SCHEMA_REGISTRY_URL: process.env['SCHEMA_REGISTRY_URL'],
        KAFKA_CONSUMER_GROUP: GROUP,
        SEARCH_INDEXER_START_FROM: 'latest',
        JWT_ISSUER: ISSUER,
        JWT_AUDIENCE: AUDIENCE,
      }),
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    c.output = '';
    c.stdout.on('data', (d) => (c.output += d));
    c.stderr.on('data', (d) => (c.output += d));
    return c;
  };
  const stop = async () => {
    if (!child) return;
    child.kill('SIGTERM');
    await new Promise((r) => (child.exitCode !== null ? r() : child.once('exit', r)));
    child = null;
  };
  const metrics = async () => {
    const res = await fetch(`http://127.0.0.1:${PORT}/metrics`);
    return res.ok ? res.json() : null;
  };
  const docExists = async (n) =>
    (await os.get({ index: INDEX_NAME, id: `obs_${run}_${n}`, routing: TENANT }, { ignore: [404] })).statusCode === 200;

  try {
    await producer.connect();
    await admin.connect();

    // -----------------------------------------------------------------------
    console.log('  the indexer has to be consuming, and caught up, before lag means anything');
    // -----------------------------------------------------------------------
    child = start();
    const assigned = await waitFor(async () => (await metrics())?.indexer.assignedPartitions > 0, 60_000);
    check(Boolean(assigned), assigned ? 'the indexer joined its group' : `Search did not start:\n${child?.output}`);
    if (!assigned) return;

    /**
     * A CANARY ON EVERY PARTITION, not one. The first version sent a single
     * canary, and every assertion after the restart failed: under 'latest', a
     * partition the group has never committed on starts at the END each time the
     * consumer starts, so the messages produced there while it was down were
     * skipped for good — and their lag was, correctly, unknown. A committed
     * position on every partition is what makes "stopped, then resumed" mean
     * what it means in production.
     */
    const partitionCount = (await admin.fetchTopicOffsets(TOPIC)).length;
    const canaries = new Map();
    for (let i = 0; i < 200 && canaries.size < partitionCount; i++) {
      const n = `canary_${i}`;
      sentCanaries.add(n);
      const partition = await send(n, 1);
      if (!canaries.has(partition)) canaries.set(partition, n);
    }
    let canary = false;
    for (let attempt = 2; attempt <= 11 && !canary; attempt++) {
      canary = Boolean(
        await waitFor(async () => (await Promise.all([...canaries.values()].map(docExists))).every(Boolean), 4_000),
      );
      // Anything produced before the group resolved its position was skipped;
      // send again, newer, until every partition's canary is in.
      if (!canary) for (const n of canaries.values()) await send(n, attempt);
    }
    check(
      canaries.size === partitionCount && canary,
      `CONTROL: a canary on each of the ${partitionCount} partitions was indexed, so the group has a committed position on every one`,
    );
    const caughtUp = await waitFor(async () => (await metrics())?.indexer.lag?.total === 0, 30_000);
    check(Boolean(caughtUp), `/metrics reports lag 0 once caught up (${JSON.stringify((await metrics())?.indexer.lag?.total)})`);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  lag rises while the indexer is down, counted in messages');
    // -----------------------------------------------------------------------
    await stop();
    for (let n = 0; n < MISSED; n++) await send(n, 1);
    const whileDown = await waitFor(async () => {
      const lag = await consumerLag(admin, GROUP, [TOPIC]);
      return lag.total !== null && lag.total >= MISSED ? lag : null;
    }, 15_000);
    check(
      Boolean(whileDown),
      `with the indexer stopped and ${MISSED} messages produced, the group is ${whileDown?.total ?? '?'} message(s) behind`,
    );
    check(!(await docExists(0)), 'CONTROL: and none of them has been indexed');

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  lag recovers because the indexer caught up, not because the measure reset');
    // -----------------------------------------------------------------------
    child = start();
    const recovered = await waitFor(async () => {
      const m = await metrics();
      return m && m.indexer.lag?.total === 0 && m.indexer.indexedTotal >= MISSED ? m : null;
    }, 60_000);
    check(
      Boolean(recovered),
      `restarted under the same group: lag ${recovered?.indexer.lag?.total}, and this process indexed ${recovered?.indexer.indexedTotal} message(s)`,
    );
    const allThere = (await Promise.all(Array.from({ length: MISSED }, (_, n) => docExists(n)))).every(Boolean);
    check(allThere, `all ${MISSED} messages missed while it was down are now in the index`);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  a replay raises the conflict counter, and dead-letters nothing');
    // -----------------------------------------------------------------------
    const before = (await metrics()).indexer;
    for (let n = 0; n < MISSED; n++) await send(n, 1);
    const replayed = await waitFor(async () => {
      const m = (await metrics())?.indexer;
      return m && m.versionConflictsTotal >= before.versionConflictsTotal + MISSED ? m : null;
    });
    check(
      replayed?.versionConflictsTotal === before.versionConflictsTotal + MISSED,
      `replaying the same ${MISSED} snapshots raised versionConflictsTotal ${before.versionConflictsTotal} -> ${replayed?.versionConflictsTotal}`,
    );
    check(replayed?.deadLetteredTotal === before.deadLetteredTotal, `and deadLetteredTotal stayed at ${replayed?.deadLetteredTotal}`);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  searches are counted by outcome and timed');
    // -----------------------------------------------------------------------
    const t = token(TENANT);
    for (let i = 0; i < 3; i++) {
      const r = await fetch(`http://127.0.0.1:${PORT}/search?q=observed`, { headers: { Authorization: `Bearer ${t}` } });
      await r.text();
    }
    const s = (await metrics()).search;
    check(s.servedTotal === 3 && s.durationMs.samples === 3, `servedTotal=${s.servedTotal}, ${s.durationMs.samples} timed`);
    check(typeof s.durationMs.p50 === 'number' && s.durationMs.p50 > 0, `with a p50 of ${s.durationMs.p50} ms`);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  the slowlog is on');
    // -----------------------------------------------------------------------
    const settings = (await os.indices.getSettings({ index: INDEX_NAME, flat_settings: true })).body[INDEX_NAME].settings;
    check(
      settings['index.search.slowlog.threshold.query.warn'] === '2s',
      `${INDEX_NAME} logs queries slower than ${settings['index.search.slowlog.threshold.query.warn']}`,
    );
  } finally {
    await stop();
    // Close every key on its highest version: the topic is compacted and shared,
    // and verify-projection-rebuild.mjs requires the last message per key to be it.
    for (const n of sentCanaries) await send(n, 99).catch(() => {});
    for (let n = 0; n < MISSED; n++) await send(n, 2).catch(() => {});
    await producer.disconnect().catch(() => {});
    await admin.deleteGroups([GROUP]).catch(() => {});
    await admin.disconnect().catch(() => {});
    const { body: del } = await os.deleteByQuery({ index: INDEX_NAME, refresh: true, body: { query: { term: { tenantId: TENANT } } } });
    const aliasHolders = await os.indices.getAlias({ name: tenantAlias(TENANT) }, { ignore: [404] });
    if (aliasHolders.statusCode === 200) {
      await os.indices.updateAliases({ body: { actions: Object.keys(aliasHolders.body).map((index) => ({ remove: { index, alias: tenantAlias(TENANT) } })) } });
    }
    console.log(`\n    cleanup: removed ${del.deleted} document(s)`);
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('/metrics shows how stale search is, whether redelivery is normal, and how search is doing');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
