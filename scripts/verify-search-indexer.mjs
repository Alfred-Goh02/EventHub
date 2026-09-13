/**
 * Asserts that the search indexer applies events.activity correctly under the
 * delivery Kafka actually provides: at least once, and in no promised order
 * across a rebalance.
 *
 * Four properties, each invisible from outside unless it is looked for:
 *
 *   IDEMPOTENT      the same snapshot twice leaves the document's _version where
 *                   it was, and the conflict counter moves
 *   ORDERED         an OLDER snapshot arriving after a newer one changes nothing
 *   NO RESURRECTION a stale snapshot replayed over a deleted event leaves it
 *                   deleted — the reason deletes are a status, not a DELETE
 *   NOT BLOCKED     a message that can never be indexed is parked on the DLQ and
 *                   the partition moves on past it
 *
 * Plus the one that joins this to Chapter 6 part 1: a document the indexer
 * writes is found through its tenant's alias and not through another's, which
 * holds only if the indexer's routing matches the alias's.
 *
 * WHY EVERY "NOTHING CHANGED" HAS A COUNTER BESIDE IT. A duplicate that was never
 * consumed also leaves _version alone. So each no-op assertion first waits for
 * versionConflictsTotal to rise by exactly one — proof the message was read and
 * refused — and only then checks the document.
 *
 * WHY THE INDEXER RUNS HERE UNDER A FRESH GROUP STARTING AT 'latest'. A fresh
 * group at 'earliest' would replay the compacted topic's whole history into the
 * shared index. 'latest' has its own trap: a message produced before the group
 * has resolved its position is silently skipped. So a canary is produced until
 * one is indexed, and nothing is asserted before that.
 *
 * WATCHED FAIL, in two runs against a deliberately broken indexer:
 *
 *   version_type external removed    the stale title overwrote the fresh one,
 *   + deletes not marked             the replay un-deleted the event: 10 red
 *   routing removed                  tenant A's own event vanished from its
 *   + undecodable throws, not parks  search; the partition behind the poison
 *                                    never moved: 11 red
 *
 * The first of those runs also caught this file lying twice. "_version is still
 * 2" passed with versioning removed, because internal versioning happened to
 * count to 2; it now compares before with after. And a search made straight after
 * a realtime GET found nothing on a slow run; it now polls.
 *
 * Run (after pnpm build, bootstrap:index, topics and schemas):
 *   node scripts/verify-search-indexer.mjs
 */

import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { Kafka, logLevel } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');

process.env['OPENSEARCH_NODE'] ??= 'http://127.0.0.1:9200';
process.env['KAFKA_BROKERS'] ??= '127.0.0.1:9092';
process.env['SCHEMA_REGISTRY_URL'] ??= 'http://127.0.0.1:8081';

const BROKERS = process.env['KAFKA_BROKERS'].split(',').map((b) => b.trim());
const REGISTRY = process.env['SCHEMA_REGISTRY_URL'];
const TOPIC = 'events.activity';
const DLQ = 'events.activity.dlq';
const PORT = 3915;

const mod = (rel) => import(pathToFileURL(join(ROOT, 'services', 'search', 'dist', rel)).href);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}

const run = randomUUID().replace(/-/g, '').slice(0, 8);
const GROUP = `verify_search_indexer_${run}`;
const DLQ_GROUP = `verify_search_dlq_${run}`;
const T1 = `t_idx_a_${run}`;
const T2 = `t_idx_b_${run}`;
const E0 = `evt_idx_${run}_canary`;
const E1 = `evt_idx_${run}_1`;
const E2 = `evt_idx_${run}_2`;
const E_BAD = `evt_idx_${run}_bad`;

function snapshot(over) {
  return {
    messageId: randomUUID(),
    occurredAt: new Date().toISOString(),
    eventId: E1,
    tenantId: T1,
    title: `alpha ${run}`,
    slug: `alpha-${run}`,
    description: null,
    status: 'PUBLISHED',
    capacity: 50,
    startsAt: '2027-05-01T18:00:00.000Z',
    endsAt: '2027-05-01T22:00:00.000Z',
    organiserId: 'u_verify',
    version: 1,
    changeType: 'UPDATED',
    deleted: false,
    ...over,
  };
}

async function waitFor(fn, timeoutMs = 20_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await fn();
    if (value) return value;
    await sleep(250);
  }
  return null;
}

async function main() {
  const { os } = await mod('opensearch.js');
  const { searchForTenant, forgetTenantAliases } = await mod('tenant-alias.js');
  const { INDEX_NAME, READ_ALIAS, WRITE_ALIAS, tenantAlias } = await mod('index-definition.js');

  const holdersOf = async (alias) => {
    const r = await os.indices.getAlias({ name: alias }, { ignore: [404] });
    return r.statusCode === 404 ? [] : Object.keys(r.body);
  };

  // -------------------------------------------------------------------------
  console.log('  the index has to be the defined one, checked before anything is produced');
  // -------------------------------------------------------------------------
  const read = await holdersOf(READ_ALIAS);
  const write = await holdersOf(WRITE_ALIAS);
  check(read.length === 1 && read[0] === INDEX_NAME, `${READ_ALIAS} resolves to [${read.join(', ')}]`);
  check(write.length === 1 && write[0] === INDEX_NAME, `${WRITE_ALIAS} resolves to [${write.join(', ')}]`);
  if (failures > 0) {
    console.error('\nFAIL: run pnpm --filter @eventhub/search run bootstrap:index first');
    process.exitCode = 1;
    return;
  }

  const kafka = new Kafka({ clientId: 'verify-search-indexer', brokers: BROKERS, logLevel: logLevel.ERROR });
  const registry = new SchemaRegistry({ host: REGISTRY });
  const producer = kafka.producer();
  const dlqReader = kafka.consumer({ groupId: DLQ_GROUP });
  const schemaId = await registry.getLatestSchemaId(`${TOPIC}-value`);

  const send = async (key, value) => producer.send({ topic: TOPIC, messages: [{ key, value }] });
  const sendSnapshot = async (over) => {
    const s = snapshot(over);
    await send(s.eventId, await registry.encode(schemaId, s));
  };

  const getDoc = async (id, routing) => {
    const r = await os.get({ index: INDEX_NAME, id, routing }, { ignore: [404] });
    return r.statusCode === 404 ? null : r.body;
  };

  let child = null;
  let childOutput = '';

  try {
    await producer.connect();

    // Read the DLQ from the END before producing, so only this run's copies count.
    const parked = [];
    let dlqAssigned = false;
    await dlqReader.connect();
    dlqReader.on(dlqReader.events.GROUP_JOIN, () => {
      dlqAssigned = true;
    });
    await dlqReader.subscribe({ topic: DLQ, fromBeginning: false });
    await dlqReader.run({
      eachMessage: async ({ message }) => {
        const headers = Object.fromEntries(
          Object.entries(message.headers ?? {}).map(([k, v]) => [k, v?.toString()]),
        );
        if (headers['dlq-consumer-group'] === GROUP) parked.push({ key: message.key?.toString(), headers });
      },
    });

    child = spawn(process.execPath, ['dist/index.js'], {
      cwd: join(ROOT, 'services', 'search'),
      env: {
        PATH: process.env['PATH'],
        SystemRoot: process.env['SystemRoot'],
        NODE_ENV: 'development',
        LOG_LEVEL: 'warn',
        PORT: String(PORT),
        OPENSEARCH_NODE: process.env['OPENSEARCH_NODE'],
        KAFKA_BROKERS: process.env['KAFKA_BROKERS'],
        SCHEMA_REGISTRY_URL: REGISTRY,
        KAFKA_CONSUMER_GROUP: GROUP,
        SEARCH_INDEXER_START_FROM: 'latest',
        // The service verifies tokens at startup since GET /search exists, so it
        // needs a key and issuer to boot at all, even though nothing here calls it.
        AUTH_PUBLIC_KEY_B64: readFileSync(join(ROOT, 'services', 'auth', 'keys', 'public.pem')).toString('base64'),
        JWT_ISSUER: process.env['JWT_ISSUER'] ?? 'https://auth.eventhub.local',
        JWT_AUDIENCE: process.env['JWT_AUDIENCE'] ?? 'eventhub-api',
        EVENT_SERVICE_URL: process.env['EVENT_URL'] ?? 'http://127.0.0.1:3002',
      },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    child.stdout.on('data', (c) => (childOutput += c));
    child.stderr.on('data', (c) => (childOutput += c));

    const metrics = async () => {
      try {
        const res = await fetch(`http://127.0.0.1:${PORT}/metrics`);
        return res.ok ? (await res.json()).indexer : null;
      } catch {
        return null;
      }
    };

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  the indexer has to be consuming before anything is asserted about it');
    // -----------------------------------------------------------------------
    const assigned = await waitFor(async () => (await metrics())?.assignedPartitions > 0, 60_000);
    check(Boolean(assigned), 'the indexer joined its group and holds partitions');
    const dlqReady = await waitFor(async () => dlqAssigned, 30_000);
    check(Boolean(dlqReady), 'the dead-letter reader joined its group');

    let canaryIndexed = false;
    for (let attempt = 1; attempt <= 10 && !canaryIndexed; attempt++) {
      await sendSnapshot({ eventId: E0, version: attempt, title: `canary ${run}` });
      canaryIndexed = Boolean(await waitFor(async () => getDoc(E0, T1), 4_000));
    }
    check(
      canaryIndexed,
      canaryIndexed
        ? 'CONTROL: a canary snapshot was indexed, so the consumer is past its starting position'
        : `no canary was indexed after 10 attempts. Service output:\n${childOutput.slice(-2000)}`,
    );
    if (!canaryIndexed) return;

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  a snapshot becomes a document, versioned by its source');
    // -----------------------------------------------------------------------
    await sendSnapshot({ version: 2 });
    const v2 = await waitFor(async () => {
      const d = await getDoc(E1, T1);
      return d?._version === 2 ? d : null;
    });
    check(
      v2?._version === 2 && v2?._source.title === `alpha ${run}` && v2?._source.status === 'published',
      `version 2 indexed: _version=${v2?._version}, title=${JSON.stringify(v2?._source.title)}, status=${v2?._source.status}`,
    );
    check(v2?._routing === T1, `and routed by tenant (_routing=${v2?._routing})`);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  the same snapshot twice changes nothing, and is seen to be refused');
    // -----------------------------------------------------------------------
    /**
     * Compared with what was there BEFORE, not with the literal 2. The first
     * version asserted `_version === 2`, and with external versioning removed it
     * passed anyway: internal versioning happened to count the original and the
     * duplicate up to 2. An assertion satisfied by the bug it guards is not one.
     */
    const conflictsBefore = (await metrics()).versionConflictsTotal;
    const beforeDup = (await getDoc(E1, T1))?._version;
    await sendSnapshot({ version: 2 });
    const refusedDup = await waitFor(
      async () => (await metrics()).versionConflictsTotal === conflictsBefore + 1,
    );
    check(Boolean(refusedDup), `CONTROL: the duplicate was consumed and refused (conflicts ${conflictsBefore} -> ${conflictsBefore + 1})`);
    const afterDup = (await getDoc(E1, T1))?._version;
    check(
      beforeDup === 2 && afterDup === beforeDup,
      `and _version is unchanged by it (${beforeDup} -> ${afterDup})`,
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  an older snapshot arriving late changes nothing');
    // -----------------------------------------------------------------------
    await sendSnapshot({ version: 1, title: `stale ${run}` });
    const refusedStale = await waitFor(
      async () => (await metrics()).versionConflictsTotal === conflictsBefore + 2,
    );
    check(Boolean(refusedStale), 'CONTROL: the stale snapshot was consumed and refused');
    const afterStale = await getDoc(E1, T1);
    check(
      afterStale?._source.title === `alpha ${run}` && afterStale?._version === 2,
      `and the title is still ${JSON.stringify(afterStale?._source.title)} at _version ${afterStale?._version}`,
    );

    await sendSnapshot({ version: 3, title: `beta ${run}` });
    const v3 = await waitFor(async () => {
      const d = await getDoc(E1, T1);
      return d?._version === 3 ? d : null;
    });
    check(v3?._source.title === `beta ${run}`, `CONTROL: a NEWER snapshot does win (title ${JSON.stringify(v3?._source.title)})`);

    // -----------------------------------------------------------------------
    console.log('');
    console.log("  the document is found through its tenant's alias, and only there");
    // -----------------------------------------------------------------------
    await sendSnapshot({ eventId: E2, tenantId: T2, version: 1, title: `gamma ${run}` });

    /**
     * POLL THE SEARCH, not the document. A GET by id is realtime — it reads the
     * translog — while a search only sees a document after the next refresh. The
     * first version waited for the GET and then searched once, and a slow run
     * answered "tenant B finds nothing" for a document that was there. Polling
     * keeps the near-real-time path honest without forcing a refresh.
     */
    forgetTenantAliases();
    const idsFor = async (tenant) =>
      (await searchForTenant(tenant, { text: run, size: 50 })).hits.hits.map((h) => h._id).sort();
    await waitFor(async () => (await idsFor(T1)).includes(E1) && (await idsFor(T2)).includes(E2));
    const t1Ids = await idsFor(T1);
    const t2Ids = await idsFor(T2);
    check(
      t1Ids.includes(E1) && !t1Ids.includes(E2),
      `tenant A's search finds [${t1Ids.join(', ')}] — its own events, not B's`,
    );
    check(t2Ids.length === 1 && t2Ids[0] === E2, `tenant B's search finds [${t2Ids.join(', ')}]`);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  a deleted event stays deleted when a stale snapshot is replayed over it');
    // -----------------------------------------------------------------------
    await sendSnapshot({ version: 4, deleted: true, changeType: 'DELETED' });
    const v4 = await waitFor(async () => {
      const d = await getDoc(E1, T1);
      return d?._version === 4 ? d : null;
    });
    check(v4?._source.status === 'deleted', `version 4 marks it deleted (status=${v4?._source.status})`);

    const conflictsBeforeReplay = (await metrics()).versionConflictsTotal;
    await sendSnapshot({ version: 3, title: `beta ${run}`, deleted: false });
    const refusedReplay = await waitFor(
      async () => (await metrics()).versionConflictsTotal === conflictsBeforeReplay + 1,
    );
    check(Boolean(refusedReplay), 'CONTROL: the replayed version 3 was consumed and refused');
    check((await getDoc(E1, T1))?._source.status === 'deleted', 'and the event is still deleted');

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  what can never be indexed is parked, and the partition moves on');
    // -----------------------------------------------------------------------
    const parkedBefore = (await metrics()).deadLetteredTotal;

    // Keyed like E2, so it lands on E2's partition and the next message behind
    // it there is the one that must still get through.
    await send(E2, Buffer.from('this is not a Confluent frame'));
    await sendSnapshot({ eventId: E2, tenantId: T2, version: 2, title: `gamma two ${run}` });
    const behindPoison = await waitFor(async () => {
      const d = await getDoc(E2, T2);
      return d?._version === 2 ? d : null;
    });
    check(Boolean(behindPoison), 'the snapshot produced behind an undecodable message on its partition was indexed');

    await sendSnapshot({ eventId: E_BAD, tenantId: 'Bad*Tenant', version: 1 });

    await waitFor(async () => parked.some((m) => m.key === E2) && parked.some((m) => m.key === E_BAD), 20_000);
    const garbage = parked.find((m) => m.key === E2);
    const badTenant = parked.find((m) => m.key === E_BAD);
    check(
      /^undecodable/.test(garbage?.headers['dlq-reason'] ?? ''),
      `the undecodable message is on the DLQ: ${JSON.stringify(garbage?.headers['dlq-reason'])}`,
    );
    check(
      /tenantId/.test(badTenant?.headers['dlq-reason'] ?? ''),
      `a snapshot whose tenantId is not a tenant id is on the DLQ: ${JSON.stringify(badTenant?.headers['dlq-reason'])}`,
    );
    const badIndexed = (
      await os.search({ index: READ_ALIAS, body: { query: { ids: { values: [E_BAD] } } } })
    ).body.hits.total.value;
    check(badIndexed === 0, `and it was not indexed (found ${badIndexed})`);
    const parkedAfter = (await metrics()).deadLetteredTotal;
    check(
      parkedAfter === parkedBefore + 2,
      `deadLetteredTotal rose by exactly 2 (${parkedBefore} -> ${parkedAfter})`,
    );
  } finally {
    if (child) {
      child.kill('SIGTERM');
      await new Promise((r) => (child.exitCode !== null ? r() : child.once('exit', r)));
    }

    /**
     * LEAVE THE TOPIC REBUILDABLE. events.activity is compacted and shared, and
     * verify-projection-rebuild.mjs asserts that the LAST message for every key
     * is also its HIGHEST version — because once the compactor runs, the last
     * message is the only one left. This file publishes out-of-order snapshots on
     * purpose, and without this E1 would end on a stale v3 behind a v4: the exact
     * condition that check exists to catch, caused by a test, red in a later CI
     * step with nothing pointing back here. In finally, so a run that failed
     * halfway still closes the key.
     */
    await sendSnapshot({ version: 99, deleted: true, changeType: 'DELETED' }).catch(() => {});

    await dlqReader.disconnect().catch(() => {});
    await producer.disconnect().catch(() => {});

    const { body: del } = await os.deleteByQuery({
      index: INDEX_NAME,
      refresh: true,
      body: { query: { terms: { tenantId: [T1, T2] } } },
    });
    console.log('');
    console.log(`    cleanup: removed ${del.deleted} document(s)`);
    const actions = [];
    for (const t of [T1, T2]) {
      for (const index of await holdersOf(tenantAlias(t))) actions.push({ remove: { index, alias: tenantAlias(t) } });
    }
    if (actions.length > 0) await os.indices.updateAliases({ body: { actions } });

    const admin = kafka.admin();
    await admin.connect();
    await admin.deleteGroups([GROUP, DLQ_GROUP]).catch(() => {});
    await admin.disconnect();
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('the indexer is idempotent, refuses stale snapshots, never resurrects, and is never blocked');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
