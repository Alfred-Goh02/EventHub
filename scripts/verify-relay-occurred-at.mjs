/**
 * Asserts that the occurredAt the relay publishes IS the outbox row's createdAt,
 * to the millisecond, whatever time zone the relay process runs in.
 *
 * IT WAS NOT. The claim selected "createdAt", a `timestamp WITHOUT time zone`
 * holding UTC wall time, node-postgres parsed it as LOCAL time, and the publisher
 * sent `row.createdAt.toISOString()`. In a UTC process those agree, which is every
 * compose container and every CI runner, so nothing ever went red. On this
 * repository's UTC+8 dev machine the shipped drain published, for one row:
 *
 *   createdAt, rendered by Postgres   2026-09-26T10:39:17.474
 *   occurredAt, published to Kafka    2026-09-26T02:39:17.474Z      (-28800 s)
 *
 * Every verifier that runs a relay pass natively published history eight hours
 * early. Community and the contact cards dodged it by stamping occurredAt into
 * their own payloads; events.activity, tickets.* and auth.email-requested took
 * the relay's value. The claim now renders the string in Postgres.
 *
 * THE ZONE IS FORCED, OR CI COULD NEVER FAIL. The relay pass runs in a child
 * process started with TZ=Asia/Kathmandu (UTC+05:45: not UTC, not this machine's
 * +08:00, and no DST to move it). The child reports the offset it actually ran
 * at, and that is checked, because a TZ that is not honoured leaves a UTC runner
 * agreeing with itself. Git Bash, for one, does not pass TZ to a native process.
 *
 * AND THE CHILD PROVES THE FIXTURE CAN SHOW THE FAILURE. In that process it reads
 * the row's createdAt the way the relay used to, as a pg Date, and that string
 * must be off by exactly the zone's offset. If it were not, the equality below
 * would pass whether or not the relay were fixed.
 *
 * THE COMPOSE RELAY CANNOT BE THE ONE THAT PUBLISHES. It drains the same outbox,
 * it runs UTC, and it would publish the right value from the wrong code. So the
 * child takes the claim's own per-stream advisory lock, in a session, BEFORE the
 * row exists: every other relay's pg_try_advisory_xact_lock then fails for this
 * aggregate, and only a drain in that session -- the child's pool has exactly one
 * connection -- can claim it. It is checked three ways: a second session cannot
 * take the lock, the drain ran on the lock's connection, and the shipped publisher,
 * wrapped only to record ids, was handed THIS row.
 *
 * The drain and the publisher are the shipped ones, imported from dist, so the
 * claim under test is the claim that runs. Compared as strings and in SQL, never
 * through Date.parse: see CLAUDE.md, "Timestamps come from the database, and are
 * compared there".
 *
 * Run: node scripts/verify-relay-occurred-at.mjs   (after building services/relay)
 */

import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import pg from 'pg';
import { Kafka, logLevel } from 'kafkajs';
import { SchemaRegistry } from '@kafkajs/confluent-schema-registry';
import { deleteGroupsWhenEmpty } from './lib/kafka-groups.mjs';

const APP_URL =
  process.env['EVENT_APP_DATABASE_URL'] ??
  'postgresql://eventhub_event_app:eventhub_event_app@localhost:5432/eventhub_event';
const RELAY_URL =
  process.env['RELAY_DATABASE_URL'] ??
  'postgresql://eventhub_event_relay:eventhub_event_relay@localhost:5432/eventhub_event';
const BROKERS = (process.env['KAFKA_BROKERS'] ?? 'localhost:9092').split(',');
const REGISTRY = process.env['SCHEMA_REGISTRY_URL'] ?? 'http://localhost:8081';

const REPO_ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const THIS_FILE = fileURLToPath(import.meta.url);

const TOPIC = 'events.activity';
const tenantId = 'tenant_acme';

/** Not UTC, not +08:00, a 45-minute part no whole-hour zone shares, and no DST. */
const ZONE = 'Asia/Kathmandu';
const ZONE_OFFSET_MINUTES = 345;

/**
 * The claim's advisory-lock key, for this row's stream, as SQL.
 *
 * Written out rather than parsed from claim-sql.ts. If the two ever drift, the
 * lock stops fencing out compose's relay, and the check that THIS drain published
 * THIS row is what goes red.
 */
const STREAM_KEY = `hashtext($1 || chr(31) || $2 || chr(31) || $3)::bigint`;

/** The shape toISOString produces, which is also what analytics insists on. */
const ISO_UTC = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** 345 -> "+05:45", 0 -> "+00:00". */
const utcOffset = (minutes) =>
  `${minutes < 0 ? '-' : '+'}${String(Math.floor(Math.abs(minutes) / 60)).padStart(2, '0')}:${String(
    Math.abs(minutes) % 60,
  ).padStart(2, '0')}`;

// ---------------------------------------------------------------------------
// The child: one relay pass, in the forced zone. Everything below it is the parent.
// ---------------------------------------------------------------------------
if (process.argv[2] === '--relay-pass') {
  const id = process.argv[3];
  const out = await relayPass(id);
  console.log(`RESULT ${JSON.stringify(out)}`);
  process.exit(0);
}

async function relayPass(id) {
  const out = { offsetMinutes: -new Date().getTimezoneOffset() };

  const drainJs = join(REPO_ROOT, 'services/relay/dist/drain.js');
  const publisherJs = join(REPO_ROOT, 'services/relay/dist/publisher.js');
  if (!existsSync(drainJs) || !existsSync(publisherJs)) {
    throw new Error('services/relay/dist is missing — build the relay first (pnpm --filter @eventhub/relay build)');
  }
  const { drainOnce } = await import(pathToFileURL(drainJs).href);
  const { createPublisher } = await import(pathToFileURL(publisherJs).href);

  // warn and error reach stderr, so a dead-lettered or failing row is visible.
  const logger = {
    debug() {},
    info() {},
    warn: (o, m) => console.error(`  relay warn: ${m} ${JSON.stringify(o)}`),
    error: (o, m) => console.error(`  relay error: ${m} ${JSON.stringify(o)}`),
    child() {
      return logger;
    },
  };

  /**
   * ONE connection, never closed while idle, so the drain runs in the session
   * that holds the stream's lock. A second connection would not hold it, and its
   * try-lock would fail like everybody else's.
   */
  const pool = new pg.Pool({
    connectionString: RELAY_URL,
    application_name: 'verify-occurred-at',
    max: 1,
    idleTimeoutMillis: 0,
  });
  pool.on('error', () => {});
  const probe = new pg.Client({ connectionString: RELAY_URL, application_name: 'verify-occurred-at-probe' });
  const app = new pg.Client({ connectionString: APP_URL, application_name: 'verify-occurred-at' });
  const publisher = createPublisher({ brokers: BROKERS, registryUrl: REGISTRY });
  let locked = false;

  try {
    await probe.connect();
    await app.connect();

    const { rows: holder } = await pool.query(
      `SELECT pg_backend_pid() AS pid, pg_try_advisory_lock(${STREAM_KEY}) AS got`,
      [tenantId, TOPIC, id],
    );
    locked = holder[0].got;
    out.lockTaken = locked;
    out.lockPid = holder[0].pid;
    if (!locked) return out;

    // Only now does the row exist. No relay could have seen it unfenced.
    await app.query('BEGIN');
    await app.query(`SELECT set_config('app.tenant_id', $1, true)`, [tenantId]);
    await app.query(
      `INSERT INTO "OutboxMessage"
         (id, "tenantId", topic, "eventType", "schemaVersion", "aggregateId", payload)
       VALUES ($1, $2, $3, 'event.snapshot', 1, $1, $4::jsonb)`,
      [
        id,
        tenantId,
        TOPIC,
        // A DRAFT that is created and nothing else: no consumer sells, indexes or
        // announces it. And no occurredAt, so the published one is the relay's own.
        JSON.stringify({
          eventId: id,
          tenantId,
          title: 'occurredAt check',
          slug: 'occurred-at-check',
          description: null,
          status: 'DRAFT',
          capacity: 1,
          startsAt: '2027-06-01T10:00:00.000Z',
          endsAt: '2027-06-01T18:00:00.000Z',
          organiserId: 'u_occurred_at',
          version: 1,
          changeType: 'CREATED',
          deleted: false,
        }),
      ],
    );
    await app.query('COMMIT');

    // Can anybody else take it? A relay that cannot is a relay that cannot claim this row.
    const { rows: other } = await probe.query(`SELECT pg_try_advisory_lock(${STREAM_KEY}) AS got`, [
      tenantId,
      TOPIC,
      id,
    ]);
    out.otherSessionGotLock = other[0].got;
    if (other[0].got) {
      await probe.query(`SELECT pg_advisory_unlock(${STREAM_KEY})`, [tenantId, TOPIC, id]);
    }

    // The old code path, as the negative control: pg's Date, through toISOString.
    const { rows: asDate } = await pool.query(`SELECT "createdAt" FROM "OutboxMessage" WHERE id = $1`, [id]);
    out.localParse = asDate[0].createdAt.toISOString();

    await publisher.connect();
    const handed = [];
    const recording = {
      async publish(row) {
        await publisher.publish(row);
        handed.push(row.id);
      },
    };

    /**
     * Retried because a compose relay's claim can hold this row's ROW lock for a
     * moment -- its CTE locks it, its outer filter then rejects it for the
     * advisory lock -- and SKIP LOCKED hides it from this drain until that commits.
     */
    let passes = 0;
    while (passes < 40 && !handed.includes(id)) {
      passes += 1;
      await drainOnce(pool, recording, logger, { batchSize: 100, maxAttempts: 5 });
      if (!handed.includes(id)) await sleep(250);
    }
    out.passes = passes;
    out.publishedByThisPass = handed.includes(id);

    const { rows: after } = await pool.query('SELECT pg_backend_pid() AS pid');
    out.drainPid = after[0].pid;
    out.poolConnections = pool.totalCount;
    return out;
  } finally {
    if (locked) await pool.query(`SELECT pg_advisory_unlock(${STREAM_KEY})`, [tenantId, TOPIC, id]).catch(() => {});
    await publisher.disconnect().catch(() => {});
    await pool.end().catch(() => {});
    await probe.end().catch(() => {});
    await app.end().catch(() => {});
  }
}

// ---------------------------------------------------------------------------
// The parent.
// ---------------------------------------------------------------------------

/** Sets the exit code where the check fails, not only in a summary at the end. */
let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

function runChild(id) {
  return new Promise((done) => {
    const child = spawn(process.execPath, [THIS_FILE, '--relay-pass', id], {
      env: { ...process.env, TZ: ZONE },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (d) => (stdout += d));
    child.stderr.on('data', (d) => (stderr += d));
    child.on('close', (code) => done({ code, stdout, stderr }));
  });
}

/** Removes this run's row, as the role and tenant that wrote it, and says how many. */
async function removeRow(id) {
  const app = new pg.Client({ connectionString: APP_URL, application_name: 'verify-occurred-at' });
  await app.connect();
  try {
    await app.query('BEGIN');
    await app.query(`SELECT set_config('app.tenant_id', $1, true)`, [tenantId]);
    const { rowCount } = await app.query(`DELETE FROM "OutboxMessage" WHERE id = $1`, [id]);
    await app.query('COMMIT');
    return rowCount;
  } finally {
    await app.end().catch(() => {});
  }
}

async function main() {
  const id = `occat_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`;
  const groupId = `verify-occurred-at-${id}`;

  const kafka = new Kafka({ clientId: 'verify-occurred-at', brokers: BROKERS, logLevel: logLevel.ERROR });
  const registry = new SchemaRegistry({ host: REGISTRY });
  const admin = kafka.admin();
  await admin.connect();
  const relayDb = new pg.Client({ connectionString: RELAY_URL, application_name: 'verify-occurred-at' });
  await relayDb.connect();

  let consumerStarted = false;
  try {
    // Where every partition ended before this run, so the scan reads only what came after.
    const before = await admin.fetchTopicOffsets(TOPIC);

    console.log(`  one relay pass, in a child process with TZ=${ZONE}`);
    const child = await runChild(id);
    const line = child.stdout.split(/\r?\n/).find((l) => l.startsWith('RESULT '));
    check(
      child.code === 0 && Boolean(line),
      `the pass ran and reported back (exit ${child.code})` +
        (child.code === 0 && line ? '' : `\n${child.stderr.trim()}`),
    );
    if (!line) return;
    const pass = JSON.parse(line.slice('RESULT '.length));

    console.log('');
    console.log('  the run can show the failure');
    check(
      pass.offsetMinutes === ZONE_OFFSET_MINUTES,
      `CONTROL: the relay pass ran at UTC${utcOffset(pass.offsetMinutes)}` +
        (pass.offsetMinutes === ZONE_OFFSET_MINUTES
          ? ''
          : ` — expected UTC${utcOffset(ZONE_OFFSET_MINUTES)}, so TZ was not honoured and a local-time ` +
            'parse cannot be told from a correct one'),
    );
    check(pass.lockTaken === true, `CONTROL: the pass took the stream's advisory lock before the row existed`);
    if (!pass.lockTaken) return;

    const { rows: facts } = await relayDb.query(
      `SELECT to_char("createdAt", 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS rendered,
              "publishedAt" IS NOT NULL AS published,
              "deadLetteredAt" IS NOT NULL AS dead,
              "lastError",
              payload ? 'occurredAt' AS payload_has_it,
              (extract(epoch FROM $2::timestamptz - ("createdAt" AT TIME ZONE 'UTC')) * 1000)::bigint::text AS local_off_ms
         FROM "OutboxMessage" WHERE id = $1`,
      [id, pass.localParse],
    );
    const row = facts[0];
    check(Boolean(row), 'the outbox row this run wrote is there to compare against');
    if (!row) return;

    const zoneMs = String(-ZONE_OFFSET_MINUTES * 60_000);
    check(
      row.local_off_ms === zoneMs,
      `CONTROL: in that process, createdAt read as a pg Date gives ${pass.localParse}, ` +
        `${row.local_off_ms} ms from the column (expected ${zoneMs}) — the old code path is wrong here`,
    );
    check(!row.payload_has_it, 'CONTROL: the stored payload carries no occurredAt, so the published one is the relay\'s own');
    check(
      pass.otherSessionGotLock === false,
      'CONTROL: no other session could take the lock, so no other relay could claim the row',
    );
    check(
      pass.publishedByThisPass === true && row.published && !row.dead,
      pass.publishedByThisPass
        ? `CONTROL: the shipped drain in that process published THIS row (pass ${pass.passes}), and publishedAt is set`
        : `the drain in that process never published this row after ${pass.passes} pass(es) ` +
            `(published=${row.published}, dead=${row.dead}, lastError=${row.lastError}); ` +
            'if something else published it, it was not the code under test',
    );
    check(
      pass.drainPid === pass.lockPid && pass.poolConnections === 1,
      `CONTROL: the drain ran on the lock's own connection (pid ${pass.drainPid} vs ${pass.lockPid}, ${pass.poolConnections} connection)`,
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log(`  what reached ${TOPIC}`);
    // -----------------------------------------------------------------------
    const after = await admin.fetchTopicOffsets(TOPIC);
    const end = new Map(after.map((p) => [p.partition, Number(p.high)]));
    const reached = new Set(
      before.filter((p) => Number(p.high) >= end.get(p.partition)).map((p) => p.partition),
    );
    const found = [];
    const consumer = kafka.consumer({ groupId });
    await consumer.connect();
    await consumer.subscribe({ topic: TOPIC });
    consumerStarted = true;
    await consumer.run({
      eachMessage: async ({ partition, message }) => {
        if (message.key?.toString() === id) found.push(await registry.decode(message.value));
        if (Number(message.offset) + 1 >= end.get(partition)) reached.add(partition);
      },
    });
    for (const p of before) consumer.seek({ topic: TOPIC, partition: p.partition, offset: p.high });
    const until = Date.now() + 30_000;
    while (reached.size < end.size && Date.now() < until) await sleep(200);
    await consumer.disconnect();

    check(
      reached.size === end.size,
      `CONTROL: the scan read every partition up to its high watermark (${reached.size}/${end.size})`,
    );
    check(found.length === 1, `exactly one message for this row's key (${found.length})`);
    if (found.length === 0) return;

    const published = found[0].occurredAt;
    check(found[0].messageId === id, `it is this row's message (messageId ${found[0].messageId})`);
    check(ISO_UTC.test(published), `occurredAt has toISOString's shape: ${published}`);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  occurredAt is the row\'s createdAt');
    // -----------------------------------------------------------------------
    console.log(`        createdAt, rendered by Postgres:  ${row.rendered}`);
    console.log(`        occurredAt, published to Kafka:   ${published}`);
    check(published === row.rendered, 'the published string is Postgres\'s own rendering of createdAt');

    if (ISO_UTC.test(published)) {
      const { rows: diff } = await relayDb.query(
        `SELECT (extract(epoch FROM $2::timestamptz - ("createdAt" AT TIME ZONE 'UTC')) * 1000)::bigint::text AS ms
           FROM "OutboxMessage" WHERE id = $1`,
        [id, published],
      );
      check(
        diff[0].ms === '0',
        diff[0].ms === '0'
          ? 'and Postgres agrees they are the same instant (0 ms apart)'
          : `Postgres puts occurredAt ${diff[0].ms} ms from createdAt — the relay's zone leaked into the message`,
      );
    }
  } finally {
    const deleted = await removeRow(id).catch((err) => `FAILED (${err.message})`);
    console.log('');
    console.log(`  cleanup: ${deleted} outbox row(s) deleted for ${id}`);
    if (consumerStarted) {
      const left = await deleteGroupsWhenEmpty(admin, [groupId]).catch(() => [groupId]);
      console.log(`  cleanup: consumer group ${left.length === 0 ? 'deleted' : 'left to expire'}`);
    }
    await admin.disconnect().catch(() => {});
    await relayDb.end().catch(() => {});
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('the relay publishes createdAt as occurredAt, whatever zone it runs in');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
