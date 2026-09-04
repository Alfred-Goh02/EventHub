/**
 * Asserts that the consumer checkpoint cannot move BACKWARDS on a replay.
 *
 * The checkpoint write runs on every message, including ones the dedup check
 * just discarded — consuming a redelivery really does mean this partition has
 * been read that far. It used to be an unconditional
 * `SET "lastOffset" = EXCLUDED."lastOffset"`.
 *
 * That is a problem because replays are ROUTINE here, not exceptional. The
 * consumer subscribes `fromBeginning: true` against a COMPACTED topic, so a
 * replay happens on a cold start against a wiped database, on a consumer-group
 * reset, and unprompted if Kafka expires the group's committed offsets after a
 * period of inactivity. Each one rewound the recorded offset to 0 and then
 * climbed back, so an operator reading the table mid-replay saw a checkpoint
 * that was confidently wrong.
 *
 * It also asserts the SECOND half, which is the part that makes the first half
 * matter: lastMessageOccurred must move WITH the offset, never independently. A
 * checkpoint whose timestamp came from a different message than its offset is
 * worse than no checkpoint, because it looks authoritative.
 *
 * The statement is imported from the built module rather than copied, so the
 * assertion cannot drift away from the code it guards.
 *
 * Run: node scripts/verify-checkpoint-monotonic.mjs
 */

import pg from 'pg';
import { CHECKPOINT_UPSERT_SQL } from '../services/ticketing/dist/checkpoint-upsert.js';

const APP_URL =
  process.env['TICKETING_APP_DATABASE_URL'] ??
  'postgresql://eventhub_ticketing_app:eventhub_ticketing_app@localhost:5432/eventhub_ticketing';

const run = `cp_${Date.now()}`;
const GROUP = `${run}_group`;
const TOPIC = 'events.activity';
const PARTITION = 0;

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}

const db = new pg.Client({ connectionString: APP_URL, application_name: 'verify-checkpoint' });
db.on('error', () => {});

const at = (offset, occurred) => db.query(CHECKPOINT_UPSERT_SQL, [GROUP, TOPIC, PARTITION, String(offset), occurred]);

async function stored() {
  const { rows } = await db.query(
    `SELECT "lastOffset"::text AS "lastOffset", "lastMessageOccurred"
       FROM "ConsumerCheckpoint"
      WHERE "consumerGroup" = $1 AND topic = $2 AND partition = $3`,
    [GROUP, TOPIC, PARTITION],
  );
  return rows[0];
}

/**
 * Connects as the APP role, and that is not the obvious choice.
 *
 * ConsumerCheckpoint carries no tenantId — a Kafka offset has no tenant — so the
 * first version of this script connected as the table OWNER and set no
 * app.tenant_id, reasoning that there was no tenant context to set. Postgres
 * refused it:
 *
 *   error: new row violates row-level security policy for table "ConsumerCheckpoint"
 *
 * The table's policy is `checkpoint_consumer USING(true) WITH CHECK(true) TO
 * eventhub_ticketing_app` — permissive, but ROLE-TARGETED. Under FORCE ROW
 * LEVEL SECURITY the owner gets no policy at all, so it sees nothing and can
 * write nothing. "No tenant column" is not the same as "no row security", and
 * the role a maintenance script connects as is a decision, not a detail.
 */
async function main() {
  await db.connect();

  const T1 = new Date('2026-01-01T00:00:00Z');
  const T2 = new Date('2026-06-01T00:00:00Z');

  console.log('  a partition consumed to offset 500, then replayed from the start');

  const first = await at(500, T2);
  check(first.rowCount === 1, `the first checkpoint was written (${first.rowCount} row)`);

  // The replay. fromBeginning against a compacted topic delivers offset 0
  // upwards, and every one of those is behind what is already recorded.
  const replay = [];
  for (const offset of [0, 1, 2, 37, 499]) {
    const r = await at(offset, T1);
    replay.push(`${offset}:${r.rowCount}`);
  }
  check(
    replay.every((r) => r.endsWith(':0')),
    `every replayed offset changed nothing (${replay.join(' ')})`,
  );

  const afterReplay = await stored();
  check(
    afterReplay.lastOffset === '500',
    `the checkpoint is still ${afterReplay.lastOffset}, expected 500`,
  );
  check(
    afterReplay.lastMessageOccurred?.toISOString() === T2.toISOString(),
    `lastMessageOccurred is ${afterReplay.lastMessageOccurred?.toISOString()}, expected ${T2.toISOString()} — ` +
      'it must not be dragged back by a replayed message the offset ignored',
  );

  console.log('');
  console.log('  and it must still advance when the replay passes where it was');

  const equal = await at(500, T1);
  check(equal.rowCount === 0, `re-applying the SAME offset changes nothing (${equal.rowCount} rows)`);

  const ahead = await at(501, T2);
  check(ahead.rowCount === 1, `offset 501 advances it (${ahead.rowCount} row)`);

  const final = await stored();
  check(final.lastOffset === '501', `the checkpoint is ${final.lastOffset}, expected 501`);

  console.log('');
  console.log('  the control: the unguarded write this replaced');

  /**
   * The same interleaving against the statement as it used to be. If this ever
   * came out at 500, the replay above would not be exercising anything and
   * every "ok" in this file would be decoration.
   */
  const UNGUARDED = CHECKPOINT_UPSERT_SQL.replace(
    /\s*WHERE "ConsumerCheckpoint"\."lastOffset" < EXCLUDED\."lastOffset"\s*$/,
    '',
  );
  check(
    UNGUARDED !== CHECKPOINT_UPSERT_SQL,
    'the guard clause was found and removed to build the control',
  );

  const controlGroup = `${run}_control`;
  await db.query(UNGUARDED, [controlGroup, TOPIC, PARTITION, '500', T2]);
  await db.query(UNGUARDED, [controlGroup, TOPIC, PARTITION, '0', T1]);
  const { rows } = await db.query(
    `SELECT "lastOffset"::text AS "lastOffset" FROM "ConsumerCheckpoint"
      WHERE "consumerGroup" = $1 AND topic = $2 AND partition = $3`,
    [controlGroup, TOPIC, PARTITION],
  );
  check(
    rows[0].lastOffset === '0',
    rows[0].lastOffset === '0'
      ? 'without the guard it rewound to 0 — the replay really does overwrite'
      : `without the guard it read ${rows[0].lastOffset}, expected 0. This test is not ` +
        'exercising the rewind, so nothing above proves anything',
  );

  await db.query(`DELETE FROM "ConsumerCheckpoint" WHERE "consumerGroup" IN ($1, $2)`, [
    GROUP,
    controlGroup,
  ]);
  await db.end();

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exit(1);
  }
  console.log('the checkpoint advances or stands still, and never rewinds on a replay');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
