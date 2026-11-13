/**
 * Verifies that the payload the Event service actually writes can be encoded
 * against the registered Avro schema.
 *
 * This is the cheapest guard in Chapter 4 and it covers the link with no other
 * coverage. Everything else is checked somewhere: the database has constraints
 * and RLS assertions, the HTTP layer has smoke tests, Kafka has a round-trip.
 * But "does the JSON we store in the outbox match the schema we will encode it
 * with?" is checked nowhere — and its failure mode is a poison message. A row
 * that cannot be serialised is re-selected on every drain, fails, is re-selected
 * forever, and nothing behind it is ever published. The only symptom is a
 * growing backlog and a repeating log line.
 *
 * That failure is guaranteed by construction if the schema uses timestamp-millis
 * while the payload stores ISO-8601 strings, which is exactly the trap this
 * project's schema avoids on purpose.
 *
 * Run: node scripts/verify-schema-contract.mjs
 */

import avro from 'avsc';
import { readFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');
const schema = JSON.parse(
  readFileSync(join(ROOT, 'contracts', 'avro', 'events.activity-value.avsc'), 'utf8'),
);
const type = avro.Type.forSchema(schema);

/**
 * The exact object shape emitEventSnapshot builds in
 * services/event/src/outbox.ts, plus the envelope fields the relay adds from
 * columns the row already has (messageId from OutboxMessage.id, occurredAt from
 * createdAt).
 *
 * Kept in sync by hand today. Once the relay exists this becomes a real test
 * against a row read out of the database.
 */
const sample = {
  messageId: 'cmt8sr74s0000y0vdoy3i7wc4',
  occurredAt: '2026-08-25T14:58:30.630Z',
  eventId: 'cmt8sr74s0000y0vdoy3i7wc4',
  tenantId: 'tenant_acme',
  title: 'Emission Test',
  slug: 'emission-test',
  description: null,
  status: 'PUBLISHED',
  capacity: 50,
  startsAt: '2027-03-01T10:00:00.000Z',
  endsAt: '2027-03-01T18:00:00.000Z',
  organiserId: 'cmt4dge5x0000i4vdzxxegdkd',
  version: 4,
  changeType: 'PUBLISHED',
  deleted: false,
};

function check(label, obj, expectValid) {
  const valid = type.isValid(obj, { errorHook: () => {} });
  const ok = valid === expectValid;
  console.log(`  ${ok ? 'ok  ' : 'FAIL'}  ${label}${valid === expectValid ? '' : `  (valid=${valid}, expected ${expectValid})`}`);
  if (!ok) process.exitCode = 1;
  return ok;
}

let allOk = true;

console.log('encoding the real payload shape:');
allOk = check('full snapshot encodes', sample, true) && allOk;

const buf = type.toBuffer(sample);
const back = type.fromBuffer(buf);
allOk = check('round-trips unchanged', back, true) && allOk;
if (JSON.stringify(back) !== JSON.stringify(sample)) {
  console.log('  FAIL  round-trip changed the value');
  console.log('        in :', JSON.stringify(sample));
  console.log('        out:', JSON.stringify(back));
  allOk = false;
  process.exitCode = 1;
}
console.log(`        ${buf.length} bytes encoded (JSON would be ${Buffer.byteLength(JSON.stringify(sample))})`);

console.log('');
console.log('rejecting what should be rejected:');
allOk = check('missing required field', { ...sample, eventId: undefined }, false) && allOk;
allOk = check('unknown status symbol', { ...sample, status: 'ARCHIVED' }, false) && allOk;
allOk = check('capacity as a string', { ...sample, capacity: '50' }, false) && allOk;
allOk = check('a Date where a string is required', { ...sample, startsAt: new Date() }, false) && allOk;

console.log('');
if (!allOk) {
  console.error('schema contract verification FAILED');
  process.exit(1);
}
console.log('schema contract verified');
