/**
 * Asserts the Notification sent-log's two guarantees against the table itself: a
 * claim is made once per tenant however many deliveries race for it, and each
 * tenant's rows are invisible and unwritable to every other tenant.
 *
 * Every later part of Chapter 8 rests on the first. Kafka delivers at least once
 * and an email cannot be recalled, so "send exactly once" comes down to "exactly
 * one INSERT wins". That is a property of a unique constraint, not of any code, so
 * it is checked where it lives, before any code relies on it.
 *
 * THE FIXTURE CAN SHOW EACH FAILURE. Every refusal is checked against a row this
 * run has already shown exists and is visible to its own tenant, so "sees nothing"
 * means the policy hid it, not that there was nothing to see. The cross-tenant
 * claim reuses a key tenant A already holds, so a constraint on the key alone
 * would suppress tenant B's row.
 *
 * Scoped to two tenants named for this run, and removed at the end by the owner,
 * one tenant at a time, because the policy applies to the owner as well.
 *
 * Run (with Postgres up and notification migrated): node scripts/verify-notification-sentlog.mjs
 */

import { randomUUID } from 'node:crypto';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { serviceEnv } from './lib/service-env.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');

// The example's connection strings, unless CI names its own.
const env = serviceEnv(ROOT, 'notification', {
  APP_DATABASE_URL: process.env['NOTIFICATION_APP_DATABASE_URL'],
  DATABASE_URL: process.env['NOTIFICATION_DATABASE_URL'],
});

const RUN = randomUUID().slice(0, 8);
const A = `sentlog_a_${RUN}`;
const B = `sentlog_b_${RUN}`;
const RACERS = 8;

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

const describe = (r) => (r.ok ? `returned ${r.value}` : `failed ${r.code ?? ''}: ${r.message}`);

/**
 * ON CONFLICT DO NOTHING with no target, so it answers to whatever unique
 * constraint the table actually has. Naming (tenantId, idempotencyKey) here would
 * make a table without that constraint fail on the statement, not on the claim.
 *
 * NO RETURNING. A statement that reads the row back makes Postgres hold the new
 * row to the policy's USING expression as well as its WITH CHECK. With RETURNING
 * here, a policy whose WITH CHECK was loosened to `true` still refused the forged
 * insert, and the check below stayed green. rowCount says whether the row went in.
 */
const INSERT = `
  INSERT INTO "NotificationDelivery"
    (id, "tenantId", "recipientId", channel, category, "templateKey", "idempotencyKey", "sourceMessageId")
  VALUES ($1, $2, 'user_1', 'email', 'account', 'password-reset', $3, $4)
  ON CONFLICT DO NOTHING`;

/** A transaction with app.tenant_id set LOCAL, as withTenant does; null leaves it unset. */
async function inTenant(client, tenant, fn) {
  await client.query('BEGIN');
  try {
    if (tenant !== null) await client.query("SELECT set_config('app.tenant_id', $1, true)", [tenant]);
    const value = await fn(client);
    await client.query('COMMIT');
    return { ok: true, value };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    return { ok: false, code: err.code, message: err.message };
  }
}

/**
 * The same, always rolled back, for the statements that must be refused. A policy
 * violation is raised by the statement, not at commit, so never committing loses
 * nothing, and a broken policy cannot leave a forged, moved or deleted row behind.
 * With RLS disabled, the unscoped UPDATE below would otherwise move every row in
 * the table.
 */
async function attempt(client, tenant, fn) {
  await client.query('BEGIN');
  try {
    if (tenant !== null) await client.query("SELECT set_config('app.tenant_id', $1, true)", [tenant]);
    return { ok: true, value: await fn(client) };
  } catch (err) {
    return { ok: false, code: err.code, message: err.message };
  } finally {
    await client.query('ROLLBACK').catch(() => {});
  }
}

/** Claims `key` for a row belonging to `rowTenant`, from a context set to `context`. */
const claim = (client, context, rowTenant, key) =>
  inTenant(client, context, async (c) => (await c.query(INSERT, [randomUUID(), rowTenant, key, `msg_${key}`])).rowCount);

const count = (client, context, where, params) =>
  inTenant(client, context, async (c) =>
    Number((await c.query(`SELECT count(*)::int AS n FROM "NotificationDelivery" WHERE ${where}`, params)).rows[0].n),
  );

async function main() {
  const app = new pg.Client({ connectionString: env['APP_DATABASE_URL'] });
  const owner = new pg.Client({ connectionString: env['DATABASE_URL'] });
  await app.connect();
  await owner.connect();
  const racers = [];

  try {
    console.log('  the fixture: a claim, visible to its own tenant');
    const K1 = `key_1_${RUN}`;
    const first = await claim(app, A, A, K1);
    check(first.ok && first.value === 1, `CONTROL: tenant A claims ${K1} (${describe(first)})`);
    const own = await count(app, A, '"idempotencyKey" = $1', [K1]);
    check(own.ok && own.value === 1, `CONTROL: and reads it back under its own context (${describe(own)})`);
    // Every check below is measured against this row. Without it they all pass.
    if (failures > 0) return;

    console.log('');
    console.log('  row-level security');
    const unset = await count(app, null, '"tenantId" IN ($1, $2)', [A, B]);
    check(unset.ok && unset.value === 0, `with no tenant set, the app role sees none of this run's rows (${describe(unset)})`);

    const across = await count(app, B, '"tenantId" = $1', [A]);
    check(across.ok && across.value === 0, `tenant B sees none of tenant A's rows (${describe(across)})`);

    const forged = await attempt(app, B, async (c) =>
      (await c.query(INSERT, [randomUUID(), A, `key_forged_${RUN}`, `msg_forged_${RUN}`])).rowCount,
    );
    check(!forged.ok && forged.code === '42501', `tenant B cannot insert a row as tenant A (${describe(forged)})`);

    // No WHERE, for the reason INSERT has no RETURNING: a WHERE reads the row, and
    // then USING refuses the new row even where WITH CHECK would allow it. Rolled
    // back whatever happens, because with the policy broken this moves every row the
    // context can see.
    const moved = await attempt(app, A, async (c) =>
      (await c.query('UPDATE "NotificationDelivery" SET "tenantId" = $1', [B])).rowCount,
    );
    check(!moved.ok && moved.code === '42501', `tenant A cannot move its own row into tenant B (${describe(moved)})`);

    const ownerScoped = await count(owner, A, '"tenantId" = $1', [A]);
    check(ownerScoped.ok && ownerScoped.value === 1, `CONTROL: the owner sees tenant A's row under A's context (${describe(ownerScoped)})`);
    const ownerUnset = await count(owner, null, '"tenantId" = $1', [A]);
    check(
      ownerUnset.ok && ownerUnset.value === 0,
      `the owner sees nothing with no tenant set, because FORCE applies the policy to it too (${describe(ownerUnset)})`,
    );

    const deleted = await attempt(app, A, async (c) =>
      (await c.query('DELETE FROM "NotificationDelivery" WHERE "tenantId" = $1', [A])).rowCount,
    );
    check(!deleted.ok && deleted.code === '42501', `the app role cannot delete from the sent-log, even its own rows (${describe(deleted)})`);

    console.log('');
    console.log('  the claim');
    const again = await claim(app, A, A, K1);
    check(again.ok && again.value === 0, `claiming ${K1} a second time inserts nothing and raises nothing (${describe(again)})`);
    const stillOne = await count(app, A, '"idempotencyKey" = $1', [K1]);
    check(stillOne.ok && stillOne.value === 1, `and tenant A still holds exactly one row for it (${describe(stillOne)})`);

    const K2 = `key_2_${RUN}`;
    for (let i = 0; i < RACERS; i++) {
      const c = new pg.Client({ connectionString: env['APP_DATABASE_URL'] });
      racers.push(c);
      await c.connect();
    }
    // Connected first, so the claims start together rather than one per handshake.
    const raced = await Promise.all(racers.map((c) => claim(c, A, A, K2)));
    const errored = raced.filter((r) => !r.ok);
    const winners = raced.filter((r) => r.ok && r.value === 1).length;
    check(
      raced.length === RACERS && errored.length === 0,
      `CONTROL: ${RACERS} concurrent claims of ${K2} all completed${errored.length ? `, but one ${describe(errored[0])}` : ''}`,
    );
    check(winners === 1, `exactly one of them inserted a row: ${winners} did`);

    const theirs = await claim(app, B, B, K1);
    check(theirs.ok && theirs.value === 1, `tenant B claims the key tenant A already holds, and gets its own row (${describe(theirs)})`);
    const ours = await count(app, A, '"idempotencyKey" = $1', [K1]);
    check(ours.ok && ours.value === 1, `and tenant A's row for that key is untouched (${describe(ours)})`);
  } finally {
    let left = 0;
    for (const tenant of [A, B]) {
      await inTenant(owner, tenant, (c) => c.query('DELETE FROM "NotificationDelivery" WHERE "tenantId" = $1', [tenant]));
      const r = await count(owner, tenant, '"tenantId" = $1', [tenant]);
      left += r.ok ? r.value : 1;
    }
    check(left === 0, `cleanup: this run's rows are gone (${left} left)`);
    await Promise.all([app, owner, ...racers].map((c) => c.end().catch(() => {})));
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('a delivery is claimed once per tenant, and no tenant can see or write another tenant\'s claims');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
