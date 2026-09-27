/**
 * Asserts that a delivery claimed and never finished is ended, and that nothing else is.
 *
 * A delivery is claimed PENDING before anything is sent. Kafka re-drives nearly all of
 * them; the ones that never move again are the ones whose message was dead-lettered or
 * whose retention passed. The sweep gives those a terminal state, so PENDING means "in
 * flight" and a count of PENDING rows is worth alerting on.
 *
 * EACH PROPERTY, AND WHAT WOULD LOOK THE SAME WITHOUT IT:
 *   - Old PENDING rows are ended. A YOUNG PENDING row sits beside them in the same
 *     table and must survive: "the row changed" is also what sweeping everything does.
 *   - SENT, FAILED and SUPPRESSED rows are untouched, so the sweep cannot rewrite an
 *     outcome that was already decided.
 *   - It crosses tenants. Two tenants' stuck rows go in one pass, which the tenant
 *     policy the app role uses could not do.
 *   - Its credential is bounded: it cannot create a delivery, cannot read the
 *     suppression list, and cannot mark anything SENT — the last of those is the
 *     policy's WITH CHECK, not a grant. The CONTROL is that it CAN end one.
 *   - The service sweeps on its own clock, not only when a person runs the script.
 *   - The service REFUSES TO START on a credential that can do more than sweep.
 *
 * The fixtures are written straight into the sent-log as the owner, backdated: this is
 * about what the sweep does to rows, and building them through Kafka would prove the
 * same thing far more slowly. scripts/verify-email-retries.mjs already shows a real
 * dead-lettered delivery being left PENDING, which is the row this ends.
 *
 * The compose Notification, if it is running, sweeps with its own hour-long threshold
 * and leaves these fixtures (seconds old) alone.
 *
 * THE OTHER WAY ROUND IS TRUE TOO: this run sweeps with a five-second threshold, and
 * the sweep crosses tenants by design, so any delivery anywhere in this database that
 * has sat PENDING for longer than that is ended as well. That is the sweep doing exactly
 * what it is for, so the count it reports is checked against every tenant at the sweep's
 * own instant, never against the number of rows this run planted.
 *
 * Run (with Postgres, Kafka and the Schema Registry up, Notification built and
 * migrated): node scripts/verify-notification-sweeper.mjs
 */

import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
// The reason the sweep writes, from the service itself rather than copied here, so the
// rows a pass ended are found by exactly what it wrote.
import { ABANDONED } from '../services/notification/dist/sweeper/sweep.js';
import { serviceEnv } from './lib/service-env.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');

const RUN = randomBytes(4).toString('hex');
const TENANT = `tenant_sweep_${RUN}`;
const OTHER_TENANT = `tenant_sweep_other_${RUN}`;
const NOTIFICATION_PORT = 3391;
/** Seconds, not the hour the service ships with: the fixtures are backdated past this and no further. */
const STUCK_AFTER_MS = 5_000;

const notificationEnv = serviceEnv(ROOT, 'notification', {
  PORT: String(NOTIFICATION_PORT),
  KAFKA_CONSUMER_GROUP: `verify-sweeper-${RUN}`,
  DISPATCH_START_FROM: 'latest',
  SWEEP_STUCK_AFTER_MS: String(STUCK_AFTER_MS),
  SWEEP_INTERVAL_MS: '1000',
  // Generated rather than configured, so .env.example leaves it empty: the same
  // exemption scripts/verify-env-example.mjs prints.
  AUTH_PUBLIC_KEY_B64: Buffer.from(readFileSync(join(ROOT, 'services', 'auth', 'keys', 'public.pem'), 'utf8')).toString('base64'),
});

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const excerpt = (log) => (log.length <= 3000 ? log : `${log.slice(0, 2000)}\n  [...]\n${log.slice(-1000)}`);

async function waitFor(fn, timeoutMs = 30_000, stepMs = 250) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await fn().catch(() => null);
    if (value) return value;
    await sleep(stepMs);
  }
  return null;
}

function start(args, env) {
  const child = spawn(process.execPath, args, { cwd: join(ROOT, 'services', 'notification'), env, stdio: ['ignore', 'pipe', 'pipe'] });
  let log = '';
  child.stdout.on('data', (d) => (log += d));
  child.stderr.on('data', (d) => (log += d));
  return { child, log: () => log };
}

async function inTenant(client, tenant, sql, params) {
  await client.query('BEGIN');
  try {
    await client.query("SELECT set_config('app.tenant_id', $1, true)", [tenant]);
    const result = await client.query(sql, params);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  }
}

async function main() {
  const taken = await fetch(`http://127.0.0.1:${NOTIFICATION_PORT}/`).then(
    () => true,
    () => false,
  );
  if (taken) {
    console.error(`FAIL: something already answers on :${NOTIFICATION_PORT}. Stop it before running this.`);
    process.exitCode = 1;
    return;
  }

  const owner = new pg.Client({ connectionString: notificationEnv.DATABASE_URL });
  const sweeperRole = new pg.Client({ connectionString: notificationEnv.SWEEPER_DATABASE_URL });
  await Promise.all([owner.connect(), sweeperRole.connect()]);

  /** One delivery row, written as the owner with its age chosen. */
  const put = async (tenant, name, status, ageMs) => {
    const id = `sweep_${name}_${RUN}`;
    await inTenant(
      owner,
      tenant,
      `INSERT INTO "NotificationDelivery"
         (id, "tenantId", "recipientId", channel, category, "templateKey", "idempotencyKey", "sourceMessageId", status, "createdAt")
       VALUES ($1, $2, $3, 'email', 'account', 'password-reset', $4, $5, $6::"DeliveryStatus", now() - ($7::double precision * interval '1 millisecond'))`,
      [id, tenant, `user_${name}_${RUN}`, `${id}:email:user_${name}_${RUN}`, `msg_${name}_${RUN}`, status, ageMs],
    );
    return id;
  };
  const rowsOf = async (tenant) =>
    (await inTenant(owner, tenant, `SELECT d.id, d.status::text AS status, d."lastError", d."failedAt" FROM "NotificationDelivery" d WHERE d."tenantId" = $1`, [tenant])).rows;
  const rowById = async (tenant, id) => (await rowsOf(tenant)).find((r) => r.id === id);

  const notification = { child: { kill: () => {} }, log: () => '' };
  let service = notification;

  try {
    console.log('  the fixture');
    const stuck = await put(TENANT, 'stuck', 'PENDING', 60_000);
    const young = await put(TENANT, 'young', 'PENDING', 0);
    const sent = await put(TENANT, 'sent', 'SENT', 60_000);
    const failed = await put(TENANT, 'failed', 'FAILED', 60_000);
    const suppressed = await put(TENANT, 'suppressed', 'SUPPRESSED', 60_000);
    const otherStuck = await put(OTHER_TENANT, 'other', 'PENDING', 60_000);
    check((await rowsOf(TENANT)).length === 5 && (await rowsOf(OTHER_TENANT)).length === 1, 'CONTROL: six deliveries written, five in one tenant and one in another');
    check((await rowById(TENANT, stuck))?.status === 'PENDING' && (await rowById(TENANT, young))?.status === 'PENDING', 'CONTROL: the old and the young delivery both start PENDING');

    console.log('');
    console.log('  one sweep by hand ends what is stuck, in every tenant, and nothing else');
    const once = await new Promise((done) => {
      const child = spawn(`pnpm --filter @eventhub/notification run sweep:once`, {
        cwd: ROOT,
        env: { ...process.env, ...notificationEnv },
        stdio: ['ignore', 'pipe', 'pipe'],
        shell: true,
      });
      let log = '';
      child.stdout.on('data', (d) => (log += d));
      child.stderr.on('data', (d) => (log += d));
      child.on('exit', (code) => done({ code, log }));
    });
    /**
     * EXACTLY THE STUCK DELIVERIES, IN EVERY TENANT, JUDGED AT THE SWEEP'S OWN INSTANT.
     *
     * This used to require "abandoned 2 deliveries", the two this run planted. The sweep
     * crosses tenants by design, so any delivery in this database left PENDING past the
     * threshold is its business, and that count was true only of a database this run had
     * to itself: it failed on a third tenant's stuck delivery, which the sweep was right
     * to end. Nor can the stuck rows be counted just before the sweep: pnpm and a new
     * process start in between, and another verifier's delivery that crosses the threshold
     * in that gap is swept correctly and then counted as a surprise.
     *
     * So the pass is found by its own mark. One UPDATE ends every row it ends, under one
     * now(), so the failedAt it gave this run's stuck delivery names the pass. The sweeper
     * role reads every tenant, and says, against that same instant: how many rows the
     * pass ended, whether any was younger than the threshold, and whether any delivery
     * that was already stuck then is still PENDING. The instant stays in SQL throughout.
     */
    const reported = Number(once.log.match(/abandoned (\d+) deliver/)?.[1] ?? NaN);
    const at = (await sweeperRole.query('SELECT d."failedAt"::text AS at FROM "NotificationDelivery" d WHERE d.id = $1', [stuck])).rows[0]?.at ?? null;
    const pass = at === null ? null : (await sweeperRole.query(
      `SELECT count(*)::int AS ended,
              count(*) FILTER (WHERE d."createdAt" >= $1::timestamp - ($2::double precision * interval '1 millisecond'))::int AS young,
              count(*) FILTER (WHERE d.id = ANY($4::text[]))::int AS ours
         FROM "NotificationDelivery" d
        WHERE d."failedAt" = $1::timestamp AND d."lastError" = $3`,
      [at, STUCK_AFTER_MS, ABANDONED, [stuck, otherStuck]],
    )).rows[0];
    const leftStuck = at === null ? null : (await sweeperRole.query(
      `SELECT count(*)::int AS n FROM "NotificationDelivery" d
        WHERE d.status = 'PENDING' AND d."createdAt" < $1::timestamp - ($2::double precision * interval '1 millisecond')`,
      [at, STUCK_AFTER_MS],
    )).rows[0].n;
    check(
      once.code === 0 && pass !== null && reported === pass.ended && pass.ours === 2,
      `sweep:once reported exactly the deliveries its pass ended, in every tenant (exit ${once.code}: "${once.log.trim().split(/\r?\n/).pop()}"; ` +
        `the pass at ${at ?? 'NO PASS FOUND'} ended ${pass?.ended ?? 'none'}, this run's two stuck ones among them: ${pass?.ours ?? 0} of 2)`,
    );
    check(pass !== null && pass.young === 0, `and not one of them was younger than the threshold at that instant (${pass?.young ?? 'no pass'})`);
    check(leftStuck === 0, `and no delivery in any tenant that was already stuck at that instant is still PENDING (${leftStuck ?? 'no pass'})`);

    const sweptRow = await rowById(TENANT, stuck);
    check(sweptRow?.status === 'FAILED' && /abandoned/.test(sweptRow.lastError ?? '') && Boolean(sweptRow.failedAt), `the stuck delivery is FAILED, with a reason and a time (${sweptRow?.status}: ${sweptRow?.lastError})`);
    check((await rowById(OTHER_TENANT, otherStuck))?.status === 'FAILED', "the other tenant's stuck delivery went in the same pass, which a tenant-scoped role could not do");
    check((await rowById(TENANT, young))?.status === 'PENDING', 'CONTROL: the young delivery beside it is untouched, so it is the age that decides');
    // One at a time: these share a client, and pg refuses to have two queries in
    // flight on one connection.
    const untouched = [await rowById(TENANT, sent), await rowById(TENANT, failed), await rowById(TENANT, suppressed)];
    check(untouched[0]?.status === 'SENT' && untouched[1]?.status === 'FAILED' && untouched[2]?.status === 'SUPPRESSED', `SENT, FAILED and SUPPRESSED rows are untouched (${untouched.map((r) => r?.status).join(', ')})`);
    check(untouched[1]?.lastError === null, 'and the FAILED one kept its own reason rather than being rewritten');

    console.log('');
    console.log('  the sweeper credential can end a delivery and do nothing else');
    const refusal = async (sql, params) => {
      await sweeperRole.query('BEGIN');
      try {
        await sweeperRole.query(sql, params);
        return null;
      } catch (err) {
        return err.code ?? String(err.message ?? err);
      } finally {
        await sweeperRole.query('ROLLBACK').catch(() => {});
      }
    };
    const creating = await refusal(
      `INSERT INTO "NotificationDelivery" (id, "tenantId", "recipientId", channel, category, "templateKey", "idempotencyKey", "sourceMessageId")
       VALUES ($1, $2, 'probe', 'email', 'account', 'password-reset', $1, 'probe')`,
      [`sweep_probe_${RUN}`, TENANT],
    );
    check(creating === '42501', `it cannot create a delivery (${creating})`);
    const reading = await refusal('SELECT 1 FROM "EmailSuppression" LIMIT 1');
    check(reading === '42501', `it cannot read the suppression list (${reading})`);
    const sending = await refusal(`UPDATE "NotificationDelivery" SET status = 'SENT' WHERE id = $1`, [young]);
    check(sending === '42501', `it cannot mark a delivery SENT, which is the policy's WITH CHECK rather than a grant (${sending})`);
    const ending = await refusal(`UPDATE "NotificationDelivery" SET status = 'FAILED', "lastError" = 'probe' WHERE id = $1`, [young]);
    check(ending === null, `CONTROL: it can end one, so the refusals above are its bounds and not a broken statement (${ending ?? 'allowed'})`);

    console.log('');
    console.log('  the service sweeps on its own clock');
    const later = await put(TENANT, 'later', 'PENDING', 60_000);
    service = start(['dist/index.js'], notificationEnv);
    const swept = await waitFor(async () => {
      const metrics = await (await fetch(`http://127.0.0.1:${NOTIFICATION_PORT}/metrics`)).json();
      return metrics.sweep && metrics.sweep.abandonedTotal >= 1 ? metrics.sweep : null;
    }, 60_000, 500);
    check(Boolean(swept), swept ? `it swept without being asked (${swept.abandonedTotal} in ${swept.passesTotal} pass(es))` : `no sweep was reported:\n${excerpt(service.log())}`);
    check((await rowById(TENANT, later))?.status === 'FAILED', 'and the delivery it found is FAILED');
    check(swept?.failuresTotal === 0 && typeof swept?.oldestPendingMs === 'number', `/metrics reports the oldest delivery still pending (${swept?.oldestPendingMs} ms) with no failed passes`);
    service.child.kill();

    console.log('');
    console.log('  the service refuses to sweep on a credential that can do more');
    const wide = start(['dist/index.js'], { ...notificationEnv, PORT: String(NOTIFICATION_PORT + 1), SWEEPER_DATABASE_URL: notificationEnv.APP_DATABASE_URL });
    const exited = await waitFor(async () => (wide.child.exitCode !== null ? { code: wide.child.exitCode } : null), 60_000, 250);
    check(Boolean(exited) && exited.code !== 0, `given the app role instead, it exits rather than sweeping (${exited ? `code ${exited.code}` : 'still running'})`);
    // The app role is caught by the first probe it fails, which is the insert: the
    // probe sets a tenant first, so row security cannot refuse it on the role's behalf.
    check(/refusing to sweep: the sweeper credential can create deliveries/.test(wide.log()), `and names the privilege that stopped it${/refusing to sweep/.test(wide.log()) ? '' : `:\n${excerpt(wide.log())}`}`);
    wide.child.kill();
  } catch (err) {
    check(false, `the run stopped early: ${err instanceof Error ? err.message : String(err)}`);
    console.error(`  Notification's output:\n${excerpt(service.log())}`);
  } finally {
    service.child.kill();
    // In each row's own tenant, the context it was written in, and reported by what was
    // DELETED: "0 left" is also what a delete that matched nothing prints.
    let deleted = 0;
    let left = 0;
    for (const tenant of [TENANT, OTHER_TENANT]) {
      deleted += (await inTenant(owner, tenant, 'DELETE FROM "NotificationDelivery" d WHERE d."tenantId" = $1', [tenant]).catch(() => ({ rowCount: 0 }))).rowCount ?? 0;
      left += (await rowsOf(tenant).catch(() => [])).length;
    }
    check(deleted > 0 && left === 0, `cleanup: this run's deliveries are gone (${deleted} deleted, ${left} left)`);
    await Promise.all([owner.end().catch(() => {}), sweeperRole.end().catch(() => {})]);
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('a delivery claimed and never finished is ended, in every tenant, by a credential that can do nothing else');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
