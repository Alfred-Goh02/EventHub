/**
 * Asserts what Community's database promises before a single route exists.
 *
 * EACH PROPERTY, AND WHAT WOULD LOOK THE SAME WITHOUT IT:
 *   - Every tenant-scoped table isolates. THE TABLE LIST IS READ FROM THE DATABASE,
 *     not written here, so a table added later with no policy fails this rather than
 *     being quietly exempt. Each table gets a row in two tenants, and each is read
 *     back with one tenant set.
 *   - A row cannot be written into another tenant. WITH CHECK is asked for separately,
 *     because USING alone would hide the row while still storing it.
 *   - The moderation trail is append-only at the privilege level: UPDATE and DELETE
 *     are refused to the service's role, and the CONTROL is that INSERT is allowed.
 *   - A thread cannot nest past the cap, and a subtree is one ltree query.
 *   - The outbox has two readers with different reach: the service may insert and read
 *     its own tenant; the relay may read every tenant and write only delivery state.
 *   - Every unique index leads with tenantId, because a unique index is not filtered by
 *     policy: a key without it lets one tenant's insert collide with a row it cannot
 *     see. Also derived from the database.
 *   - The service refuses to start on a credential that can bypass row security.
 *
 * Scoped to two tenants created for this run; every row is removed at the end.
 *
 * Run (with Postgres up and Community built and migrated):
 *   node scripts/verify-community-foundation.mjs
 */

import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';
import { serviceEnv } from './lib/service-env.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');

const RUN = randomBytes(4).toString('hex');
const TENANT = `tenant_community_${RUN}`;
const OTHER_TENANT = `tenant_community_other_${RUN}`;
const PORT = 3812;

const communityEnv = serviceEnv(ROOT, 'community', {
  PORT: String(PORT),
  // No consumer: this run is about the database's own rules, and a consumer started
  // here would join the shared group and commit other runs' offsets.
  FEED_CONSUMER_ENABLED: 'false',
  // Generated rather than configured, so .env.example leaves it empty.
  AUTH_PUBLIC_KEY_B64: Buffer.from(readFileSync(join(ROOT, 'services', 'auth', 'keys', 'public.pem'), 'utf8')).toString('base64'),
});
/** The relay's own credential, as compose spells it. */
const RELAY_URL =
  process.env['COMMUNITY_RELAY_DATABASE_URL'] ??
  'postgresql://eventhub_community_relay:eventhub_community_relay@127.0.0.1:5432/eventhub_community';

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const excerpt = (log) => (log.length <= 2000 ? log : `${log.slice(0, 1500)}\n  [...]\n${log.slice(-500)}`);

async function waitFor(fn, timeoutMs = 30_000, stepMs = 250) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await fn().catch(() => null);
    if (value) return value;
    await sleep(stepMs);
  }
  return null;
}

function start(env) {
  const child = spawn(process.execPath, ['dist/index.js'], {
    cwd: join(ROOT, 'services', 'community'),
    env,
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  let log = '';
  child.stdout.on('data', (d) => (log += d));
  child.stderr.on('data', (d) => (log += d));
  return { child, log: () => log };
}

/** One statement with a tenant set, rolled back if it throws. Resolves to the SQLSTATE when refused. */
async function inTenant(client, tenant, sql, params = []) {
  await client.query('BEGIN');
  try {
    const result = await client.query(sql, params);
    await client.query('COMMIT');
    return { rows: result.rows, rowCount: result.rowCount, code: null };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    return { rows: [], rowCount: 0, code: err.code ?? String(err.message ?? err) };
  }
}

/** BEGIN, set the tenant, run, COMMIT — the shape the service uses. */
async function asTenant(client, tenant, sql, params = []) {
  await client.query('BEGIN');
  try {
    await client.query("SELECT set_config('app.tenant_id', $1, true)", [tenant]);
    const result = await client.query(sql, params);
    await client.query('COMMIT');
    return { rows: result.rows, rowCount: result.rowCount, code: null };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    return { rows: [], rowCount: 0, code: err.code ?? String(err.message ?? err) };
  }
}

async function main() {
  const app = new pg.Client({ connectionString: communityEnv.APP_DATABASE_URL });
  const owner = new pg.Client({ connectionString: communityEnv.DATABASE_URL });
  const relay = new pg.Client({ connectionString: RELAY_URL });
  await Promise.all([app.connect(), owner.connect(), relay.connect()]);

  const service = start(communityEnv);

  try {
    console.log('  the service and its credential');
    const ready = await waitFor(async () => (await fetch(`http://127.0.0.1:${PORT}/ready`)).ok, 60_000, 500);
    check(Boolean(ready), ready ? `CONTROL: a private Community is ready on :${PORT}` : `it never became ready:\n${excerpt(service.log())}`);
    const role = await app.query('SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname = current_user');
    check(role.rows[0]?.rolsuper === false && role.rows[0]?.rolbypassrls === false, 'CONTROL: it connects as a role that cannot bypass row security');
    if (failures > 0) return;

    console.log('');
    console.log('  every tenant-scoped table isolates, and the list comes from the database');
    // Read rather than written here: a table added later with no policy has to fail
    // this check, and a hand-kept list is exactly what would let it pass.
    const tables = (
      await owner.query(`
        SELECT c.relname AS table
          FROM pg_class c
         WHERE c.relnamespace = 'public'::regnamespace AND c.relkind = 'r'
           AND c.relname <> '_prisma_migrations'
         ORDER BY c.relname`)
    ).rows.map((r) => r.table);
    check(tables.length === 9, `CONTROL: it found ${tables.length} tables to check (${tables.join(', ')})`);

    /** One row per table, with only the columns that table requires. */
    const insert = (table, tenant, id) => {
      const rows = {
        CommunitySpace: [`INSERT INTO "CommunitySpace" (id, "tenantId", slug, name, "updatedAt") VALUES ($1, $2, $3, 'A space', now())`, [id, tenant, `space-${id}`]],
        Membership: [`INSERT INTO "Membership" (id, "tenantId", "spaceId", "userId") VALUES ($1, $2, $3, $4)`, [id, tenant, `space_${RUN}`, `user_${id}`]],
        Post: [`INSERT INTO "Post" (id, "tenantId", "spaceId", "authorId", body) VALUES ($1, $2, $3, 'author', 'hello')`, [id, tenant, `space_${RUN}`]],
        Comment: [`INSERT INTO "Comment" (id, "tenantId", "postId", "authorId", path, depth, body) VALUES ($1, $2, $3, 'author', $4::ltree, 1, 'hi')`, [id, tenant, `post_${RUN}`, id.replace(/-/g, '')]],
        Reaction: [`INSERT INTO "Reaction" (id, "tenantId", "targetType", "targetId", "userId", kind) VALUES ($1, $2, 'POST', $3, $4, 'LIKE')`, [id, tenant, `post_${RUN}`, `user_${id}`]],
        Report: [`INSERT INTO "Report" (id, "tenantId", "targetType", "targetId", "reporterId", reason) VALUES ($1, $2, 'POST', $3, $4, 'SPAM')`, [id, tenant, `post_${RUN}`, `user_${id}`]],
        ModerationAction: [`INSERT INTO "ModerationAction" (id, "tenantId", "spaceId", "actorId", "targetType", "targetId", action) VALUES ($1, $2, $3, 'mod', 'POST', $4, 'HIDE')`, [id, tenant, `space_${RUN}`, `post_${RUN}`]],
        FeedEntry: [`INSERT INTO "FeedEntry" (id, "tenantId", "userId", "postId", "spaceId", "rankScore") VALUES ($1, $2, $3, $4, $5, 1)`, [id, tenant, `user_${id}`, `post_${id}`, `space_${RUN}`]],
        OutboxMessage: [`INSERT INTO "OutboxMessage" (id, "tenantId", topic, "eventType", "aggregateId", payload) VALUES ($1, $2, 'community.feed', 'PostCreated', $3, '{}'::jsonb)`, [id, tenant, `post_${RUN}`]],
      };
      return rows[table];
    };

    // A label per tenant, not a slice of its name: both names end in the same run id,
    // so slicing gave the two tenants the same row ids and the second seed collided.
    for (const [label, tenant] of [['a', TENANT], ['b', OTHER_TENANT]]) {
      for (const table of tables) {
        const [sql, params] = insert(table, tenant, `${table.toLowerCase()}_${label}_${RUN}`);
        const written = await asTenant(app, tenant, sql, params);
        if (written.code) check(false, `could not seed ${table} for ${tenant}: ${written.code}`);
      }
    }
    if (failures > 0) return;

    const leaks = [];
    for (const table of tables) {
      const seen = await asTenant(app, TENANT, `SELECT "tenantId" FROM "${table}"`);
      const foreign = seen.rows.filter((r) => r.tenantId !== TENANT);
      if (seen.rows.length === 0 || foreign.length > 0) leaks.push(`${table} (${seen.rows.length} rows, ${foreign.length} from another tenant)`);
    }
    check(leaks.length === 0, leaks.length === 0 ? `with one tenant set, each of the ${tables.length} tables shows that tenant's row and no other` : `these tables did not isolate: ${leaks.join('; ')}`);

    const smuggled = await asTenant(
      app,
      TENANT,
      `INSERT INTO "Post" (id, "tenantId", "spaceId", "authorId", body) VALUES ($1, $2, 'space', 'author', 'smuggled')`,
      [`post_smuggled_${RUN}`, OTHER_TENANT],
    );
    check(smuggled.code === '42501', `a row cannot be written into another tenant (${smuggled.code})`);

    console.log('');
    console.log('  the moderation trail is append-only, by privilege');
    const rewrite = await asTenant(app, TENANT, `UPDATE "ModerationAction" SET reason = 'rewritten'`);
    check(rewrite.code === '42501', `it cannot be updated (${rewrite.code})`);
    const erase = await asTenant(app, TENANT, `DELETE FROM "ModerationAction"`);
    check(erase.code === '42501', `nor deleted (${erase.code})`);
    const added = await asTenant(
      app,
      TENANT,
      `INSERT INTO "ModerationAction" (id, "tenantId", "spaceId", "actorId", "targetType", "targetId", action) VALUES ($1, $2, 'space', 'mod', 'POST', 'post', 'UNHIDE')`,
      [`mod_second_${RUN}`, TENANT],
    );
    check(added.code === null, `CONTROL: but it can be added to, so the refusals are the grants (${added.code ?? 'allowed'})`);

    console.log('');
    console.log('  a thread has a floor and a ceiling');
    const root = `c1${RUN}`;
    const child = `c2${RUN}`;
    const grand = `c3${RUN}`;
    for (const [id, path, depth] of [[root, root, 1], [child, `${root}.${child}`, 2], [grand, `${root}.${child}.${grand}`, 3]]) {
      const written = await asTenant(
        app,
        TENANT,
        `INSERT INTO "Comment" (id, "tenantId", "postId", "authorId", path, depth, body) VALUES ($1, $2, $3, 'author', $4::ltree, $5, 'reply')`,
        [id, TENANT, `post_thread_${RUN}`, path, depth],
      );
      if (written.code) check(false, `could not build the thread at depth ${depth}: ${written.code}`);
    }
    const subtree = await asTenant(app, TENANT, `SELECT id FROM "Comment" WHERE path <@ $1::ltree ORDER BY path`, [root]);
    check(subtree.rows.length === 3, `a subtree is one query: the root and its two descendants (${subtree.rows.length})`);
    const tooDeep = Array.from({ length: 9 }, (_, i) => `d${i}${RUN}`).join('.');
    const refused = await asTenant(
      app,
      TENANT,
      `INSERT INTO "Comment" (id, "tenantId", "postId", "authorId", path, depth, body) VALUES ($1, $2, $3, 'author', $4::ltree, 9, 'too deep')`,
      [`deep_${RUN}`, TENANT, `post_thread_${RUN}`, tooDeep],
    );
    check(refused.code === '23514', `and nine levels is refused by the database, not by whoever wrote the handler (${refused.code})`);

    console.log('');
    console.log('  the outbox has two readers with different reach');
    const serviceUpdate = await asTenant(app, TENANT, `UPDATE "OutboxMessage" SET "publishedAt" = now()`);
    check(serviceUpdate.code === '42501', `the service cannot mark its own message published (${serviceUpdate.code})`);
    const relaySees = await inTenant(relay, TENANT, `SELECT DISTINCT "tenantId" FROM "OutboxMessage" WHERE "tenantId" IN ($1, $2)`, [TENANT, OTHER_TENANT]);
    check(relaySees.rows.length === 2, `the relay reads both tenants' messages, which is what draining means (${relaySees.rows.length})`);
    const relayRewrite = await inTenant(relay, TENANT, `UPDATE "OutboxMessage" SET payload = '{"tampered":true}'::jsonb WHERE "tenantId" = $1`, [TENANT]);
    check(relayRewrite.code === '42501', `and cannot rewrite a payload (${relayRewrite.code})`);
    const relayReadsPosts = await inTenant(relay, TENANT, `SELECT 1 FROM "Post" LIMIT 1`);
    check(relayReadsPosts.code === '42501', `nor read the posts themselves (${relayReadsPosts.code})`);
    const relayMarks = await inTenant(relay, TENANT, `UPDATE "OutboxMessage" SET "publishedAt" = now() WHERE "tenantId" = $1`, [TENANT]);
    check(relayMarks.code === null, `CONTROL: it can mark delivery state, so the refusals above are its column grants (${relayMarks.code ?? 'allowed'})`);

    console.log('');
    console.log('  every unique key leads with the tenant');
    const uniques = (
      await owner.query(`
        SELECT c.relname AS table, i.relname AS index, pg_get_indexdef(x.indexrelid) AS def
          FROM pg_index x
          JOIN pg_class c ON c.oid = x.indrelid
          JOIN pg_class i ON i.oid = x.indexrelid
         WHERE c.relnamespace = 'public'::regnamespace AND x.indisunique
           AND c.relname <> '_prisma_migrations'
         ORDER BY c.relname, i.relname`)
    ).rows;
    // The primary keys are on a cuid, which is unique on its own; what matters is
    // every OTHER unique key, where a collision would be one tenant learning about
    // another tenant's row.
    const composite = uniques.filter((u) => !u.def.includes('(id)'));
    const missing = composite.filter((u) => !u.def.includes('"tenantId"'));
    check(composite.length >= 5, `CONTROL: ${composite.length} unique keys beyond the primary keys were found to check`);
    check(missing.length === 0, missing.length === 0 ? 'and each one starts from the tenant' : `these omit tenantId: ${missing.map((m) => m.index).join(', ')}`);

    console.log('');
    console.log('  it refuses a credential that is more than it should be');
    // The OWNER, not a superuser: FORCE row security filters the owner too, so the
    // bypass check alone passes for it — and it can still drop the policy or turn
    // FORCE off, which is the reason the service must not run as it.
    const wide = start({ ...communityEnv, PORT: String(PORT + 1), APP_DATABASE_URL: communityEnv.DATABASE_URL });
    const exited = await waitFor(async () => (wide.child.exitCode !== null ? { code: wide.child.exitCode } : null), 60_000, 250);
    check(Boolean(exited) && exited.code !== 0, `given the owner's connection instead, it exits rather than serving (${exited ? `code ${exited.code}` : 'still running'})`);
    check(/OWNS \d+ table/.test(wide.log()), `and says which privilege stopped it${/OWNS \d+ table/.test(wide.log()) ? '' : `:\n${excerpt(wide.log())}`}`);
    wide.child.kill();
  } catch (err) {
    check(false, `the run stopped early: ${err instanceof Error ? err.message : String(err)}`);
    console.error(`  Community's output:\n${excerpt(service.log())}`);
  } finally {
    service.child.kill();
    // As the owner, which FORCE row security binds too, so each tenant is cleared
    // with its own context — the same way the service would.
    /**
     * THE CLEANUP HAS TO SET THE TENANT, AND HAS TO PROVE IT DELETED SOMETHING.
     *
     * This deleted nothing for several chapters and said so was fine. FORCE row
     * security binds the owner too, so a DELETE with no app.tenant_id matches zero
     * rows — and the count below asked through the SAME filter, so it saw an empty
     * table and reported "0 left". A cleanup that deletes nothing and a cleanup that
     * works looked identical.
     *
     * What it cost: every run left two OutboxMessage rows on community.feed with a
     * payload of '{}', which no relay can ever encode. A later verifier's relay pass
     * claimed one, dead-lettered it and exited 1 — and that is how this was found,
     * eight rows and four abandoned runs later. Locally it hid because compose's
     * relay-community had already dead-lettered them; CI runs infrastructure only,
     * so there the leftovers were still waiting.
     *
     * `deleted > 0` is the positive control. Without it "0 deleted, 0 left" passes,
     * which is precisely the bug.
     */
    const tables = ['CommunitySpace', 'Membership', 'Post', 'Comment', 'Reaction', 'Report', 'ModerationAction', 'FeedEntry', 'OutboxMessage'];
    let deleted = 0;
    for (const tenant of [TENANT, OTHER_TENANT]) {
      for (const table of tables) {
        const gone = await asTenant(owner, tenant, `DELETE FROM "${table}" WHERE "tenantId" = $1`, [tenant]);
        deleted += gone.rowCount ?? 0;
      }
    }
    let left = 0;
    for (const tenant of [TENANT, OTHER_TENANT]) {
      for (const table of tables) {
        const count = await asTenant(owner, tenant, `SELECT count(*)::int AS n FROM "${table}" WHERE "tenantId" = $1`, [tenant]);
        left += count.rows[0]?.n ?? 0;
      }
    }
    check(deleted > 0 && left === 0, `cleanup: this run's rows are gone (${deleted} deleted, ${left} left)`);
    await Promise.all([app.end().catch(() => {}), owner.end().catch(() => {}), relay.end().catch(() => {})]);
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log("every table isolates by tenant, the moderation trail cannot be rewritten, threads have a ceiling, and the outbox's two readers reach exactly as far as they should");
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
