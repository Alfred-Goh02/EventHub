/**
 * Asserts that Community works in the COMPOSE STACK, container to container: the
 * chapter's end-to-end smoke test.
 *
 * Every other Community verifier runs the service natively, with keys from the working
 * tree and the consumer on its own group. This one asks the running containers, so what
 * it proves is the wiring those runs cannot see:
 *
 *   - Auth signs with the keypair the SETUP CONTAINER generated, and Community verifies
 *     with the public half from the jwt-public volume.
 *   - relay-community drains Community's outbox inside the network, on its own
 *     credential, and publishes Avro through the registry.
 *   - Community's own feed consumer, on the real group, fans the post out.
 *   - The feed pages across both posts with a cursor, as a reader sees it.
 *
 * WHAT WOULD LOOK THE SAME WITHOUT IT. A post the consumer never fanned out is still in
 * the feed: the read-time half serves every post with fannedOut = false. So the feed
 * answering is NOT evidence that Kafka worked. The fan-out is asserted on the rows —
 * a FeedEntry for each member and the post's flag — and the publish on THIS run's
 * outbox rows, never on a process's exit code.
 *
 * NO DEPENDENCIES, on purpose: the compose job installs nothing. Postgres is reached with
 * `docker exec psql` as the container's superuser, which row security does not bind, so
 * every query here names this run's ids itself.
 *
 * Its two users are created for the run and removed at the end, with everything they
 * wrote, and the cleanup reports what it DELETED as well as what is left.
 *
 * Run (with `docker compose up -d --wait` done): node scripts/verify-compose-community.mjs
 */

import { execFile } from 'node:child_process';
import { randomBytes } from 'node:crypto';

const AUTH = process.env['AUTH_URL'] ?? 'http://127.0.0.1:3001';
const COMMUNITY = process.env['COMMUNITY_URL'] ?? 'http://127.0.0.1:3008';
const POSTGRES = process.env['POSTGRES_CONTAINER'] ?? 'eventhub-postgres';
/** The seeded tenant: this adds two users to it and takes them away again. */
const TENANT = 'tenant_acme';
/** Its subdomain: Auth takes the tenant from the gateway's X-Tenant-Slug. */
const TENANT_SLUG = 'acme';
const PASSWORD = 'hunter2222';

const RUN = randomBytes(4).toString('hex');
const AUTHOR = { id: `user_composecommunity_a_${RUN}`, email: `compose-community-a-${RUN}@mailcheck.example` };
const READER = { id: `user_composecommunity_b_${RUN}`, email: `compose-community-b-${RUN}@mailcheck.example` };

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) {
    failures += 1;
    process.exitCode = 1;
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function waitFor(fn, timeoutMs = 60_000, stepMs = 500) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await fn().catch(() => null);
    if (value) return value;
    await sleep(stepMs);
  }
  return null;
}

/** One statement, as the superuser inside the Postgres container. */
function psql(database, sql) {
  return new Promise((resolve, reject) => {
    execFile(
      'docker',
      ['exec', '-i', POSTGRES, 'psql', '-U', 'eventhub', '-d', database, '-v', 'ON_ERROR_STOP=1', '-tAc', sql],
      { maxBuffer: 16 * 1024 * 1024 },
      (err, stdout, stderr) => (err ? reject(new Error(`${err.message}\n${stderr}`)) : resolve(stdout.trim())),
    );
  });
}

/** Deletes and returns HOW MANY, so a cleanup that removes nothing cannot pass as one that worked. */
const deleted = async (database, table, where) =>
  Number(await psql(database, `WITH d AS (DELETE FROM "${table}" WHERE ${where} RETURNING 1) SELECT count(*) FROM d`).catch(() => '0'));

async function api(base, path, { method = 'GET', token, slug, body } = {}) {
  const res = await fetch(`${base}${path}`, {
    method,
    headers: {
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
      ...(slug ? { 'X-Tenant-Slug': slug } : {}),
      ...(body ? { 'Content-Type': 'application/json' } : {}),
    },
    ...(body ? { body: JSON.stringify(body) } : {}),
  });
  return { status: res.status, body: await res.json().catch(() => null) };
}

async function main() {
  console.log('  the stack answers');
  const [authUp, communityReady] = await Promise.all([
    fetch(`${AUTH}/health`).then((r) => r.ok, () => false),
    fetch(`${COMMUNITY}/ready`).then((r) => r.ok, () => false),
  ]);
  check(authUp && communityReady, `CONTROL: Auth and Community are up (auth ${authUp}, community ${communityReady})`);
  if (failures > 0) {
    console.error('\nStart the stack first: docker compose up -d --wait');
    return;
  }

  // The seeded account's own hash, so these users have a password without hashing one:
  // the compose job has no dependencies.
  const seededHash = await psql('eventhub_auth', `SELECT "passwordHash" FROM "User" WHERE email = 'alice@acme.com' AND "tenantId" = '${TENANT}'`);
  check(seededHash.startsWith('$argon2'), `CONTROL: the seeded account's hash was read (${seededHash.slice(0, 12)}…)`);
  if (failures > 0) return;

  let spaceId = null;
  const posts = [];
  try {
    for (const user of [AUTHOR, READER]) {
      await psql(
        'eventhub_auth',
        `INSERT INTO "User" (id, "tenantId", email, "passwordHash", role, "updatedAt")
         VALUES ('${user.id}', '${TENANT}', '${user.email}', '${seededHash}', 'ATTENDEE', now())`,
      );
    }

    console.log('');
    console.log("  Auth's token, verified by Community with the volume's public key");
    const signIn = async (user) => (await api(AUTH, '/login', { method: 'POST', slug: TENANT_SLUG, body: { email: user.email, password: PASSWORD } })).body?.accessToken;
    const asAuthor = await signIn(AUTHOR);
    const asReader = await signIn(READER);
    check(Boolean(asAuthor && asReader), 'both users sign in through Auth');
    const space = await api(COMMUNITY, '/spaces', { method: 'POST', token: asAuthor, body: { slug: `compose-${RUN}`, name: 'Compose smoke' } });
    spaceId = space.body?.id ?? null;
    check(space.status === 201 && Boolean(spaceId), `Community accepts the token and creates a space (${space.status})`);
    const joined = await api(COMMUNITY, `/spaces/${spaceId}/join`, { method: 'POST', token: asReader });
    check(joined.status === 200, `the reader joins it (${joined.status})`);

    console.log('');
    console.log('  a post and a comment, published by relay-community');
    for (const body of ['first post', 'second post']) {
      const p = await api(COMMUNITY, `/spaces/${spaceId}/posts`, { method: 'POST', token: asAuthor, body: { body: `${body} ${RUN}` } });
      posts.push(p.body?.id);
      // Distinct createdAt, so the two pages below have a defined order.
      await sleep(20);
    }
    const [first, second] = posts;
    const comment = await api(COMMUNITY, `/posts/${first}/comments`, { method: 'POST', token: asReader, body: { body: `a reply ${RUN}` } });
    check(Boolean(first && second) && comment.status === 201, `two posts and a comment are written (${comment.status})`);

    const outbox = () =>
      psql(
        'eventhub_community',
        `SELECT count(*) FILTER (WHERE "publishedAt" IS NOT NULL) || '/' || count(*) FROM "OutboxMessage"
          WHERE "tenantId" = '${TENANT}' AND "aggregateId" = '${spaceId}'`,
      );
    const published = await waitFor(async () => ((await outbox()) === '3/3' ? '3/3' : null), 60_000);
    check(Boolean(published), `this run's three outbox rows are published — the posts and the comment (${await outbox()})`);

    console.log('');
    console.log("  Community's own consumer fans the posts out");
    const fanned = () =>
      psql(
        'eventhub_community',
        `SELECT (SELECT count(*) FROM "FeedEntry" WHERE "postId" IN ('${first}', '${second}'))
                || '/' || (SELECT count(*) FROM "Post" WHERE id IN ('${first}', '${second}') AND "fannedOut")`,
      );
    const done = await waitFor(async () => ((await fanned()) === '4/2' ? '4/2' : null), 90_000);
    check(
      Boolean(done),
      `a FeedEntry per member per post, and both posts marked fanned out (${await fanned()} rows/posts) — what the feed alone could not show, since it serves unfanned posts too`,
    );

    console.log('');
    console.log('  the reader pages the feed with its cursor');
    const page1 = await api(COMMUNITY, '/feed?limit=1', { token: asReader });
    const page2 = page1.body?.nextCursor
      ? await api(COMMUNITY, `/feed?limit=1&cursor=${encodeURIComponent(page1.body.nextCursor)}`, { token: asReader })
      : null;
    const ids = [...(page1.body?.posts ?? []), ...(page2?.body?.posts ?? [])].map((p) => p.id);
    check(
      page1.status === 200 && ids[0] === second && ids[1] === first && Boolean(page1.body?.nextCursor),
      `page one is the newer post with a nextCursor, page two the older (${ids.map((id) => (id === first ? 'first' : id === second ? 'second' : id)).join(', ')})`,
    );
    const firstOnPage = page2?.body?.posts?.[0];
    check(
      firstOnPage?.commentCount === 1 && page2?.body?.nextCursor === null,
      `the older post carries its comment, and the feed ends there (commentCount ${firstOnPage?.commentCount}, nextCursor ${JSON.stringify(page2?.body?.nextCursor)})`,
    );
  } catch (err) {
    check(false, `the run stopped early: ${err instanceof Error ? err.message : String(err)}`);
  } finally {
    let removed = 0;
    if (spaceId) {
      const postIds = posts.filter(Boolean).map((id) => `'${id}'`).join(',') || "''";
      removed += await deleted('eventhub_community', 'FeedEntry', `"postId" IN (${postIds})`);
      removed += await deleted('eventhub_community', 'Comment', `"postId" IN (${postIds})`);
      removed += await deleted('eventhub_community', 'Post', `"spaceId" = '${spaceId}'`);
      removed += await deleted('eventhub_community', 'Membership', `"spaceId" = '${spaceId}'`);
      removed += await deleted('eventhub_community', 'CommunitySpace', `id = '${spaceId}'`);
      removed += await deleted('eventhub_community', 'OutboxMessage', `"aggregateId" = '${spaceId}'`);
    }
    for (const user of [AUTHOR, READER]) {
      removed += await deleted('eventhub_auth', 'RefreshToken', `"userId" = '${user.id}'`);
      removed += await deleted('eventhub_auth', 'OutboxMessage', `"aggregateId" = '${user.id}'`);
      removed += await deleted('eventhub_auth', 'User', `id = '${user.id}'`);
    }
    const left = await psql(
      'eventhub_community',
      `SELECT (SELECT count(*) FROM "CommunitySpace" WHERE id = '${spaceId ?? ''}')
            + (SELECT count(*) FROM "Post" WHERE "spaceId" = '${spaceId ?? ''}')
            + (SELECT count(*) FROM "OutboxMessage" WHERE "aggregateId" = '${spaceId ?? ''}')`,
    ).catch(() => '?');
    const usersLeft = await psql('eventhub_auth', `SELECT count(*) FROM "User" WHERE id IN ('${AUTHOR.id}', '${READER.id}')`).catch(() => '?');
    check(removed > 0 && left === '0' && usersLeft === '0', `cleanup: this run's rows are gone (${removed} deleted, ${left} community rows and ${usersLeft} users left)`);
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log("Community works across the compose stack: Auth's token verified with the volume key, the outbox published by relay-community, the posts fanned out by Community's own consumer, and the feed paged with its cursor");
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
