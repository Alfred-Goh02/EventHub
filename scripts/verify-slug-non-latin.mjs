/**
 * Asserts that a tenant can hold more than one event whose title is not ASCII.
 *
 * slugify() kept only [a-z0-9] and threw everything else away, so:
 *
 *   'Summer Party'    -> 'summer-party'
 *   '活动预告'          -> ''
 *   'Летний концерт'  -> ''
 *   'مهرجان'          -> ''
 *   '🎉🎉'             -> ''
 *
 * There is a partial unique index on (tenantId, slug) WHERE deletedAt IS NULL,
 * so the FIRST non-Latin title in a tenant took the empty slug and every one
 * after it was refused. Measured:
 *
 *   CJK title      -> 201  slug ""
 *   Cyrillic title -> 409  "an event with that title already exists"
 *
 * Two titles that share nothing, and a message that is simply untrue. A tenant
 * could hold exactly one event whose title was not mostly ASCII.
 *
 * WHAT IS DELIBERATELY NOT FIXED, and asserted so it stays that way: two events
 * genuinely called the same Latin thing in one tenant still collide. That is a
 * real conflict and the message is true in that case. A fix that made every
 * slug unique would have passed the assertions above while removing a
 * constraint somebody chose on purpose — so the duplicate-title case is a
 * control here, not an oversight.
 *
 * Run: node scripts/verify-slug-non-latin.mjs
 */

const AUTH = process.env['AUTH_URL'] ?? 'http://localhost:3001';
const EVENT = process.env['EVENT_URL'] ?? 'http://localhost:3002';
const TENANT = 'tenant_acme';

const run = `sl_${Date.now()}`;

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}

/**
 * Scripts that produce nothing at all under an [a-z0-9] filter. Several rather
 * than one, because a fix that special-cased a single alphabet would pass with
 * one and fail here.
 */
const TITLES = [
  ['Chinese', '活动预告'],
  ['Cyrillic', 'Летний концерт'],
  ['Arabic', 'مهرجان الصيف'],
  ['emoji only', '🎉🎊🎈'],
  ['Greek', 'Καλοκαιρινή γιορτή'],
];

async function main() {
  const login = await fetch(`${AUTH}/login`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Tenant-ID': TENANT },
    body: JSON.stringify({ email: 'alice@acme.com', password: 'hunter2222' }),
  });
  const { accessToken } = await login.json();
  if (!accessToken) {
    console.error('FAIL: could not log in');
    process.exitCode = 1;
    return;
  }

  const base = Date.now();
  const create = (title) =>
    fetch(`${EVENT}/events`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Authorization: `Bearer ${accessToken}` },
      body: JSON.stringify({
        title,
        capacity: 10,
        startsAt: new Date(base + 86_400_000).toISOString(),
        endsAt: new Date(base + 90_000_000).toISOString(),
      }),
    }).then(async (r) => ({ status: r.status, body: await r.json() }));

  console.log(`  ${TITLES.length} events, each titled in a script that survives no [a-z0-9] filter`);

  const slugs = [];
  for (const [label, title] of TITLES) {
    const res = await create(title);
    check(
      res.status === 201,
      res.status === 201
        ? `${label} accepted, slug ${JSON.stringify(res.body.slug)}`
        : `${label} -> ${res.status} ${JSON.stringify(res.body?.error)} — only the first ` +
          'non-Latin title in a tenant could be created',
    );
    if (res.status === 201) slugs.push(res.body.slug);
  }

  /**
   * Before anything is said ABOUT the slugs, assert there are some.
   *
   * The first version of this file went straight to `slugs.every(...)` and
   * `new Set(slugs).size === slugs.length`, both of which are trivially true of
   * an empty array. Against the unfixed service — where every create was
   * refused — they printed:
   *
   *   ok    no slug is empty ()
   *   ok    every slug is distinct (0 of 0)
   *
   * two green ticks describing nothing, in a file whose whole subject is a
   * constraint that stopped rows existing.
   */
  check(
    slugs.length === TITLES.length,
    `${slugs.length} of ${TITLES.length} were created — the assertions below need something to look at`,
  );
  check(
    slugs.length > 0 && slugs.every((s) => s !== '' && s != null),
    `no slug is empty (${slugs.map((s) => JSON.stringify(s)).join(', ') || 'nothing to check'})`,
  );
  check(
    slugs.length > 0 && new Set(slugs).size === slugs.length,
    `every slug is distinct (${new Set(slugs).size} of ${slugs.length})`,
  );

  // -------------------------------------------------------------------------
  console.log('');
  console.log('  the controls: ordinary titles keep working, and duplicates still conflict');
  // -------------------------------------------------------------------------
  const latin = await create(`Summer Party ${run}`);
  check(latin.status === 201, `a Latin title is still accepted, got ${latin.status}`);
  check(
    /^summer-party-/.test(latin.body?.slug ?? ''),
    `and still gets a readable slug, not a random one (${JSON.stringify(latin.body?.slug)})`,
  );

  /**
   * The constraint this fix must NOT have removed. A change that gave every
   * slug a random suffix would satisfy every assertion above and quietly drop a
   * uniqueness rule somebody chose.
   */
  const duplicate = await create(`Summer Party ${run}`);
  check(
    duplicate.status === 409,
    duplicate.status === 409
      ? 'a genuinely duplicate title still conflicts — the uniqueness rule survives'
      : `a duplicate title got ${duplicate.status}; the uniqueness constraint was lost`,
  );

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('a title in any script gets a usable slug, and only real duplicates conflict');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
