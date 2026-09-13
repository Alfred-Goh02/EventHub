/**
 * Asserts that moving search onto a new index loses no write, serves no error,
 * and keeps every tenant confined — and that it can be undone.
 *
 * This is the whole answer to "how do you change an analyzer without downtime",
 * so it is run against the real migration code, on scratch indices named for this
 * run, while writes and reads carry on:
 *
 *   NO LOST WRITES   during the copy, 10 events are updated to a newer version
 *                    and 5 are created; afterwards the target holds all of them,
 *                    and the older copies of the 10 did not overwrite the newer
 *   NO ERRORS        a tenant's search runs in a loop for the whole migration and
 *                    never fails and never comes back empty
 *   STILL CONFINED   every tenant alias moved, with its filter and routing, and a
 *                    tenant still sees only its own documents on the new index
 *   TUNED, RESTORED  the target loads with refresh off, and ends with the
 *                    definition's settings
 *   REVERSIBLE       the same function pointed back at the source moves search
 *                    back, carrying the writes made in between
 *
 * WHY THE CONFLICT COUNT IS A CONTROL. "The newer title survived" is also what a
 * copy that never touched those documents produces. The copy's own
 * version_conflicts count must be at least 10 — proof it tried to write the older
 * copies and was refused.
 *
 * WATCHED FAIL, against a deliberately broken migration:
 *
 *   copy without external versioning   0 conflicts, and all 10 updated events
 *                                      overwritten by their older copies
 *   tenant aliases left on the source  both tenant-alias checks red, and the
 *                                      search loop only ever saw the source
 *   loading settings never restored    refresh_interval stayed -1
 *   writes never moved first           the migration refused to swap: the
 *                                      target held 40, the source 46
 *
 * That last one showed more than it was aimed at. Not only the write after the
 * copy was missing — so were the 15 written just before it started. _reindex
 * reads a search snapshot, and a write no refresh has made visible yet is not in
 * it. Moving writes first is what makes that harmless; the refusal to swap on a
 * short count is what catches it when it is not.
 *
 * It also caught this file hanging: a migration that threw left the search loop
 * spinning, so the run never ended. The loop is now stopped in finally.
 *
 * Run (after pnpm build): node scripts/verify-search-reindex.mjs
 */

import { randomUUID } from 'node:crypto';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
process.env['OPENSEARCH_NODE'] ??= 'http://127.0.0.1:9200';

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
const V1 = `verify-reindex-${run}-v1`;
const V2 = `verify-reindex-${run}-v2`;
const READ = `verify-reindex-${run}-read`;
const WRITE = `verify-reindex-${run}-write`;
const TA = `t_rx_a_${run}`;
const TB = `t_rx_b_${run}`;

async function main() {
  const { os } = await mod('opensearch.js');
  const { migrateIndex } = await mod('migrate.js');
  const { indexBody, tenantAlias } = await mod('index-definition.js');

  const holdersOf = async (alias) => {
    const r = await os.indices.getAlias({ name: alias }, { ignore: [404] });
    return r.statusCode === 404 ? {} : r.body;
  };

  const doc = (tenantId, n, over = {}) => ({
    tenantId,
    eventId: `rx_${run}_${tenantId === TA ? 'a' : 'b'}_${n}`,
    slug: `rx-${run}-${n}`,
    title: `original ${n}`,
    status: 'published',
    startsAt: '2028-03-01T18:00:00.000Z',
    endsAt: '2028-03-01T22:00:00.000Z',
    capacity: 10,
    updatedAt: new Date().toISOString(),
    docVersion: 5,
    ...over,
  });

  /**
   * `refresh` is a parameter because writes DURING the migration must not wait
   * for one. The target loads with refresh_interval -1, and a bulk with
   * refresh=wait_for against it blocks for a refresh that never comes — measured:
   * the request hung until the index was deleted. The client timed out after five
   * seconds and retried a bulk that had already landed, and the retry came back
   * as ten version conflicts. The migration refreshes the target itself at the
   * end; nothing here needs to wait.
   */
  const indexAll = async (docs, version, refresh = 'wait_for') => {
    const { body } = await os.bulk({
      refresh,
      require_alias: true,
      body: docs.flatMap((d) => [
        { index: { _index: WRITE, _id: d.eventId, routing: d.tenantId, version, version_type: 'external' } },
        d,
      ]),
    });
    return body;
  };

  /**
   * Outside the try, so finally can stop it. The first version stopped the loop
   * only after a SUCCESSFUL migration, so a migration that threw — which is what
   * a broken one does — left the loop spinning and the process never exited: a
   * guard that hangs when the thing it guards breaks.
   */
  let loopRunning = false;
  let reader = Promise.resolve();

  try {
    // -----------------------------------------------------------------------
    console.log('  a source index as production has it: aliases, tenant aliases, documents');
    // -----------------------------------------------------------------------
    await os.indices.create({ index: V1, body: indexBody });
    await os.indices.updateAliases({
      body: {
        actions: [
          { add: { index: V1, alias: READ } },
          { add: { index: V1, alias: WRITE, is_write_index: true } },
          ...[TA, TB].map((t) => ({ add: { index: V1, alias: tenantAlias(t), filter: { term: { tenantId: t } }, routing: t } })),
        ],
      },
    });
    const aDocs = Array.from({ length: 30 }, (_, i) => doc(TA, i));
    const bDocs = Array.from({ length: 10 }, (_, i) => doc(TB, i));
    const seeded = await indexAll([...aDocs, ...bDocs], 5);
    check(!seeded.errors, `seeded ${aDocs.length} events for tenant A and ${bDocs.length} for tenant B at version 5`);

    // A tenant's search, running for the whole migration.
    /**
     * The control is WHICH INDEX answered, not how many times. The first version
     * required more than 20 iterations, and passed with 22 on a run where the
     * migration happened to be quick — a threshold that measured the machine,
     * not the overlap. Seeing both the source and the target answer proves the
     * loop was running on both sides of the swap.
     */
    loopRunning = true;
    const loop = { iterations: 0, errors: [], empty: 0, servedBy: new Set() };
    reader = (async () => {
      while (loopRunning) {
        try {
          const { body } = await os.search({ index: tenantAlias(TA), body: { size: 1, _source: false, track_total_hits: true } });
          loop.iterations += 1;
          if (body.hits.total.value === 0) loop.empty += 1;
          if (body.hits.hits[0]) loop.servedBy.add(body.hits.hits[0]._index);
        } catch (err) {
          loop.errors.push(err?.meta?.body?.error?.type ?? err?.message ?? String(err));
        }
        await sleep(15);
      }
    })();

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  migrate to a new index while writes land');
    // -----------------------------------------------------------------------
    const updatedIds = aDocs.slice(0, 10).map((d) => d.eventId);
    const result = await migrateIndex({
      target: V2,
      readAlias: READ,
      writeAlias: WRITE,
      body: indexBody,
      pollMs: 100,
      onWriteAliasMoved: async () => {
        // Newer versions of 10 existing events, and 5 new ones — written after
        // writes moved and before the copy, so the copy has to arrive behind them.
        const newer = aDocs.slice(0, 10).map((d) => ({ ...d, title: `updated ${d.eventId}`, docVersion: 6 }));
        const created = Array.from({ length: 5 }, (_, i) => doc(TA, 100 + i, { title: `created during ${i}`, docVersion: 1 }));
        const w1 = await indexAll(newer, 6, false);
        const w2 = await indexAll(created, 1, false);
        const firstError = [...w1.items, ...w2.items].find((it) => it.index?.error)?.index;
        check(
          !w1.errors && !w2.errors,
          !w1.errors && !w2.errors
            ? 'during the migration: 10 events updated to version 6, 5 created'
            : `a write during the migration failed: ${JSON.stringify(firstError)}`,
        );
        const loading = (await os.indices.getSettings({ index: V2 })).body[V2].settings.index.refresh_interval;
        check(loading === '-1', `the target is loading with refresh off (refresh_interval=${loading})`);
      },
      onCopied: async () => {
        // After the copy, before the swap: nothing will copy this. Only the write
        // alias decides where it lands, which is what moving writes first is for.
        const late = await indexAll([doc(TA, 150, { title: 'written after the copy' })], 1, false);
        check(!late.errors, 'after the copy and before the swap: one more event written');
      },
    });

    // Let the loop observe the target before stopping it, however fast the swap was.
    for (let i = 0; i < 100 && !loop.servedBy.has(V2); i++) await sleep(50);
    loopRunning = false;
    await reader;

    check(result.source === V1 && result.target === V2, `migrated ${result.source} -> ${result.target}`);
    check(
      result.versionConflicts >= 10,
      `CONTROL: the copy was refused ${result.versionConflicts} time(s) for older versions — it did reach the updated events`,
    );

    const counts = { v1: (await os.count({ index: V1 })).body.count, v2: (await os.count({ index: V2 })).body.count };
    check(counts.v2 === 46 && counts.v1 === 40, `the target holds ${counts.v2} (30 + 10 + 5 + 1), the source still holds ${counts.v1}`);
    const lateOnTarget = await os.get({ index: V2, id: `rx_${run}_a_150`, routing: TA }, { ignore: [404] });
    check(lateOnTarget.statusCode === 200, 'the event written after the copy is on the target — writes had already moved');

    const { body: mget } = await os.mget({ index: V2, body: { docs: updatedIds.map((id) => ({ _id: id, routing: TA })) } });
    const survived = mget.docs.filter((d) => d.found && d._source.title.startsWith('updated') && d._version === 6).length;
    check(survived === 10, `${survived} of the 10 updated events kept their newer title and version 6 — no older copy overwrote them`);

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  no search failed, and every tenant is still confined');
    // -----------------------------------------------------------------------
    check(
      loop.servedBy.has(V1) && loop.servedBy.has(V2),
      `CONTROL: tenant A's search ran ${loop.iterations} times and was answered by [${[...loop.servedBy].join(', ')}] — both sides of the swap`,
    );
    check(loop.errors.length === 0, `and failed ${loop.errors.length} time(s) ${loop.errors.length ? JSON.stringify([...new Set(loop.errors)]) : ''}`);
    check(loop.empty === 0, `and came back empty ${loop.empty} time(s)`);

    const aliasesNow = async () => {
      const out = {};
      for (const name of [READ, WRITE, tenantAlias(TA), tenantAlias(TB)]) {
        const h = await holdersOf(name);
        out[name] = Object.entries(h).map(([index, v]) => ({ index, def: v.aliases[name] }));
      }
      return out;
    };
    const after = await aliasesNow();
    const onlyOn = (name, index) => after[name].length === 1 && after[name][0].index === index;
    check(onlyOn(READ, V2) && onlyOn(WRITE, V2), 'the read and write aliases resolve only to the target');
    check(after[WRITE][0]?.def?.is_write_index === true, 'and the target is the write index');
    for (const t of [TA, TB]) {
      const a = after[tenantAlias(t)];
      check(
        a.length === 1 && a[0].index === V2 && JSON.stringify(a[0].def.filter) === JSON.stringify({ term: { tenantId: t } }) && a[0].def.index_routing === t,
        `${tenantAlias(t)} moved with its filter and routing`,
      );
    }
    check(result.tenantAliasesMoved === 2, `the swap moved ${result.tenantAliasesMoved} tenant alias(es)`);

    const aSees = await os.search({ index: tenantAlias(TA), body: { size: 100, _source: ['tenantId'] } });
    const aTenants = [...new Set(aSees.body.hits.hits.map((h) => h._source.tenantId))];
    check(
      aSees.body.hits.total.value === 36 && aTenants.length === 1 && aTenants[0] === TA,
      `tenant A sees ${aSees.body.hits.total.value} events on the new index, from [${aTenants.join(', ')}]`,
    );

    const restored = (await os.indices.getSettings({ index: V2 })).body[V2].settings.index;
    check(
      restored.refresh_interval === indexBody.settings.index.refresh_interval &&
        Number(restored.number_of_replicas) === indexBody.settings.index.number_of_replicas,
      `the target ends with the definition's settings (refresh_interval=${restored.refresh_interval}, replicas=${restored.number_of_replicas})`,
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  and it can be undone');
    // -----------------------------------------------------------------------
    await indexAll([doc(TA, 200, { title: 'written after the migration' })], 1);
    const back = await migrateIndex({ target: V1, readAlias: READ, writeAlias: WRITE, body: indexBody, pollMs: 100 });
    const backAliases = await aliasesNow();
    check(
      back.source === V2 && backAliases[READ][0]?.index === V1 && backAliases[tenantAlias(TA)][0]?.index === V1,
      `rolled back ${back.source} -> ${back.target}, tenant aliases included`,
    );
    const v1Count = (await os.count({ index: V1 })).body.count;
    const carried = await os.get({ index: V1, id: `rx_${run}_a_200`, routing: TA }, { ignore: [404] });
    check(
      v1Count === 47 && carried.statusCode === 200,
      `the source now holds ${v1Count}, including the event written to the target after the first migration`,
    );
  } finally {
    loopRunning = false;
    await reader;
    await os.indices.delete({ index: [V1, V2] }, { ignore: [404] }).catch(() => {});
    const leftover = [];
    for (const name of [tenantAlias(TA), tenantAlias(TB)]) leftover.push(...Object.keys(await holdersOf(name)));
    console.log(`\n    cleanup: scratch indices deleted, ${leftover.length} tenant alias(es) left`);
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log('a migration loses no write, serves no error, keeps every tenant confined, and can be undone');
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
