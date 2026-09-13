/**
 * Asserts that a tenant's search cannot reach another tenant's data — neither
 * through the documents it matches nor through what the engine reads off the
 * shard — and that this holds because of how the module is built rather than
 * because a caller remembered something.
 *
 * WHAT IS UNDER TEST. Every tenant's events share one index, and two layers keep
 * them apart (services/search/src/tenant-alias.ts has the full account):
 *
 *   the filtered alias     confines which DOCUMENTS can match
 *   the closed request     confines which REQUESTS can be sent, because the alias
 *                          does not confine shard-level data — aggregation buckets
 *                          at min_doc_count 0, suggesters, explain
 *
 * The first version of this file tested only the first layer, and said "there is
 * no query a caller can write through this alias that escapes it". The review of
 * Chapter 6 wrote several. That sentence was checked by nothing, which is how it
 * came to be false.
 *
 * WHY EVERY TENANT HERE SHARES ONE SHARD. Shard-level data can only leak between
 * tenants on the same shard, and a missing alias filter can only expose documents
 * on the shard being searched. With three shards, tenants picked at random sit
 * apart about two times in three, and every leak below would pass as isolation.
 * So B and C are chosen, by asking the cluster, to route to A's shard.
 *
 * WHY THE CONTROLS ARE MOST OF THE TEST. "A saw none of B's data" is also what an
 * empty index, a query that matches nothing, or a probe blind to leaks produce.
 * So each negative assertion has a positive twin: the unfiltered read alias must
 * find B's documents; the RAW engine request through A's alias must leak B's
 * values, proving the probe can see a leak before the module is asked to prevent
 * one; and a drifted alias must leak before the module is asked to repair it.
 *
 * NOTHING IS WRITTEN UNTIL THE INDEX IS CONFIRMED. The first version's first act
 * was a bulk write through events-write. On a cluster where bootstrap had not
 * run, that invented an INDEX named events-write with tenantId as analysed text;
 * the next call threw; cleanup, sitting on the success path, never ran; and
 * bootstrap:index failed from then on, because an index held the alias's name.
 *
 * WATCHED FAIL. Four guards in the module were broken at once, rebuilt, and run;
 * 12 assertions went red and every one named its own guard:
 *
 *   facets at min_doc_count 0         A's facets held B's category, city, tag
 *   .strict() removed from the request raw suggest, aggs and explain accepted
 *   tenant id check disabled          all six hostile ids reached OpenSearch
 *   existing alias trusted by name    tenant C saw 4 documents from 3 tenants
 *
 * Against a one-shard index this refuses to run at all, before writing anything.
 *
 * Imports the real compiled module rather than rebuilding any query here, for the
 * reason #71 established: a test that reconstructs the thing it tests is testing
 * its own copy.
 *
 * Run, after `pnpm build` and `pnpm --filter @eventhub/search run bootstrap:index`:
 *   node scripts/verify-search-tenant-isolation.mjs
 */

import { randomUUID } from 'node:crypto';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');

// The Search env schema is fail-fast, so these must be present before the module
// graph loads. Defaults match services/search/.env.example.
process.env['NODE_ENV'] ??= 'development';
process.env['PORT'] ??= '3005';
process.env['OPENSEARCH_NODE'] ??= 'http://127.0.0.1:9200';
process.env['KAFKA_BROKERS'] ??= '127.0.0.1:9092';

const mod = (rel) => import(pathToFileURL(join(ROOT, 'services', 'search', 'dist', rel)).href);

let failures = 0;
function check(ok, msg) {
  console.log(`    ${ok ? 'ok  ' : 'FAIL'}  ${msg}`);
  if (!ok) failures += 1;
}

const run = randomUUID().replace(/-/g, '').slice(0, 8);

/** In every fixture's title, so one query finds all of them and nothing else. */
const TERM = `klezmer${run}`;
/** A word that exists only in tenant B's title. */
const SECRET = `zanzibar${run}`;

async function main() {
  const { os } = await mod('opensearch.js');
  const { searchForTenant, forgetTenantAliases } = await mod('tenant-alias.js');
  const { INDEX_NAME, READ_ALIAS, WRITE_ALIAS, tenantAlias } = await mod('index-definition.js');

  const holdersOf = async (alias) => {
    const r = await os.indices.getAlias({ name: alias }, { ignore: [404] });
    return r.statusCode === 404 ? {} : r.body;
  };

  // -------------------------------------------------------------------------
  console.log('  the index this writes to has to be the one the code defines, checked before any write');
  // -------------------------------------------------------------------------
  const read = Object.keys(await holdersOf(READ_ALIAS));
  const write = Object.keys(await holdersOf(WRITE_ALIAS));
  check(
    read.length === 1 && read[0] === INDEX_NAME,
    `${READ_ALIAS} resolves to [${read.join(', ')}]` + (read[0] === INDEX_NAME ? '' : `, expected ${INDEX_NAME}`),
  );
  check(
    write.length === 1 && write[0] === INDEX_NAME,
    `${WRITE_ALIAS} resolves to [${write.join(', ')}]` + (write[0] === INDEX_NAME ? '' : `, expected ${INDEX_NAME}`),
  );

  let shards = 0;
  let tenantType;
  if ((await os.indices.exists({ index: INDEX_NAME })).body) {
    const settings = (await os.indices.getSettings({ index: INDEX_NAME })).body;
    shards = Number(settings[INDEX_NAME]?.settings?.index?.number_of_shards ?? 0);
    const mappings = (await os.indices.getMapping({ index: INDEX_NAME })).body;
    tenantType = mappings[INDEX_NAME]?.mappings?.properties?.tenantId?.type;
  }
  check(
    shards >= 2,
    shards >= 2
      ? `${INDEX_NAME} has ${shards} shards, so a routing mistake is something this can see`
      : `${INDEX_NAME} has ${shards} shard(s). On one shard every routing value finds every document, ` +
        'so the routing assertion below could not fail. Delete the index and run bootstrap:index.',
  );
  check(tenantType === 'keyword', `tenantId is mapped as ${tenantType ?? 'nothing'}`);

  if (failures > 0) {
    console.error(
      '\nFAIL: refusing to write fixtures into an index that is not the defined one. ' +
        'Run: pnpm --filter @eventhub/search run bootstrap:index',
    );
    process.exitCode = 1;
    return;
  }

  const shardOf = async (routing) =>
    (await os.searchShards({ index: INDEX_NAME, routing })).body.shards[0][0].shard;

  const TENANT_A = `t_iso_a_${run}`;
  const shardA = await shardOf(TENANT_A);

  async function idOnShard(prefix, same) {
    for (let i = 0; i < 100; i++) {
      const id = `${prefix}_${run}_${i}`;
      if (((await shardOf(id)) === shardA) === same) return id;
    }
    throw new Error(`no ${prefix} id ${same ? 'on' : 'off'} shard ${shardA} in 100 tries`);
  }

  const TENANT_B = await idOnShard('t_iso_b', true);
  const TENANT_C = await idOnShard('t_iso_c', true);
  /** An _id whose DEFAULT routing (the _id itself) lands away from A's shard. */
  const MISROUTED = await idOnShard('t_iso_misrouted', false);

  const doc = (tenantId, eventId, over = {}) => ({
    tenantId,
    eventId,
    slug: eventId.replace(/_/g, '-'),
    title: `${TERM} night`,
    description: `an evening of ${TERM}`,
    category: 'music',
    tags: ['live'],
    city: 'berlin',
    country: 'DE',
    status: 'published',
    visibility: 'public',
    startsAt: '2027-03-01T19:00:00.000Z',
    endsAt: '2027-03-01T23:00:00.000Z',
    capacity: 100,
    updatedAt: new Date().toISOString(),
    docVersion: 1,
    ...over,
  });

  const fixtures = [
    { doc: doc(TENANT_A, `${TENANT_A}_e1`), routing: TENANT_A },
    { doc: doc(TENANT_A, `${TENANT_A}_e2`), routing: TENANT_A },
    {
      doc: doc(TENANT_B, `${TENANT_B}_e1`, {
        title: `${TERM} ${SECRET} gala`,
        category: `bcat${run}`,
        city: `bcity${run}`,
        tags: [`btag${run}`],
      }),
      routing: TENANT_B,
    },
    { doc: doc(TENANT_C, `${TENANT_C}_e1`), routing: TENANT_C },
    // Tenant A's, indexed the way a careless indexer would: no routing, so it is
    // placed by its _id, on a shard A's searches never visit.
    { doc: doc(TENANT_A, MISROUTED), routing: undefined },
  ];

  let written = 0;

  try {
    // -----------------------------------------------------------------------
    console.log('');
    console.log('  the fixtures have to actually be there');
    // -----------------------------------------------------------------------
    const operations = fixtures.flatMap(({ doc: d, routing }) => [
      { index: { _index: WRITE_ALIAS, _id: d.eventId, ...(routing ? { routing } : {}) } },
      d,
    ]);

    // require_alias: a write through a name that is not an alias FAILS, rather
    // than inventing an index. refresh wait_for, never true: true forces a
    // segment per call; wait_for waits for the scheduled refresh that makes a
    // write searchable.
    const { body: bulk } = await os.bulk({ refresh: 'wait_for', require_alias: true, body: operations });
    written = bulk.items.filter((it) => it.index && it.index.status < 300).length;
    check(
      !bulk.errors && written === fixtures.length,
      !bulk.errors
        ? `indexed ${written} documents: 2 routed for A, 1 for B, 1 for C, 1 of A's misrouted`
        : `indexing failed: ${JSON.stringify(bulk.items.find((it) => it.index?.error))}`,
    );
    if (bulk.errors) return;

    const all = (await os.search({ index: READ_ALIAS, body: { size: 50, query: { match: { title: TERM } } } }))
      .body;
    check(
      all.hits.total.value === fixtures.length,
      `CONTROL: the unfiltered read alias finds all ${all.hits.total.value} — the documents exist and the query matches them`,
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  a tenant sees only its own documents, through the module');
    // -----------------------------------------------------------------------
    forgetTenantAliases();
    const a = await searchForTenant(TENANT_A, { text: TERM, size: 50 });
    const aIds = a.hits.hits.map((h) => h._id);
    const aTenants = [...new Set(a.hits.hits.map((h) => h._source.tenantId))];
    check(
      a.hits.total.value === 2 && aTenants.length === 1 && aTenants[0] === TENANT_A,
      `tenant A sees ${a.hits.total.value} document(s) from [${aTenants.join(', ')}] — expected its 2 routed ones`,
    );

    const b = await searchForTenant(TENANT_B, { text: TERM, size: 50 });
    const bTenants = [...new Set(b.hits.hits.map((h) => h._source.tenantId))];
    check(
      b.hits.total.value === 1 && bTenants[0] === TENANT_B,
      `tenant B sees ${b.hits.total.value} document(s) from [${bTenants.join(', ')}] — expected its 1`,
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  a document routed to the wrong shard is lost to its own tenant');
    // -----------------------------------------------------------------------
    /**
     * Not a guard in the module: a property of the design the indexer has to
     * respect, asserted so it is seen rather than assumed. On one shard it could
     * not fail, which is why the index definition asks for three.
     */
    const misroutedExists = (
      await os.search({ index: READ_ALIAS, body: { query: { ids: { values: [MISROUTED] } } } })
    ).body.hits.total.value;
    check(
      misroutedExists === 1 && !aIds.includes(MISROUTED),
      misroutedExists === 1 && !aIds.includes(MISROUTED)
        ? "A's own document, indexed without routing, exists and A cannot see it — what an indexer " +
          'that forgets routing: tenantId would do to every tenant'
        : `misrouted document: exists=${misroutedExists}, visible to A=${aIds.includes(MISROUTED)}`,
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  naming the other tenant through the alias buys nothing');
    // -----------------------------------------------------------------------
    const crafted = (
      await os.search({
        index: tenantAlias(TENANT_A),
        body: { size: 50, query: { bool: { filter: [{ term: { tenantId: TENANT_B } }] } } },
      })
    ).body;
    check(
      crafted.hits.total.value === 0,
      `a query through A's alias asking for B's tenantId returns ${crafted.hits.total.value} — the filter is ANDed, not replaced`,
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log("  the engine DOES leak shard data through the alias — the control for what follows");
    // -----------------------------------------------------------------------
    const raw = (
      await os.search({
        index: tenantAlias(TENANT_A),
        body: {
          size: 0,
          aggs: { cats: { terms: { field: 'category', min_doc_count: 0, size: 1000 } } },
          suggest: { s: { text: SECRET.slice(0, -1), term: { field: 'title' } } },
        },
      })
    ).body;
    const rawBuckets = raw.aggregations.cats.buckets.map((x) => String(x.key));
    const rawSuggestions = raw.suggest.s.flatMap((entry) => entry.options.map((o) => o.text));
    check(
      rawBuckets.includes(`bcat${run}`),
      rawBuckets.includes(`bcat${run}`)
        ? "a raw min_doc_count 0 aggregation through A's alias lists B's category — the probe can see a leak"
        : `the raw aggregation did not show B's category (buckets: ${rawBuckets.join(', ')}), so the ` +
          'module-side check below would prove nothing',
    );
    check(
      rawSuggestions.some((t) => t.startsWith('zanzibar')),
      rawSuggestions.some((t) => t.startsWith('zanzibar'))
        ? "a raw term suggester through A's alias offers a word only B's title contains"
        : `the raw suggester offered [${rawSuggestions.join(', ')}], not B's word`,
    );

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  and the module cannot express it');
    // -----------------------------------------------------------------------
    const faceted = await searchForTenant(TENANT_A, { size: 0, facets: ['category', 'city', 'tags', 'country'] });
    const keys = Object.values(faceted.aggregations).flatMap((agg) => agg.buckets.map((x) => String(x.key)));
    check(
      keys.includes('music') && keys.includes('berlin'),
      `the facets ran: A's own values are in them (${keys.join(', ')})`,
    );
    const leaked = keys.filter((k) => k.endsWith(run));
    check(
      leaked.length === 0,
      leaked.length === 0
        ? "none of B's category, city or tag values appear in A's facets"
        : `A's facets contain B's values: ${leaked.join(', ')}`,
    );

    async function refused(label, request) {
      try {
        await searchForTenant(TENANT_A, request);
        check(false, `${label} was accepted`);
      } catch (err) {
        const ok = err?.name === 'ZodError';
        check(ok, ok ? `${label} is refused before anything is sent` : `${label} threw, but not as a refused request: ${err?.message}`);
      }
    }
    await refused('a raw suggest block', { suggest: { s: { text: SECRET, term: { field: 'title' } } } });
    await refused('a raw aggs block', { aggs: { x: { terms: { field: 'category', min_doc_count: 0 } } } });
    await refused('a facet on tenantId', { facets: ['tenantId'] });
    await refused('a filter on tenantId', { filters: { tenantId: [TENANT_B] } });
    await refused('explain', { explain: true });

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  a tenant id that is an index expression is refused before it becomes one');
    // -----------------------------------------------------------------------
    const hostile = [
      '*',
      't_iso_*',
      `${TENANT_A},${READ_ALIAS}`,
      `${TENANT_A}-events,${READ_ALIAS},tenant-${TENANT_A}`,
      'T_ISO_UPPER',
      '',
    ];
    for (const id of hostile) {
      try {
        await searchForTenant(id, { text: TERM });
        check(false, `tenant id ${JSON.stringify(id)} was searched`);
      } catch (err) {
        const ok = /refusing to build a tenant alias/.test(String(err?.message));
        check(ok, ok ? `${JSON.stringify(id)} refused` : `${JSON.stringify(id)} threw something else: ${err?.message}`);
      }
    }

    // -----------------------------------------------------------------------
    console.log('');
    console.log('  an alias that already exists is checked, not trusted by its name');
    // -----------------------------------------------------------------------
    /**
     * C's alias as a hand edit or an older definition might leave it: the right
     * name, the right routing, and no filter.
     */
    await os.indices.updateAliases({
      body: { actions: [{ add: { index: INDEX_NAME, alias: tenantAlias(TENANT_C), routing: TENANT_C } }] },
    });
    const drifted = (
      await os.search({ index: tenantAlias(TENANT_C), body: { size: 50, query: { match: { title: TERM } } } })
    ).body;
    const driftedTenants = new Set(drifted.hits.hits.map((h) => h._source.tenantId));
    check(
      driftedTenants.has(TENANT_A) && driftedTenants.has(TENANT_B),
      `CONTROL: through the drifted alias as it stands, tenants [${[...driftedTenants].join(', ')}] are visible — it leaks`,
    );

    forgetTenantAliases();
    const c = await searchForTenant(TENANT_C, { text: TERM, size: 50 });
    const cTenants = [...new Set(c.hits.hits.map((h) => h._source.tenantId))];
    check(
      c.hits.total.value === 1 && cTenants.length === 1 && cTenants[0] === TENANT_C,
      `through the module, tenant C sees ${c.hits.total.value} document(s) from [${cTenants.join(', ')}] — expected its 1`,
    );
    const repaired = (await holdersOf(tenantAlias(TENANT_C)))[INDEX_NAME]?.aliases?.[tenantAlias(TENANT_C)];
    check(
      JSON.stringify(repaired?.filter) === JSON.stringify({ term: { tenantId: TENANT_C } }),
      `and the alias now carries ${JSON.stringify(repaired?.filter ?? null)}`,
    );
  } finally {
    /**
     * In finally, and checked. The first version cleaned up on the success path
     * only, so any throw left fixtures and aliases in a shared index under random
     * tenant ids that no later run would ever find.
     */
    if (written > 0) {
      const { body: del } = await os.deleteByQuery({
        index: INDEX_NAME,
        refresh: true,
        body: { query: { terms: { tenantId: [TENANT_A, TENANT_B, TENANT_C] } } },
      });
      check(del.deleted === written, `cleanup removed ${del.deleted} of the ${written} fixture document(s)`);
    }
    const actions = [];
    for (const t of [TENANT_A, TENANT_B, TENANT_C]) {
      const alias = tenantAlias(t);
      for (const index of Object.keys(await holdersOf(alias))) actions.push({ remove: { index, alias } });
    }
    if (actions.length > 0) await os.indices.updateAliases({ body: { actions } });
    forgetTenantAliases();
  }

  console.log('');
  if (failures > 0) {
    console.error(`FAIL: ${failures} assertion(s) failed`);
    process.exitCode = 1;
    return;
  }
  console.log("a tenant's search reaches only its own data: by the alias for documents, by the module for everything else");
}

main().catch((err) => {
  console.error(err);
  process.exitCode = 1;
});
