import { createLogger } from '@eventhub/logger';
import { env } from '../env.js';
import { os } from '../opensearch.js';
import { INDEX_NAME, READ_ALIAS, WRITE_ALIAS, indexBody } from '../index-definition.js';

/**
 * Creates the index and points the two cluster-wide aliases at it — and refuses,
 * loudly, each situation in which doing that would be wrong.
 *
 * IDEMPOTENT ON PURPOSE. This is the shape of a Kubernetes init container or a
 * Helm hook — it runs on every rollout, not once by hand — so "already done" is
 * a normal outcome rather than an error.
 *
 * THREE REFUSALS, each a way this went wrong or demonstrably would have:
 *
 *   AN INDEX HOLDING AN ALIAS'S NAME. With auto-creation on, a write through
 *   events-write before this script ran invented an index called events-write,
 *   and every run after that failed with an alias-name error that did not say how
 *   the index got there. docker-compose.yml now forbids auto-creating events-*
 *   names; a managed domain may not, so this checks regardless.
 *
 *   AN EXISTING INDEX THAT DOES NOT MATCH THE DEFINITION. The first version
 *   printed "already exists, leaving it alone" for ANY index of that name —
 *   including one whose tenantId an early write had typed as analysed text, the
 *   one field isolation rests on. Shard count, dynamic and that mapping cannot be
 *   changed in place, so the only honest answers are a new INDEX_VERSION or, on a
 *   development cluster, deleting it.
 *
 *   ALIASES ALREADY POINTING AT A DIFFERENT INDEX. Moving them is the last step
 *   of a migration, after the new index has been filled. Doing it here would swap
 *   search onto an empty index the moment INDEX_VERSION was bumped — so this
 *   creates the new index, leaves the aliases where they are, and says so.
 *
 * is_write_index is set even while the write alias resolves to one index. It is
 * REQUIRED the moment a migration has that alias spanning two: indexing through a
 * multi-index alias with no write index is refused.
 */
const log = createLogger('search-bootstrap');

async function holdersOf(alias: string): Promise<string[]> {
  const response = await os.indices.getAlias({ name: alias }, { ignore: [404] });
  return response.statusCode === 404 ? [] : Object.keys(response.body as Record<string, unknown>);
}

async function refuseIndexNamedLikeAnAlias(): Promise<void> {
  for (const alias of [READ_ALIAS, WRITE_ALIAS]) {
    const { body: isAlias } = await os.indices.existsAlias({ name: alias });
    const { body: resolves } = await os.indices.exists({ index: alias });
    if (resolves && !isAlias) {
      throw new Error(
        `an INDEX named ${alias} exists, so no alias can have that name. It was almost ` +
          'certainly invented by a write that reached the cluster before this script ran. ' +
          'Nothing in it is authoritative — the read model is rebuilt from Kafka — so delete ' +
          `it (curl -X DELETE ${env.OPENSEARCH_NODE}/${alias}) and run this again.`,
      );
    }
  }
}

async function ensureIndex(): Promise<void> {
  const { body: exists } = await os.indices.exists({ index: INDEX_NAME });

  if (!exists) {
    await os.indices.create({
      index: INDEX_NAME,
      body: indexBody as unknown as Record<string, unknown>,
    });
    log.info({ index: INDEX_NAME }, 'created index');
    return;
  }

  const { body: settings } = await os.indices.getSettings({ index: INDEX_NAME });
  const { body: mappings } = await os.indices.getMapping({ index: INDEX_NAME });
  const shards = Number(settings[INDEX_NAME]?.settings?.index?.number_of_shards);
  const mapping = mappings[INDEX_NAME]?.mappings;
  const wantShards = indexBody.settings.index.number_of_shards;

  const problems: string[] = [];
  if (shards !== wantShards) {
    problems.push(`it has ${shards} shard(s) and the definition says ${wantShards}`);
  }
  if (mapping?.properties?.tenantId?.type !== 'keyword') {
    problems.push(`tenantId is mapped as ${mapping?.properties?.tenantId?.type ?? 'nothing'}, not keyword`);
  }
  if (mapping?.dynamic !== 'strict') {
    problems.push(`dynamic is ${mapping?.dynamic ?? 'unset'}, not strict`);
  }

  if (problems.length > 0) {
    throw new Error(
      `${INDEX_NAME} exists and does not match index-definition.ts: ${problems.join('; ')}. ` +
        'None of these can be changed on an existing index. Bump INDEX_VERSION and migrate — ' +
        `or, on a development cluster with nothing worth keeping, delete ${INDEX_NAME} and run ` +
        'this again.',
    );
  }

  log.info({ index: INDEX_NAME, shards }, 'index already exists and matches the definition');
}

async function pointAliases(): Promise<void> {
  const wanted: Array<[string, Record<string, unknown>]> = [
    [READ_ALIAS, {}],
    [WRITE_ALIAS, { is_write_index: true }],
  ];

  const actions: Record<string, unknown>[] = [];
  for (const [alias, extra] of wanted) {
    const holders = await holdersOf(alias);
    if (holders.length === 0 || (holders.length === 1 && holders[0] === INDEX_NAME)) {
      // Re-adding an alias that is already there is a no-op, and it restores
      // is_write_index if something removed it.
      actions.push({ add: { index: INDEX_NAME, alias, ...extra } });
    } else {
      log.warn(
        { alias, holders, index: INDEX_NAME },
        'alias already points at a different index. Moving it is the last step of a ' +
          'migration, not a bootstrap, so it stays where it is',
      );
    }
  }

  if (actions.length > 0) await os.indices.updateAliases({ body: { actions } });
  log.info({ read: READ_ALIAS, write: WRITE_ALIAS, index: INDEX_NAME }, 'aliases checked');
}

async function main(): Promise<void> {
  // In this order: the alias-name check has to come before anything that could
  // itself fail on the name, or the error blames the wrong thing.
  await refuseIndexNamedLikeAnAlias();
  await ensureIndex();
  await pointAliases();
}

main().then(
  () => process.exit(0),
  (err: unknown) => {
    log.error({ err }, 'bootstrap failed');
    process.exit(1);
  },
);
