import { setTimeout as sleep } from 'node:timers/promises';
import { createLogger } from '@eventhub/logger';
import { os } from './opensearch.js';
import { TENANT_ID_PATTERN, tenantAlias } from './index-definition.js';

const log = createLogger('search-migrate');

export interface MigrateOptions {
  /** The index to end up on: events-v{INDEX_VERSION} in production. */
  target: string;
  readAlias: string;
  writeAlias: string;
  /** The definition the target is created from, when it does not exist yet. */
  body: {
    settings: { index: Record<string, unknown> } & Record<string, unknown>;
    mappings: unknown;
  };
  /** Test seam: runs after writes have moved to the target, before the copy. */
  onWriteAliasMoved?: () => Promise<void>;
  /**
   * Test seam: runs after the copy has finished, before the swap. A write landing
   * here is the one that distinguishes moving writes first from not: the copy is
   * over, so only the write alias decides where it goes.
   */
  onCopied?: () => Promise<void>;
  pollMs?: number;
}

export interface MigrateResult {
  source: string;
  target: string;
  created: number;
  versionConflicts: number;
  sourceCount: number;
  targetCount: number;
  tenantAliasesMoved: number;
}

async function holdersOf(alias: string): Promise<string[]> {
  const response = await os.indices.getAlias({ name: alias }, { ignore: [404] });
  return response.statusCode === 404 ? [] : Object.keys(response.body as Record<string, unknown>);
}

/** Tenant ids with an alias on `index`, read from the alias names. */
async function tenantsWithAliasesOn(index: string): Promise<string[]> {
  const { body } = await os.indices.getAlias({ index });
  const names = Object.keys((body as Record<string, { aliases: Record<string, unknown> }>)[index]?.aliases ?? {});
  const tenants: string[] = [];
  for (const name of names) {
    const match = /^tenant-(.+)-events$/.exec(name);
    if (!match) continue;
    if (!TENANT_ID_PATTERN.test(match[1]!)) {
      // Not moved, and said so. An alias this code could never have built is not
      // one to carry forward on its word.
      log.warn({ alias: name }, 'alias looks like a tenant alias but its id is invalid; not moving it');
      continue;
    }
    tenants.push(match[1]!);
  }
  return tenants;
}

/**
 * Moves search from the index the read alias resolves to onto `target`, without
 * downtime and without losing a write.
 *
 * WHY THIS EXISTS: analysis happens at index time. A new analyzer applies to
 * nothing already indexed, and shard count and most of the mapping cannot change
 * in place. So a mapping change is a new index, filled from the old one, and a
 * swap — which is why the application only ever addresses aliases.
 *
 * THE ORDER, and what each step buys:
 *
 *   1. CREATE the target tuned for loading: refresh off, no replicas. Every
 *      refresh during a bulk copy is a segment nobody searches yet.
 *
 *   2. MOVE WRITES FIRST. From here the indexer writes only to the target. The
 *      alternative the Field Guide describes is dual-writing to both, which needs
 *      a flag in the indexer; moving the write alias needs nothing, and loses
 *      nothing. The price is bounded staleness: until step 5, searches read the
 *      source, which stops receiving new writes. Fine for a copy measured in
 *      minutes; a dual-write is the answer when it is measured in hours.
 *
 *   3. COPY with version_type external and conflicts: proceed. The source's
 *      documents carry their source versions, so a copy of an older snapshot that
 *      arrives after the indexer has written a newer one to the target is refused
 *      with a version conflict — counted, not an error. Without external
 *      versioning the copy would overwrite fresh writes with stale ones.
 *
 *   4. RESTORE refresh and replicas from the definition, refresh once, and COMPARE
 *      counts. Refuse to swap onto a target with fewer documents than the source.
 *
 *   5. SWAP the read alias AND EVERY TENANT ALIAS in ONE _aliases call. The cluster
 *      applies it as a single cluster-state update, so there is no moment when a
 *      tenant's alias points at nothing — which is exactly what a remove followed
 *      by an add would produce.
 *
 * The source is left in place. Rolling back is this same function pointed at the
 * source: writes made to the target since step 2 are copied back the same way.
 */
export async function migrateIndex(opts: MigrateOptions): Promise<MigrateResult> {
  const pollMs = opts.pollMs ?? 500;
  const readHolders = await holdersOf(opts.readAlias);
  if (readHolders.length !== 1) {
    throw new Error(
      `${opts.readAlias} resolves to [${readHolders.join(', ')}]; a migration needs exactly one source`,
    );
  }
  const source = readHolders[0]!;

  if (source === opts.target) {
    log.info({ index: source }, 'the read alias already resolves to the target; nothing to migrate');
    return { source, target: opts.target, created: 0, versionConflicts: 0, sourceCount: 0, targetCount: 0, tenantAliasesMoved: 0 };
  }

  // 1. The target, tuned for loading.
  const loading = { refresh_interval: '-1', number_of_replicas: 0 };
  const { body: exists } = await os.indices.exists({ index: opts.target });
  if (exists) {
    await os.indices.putSettings({ index: opts.target, body: { index: loading } });
  } else {
    await os.indices.create({
      index: opts.target,
      body: {
        ...opts.body,
        settings: { ...opts.body.settings, index: { ...opts.body.settings.index, ...loading } },
      } as unknown as Record<string, unknown>,
    });
  }
  log.info({ source, target: opts.target, existed: exists }, 'target ready for loading');

  // 2. Writes move first.
  await os.indices.updateAliases({
    body: {
      actions: [
        { remove: { index: source, alias: opts.writeAlias } },
        { add: { index: opts.target, alias: opts.writeAlias, is_write_index: true } },
      ],
    },
  });
  log.info({ alias: opts.writeAlias, target: opts.target }, 'writes now go to the target');
  await opts.onWriteAliasMoved?.();

  // 3. The copy.
  const { body: started } = await os.reindex({
    wait_for_completion: false,
    slices: 'auto',
    refresh: false,
    body: {
      conflicts: 'proceed',
      source: { index: source, size: 1000 },
      dest: { index: opts.target, version_type: 'external' },
    },
  });
  const taskId = (started as { task: string }).task;

  let finished: { error?: unknown; response?: { created?: number; version_conflicts?: number; failures?: unknown[] } };
  for (;;) {
    const { body } = await os.tasks.get({ task_id: taskId });
    const task = body as { completed: boolean };
    if (task.completed) {
      finished = body as typeof finished;
      break;
    }
    await sleep(pollMs);
  }
  if (finished.error || (finished.response?.failures?.length ?? 0) > 0) {
    throw new Error(
      `the copy failed; aliases other than the write alias are unchanged: ` +
        JSON.stringify(finished.error ?? finished.response?.failures),
    );
  }

  await opts.onCopied?.();

  // 4. Restore, refresh, compare.
  await os.indices.putSettings({
    index: opts.target,
    body: {
      index: {
        refresh_interval: opts.body.settings.index['refresh_interval'],
        number_of_replicas: opts.body.settings.index['number_of_replicas'],
      },
    },
  });
  await os.indices.refresh({ index: opts.target });
  await os.indices.refresh({ index: source });

  const sourceCount = Number((await os.count({ index: source })).body.count);
  const targetCount = Number((await os.count({ index: opts.target })).body.count);
  if (targetCount < sourceCount) {
    throw new Error(
      `refusing to swap: ${opts.target} holds ${targetCount} documents and ${source} holds ${sourceCount}`,
    );
  }

  // 5. One atomic swap: the read alias and every tenant alias.
  const tenants = await tenantsWithAliasesOn(source);
  await os.indices.updateAliases({
    body: {
      actions: [
        { remove: { index: source, alias: opts.readAlias } },
        { add: { index: opts.target, alias: opts.readAlias } },
        ...tenants.flatMap((tenantId) => [
          { remove: { index: source, alias: tenantAlias(tenantId) } },
          {
            add: {
              index: opts.target,
              alias: tenantAlias(tenantId),
              filter: { term: { tenantId } },
              routing: tenantId,
            },
          },
        ]),
      ],
    },
  });

  const result: MigrateResult = {
    source,
    target: opts.target,
    created: finished.response?.created ?? 0,
    versionConflicts: finished.response?.version_conflicts ?? 0,
    sourceCount,
    targetCount,
    tenantAliasesMoved: tenants.length,
  };
  log.info(result, 'search now reads the target');
  return result;
}
