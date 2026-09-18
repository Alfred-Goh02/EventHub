/**
 * THE NAMED EXPORT, not the default — the same interop trap services/live/src/redis.ts
 * documents. ioredis is CommonJS, and under NodeNext ESM a default import resolves to
 * the module namespace, so `new Redis(...)` will not compile.
 */
import { Redis } from 'ioredis';
import { env } from './env.js';

/**
 * One connection, unlike Live's two.
 *
 * Live needs a second client because SUBSCRIBE puts a connection into subscriber mode,
 * where it refuses almost everything else. Nothing here subscribes: this client caches
 * memberships and runs the rate limiter's script, both ordinary commands.
 */
export const redis = new Redis(env.REDIS_URL, {
  /**
   * Fail a command rather than hang on it.
   *
   * The opposite of Live's choice, and for the opposite reason. There, a dropped
   * command is a lost fan-out nobody can retry, so it waits. Here, both callers sit in
   * front of a request: a membership lookup falls back to Postgres, and the rate
   * limiter refuses the write. Waiting would turn a Redis blip into hung requests.
   */
  maxRetriesPerRequest: 2,
  enableOfflineQueue: false,
});

redis.on('error', () => {
  // Swallowed here and reported by the callers, which know what to do without it:
  // ioredis emits on every reconnection attempt, and an unhandled 'error' on an
  // EventEmitter exits the process.
});
