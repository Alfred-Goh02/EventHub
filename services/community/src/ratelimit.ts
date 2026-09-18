import type { RequestHandler } from 'express';
import { getAuth } from '@eventhub/auth-middleware';
import type { Logger } from '@eventhub/logger';
import { redis } from './redis.js';

/**
 * A token bucket per user, per action, evaluated inside Redis.
 *
 * WHY A SCRIPT AND NOT GET-THEN-SET. Read the count, decide, write it back: two
 * concurrent requests both read the last token and both pass. That is not a rare
 * interleaving — it is what a burst IS, and a burst is exactly when the limit matters.
 * Everything below happens in one atomic evaluation.
 *
 * WHY THE CLOCK COMES FROM REDIS. The obvious version passes `now` from the caller,
 * and then two replicas with skewed clocks refill the same bucket at different rates:
 * the one that is running fast hands out tokens the other has not earned. `TIME` is
 * one clock for every caller. Scripts may call it because Redis replicates their
 * effects rather than the script itself.
 *
 * WHY THE TENANT LEADS THE KEY: one tenant's traffic can never spend another's budget,
 * whatever a future caller passes as an action.
 */
const TOKEN_BUCKET = `
local key, capacity, refill, cost = KEYS[1], tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local t = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000
local bucket = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(bucket[1])
local ts = tonumber(bucket[2])
if tokens == nil or ts == nil then
  tokens = capacity
  ts = now
end
tokens = math.min(capacity, tokens + (now - ts) * refill)
if tokens < cost then
  return { 0, math.ceil((cost - tokens) / refill) }
end
tokens = tokens - cost
redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
-- Long enough that a full bucket cannot expire mid-window, short enough that idle
-- users leave nothing behind.
redis.call('EXPIRE', key, math.ceil(capacity / refill) * 2)
return { 1, 0 }
`;

export interface Decision {
  allowed: boolean;
  /** Whole seconds until the next token, for Retry-After. */
  retryAfterS: number;
}

export function createRateLimiter(logger: Logger) {
  /** Loaded once and called by digest; EVAL is the fallback after a SCRIPT FLUSH. */
  let sha: string | null = null;

  async function take(key: string, capacity: number, refillPerSec: number, cost = 1): Promise<Decision> {
    const args = [String(capacity), String(refillPerSec), String(cost)];
    let raw: [number, number];
    try {
      sha ??= await redis.script('LOAD', TOKEN_BUCKET) as string;
      raw = (await redis.evalsha(sha, 1, key, ...args)) as [number, number];
    } catch (err) {
      // NOSCRIPT means the digest is gone — a restart, or someone ran SCRIPT FLUSH.
      // Re-loading on the next call is the whole recovery.
      if (err instanceof Error && err.message.includes('NOSCRIPT')) {
        sha = null;
        raw = (await redis.eval(TOKEN_BUCKET, 1, key, ...args)) as [number, number];
      } else {
        throw err;
      }
    }
    return { allowed: raw[0] === 1, retryAfterS: raw[1] };
  }

  return {
    take,

    /**
     * Express middleware for one action.
     *
     * OPEN, NOT CLOSED, when Redis is unreachable: the limiter exists to stop one user
     * flooding the tables, and refusing every write in the meantime would turn a cache
     * outage into a service outage. It is logged, so the gap is visible rather than
     * silent.
     */
    middleware(action: string, perMinute: number): RequestHandler {
      const capacity = perMinute;
      const refillPerSec = perMinute / 60;
      return async (req, res, next) => {
        const { tenantId, userId } = getAuth(req);
        const key = `rl:${tenantId}:${userId}:${action}`;
        let decision: Decision;
        try {
          decision = await take(key, capacity, refillPerSec);
        } catch (err) {
          logger.error({ err, action }, 'the rate limiter could not reach Redis; allowing the request');
          return next();
        }
        if (!decision.allowed) {
          res.setHeader('Retry-After', String(Math.max(1, decision.retryAfterS)));
          return res.status(429).json({ error: `too many ${action} requests`, retryAfterSeconds: decision.retryAfterS });
        }
        next();
      };
    },
  };
}
