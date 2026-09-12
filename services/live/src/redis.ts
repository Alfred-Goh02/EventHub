/**
 * The NAMED export, not the default.
 *
 * ioredis is CommonJS, and under NodeNext ESM a default import of it resolves to
 * the module namespace rather than the class — so `new Redis(...)` fails to
 * compile with "This expression is not constructable". Every ioredis example
 * online shows the default import, because they are written for CJS.
 *
 * Same interop trap as kafkajs's ConfigResourceTypes in scripts/create-topics.mjs,
 * from the same cause: only some of a CommonJS module's exports survive as named
 * bindings, and which ones is not obvious from the docs.
 */
import { Redis } from 'ioredis';
import { env } from './env.js';

/**
 * TWO CONNECTIONS PER POD, ALWAYS — and this is not a style choice.
 *
 * Once a Redis connection issues SUBSCRIBE it is in SUBSCRIBER MODE, which is a
 * property of the connection rather than a command that ran. It will then refuse
 * almost everything else: only SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE,
 * SSUBSCRIBE, SUNSUBSCRIBE, PING, QUIT and RESET are allowed.
 *
 * Reuse one client and the first PUBLISH after the first SUBSCRIBE throws. The
 * reason that is worth a comment rather than a shrug is WHEN it throws: nothing
 * subscribes until a client joins a room, so a single-client smoke test passes
 * and the failure appears the moment two clients share an event. Classic "works
 * on my machine, breaks with real traffic".
 *
 * If presence counters with INCR ever land, those go on the publisher or a third
 * client — never on the subscriber.
 */
export const publisher = new Redis(env.REDIS_URL, {
  /**
   * Never give up on a command because the connection is down.
   *
   * The default retries a few times and then fails the command. For a fan-out
   * bus whose whole failure story is "reconnect and carry on", surfacing a
   * transient reconnect as an error in the question handler would turn a blip
   * into a user-visible failure of a write that already succeeded in Postgres.
   */
  maxRetriesPerRequest: null,
});

export const subscriber = publisher.duplicate();

/**
 * The channel name, and the reason cross-tenant leakage is impossible here
 * rather than merely checked for.
 *
 * tenantId comes from the VERIFIED JWT claim and never from anything the client
 * sent — the client only ever names an eventId. So a user in tenant A can only
 * be subscribed to channels beginning `live:t:A:`, and pods serving tenant B
 * only ever publish to `live:t:B:`. A malicious client that guesses another
 * tenant's event id subscribes to a channel nobody publishes to.
 *
 * The honest limit, worth volunteering rather than hiding: this gives nothing
 * for a PRIVATE event within a tenant. Any authenticated tenant A user who
 * learns a tenant A eventId can join that room. That gap is real, it is the
 * subscribe handler's to close, and it is not closed here.
 */
export const channelFor = (tenantId: string, eventId: string) =>
  `live:t:${tenantId}:e:${eventId}`;

/**
 * Moving to ElastiCache cluster mode means swapping subscribe/publish for
 * ssubscribe/spublish, because plain PUBLISH broadcasts across the cluster bus
 * to every node — O(nodes) per message. Keeping both calls behind this module is
 * what makes that a one-file change.
 */
export async function closeRedis(): Promise<void> {
  await Promise.allSettled([publisher.quit(), subscriber.quit()]);
}
