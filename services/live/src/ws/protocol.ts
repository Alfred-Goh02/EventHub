import { z } from 'zod';

/**
 * Every byte arriving on a socket is untrusted input, exactly like an HTTP body,
 * and gets the same treatment.
 *
 * A discriminated union on `t` also buys exhaustiveness checking in the handler
 * switch: adding a message type here without handling it there is a compile
 * error rather than a message the server silently ignores.
 */
export const clientMessage = z.discriminatedUnion('t', [
  z.object({ t: z.literal('subscribe'), eventId: z.string().min(1).max(64) }),
  z.object({ t: z.literal('unsubscribe'), eventId: z.string().min(1).max(64) }),
  z.object({
    t: z.literal('question.create'),
    eventId: z.string().min(1).max(64),
    body: z.string().trim().min(1).max(500),
    /**
     * Idempotency for a reconnecting client.
     *
     * A client that resends after a dropped connection cannot know whether the
     * first attempt landed. Echoing this back on the broadcast lets it match the
     * question it sees to the one it sent instead of rendering a duplicate.
     */
    clientMsgId: z.string().uuid(),
  }),
  z.object({
    t: z.literal('question.upvote'),
    eventId: z.string().min(1).max(64),
    questionId: z.string().min(1).max(64),
  }),
]);

export type ClientMessage = z.infer<typeof clientMessage>;

/**
 * Close codes the server sends.
 *
 * 1000 and 3000-4999 are the only ranges application code may use — passing a
 * reserved code like 1005, 1006 or 1015 to ws.close() throws a RangeError. The
 * 4xxx entries exist because a REJECTED HANDSHAKE tells browser JavaScript
 * almost nothing: it surfaces as a close event with code 1006 and no detail. Any
 * failure the client needs to act on differently therefore has to happen AFTER
 * the upgrade, with a code it can actually read.
 *
 * The reason string is capped at 123 bytes of UTF-8 by the protocol, so anything
 * longer than a couple of words goes in a normal message sent just before the
 * close frame.
 */
export const CLOSE = Object.freeze({
  NORMAL: 1000,
  /** The pod is draining. Reconnect after the jittered delay we just sent. */
  GOING_AWAY: 1001,
  /** Protocol abuse — unparseable frames. */
  POLICY: 1008,
  INTERNAL: 1011,
  /** bufferedAmount blew the cap. Reconnect immediately; re-snapshot on connect. */
  SLOW_CONSUMER: 4008,
  /** The access token reached its exp. Refresh, then reconnect. */
  TOKEN_EXPIRED: 4401,
  /** Not authorised for this event. TERMINAL — retrying it is a self-inflicted DDoS. */
  FORBIDDEN: 4403,
  /** Too many messages on this socket. Retry with a long backoff. */
  RATE_LIMITED: 4429,
});

/** Messages the server sends. Serialised once and broadcast as a string. */
export type ServerMessage =
  | { t: 'ready'; expiresAt: number }
  | { t: 'subscribed'; eventId: string }
  | { t: 'unsubscribed'; eventId: string }
  | { t: 'error'; code: string }
  | { t: 'shutdown'; retryAfterMs: number }
  | { t: 'question.created'; clientMsgId: string; question: unknown }
  | { t: 'question.upvoted'; id: string; voteCount: number };
