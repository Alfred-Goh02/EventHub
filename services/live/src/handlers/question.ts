import { withTenant } from '../db.js';
import { Prisma } from '../generated/prisma/client.js';
import { channelFor, publisher } from '../redis.js';
import { emitInteraction } from '../kafka.js';
import { join, leave } from '../ws/rooms.js';
import { send, type LiveSocket } from '../ws/connection.js';
import type { ClientMessage } from '../ws/protocol.js';

export async function handleClientMessage(ws: LiveSocket, msg: ClientMessage): Promise<void> {
  switch (msg.t) {
    case 'subscribe':
      return onSubscribe(ws, msg.eventId);
    case 'unsubscribe':
      return onUnsubscribe(ws, msg.eventId);
    case 'question.create':
      return onCreate(ws, msg);
    case 'question.upvote':
      return onUpvote(ws, msg);
    default: {
      // Exhaustiveness. Adding a message type to the union without handling it
      // here is a compile error rather than a frame the server silently drops.
      const unreachable: never = msg;
      throw new Error(`unhandled message: ${JSON.stringify(unreachable)}`);
    }
  }
}

async function onSubscribe(ws: LiveSocket, eventId: string): Promise<void> {
  /**
   * TODO (ADR-507, still open): this authorises nothing beyond the tenant.
   *
   * The channel key makes cross-TENANT access impossible by construction. It
   * does nothing for a private event WITHIN a tenant: any authenticated user of
   * tenant A who learns a tenant A eventId can join that room. Live owns no
   * event data — the Event service does — so closing this needs either a
   * synchronous access check against Event on every subscribe (correct, always
   * current, and puts Event on the critical path of every reconnect storm) or a
   * local read model built from the events.activity topic (no synchronous
   * dependency, eventually consistent, so a just-granted user may be refused).
   *
   * Shipping it open and saying so beats claiming an isolation that is not there.
   * Whichever option wins, the check belongs here and nowhere else.
   */
  await join(ws, channelFor(ws.tenantId, eventId));
  send(ws, { t: 'subscribed', eventId });
}

async function onUnsubscribe(ws: LiveSocket, eventId: string): Promise<void> {
  await leave(ws, channelFor(ws.tenantId, eventId));
  send(ws, { t: 'unsubscribed', eventId });
}

async function onCreate(
  ws: LiveSocket,
  msg: Extract<ClientMessage, { t: 'question.create' }>,
): Promise<void> {
  const channel = channelFor(ws.tenantId, msg.eventId);

  // 1. Postgres FIRST, because it is the system of record and what the snapshot
  //    endpoint reads. If the process dies after this and before the publish,
  //    the question exists and nobody sees it until they reconnect — which the
  //    snapshot covers, and is the whole reason a lossy bus is acceptable.
  const question = await withTenant(ws.tenantId, (tx) =>
    tx.question.create({
      data: {
        tenantId: ws.tenantId,
        eventId: msg.eventId,
        userId: ws.userId,
        body: msg.body,
      },
      select: { id: true, body: true, voteCount: true, createdAt: true, userId: true },
    }),
  );

  /**
   * 2. PUBLISH ONLY. Do NOT also write to this pod's own sockets.
   *
   * Redis delivers a published message back to the publishing pod's subscriber
   * connection as well, so the local clients are already covered by the normal
   * delivery path. Writing locally too is the classic bug: everyone on the
   * posting pod sees the question twice, and the author sees their own question
   * duplicated. One code path, no "is this local" branch — that branch IS the bug.
   */
  await publisher.publish(
    channel,
    JSON.stringify({ t: 'question.created', clientMsgId: msg.clientMsgId, question }),
  );

  // 3. Kafka last and unawaited. See kafka.ts for why this one is not an outbox.
  //
  //    occurredAt is the row's createdAt, which Postgres filled. It was `new Date()`,
  //    this process's clock, and Analytics stores it as occurred_at: measured with
  //    Live an hour fast, +3600 s from the database's now(). See CLAUDE.md,
  //    "Timestamps come from the database".
  emitInteraction({
    kind: 'question.created',
    tenantId: ws.tenantId,
    eventId: msg.eventId,
    userId: ws.userId,
    questionId: question.id,
    occurredAt: question.createdAt.toISOString(),
  });
}

async function onUpvote(
  ws: LiveSocket,
  msg: Extract<ClientMessage, { t: 'question.upvote' }>,
): Promise<void> {
  const channel = channelFor(ws.tenantId, msg.eventId);

  try {
    /**
     * LET THE COMPOSITE PRIMARY KEY DECIDE THE RACE.
     *
     * QuestionVote is keyed on (questionId, userId), so a second vote from the
     * same user violates the key. Inserting and catching P2002 is both cheaper
     * and more correct than reading first: two concurrent clicks both pass a
     * read-then-write check and both increment, and the count silently drifts
     * from the number of voters.
     *
     * Both statements are in one transaction, so voteCount cannot drift from the
     * vote rows even if the second fails.
     *
     * THE EVENT IS PART OF THE MATCH. The vote row keys only on the question, and
     * the eventId here is whatever the client sent — the same value that names the
     * channel below and goes to Kafka for Analytics to count. Matching on id alone
     * let any member of a tenant credit an upvote to any event id, one that does
     * not exist included (found by the review of Analytics' ingest, and measured
     * in scripts/verify-live-fanout.mjs). With the event in the WHERE, a question
     * that is not in that event is P2025, which rolls the vote row back with it.
     */
    const { vote, updated } = await withTenant(ws.tenantId, async (tx) => {
      // The vote's createdAt is Postgres's, and is the interaction's occurredAt below.
      const vote = await tx.questionVote.create({
        data: { questionId: msg.questionId, userId: ws.userId, tenantId: ws.tenantId },
        select: { createdAt: true },
      });
      const updated = await tx.question.update({
        where: { id: msg.questionId, eventId: msg.eventId },
        data: { voteCount: { increment: 1 } },
        select: { id: true, voteCount: true },
      });
      return { vote, updated };
    });

    await publisher.publish(
      channel,
      JSON.stringify({ t: 'question.upvoted', id: updated.id, voteCount: updated.voteCount }),
    );

    emitInteraction({
      kind: 'question.upvoted',
      tenantId: ws.tenantId,
      eventId: msg.eventId,
      userId: ws.userId,
      questionId: msg.questionId,
      occurredAt: vote.createdAt.toISOString(),
    });
  } catch (err) {
    if (err instanceof Prisma.PrismaClientKnownRequestError) {
      // P2002: already voted. Not an error the user needs to see as a failure.
      if (err.code === 'P2002') {
        send(ws, { t: 'error', code: 'already_voted' });
        return;
      }
      // P2025: the question does not exist, is not in the event named, or RLS
      // hid it because it belongs to another tenant. Those are indistinguishable
      // from here, and deliberately so — saying which would confirm the existence
      // of another tenant's row.
      if (err.code === 'P2025') {
        send(ws, { t: 'error', code: 'no_such_question' });
        return;
      }
    }
    throw err;
  }
}
