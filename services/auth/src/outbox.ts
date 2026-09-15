import type { Prisma } from './generated/prisma/client.js';

/**
 * The single place this service writes to its outbox.
 *
 * The same shape as Event's and Ticketing's, deliberately: the relay reads `topic`
 * off the row and is otherwise routing-free, so draining this table needs a
 * different connection string and nothing else.
 *
 * MUST be called with the same `tx` as the change the message describes. That is
 * the whole pattern: the row and the change commit together or not at all.
 *
 * `id` may be chosen by the caller, for a message that has to name itself before
 * the row exists. A sealed email request carries its own id as its jti.
 */
export async function emit(
  tx: Prisma.TransactionClient,
  params: {
    id?: string;
    tenantId: string;
    topic: string;
    eventType: string;
    aggregateId: string;
    payload: unknown;
    schemaVersion?: number;
  },
): Promise<void> {
  await tx.outboxMessage.create({
    data: {
      ...(params.id ? { id: params.id } : {}),
      tenantId: params.tenantId,
      topic: params.topic,
      eventType: params.eventType,
      schemaVersion: params.schemaVersion ?? 1,
      aggregateId: params.aggregateId,
      payload: params.payload as Prisma.InputJsonValue,
    },
  });
}
