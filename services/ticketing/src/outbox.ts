import type { Prisma } from './generated/prisma/client.js';

/**
 * The single place this service writes to its outbox.
 *
 * Identical in shape to the Event service's, and deliberately so: the relay
 * reads `topic` off the row and is otherwise routing-free, so draining this
 * table needs a different connection string and nothing else.
 */
export async function emit(
  tx: Prisma.TransactionClient,
  params: {
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
      tenantId: params.tenantId,
      topic: params.topic,
      eventType: params.eventType,
      schemaVersion: params.schemaVersion ?? 1,
      aggregateId: params.aggregateId,
      payload: params.payload as Prisma.InputJsonValue,
    },
  });
}
