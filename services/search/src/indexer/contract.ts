import { z } from 'zod';
import { TENANT_ID_PATTERN } from '../index-definition.js';

/**
 * What the indexer requires of an events.activity message, checked AFTER Avro
 * has decoded it.
 *
 * Avro already guarantees the shape: every field present, with its declared
 * type. It guarantees nothing a plain `string` cannot express, and two of those
 * matter here:
 *
 *   - an empty eventId is valid Avro. scripts/verify-dlq-malformed.mjs produces
 *     exactly that, and Ticketing learned to dead-letter it.
 *   - tenantId becomes the document's routing, and on the read side part of an
 *     alias name. A value that is not a tenant id must never reach either — the
 *     same pattern tenantAlias() enforces, so the write side cannot store a
 *     document the read side could never address.
 *
 * NOT .strict(). A compatible schema evolution adds a field with a default, every
 * message after it decodes with that field, and a strict parse would dead-letter
 * the whole topic on the day the new schema shipped.
 */
export const eventSnapshot = z.object({
  messageId: z.string().min(1),
  occurredAt: z.string().datetime({ offset: true }),
  // 512 bytes is OpenSearch's limit on _id.
  eventId: z.string().min(1).max(512),
  tenantId: z.string().regex(TENANT_ID_PATTERN, 'not a valid tenant id'),
  title: z.string(),
  slug: z.string(),
  description: z.string().nullable(),
  status: z.enum(['DRAFT', 'PUBLISHED', 'CANCELLED', 'COMPLETED']),
  capacity: z.number().int(),
  startsAt: z.string(),
  endsAt: z.string(),
  organiserId: z.string(),
  version: z.number().int().positive(),
  changeType: z.enum(['CREATED', 'UPDATED', 'PUBLISHED', 'CANCELLED', 'DELETED']),
  deleted: z.boolean(),
});

export type EventSnapshot = z.infer<typeof eventSnapshot>;
