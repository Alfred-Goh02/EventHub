import { z } from 'zod';

/** One row of analytics.activity_events, as JSONEachRow expects it. */
export interface ActivityRow {
  tenant_id: string;
  activity: string;
  occurred_at: string;
  message_id: string;
  line: number;
  event_id: string;
  ticket_type_id: string;
  user_id: string;
  quantity: number;
  amount_cents: number;
  currency: string;
  source_topic: string;
  kafka_partition: number;
  kafka_offset: number;
  props: Record<string, string>;
}

export interface KafkaCoordinates {
  topic: string;
  partition: number;
  offset: string;
}

/** The topics Analytics ingests, and how each is encoded on the wire. */
export const INGEST_TOPICS = {
  'tickets.reserved': 'avro',
  'tickets.purchased': 'avro',
  'live.interactions': 'json',
} as const;

export type IngestTopic = keyof typeof INGEST_TOPICS;

/**
 * Only topics whose rows feed a rollup. events.activity carries event lifecycle
 * snapshots, which no rollup reads, and community.feed has no producer at all;
 * ingesting either would add a consumer, a DLQ source and storage for nothing a
 * dashboard shows.
 */
export const isIngestTopic = (topic: string): topic is IngestTopic => topic in INGEST_TOPICS;

/**
 * The same pattern the rest of the system holds a tenant id to. A tenant id is
 * the first column of every sort key and the value every read filters on; a
 * malformed one would be stored under a tenant no query can ever name.
 */
const tenantId = z.string().regex(/^[a-z0-9][a-z0-9_]{0,63}$/, 'not a valid tenant id');

/**
 * 'YYYY-MM-DD HH:mm:ss.SSS' in UTC — the one format DateTime64(3, 'UTC') parses
 * the same way everywhere. An ISO string with a trailing Z parses only under
 * date_time_input_format = best_effort, and a local-time string would be read as
 * UTC and shift silently: a funnel that dips at midnight months later.
 */
export function toClickHouseTime(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) throw new Error(`not a timestamp: ${JSON.stringify(iso)}`);
  return d.toISOString().replace('T', ' ').replace('Z', '');
}

const reservation = z.object({
  messageId: z.string().min(1),
  occurredAt: z.string(),
  reservationId: z.string().min(1),
  tenantId,
  eventId: z.string().min(1),
  userId: z.string().min(1),
  state: z.enum(['RESERVED', 'RELEASED', 'EXPIRED', 'CANCELLED', 'CONFIRMED']),
  items: z.array(z.object({ ticketTypeId: z.string().min(1), quantity: z.number().int().positive() })).min(1),
});

const RESERVATION_ACTIVITY = {
  RESERVED: 'ticket_reserved',
  RELEASED: 'reservation_released',
  EXPIRED: 'reservation_expired',
  CANCELLED: 'reservation_cancelled',
  CONFIRMED: 'reservation_confirmed',
} as const;

const purchase = z.object({
  messageId: z.string().min(1),
  occurredAt: z.string(),
  paymentId: z.string().min(1),
  reservationId: z.string().min(1),
  tenantId,
  eventId: z.string().min(1),
  userId: z.string().min(1),
  amountCents: z.number().int().nonnegative(),
  currency: z.string().regex(/^[A-Z]{3}$/, 'not an ISO-4217 code'),
  items: z
    .array(
      z.object({
        ticketTypeId: z.string().min(1),
        quantity: z.number().int().positive(),
        unitPriceCents: z.number().int().nonnegative(),
      }),
    )
    .min(1),
});

const interaction = z.object({
  kind: z.enum(['question.created', 'question.upvoted']),
  tenantId,
  eventId: z.string().min(1),
  userId: z.string().min(1),
  questionId: z.string().min(1),
  occurredAt: z.string(),
});

const base = (at: KafkaCoordinates) => ({
  source_topic: at.topic,
  kafka_partition: at.partition,
  kafka_offset: Number(at.offset),
});

/**
 * A decoded message becomes one row PER LINE, not per message.
 *
 * A reservation or a purchase can cover several ticket types, and the funnel is
 * kept per ticket type. One row per message would have to pick one ticket type
 * or invent a composite; one row per line keeps each line's quantity and money
 * where a GROUP BY can find it. `line` plus `message_id` identifies each row by
 * its source, so a duplicate is visible even when Kafka redelivered it at a
 * different offset.
 *
 * Throws on anything that does not match. The caller dead-letters what throws,
 * so a schema-drifted message becomes an inspectable row rather than a crash or a
 * silently wrong one.
 */
export function toRows(at: KafkaCoordinates, decoded: unknown): ActivityRow[] {
  const topic = at.topic;

  if (topic === 'tickets.reserved') {
    const r = reservation.parse(decoded);
    return r.items.map((item, line) => ({
      ...base(at),
      tenant_id: r.tenantId,
      activity: RESERVATION_ACTIVITY[r.state],
      occurred_at: toClickHouseTime(r.occurredAt),
      message_id: r.messageId,
      line,
      event_id: r.eventId,
      ticket_type_id: item.ticketTypeId,
      user_id: r.userId,
      quantity: item.quantity,
      amount_cents: 0,
      currency: '',
      props: { reservation_id: r.reservationId },
    }));
  }

  if (topic === 'tickets.purchased') {
    const p = purchase.parse(decoded);
    return p.items.map((item, line) => ({
      ...base(at),
      tenant_id: p.tenantId,
      activity: 'ticket_purchased',
      occurred_at: toClickHouseTime(p.occurredAt),
      message_id: p.messageId,
      line,
      event_id: p.eventId,
      ticket_type_id: item.ticketTypeId,
      user_id: p.userId,
      quantity: item.quantity,
      // The line's own money: the price captured at reservation time, times the
      // quantity. The message's amountCents is the charge total and is kept in
      // props, so a gap between the two — fees, a discount — stays visible
      // rather than being spread across lines by a guess.
      amount_cents: item.quantity * item.unitPriceCents,
      currency: p.currency,
      props: { payment_id: p.paymentId, reservation_id: p.reservationId, charge_total_cents: String(p.amountCents) },
    }));
  }

  if (topic === 'live.interactions') {
    const i = interaction.parse(decoded);
    return [
      {
        ...base(at),
        tenant_id: i.tenantId,
        activity: i.kind === 'question.created' ? 'question_asked' : 'question_upvoted',
        occurred_at: toClickHouseTime(i.occurredAt),
        // Live publishes without an outbox, so there is no message id. This is
        // the interaction's natural identity: one upvote per user per question.
        message_id: `${i.kind}:${i.questionId}:${i.userId}`,
        line: 0,
        event_id: i.eventId,
        ticket_type_id: '',
        user_id: i.userId,
        quantity: 0,
        amount_cents: 0,
        currency: '',
        props: { question_id: i.questionId },
      },
    ];
  }

  throw new Error(`no mapper for topic ${topic}`);
}
