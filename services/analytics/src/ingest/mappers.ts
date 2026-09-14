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

/**
 * Every string that reaches a row must be well-formed Unicode.
 *
 * A lone UTF-16 surrogate is a valid JavaScript string and nothing more: any Live
 * client can put one in an eventId, and JSON.stringify carries it to Kafka as the
 * escape \ud800. Measured on ClickHouse 25.8, one in a String column refused the
 * whole insert (CANNOT_PARSE_ESCAPE_SEQUENCE), which stalled the partition until
 * writer.ts learned to find the refused message, and one in a Map value was stored
 * without complaint as U+FFFD. Refused here, it is one dead letter naming the field.
 */
const id = z
  .string()
  .min(1)
  .refine((s) => s.isWellFormed(), 'not well-formed Unicode (a lone surrogate)');

/**
 * Column bounds, enforced here because ClickHouse does not enforce them. Measured
 * on 25.8, each of these was stored without an error: 70000 in a UInt16 as 4464,
 * 5e9 in a UInt32 as 705032704, 2^64 in a UInt64 as 384.
 */
const UINT16_MAX = 65_535;
const UINT32_MAX = 4_294_967_295;
const quantity = z.number().int().positive().max(UINT32_MAX);
const cents = z.number().int().nonnegative().max(Number.MAX_SAFE_INTEGER);
/** A line's position becomes its row's `line`, a UInt16. */
const MAX_LINES = UINT16_MAX + 1;

/**
 * An instant exactly as Date.prototype.toISOString writes one, which is how every
 * producer here stamps it: the relay from the outbox row's createdAt, Live from
 * new Date().
 *
 * Measured on 25.8, DateTime64 coerces rather than refuses: 'not a time' was
 * stored as 1900-01-01, '2026-02-30' as 2 March, year 2500 as 2299-12-31. So the
 * value must survive a round trip through Date unchanged — which refuses a day
 * that does not exist, since Date rolls it into the next month — and sit well
 * inside the column's range.
 */
const EARLIEST = Date.parse('1970-01-01T00:00:00.000Z');
const LATEST = Date.parse('2200-01-01T00:00:00.000Z');
const instant = z.string().refine((s) => {
  const t = Date.parse(s);
  return !Number.isNaN(t) && t >= EARLIEST && t < LATEST && new Date(t).toISOString() === s;
}, 'not a UTC instant as toISOString writes one, between 1970 and 2200');

const reservation = z.object({
  messageId: id,
  occurredAt: instant,
  reservationId: id,
  tenantId,
  eventId: id,
  userId: id,
  state: z.enum(['RESERVED', 'RELEASED', 'EXPIRED', 'CANCELLED', 'CONFIRMED']),
  items: z.array(z.object({ ticketTypeId: id, quantity })).min(1).max(MAX_LINES),
});

const RESERVATION_ACTIVITY = {
  RESERVED: 'ticket_reserved',
  RELEASED: 'reservation_released',
  EXPIRED: 'reservation_expired',
  CANCELLED: 'reservation_cancelled',
  CONFIRMED: 'reservation_confirmed',
} as const;

const purchase = z.object({
  messageId: id,
  occurredAt: instant,
  paymentId: id,
  reservationId: id,
  tenantId,
  eventId: id,
  userId: id,
  amountCents: cents,
  currency: z.string().regex(/^[A-Z]{3}$/, 'not an ISO-4217 code'),
  items: z
    .array(
      z
        .object({ ticketTypeId: id, quantity, unitPriceCents: cents })
        // The row's amount is quantity × price, and two in-range factors can still
        // multiply past 2^53, where a JavaScript number stops being exact — and
        // past 2^64, which ClickHouse wraps rather than refuses.
        .refine((l) => Number.isSafeInteger(l.quantity * l.unitPriceCents), {
          message: 'quantity × unitPriceCents is past 2^53 − 1 cents',
          path: ['unitPriceCents'],
        }),
    )
    .min(1)
    .max(MAX_LINES),
});

const interaction = z.object({
  kind: z.enum(['question.created', 'question.upvoted']),
  tenantId,
  eventId: id,
  userId: id,
  questionId: id,
  occurredAt: instant,
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
