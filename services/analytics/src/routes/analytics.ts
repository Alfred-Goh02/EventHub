import { Router, type Request, type RequestHandler, type Response } from 'express';
import { z } from 'zod';
import type { ClickHouseClient } from '@clickhouse/client';
import { getAuth } from '@eventhub/auth-middleware';
import { recordQuery, type RouteName } from '../query-metrics.js';

/**
 * The read API: what a dashboard asks ClickHouse, with the tenant taken from the token.
 *
 * A FACTORY, NOT A MODULE THAT CONNECTS. It takes the client and the auth guards
 * as arguments, so importing it loads no environment and no key.
 * scripts/verify-analytics-api.mjs imports it only to list the routes it declares,
 * and fails when a route has no isolation probe — so a route added later cannot
 * ship untested for another tenant's data.
 *
 * EVERY QUERY BINDS {tenant:String} FROM req.auth, as a parameter, never
 * interpolated. The tenant is also the first column of every rollup's sort key,
 * so the isolation filter and the index are the same line of SQL.
 *
 * READS MERGE. The rollups are AggregatingMergeTree: until a background merge
 * runs, one key legitimately has several rows, so every read groups and applies
 * sum and uniqMerge. And a total of PEOPLE is uniqMerge across the whole range,
 * never the sum of each day's count, which counts a buyer once per day they bought.
 *
 * MONEY IS PER CURRENCY, always a list and never one number: a sum across
 * currencies looks right and means nothing.
 */

/** The longest range one request may cover. The overview's cost grows with it. */
export const MAX_RANGE_DAYS = 366;
export const OVERVIEW_LIMIT_MAX = 200;
const DEFAULT_RANGE_DAYS = 30;
const DAY_MS = 86_400_000;

const isoDay = (t: number): string => new Date(t).toISOString().slice(0, 10);
const dayStart = (day: string): number => Date.parse(`${day}T00:00:00Z`);

/**
 * 'YYYY-MM-DD', and a day that exists. The pattern is not enough on its own:
 * Date.parse('2026-02-30T00:00:00Z') is 2 March rather than an error — measured on
 * this Node — so a range ending on a day that does not exist would quietly become
 * a range nobody asked for. Round-tripping the parsed day catches it.
 */
const calendarDay = z
  .string()
  .regex(/^\d{4}-\d{2}-\d{2}$/, 'expected YYYY-MM-DD')
  .refine((s) => {
    const t = dayStart(s);
    return !Number.isNaN(t) && isoDay(t) === s;
  }, 'not a calendar date');

/**
 * The query every route takes: both ends inclusive, in UTC days, the last 30 days
 * by default, at most MAX_RANGE_DAYS. `.strict()`, as in Search: ?tenantId=... is
 * a 400, so a client that believes it can choose a tenant is told it cannot.
 */
function withRange<T extends z.ZodRawShape>(extra: T) {
  return z
    .object({ from: calendarDay.optional(), to: calendarDay.optional(), ...extra })
    .strict()
    .transform((q) => {
      const to = q.to ?? isoDay(Date.now());
      const from = q.from ?? isoDay(dayStart(to) - (DEFAULT_RANGE_DAYS - 1) * DAY_MS);
      return { ...q, from, to };
    })
    .refine((q) => q.from <= q.to, { message: 'from is after to' })
    .refine((q) => (dayStart(q.to) - dayStart(q.from)) / DAY_MS < MAX_RANGE_DAYS, {
      message: `a range covers at most ${MAX_RANGE_DAYS} days`,
    });
}

export const rangeParams = withRange({});
export const overviewParams = withRange({
  limit: z.coerce.number().int().min(1).max(OVERVIEW_LIMIT_MAX).default(50),
});

/** An id outside this pattern cannot name an event this system created, so it never reaches a query. */
const eventIdParam = z.string().regex(/^[A-Za-z0-9_-]{1,64}$/, 'not an event id');

/**
 * Whether a failure is the STORE being unreachable, which is a 503 that says so.
 * Anything else — a query this file got wrong — is a bug, and goes to the error
 * handler as a 500 rather than being dressed up as an outage.
 */
function storeUnavailable(err: unknown): boolean {
  const e = err as { code?: unknown; message?: unknown } | null;
  const code = typeof e?.code === 'string' ? e.code : '';
  const message = typeof e?.message === 'string' ? e.message : '';
  return (
    ['ECONNREFUSED', 'ECONNRESET', 'ETIMEDOUT', 'EHOSTUNREACH', 'ENOTFOUND', 'EAI_AGAIN'].includes(code) ||
    /timeout|socket hang up/i.test(message)
  );
}

async function rows<T>(ch: ClickHouseClient, query: string, params: Record<string, unknown>): Promise<T[]> {
  const result = await ch.query({
    query,
    query_params: params,
    format: 'JSONEachRow',
    // UInt64 as JSON numbers rather than strings. Exact below 2^53: about 90
    // quadrillion cents, or more tickets than any event will sell.
    clickhouse_settings: { output_format_json_quote_64bit_integers: 0 },
  });
  return result.json<T>();
}

/** A 400 naming each problem, one line per issue. Returns null: the handler has answered. */
function reject(res: Response, errors: z.ZodError[]): null {
  res.status(400).json({
    error: errors.flatMap((e) => e.issues.map((i) => `${i.path.join('.') || 'query'}: ${i.message}`)),
  });
  return null;
}

/**
 * Times the route, answers, and sorts its failures by what they mean to an operator.
 *
 * `Cache-Control: private, no-store` on every answer. private alone still lets a
 * browser's disk cache keep a tenant's revenue on a shared machine; no-store keeps
 * it nowhere.
 */
function handle(route: RouteName, run: (req: Request, res: Response) => Promise<object | null>): RequestHandler {
  return async (req, res, next) => {
    const started = performance.now();
    try {
      const body = await run(req, res);
      if (body === null) {
        recordQuery(route, 'rejected', 0);
        return;
      }
      res.set('Cache-Control', 'private, no-store').json(body);
      recordQuery(route, 'served', performance.now() - started);
    } catch (err) {
      if (storeUnavailable(err)) {
        recordQuery(route, 'unavailable', 0);
        res.status(503).json({ error: 'analytics store unavailable' });
        return;
      }
      recordQuery(route, 'failed', 0);
      next(err);
    }
  };
}

interface CurrencyRow {
  currency: string;
  cents: number;
}
interface FunnelCounts {
  reserver_count: number;
  purchaser_count: number;
  reserved: number;
  released: number;
  expired: number;
  sold: number;
}

// Aliases never reuse a column's name. `sum(tickets_sold) AS tickets_sold` puts an
// alias and a column under one name in one query, and which one a later reference
// means is exactly the kind of question this repository has been bitten by before.
const FUNNEL_COUNTS = `uniqMerge(reservers) AS reserver_count, uniqMerge(purchasers) AS purchaser_count,
       sum(seats_reserved) AS reserved, sum(seats_released) AS released,
       sum(seats_expired) AS expired, sum(tickets_sold) AS sold`;

const funnelOut = (r: FunnelCounts) => ({
  reservers: r.reserver_count,
  purchasers: r.purchaser_count,
  seatsReserved: r.reserved,
  seatsReleased: r.released,
  seatsExpired: r.expired,
  ticketsSold: r.sold,
});

const money = (rs: CurrencyRow[]) => rs.map((r) => ({ currency: r.currency, cents: r.cents }));

export function createAnalyticsRouter(opts: { ch: ClickHouseClient; guards: RequestHandler[] }): Router {
  const { ch, guards } = opts;
  const router: Router = Router();

  /** One event's ticket funnel: totals for the range, per ticket type, and per day. */
  router.get(
    '/analytics/events/:eventId/funnel',
    ...guards,
    handle('funnel', async (req, res) => {
      const event = eventIdParam.safeParse(req.params['eventId']);
      const range = rangeParams.safeParse(req.query);
      if (!event.success || !range.success) {
        return reject(res, [event, range].flatMap((r): z.ZodError[] => (r.success ? [] : [r.error])));
      }

      const { tenantId } = getAuth(req);
      const p = { tenant: tenantId, event: event.data, from: range.data.from, to: range.data.to };
      const where = 'tenant_id = {tenant:String} AND event_id = {event:String} AND day BETWEEN {from:Date} AND {to:Date}';

      const [totals, totalMoney, byType, days, dayMoney] = await Promise.all([
        rows<FunnelCounts>(ch, `SELECT ${FUNNEL_COUNTS} FROM ticket_funnel_daily WHERE ${where}`, p),
        rows<CurrencyRow>(
          ch,
          `SELECT currency, sum(gross_cents) AS cents FROM ticket_funnel_daily
            WHERE ${where} AND currency != '' GROUP BY currency ORDER BY currency`,
          p,
        ),
        rows<Omit<FunnelCounts, 'reserver_count' | 'purchaser_count'> & { ticket_type: string }>(
          ch,
          `SELECT ticket_type_id AS ticket_type, sum(seats_reserved) AS reserved, sum(seats_released) AS released,
                  sum(seats_expired) AS expired, sum(tickets_sold) AS sold
             FROM ticket_funnel_daily WHERE ${where} GROUP BY ticket_type_id ORDER BY ticket_type_id`,
          p,
        ),
        rows<FunnelCounts & { d: string }>(
          ch,
          `SELECT toString(day) AS d, ${FUNNEL_COUNTS} FROM ticket_funnel_daily WHERE ${where} GROUP BY day ORDER BY day`,
          p,
        ),
        rows<CurrencyRow & { d: string }>(
          ch,
          `SELECT toString(day) AS d, currency, sum(gross_cents) AS cents FROM ticket_funnel_daily
            WHERE ${where} AND currency != '' GROUP BY day, currency ORDER BY day, currency`,
          p,
        ),
      ]);

      return {
        eventId: event.data,
        from: p.from,
        to: p.to,
        // An aggregate with no GROUP BY returns one row even over nothing, so an
        // event with no activity — or another tenant's event — is all zeros.
        totals: { ...funnelOut(totals[0]!), gross: money(totalMoney) },
        byTicketType: byType.map((r) => ({
          ticketTypeId: r.ticket_type,
          seatsReserved: r.reserved,
          seatsReleased: r.released,
          seatsExpired: r.expired,
          ticketsSold: r.sold,
        })),
        days: days.map((d) => ({ day: d.d, ...funnelOut(d), gross: money(dayMoney.filter((m) => m.d === d.d)) })),
      };
    }),
  );

  /** One event's live engagement, by the hour: the grain that shows a session's shape. */
  router.get(
    '/analytics/events/:eventId/engagement',
    ...guards,
    handle('engagement', async (req, res) => {
      const event = eventIdParam.safeParse(req.params['eventId']);
      const range = rangeParams.safeParse(req.query);
      if (!event.success || !range.success) {
        return reject(res, [event, range].flatMap((r): z.ZodError[] => (r.success ? [] : [r.error])));
      }

      const { tenantId } = getAuth(req);
      const p = { tenant: tenantId, event: event.data, from: range.data.from, to: range.data.to };
      // Whole UTC days, `to` inclusive: from its midnight up to the next one.
      const where = `tenant_id = {tenant:String} AND event_id = {event:String}
        AND hour >= toDateTime({from:Date}, 'UTC') AND hour < toDateTime({to:Date}, 'UTC') + INTERVAL 1 DAY`;
      const counts = 'sum(questions_asked) AS asked, sum(upvotes) AS votes, uniqMerge(active_users) AS users';

      const [totals, hours] = await Promise.all([
        rows<{ asked: number; votes: number; users: number }>(ch, `SELECT ${counts} FROM engagement_hourly WHERE ${where}`, p),
        rows<{ h: string; asked: number; votes: number; users: number }>(
          ch,
          `SELECT formatDateTime(hour, '%Y-%m-%dT%H:00:00Z', 'UTC') AS h, ${counts}
             FROM engagement_hourly WHERE ${where} GROUP BY hour ORDER BY hour`,
          p,
        ),
      ]);

      const out = (r: { asked: number; votes: number; users: number }) => ({
        questionsAsked: r.asked,
        upvotes: r.votes,
        activeUsers: r.users,
      });
      return {
        eventId: event.data,
        from: p.from,
        to: p.to,
        totals: out(totals[0]!),
        hours: hours.map((h) => ({ hour: h.h, ...out(h) })),
      };
    }),
  );

  /**
   * The tenant's events ranked by tickets sold, with the tenant's totals.
   *
   * Only events with ticket activity in the range appear: the ranking is a funnel
   * ranking, and an event with questions but no sales has nothing to rank on.
   */
  router.get(
    '/analytics/overview',
    ...guards,
    handle('overview', async (req, res) => {
      const q = overviewParams.safeParse(req.query);
      if (!q.success) return reject(res, [q.error]);

      const { tenantId } = getAuth(req);
      const p = { tenant: tenantId, from: q.data.from, to: q.data.to, limit: q.data.limit };
      const where = 'tenant_id = {tenant:String} AND day BETWEEN {from:Date} AND {to:Date}';

      const [totals, totalMoney, events] = await Promise.all([
        rows<{ purchaser_count: number; sold: number; event_count: number }>(
          ch,
          `SELECT uniqMerge(purchasers) AS purchaser_count, sum(tickets_sold) AS sold, count(DISTINCT event_id) AS event_count
             FROM ticket_funnel_daily WHERE ${where}`,
          p,
        ),
        rows<CurrencyRow>(
          ch,
          `SELECT currency, sum(gross_cents) AS cents FROM ticket_funnel_daily
            WHERE ${where} AND currency != '' GROUP BY currency ORDER BY currency`,
          p,
        ),
        rows<{ eid: string; purchaser_count: number; reserved: number; sold: number }>(
          ch,
          `SELECT event_id AS eid, uniqMerge(purchasers) AS purchaser_count, sum(seats_reserved) AS reserved, sum(tickets_sold) AS sold
             FROM ticket_funnel_daily WHERE ${where}
            GROUP BY event_id ORDER BY sold DESC, event_id LIMIT {limit:UInt32}`,
          p,
        ),
      ]);

      const ids = events.map((e) => e.eid);
      const eventMoney =
        ids.length === 0
          ? []
          : await rows<CurrencyRow & { eid: string }>(
              ch,
              `SELECT event_id AS eid, currency, sum(gross_cents) AS cents FROM ticket_funnel_daily
                WHERE ${where} AND event_id IN {events:Array(String)} AND currency != ''
                GROUP BY event_id, currency ORDER BY event_id, currency`,
              { ...p, events: ids },
            );

      const t = totals[0]!;
      return {
        from: p.from,
        to: p.to,
        totals: { events: t.event_count, purchasers: t.purchaser_count, ticketsSold: t.sold, gross: money(totalMoney) },
        events: events.map((e) => ({
          eventId: e.eid,
          purchasers: e.purchaser_count,
          seatsReserved: e.reserved,
          ticketsSold: e.sold,
          gross: money(eventMoney.filter((m) => m.eid === e.eid)),
        })),
        truncated: t.event_count > events.length,
      };
    }),
  );

  return router;
}
