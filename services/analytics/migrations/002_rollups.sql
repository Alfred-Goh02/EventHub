-- The rollups dashboards read, each fed by one materialized view.
--
-- ONLY WHAT SOMETHING PRODUCES. The Field Guide's funnel starts at "event
-- viewed" and "checkout started", and its attendance family counts check-ins.
-- Nothing in this repository emits a view, a checkout start or a check-in, and
-- community.feed has no producer at all. A column no message can ever fill would
-- read as zero, and a zero on a dashboard is a claim. So the funnel here begins
-- at the reservation, and attendance is absent until check-ins exist.
--
-- A MATERIALIZED VIEW IS AN INSERT TRIGGER, not a cached query. Each view runs
-- its SELECT over each block inserted into activity_events and writes the result
-- to its target. Three consequences to design around: it never sees rows that
-- existed before it was created (backfill is an explicit INSERT ... SELECT, never
-- POPULATE, which drops rows arriving while it runs); it never sees merges; and a
-- duplicate insert is aggregated in permanently — which is why deduplication has
-- to happen at insert, with a token that propagates to these views.
--
-- The TO form, always, so each target is a real table that can be truncated,
-- backfilled and inspected on its own.
--
-- uniq STATES for anything that counts people: distinct counts do not add up
-- across days, but HyperLogLog sketches merge, so uniqMerge over 90 daily rows is
-- still a correct 90-day distinct. SimpleAggregateFunction(sum) for seats and
-- money, because a sum of sums is a sum and a plain value is cheaper than a state.
--
-- Every read of these tables must GROUP BY the sort key and apply uniqMerge and
-- sum. Merges are eventual; until one happens a key legitimately has several rows.
--
-- THE DEDUP TOKEN DOES NOT REACH THESE TABLES ON ITS OWN. The Field Guide says an
-- insert_deduplication_token "propagates to the materialized views". Measured on
-- ClickHouse 25.8 as analytics_app: the same two-row block inserted twice with
-- one token left the raw table at 2 rows — deduplicated — and this rollup at
-- tickets_sold 6 and gross_cents 27000, exactly double. The duplicate block was
-- dropped from activity_events and aggregated into the rollup anyway.
--
-- It takes two things together: each target table carries its own
-- non_replicated_deduplication_window (below), and the insert sets
-- deduplicate_blocks_in_dependent_materialized_views = 1. Without both, a retried
-- batch is silently double-counted — and the distinct counts hide it, because
-- uniqMerge of the same user twice is still one user. Only the sums show it.

CREATE TABLE IF NOT EXISTS ticket_funnel_daily
(
    tenant_id        LowCardinality(String),
    event_id         String,
    day              Date,
    ticket_type_id   String,
    -- Money is summed per currency, never across. Reservations carry no
    -- currency, so their rows sit under '' and are merged across currencies by
    -- any query that does not group by it.
    currency         LowCardinality(String),
    reservers        AggregateFunction(uniq, String),
    purchasers       AggregateFunction(uniq, String),
    seats_reserved   SimpleAggregateFunction(sum, UInt64),
    seats_released   SimpleAggregateFunction(sum, UInt64),
    seats_expired    SimpleAggregateFunction(sum, UInt64),
    tickets_sold     SimpleAggregateFunction(sum, UInt64),
    gross_cents      SimpleAggregateFunction(sum, UInt64)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(day)
ORDER BY (tenant_id, event_id, day, ticket_type_id, currency)
SETTINGS non_replicated_deduplication_window = 1000;

-- >>>

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_ticket_funnel_daily
TO ticket_funnel_daily AS
SELECT
    tenant_id,
    event_id,
    toDate(occurred_at) AS day,
    ticket_type_id,
    currency,
    uniqStateIf(user_id, activity = 'ticket_reserved') AS reservers,
    uniqStateIf(user_id, activity = 'ticket_purchased') AS purchasers,
    toUInt64(sumIf(quantity, activity = 'ticket_reserved')) AS seats_reserved,
    toUInt64(sumIf(quantity, activity = 'reservation_released')) AS seats_released,
    toUInt64(sumIf(quantity, activity = 'reservation_expired')) AS seats_expired,
    toUInt64(sumIf(quantity, activity = 'ticket_purchased')) AS tickets_sold,
    toUInt64(sumIf(amount_cents, activity = 'ticket_purchased')) AS gross_cents
FROM activity_events
WHERE activity IN ('ticket_reserved', 'reservation_released', 'reservation_expired', 'ticket_purchased')
GROUP BY tenant_id, event_id, day, ticket_type_id, currency;

-- >>>

-- Hourly, not daily: engagement is bursty around a live session, and a daily
-- bucket hides the shape of it. Grain is a product decision.
CREATE TABLE IF NOT EXISTS engagement_hourly
(
    tenant_id        LowCardinality(String),
    event_id         String,
    hour             DateTime('UTC'),
    questions_asked  SimpleAggregateFunction(sum, UInt64),
    upvotes          SimpleAggregateFunction(sum, UInt64),
    active_users     AggregateFunction(uniq, String)
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(hour)
ORDER BY (tenant_id, event_id, hour)
SETTINGS non_replicated_deduplication_window = 1000;

-- >>>

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_engagement_hourly
TO engagement_hourly AS
SELECT
    tenant_id,
    event_id,
    toStartOfHour(occurred_at) AS hour,
    toUInt64(countIf(activity = 'question_asked')) AS questions_asked,
    toUInt64(countIf(activity = 'question_upvoted')) AS upvotes,
    uniqState(user_id) AS active_users
FROM activity_events
WHERE activity IN ('question_asked', 'question_upvoted')
GROUP BY tenant_id, event_id, hour;
