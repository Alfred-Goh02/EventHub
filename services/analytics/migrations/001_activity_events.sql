-- The raw, append-only record of everything that happened, from every topic.
--
-- ONE WIDE TABLE WITH A DISCRIMINATOR, not a table per topic. In a column store
-- a column a given activity never fills compresses to almost nothing and is never
-- read by a query that does not select it, so width is nearly free; a table per
-- topic would turn every cross-topic question into a UNION or a join.
--
-- ORDER BY (tenant_id, event_id, activity, occurred_at). Every read starts from
-- the tenant in a verified token, so tenant_id first makes a tenant's rows
-- physically contiguous and lets the sparse index skip every other tenant's
-- granules before anything is decompressed: the isolation choice and the
-- performance choice are one choice. The trap is the prefix rule — a filter on
-- event_id WITHOUT tenant_id prunes nothing, because event_id is only sorted
-- within a tenant. PRIMARY KEY is the shorter prefix, which keeps the in-memory
-- index small without changing the sort.
--
-- PARTITION BY month: a dashboard's 7 to 90 days skips whole months, and
-- retention is dropping a partition rather than a mutation that rewrites parts.
-- Not by tenant — every insert batch spanning N tenants would write N parts, and
-- that is how "too many parts" gets manufactured.
--
-- non_replicated_deduplication_window is what makes insert_deduplication_token
-- work on a non-replicated table. Without it the token is accepted and IGNORED,
-- which looks like it works until a retried batch is counted twice.
--
-- No Nullable columns. A Nullable costs a second file and a branch per value, and
-- here an empty string or a zero means the same as absent. LowCardinality only on
-- genuinely low-cardinality columns: it is a dictionary, and on user_id or
-- event_id the dictionary would be as large as the data.
--
-- message_id and line identify a row by its source: the outbox message id, and
-- the line within a multi-line purchase or reservation. They make a duplicate
-- visible even when Kafka delivered it at a different offset, which the dedup
-- token cannot catch.

CREATE TABLE IF NOT EXISTS activity_events
(
    tenant_id       LowCardinality(String),
    activity        LowCardinality(String),
    occurred_at     DateTime64(3, 'UTC') CODEC(Delta, ZSTD(1)),
    ingested_at     DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(Delta, ZSTD(1)),
    message_id      String,
    line            UInt16 DEFAULT 0,
    event_id        String,
    ticket_type_id  String DEFAULT '',
    user_id         String DEFAULT '',
    quantity        UInt32 DEFAULT 0,
    amount_cents    UInt64 DEFAULT 0,
    currency        LowCardinality(String) DEFAULT '',
    source_topic    LowCardinality(String),
    kafka_partition UInt16,
    kafka_offset    UInt64,
    props           Map(LowCardinality(String), String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(occurred_at)
PRIMARY KEY (tenant_id, event_id)
ORDER BY (tenant_id, event_id, activity, occurred_at)
TTL toDateTime(occurred_at) + INTERVAL 180 DAY DELETE
SETTINGS index_granularity = 8192, non_replicated_deduplication_window = 1000;

-- >>>

-- What could not be turned into a row, kept with enough to find and replay it:
-- the raw bytes, and exactly where on which topic they came from. This is the
-- thing the ClickHouse Kafka table engine cannot give — its answer to a poison
-- message is a count of how many to skip.
CREATE TABLE IF NOT EXISTS ingest_dlq
(
    seen_at         DateTime64(3, 'UTC') DEFAULT now64(3),
    source_topic    LowCardinality(String),
    kafka_partition UInt16,
    kafka_offset    UInt64,
    error           String,
    payload         String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(seen_at)
ORDER BY (source_topic, seen_at)
TTL toDateTime(seen_at) + INTERVAL 30 DAY DELETE;
