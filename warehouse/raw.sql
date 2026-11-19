-- Landing layer: Redpanda (Kafka API) -> ClickHouse Kafka engine -> materialized view -> raw MergeTree.
-- Raw keeps every delivery as it arrived, duplicates and all, stamped with the Kafka offset and ingest time.

CREATE DATABASE IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.identification_events
(
    request_id      String,
    visitor_id      Nullable(String),
    subscription_id String,
    event_time      DateTime64(3, 'UTC'),
    ip              String,
    country         String,
    browser         String,
    os              String,
    incognito       UInt8,
    confidence      Float32,
    bot_result      String,
    vpn             UInt8,
    linked_id       String,
    url             String,
    sdk_version     String,
    _kafka_partition UInt16,
    _kafka_offset   UInt64,
    _ingested_at    DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
PARTITION BY toDate(_ingested_at)
ORDER BY (_ingested_at, request_id)
TTL toDateTime(_ingested_at) + INTERVAL 30 DAY;

CREATE TABLE IF NOT EXISTS raw.identification_events_queue
(
    request_id      String,
    visitor_id      Nullable(String),
    subscription_id String,
    event_time      DateTime64(3, 'UTC'),
    ip              String,
    country         String,
    browser         String,
    os              String,
    incognito       UInt8,
    confidence      Float32,
    bot_result      String,
    vpn             UInt8,
    linked_id       String,
    url             String,
    sdk_version     String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'redpanda:9092',
         kafka_topic_list = 'identification_events',
         kafka_group_name = 'clickhouse_raw_loader',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 1,
         kafka_max_block_size = 65536,
         kafka_flush_interval_ms = 1000,
         -- A malformed message must not stall the consumer: route it to a dead letter table instead.
         kafka_handle_error_mode = 'stream';

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.identification_events_mv
TO raw.identification_events AS
SELECT
    request_id, visitor_id, subscription_id, event_time, ip, country, browser, os,
    incognito, confidence, bot_result, vpn, linked_id, url, sdk_version,
    _partition AS _kafka_partition,
    _offset    AS _kafka_offset,
    now64(3)   AS _ingested_at
FROM raw.identification_events_queue
WHERE length(_error) = 0;

CREATE TABLE IF NOT EXISTS raw.dead_letter
(
    raw_message      String,
    error            String,
    _kafka_partition UInt16,
    _kafka_offset    UInt64,
    _ingested_at     DateTime64(3, 'UTC')
)
ENGINE = MergeTree
ORDER BY _ingested_at;

CREATE MATERIALIZED VIEW IF NOT EXISTS raw.dead_letter_mv
TO raw.dead_letter AS
SELECT _raw_message AS raw_message, _error AS error,
       _partition AS _kafka_partition, _offset AS _kafka_offset, now64(3) AS _ingested_at
FROM raw.identification_events_queue
WHERE length(_error) > 0;
