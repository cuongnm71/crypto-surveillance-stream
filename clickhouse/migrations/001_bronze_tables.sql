-- Migration 001: Bronze Layer Tables & Kafka Ingestion Engine
-- Database: crypto

CREATE DATABASE IF NOT EXISTS crypto;

-- 1. Main Storage: bronze_trades
-- MergeTree partitioned daily for zero-IOPS partition drops via 14-day TTL.
-- Compressed with Delta and ZSTD level 3 for high columnar density (~15 bytes/row).
CREATE TABLE IF NOT EXISTS crypto.bronze_trades (
    exchange LowCardinality(String),
    market LowCardinality(String),
    symbol LowCardinality(String),
    trade_id String,
    price Float64 CODEC(Delta, ZSTD(3)),
    qty Float64 CODEC(Delta, ZSTD(3)),
    notional_usd Float64 CODEC(Delta, ZSTD(3)),
    side LowCardinality(String),
    event_time DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(3)),
    ingested_at DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(3)),
    v UInt8 DEFAULT 1,
    is_simulated UInt8 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(event_time)
PRIMARY KEY (symbol, event_time, trade_id)
ORDER BY (symbol, event_time, trade_id)
TTL toDateTime(event_time) + INTERVAL 14 DAY;

-- 2. Buffer Queue: kafka_trades_queue
-- Native C++ Kafka engine consuming raw JSON trades directly from topic `md.trades`.
CREATE TABLE IF NOT EXISTS crypto.kafka_trades_queue (
    exchange LowCardinality(String),
    market LowCardinality(String),
    symbol LowCardinality(String),
    trade_id String,
    price Float64,
    qty Float64,
    notional_usd Float64,
    side LowCardinality(String),
    event_time Int64,
    ingested_at Int64,
    v UInt8,
    is_simulated UInt8
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'md.trades',
    kafka_group_name = 'ch_bronze_trades',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 100;

-- 3. Ingestion Pipeline: mv_kafka_trades_to_bronze
-- Automatically routes and transforms millisecond epoch times into UTC DateTime64.
CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.mv_kafka_trades_to_bronze TO crypto.bronze_trades AS
SELECT
    exchange,
    market,
    symbol,
    trade_id,
    price,
    qty,
    notional_usd,
    side,
    fromUnixTimestamp64Milli(event_time) AS event_time,
    fromUnixTimestamp64Milli(ingested_at) AS ingested_at,
    v,
    is_simulated
FROM crypto.kafka_trades_queue;

-- 4. Main Storage: bronze_liquidations
-- Forced liquidation orders partitioned monthly with 90-day retention for backtesting.
CREATE TABLE IF NOT EXISTS crypto.bronze_liquidations (
    exchange LowCardinality(String),
    market LowCardinality(String),
    symbol LowCardinality(String),
    order_id String,
    price Float64 CODEC(Delta, ZSTD(3)),
    qty Float64 CODEC(Delta, ZSTD(3)),
    notional_usd Float64 CODEC(Delta, ZSTD(3)),
    side LowCardinality(String),
    event_time DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(3)),
    ingested_at DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(3)),
    v UInt8 DEFAULT 1,
    is_simulated UInt8 DEFAULT 0
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_time)
PRIMARY KEY (symbol, event_time, order_id)
ORDER BY (symbol, event_time, order_id)
TTL toDateTime(event_time) + INTERVAL 90 DAY;

-- 5. Buffer Queue: kafka_liquidations_queue
-- Native C++ Kafka engine consuming liquidation events from topic `md.liquidations`.
CREATE TABLE IF NOT EXISTS crypto.kafka_liquidations_queue (
    exchange LowCardinality(String),
    market LowCardinality(String),
    symbol LowCardinality(String),
    order_id String,
    price Float64,
    qty Float64,
    notional_usd Float64,
    side LowCardinality(String),
    event_time Int64,
    ingested_at Int64,
    v UInt8,
    is_simulated UInt8
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'md.liquidations',
    kafka_group_name = 'ch_bronze_liquidations',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 100;

-- 6. Ingestion Pipeline: mv_kafka_liquidations_to_bronze
-- Routes parsed liquidation records directly into bronze_liquidations.
CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.mv_kafka_liquidations_to_bronze TO crypto.bronze_liquidations AS
SELECT
    exchange,
    market,
    symbol,
    order_id,
    price,
    qty,
    notional_usd,
    side,
    fromUnixTimestamp64Milli(event_time) AS event_time,
    fromUnixTimestamp64Milli(ingested_at) AS ingested_at,
    v,
    is_simulated
FROM crypto.kafka_liquidations_queue;
