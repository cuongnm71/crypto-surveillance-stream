-- Migration 002: Silver Layer Tables
-- Database: crypto

-- 1. Silver Candles 1-Minute: silver_candles_1m
-- ReplacingMergeTree(updated_at) guarantees idempotent deduplication across PySpark micro-batches,
-- backfills from S3 historical seeder, and auto-heal updates from daily reconciliation.
CREATE TABLE IF NOT EXISTS crypto.silver_candles_1m (
    exchange LowCardinality(String),
    symbol LowCardinality(String),
    timeframe LowCardinality(String) DEFAULT '1m',
    open_time DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(3)),
    close_time DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(3)),
    open Float64 CODEC(Delta, ZSTD(3)),
    high Float64 CODEC(Delta, ZSTD(3)),
    low Float64 CODEC(Delta, ZSTD(3)),
    close Float64 CODEC(Delta, ZSTD(3)),
    volume Float64 CODEC(Delta, ZSTD(3)),
    quote_volume Float64 CODEC(Delta, ZSTD(3)),
    trade_count UInt64 CODEC(Delta, ZSTD(3)),
    buy_volume Float64 CODEC(Delta, ZSTD(3)),
    sell_volume Float64 CODEC(Delta, ZSTD(3)),
    volume_delta Float64 CODEC(Delta, ZSTD(3)),
    is_closed UInt8 DEFAULT 1,
    emitted_at DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(3)),
    updated_at DateTime64(3, 'UTC') DEFAULT now64(3) CODEC(DoubleDelta, ZSTD(3)),
    v UInt8 DEFAULT 1
) ENGINE = ReplacingMergeTree(updated_at)
PARTITION BY toYYYYMM(open_time)
PRIMARY KEY (symbol, open_time)
ORDER BY (symbol, open_time);

-- 2. Silver Anomaly Surveillance Events: silver_alert_events
-- Stores stateful market manipulation detections (EWMA Z-score spikes, whale clusters, cascades).
-- Deduplicated by deterministic UUIDv5 alert_id and sorting key via ReplacingMergeTree(emitted_at).
CREATE TABLE IF NOT EXISTS crypto.silver_alert_events (
    alert_id String,
    alert_type LowCardinality(String),
    severity LowCardinality(String),
    source LowCardinality(String),
    exchange LowCardinality(String),
    symbol LowCardinality(String),
    message String,
    price Float64 CODEC(Delta, ZSTD(3)),
    return_pct Float64 CODEC(Delta, ZSTD(3)),
    zscore Float64 CODEC(Delta, ZSTD(3)),
    sigma_pct Float64 CODEC(Delta, ZSTD(3)),
    ewma_vol_ratio Float64 CODEC(Delta, ZSTD(3)),
    notional_usd Float64 CODEC(Delta, ZSTD(3)),
    event_time DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(3)),
    emitted_at DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(3)),
    logic_version LowCardinality(String) DEFAULT 'surveillance@1.0.0',
    is_simulated UInt8 DEFAULT 0,
    v UInt8 DEFAULT 1
) ENGINE = ReplacingMergeTree(emitted_at)
PARTITION BY toYYYYMM(event_time)
PRIMARY KEY (symbol, event_time, alert_id)
ORDER BY (symbol, event_time, alert_id);

-- 3. Silver Reconciliation Audit Log: silver_recon_audit_log
-- Audit trail comparing ClickHouse Silver candles against ground-truth Binance REST Klines.
-- Records volume/price drift and flags DRIFT_DETECTED when deviation exceeds 0.05% SLA threshold.
CREATE TABLE IF NOT EXISTS crypto.silver_recon_audit_log (
    recon_id String,
    symbol LowCardinality(String),
    recon_date Date,
    timeframe LowCardinality(String),
    candle_time DateTime64(3, 'UTC'),
    clickhouse_close Float64,
    binance_close Float64,
    close_drift_pct Float64,
    clickhouse_volume Float64,
    binance_volume Float64,
    volume_drift_pct Float64,
    status LowCardinality(String),
    auto_healed UInt8 DEFAULT 0,
    reconciled_at DateTime64(3, 'UTC') DEFAULT now64(3)
) ENGINE = ReplacingMergeTree(reconciled_at)
PARTITION BY toYYYYMM(recon_date)
PRIMARY KEY (symbol, recon_date, timeframe, candle_time)
ORDER BY (symbol, recon_date, timeframe, candle_time);
