-- Migration 003: Incremental Materialized Views (Rollups from 1m to 5m, 15m, 1h, 1d)
-- Database: crypto

-- ============================================================================
-- 1. 5-Minute Timeframe Rollup
-- ============================================================================

CREATE TABLE IF NOT EXISTS crypto.silver_candles_5m (
    exchange LowCardinality(String),
    symbol LowCardinality(String),
    open_time DateTime64(3, 'UTC'),
    open AggregateFunction(argMin, Float64, DateTime64(3, 'UTC')),
    high AggregateFunction(max, Float64),
    low AggregateFunction(min, Float64),
    close AggregateFunction(argMax, Float64, DateTime64(3, 'UTC')),
    volume AggregateFunction(sum, Float64),
    quote_volume AggregateFunction(sum, Float64),
    trade_count AggregateFunction(sum, UInt64),
    buy_volume AggregateFunction(sum, Float64),
    sell_volume AggregateFunction(sum, Float64),
    volume_delta AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(open_time)
PRIMARY KEY (symbol, open_time)
ORDER BY (symbol, open_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.mv_silver_candles_1m_to_5m TO crypto.silver_candles_5m AS
SELECT
    exchange,
    symbol,
    bucket_open_time AS open_time,
    argMinState(open, src_open_time) AS open,
    maxState(high) AS high,
    minState(low) AS low,
    argMaxState(close, src_open_time) AS close,
    sumState(volume) AS volume,
    sumState(quote_volume) AS quote_volume,
    sumState(trade_count) AS trade_count,
    sumState(buy_volume) AS buy_volume,
    sumState(sell_volume) AS sell_volume,
    sumState(volume_delta) AS volume_delta
FROM (
    SELECT
        exchange,
        symbol,
        open_time AS src_open_time,
        toStartOfInterval(open_time, INTERVAL 5 MINUTE) AS bucket_open_time,
        open, high, low, close, volume, quote_volume, trade_count, buy_volume, sell_volume, volume_delta
    FROM crypto.silver_candles_1m
)
GROUP BY exchange, symbol, bucket_open_time;

-- ============================================================================
-- 2. 15-Minute Timeframe Rollup
-- ============================================================================

CREATE TABLE IF NOT EXISTS crypto.silver_candles_15m (
    exchange LowCardinality(String),
    symbol LowCardinality(String),
    open_time DateTime64(3, 'UTC'),
    open AggregateFunction(argMin, Float64, DateTime64(3, 'UTC')),
    high AggregateFunction(max, Float64),
    low AggregateFunction(min, Float64),
    close AggregateFunction(argMax, Float64, DateTime64(3, 'UTC')),
    volume AggregateFunction(sum, Float64),
    quote_volume AggregateFunction(sum, Float64),
    trade_count AggregateFunction(sum, UInt64),
    buy_volume AggregateFunction(sum, Float64),
    sell_volume AggregateFunction(sum, Float64),
    volume_delta AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(open_time)
PRIMARY KEY (symbol, open_time)
ORDER BY (symbol, open_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.mv_silver_candles_1m_to_15m TO crypto.silver_candles_15m AS
SELECT
    exchange,
    symbol,
    bucket_open_time AS open_time,
    argMinState(open, src_open_time) AS open,
    maxState(high) AS high,
    minState(low) AS low,
    argMaxState(close, src_open_time) AS close,
    sumState(volume) AS volume,
    sumState(quote_volume) AS quote_volume,
    sumState(trade_count) AS trade_count,
    sumState(buy_volume) AS buy_volume,
    sumState(sell_volume) AS sell_volume,
    sumState(volume_delta) AS volume_delta
FROM (
    SELECT
        exchange,
        symbol,
        open_time AS src_open_time,
        toStartOfInterval(open_time, INTERVAL 15 MINUTE) AS bucket_open_time,
        open, high, low, close, volume, quote_volume, trade_count, buy_volume, sell_volume, volume_delta
    FROM crypto.silver_candles_1m
)
GROUP BY exchange, symbol, bucket_open_time;

-- ============================================================================
-- 3. 1-Hour Timeframe Rollup
-- ============================================================================

CREATE TABLE IF NOT EXISTS crypto.silver_candles_1h (
    exchange LowCardinality(String),
    symbol LowCardinality(String),
    open_time DateTime64(3, 'UTC'),
    open AggregateFunction(argMin, Float64, DateTime64(3, 'UTC')),
    high AggregateFunction(max, Float64),
    low AggregateFunction(min, Float64),
    close AggregateFunction(argMax, Float64, DateTime64(3, 'UTC')),
    volume AggregateFunction(sum, Float64),
    quote_volume AggregateFunction(sum, Float64),
    trade_count AggregateFunction(sum, UInt64),
    buy_volume AggregateFunction(sum, Float64),
    sell_volume AggregateFunction(sum, Float64),
    volume_delta AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(open_time)
PRIMARY KEY (symbol, open_time)
ORDER BY (symbol, open_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.mv_silver_candles_1m_to_1h TO crypto.silver_candles_1h AS
SELECT
    exchange,
    symbol,
    bucket_open_time AS open_time,
    argMinState(open, src_open_time) AS open,
    maxState(high) AS high,
    minState(low) AS low,
    argMaxState(close, src_open_time) AS close,
    sumState(volume) AS volume,
    sumState(quote_volume) AS quote_volume,
    sumState(trade_count) AS trade_count,
    sumState(buy_volume) AS buy_volume,
    sumState(sell_volume) AS sell_volume,
    sumState(volume_delta) AS volume_delta
FROM (
    SELECT
        exchange,
        symbol,
        open_time AS src_open_time,
        toStartOfInterval(open_time, INTERVAL 1 HOUR) AS bucket_open_time,
        open, high, low, close, volume, quote_volume, trade_count, buy_volume, sell_volume, volume_delta
    FROM crypto.silver_candles_1m
)
GROUP BY exchange, symbol, bucket_open_time;

-- ============================================================================
-- 4. 1-Day Timeframe Rollup
-- ============================================================================

CREATE TABLE IF NOT EXISTS crypto.silver_candles_1d (
    exchange LowCardinality(String),
    symbol LowCardinality(String),
    open_time DateTime64(3, 'UTC'),
    open AggregateFunction(argMin, Float64, DateTime64(3, 'UTC')),
    high AggregateFunction(max, Float64),
    low AggregateFunction(min, Float64),
    close AggregateFunction(argMax, Float64, DateTime64(3, 'UTC')),
    volume AggregateFunction(sum, Float64),
    quote_volume AggregateFunction(sum, Float64),
    trade_count AggregateFunction(sum, UInt64),
    buy_volume AggregateFunction(sum, Float64),
    sell_volume AggregateFunction(sum, Float64),
    volume_delta AggregateFunction(sum, Float64)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(open_time)
PRIMARY KEY (symbol, open_time)
ORDER BY (symbol, open_time);

CREATE MATERIALIZED VIEW IF NOT EXISTS crypto.mv_silver_candles_1m_to_1d TO crypto.silver_candles_1d AS
SELECT
    exchange,
    symbol,
    bucket_open_time AS open_time,
    argMinState(open, src_open_time) AS open,
    maxState(high) AS high,
    minState(low) AS low,
    argMaxState(close, src_open_time) AS close,
    sumState(volume) AS volume,
    sumState(quote_volume) AS quote_volume,
    sumState(trade_count) AS trade_count,
    sumState(buy_volume) AS buy_volume,
    sumState(sell_volume) AS sell_volume,
    sumState(volume_delta) AS volume_delta
FROM (
    SELECT
        exchange,
        symbol,
        open_time AS src_open_time,
        toStartOfInterval(open_time, INTERVAL 1 DAY) AS bucket_open_time,
        open, high, low, close, volume, quote_volume, trade_count, buy_volume, sell_volume, volume_delta
    FROM crypto.silver_candles_1m
)
GROUP BY exchange, symbol, bucket_open_time;
