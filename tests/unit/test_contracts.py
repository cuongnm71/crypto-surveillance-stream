"""Unit tests for data contracts, symbol catalog constants, Redis keys, and deterministic UUIDv5."""

from __future__ import annotations

import uuid

import msgspec
import pytest

from common.constants import (
    ALERT_COOLDOWN_KEY_TEMPLATE,
    ALERTS_STREAM_KEY,
    CANDLES_1M_KEY_TEMPLATE,
    INGEST_CONN_BINANCE_KEY,
    KAFKA_TOPIC_ALERTS,
    KAFKA_TOPIC_CANDLES_1M,
    KAFKA_TOPIC_DLQ,
    KAFKA_TOPIC_LIQUIDATIONS,
    KAFKA_TOPIC_TICKERS,
    KAFKA_TOPIC_TRADES,
    PRICE_PUBSUB_KEY_TEMPLATE,
    SYMBOL_CATALOG,
    TELEMETRY_METRICS_KEY,
    TICKER_KEY_TEMPLATE,
    TIER_1_SYMBOLS,
    TIER_2_SYMBOLS,
    TIER_3_SYMBOLS,
    TOP_100_SYMBOLS,
    LiquidityTier,
    MarketCategory,
    RedisKeys,
    get_symbol_metadata,
    normalize_symbol,
)
from common.contracts import (
    AlertEvent,
    AlertMetrics,
    AlertSeverity,
    AlertType,
    CandleEvent,
    DLQEvent,
    RawLiquidationEvent,
    RawTradeEvent,
    TradeSide,
    decode_alert_event,
    decode_candle_event,
    decode_liquidation_event,
    decode_trade_event,
    encode_event,
    infer_taker_side,
)
from common.crypto import generate_deterministic_alert_id


class TestConstantsAndCatalog:
    """Verify the 100-symbol catalog, liquidity tiers, and Redis/Kafka constants."""

    def test_top_100_symbols_exact_count_and_uniqueness(self) -> None:
        assert len(TOP_100_SYMBOLS) == 100
        assert len(set(TOP_100_SYMBOLS)) == 100
        for sym in TOP_100_SYMBOLS:
            assert sym == sym.upper()
            assert sym.endswith("USDT")

    def test_tier_partition_covers_all_100_symbols(self) -> None:
        assert len(TIER_1_SYMBOLS) == 4
        assert set(TIER_1_SYMBOLS) == {"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"}
        assert len(TIER_2_SYMBOLS) == 26
        assert len(TIER_3_SYMBOLS) == 70
        combined = set(TIER_1_SYMBOLS) | set(TIER_2_SYMBOLS) | set(TIER_3_SYMBOLS)
        assert len(combined) == 100
        assert combined == set(TOP_100_SYMBOLS)

    def test_symbol_catalog_thresholds_match_adr_d7(self) -> None:
        assert len(SYMBOL_CATALOG) == 100

        btc_meta = get_symbol_metadata("BTCUSDT")
        assert btc_meta.liquidity_tier == LiquidityTier.TIER_1
        assert btc_meta.category == MarketCategory.LAYER_1
        assert btc_meta.single_whale_threshold_usd == 500_000.0
        assert btc_meta.cluster_whale_threshold_usd == 200_000.0

        link_meta = get_symbol_metadata("LINKUSDT")
        assert link_meta.liquidity_tier == LiquidityTier.TIER_2
        assert link_meta.single_whale_threshold_usd == 200_000.0
        assert link_meta.cluster_whale_threshold_usd == 80_000.0

        pepe_meta = get_symbol_metadata("PEPEUSDT")
        assert pepe_meta.liquidity_tier == LiquidityTier.TIER_3
        assert pepe_meta.single_whale_threshold_usd == 75_000.0
        assert pepe_meta.cluster_whale_threshold_usd == 30_000.0

    def test_normalize_futures_1000_prefix_symbols(self) -> None:
        assert normalize_symbol("1000PEPEUSDT") == "PEPEUSDT"
        assert normalize_symbol("1000SHIBUSDT") == "SHIBUSDT"
        assert normalize_symbol("1000BONKUSDT") == "BONKUSDT"
        assert normalize_symbol("1000FLOKIUSDT") == "FLOKIUSDT"
        assert normalize_symbol("btcusdt") == "BTCUSDT"

    def test_redis_key_templates_and_builders(self) -> None:
        assert TICKER_KEY_TEMPLATE == "crypto:ticker:{symbol}"
        assert PRICE_PUBSUB_KEY_TEMPLATE == "crypto:px:{symbol}"
        assert CANDLES_1M_KEY_TEMPLATE == "crypto:candles:{symbol}:1m"
        assert ALERTS_STREAM_KEY == "crypto:alerts:stream"
        assert TELEMETRY_METRICS_KEY == "crypto:telemetry:metrics"
        assert INGEST_CONN_BINANCE_KEY == "crypto:ingest:conn:binance"
        assert ALERT_COOLDOWN_KEY_TEMPLATE == "crypto:alert:cooldown:{symbol}"

        assert RedisKeys.ticker("BTCUSDT") == "crypto:ticker:BTCUSDT"
        assert RedisKeys.price_pubsub("ETHUSDT") == "crypto:px:ETHUSDT"
        assert RedisKeys.candles_1m("SOLUSDT") == "crypto:candles:SOLUSDT:1m"
        assert RedisKeys.alerts_stream() == "crypto:alerts:stream"
        assert RedisKeys.alert_cooldown("BTCUSDT") == "crypto:alert:cooldown:BTCUSDT"

    def test_kafka_topic_constants(self) -> None:
        assert KAFKA_TOPIC_TRADES == "md.trades"
        assert KAFKA_TOPIC_TICKERS == "md.tickers"
        assert KAFKA_TOPIC_LIQUIDATIONS == "md.liquidations"
        assert KAFKA_TOPIC_CANDLES_1M == "candles.1m"
        assert KAFKA_TOPIC_ALERTS == "alerts.events"
        assert KAFKA_TOPIC_DLQ == "dlq"


class TestDeterministicUUIDv5:
    """Verify deterministic UUIDv5 generation for alert idempotency."""

    def test_deterministic_alert_id_is_valid_uuidv5(self) -> None:
        alert_id = generate_deterministic_alert_id(
            symbol="BTCUSDT",
            alert_type="ZSCORE_SPIKE",
            window_start=1773496620000,
        )
        parsed = uuid.UUID(alert_id)
        assert parsed.version == 5

    def test_deterministic_alert_id_idempotency_across_restarts(self) -> None:
        id_first = generate_deterministic_alert_id("BTCUSDT", "WHALE_TRADE", 1773496620000)
        id_second = generate_deterministic_alert_id("btcusdt", "whale_trade", "1773496620000")
        assert id_first == id_second

    def test_deterministic_alert_id_uniqueness_across_inputs(self) -> None:
        base_id = generate_deterministic_alert_id("BTCUSDT", "ZSCORE_SPIKE", 1773496620000)
        diff_symbol = generate_deterministic_alert_id("ETHUSDT", "ZSCORE_SPIKE", 1773496620000)
        diff_type = generate_deterministic_alert_id("BTCUSDT", "VOLUME_ANOMALY", 1773496620000)
        diff_window = generate_deterministic_alert_id("BTCUSDT", "ZSCORE_SPIKE", 1773496680000)

        assert len({base_id, diff_symbol, diff_type, diff_window}) == 4


class TestDataContracts:
    """Verify msgspec.Struct models and zero-copy serialization."""

    def test_structs_inherit_from_msgspec_struct(self) -> None:
        assert issubclass(RawTradeEvent, msgspec.Struct)
        assert issubclass(RawLiquidationEvent, msgspec.Struct)
        assert issubclass(CandleEvent, msgspec.Struct)
        assert issubclass(AlertEvent, msgspec.Struct)
        assert issubclass(DLQEvent, msgspec.Struct)

    def test_infer_taker_side_from_buyer_maker_flag(self) -> None:
        # When buyer is maker (m=True), the aggressive taker is the seller
        assert infer_taker_side(is_buyer_maker=True) == TradeSide.SELL
        # When seller is maker (m=False), the aggressive taker is the buyer
        assert infer_taker_side(is_buyer_maker=False) == TradeSide.BUY

    def test_raw_trade_event_roundtrip_and_validation(self) -> None:
        trade = RawTradeEvent(
            exchange="binance",
            market="spot",
            symbol="BTCUSDT",
            trade_id="982341234",
            price=64250.50,
            qty=0.042,
            notional_usd=2698.521,
            side=TradeSide.BUY,
            event_time=1773496655123,
            ingested_at=1773496655130,
        )
        assert trade.v == 1
        assert trade.is_simulated == 0
        trade.validate(reference_time_ms=1773496655130)

        payload = encode_event(trade)
        decoded = decode_trade_event(payload)
        assert decoded == trade

    def test_raw_trade_event_validation_rejects_invalid_values(self) -> None:
        bad_price = RawTradeEvent(
            exchange="binance",
            market="spot",
            symbol="BTCUSDT",
            trade_id="1",
            price=-10.0,
            qty=1.0,
            notional_usd=-10.0,
            side=TradeSide.BUY,
            event_time=1773496655123,
            ingested_at=1773496655130,
        )
        with pytest.raises(ValueError, match="price"):
            bad_price.validate(reference_time_ms=1773496655130)

        stale_event = RawTradeEvent(
            exchange="binance",
            market="spot",
            symbol="BTCUSDT",
            trade_id="2",
            price=64000.0,
            qty=1.0,
            notional_usd=64000.0,
            side=TradeSide.BUY,
            event_time=1773496000000,
            ingested_at=1773496655130,
        )
        with pytest.raises(ValueError, match="event_time"):
            stale_event.validate(reference_time_ms=1773496655130)

    def test_raw_liquidation_event_roundtrip(self) -> None:
        liq = RawLiquidationEvent(
            exchange="binance",
            market="futures",
            symbol="ETHUSDT",
            order_id="liq-1001",
            price=3200.0,
            qty=200.0,
            notional_usd=640_000.0,
            side=TradeSide.SELL,
            event_time=1773496655000,
            ingested_at=1773496655010,
        )
        raw_bytes = encode_event(liq)
        decoded = decode_liquidation_event(raw_bytes)
        assert decoded == liq
        assert decoded.notional_usd == 640_000.0

    def test_candle_event_roundtrip_and_cvd(self) -> None:
        candle = CandleEvent(
            exchange="binance",
            symbol="SOLUSDT",
            timeframe="1m",
            open_time=1773496620000,
            close_time=1773496679999,
            open=150.0,
            high=152.5,
            low=149.8,
            close=152.0,
            volume=12500.0,
            quote_volume=1_887_500.0,
            trade_count=4320,
            buy_volume=7500.0,
            sell_volume=5000.0,
            volume_delta=2500.0,
            is_closed=True,
            emitted_at=1773496682000,
        )
        raw_bytes = encode_event(candle)
        decoded = decode_candle_event(raw_bytes)
        assert decoded == candle
        assert decoded.volume_delta == pytest.approx(decoded.buy_volume - decoded.sell_volume)

    def test_alert_event_matches_spec_contract(self) -> None:
        alert_id = generate_deterministic_alert_id("BTCUSDT", AlertType.ZSCORE_SPIKE, 1773496620000)
        alert = AlertEvent(
            alert_id=alert_id,
            alert_type=AlertType.ZSCORE_SPIKE,
            severity=AlertSeverity.CRITICAL,
            source="spark.surveillance_alerts",
            exchange="binance",
            symbol="BTCUSDT",
            message="BTCUSDT surged +1.9% in 1m (z=4.6σ vs EWMA)",
            price=64250.50,
            metrics=AlertMetrics(
                return_pct=1.9,
                zscore=4.6,
                sigma_pct=0.41,
                ewma_vol_ratio=3.8,
                notional_usd=1_450_000.0,
            ),
            event_time=1773496679999,
            emitted_at=1773496682120,
        )
        raw_bytes = encode_event(alert)
        decoded = decode_alert_event(raw_bytes)
        assert decoded == alert
        assert decoded.logic_version == "surveillance@1.0.0"
        assert decoded.metrics.zscore == 4.6
