"""Unit tests for Fast-Path sub-second alert detection and dispatch."""

from __future__ import annotations

import time
from unittest.mock import AsyncMock, MagicMock

import pytest

from common.contracts import RawLiquidationEvent, RawTradeEvent, TradeSide
from ingestors.fast_alerts import (
    WHALE_TRADE_THRESHOLD_USD,
    dispatch_fast_path_alert,
    evaluate_liquidation_alert,
    evaluate_trade_for_whale_alert,
)


class TestFastAlertEvaluator:
    """Test suite for Fast-Path alert evaluation logic."""

    def test_trade_below_1m_returns_none(self) -> None:
        """Trades below $1,000,000 USD must not trigger Fast-Path alert."""
        trade = RawTradeEvent(
            exchange="binance",
            market="spot",
            symbol="BTCUSDT",
            trade_id="1001",
            price=65000.0,
            qty=10.0,
            notional_usd=650000.0,
            side=TradeSide.BUY,
            event_time=1700000000000,
            ingested_at=1700000000005,
        )
        assert evaluate_trade_for_whale_alert(trade) is None

    def test_trade_at_or_above_1m_triggers_whale_alert(self) -> None:
        """Trades >= $1,000,000 USD must immediately generate CRITICAL Whale alert."""
        trade = RawTradeEvent(
            exchange="binance",
            market="spot",
            symbol="BTCUSDT",
            trade_id="1002",
            price=68000.0,
            qty=20.0,
            notional_usd=1360000.0,
            side=TradeSide.BUY,
            event_time=1700000000000,
            ingested_at=1700000000005,
        )
        alert = evaluate_trade_for_whale_alert(trade)
        assert alert is not None
        assert alert.symbol == "BTCUSDT"
        assert alert.alert_type == "WHALE_TRADE"
        assert alert.severity == "CRITICAL"
        assert alert.metrics.notional_usd == 1360000.0
        assert "FAST-PATH" in alert.message

    def test_liquidation_below_500k_returns_none(self) -> None:
        """Liquidations below $500,000 USD must not trigger Fast-Path alert."""
        liq = RawLiquidationEvent(
            exchange="binance",
            market="futures",
            symbol="ETHUSDT",
            order_id="9001",
            price=3000.0,
            qty=100.0,
            notional_usd=300000.0,
            side=TradeSide.SELL,
            event_time=1700000000000,
            ingested_at=1700000000005,
        )
        assert evaluate_liquidation_alert(liq) is None

    def test_liquidation_at_or_above_500k_triggers_alert(self) -> None:
        """Liquidations >= $500,000 USD must trigger extreme liquidation alert."""
        liq = RawLiquidationEvent(
            exchange="binance",
            market="futures",
            symbol="ETHUSDT",
            order_id="9002",
            price=3500.0,
            qty=200.0,
            notional_usd=700000.0,
            side=TradeSide.SELL,
            event_time=1700000000000,
            ingested_at=1700000000005,
        )
        alert = evaluate_liquidation_alert(liq)
        assert alert is not None
        assert alert.symbol == "ETHUSDT"
        assert alert.alert_type == "LIQUIDATION_CASCADE"
        assert alert.severity == "CRITICAL"
        assert alert.metrics.notional_usd == 700000.0

    def test_in_memory_evaluation_latency_under_1ms(self) -> None:
        """Verify rule-based evaluation completes in sub-millisecond in-memory time."""
        trade = RawTradeEvent(
            exchange="binance",
            market="spot",
            symbol="BTCUSDT",
            trade_id="9999",
            price=70000.0,
            qty=25.0,
            notional_usd=1750000.0,
            side=TradeSide.BUY,
            event_time=1700000000000,
            ingested_at=1700000000005,
        )
        start = time.perf_counter()
        for _ in range(1000):
            evaluate_trade_for_whale_alert(trade)
        elapsed = (time.perf_counter() - start) / 1000.0
        assert elapsed < 0.001, f"Evaluation took {elapsed * 1000:.4f}ms, expected < 1ms"


class TestFastAlertDispatch:
    """Test suite for dispatch_fast_path_alert."""

    @pytest.mark.asyncio
    async def test_dispatch_pipes_to_redis_and_kafka(self) -> None:
        """Verify alert is serialized and piped to both Redis and Kafka."""
        trade = RawTradeEvent(
            exchange="binance",
            market="spot",
            symbol="BTCUSDT",
            trade_id="5555",
            price=WHALE_TRADE_THRESHOLD_USD,
            qty=1.0,
            notional_usd=WHALE_TRADE_THRESHOLD_USD,
            side=TradeSide.SELL,
            event_time=1700000000000,
            ingested_at=1700000000005,
        )
        alert = evaluate_trade_for_whale_alert(trade)
        assert alert is not None

        mock_redis = MagicMock()
        mock_pipe = MagicMock()
        mock_pipe.xadd.return_value = mock_pipe
        mock_pipe.publish.return_value = mock_pipe
        mock_pipe.execute = AsyncMock(return_value=[True, 1, 1])
        mock_redis.pipeline.return_value = mock_pipe

        mock_producer = MagicMock()
        mock_producer.send = AsyncMock()

        await dispatch_fast_path_alert(alert, redis_client=mock_redis, kafka_producer=mock_producer)

        mock_pipe.xadd.assert_called_once()
        assert mock_pipe.publish.call_count == 2
        mock_pipe.execute.assert_awaited_once()
        mock_producer.send.assert_awaited_once()
