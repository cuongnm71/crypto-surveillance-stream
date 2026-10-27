"""Comprehensive unit tests for Binance WebSocket Ingestor and Dual-Path Gatekeeper."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from common.constants import KAFKA_TOPIC_DLQ, KAFKA_TOPIC_LIQUIDATIONS, KAFKA_TOPIC_TRADES
from common.contracts import TradeSide
from ingestors.binance import BinanceIngestor

SAMPLE_AGG_TRADE_JSON = (
    b'{"e":"aggTrade","E":1700000000000,"s":"BTCUSDT","a":59918237,'
    b'"p":"65000.50","q":"1.50000000","f":1001,"l":1002,"T":1700000000000,"m":true}'
)

SAMPLE_AGG_TRADE_BUY_JSON = (
    b'{"e":"aggTrade","E":1700000000000,"s":"ETHUSDT","a":89918237,'
    b'"p":"3500.00","q":"2.00000000","f":2001,"l":2002,"T":1700000000000,"m":false}'
)

SAMPLE_WHALE_TRADE_JSON = (
    b'{"e":"aggTrade","E":1700000000000,"s":"BTCUSDT","a":77718237,'
    b'"p":"68000.00","q":"25.00000000","f":3001,"l":3002,"T":1700000000000,"m":false}'
)

SAMPLE_FORCE_ORDER_JSON = (
    b'{"e":"forceOrder","E":1700000000000,"o":{"s":"SOLUSDT","S":"SELL","o":"LIMIT",'
    b'"f":"IOC","q":"150.000","p":"180.00","ap":"180.00","X":"FILLED","l":"150.000",'
    b'"z":"150.000","T":1700000000000}}'
)

SAMPLE_MINI_TICKER_JSON = (
    b'{"e":"24hrMiniTicker","s":"DOGEUSDT","c":"0.1550","h":"0.1600","l":"0.1450",'
    b'"v":"50000000.0","q":"7750000.0"}'
)


class TestBinanceIngestorParsing:
    """Test suite for zero-copy parsing and normalization."""

    def test_parse_agg_trade_normalizes_side_sell_when_buyer_is_maker(self) -> None:
        """Verify m: true is normalized to TradeSide.SELL (Aggressive Dump)."""
        ingestor = BinanceIngestor()
        trade = ingestor.parse_agg_trade(SAMPLE_AGG_TRADE_JSON)

        assert trade is not None
        assert trade.symbol == "BTCUSDT"
        assert trade.trade_id == "59918237"
        assert trade.price == 65000.50
        assert trade.qty == 1.5
        assert trade.notional_usd == 65000.50 * 1.5
        assert trade.side == TradeSide.SELL

    def test_parse_agg_trade_normalizes_side_buy_when_seller_is_maker(self) -> None:
        """Verify m: false is normalized to TradeSide.BUY (Aggressive Pump)."""
        ingestor = BinanceIngestor()
        trade = ingestor.parse_agg_trade(SAMPLE_AGG_TRADE_BUY_JSON)

        assert trade is not None
        assert trade.symbol == "ETHUSDT"
        assert trade.side == TradeSide.BUY

    def test_parse_agg_trade_deduplication(self) -> None:
        """Verify duplicate aggTrade IDs are ignored on subsequent parses."""
        ingestor = BinanceIngestor()
        first_pass = ingestor.parse_agg_trade(SAMPLE_AGG_TRADE_JSON)
        assert first_pass is not None

        second_pass = ingestor.parse_agg_trade(SAMPLE_AGG_TRADE_JSON)
        assert second_pass is None

    def test_parse_force_order_liquidation(self) -> None:
        """Verify Futures forceOrder parsing into RawLiquidationEvent."""
        ingestor = BinanceIngestor()
        liq = ingestor.parse_force_order(SAMPLE_FORCE_ORDER_JSON)

        assert liq is not None
        assert liq.symbol == "SOLUSDT"
        assert liq.side == TradeSide.SELL
        assert liq.price == 180.0
        assert liq.qty == 150.0
        assert liq.notional_usd == 180.0 * 150.0

    def test_parse_mini_ticker_updates_buffer(self) -> None:
        """Verify 24hrMiniTicker updates in-memory conflation dictionary."""
        ingestor = BinanceIngestor()
        result = ingestor.parse_mini_ticker(SAMPLE_MINI_TICKER_JSON)

        assert result is not None
        symbol, data = result
        assert symbol == "DOGEUSDT"
        assert data["price"] == "0.1550"
        assert data["high"] == "0.1600"
        assert data["low"] == "0.1450"
        assert ingestor._latest_tickers["DOGEUSDT"]["price"] == "0.1550"

    def test_parse_invalid_payload_returns_none(self) -> None:
        """Malformed JSON must safely return None without throwing uncaught exceptions."""
        ingestor = BinanceIngestor()
        assert ingestor.parse_agg_trade(b"invalid-json") is None
        assert ingestor.parse_force_order(b"{incomplete") is None
        assert ingestor.parse_mini_ticker(b"") is None


class TestBinanceIngestorRouting:
    """Test suite for Kafka publishing, Fast-Path alerting, and DLQ."""

    @pytest.mark.asyncio
    async def test_handle_trade_event_publishes_to_kafka_with_partition(self) -> None:
        """Verify valid trade is published to md.trades with custom partitioner."""
        mock_producer = MagicMock()
        mock_producer.send = AsyncMock()
        ingestor = BinanceIngestor(kafka_producer=mock_producer)

        # BTCUSDT should be pinned to partition 0
        trade = await ingestor.handle_trade_event(SAMPLE_AGG_TRADE_JSON)
        assert trade is not None

        mock_producer.send.assert_awaited_once()
        call_args = mock_producer.send.call_args[0]
        call_kwargs = mock_producer.send.call_args[1]

        assert call_args[0] == KAFKA_TOPIC_TRADES
        assert call_kwargs["partition"] == 0  # BTC pinned to partition 0
        assert call_kwargs["key"] == b"binance:BTCUSDT"

    @pytest.mark.asyncio
    async def test_handle_whale_trade_triggers_fast_path(self) -> None:
        """Verify trade >= $1M triggers Fast-Path alert to Redis and Kafka."""
        mock_producer = MagicMock()
        mock_producer.send = AsyncMock()

        mock_redis = MagicMock()
        mock_pipe = MagicMock()
        mock_pipe.xadd.return_value = mock_pipe
        mock_pipe.publish.return_value = mock_pipe
        mock_pipe.execute = AsyncMock(return_value=[True, 1, 1])
        mock_redis.pipeline.return_value = mock_pipe

        ingestor = BinanceIngestor(kafka_producer=mock_producer, redis_client=mock_redis)

        # Whale trade: 25 BTC @ $68k = $1.7M
        trade = await ingestor.handle_trade_event(SAMPLE_WHALE_TRADE_JSON)
        assert trade is not None

        # Verify Kafka received both trade and alert
        assert mock_producer.send.await_count == 2
        # Verify Redis pipeline received xadd and publish
        mock_pipe.xadd.assert_called_once()
        assert mock_pipe.publish.call_count == 2

    @pytest.mark.asyncio
    async def test_handle_liquidation_event_publishes_to_kafka(self) -> None:
        """Verify forceOrder is published to topic md.liquidations."""
        mock_producer = MagicMock()
        mock_producer.send = AsyncMock()
        ingestor = BinanceIngestor(kafka_producer=mock_producer)

        liq = await ingestor.handle_liquidation_event(SAMPLE_FORCE_ORDER_JSON)
        assert liq is not None

        mock_producer.send.assert_awaited_once()
        call_args = mock_producer.send.call_args[0]
        assert call_args[0] == KAFKA_TOPIC_LIQUIDATIONS

    @pytest.mark.asyncio
    async def test_malformed_event_routes_to_dlq(self) -> None:
        """Verify corrupted message routes to DLQ topic."""
        mock_producer = MagicMock()
        mock_producer.send = AsyncMock()
        ingestor = BinanceIngestor(kafka_producer=mock_producer)

        bad_bytes = b'{"corrupted": true}'
        result = await ingestor.handle_trade_event(bad_bytes)
        assert result is None

        mock_producer.send.assert_awaited_once()
        call_args = mock_producer.send.call_args[0]
        assert call_args[0] == KAFKA_TOPIC_DLQ

    @pytest.mark.asyncio
    async def test_conflation_loop_publishes_to_redis(self) -> None:
        """Verify 4Hz Conflation loop pipelines buffered prices to Redis."""
        mock_redis = MagicMock()
        mock_pipe = MagicMock()
        mock_pipe.hset.return_value = mock_pipe
        mock_pipe.publish.return_value = mock_pipe
        mock_pipe.execute = AsyncMock(return_value=[True, 1])
        mock_redis.pipeline.return_value = mock_pipe

        ingestor = BinanceIngestor(redis_client=mock_redis)
        ingestor.parse_mini_ticker(SAMPLE_MINI_TICKER_JSON)

        ingestor.start_conflation(interval_seconds=0.02)
        await asyncio.sleep(0.06)
        await ingestor.stop()

        assert mock_pipe.hset.called
        assert mock_pipe.publish.called
        assert mock_pipe.execute.awaited
