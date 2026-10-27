"""Integration tests verifying end-to-end ingestion flow against live Redis container."""

from __future__ import annotations

import asyncio

import msgspec
import pytest
import redis.asyncio as aioredis

from common.constants import ALERTS_STREAM_KEY, RedisKeys
from common.contracts import AlertEvent
from ingestors.binance import BinanceIngestor

RAW_MOCK_WHALE_TRADE = (
    b'{"e":"aggTrade","E":1700000000000,"s":"BTCUSDT","a":888999,'
    b'"p":"68000.00","q":"30.00000000","f":101,"l":102,"T":1700000000000,"m":false}'
)

RAW_MOCK_TICKER = (
    b'{"e":"24hrMiniTicker","s":"ETHUSDT","c":"3550.25","h":"3600.00","l":"3400.00",'
    b'"v":"125000.0","q":"443750000.0"}'
)


class TestIngestionPipelineIntegration:
    """Integration test suite executing against real local Redis service."""

    @pytest.mark.asyncio
    async def test_live_redis_fast_path_and_conflation(self) -> None:
        """Verify Fast-Path Whale alert and 4Hz conflation writes to live Redis container."""
        redis_client = aioredis.from_url("redis://localhost:6379/0", decode_responses=False)
        await redis_client.ping()

        ingestor = BinanceIngestor(redis_client=redis_client)

        try:
            # 1. Feed Whale Trade ($68k * 30 = $2,040,000 > $1M)
            trade = await ingestor.handle_trade_event(RAW_MOCK_WHALE_TRADE)
            assert trade is not None
            assert trade.symbol == "BTCUSDT"

            # Verify alert arrived in Redis Streams
            stream_messages = await redis_client.xrevrange(ALERTS_STREAM_KEY, count=1)
            assert stream_messages is not None
            assert len(stream_messages) > 0
            _msg_id, msg_data = stream_messages[0]
            assert isinstance(msg_data, dict)
            payload_data = msg_data[b"data"]
            assert isinstance(payload_data, (bytes, bytearray))
            alert = msgspec.json.decode(payload_data, type=AlertEvent)
            assert alert.symbol == "BTCUSDT"
            assert alert.alert_type == "WHALE_TRADE"
            assert alert.severity == "CRITICAL"
            assert alert.metrics.notional_usd == 2040000.0

            # 2. Feed MiniTicker and start 4Hz Conflation
            ingestor.parse_mini_ticker(RAW_MOCK_TICKER)
            ingestor.start_conflation(interval_seconds=0.05)
            await asyncio.sleep(0.12)
            await ingestor.stop()

            # Verify Redis HSET for ETHUSDT ticker was updated
            ticker_key = RedisKeys.ticker("ETHUSDT")
            ticker_data = await redis_client.hgetall(ticker_key)
            assert b"price" in ticker_data
            assert ticker_data[b"price"] == b"3550.25"
            assert ticker_data[b"high"] == b"3600.00"

        finally:
            await redis_client.aclose()
