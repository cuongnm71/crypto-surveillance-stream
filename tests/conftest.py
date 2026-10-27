"""Shared pytest fixtures and mock factories for test suite (ADR D6, D12, CONSTRAINTS.md)."""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest


def make_sample_trade_row(
    symbol: str = "BTCUSDT",
    trade_id: str = "12345",
    price: float = 69000.0,
    qty: float = 1.5,
    side: str = "buy",
    event_time: Any = None,
    is_simulated: int = 0,
) -> tuple[Any, ...]:
    """Generate a single trade row tuple matching ClickHouse bronze_trades schema."""
    return (symbol, trade_id, price, qty, side, event_time, is_simulated)


def make_sample_alert_dict(
    alert_id: str = "test-uuid",
    symbol: str = "BTCUSDT",
    alert_type: str = "WHALE_TRADE",
    severity: str = "CRITICAL",
    price: float = 69000.0,
    message: str = "Whale alert test",
) -> dict[str, Any]:
    """Generate a dictionary payload for real-time market alerts."""
    return {
        "alert_id": alert_id,
        "symbol": symbol,
        "alert_type": alert_type,
        "severity": severity,
        "price": price,
        "message": message,
    }


def make_sample_stream_entry(
    entry_id: bytes = b"1700000000000-0",
    alert_dict: dict[str, Any] | None = None,
) -> tuple[bytes, dict[bytes, bytes]]:
    """Generate a Redis Stream (xrevrange/xread) entry with serialized JSON alert."""
    payload = alert_dict or make_sample_alert_dict()
    return (entry_id, {b"data": json.dumps(payload).encode()})


def create_mock_clickhouse(
    bronze_trades_count: int = 640,
    active_parts: int = 100,
    sample_trades: list[tuple[Any, ...]] | None = None,
) -> MagicMock:
    """Build a mocked ClickHouse client with realistic schema responses."""
    trades = sample_trades or [make_sample_trade_row()]
    mock_ch = MagicMock()

    def _query_handler(sql: str, parameters: dict[str, Any] | None = None) -> MagicMock:
        rows: list[tuple[Any, ...]]
        if "parts" in sql:
            rows = [(active_parts, 1024, 4096)]
        elif "bronze_trades" in sql and "SELECT symbol" in sql:
            rows = trades
        else:
            rows = [(bronze_trades_count,)]
        return MagicMock(
            result_rows=rows,
            column_names=[
                "symbol",
                "trade_id",
                "price",
                "qty",
                "side",
                "event_time",
                "is_simulated",
            ],
        )

    mock_ch.query.side_effect = _query_handler
    return mock_ch


def create_mock_redis(
    used_memory_human: str = "2.5M",
    used_memory_bytes: int = 2621440,
    connected_clients: int = 3,
    dbsize: int = 42,
    stream_entries: list[tuple[bytes, dict[bytes, bytes]]] | None = None,
) -> AsyncMock:
    """Build a mocked Redis client with PubSub and Streams support."""
    entries = stream_entries or [make_sample_stream_entry()]
    mock_redis = AsyncMock()

    mock_redis.info.side_effect = lambda section: {
        "memory": {"used_memory_human": used_memory_human, "used_memory": used_memory_bytes},
        "clients": {"connected_clients": connected_clients},
    }.get(section, {})

    mock_redis.dbsize.return_value = dbsize
    mock_redis.xrevrange.return_value = entries

    mock_pubsub = AsyncMock()
    mock_redis.pubsub = MagicMock(return_value=mock_pubsub)
    return mock_redis


@pytest.fixture
def mock_ch_client() -> MagicMock:
    """Fixture providing a standard mocked ClickHouse client."""
    return create_mock_clickhouse()


@pytest.fixture
def mock_redis_client() -> AsyncMock:
    """Fixture providing a standard mocked Redis client."""
    return create_mock_redis()
