"""REST API endpoints for pipeline telemetry, live trades, and surveillance alerts."""

from __future__ import annotations

import datetime
import json
import logging
import re
import time
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from common.constants import ALERTS_STREAM_KEY, SYMBOL_CATALOG

logger = logging.getLogger("backend.routes.api")
router = APIRouter(prefix="/api/v1", tags=["Surveillance Analytics"])

_SYMBOL_REGEX = re.compile(r"^[A-Z0-9_]{2,20}$")
_ALLOWED_TIMEFRAMES: dict[str, str] = {
    "1m": "1 MINUTE",
    "5m": "5 MINUTE",
    "15m": "15 MINUTE",
    "1h": "1 HOUR",
    "1d": "1 DAY",
}
_TF_SQL_MAP: dict[str, str] = {
    "1m": (
        "SELECT open_time, open, high, low, close, volume "
        "FROM crypto.silver_candles_1m "
        "WHERE symbol = %(symbol)s "
        "ORDER BY open_time ASC LIMIT %(limit)s"
    ),
    "5m": (
        "SELECT open_time, argMinMerge(open) AS open, maxMerge(high) AS high, "
        "minMerge(low) AS low, argMaxMerge(close) AS close, sumMerge(volume) AS volume "
        "FROM crypto.silver_candles_5m WHERE symbol = %(symbol)s "
        "GROUP BY open_time ORDER BY open_time ASC LIMIT %(limit)s"
    ),
    "15m": (
        "SELECT open_time, argMinMerge(open) AS open, maxMerge(high) AS high, "
        "minMerge(low) AS low, argMaxMerge(close) AS close, sumMerge(volume) AS volume "
        "FROM crypto.silver_candles_15m WHERE symbol = %(symbol)s "
        "GROUP BY open_time ORDER BY open_time ASC LIMIT %(limit)s"
    ),
    "1h": (
        "SELECT open_time, argMinMerge(open) AS open, maxMerge(high) AS high, "
        "minMerge(low) AS low, argMaxMerge(close) AS close, sumMerge(volume) AS volume "
        "FROM crypto.silver_candles_1h WHERE symbol = %(symbol)s "
        "GROUP BY open_time ORDER BY open_time ASC LIMIT %(limit)s"
    ),
    "1d": (
        "SELECT open_time, argMinMerge(open) AS open, maxMerge(high) AS high, "
        "minMerge(low) AS low, argMaxMerge(close) AS close, sumMerge(volume) AS volume "
        "FROM crypto.silver_candles_1d WHERE symbol = %(symbol)s "
        "GROUP BY open_time ORDER BY open_time ASC LIMIT %(limit)s"
    ),
}
_BRONZE_FALLBACK_SQL_MAP: dict[str, str] = {
    "1m": (
        "SELECT toStartOfInterval(event_time, INTERVAL 1 MINUTE) AS bucket_time, "
        "argMin(price, event_time) AS open, max(price) AS high, min(price) AS low, "
        "argMax(price, event_time) AS close, sum(qty) AS volume FROM crypto.bronze_trades "
        "WHERE symbol = %(symbol)s GROUP BY bucket_time ORDER BY bucket_time ASC LIMIT %(limit)s"
    ),
    "5m": (
        "SELECT toStartOfInterval(event_time, INTERVAL 5 MINUTE) AS bucket_time, "
        "argMin(price, event_time) AS open, max(price) AS high, min(price) AS low, "
        "argMax(price, event_time) AS close, sum(qty) AS volume FROM crypto.bronze_trades "
        "WHERE symbol = %(symbol)s GROUP BY bucket_time ORDER BY bucket_time ASC LIMIT %(limit)s"
    ),
    "15m": (
        "SELECT toStartOfInterval(event_time, INTERVAL 15 MINUTE) AS bucket_time, "
        "argMin(price, event_time) AS open, max(price) AS high, min(price) AS low, "
        "argMax(price, event_time) AS close, sum(qty) AS volume FROM crypto.bronze_trades "
        "WHERE symbol = %(symbol)s GROUP BY bucket_time ORDER BY bucket_time ASC LIMIT %(limit)s"
    ),
    "1h": (
        "SELECT toStartOfInterval(event_time, INTERVAL 1 HOUR) AS bucket_time, "
        "argMin(price, event_time) AS open, max(price) AS high, min(price) AS low, "
        "argMax(price, event_time) AS close, sum(qty) AS volume FROM crypto.bronze_trades "
        "WHERE symbol = %(symbol)s GROUP BY bucket_time ORDER BY bucket_time ASC LIMIT %(limit)s"
    ),
    "1d": (
        "SELECT toStartOfInterval(event_time, INTERVAL 1 DAY) AS bucket_time, "
        "argMin(price, event_time) AS open, max(price) AS high, min(price) AS low, "
        "argMax(price, event_time) AS close, sum(qty) AS volume FROM crypto.bronze_trades "
        "WHERE symbol = %(symbol)s GROUP BY bucket_time ORDER BY bucket_time ASC LIMIT %(limit)s"
    ),
}


def _sanitize_symbol(symbol: str | None) -> str | None:
    """Validate and sanitize symbol query parameter."""
    if symbol is None:
        return None
    cleaned = symbol.strip().upper()
    if not _SYMBOL_REGEX.match(cleaned):
        raise HTTPException(status_code=400, detail=f"Invalid symbol format: {symbol}")
    return cleaned


@router.get("/telemetry")
async def get_telemetry(request: Request) -> dict[str, Any]:
    """Return real-time health telemetry across ClickHouse, Redis, and WebSocket hub."""
    now_ms = int(time.time() * 1000)
    ch_metrics: dict[str, Any] = {"status": "unavailable"}
    redis_metrics: dict[str, Any] = {"status": "unavailable"}

    # 1. ClickHouse OLAP Metrics
    ch_client = getattr(request.app.state, "ch_client", None)
    if ch_client is not None:
        try:
            trades_cnt_res = ch_client.query("SELECT count() FROM crypto.bronze_trades")
            trades_count = trades_cnt_res.result_rows[0][0] if trades_cnt_res.result_rows else 0

            parts_res = ch_client.query(
                "SELECT count(), sum(bytes_on_disk), sum(data_uncompressed_bytes) "
                "FROM system.parts WHERE database = 'crypto' AND active = 1"
            )
            parts_row = parts_res.result_rows[0] if parts_res.result_rows else (0, 0, 0)
            active_parts = parts_row[0] or 0
            disk_bytes = parts_row[1] or 0
            uncompressed_bytes = parts_row[2] or 0
            ratio = round(uncompressed_bytes / max(1, disk_bytes), 2)

            ch_metrics = {
                "status": "connected",
                "bronze_trades_count": trades_count,
                "active_parts": active_parts,
                "disk_bytes": disk_bytes,
                "uncompressed_bytes": uncompressed_bytes,
                "compression_ratio": ratio,
            }
        except Exception as e:
            logger.warning("ClickHouse telemetry query failed: %s", e)
            ch_metrics = {"status": "error", "error": str(e)}

    # 2. Redis In-Memory Metrics
    redis_client = getattr(request.app.state, "redis_client", None)
    if redis_client is not None:
        try:
            mem_info = await redis_client.info("memory")
            clients_info = await redis_client.info("clients")
            total_keys = await redis_client.dbsize()
            redis_metrics = {
                "status": "connected",
                "used_memory_human": mem_info.get("used_memory_human", "0B"),
                "used_memory_bytes": mem_info.get("used_memory", 0),
                "connected_clients": clients_info.get("connected_clients", 0),
                "total_keys": total_keys,
            }
        except Exception as e:
            logger.warning("Redis telemetry query failed: %s", e)
            redis_metrics = {"status": "error", "error": str(e)}

    # 3. WebSocket Hub Connections
    ws_hub = getattr(request.app.state, "ws_hub", None)
    active_ws = ws_hub.active_connections_count if ws_hub is not None else 0

    return {
        "status": "healthy",
        "timestamp": now_ms,
        "clickhouse": ch_metrics,
        "redis": redis_metrics,
        "ws_hub": {
            "active_connections": active_ws,
        },
    }


@router.get("/trades/live")
async def get_live_trades(
    request: Request,
    symbol: str | None = Query(default=None, description="Optional symbol filter, e.g. BTCUSDT"),
    limit: int = Query(default=50, ge=1, le=200, description="Max trades to return (1-200)"),
) -> list[dict[str, Any]]:
    """Query recent market trades from ClickHouse bronze_trades table (ADR D12)."""
    clean_symbol = _sanitize_symbol(symbol)
    ch_client = getattr(request.app.state, "ch_client", None)
    if ch_client is None:
        return []

    sql = (
        "SELECT symbol, trade_id, price, qty, side, event_time, is_simulated "
        "FROM crypto.bronze_trades "
    )
    params: dict[str, Any] = {"limit": limit}

    if clean_symbol:
        sql += "WHERE symbol = %(symbol)s "
        params["symbol"] = clean_symbol

    sql += "ORDER BY event_time DESC, trade_id DESC LIMIT %(limit)s"

    try:
        res = ch_client.query(sql, parameters=params)
        results: list[dict[str, Any]] = []
        for row in res.result_rows:
            trade_dict = dict(zip(res.column_names, row, strict=False))
            ev_time = trade_dict.get("event_time")
            if isinstance(ev_time, datetime.datetime):
                trade_dict["event_time"] = ev_time.isoformat()
            results.append(trade_dict)
        return results
    except Exception as e:
        logger.error("Failed to query ClickHouse bronze_trades: %s", e)
        raise HTTPException(status_code=500, detail="Database query failed") from e


@router.get("/alerts")
async def get_alerts(
    request: Request,
    symbol: str | None = Query(default=None, description="Optional symbol filter, e.g. BTCUSDT"),
    limit: int = Query(default=50, ge=1, le=100, description="Max alerts to return (1-100)"),
) -> list[dict[str, Any]]:
    """Fetch recent market surveillance alerts from Redis Stream (ADR D13)."""
    clean_symbol = _sanitize_symbol(symbol)
    redis_client = getattr(request.app.state, "redis_client", None)
    if redis_client is None:
        return []

    count_to_fetch = limit * 3 if clean_symbol else limit
    try:
        raw_entries = await redis_client.xrevrange(
            ALERTS_STREAM_KEY,
            max="+",
            min="-",
            count=count_to_fetch,
        )
    except Exception as e:
        logger.warning("Error fetching alerts from Redis Stream: %s", e)
        return []

    alerts: list[dict[str, Any]] = []
    for _msg_id, fields in raw_entries:
        raw_data = fields.get(b"data") or fields.get("data")
        if not raw_data:
            continue
        try:
            alert = json.loads(raw_data)
            if clean_symbol and alert.get("symbol") != clean_symbol:
                continue
            alerts.append(alert)
            if len(alerts) >= limit:
                break
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError) as e:
            logger.debug("Skipping malformed alert payload: %s", e)
            continue

    return alerts


@router.get("/symbols")
async def get_symbols() -> list[dict[str, Any]]:
    """Return catalog of Top 100 tracked symbols with liquidity tier and market category."""
    return [
        {
            "symbol": sym.symbol,
            "base_asset": sym.base_asset,
            "quote_asset": sym.quote_asset,
            "tier": str(sym.liquidity_tier),
            "category": str(sym.category),
        }
        for sym in SYMBOL_CATALOG.values()
    ]


def _to_unix_seconds(val: Any) -> int:
    """Convert DateTime or numeric timestamp to integer Unix seconds."""
    if isinstance(val, (int, float)):
        return int(val)
    if isinstance(val, datetime.datetime):
        return int(val.replace(tzinfo=datetime.UTC).timestamp())
    return int(time.time())


def _query_candles_from_ch(
    ch_client: Any, symbol: str, tf: str, limit: int
) -> list[dict[str, Any]]:
    """Query candle bars from ClickHouse Silver or fallback to aggregated Bronze trades."""
    params: dict[str, Any] = {"symbol": symbol, "limit": limit}
    results: list[dict[str, Any]] = []

    # 1. Try querying Silver table directly via constant SQL map
    sql = _TF_SQL_MAP.get(tf)
    if sql:
        try:
            res = ch_client.query(sql, parameters=params)
            for row in res.result_rows:
                results.append(
                    {
                        "time": _to_unix_seconds(row[0]),
                        "open": float(row[1]),
                        "high": float(row[2]),
                        "low": float(row[3]),
                        "close": float(row[4]),
                        "volume": float(row[5]),
                    }
                )
        except Exception as e:
            logger.debug("Silver candles query skipped/empty: %s", e)

    if results:
        return results

    # 2. Fallback: Aggregate directly from bronze_trades via constant SQL map
    fallback_sql = _BRONZE_FALLBACK_SQL_MAP.get(tf)
    if fallback_sql:
        try:
            res = ch_client.query(fallback_sql, parameters=params)
            for row in res.result_rows:
                results.append(
                    {
                        "time": _to_unix_seconds(row[0]),
                        "open": float(row[1]),
                        "high": float(row[2]),
                        "low": float(row[3]),
                        "close": float(row[4]),
                        "volume": float(row[5]),
                    }
                )
        except Exception as e:
            logger.warning("Bronze fallback candles query failed: %s", e)

    return results


@router.get("/candles/{symbol}")
async def get_candles(
    request: Request,
    symbol: str,
    tf: str = Query(default="1m", description="Candle timeframe (1m, 5m, 15m, 1h, 1d)"),
    limit: int = Query(default=150, ge=1, le=500, description="Max candle bars to return (1-500)"),
) -> list[dict[str, Any]]:
    """Return historical and aggregated candle bars for TradingView Lightweight Charts (ADR D12)."""
    clean_symbol = _sanitize_symbol(symbol)
    if not clean_symbol:
        raise HTTPException(status_code=400, detail="Symbol is required")

    clean_tf = tf.strip().lower()
    if clean_tf not in _ALLOWED_TIMEFRAMES:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported timeframe '{tf}'. Allowed: {list(_ALLOWED_TIMEFRAMES.keys())}",
        )

    ch_client = getattr(request.app.state, "ch_client", None)
    if ch_client is None:
        return []

    return _query_candles_from_ch(ch_client, clean_symbol, clean_tf, limit)
