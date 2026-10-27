"""Prometheus metrics endpoint and telemetry instrumentation (ADR D15)."""

from __future__ import annotations

import logging
from typing import Final

from fastapi import APIRouter, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, generate_latest

logger = logging.getLogger("backend.routes.metrics")
router = APIRouter(tags=["Observability"])

ACTIVE_WS_CONNECTIONS: Final[Gauge] = Gauge(
    "crypto_active_ws_connections",
    "Current active client WebSocket connections to the broadcast hub",
)

CLICKHOUSE_BRONZE_TRADES: Final[Gauge] = Gauge(
    "crypto_clickhouse_bronze_trades_total",
    "Total raw trade events successfully stored in ClickHouse bronze_trades",
)

REDIS_USED_MEMORY_BYTES: Final[Gauge] = Gauge(
    "crypto_redis_used_memory_bytes",
    "Redis memory consumption in bytes",
)

REDIS_CONNECTED_CLIENTS: Final[Gauge] = Gauge(
    "crypto_redis_connected_clients",
    "Current number of client connections to Redis instance",
)

HTTP_REQUESTS_TOTAL: Final[Counter] = Counter(
    "crypto_http_requests_total",
    "Total HTTP requests handled by FastAPI service",
    ["endpoint", "method"],
)


@router.get("/metrics")
async def get_prometheus_metrics(request: Request) -> Response:
    """Scrape endpoint exporting real-time platform telemetry for Prometheus (ADR D15)."""
    HTTP_REQUESTS_TOTAL.labels(endpoint="/metrics", method="GET").inc()

    # 1. Update WebSocket connections gauge
    ws_hub = getattr(request.app.state, "ws_hub", None)
    if ws_hub is not None:
        ACTIVE_WS_CONNECTIONS.set(ws_hub.active_connections_count)

    # 2. Update ClickHouse row count gauge
    ch_client = getattr(request.app.state, "ch_client", None)
    if ch_client is not None:
        try:
            res = ch_client.query("SELECT count() FROM crypto.bronze_trades")
            if res.result_rows:
                CLICKHOUSE_BRONZE_TRADES.set(res.result_rows[0][0])
        except Exception as e:
            logger.debug("Failed to query ClickHouse rows for metrics: %s", e)

    # 3. Update Redis metrics gauge
    redis_client = getattr(request.app.state, "redis_client", None)
    if redis_client is not None:
        try:
            mem_info = await redis_client.info("memory")
            clients_info = await redis_client.info("clients")
            REDIS_USED_MEMORY_BYTES.set(float(mem_info.get("used_memory", 0)))
            REDIS_CONNECTED_CLIENTS.set(float(clients_info.get("connected_clients", 0)))
        except Exception as e:
            logger.debug("Failed to query Redis info for metrics: %s", e)

    payload = generate_latest()
    return Response(content=payload, media_type=CONTENT_TYPE_LATEST)
