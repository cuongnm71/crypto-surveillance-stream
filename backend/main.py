"""FastAPI application entrypoint, lifespan manager, and WebSocket broadcast hub (ADR D12)."""

from __future__ import annotations

import logging
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import clickhouse_connect
import redis.asyncio as aioredis
import uvicorn
from fastapi import FastAPI, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware

from backend.config import settings
from backend.routes.api import router as api_router
from backend.routes.metrics import HTTP_REQUESTS_TOTAL
from backend.routes.metrics import router as metrics_router
from backend.ws_hub import WebSocketHub

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("backend.main")


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage application startup, database connection pools, and graceful shutdown."""
    logger.info("Initializing FastAPI Backend Service (APP_ENV=%s)...", settings.app_env)

    # 1. Initialize ClickHouse read-only client
    ch_client: Any = None
    try:
        ch_client = clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            database=settings.clickhouse_db,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
        )
        logger.info(
            "Connected to ClickHouse OLAP at %s:%s",
            settings.clickhouse_host,
            settings.clickhouse_port,
        )
    except Exception as e:
        logger.warning("Could not connect to ClickHouse at startup: %s", e)

    # 2. Initialize Redis async client
    redis_client: aioredis.Redis | None = None
    try:
        redis_client = aioredis.from_url(settings.redis_url, decode_responses=False)
        await redis_client.ping()
        logger.info("Connected to Redis at %s", settings.redis_url)
    except Exception as e:
        logger.warning("Could not connect to Redis at startup: %s", e)

    # 3. Initialize and start WebSocket Hub
    ws_hub = WebSocketHub(redis_url=settings.redis_url)
    await ws_hub.start()

    # Store in application state for route dependency access
    app.state.ch_client = ch_client
    app.state.redis_client = redis_client
    app.state.ws_hub = ws_hub

    yield

    # Teardown
    logger.info("Shutting down FastAPI Backend Service...")
    await ws_hub.stop()

    if redis_client is not None:
        try:
            await redis_client.aclose()
        except Exception as e:
            logger.debug("Error closing Redis client: %s", e)

    if ch_client is not None and hasattr(ch_client, "close"):
        try:
            ch_client.close()
        except Exception as e:
            logger.debug("Error closing ClickHouse client: %s", e)

    logger.info("Backend service teardown complete.")


def create_app() -> FastAPI:
    """Construct and configure the FastAPI application instance."""
    app = FastAPI(
        title="Crypto Market Surveillance & Streaming Platform",
        version="4.2.0",
        description=(
            "High-throughput low-latency real-time market surveillance, "
            "4Hz ticker hub, and OLAP analytics."
        ),
        lifespan=lifespan,
    )

    # Enforce Read-Only API CORS: only GET, HEAD, OPTIONS permitted
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["GET", "HEAD", "OPTIONS"],
        allow_headers=["*"],
    )

    @app.middleware("http")
    async def request_metrics_middleware(request: Request, call_next: Any) -> Response:
        """Measure HTTP request latency and record Prometheus metrics."""
        start_time = time.monotonic()
        response: Response = await call_next(request)
        duration_ms = (time.monotonic() - start_time) * 1000
        path = request.url.path
        method = request.method
        HTTP_REQUESTS_TOTAL.labels(endpoint=path, method=method).inc()
        if duration_ms > 100.0:
            logger.warning("Slow HTTP %s %s: %.1fms", method, path, duration_ms)
        return response

    @app.get("/health", tags=["Health"])
    async def health_check() -> dict[str, str]:
        """Healthcheck endpoint for container orchestration and uptime probes."""
        return {
            "status": "ok",
            "app": "crypto-surveillance-stream",
            "version": "4.2.0",
        }

    app.include_router(api_router)
    app.include_router(metrics_router)

    @app.websocket("/ws")
    async def websocket_endpoint(websocket: WebSocket) -> None:
        """Public WebSocket hub broadcasting 4Hz price tickers, live alerts, and telemetry."""
        ws_hub: WebSocketHub = websocket.app.state.ws_hub
        await ws_hub.connect(websocket)
        try:
            while True:
                data = await websocket.receive_text()
                # Security: Disconnect clients sending oversized frames (> 4KB)
                if len(data) > 4096:
                    await websocket.close(code=1009, reason="Message too large")
                    break
                # Respond to client ping heartbeats
                if "ping" in data.lower():
                    now_ms = int(time.time() * 1000)
                    await websocket.send_text(f'{{"type": "pong", "timestamp": {now_ms}}}')
        except (WebSocketDisconnect, ConnectionResetError):
            ws_hub.disconnect(websocket)
        except Exception as e:
            logger.debug("WebSocket client error: %s", e)
            ws_hub.disconnect(websocket)

    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run(
        "backend.main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=False,
    )
