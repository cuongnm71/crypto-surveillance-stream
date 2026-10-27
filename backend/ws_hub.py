"""Async WebSocket broadcast hub for 4Hz prices, live alerts, and telemetry (ADR D12)."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

import redis.asyncio as aioredis
from fastapi import WebSocket, WebSocketDisconnect

from common.constants import ALERTS_STREAM_KEY

logger = logging.getLogger("backend.ws_hub")


class WebSocketHub:
    """Manages active WebSocket connections and broadcasts real-time market updates."""

    def __init__(self, redis_url: str, max_connections: int = 500) -> None:
        self.redis_url = redis_url
        self.max_connections = max_connections
        self._connections: set[WebSocket] = set()
        self._redis_client: aioredis.Redis | None = None
        self._pubsub: Any = None
        self._running: bool = False
        self._tasks: list[asyncio.Task[None]] = []

    @property
    def active_connections_count(self) -> int:
        """Return the number of currently connected WebSocket clients."""
        return len(self._connections)

    async def connect(self, websocket: WebSocket) -> None:
        """Register and accept a new client WebSocket connection with capacity check."""
        if len(self._connections) >= self.max_connections:
            logger.warning(
                "Rejected WebSocket connection: capacity limit of %d reached",
                self.max_connections,
            )
            await websocket.close(code=1008, reason="Server connection capacity reached")
            return

        await websocket.accept()
        self._connections.add(websocket)
        logger.info("WebSocket client connected. Active connections: %d", len(self._connections))
        now_ms = int(time.time() * 1000)
        welcome_payload = {
            "type": "connected",
            "message": "Connected to Crypto Surveillance Live Hub",
            "timestamp": now_ms,
        }
        await websocket.send_text(json.dumps(welcome_payload))

    def disconnect(self, websocket: WebSocket) -> None:
        """Remove a disconnected client from the active connections registry."""
        self._connections.discard(websocket)
        logger.info("WebSocket client disconnected. Active connections: %d", len(self._connections))

    async def broadcast(self, message: dict[str, Any] | str) -> None:
        """Broadcast a message to all active WebSocket clients safely."""
        if not self._connections:
            return

        text = message if isinstance(message, str) else json.dumps(message)
        stale_connections: list[WebSocket] = []

        async def _send_safe(ws: WebSocket) -> None:
            try:
                await ws.send_text(text)
            except (WebSocketDisconnect, RuntimeError, ConnectionResetError):
                stale_connections.append(ws)

        send_coros = [_send_safe(ws) for ws in list(self._connections)]
        await asyncio.gather(*send_coros, return_exceptions=True)

        for ws in stale_connections:
            self.disconnect(ws)

    async def start(self) -> None:
        """Start Redis Pub/Sub and Stream listener background loops."""
        if self._running:
            return
        self._running = True
        try:
            self._redis_client = aioredis.from_url(self.redis_url, decode_responses=False)
            self._pubsub = self._redis_client.pubsub()
            await self._pubsub.psubscribe("crypto:px:*")
            self._tasks.append(asyncio.create_task(self._listen_pubsub()))
            self._tasks.append(asyncio.create_task(self._listen_alerts_stream()))
            self._tasks.append(asyncio.create_task(self._telemetry_heartbeat()))
            logger.info("WebSocketHub background listeners started successfully.")
        except Exception as e:
            logger.warning("WebSocketHub failed to connect to Redis: %s", e)

    async def stop(self) -> None:
        """Gracefully shut down all background listeners and close Redis connections."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
            self._tasks.clear()

        if self._pubsub is not None:
            try:
                await self._pubsub.punsubscribe("crypto:px:*")
                await self._pubsub.close()
            except Exception as e:
                logger.debug("Error closing pubsub: %s", e)
            self._pubsub = None

        if self._redis_client is not None:
            try:
                await self._redis_client.aclose()
            except Exception as e:
                logger.debug("Error closing redis client: %s", e)
            self._redis_client = None

        for ws in list(self._connections):
            try:
                await ws.close()
            except Exception as e:
                logger.debug("Error closing client connection during stop: %s", e)
        self._connections.clear()
        logger.info("WebSocketHub stopped.")

    def _parse_ticker_message(
        self, raw_channel: bytes | str, raw_data: bytes | str
    ) -> dict[str, Any] | None:
        """Extract symbol and price from Redis Pub/Sub message."""
        channel_str = (
            raw_channel.decode("utf-8") if isinstance(raw_channel, bytes) else str(raw_channel)
        )
        symbol = channel_str.split(":")[-1] if ":" in channel_str else ""
        if not symbol:
            return None

        data_str = raw_data.decode("utf-8") if isinstance(raw_data, bytes) else str(raw_data)
        try:
            price_val = float(data_str)
        except (ValueError, TypeError):
            return None

        now_ms = int(time.time() * 1000)
        return {
            "type": "ticker",
            "symbol": symbol,
            "price": price_val,
            "timestamp": now_ms,
        }

    async def _listen_pubsub(self) -> None:
        """Listen to Redis Pub/Sub 4Hz price ticker updates."""
        if self._pubsub is None:
            return

        while self._running:
            try:
                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True, timeout=1.0
                )
                if not message:
                    await asyncio.sleep(0.01)
                    continue

                if message.get("type") in ("pmessage", "message"):
                    channel = message.get("channel", b"")
                    data = message.get("data", b"")
                    payload = self._parse_ticker_message(channel, data)
                    if payload is not None:
                        await self.broadcast(payload)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Error reading from Redis Pub/Sub: %s", e)
                await asyncio.sleep(0.5)

    def _parse_stream_alert(self, raw_fields: dict[Any, Any]) -> dict[str, Any] | None:
        """Parse raw stream entry fields into an alert dictionary."""
        data_bytes = raw_fields.get(b"data") or raw_fields.get("data")
        if not data_bytes:
            return None

        try:
            parsed = json.loads(data_bytes)
            return {"type": "alert", "data": parsed}
        except Exception:
            return None

    def _extract_alerts_from_entries(
        self, entries: Any, default_last_id: str
    ) -> tuple[str, list[dict[str, Any]]]:
        """Parse raw Redis xread entries into a list of broadcast alerts and new last_id."""
        if not isinstance(entries, list):
            return default_last_id, []

        last_id = default_last_id
        alerts: list[dict[str, Any]] = []

        for entry in entries:
            if not isinstance(entry, (list, tuple)) or len(entry) < 2:
                continue
            messages = entry[1]
            if not isinstance(messages, list):
                continue
            for msg in messages:
                if not isinstance(msg, (list, tuple)) or len(msg) < 2:
                    continue
                msg_id, fields = msg[0], msg[1]
                last_id = msg_id.decode("utf-8") if isinstance(msg_id, bytes) else str(msg_id)
                if isinstance(fields, dict):
                    alert_msg = self._parse_stream_alert(fields)
                    if alert_msg is not None:
                        alerts.append(alert_msg)

        return last_id, alerts

    async def _listen_alerts_stream(self) -> None:
        """Listen to Redis Stream crypto:alerts:stream for real-time market alerts."""
        if self._redis_client is None:
            return

        last_id = "$"
        while self._running:
            try:
                entries = await self._redis_client.xread(
                    {ALERTS_STREAM_KEY: last_id},
                    count=50,
                    block=1000,
                )
                last_id, alerts = self._extract_alerts_from_entries(entries, last_id)
                for alert in alerts:
                    await self.broadcast(alert)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.warning("Error reading from Redis Stream: %s", e)
                await asyncio.sleep(1.0)

    async def _telemetry_heartbeat(self, interval_seconds: float = 2.0) -> None:
        """Broadcast periodic telemetry heartbeat to connected clients."""
        while self._running:
            try:
                await asyncio.sleep(interval_seconds)
                if not self._connections:
                    continue
                now_ms = int(time.time() * 1000)
                heartbeat = {
                    "type": "telemetry",
                    "data": {
                        "active_ws_connections": len(self._connections),
                        "timestamp": now_ms,
                    },
                }
                await self.broadcast(heartbeat)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.debug("Error in telemetry heartbeat: %s", e)
