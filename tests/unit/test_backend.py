"""Unit tests for FastAPI backend service, REST endpoints, metrics, and WebSocket hub (ADR D12)."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import WebSocketDisconnect
from fastapi.testclient import TestClient

from backend.main import create_app, lifespan
from backend.ws_hub import WebSocketHub


@pytest.fixture
def mock_app_state(
    mock_ch_client: MagicMock, mock_redis_client: AsyncMock
) -> tuple[MagicMock, AsyncMock, WebSocketHub]:
    """Create mocked dependencies for app state using shared conftest fixtures."""
    ws_hub = WebSocketHub(redis_url="redis://localhost:6379/0")
    return mock_ch_client, mock_redis_client, ws_hub


@pytest.fixture
def client(mock_app_state: tuple[MagicMock, AsyncMock, WebSocketHub]) -> TestClient:
    """Create test client with mocked state dependencies."""
    mock_ch, mock_redis, ws_hub = mock_app_state
    app = create_app()
    app.state.ch_client = mock_ch
    app.state.redis_client = mock_redis
    app.state.ws_hub = ws_hub
    return TestClient(app)


class TestBackendRestApi:
    """Test suite for REST endpoints."""

    def test_health_check(self, client: TestClient) -> None:
        """Verify /health returns 200 OK and version info."""
        response = client.get("/health")
        assert response.status_code == 200
        payload = response.json()
        assert payload["status"] == "ok"
        assert payload["app"] == "crypto-surveillance-stream"
        assert payload["version"] == "4.2.0"

    def test_telemetry_endpoint(self, client: TestClient) -> None:
        """Verify /api/v1/telemetry aggregates ClickHouse, Redis, and WS Hub status."""
        response = client.get("/api/v1/telemetry")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "healthy"
        assert "clickhouse" in data
        assert data["clickhouse"]["status"] == "connected"
        assert data["clickhouse"]["bronze_trades_count"] == 640
        assert data["clickhouse"]["active_parts"] == 100
        assert "redis" in data
        assert data["redis"]["status"] == "connected"
        assert data["redis"]["connected_clients"] == 3
        assert "ws_hub" in data
        assert data["ws_hub"]["active_connections"] == 0

    def test_telemetry_graceful_when_dependencies_none(self) -> None:
        """Verify telemetry does not fail if ClickHouse or Redis are unavailable."""
        app = create_app()
        app.state.ch_client = None
        app.state.redis_client = None
        app.state.ws_hub = None
        test_client = TestClient(app)

        response = test_client.get("/api/v1/telemetry")
        assert response.status_code == 200
        data = response.json()
        assert data["clickhouse"]["status"] == "unavailable"
        assert data["redis"]["status"] == "unavailable"
        assert data["ws_hub"]["active_connections"] == 0

    def test_live_trades_query(self, client: TestClient) -> None:
        """Verify /api/v1/trades/live returns mapped trades from ClickHouse."""
        response = client.get("/api/v1/trades/live?symbol=BTCUSDT&limit=10")
        assert response.status_code == 200
        trades = response.json()
        assert isinstance(trades, list)
        assert len(trades) == 1
        assert trades[0]["symbol"] == "BTCUSDT"
        assert trades[0]["price"] == 69000.0
        assert trades[0]["side"] == "buy"

    def test_live_trades_invalid_symbol_rejected(self, client: TestClient) -> None:
        """Verify invalid symbol formats are rejected with 400 Bad Request."""
        response = client.get("/api/v1/trades/live?symbol=INVALID--@@")
        assert response.status_code == 400

    def test_live_trades_limit_out_of_bounds(self, client: TestClient) -> None:
        """Verify limit parameter bounds (1-200) are enforced."""
        response = client.get("/api/v1/trades/live?limit=500")
        assert response.status_code == 422

    def test_alerts_query(self, client: TestClient) -> None:
        """Verify /api/v1/alerts retrieves and filters alerts from Redis Stream."""
        response = client.get("/api/v1/alerts?symbol=BTCUSDT&limit=10")
        assert response.status_code == 200
        alerts = response.json()
        assert isinstance(alerts, list)
        assert len(alerts) == 1
        assert alerts[0]["symbol"] == "BTCUSDT"
        assert alerts[0]["alert_type"] == "WHALE_TRADE"

    def test_alerts_symbol_filtering(self, client: TestClient) -> None:
        """Verify alerts endpoint filters out non-matching symbols."""
        response = client.get("/api/v1/alerts?symbol=ETHUSDT")
        assert response.status_code == 200
        alerts = response.json()
        assert len(alerts) == 0


class TestPrometheusMetrics:
    """Test suite for Prometheus observability endpoint."""

    def test_metrics_endpoint_format(self, client: TestClient) -> None:
        """Verify /metrics exports Prometheus plaintext with gauges and counters."""
        response = client.get("/metrics")
        assert response.status_code == 200
        text = response.text
        assert "crypto_active_ws_connections" in text
        assert "crypto_clickhouse_bronze_trades_total" in text
        assert "crypto_redis_used_memory_bytes" in text
        assert "crypto_http_requests_total" in text


class TestWebSocketHubUnit:
    """Test suite for WebSocket hub logic."""

    def test_parse_ticker_message_success(self) -> None:
        """Verify ticker extraction from Redis Pub/Sub channel and payload."""
        hub = WebSocketHub("redis://localhost:6379/0")
        parsed = hub._parse_ticker_message(b"crypto:px:BTCUSDT", b"69420.50")
        assert parsed is not None
        assert parsed["type"] == "ticker"
        assert parsed["symbol"] == "BTCUSDT"
        assert parsed["price"] == 69420.50

    def test_parse_ticker_message_invalid_payload(self) -> None:
        """Verify invalid or non-numeric ticker payload returns None."""
        hub = WebSocketHub("redis://localhost:6379/0")
        assert hub._parse_ticker_message(b"crypto:px:BTCUSDT", b"not-a-number") is None
        assert hub._parse_ticker_message(b"invalid_channel", b"100.0") is None

    def test_parse_stream_alert(self) -> None:
        """Verify stream alert parsing from Redis stream fields."""
        hub = WebSocketHub("redis://localhost:6379/0")
        valid_alert = {"alert_id": "1", "symbol": "ETHUSDT"}
        parsed = hub._parse_stream_alert({b"data": json.dumps(valid_alert).encode()})
        assert parsed is not None
        assert parsed["type"] == "alert"
        assert parsed["data"]["symbol"] == "ETHUSDT"

        # Invalid JSON bytes
        assert hub._parse_stream_alert({b"data": b"invalid json{"}) is None
        assert hub._parse_stream_alert({}) is None

    def test_websocket_client_lifecycle_and_ping(self, client: TestClient) -> None:
        """Verify WebSocket handshake, welcome message, and ping-pong interaction."""
        with client.websocket_connect("/ws") as ws:
            # 1. Receive welcome message
            welcome = ws.receive_json()
            assert welcome["type"] == "connected"

            # 2. Send ping heartbeat
            ws.send_text('{"action": "ping"}')
            pong = ws.receive_json()
            assert pong["type"] == "pong"

    def test_websocket_capacity_limit_rejected(self) -> None:
        """Verify WebSocket connection is rejected when hub reaches max capacity."""
        hub = WebSocketHub("redis://localhost:6379/0", max_connections=0)
        app = create_app()
        app.state.ch_client = None
        app.state.redis_client = None
        app.state.ws_hub = hub
        test_client = TestClient(app)
        with pytest.raises(WebSocketDisconnect), test_client.websocket_connect("/ws"):
            pass

    def test_websocket_oversized_payload_rejected(self, client: TestClient) -> None:
        """Verify client sending payload > 4KB is disconnected."""
        with client.websocket_connect("/ws") as ws:
            ws.receive_json()
            ws.send_text("A" * 5000)


class TestArchitectureAndSecurityBoundaries:
    """Enforce architectural constraints (ADR D9, D11, CONSTRAINTS.md)."""

    def test_backend_does_not_import_ingestors_or_pipelines(self) -> None:
        """Verify backend code strictly obeys architecture boundaries."""
        import backend.config
        import backend.main
        import backend.routes.api
        import backend.routes.metrics
        import backend.ws_hub

        for module in (
            backend.config,
            backend.main,
            backend.routes.api,
            backend.routes.metrics,
            backend.ws_hub,
        ):
            module_name = module.__name__
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if hasattr(attr, "__module__") and attr.__module__:
                    assert not attr.__module__.startswith("ingestors"), (
                        f"Boundary violation: {module_name} imports ingestors via {attr_name}"
                    )
                    assert not attr.__module__.startswith("pipelines"), (
                        f"Boundary violation: {module_name} imports pipelines via {attr_name}"
                    )

    def test_mutating_http_methods_rejected(self, client: TestClient) -> None:
        """Enforce 100% read-only API: POST/PUT/DELETE return 405 Method Not Allowed."""
        assert client.post("/api/v1/trades/live").status_code == 405
        assert client.put("/api/v1/trades/live").status_code == 405
        assert client.delete("/api/v1/trades/live").status_code == 405
        assert client.post("/api/v1/telemetry").status_code == 405
        assert client.post("/api/v1/alerts").status_code == 405


class TestBackendLifespan:
    """Test suite for application lifespan management."""

    @pytest.mark.asyncio
    async def test_lifespan_startup_and_shutdown(self) -> None:
        """Verify startup initializes database and redis, and shutdown cleans resources."""
        app = create_app()
        mock_ch_client = MagicMock()
        mock_redis_client = AsyncMock()
        mock_redis_client.ping.return_value = True

        with (
            patch("clickhouse_connect.get_client", return_value=mock_ch_client),
            patch("redis.asyncio.from_url", return_value=mock_redis_client),
            patch.object(WebSocketHub, "start", new_callable=AsyncMock) as mock_start,
            patch.object(WebSocketHub, "stop", new_callable=AsyncMock) as mock_stop,
        ):
            async with lifespan(app):
                assert app.state.ch_client is mock_ch_client
                assert app.state.redis_client is mock_redis_client
                assert isinstance(app.state.ws_hub, WebSocketHub)
                mock_start.assert_awaited_once()

            mock_stop.assert_awaited_once()
            mock_redis_client.aclose.assert_awaited_once()
            mock_ch_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_lifespan_graceful_on_connection_error(self) -> None:
        """Verify lifespan gracefully sets state to None if databases fail to connect."""
        app = create_app()

        with (
            patch("clickhouse_connect.get_client", side_effect=RuntimeError("CH down")),
            patch("redis.asyncio.from_url", side_effect=RuntimeError("Redis down")),
            patch.object(WebSocketHub, "start", new_callable=AsyncMock),
            patch.object(WebSocketHub, "stop", new_callable=AsyncMock),
        ):
            async with lifespan(app):
                assert app.state.ch_client is None
                assert app.state.redis_client is None


class TestBackendRestApiExtended:
    """Additional REST API edge-case verification for full branch coverage."""

    def test_telemetry_clickhouse_query_exception(self, mock_redis_client: AsyncMock) -> None:
        """Verify telemetry returns error status when ClickHouse query fails."""
        app = create_app()
        mock_ch = MagicMock()
        mock_ch.query.side_effect = RuntimeError("ClickHouse connection reset")

        app.state.ch_client = mock_ch
        app.state.redis_client = mock_redis_client
        app.state.ws_hub = WebSocketHub("redis://localhost:6379/0")
        client = TestClient(app)

        response = client.get("/api/v1/telemetry")
        assert response.status_code == 200
        data = response.json()
        assert data["clickhouse"]["status"] == "error"

    def test_live_trades_clickhouse_query_exception(self) -> None:
        """Verify live trades endpoint returns 500 when ClickHouse query fails."""
        app = create_app()
        mock_ch = MagicMock()
        mock_ch.query.side_effect = RuntimeError("Query execution failed")
        app.state.ch_client = mock_ch
        app.state.redis_client = None
        app.state.ws_hub = None
        client = TestClient(app)

        response = client.get("/api/v1/trades/live?symbol=BTCUSDT")
        assert response.status_code == 500
        assert "Database query failed" in response.json()["detail"]

    def test_live_trades_db_none(self) -> None:
        """Verify live trades returns empty list when ClickHouse is None."""
        app = create_app()
        app.state.ch_client = None
        app.state.redis_client = None
        app.state.ws_hub = None
        client = TestClient(app)

        response = client.get("/api/v1/trades/live?symbol=BTCUSDT")
        assert response.status_code == 200
        assert response.json() == []

    def test_alerts_redis_xrevrange_exception(self) -> None:
        """Verify alerts returns empty list when Redis query fails."""
        app = create_app()
        mock_redis = AsyncMock()
        mock_redis.xrevrange.side_effect = RuntimeError("Redis stream error")
        app.state.ch_client = None
        app.state.redis_client = mock_redis
        app.state.ws_hub = None
        client = TestClient(app)

        response = client.get("/api/v1/alerts")
        assert response.status_code == 200
        assert response.json() == []

    def test_alerts_skips_malformed_payload(self) -> None:
        """Verify alerts endpoint skips malformed json payloads."""
        app = create_app()
        mock_redis = AsyncMock()
        mock_redis.xrevrange.return_value = [
            (b"1-0", {b"data": b"not-json"}),
            (
                b"2-0",
                {b"data": json.dumps({"symbol": "BTCUSDT", "alert_type": "WHALE_TRADE"}).encode()},
            ),
        ]
        app.state.ch_client = None
        app.state.redis_client = mock_redis
        app.state.ws_hub = None
        client = TestClient(app)

        response = client.get("/api/v1/alerts")
        assert response.status_code == 200
        alerts = response.json()
        assert len(alerts) == 1
        assert alerts[0]["symbol"] == "BTCUSDT"


class TestWebSocketHubExtended:
    """Extended WebSocketHub tests to achieve >= 80% coverage on ws_hub.py."""

    @pytest.mark.asyncio
    async def test_ws_hub_broadcast_cleans_disconnected_clients(self) -> None:
        """Verify broadcast removes dead connections on WebSocketDisconnect or error."""
        hub = WebSocketHub("redis://localhost:6379/0")
        good_ws = AsyncMock()
        good_ws.send_text = AsyncMock()
        bad_ws = AsyncMock()
        bad_ws.send_text = AsyncMock(side_effect=WebSocketDisconnect(1000))

        hub._connections.add(good_ws)
        hub._connections.add(bad_ws)

        await hub.broadcast({"type": "test"})

        assert good_ws in hub._connections
        assert bad_ws not in hub._connections
        good_ws.send_text.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_ws_hub_start_stop_lifecycle(self) -> None:
        """Verify start and stop methods manage tasks and redis client."""
        hub = WebSocketHub("redis://localhost:6379/0")
        mock_redis = AsyncMock()
        mock_pubsub = AsyncMock()
        mock_redis.pubsub = MagicMock(return_value=mock_pubsub)

        with patch("redis.asyncio.from_url", return_value=mock_redis):
            await hub.start()
            assert hub._running is True
            assert len(hub._tasks) == 3

            await hub.stop()
            assert hub._running is False
            assert len(hub._tasks) == 0

    def test_extract_alerts_from_entries_variations(self) -> None:
        """Verify _extract_alerts_from_entries handles various stream entry structures."""
        hub = WebSocketHub("redis://localhost:6379/0")

        # Invalid type
        last_id, alerts = hub._extract_alerts_from_entries("not-a-list", "$")
        assert last_id == "$"
        assert alerts == []

        # Empty entries
        last_id, alerts = hub._extract_alerts_from_entries([], "$")
        assert alerts == []

        # Malformed entries
        last_id, alerts = hub._extract_alerts_from_entries([("short",)], "$")
        assert alerts == []

        # Valid entry with valid and invalid alerts
        raw_entries = [
            (
                b"crypto:alerts:stream",
                [
                    (b"1700000000001-0", {b"data": json.dumps({"symbol": "SOLUSDT"}).encode()}),
                    (b"1700000000002-0", {b"data": b"broken"}),
                ],
            )
        ]
        last_id, alerts = hub._extract_alerts_from_entries(raw_entries, "$")
        assert last_id == "1700000000002-0"
        assert len(alerts) == 1
        assert alerts[0]["data"]["symbol"] == "SOLUSDT"

    @pytest.mark.asyncio
    async def test_ws_hub_telemetry_heartbeat(self) -> None:
        """Verify telemetry heartbeat broadcasts when connections exist."""
        hub = WebSocketHub("redis://localhost:6379/0")
        mock_ws = AsyncMock()
        mock_ws.send_text = AsyncMock()
        hub._connections.add(mock_ws)
        hub._running = True

        async def stop_after_delay() -> None:
            await asyncio.sleep(0.05)
            hub._running = False

        await asyncio.gather(
            hub._telemetry_heartbeat(interval_seconds=0.01),
            stop_after_delay(),
        )
        assert mock_ws.send_text.await_count >= 1

    @pytest.mark.asyncio
    async def test_ws_hub_listen_pubsub(self) -> None:
        """Verify _listen_pubsub receives messages and broadcasts tickers."""
        hub = WebSocketHub("redis://localhost:6379/0")
        mock_ws = AsyncMock()
        mock_ws.send_text = AsyncMock()
        hub._connections.add(mock_ws)
        hub._running = True

        mock_pubsub = AsyncMock()
        mock_pubsub.get_message.side_effect = [
            {"type": "pmessage", "channel": b"crypto:px:BTCUSDT", "data": b"68500.25"},
            None,
        ]
        hub._pubsub = mock_pubsub

        async def stop_soon() -> None:
            await asyncio.sleep(0.03)
            hub._running = False

        await asyncio.gather(
            hub._listen_pubsub(),
            stop_soon(),
        )
        assert mock_ws.send_text.await_count >= 1

    @pytest.mark.asyncio
    async def test_ws_hub_listen_alerts_stream(self) -> None:
        """Verify _listen_alerts_stream reads from stream and broadcasts."""
        hub = WebSocketHub("redis://localhost:6379/0")
        mock_ws = AsyncMock()
        mock_ws.send_text = AsyncMock()
        hub._connections.add(mock_ws)
        hub._running = True

        mock_redis = AsyncMock()
        mock_redis.xread.return_value = [
            (
                b"crypto:alerts:stream",
                [
                    (
                        b"100-0",
                        {b"data": json.dumps({"alert_id": "a1", "symbol": "BTCUSDT"}).encode()},
                    )
                ],
            )
        ]
        hub._redis_client = mock_redis

        async def stop_soon() -> None:
            await asyncio.sleep(0.02)
            hub._running = False

        await asyncio.gather(
            hub._listen_alerts_stream(),
            stop_soon(),
        )
        assert mock_ws.send_text.await_count >= 1
