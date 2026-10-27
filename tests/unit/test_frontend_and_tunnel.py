"""Unit tests for Task 6: 4-Panel Live Web Dashboard, Candle APIs, and Cloudflare Tunnel."""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.main import create_app


@pytest.fixture
def app_instance() -> FastAPI:
    """Create a FastAPI application instance with detached state."""
    app = create_app()
    app.state.ch_client = None
    app.state.redis_client = None
    return app


@pytest.fixture
def test_client(app_instance: FastAPI) -> TestClient:
    """Create a test client wrapping app_instance."""
    return TestClient(app_instance)


class TestFrontendStaticFiles:
    """Verify static assets are properly mounted and served by FastAPI."""

    def test_root_serves_dashboard_index_html(self, test_client: TestClient) -> None:
        """Root GET / must serve frontend/index.html with 200 OK and valid HTML."""
        response = test_client.get("/")
        assert response.status_code == 200
        assert "text/html" in response.headers.get("content-type", "")
        assert "Crypto Market Surveillance" in response.text
        assert "panel-telemetry" in response.text
        assert "panel-chart" in response.text
        assert "panel-alerts" in response.text
        assert "panel-drawer" in response.text

    def test_serves_css_stylesheet(self, test_client: TestClient) -> None:
        """GET /styles.css must serve the stylesheet with 200 OK."""
        response = test_client.get("/styles.css")
        assert response.status_code == 200
        assert "text/css" in response.headers.get("content-type", "")
        assert "--accent-cyan" in response.text

    def test_serves_chart_and_app_js(self, test_client: TestClient) -> None:
        """GET /chart.js and /app.js must return 200 OK with javascript content."""
        chart_res = test_client.get("/chart.js")
        assert chart_res.status_code == 200
        assert "SurveillanceChart" in chart_res.text

        app_res = test_client.get("/app.js")
        assert app_res.status_code == 200
        assert "connectWebSocket" in app_res.text

    def test_serves_vendor_lightweight_charts(self, test_client: TestClient) -> None:
        """GET /vendor/lightweight-charts.standalone.production.js must serve vendored library."""
        res = test_client.get("/vendor/lightweight-charts.standalone.production.js")
        assert res.status_code == 200
        assert len(res.content) > 100_000


class TestCandleAndSymbolApis:
    """Verify GET /api/v1/symbols and GET /api/v1/candles/{symbol}."""

    def test_get_symbols_catalog(self, test_client: TestClient) -> None:
        """GET /api/v1/symbols must return all 100 tracked pairs with metadata."""
        response = test_client.get("/api/v1/symbols")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 100
        symbols = [item["symbol"] for item in data]
        assert "BTCUSDT" in symbols
        assert "ETHUSDT" in symbols
        assert "SOLUSDT" in symbols
        assert "PEPEUSDT" in symbols

        # Verify attributes exist
        first = data[0]
        assert "symbol" in first
        assert "base_asset" in first
        assert "quote_asset" in first
        assert "tier" in first
        assert "category" in first

    def test_get_candles_without_db_returns_empty_list(self, test_client: TestClient) -> None:
        """GET /api/v1/candles/{symbol} when ch_client is None returns empty list."""
        response = test_client.get("/api/v1/candles/BTCUSDT?tf=1m")
        assert response.status_code == 200
        assert response.json() == []

    def test_get_candles_invalid_symbol_rejects(self, test_client: TestClient) -> None:
        """GET /api/v1/candles/{symbol} rejects malformed or SQL-injected symbols."""
        response = test_client.get("/api/v1/candles/INVALID;DROP--")
        assert response.status_code == 400
        assert "Invalid symbol" in response.text

    def test_get_candles_invalid_timeframe_rejects(self, test_client: TestClient) -> None:
        """GET /api/v1/candles/BTCUSDT with unsupported timeframe returns 400."""
        response = test_client.get("/api/v1/candles/BTCUSDT?tf=4h")
        assert response.status_code == 400
        assert "Unsupported timeframe" in response.text

    def test_get_candles_with_mocked_silver_data(
        self, test_client: TestClient, app_instance: FastAPI
    ) -> None:
        """GET /api/v1/candles/{symbol} parses ClickHouse Silver query results."""
        mock_ch = MagicMock()
        mock_result = MagicMock()
        # Row format: open_time, open, high, low, close, volume
        mock_result.result_rows = [
            (1700000000, 65000.0, 65500.0, 64900.0, 65200.0, 120.5),
            (1700000060, 65200.0, 65300.0, 65100.0, 65250.0, 85.2),
        ]
        mock_ch.query.return_value = mock_result
        app_instance.state.ch_client = mock_ch

        response = test_client.get("/api/v1/candles/BTCUSDT?tf=1m&limit=50")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["time"] == 1700000000
        assert data[0]["open"] == 65000.0
        assert data[0]["high"] == 65500.0
        assert data[0]["low"] == 64900.0
        assert data[0]["close"] == 65200.0
        assert data[0]["volume"] == 120.5

    def test_get_candles_fallback_to_bronze_when_silver_empty(
        self, test_client: TestClient, app_instance: FastAPI
    ) -> None:
        """GET /api/v1/candles/{symbol} queries bronze trades if silver returns 0 rows."""
        mock_ch = MagicMock()
        empty_res = MagicMock()
        empty_res.result_rows = []

        bronze_res = MagicMock()
        bronze_res.result_rows = [
            (1700000120, 66000.0, 66100.0, 65950.0, 66050.0, 42.0),
        ]

        mock_ch.query.side_effect = [empty_res, bronze_res]
        app_instance.state.ch_client = mock_ch

        response = test_client.get("/api/v1/candles/BTCUSDT?tf=5m")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["time"] == 1700000120
        assert data[0]["open"] == 66000.0
        assert mock_ch.query.call_count == 2


class TestFrontendDomSecurityCompliance:
    """Security verification: Assert zero innerHTML, outerHTML, or document.write in frontend."""

    def test_no_unsafe_dom_sinks_in_frontend_js(self) -> None:
        """Enforce secure web skills: prohibit innerHTML, outerHTML, document.write."""
        project_root = Path(__file__).resolve().parent.parent.parent
        frontend_dir = project_root / "frontend"
        js_files = [frontend_dir / "app.js", frontend_dir / "chart.js"]

        forbidden_patterns = ["innerHTML", "outerHTML", "document.write", "insertAdjacentHTML"]

        for js_file in js_files:
            assert js_file.is_file(), f"Expected file {js_file} does not exist"
            content = js_file.read_text(encoding="utf-8")
            for sink in forbidden_patterns:
                assert sink not in content, (
                    f"Forbidden unsafe DOM sink '{sink}' found in {js_file.name}. "
                    "Use textContent, document.createElement, or replaceChildren instead."
                )


class TestCloudflareTunnelScript:
    """Verify scripts/tunnel.sh execution and --test flag."""

    def test_tunnel_script_exists_and_executable(self) -> None:
        """scripts/tunnel.sh must exist and be executable."""
        project_root = Path(__file__).resolve().parent.parent.parent
        script_path = project_root / "scripts" / "tunnel.sh"
        assert script_path.is_file()
        assert os.access(script_path, os.X_OK), "scripts/tunnel.sh is not executable"

    def test_tunnel_script_test_mode_and_architecture(self) -> None:
        """Verify tunnel.sh contains strict flags, trap handling, and trycloudflare regex."""
        project_root = Path(__file__).resolve().parent.parent.parent
        script_path = project_root / "scripts" / "tunnel.sh"
        content = script_path.read_text(encoding="utf-8")

        assert content.startswith("#!/usr/bin/env bash")
        assert "set -euo pipefail" in content
        assert "--test" in content
        assert "cloudflared tunnel" in content
        assert "trycloudflare.com" in content
        assert "trap cleanup EXIT INT TERM" in content
