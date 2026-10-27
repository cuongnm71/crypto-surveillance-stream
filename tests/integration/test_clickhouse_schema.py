"""Integration tests validating ClickHouse Medallion schemas, Kafka engine, and rollups."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from typing import Final

import clickhouse_connect
import pytest
from aiokafka import AIOKafkaProducer

from clickhouse.migrate import run_migrations
from common.contracts import RawTradeEvent, TradeSide, encode_event

TEST_SYMBOL: Final[str] = "BTCUSDT"
KAFKA_BOOTSTRAP_SERVERS: Final[str] = "localhost:9094"


class TestClickHouseSchemaIntegration:
    """Test suite running end-to-end against live ClickHouse and Kafka containers."""

    @pytest.fixture(autouse=True)
    def setup_database(self) -> None:
        """Ensure all migrations are applied and tables cleaned prior to running test cases."""
        run_migrations()
        client = clickhouse_connect.get_client(host="localhost", port=8123, database="crypto")
        client.command("TRUNCATE TABLE IF EXISTS crypto.silver_candles_1m")
        client.command("TRUNCATE TABLE IF EXISTS crypto.silver_candles_5m")
        client.command("TRUNCATE TABLE IF EXISTS crypto.silver_candles_15m")
        client.command("TRUNCATE TABLE IF EXISTS crypto.silver_candles_1h")
        client.command("TRUNCATE TABLE IF EXISTS crypto.silver_candles_1d")

    def test_migration_runner_is_idempotent(self) -> None:
        """Calling run_migrations when schemas are up-to-date returns 0 new migrations."""
        new_migrations = run_migrations()
        assert len(new_migrations) == 0

    @pytest.mark.asyncio
    async def test_native_kafka_engine_ingests_to_bronze_trades(self) -> None:
        """Verify ClickHouse Native Kafka Engine automatically ingests from topic md.trades."""
        client = clickhouse_connect.get_client(host="localhost", port=8123, database="crypto")
        unique_trade_id = f"test_trade_{int(time.time() * 1000)}"
        now_ms = int(time.time() * 1000)

        trade_event = RawTradeEvent(
            exchange="binance",
            market="spot",
            symbol=TEST_SYMBOL,
            trade_id=unique_trade_id,
            price=68500.50,
            qty=1.25,
            notional_usd=68500.50 * 1.25,
            side=TradeSide.BUY,
            event_time=now_ms,
            ingested_at=now_ms,
            is_simulated=1,
        )

        producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
        await producer.start()
        try:
            payload = encode_event(trade_event)
            await producer.send_and_wait(
                "md.trades",
                key=f"binance:{TEST_SYMBOL}".encode(),
                value=payload,
            )
        finally:
            await producer.stop()

        # Poll ClickHouse bronze_trades until Kafka engine flushes batch (usually 500ms - 2s)
        max_retries = 20
        row_found = False
        for _ in range(max_retries):
            result = client.query(
                "SELECT trade_id, symbol, price, qty, side, is_simulated "
                "FROM crypto.bronze_trades WHERE trade_id = %(trade_id)s",
                parameters={"trade_id": unique_trade_id},
            )
            if len(result.result_rows) > 0:
                row_found = True
                row = result.result_rows[0]
                assert row[0] == unique_trade_id
                assert row[1] == TEST_SYMBOL
                assert abs(float(row[2]) - 68500.50) < 1e-4
                assert abs(float(row[3]) - 1.25) < 1e-4
                assert row[4] == "buy"
                assert row[5] == 1
                break
            await asyncio.sleep(0.5)

        assert row_found, (
            f"Trade {unique_trade_id} was not ingested into bronze_trades within timeout"
        )

    def test_silver_candles_1m_and_materialized_views_rollups(self) -> None:
        """Verify inserting into silver_candles_1m automatically rolls up to 5m, 15m, 1h, and 1d."""
        client = clickhouse_connect.get_client(host="localhost", port=8123, database="crypto")
        symbol = "ETHUSDT"

        # 3 sequential 1m candles within the 02:00 - 02:05 interval
        candles_1m = [
            # minute 0: open=3400, high=3420, low=3390, close=3410, volume=10.0
            (
                "binance",
                symbol,
                "1m",
                datetime(2026, 10, 27, 2, 0, 0, tzinfo=UTC),
                datetime(2026, 10, 27, 2, 0, 59, 999000, tzinfo=UTC),
                3400.0,
                3420.0,
                3390.0,
                3410.0,
                10.0,
                34100.0,
                50,
                6.0,
                4.0,
                2.0,
                1,
                datetime(2026, 10, 27, 2, 1, 0, tzinfo=UTC),
                datetime(2026, 10, 27, 2, 1, 0, tzinfo=UTC),
                1,
            ),
            # minute 1: open=3410, high=3450, low=3405, close=3440, volume=15.0
            (
                "binance",
                symbol,
                "1m",
                datetime(2026, 10, 27, 2, 1, 0, tzinfo=UTC),
                datetime(2026, 10, 27, 2, 1, 59, 999000, tzinfo=UTC),
                3410.0,
                3450.0,
                3405.0,
                3440.0,
                15.0,
                51600.0,
                75,
                9.0,
                6.0,
                3.0,
                1,
                datetime(2026, 10, 27, 2, 2, 0, tzinfo=UTC),
                datetime(2026, 10, 27, 2, 2, 0, tzinfo=UTC),
                1,
            ),
            # minute 2: open=3440, high=3445, low=3380, close=3385, volume=25.0
            (
                "binance",
                symbol,
                "1m",
                datetime(2026, 10, 27, 2, 2, 0, tzinfo=UTC),
                datetime(2026, 10, 27, 2, 2, 59, 999000, tzinfo=UTC),
                3440.0,
                3445.0,
                3380.0,
                3385.0,
                25.0,
                84625.0,
                120,
                10.0,
                15.0,
                -5.0,
                1,
                datetime(2026, 10, 27, 2, 3, 0, tzinfo=UTC),
                datetime(2026, 10, 27, 2, 3, 0, tzinfo=UTC),
                1,
            ),
        ]

        columns = [
            "exchange",
            "symbol",
            "timeframe",
            "open_time",
            "close_time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "quote_volume",
            "trade_count",
            "buy_volume",
            "sell_volume",
            "volume_delta",
            "is_closed",
            "emitted_at",
            "updated_at",
            "v",
        ]
        client.insert("silver_candles_1m", candles_1m, column_names=columns)

        # Query 5-minute rolled up candle
        result_5m = client.query(
            """
            SELECT
                symbol,
                open_time,
                argMinMerge(open) AS open,
                maxMerge(high) AS high,
                minMerge(low) AS low,
                argMaxMerge(close) AS close,
                sumMerge(volume) AS volume,
                sumMerge(trade_count) AS trade_count,
                sumMerge(volume_delta) AS volume_delta
            FROM crypto.silver_candles_5m
            WHERE symbol = 'ETHUSDT' AND open_time = '2026-10-27 02:00:00'
            GROUP BY symbol, open_time
            """
        )

        assert len(result_5m.result_rows) == 1
        r5 = result_5m.result_rows[0]
        assert r5[0] == "ETHUSDT"
        assert abs(float(r5[2]) - 3400.0) < 1e-4  # open of minute 0
        assert abs(float(r5[3]) - 3450.0) < 1e-4  # max high from minute 1
        assert abs(float(r5[4]) - 3380.0) < 1e-4  # min low from minute 2
        assert abs(float(r5[5]) - 3385.0) < 1e-4  # close of minute 2
        assert abs(float(r5[6]) - 50.0) < 1e-4  # total volume 10 + 15 + 25 = 50.0
        assert r5[7] == 245  # total trades 50 + 75 + 120 = 245
        assert abs(float(r5[8]) - 0.0) < 1e-4  # volume delta 2 + 3 - 5 = 0.0

        # Query 1-hour rolled up candle
        result_1h = client.query(
            """
            SELECT
                symbol,
                open_time,
                argMinMerge(open) AS open,
                maxMerge(high) AS high,
                minMerge(low) AS low,
                argMaxMerge(close) AS close,
                sumMerge(volume) AS volume
            FROM crypto.silver_candles_1h
            WHERE symbol = 'ETHUSDT' AND open_time = '2026-10-27 02:00:00'
            GROUP BY symbol, open_time
            """
        )
        assert len(result_1h.result_rows) == 1
        r1h = result_1h.result_rows[0]
        assert abs(float(r1h[2]) - 3400.0) < 1e-4
        assert abs(float(r1h[3]) - 3450.0) < 1e-4
        assert abs(float(r1h[4]) - 3380.0) < 1e-4
        assert abs(float(r1h[5]) - 3385.0) < 1e-4
        assert abs(float(r1h[6]) - 50.0) < 1e-4

    def test_silver_alert_events_replacing_merge_tree(self) -> None:
        """Verify silver_alert_events stores anomaly records and deduplicates by alert_id."""
        client = clickhouse_connect.get_client(host="localhost", port=8123, database="crypto")
        alert_id = f"test_alert_{int(time.time() * 1000)}"
        event_time = datetime(2026, 10, 27, 2, 30, 0, tzinfo=UTC)
        initial_emitted = datetime(2026, 10, 27, 2, 30, 1, tzinfo=UTC)
        updated_emitted = datetime(2026, 10, 27, 2, 30, 5, tzinfo=UTC)

        initial_row = [
            alert_id,
            "WHALE_TRADE",
            "CRITICAL",
            "fast_path",
            "binance",
            "BTCUSDT",
            "Whale buy: 50.0 BTC ($3.4M)",
            68000.0,
            0.0,
            0.0,
            0.0,
            0.0,
            3400000.0,
            event_time,
            initial_emitted,
            "surveillance@1.0.0",
            0,
            1,
        ]
        columns = [
            "alert_id",
            "alert_type",
            "severity",
            "source",
            "exchange",
            "symbol",
            "message",
            "price",
            "return_pct",
            "zscore",
            "sigma_pct",
            "ewma_vol_ratio",
            "notional_usd",
            "event_time",
            "emitted_at",
            "logic_version",
            "is_simulated",
            "v",
        ]
        client.insert("silver_alert_events", [initial_row], column_names=columns)

        # Insert updated row with same alert_id and event_time but later emitted_at
        updated_row = list(initial_row)
        updated_row[14] = updated_emitted
        updated_row[6] = "Whale buy updated: 50.0 BTC ($3.4M)"
        client.insert("silver_alert_events", [updated_row], column_names=columns)

        # Using FINAL to verify ReplacingMergeTree deduplication
        res = client.query(
            """
            SELECT alert_id, symbol, message, notional_usd
            FROM crypto.silver_alert_events FINAL
            WHERE symbol = %(symbol)s AND alert_id = %(alert_id)s
            """,
            parameters={"symbol": "BTCUSDT", "alert_id": alert_id},
        )
        assert len(res.result_rows) == 1
        assert res.result_rows[0][0] == alert_id
        assert res.result_rows[0][2] == "Whale buy updated: 50.0 BTC ($3.4M)"
        assert abs(float(res.result_rows[0][3]) - 3400000.0) < 1e-4

    def test_silver_recon_audit_log_inserts_and_queries(self) -> None:
        """Verify silver_recon_audit_log stores reconciliation drift check records."""
        client = clickhouse_connect.get_client(host="localhost", port=8123, database="crypto")
        recon_id = f"recon_{int(time.time() * 1000)}"
        candle_time = datetime(2026, 10, 27, 1, 0, 0, tzinfo=UTC)
        recon_date = datetime(2026, 10, 27, 0, 0, 0).date()

        row = [
            recon_id,
            "SOLUSDT",
            recon_date,
            "1h",
            candle_time,
            185.50,
            185.52,
            0.0108,
            125000.0,
            125020.0,
            0.0160,
            "PASSED",
            0,
            datetime.now(UTC),
        ]
        columns = [
            "recon_id",
            "symbol",
            "recon_date",
            "timeframe",
            "candle_time",
            "clickhouse_close",
            "binance_close",
            "close_drift_pct",
            "clickhouse_volume",
            "binance_volume",
            "volume_drift_pct",
            "status",
            "auto_healed",
            "reconciled_at",
        ]
        client.insert("silver_recon_audit_log", [row], column_names=columns)

        res = client.query(
            """
            SELECT recon_id, symbol, timeframe, status, auto_healed
            FROM crypto.silver_recon_audit_log
            WHERE recon_id = %(recon_id)s
            """,
            parameters={"recon_id": recon_id},
        )
        assert len(res.result_rows) == 1
        assert res.result_rows[0][0] == recon_id
        assert res.result_rows[0][1] == "SOLUSDT"
        assert res.result_rows[0][2] == "1h"
        assert res.result_rows[0][3] == "PASSED"
        assert res.result_rows[0][4] == 0
