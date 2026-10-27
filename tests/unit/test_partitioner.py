"""Unit tests for deterministic trade partitioner."""

from __future__ import annotations

from common.constants import TOP_100_SYMBOLS
from ingestors.partitioner import TOTAL_PARTITIONS, get_trade_partition


class TestPartitioner:
    """Test suite for get_trade_partition."""

    def test_pinned_partitions_exact_match(self) -> None:
        """Verify BTC, ETH, SOL are strictly assigned to partitions 0, 1, 2."""
        assert get_trade_partition("BTCUSDT") == 0
        assert get_trade_partition("btcusdt") == 0
        assert get_trade_partition("ETHUSDT") == 1
        assert get_trade_partition("ethusdt") == 1
        assert get_trade_partition("SOLUSDT") == 2
        assert get_trade_partition("solusdt") == 2

    def test_deterministic_consistency_across_runs(self) -> None:
        """Verify partition index is strictly idempotent across multiple invocations."""
        for symbol in ["DOGEUSDT", "ADAUSDT", "XRPUSDT", "PEPEUSDT"]:
            expected = get_trade_partition(symbol)
            for _ in range(50):
                assert get_trade_partition(symbol) == expected

    def test_all_100_symbols_within_partition_bounds(self) -> None:
        """Verify all Top 100 symbols produce a valid partition index [0, 5]."""
        partition_counts: dict[int, int] = {i: 0 for i in range(TOTAL_PARTITIONS)}
        for symbol in TOP_100_SYMBOLS:
            partition = get_trade_partition(symbol)
            assert 0 <= partition < TOTAL_PARTITIONS
            partition_counts[partition] += 1

        # Ensure no partition is completely starved among the 100 symbols
        for p, count in partition_counts.items():
            assert count >= 5, f"Partition {p} has only {count} symbols assigned"

    def test_custom_partition_counts(self) -> None:
        """Verify behaviour when custom partition counts are supplied."""
        # Pinned symbols still map to their assigned indices
        assert get_trade_partition("BTCUSDT", total_partitions=12) == 0
        # Altcoins hash within custom bounds
        alt_partition = get_trade_partition("DOGEUSDT", total_partitions=12)
        assert 0 <= alt_partition < 12
