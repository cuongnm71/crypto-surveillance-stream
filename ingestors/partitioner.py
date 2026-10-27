"""Deterministic partitioner for Kafka trade stream to balance data skew (ADR D7).

Major coins (BTC, ETH, SOL) dominate trading volume (~45% of total events).
This partitioner pins them to dedicated partitions and hashes the remaining
97 altcoins deterministically across all partitions.
"""

from __future__ import annotations

import zlib

TOTAL_PARTITIONS: int = 6

# Fixed partition assignments for top volume liquidity drivers
PINNED_PARTITIONS: dict[str, int] = {
    "BTCUSDT": 0,
    "ETHUSDT": 1,
    "SOLUSDT": 2,
}


def get_trade_partition(symbol: str, total_partitions: int = TOTAL_PARTITIONS) -> int:
    """Return deterministic partition index for a given trading symbol.

    Args:
        symbol: Trading pair symbol (e.g., 'BTCUSDT', 'DOGEUSDT').
        total_partitions: Total number of partitions available (default 6).

    Returns:
        Integer partition index in range [0, total_partitions - 1].
    """
    clean_symbol = symbol.upper().strip()
    if clean_symbol in PINNED_PARTITIONS:
        return PINNED_PARTITIONS[clean_symbol]

    # CRC32 deterministic hash across all available partitions
    hash_val = zlib.crc32(clean_symbol.encode("utf-8"))
    return hash_val % total_partitions
