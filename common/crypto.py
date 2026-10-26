"""Deterministic cryptographic identifiers (UUIDv5) for idempotent stream processing."""

from __future__ import annotations

import uuid
from typing import Final

# Dedicated namespace UUID for Crypto Market Surveillance Platform
SURVEILLANCE_NAMESPACE_UUID: Final[uuid.UUID] = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


def generate_deterministic_alert_id(
    symbol: str,
    alert_type: str,
    window_start: int | str,
) -> str:
    """Generate a deterministic UUIDv5 string from (symbol, alert_type, window_start).

    Guarantees idempotency across PySpark checkpoint restarts and replays so that
    re-processing the same window produces the exact same primary key in ClickHouse
    ReplacingMergeTree (`silver_alert_events`).
    """
    norm_symbol = symbol.strip().upper()
    norm_type = str(alert_type).strip().upper()
    norm_window = str(window_start).strip()

    if not norm_symbol or not norm_type or not norm_window:
        raise ValueError("symbol, alert_type, and window_start must be non-empty")

    canonical_name = f"crypto-surveillance:alert:v1:{norm_symbol}:{norm_type}:{norm_window}"
    return str(uuid.uuid5(SURVEILLANCE_NAMESPACE_UUID, canonical_name))
