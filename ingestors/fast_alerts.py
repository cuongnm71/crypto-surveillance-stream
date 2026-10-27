"""Sub-second (< 30ms) Fast-Path market alert detector and dispatcher (ADR D13).

Catches single extreme market shocks at the ingestor level without waiting
for Spark 10s micro-batches:
1. Single Whale Trade >= $1,000,000 USD
2. Single Extreme Liquidation >= $500,000 USD

Immediately dispatches alerts to Redis Streams (crypto:alerts:stream),
Redis Pub/Sub (crypto:alerts:live), and Kafka (alerts.events).
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

from common.constants import (
    ALERTS_STREAM_KEY,
    KAFKA_TOPIC_ALERTS,
)
from common.contracts import (
    AlertEvent,
    AlertMetrics,
    AlertSeverity,
    AlertType,
    RawLiquidationEvent,
    RawTradeEvent,
    encode_event,
)
from common.crypto import generate_deterministic_alert_id

if TYPE_CHECKING:
    import redis.asyncio as aioredis
    from aiokafka import AIOKafkaProducer

WHALE_TRADE_THRESHOLD_USD: float = 1_000_000.0
EXTREME_LIQUIDATION_THRESHOLD_USD: float = 500_000.0


def evaluate_trade_for_whale_alert(event: RawTradeEvent) -> AlertEvent | None:
    """Evaluate if a single raw trade qualifies as an extreme whale shock.

    Args:
        event: Validated RawTradeEvent.

    Returns:
        AlertEvent if notional >= $1,000,000 USD, else None.
    """
    notional = event.notional_usd
    if notional < WHALE_TRADE_THRESHOLD_USD:
        return None

    now_ms = int(time.time() * 1000)
    alert_id = generate_deterministic_alert_id(
        symbol=event.symbol,
        alert_type=AlertType.WHALE_TRADE.value,
        window_start=f"{event.event_time}_{event.trade_id}",
    )

    side_str = event.side.value if hasattr(event.side, "value") else str(event.side)
    message = (
        f"🐋 [FAST-PATH] Extreme Whale Trade on {event.symbol}: "
        f"${notional:,.0f} {side_str.upper()} @ {event.price:,.4f}"
    )

    return AlertEvent(
        alert_id=alert_id,
        alert_type=AlertType.WHALE_TRADE,
        severity=AlertSeverity.CRITICAL,
        source="ingestor:fast_path",
        exchange=event.exchange,
        symbol=event.symbol,
        message=message,
        price=event.price,
        metrics=AlertMetrics(notional_usd=notional),
        event_time=event.event_time,
        emitted_at=now_ms,
        is_simulated=event.is_simulated,
    )


def evaluate_liquidation_alert(event: RawLiquidationEvent) -> AlertEvent | None:
    """Evaluate if a single raw liquidation qualifies as an extreme shock.

    Args:
        event: Validated RawLiquidationEvent.

    Returns:
        AlertEvent if notional >= $500,000 USD, else None.
    """
    notional = event.notional_usd
    if notional < EXTREME_LIQUIDATION_THRESHOLD_USD:
        return None

    now_ms = int(time.time() * 1000)
    alert_id = generate_deterministic_alert_id(
        symbol=event.symbol,
        alert_type=AlertType.LIQUIDATION_CASCADE.value,
        window_start=f"{event.event_time}_{event.order_id}",
    )

    side_str = event.side.value if hasattr(event.side, "value") else str(event.side)
    message = (
        f"💀 [FAST-PATH] Extreme Liquidation on {event.symbol}: "
        f"${notional:,.0f} {side_str.upper()} @ {event.price:,.4f}"
    )

    return AlertEvent(
        alert_id=alert_id,
        alert_type=AlertType.LIQUIDATION_CASCADE,
        severity=AlertSeverity.CRITICAL,
        source="ingestor:fast_path",
        exchange=event.exchange,
        symbol=event.symbol,
        message=message,
        price=event.price,
        metrics=AlertMetrics(notional_usd=notional),
        event_time=event.event_time,
        emitted_at=now_ms,
    )


async def dispatch_fast_path_alert(
    alert: AlertEvent,
    redis_client: aioredis.Redis | None = None,
    kafka_producer: AIOKafkaProducer | None = None,
) -> None:
    """Dispatch generated Fast-Path AlertEvent to Redis Stream, Pub/Sub, and Kafka."""
    payload_bytes = encode_event(alert)

    if redis_client is not None:
        pipe = redis_client.pipeline()
        pipe.xadd(
            ALERTS_STREAM_KEY,
            {"data": payload_bytes},
            maxlen=5000,
            approximate=True,
        )
        pipe.publish("crypto:alerts:live", payload_bytes)
        pipe.publish(f"crypto:alerts:{alert.symbol}", payload_bytes)
        await pipe.execute()

    if kafka_producer is not None:
        await kafka_producer.send(
            KAFKA_TOPIC_ALERTS,
            key=alert.symbol.encode("utf-8"),
            value=payload_bytes,
        )
