"""High-performance zero-copy data contracts using msgspec.Struct."""

from __future__ import annotations

import time
from enum import StrEnum
from typing import Final

import msgspec

from common.constants import TOP_100_SYMBOLS_SET, normalize_symbol

MAX_CLOCK_DRIFT_MS: Final[int] = 60_000
SCHEMA_VERSION: Final[int] = 1
DEFAULT_LOGIC_VERSION: Final[str] = "surveillance@1.0.0"


class TradeSide(StrEnum):
    """Normalized aggressive taker side of a trade or liquidation order."""

    BUY = "buy"
    SELL = "sell"


class AlertType(StrEnum):
    """Market manipulation and surveillance anomaly categories."""

    ZSCORE_SPIKE = "ZSCORE_SPIKE"
    VOLUME_ANOMALY = "VOLUME_ANOMALY"
    WHALE_TRADE = "WHALE_TRADE"
    LIQUIDATION_CASCADE = "LIQUIDATION_CASCADE"


class AlertSeverity(StrEnum):
    """Alert severity levels for dashboard filtering and Telegram routing."""

    INFO = "INFO"
    WARNING = "WARNING"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


def infer_taker_side(is_buyer_maker: bool) -> TradeSide:
    """Infer the aggressive taker side from Binance aggTrade 'm' (is_buyer_maker) flag.

    - When is_buyer_maker is True: Buyer placed passive limit order (Maker), so Taker is Seller.
    - When is_buyer_maker is False: Seller placed passive limit order (Maker), so Taker is Buyer.
    """
    return TradeSide.SELL if is_buyer_maker else TradeSide.BUY


def _validate_market_event_fields(
    *,
    symbol: str,
    price: float,
    qty: float,
    notional_usd: float,
    event_time: int,
    reference_time_ms: int | None = None,
) -> None:
    """Validate common numeric, symbol, and clock-drift constraints for market events."""
    if price <= 0.0:
        raise ValueError(f"Invalid price {price}: must be > 0")
    if qty <= 0.0:
        raise ValueError(f"Invalid qty {qty}: must be > 0")
    if notional_usd <= 0.0:
        raise ValueError(f"Invalid notional_usd {notional_usd}: must be > 0")

    norm_sym = normalize_symbol(symbol)
    if norm_sym not in TOP_100_SYMBOLS_SET:
        raise ValueError(f"Unsupported symbol '{symbol}' not in Top 100 catalog")

    now_ms = reference_time_ms if reference_time_ms is not None else int(time.time() * 1000)
    if abs(now_ms - event_time) > MAX_CLOCK_DRIFT_MS:
        raise ValueError(
            f"Stale or future event_time {event_time} "
            f"(now={now_ms}, max_drift={MAX_CLOCK_DRIFT_MS}ms)"
        )


class RawTradeEvent(msgspec.Struct, kw_only=True):
    """Canonical spot/futures trade event published to Kafka topic `md.trades`."""

    exchange: str
    market: str
    symbol: str
    trade_id: str
    price: float
    qty: float
    notional_usd: float
    side: TradeSide
    event_time: int
    ingested_at: int
    v: int = SCHEMA_VERSION
    is_simulated: int = 0

    def validate(self, reference_time_ms: int | None = None) -> None:
        """Validate trade constraints at the ingestion boundary before publishing to Kafka."""
        _validate_market_event_fields(
            symbol=self.symbol,
            price=self.price,
            qty=self.qty,
            notional_usd=self.notional_usd,
            event_time=self.event_time,
            reference_time_ms=reference_time_ms,
        )


class RawLiquidationEvent(msgspec.Struct, kw_only=True):
    """Canonical forced liquidation event published to Kafka topic `md.liquidations`."""

    exchange: str
    market: str
    symbol: str
    order_id: str
    price: float
    qty: float
    notional_usd: float
    side: TradeSide
    event_time: int
    ingested_at: int
    v: int = SCHEMA_VERSION
    is_simulated: int = 0

    def validate(self, reference_time_ms: int | None = None) -> None:
        """Validate liquidation event constraints at the ingestion boundary."""
        _validate_market_event_fields(
            symbol=self.symbol,
            price=self.price,
            qty=self.qty,
            notional_usd=self.notional_usd,
            event_time=self.event_time,
            reference_time_ms=reference_time_ms,
        )


class CandleEvent(msgspec.Struct, kw_only=True):
    """Watermarked 1m OHLCV + Order Flow candle event (`candles.1m` & `silver_candles_1m`)."""

    exchange: str
    symbol: str
    timeframe: str
    open_time: int
    close_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    quote_volume: float
    trade_count: int
    buy_volume: float
    sell_volume: float
    volume_delta: float
    is_closed: bool
    emitted_at: int
    v: int = SCHEMA_VERSION


class AlertMetrics(msgspec.Struct, kw_only=True):
    """Statistical context attached to a market surveillance AlertEvent."""

    return_pct: float = 0.0
    zscore: float = 0.0
    sigma_pct: float = 0.0
    ewma_vol_ratio: float = 0.0
    notional_usd: float = 0.0


class AlertEvent(msgspec.Struct, kw_only=True):
    """Market surveillance anomaly event (`alerts.events`, `silver_alert_events`, Redis Stream)."""

    alert_id: str
    alert_type: AlertType
    severity: AlertSeverity
    source: str
    exchange: str
    symbol: str
    message: str
    price: float
    metrics: AlertMetrics
    event_time: int
    emitted_at: int
    v: int = SCHEMA_VERSION
    logic_version: str = DEFAULT_LOGIC_VERSION
    is_simulated: int = 0


class DLQEvent(msgspec.Struct, kw_only=True):
    """Dead-Letter Queue event for malformed or schema-violating incoming payloads (`dlq`)."""

    source: str
    raw_payload: str
    error_reason: str
    failed_at: int
    v: int = SCHEMA_VERSION


# Pre-allocated zero-copy msgspec JSON encoder and typed decoders
_encoder: Final[msgspec.json.Encoder] = msgspec.json.Encoder()
_trade_decoder: Final[msgspec.json.Decoder[RawTradeEvent]] = msgspec.json.Decoder(RawTradeEvent)
_liq_decoder: Final[msgspec.json.Decoder[RawLiquidationEvent]] = msgspec.json.Decoder(
    RawLiquidationEvent
)
_candle_decoder: Final[msgspec.json.Decoder[CandleEvent]] = msgspec.json.Decoder(CandleEvent)
_alert_decoder: Final[msgspec.json.Decoder[AlertEvent]] = msgspec.json.Decoder(AlertEvent)
_dlq_decoder: Final[msgspec.json.Decoder[DLQEvent]] = msgspec.json.Decoder(DLQEvent)


def encode_event(event: msgspec.Struct) -> bytes:
    """Encode any msgspec.Struct event into UTF-8 JSON bytes using a shared encoder."""
    return _encoder.encode(event)


def decode_trade_event(payload: bytes | str) -> RawTradeEvent:
    """Decode UTF-8 JSON bytes or string into a validated RawTradeEvent."""
    return _trade_decoder.decode(payload)


def decode_liquidation_event(payload: bytes | str) -> RawLiquidationEvent:
    """Decode UTF-8 JSON bytes or string into a validated RawLiquidationEvent."""
    return _liq_decoder.decode(payload)


def decode_candle_event(payload: bytes | str) -> CandleEvent:
    """Decode UTF-8 JSON bytes or string into a validated CandleEvent."""
    return _candle_decoder.decode(payload)


def decode_alert_event(payload: bytes | str) -> AlertEvent:
    """Decode UTF-8 JSON bytes or string into a validated AlertEvent."""
    return _alert_decoder.decode(payload)


def decode_dlq_event(payload: bytes | str) -> DLQEvent:
    """Decode UTF-8 JSON bytes or string into a validated DLQEvent."""
    return _dlq_decoder.decode(payload)
