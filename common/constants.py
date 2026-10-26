"""System-wide constants, Top-100 symbol catalog, liquidity tiers, and Redis/Kafka naming."""

from __future__ import annotations

from enum import StrEnum
from typing import Final

import msgspec

# ------------------------------------------------------------------------------
# Kafka Topics (ADR D1 / Section 4.2)
# ------------------------------------------------------------------------------
KAFKA_TOPIC_TRADES: Final[str] = "md.trades"
KAFKA_TOPIC_TICKERS: Final[str] = "md.tickers"
KAFKA_TOPIC_LIQUIDATIONS: Final[str] = "md.liquidations"
KAFKA_TOPIC_CANDLES_1M: Final[str] = "candles.1m"
KAFKA_TOPIC_ALERTS: Final[str] = "alerts.events"
KAFKA_TOPIC_DLQ: Final[str] = "dlq"

ALL_KAFKA_TOPICS: Final[tuple[str, ...]] = (
    KAFKA_TOPIC_TRADES,
    KAFKA_TOPIC_TICKERS,
    KAFKA_TOPIC_LIQUIDATIONS,
    KAFKA_TOPIC_CANDLES_1M,
    KAFKA_TOPIC_ALERTS,
    KAFKA_TOPIC_DLQ,
)

# ------------------------------------------------------------------------------
# Redis Key Templates & Builders (Section 4.4.B)
# ------------------------------------------------------------------------------
TICKER_KEY_TEMPLATE: Final[str] = "crypto:ticker:{symbol}"
PRICE_PUBSUB_KEY_TEMPLATE: Final[str] = "crypto:px:{symbol}"
CANDLES_1M_KEY_TEMPLATE: Final[str] = "crypto:candles:{symbol}:1m"
ALERTS_STREAM_KEY: Final[str] = "crypto:alerts:stream"
TELEMETRY_METRICS_KEY: Final[str] = "crypto:telemetry:metrics"
INGEST_CONN_BINANCE_KEY: Final[str] = "crypto:ingest:conn:binance"
ALERT_COOLDOWN_KEY_TEMPLATE: Final[str] = "crypto:alert:cooldown:{symbol}"


class RedisKeys:
    """Helper methods for formatting canonical Redis keys."""

    @staticmethod
    def ticker(symbol: str) -> str:
        """Return Redis hash key for 4Hz latest ticker snapshot."""
        return TICKER_KEY_TEMPLATE.format(symbol=normalize_symbol(symbol))

    @staticmethod
    def price_pubsub(symbol: str) -> str:
        """Return Redis Pub/Sub channel key for 4Hz live price updates."""
        return PRICE_PUBSUB_KEY_TEMPLATE.format(symbol=normalize_symbol(symbol))

    @staticmethod
    def candles_1m(symbol: str) -> str:
        """Return Redis Sorted Set key storing hot 300 1-minute candles."""
        return CANDLES_1M_KEY_TEMPLATE.format(symbol=normalize_symbol(symbol))

    @staticmethod
    def alerts_stream() -> str:
        """Return Redis Stream key for market surveillance alerts."""
        return ALERTS_STREAM_KEY

    @staticmethod
    def telemetry_metrics() -> str:
        """Return Redis hash key for pipeline health telemetry."""
        return TELEMETRY_METRICS_KEY

    @staticmethod
    def ingest_conn_binance() -> str:
        """Return Redis counter key for Binance WebSocket connection guard."""
        return INGEST_CONN_BINANCE_KEY

    @staticmethod
    def alert_cooldown(symbol: str) -> str:
        """Return Redis key for Telegram per-symbol cooldown lock."""
        return ALERT_COOLDOWN_KEY_TEMPLATE.format(symbol=normalize_symbol(symbol))


# ------------------------------------------------------------------------------
# Liquidity Tiers & Market Categories (ADR D7 / Section 4.3)
# ------------------------------------------------------------------------------
class LiquidityTier(StrEnum):
    """Liquidity tier classification for dynamic whale & anomaly thresholds."""

    TIER_1 = "TIER_1"  # BTC, ETH, SOL, BNB (Single Whale >= $500k, Cluster >= $200k)
    TIER_2 = "TIER_2"  # Top 5-30 Large Caps (Single Whale >= $200k, Cluster >= $80k)
    TIER_3 = "TIER_3"  # Top 31-100 Mid Caps / Memes (Single Whale >= $75k, Cluster >= $30k)


class MarketCategory(StrEnum):
    """Market sector classification for Kimball dim_symbols."""

    LAYER_1 = "LAYER_1"
    LAYER_2 = "LAYER_2"
    DEFI = "DEFI"
    INFRA = "INFRA"
    AI = "AI"
    GAMING = "GAMING"
    MEME = "MEME"


class SymbolMetadata(msgspec.Struct, frozen=True):
    """Dimensional metadata for a tracked cryptocurrency pair (dim_symbols)."""

    symbol: str
    base_asset: str
    quote_asset: str
    liquidity_tier: LiquidityTier
    category: MarketCategory
    single_whale_threshold_usd: float
    cluster_whale_threshold_usd: float


TIER_THRESHOLDS_USD: Final[dict[LiquidityTier, tuple[float, float]]] = {
    LiquidityTier.TIER_1: (500_000.0, 200_000.0),
    LiquidityTier.TIER_2: (200_000.0, 80_000.0),
    LiquidityTier.TIER_3: (75_000.0, 30_000.0),
}

# Fast-path emergency shock thresholds at Ingestor (< 30ms)
FAST_PATH_WHALE_USD: Final[float] = 1_000_000.0
FAST_PATH_LIQUIDATION_USD: Final[float] = 500_000.0

# ------------------------------------------------------------------------------
# Top 100 Symbols Definition (4 Tier-1 + 26 Tier-2 + 70 Tier-3 = 100)
# ------------------------------------------------------------------------------
_TIER_1_DEFS: Final[tuple[tuple[str, MarketCategory], ...]] = (
    ("BTCUSDT", MarketCategory.LAYER_1),
    ("ETHUSDT", MarketCategory.LAYER_1),
    ("SOLUSDT", MarketCategory.LAYER_1),
    ("BNBUSDT", MarketCategory.LAYER_1),
)

_TIER_2_DEFS: Final[tuple[tuple[str, MarketCategory], ...]] = (
    ("XRPUSDT", MarketCategory.LAYER_1),
    ("DOGEUSDT", MarketCategory.MEME),
    ("ADAUSDT", MarketCategory.LAYER_1),
    ("TRXUSDT", MarketCategory.LAYER_1),
    ("AVAXUSDT", MarketCategory.LAYER_1),
    ("TONUSDT", MarketCategory.LAYER_1),
    ("LINKUSDT", MarketCategory.INFRA),
    ("DOTUSDT", MarketCategory.LAYER_1),
    ("BCHUSDT", MarketCategory.LAYER_1),
    ("NEARUSDT", MarketCategory.LAYER_1),
    ("SUIUSDT", MarketCategory.LAYER_1),
    ("LTCUSDT", MarketCategory.LAYER_1),
    ("UNIUSDT", MarketCategory.DEFI),
    ("APTUSDT", MarketCategory.LAYER_1),
    ("ICPUSDT", MarketCategory.LAYER_1),
    ("ETCUSDT", MarketCategory.LAYER_1),
    ("RENDERUSDT", MarketCategory.AI),
    ("FETUSDT", MarketCategory.AI),
    ("ATOMUSDT", MarketCategory.LAYER_1),
    ("FILUSDT", MarketCategory.INFRA),
    ("ARBUSDT", MarketCategory.LAYER_2),
    ("OPUSDT", MarketCategory.LAYER_2),
    ("INJUSDT", MarketCategory.DEFI),
    ("STXUSDT", MarketCategory.LAYER_2),
    ("AAVEUSDT", MarketCategory.DEFI),
    ("POLUSDT", MarketCategory.LAYER_2),
)

_TIER_3_DEFS: Final[tuple[tuple[str, MarketCategory], ...]] = (
    ("PEPEUSDT", MarketCategory.MEME),
    ("SHIBUSDT", MarketCategory.MEME),
    ("WIFUSDT", MarketCategory.MEME),
    ("BONKUSDT", MarketCategory.MEME),
    ("FLOKIUSDT", MarketCategory.MEME),
    ("TIAUSDT", MarketCategory.INFRA),
    ("SEIUSDT", MarketCategory.LAYER_1),
    ("TAOUSDT", MarketCategory.AI),
    ("IMXUSDT", MarketCategory.GAMING),
    ("VETUSDT", MarketCategory.LAYER_1),
    ("MKRUSDT", MarketCategory.DEFI),
    ("GRTUSDT", MarketCategory.INFRA),
    ("THETAUSDT", MarketCategory.INFRA),
    ("RUNEUSDT", MarketCategory.DEFI),
    ("ARUSDT", MarketCategory.INFRA),
    ("ALGOUSDT", MarketCategory.LAYER_1),
    ("LDOUSDT", MarketCategory.DEFI),
    ("JUPUSDT", MarketCategory.DEFI),
    ("FLOWUSDT", MarketCategory.LAYER_1),
    ("PYTHUSDT", MarketCategory.INFRA),
    ("ENAUSDT", MarketCategory.DEFI),
    ("ONDOUSDT", MarketCategory.DEFI),
    ("PENDLEUSDT", MarketCategory.DEFI),
    ("GALAUSDT", MarketCategory.GAMING),
    ("BEAMXUSDT", MarketCategory.GAMING),
    ("ORDIUSDT", MarketCategory.MEME),
    ("WLDUSDT", MarketCategory.AI),
    ("STRKUSDT", MarketCategory.LAYER_2),
    ("AXSUSDT", MarketCategory.GAMING),
    ("SANDUSDT", MarketCategory.GAMING),
    ("MANAUSDT", MarketCategory.GAMING),
    ("EOSUSDT", MarketCategory.LAYER_1),
    ("XTZUSDT", MarketCategory.LAYER_1),
    ("NEOUSDT", MarketCategory.LAYER_1),
    ("KAVAUSDT", MarketCategory.LAYER_1),
    ("MINAUSDT", MarketCategory.LAYER_1),
    ("EGLDUSDT", MarketCategory.LAYER_1),
    ("CHZUSDT", MarketCategory.GAMING),
    ("CFXUSDT", MarketCategory.LAYER_1),
    ("SNXUSDT", MarketCategory.DEFI),
    ("CRVUSDT", MarketCategory.DEFI),
    ("DYDXUSDT", MarketCategory.DEFI),
    ("COMPUSDT", MarketCategory.DEFI),
    ("1INCHUSDT", MarketCategory.DEFI),
    ("ENSUSDT", MarketCategory.INFRA),
    ("BLURUSDT", MarketCategory.DEFI),
    ("APEUSDT", MarketCategory.GAMING),
    ("GMTUSDT", MarketCategory.GAMING),
    ("ROSEUSDT", MarketCategory.LAYER_1),
    ("ZILUSDT", MarketCategory.LAYER_1),
    ("IOTAUSDT", MarketCategory.LAYER_1),
    ("QTUMUSDT", MarketCategory.LAYER_1),
    ("ZECUSDT", MarketCategory.LAYER_1),
    ("DASHUSDT", MarketCategory.LAYER_1),
    ("CELOUSDT", MarketCategory.LAYER_1),
    ("ONEUSDT", MarketCategory.LAYER_1),
    ("HOTUSDT", MarketCategory.INFRA),
    ("ENJUSDT", MarketCategory.GAMING),
    ("BATUSDT", MarketCategory.INFRA),
    ("ANKRUSDT", MarketCategory.INFRA),
    ("ZRXUSDT", MarketCategory.DEFI),
    ("SUSHIUSDT", MarketCategory.DEFI),
    ("YFIUSDT", MarketCategory.DEFI),
    ("GMXUSDT", MarketCategory.DEFI),
    ("CAKEUSDT", MarketCategory.DEFI),
    ("WOOUSDT", MarketCategory.DEFI),
    ("JASMYUSDT", MarketCategory.INFRA),
    ("NOTUSDT", MarketCategory.GAMING),
    ("IOUSDT", MarketCategory.AI),
    ("ZKUSDT", MarketCategory.LAYER_2),
)


def _build_catalog() -> dict[str, SymbolMetadata]:
    catalog: dict[str, SymbolMetadata] = {}
    tier_Mapping: tuple[tuple[LiquidityTier, tuple[tuple[str, MarketCategory], ...]], ...] = (
        (LiquidityTier.TIER_1, _TIER_1_DEFS),
        (LiquidityTier.TIER_2, _TIER_2_DEFS),
        (LiquidityTier.TIER_3, _TIER_3_DEFS),
    )
    for tier, defs in tier_Mapping:
        single_usd, cluster_usd = TIER_THRESHOLDS_USD[tier]
        for sym, category in defs:
            base = sym.removesuffix("USDT")
            catalog[sym] = SymbolMetadata(
                symbol=sym,
                base_asset=base,
                quote_asset="USDT",
                liquidity_tier=tier,
                category=category,
                single_whale_threshold_usd=single_usd,
                cluster_whale_threshold_usd=cluster_usd,
            )
    return catalog


SYMBOL_CATALOG: Final[dict[str, SymbolMetadata]] = _build_catalog()

TIER_1_SYMBOLS: Final[tuple[str, ...]] = tuple(sym for sym, _ in _TIER_1_DEFS)
TIER_2_SYMBOLS: Final[tuple[str, ...]] = tuple(sym for sym, _ in _TIER_2_DEFS)
TIER_3_SYMBOLS: Final[tuple[str, ...]] = tuple(sym for sym, _ in _TIER_3_DEFS)
TOP_100_SYMBOLS: Final[tuple[str, ...]] = TIER_1_SYMBOLS + TIER_2_SYMBOLS + TIER_3_SYMBOLS
TOP_100_SYMBOLS_SET: Final[frozenset[str]] = frozenset(TOP_100_SYMBOLS)

# Binance USD-M Futures 1000-multiplier contract normalization mapping
FUTURES_SYMBOL_NORMALIZATION: Final[dict[str, str]] = {
    "1000PEPEUSDT": "PEPEUSDT",
    "1000SHIBUSDT": "SHIBUSDT",
    "1000BONKUSDT": "BONKUSDT",
    "1000FLOKIUSDT": "FLOKIUSDT",
}


def normalize_symbol(symbol: str) -> str:
    """Normalize exchange symbol to uppercase Spot format (e.g. 1000PEPEUSDT -> PEPEUSDT)."""
    cleaned = symbol.strip().upper()
    return FUTURES_SYMBOL_NORMALIZATION.get(cleaned, cleaned)


def get_symbol_metadata(symbol: str) -> SymbolMetadata:
    """Return SymbolMetadata for a given symbol, raising KeyError if not in Top 100 catalog."""
    normalized = normalize_symbol(symbol)
    if normalized not in SYMBOL_CATALOG:
        raise KeyError(f"Unsupported symbol '{symbol}' (normalized: '{normalized}')")
    return SYMBOL_CATALOG[normalized]
