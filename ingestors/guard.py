"""Network reconnection guard, connection rate limiting, and rotation manager.

Implements:
1. Exponential backoff with jitter (prevent thundering herd).
2. Redis-backed connection rate limiter (Binance max 300 conns / 5 min).
3. Make-before-break connection rotation before Binance 24h disconnect (23h50m).
4. In-memory bounded deduplication buffer for seamless transition.
"""

from __future__ import annotations

import collections
import secrets
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import redis.asyncio as aioredis

_SYS_RANDOM = secrets.SystemRandom()

# Binance limits: max 300 connections per 5 minutes per IP
REDIS_KEY_CONN_COUNTER = "crypto:ingest:conn:binance"
MAX_CONNS_PER_WINDOW = 300
CIRCUIT_BREAKER_THRESHOLD = 240  # 80% safety ceiling
WINDOW_SECONDS = 300

# 23 hours 50 minutes in seconds (85,800s)
DEFAULT_ROTATION_INTERVAL_SEC = 85800.0


def calculate_backoff_delay(
    retry_count: int,
    initial_delay: float = 1.0,
    max_delay: float = 60.0,
    min_jitter: float = 0.5,
    max_jitter: float = 1.0,
) -> float:
    """Calculate exponential backoff delay with uniform jitter.

    Formula: min(max_delay, initial_delay * 2^retry_count) * Uniform(min_jitter, max_jitter)
    """
    retry_count = max(0, retry_count)
    exponential_base = initial_delay * (2**retry_count)
    capped_delay = min(max_delay, exponential_base)
    jitter = _SYS_RANDOM.uniform(min_jitter, max_jitter)
    return float(capped_delay * jitter)


class ConnectionRateLimiter:
    """Monitors connection frequency to Binance to prevent IP bans."""

    def __init__(
        self,
        max_connections: int = MAX_CONNS_PER_WINDOW,
        circuit_breaker_threshold: int = CIRCUIT_BREAKER_THRESHOLD,
        window_seconds: int = WINDOW_SECONDS,
        key: str = REDIS_KEY_CONN_COUNTER,
    ) -> None:
        self.max_connections = max_connections
        self.circuit_breaker_threshold = circuit_breaker_threshold
        self.window_seconds = window_seconds
        self.key = key

    async def record_connection_attempt(self, redis_client: aioredis.Redis) -> tuple[int, bool]:
        """Record a connection attempt in Redis and evaluate circuit breaker.

        Returns:
            Tuple of (current_count_in_window, is_allowed).
            If is_allowed is False, the circuit breaker is tripped.
        """
        pipe = redis_client.pipeline()
        pipe.incr(self.key)
        pipe.expire(self.key, self.window_seconds)
        results = await pipe.execute()
        current_count: int = int(results[0])

        is_allowed = current_count <= self.circuit_breaker_threshold
        return current_count, is_allowed


class DeduplicationCache:
    """Bounded in-memory cache for trade deduplication during connection rotation."""

    def __init__(self, capacity: int = 20_000) -> None:
        self.capacity = capacity
        self._deque: collections.deque[int] = collections.deque()
        self._set: set[int] = set()

    def is_duplicate(self, trade_id: int) -> bool:
        """Return True if trade_id has been seen recently."""
        return trade_id in self._set

    def add(self, trade_id: int) -> None:
        """Register trade_id into the bounded cache."""
        if trade_id in self._set:
            return
        if len(self._deque) >= self.capacity:
            oldest = self._deque.popleft()
            self._set.discard(oldest)
        self._deque.append(trade_id)
        self._set.add(trade_id)

    def __len__(self) -> int:
        return len(self._set)


def should_rotate_connection(
    uptime_seconds: float,
    rotation_interval_sec: float = DEFAULT_ROTATION_INTERVAL_SEC,
) -> bool:
    """Return True if connection uptime has reached the rotation mark (23h50m)."""
    return uptime_seconds >= rotation_interval_sec
