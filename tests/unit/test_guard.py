"""Unit tests for connection guard, rate limiter, and deduplication cache."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from ingestors.guard import (
    CIRCUIT_BREAKER_THRESHOLD,
    DEFAULT_ROTATION_INTERVAL_SEC,
    ConnectionRateLimiter,
    DeduplicationCache,
    calculate_backoff_delay,
    should_rotate_connection,
)


class TestBackoffDelay:
    """Test suite for calculate_backoff_delay."""

    def test_backoff_within_jitter_bounds(self) -> None:
        """Verify backoff delay adheres to min/max jitter factors."""
        for retry in range(6):
            delay = calculate_backoff_delay(
                retry, initial_delay=1.0, max_delay=60.0, min_jitter=0.5, max_jitter=1.0
            )
            max_expected = min(60.0, 1.0 * (2**retry))
            min_expected = max_expected * 0.5
            assert min_expected <= delay <= max_expected

    def test_backoff_caps_at_max_delay(self) -> None:
        """Verify backoff never exceeds max_delay regardless of large retry counts."""
        for retry in [10, 20, 50]:
            delay = calculate_backoff_delay(retry, initial_delay=1.0, max_delay=30.0)
            assert 15.0 <= delay <= 30.0

    def test_backoff_negative_retry_count_handled(self) -> None:
        """Verify negative retry count defaults to 0 safely."""
        delay = calculate_backoff_delay(-5, initial_delay=2.0)
        assert 1.0 <= delay <= 2.0


class TestConnectionRateLimiter:
    """Test suite for ConnectionRateLimiter."""

    @pytest.mark.asyncio
    async def test_rate_limiter_allows_below_threshold(self) -> None:
        """Verify connections below 240 are allowed."""
        limiter = ConnectionRateLimiter()
        mock_redis = MagicMock()
        mock_pipe = MagicMock()
        mock_pipe.incr.return_value = mock_pipe
        mock_pipe.expire.return_value = mock_pipe
        mock_pipe.execute = AsyncMock(return_value=[100, True])
        mock_redis.pipeline.return_value = mock_pipe

        count, allowed = await limiter.record_connection_attempt(mock_redis)
        assert count == 100
        assert allowed is True

    @pytest.mark.asyncio
    async def test_rate_limiter_blocks_above_threshold(self) -> None:
        """Verify circuit breaker trips when count exceeds threshold (240)."""
        limiter = ConnectionRateLimiter()
        mock_redis = MagicMock()
        mock_pipe = MagicMock()
        mock_pipe.incr.return_value = mock_pipe
        mock_pipe.expire.return_value = mock_pipe
        mock_pipe.execute = AsyncMock(return_value=[CIRCUIT_BREAKER_THRESHOLD + 1, True])
        mock_redis.pipeline.return_value = mock_pipe

        count, allowed = await limiter.record_connection_attempt(mock_redis)
        assert count == CIRCUIT_BREAKER_THRESHOLD + 1
        assert allowed is False


class TestDeduplicationCache:
    """Test suite for DeduplicationCache."""

    def test_dedup_detects_duplicates(self) -> None:
        """Verify repeated IDs are correctly marked as duplicates."""
        cache = DeduplicationCache(capacity=100)
        assert not cache.is_duplicate(12345)
        cache.add(12345)
        assert cache.is_duplicate(12345)
        assert len(cache) == 1

    def test_dedup_evicts_oldest_when_exceeding_capacity(self) -> None:
        """Verify FIFO eviction maintains bounded memory."""
        capacity = 5
        cache = DeduplicationCache(capacity=capacity)
        for i in range(10):
            cache.add(i)

        assert len(cache) == capacity
        # 0..4 should have been evicted
        for i in range(5):
            assert not cache.is_duplicate(i)
        # 5..9 should be retained
        for i in range(5, 10):
            assert cache.is_duplicate(i)


class TestRotationTrigger:
    """Test suite for should_rotate_connection."""

    def test_rotation_threshold_evaluation(self) -> None:
        """Verify rotation triggers at exactly 23h50m (85,800s)."""
        assert not should_rotate_connection(85799.0)
        assert should_rotate_connection(DEFAULT_ROTATION_INTERVAL_SEC)
        assert should_rotate_connection(90000.0)
