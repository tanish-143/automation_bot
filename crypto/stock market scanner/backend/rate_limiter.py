"""
Distributed Rate Limiter — Redis-backed sliding window for Binance API.

Binance limits:
  REST: 1200 request weight / minute (sliding window)
  WebSocket: 5 messages/sec inbound, 1024 streams/conn

This module provides:
  1. BinanceRateLimiter — sliding-window token bucket via Redis
  2. Partition-aware worker scaling — splits symbol universe across N workers

Usage:
    limiter = BinanceRateLimiter(redis_client)
    await limiter.acquire(weight=40)  # blocks until budget available
"""

from __future__ import annotations

import asyncio
import logging
import time

import redis.asyncio as aioredis

from config import settings

logger = logging.getLogger(__name__)

# ─── Keys ───────────────────────────────────────────────────────────────────

_RATE_KEY = "scanner:binance:rate:window"
_LOCK_KEY = "scanner:binance:rate:lock"


class BinanceRateLimiter:
    """
    Redis sliding-window rate limiter for Binance REST API.

    Strategy:
      • Each request adds an entry (timestamp, weight) to a Redis sorted set.
      • Before a request, we trim entries older than 60s and sum remaining weight.
      • If remaining weight + requested weight > limit, sleep until budget frees.
      • Redis ensures consistency across multiple worker processes / containers.

    Scaling to 1000+ symbols:
      With 1200 weight/min and ticker endpoint = 40 weight:
        - 1 ticker call = 40 weight → 30 calls/min → can cover full universe
        - Individual candle calls = 1 weight each → 1160 remaining → ~1160 symbols/min
      For 2000 symbols at 2 intervals (1h + 4h): 4000 weight/min needed
        → Shard into 4 worker groups, each with its own rate window (300 weight/min each)
        → Or stagger: 1000 symbols in first 30s, next 1000 in second 30s
    """

    def __init__(
        self,
        redis: aioredis.Redis,
        max_weight_per_min: int = settings.binance_rate_limit_per_min,
        window_seconds: int = 60,
    ):
        self._redis = redis
        self._max_weight = max_weight_per_min
        self._window = window_seconds

    async def acquire(self, weight: int = 1, max_wait: float = 30.0) -> bool:
        """
        Acquire rate limit budget. Blocks up to max_wait seconds.
        Returns True if acquired, False if timed out.
        """
        deadline = time.monotonic() + max_wait

        while time.monotonic() < deadline:
            now = time.time()
            window_start = now - self._window

            pipe = self._redis.pipeline()
            # Remove expired entries
            pipe.zremrangebyscore(_RATE_KEY, "-inf", window_start)
            # Get current total weight in window
            pipe.zrangebyscore(_RATE_KEY, window_start, "+inf", withscores=True)
            results = await pipe.execute()

            entries = results[1]
            current_weight = sum(score for _, score in entries)

            if current_weight + weight <= self._max_weight:
                # Budget available — claim it
                entry_id = f"{now}:{id(asyncio.current_task())}"
                await self._redis.zadd(_RATE_KEY, {entry_id: now}, gt=True)
                # Store weight as a separate hash for accurate accounting
                await self._redis.hset(f"{_RATE_KEY}:weights", entry_id, weight)
                await self._redis.expire(_RATE_KEY, self._window + 10)

                remaining = self._max_weight - current_weight - weight
                logger.debug(
                    "rate limit acquired",
                    extra={"rate_remaining": remaining},
                )
                return True

            # Budget exhausted — calculate sleep time
            if entries:
                oldest_ts = min(score for _, score in entries)
                sleep_for = max(0.1, (oldest_ts + self._window) - now + 0.1)
            else:
                sleep_for = 1.0

            logger.warning(
                "rate limit near capacity, sleeping",
                extra={"rate_remaining": self._max_weight - current_weight},
            )
            await asyncio.sleep(min(sleep_for, deadline - time.monotonic()))

        logger.error("rate limit acquire timed out")
        return False

    async def get_remaining(self) -> int:
        """Return remaining weight budget in current window."""
        now = time.time()
        window_start = now - self._window
        await self._redis.zremrangebyscore(_RATE_KEY, "-inf", window_start)
        entries = await self._redis.zrangebyscore(
            _RATE_KEY, window_start, "+inf", withscores=True
        )
        current_weight = sum(score for _, score in entries)
        return self._max_weight - int(current_weight)


# ─── Worker Scaling Strategy ─────────────────────────────────────────────────

def partition_symbols(symbols: list[str], num_workers: int) -> list[list[str]]:
    """
    Split symbol universe across N worker instances.

    Each worker gets a roughly equal slice of symbols. Combined with
    the rate limiter, this ensures:
      - 4 workers × 300 weight/min each = 1200 total (within Binance limit)
      - Each worker scans its partition independently
      - If a worker dies, its symbols are redistributed on next rebalance

    Scaling guide (symbol count → worker count):
      100-500   symbols: 1 worker  (concurrency=4)
      500-1000  symbols: 2 workers (concurrency=4 each)
      1000-2000 symbols: 4 workers (concurrency=4 each)
      2000-5000 symbols: 8 workers (concurrency=2 each, more IO-bound)
    """
    partitions: list[list[str]] = [[] for _ in range(num_workers)]
    for i, symbol in enumerate(symbols):
        partitions[i % num_workers].append(symbol)
    return partitions


def get_worker_partition(
    symbols: list[str],
    worker_id: int,
    total_workers: int,
) -> list[str]:
    """
    Get the symbol slice for a specific worker instance.

    Usage in Celery task:
        worker_id = int(os.environ.get("WORKER_ID", 0))
        total_workers = int(os.environ.get("TOTAL_WORKERS", 1))
        my_symbols = get_worker_partition(all_symbols, worker_id, total_workers)
    """
    return [s for i, s in enumerate(symbols) if i % total_workers == worker_id]
