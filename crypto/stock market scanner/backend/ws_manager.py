"""
Binance WebSocket Manager — Real-Time Market Data
===================================================

Strategy:
  • Binance allows up to 1024 streams per WebSocket connection
  • We use 200 streams/connection (conservative, reduces reconnect blast radius)
  • For 2000 symbols: 2000 / 200 = 10 connections
  • Each connection subscribes to: {symbol}@aggTrade + {symbol}@kline_1m
  • So 200 symbols → 400 streams per connection (within 1024 limit)

Reconnect logic:
  • On disconnect: exponential backoff 1s → 2s → 4s → 8s → cap at 30s
  • On Binance maintenance (planned): backoff longer, log warning
  • Ping/pong every 30s to detect dead connections
  • If >3 consecutive failures: mark symbols as stale, alert admins

Data flow:
  WebSocket → parse → update in-memory snapshot cache → periodic DB flush
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone

import websockets

from config import settings

logger = logging.getLogger(__name__)


@dataclass
class StreamStats:
    """Tracks per-connection health."""
    connected_at: float | None = None
    last_message_at: float | None = None
    messages_received: int = 0
    reconnect_count: int = 0
    consecutive_failures: int = 0


class BinanceWSManager:
    """
    Manages multiple WebSocket connections to Binance,
    distributing symbols across connections.
    """

    MAX_STREAMS_PER_CONN = settings.binance_ws_max_streams_per_conn  # 200
    MAX_RECONNECT_BACKOFF = 30.0
    PING_INTERVAL = 30
    STALE_THRESHOLD = 60  # seconds without a message = stale

    def __init__(
        self,
        symbols: list[str],
        on_agg_trade: callable | None = None,
        on_kline: callable | None = None,
    ):
        """
        Parameters
        ----------
        symbols : list[str]
            Binance symbol names, e.g. ["BTCUSDT", "ETHUSDT", ...]
        on_agg_trade : callable
            Async callback(symbol, data) for each aggTrade event.
        on_kline : callable
            Async callback(symbol, data) for each kline event.
        """
        self.symbols = symbols
        self.on_agg_trade = on_agg_trade
        self.on_kline = on_kline

        # Partition symbols into connection groups
        self._conn_groups = self._partition_symbols()
        self._stats: dict[int, StreamStats] = {}
        self._tasks: list[asyncio.Task] = []
        self._running = False

    def _partition_symbols(self) -> list[list[str]]:
        """
        Split symbols into groups. Each group becomes one WebSocket connection.
        Each symbol generates 2 streams (aggTrade + kline_1m), so max symbols
        per connection = MAX_STREAMS_PER_CONN / 2.
        """
        max_symbols = self.MAX_STREAMS_PER_CONN // 2
        groups = []
        for i in range(0, len(self.symbols), max_symbols):
            groups.append(self.symbols[i : i + max_symbols])

        logger.info(
            "WebSocket: %d symbols → %d connections (%d symbols/conn)",
            len(self.symbols), len(groups),
            max_symbols,
        )
        return groups

    def _build_stream_url(self, symbols: list[str]) -> str:
        """
        Build combined stream URL.
        Format: wss://stream.binance.com:9443/stream?streams=btcusdt@aggTrade/btcusdt@kline_1m/...
        """
        streams = []
        for sym in symbols:
            s = sym.lower()
            streams.append(f"{s}@aggTrade")
            streams.append(f"{s}@kline_1m")

        stream_path = "/".join(streams)
        return f"{settings.binance_ws_base}/stream?streams={stream_path}"

    async def start(self):
        """Launch all WebSocket connections concurrently."""
        self._running = True
        for conn_id, group in enumerate(self._conn_groups):
            self._stats[conn_id] = StreamStats()
            task = asyncio.create_task(
                self._run_connection(conn_id, group),
                name=f"ws-conn-{conn_id}",
            )
            self._tasks.append(task)

        logger.info("WebSocket manager started: %d connections", len(self._tasks))

    async def stop(self):
        """Gracefully shut down all connections."""
        self._running = False
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        logger.info("WebSocket manager stopped")

    async def _run_connection(self, conn_id: int, symbols: list[str]):
        """
        Single connection lifecycle with reconnect logic.

        Backoff strategy:
          attempt 1: wait 1s
          attempt 2: wait 2s
          attempt 3: wait 4s
          attempt 4: wait 8s
          ...
          capped at 30s

        After 3 consecutive failures without receiving any message,
        log a critical warning (feed may be down).
        """
        stats = self._stats[conn_id]
        url = self._build_stream_url(symbols)
        backoff = 1.0

        while self._running:
            try:
                async with websockets.connect(
                    url,
                    ping_interval=self.PING_INTERVAL,
                    ping_timeout=10,
                    close_timeout=5,
                    max_size=2**20,  # 1 MB max message
                ) as ws:
                    stats.connected_at = time.monotonic()
                    stats.consecutive_failures = 0
                    backoff = 1.0  # reset on successful connect

                    logger.info(
                        "WS conn-%d connected: %d symbols", conn_id, len(symbols)
                    )

                    async for raw_msg in ws:
                        stats.last_message_at = time.monotonic()
                        stats.messages_received += 1

                        try:
                            msg = json.loads(raw_msg)
                            await self._handle_message(msg)
                        except (json.JSONDecodeError, KeyError) as e:
                            logger.debug("WS conn-%d bad message: %s", conn_id, e)

            except websockets.ConnectionClosed as e:
                logger.warning(
                    "WS conn-%d closed: code=%s reason=%s",
                    conn_id, e.code, e.reason,
                )
            except Exception as e:
                logger.error("WS conn-%d error: %s", conn_id, e)

            # ── Reconnect backoff ──
            stats.reconnect_count += 1
            stats.consecutive_failures += 1

            if stats.consecutive_failures >= 3:
                logger.critical(
                    "WS conn-%d: %d consecutive failures — possible Binance outage",
                    conn_id, stats.consecutive_failures,
                )

            if self._running:
                logger.info("WS conn-%d reconnecting in %.1fs", conn_id, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self.MAX_RECONNECT_BACKOFF)

    async def _handle_message(self, msg: dict):
        """
        Route incoming WebSocket messages to the appropriate callback.

        Combined stream messages have format:
          {"stream": "btcusdt@aggTrade", "data": {...}}
        """
        stream = msg.get("stream", "")
        data = msg.get("data", {})

        if "@aggTrade" in stream:
            symbol = data.get("s", "")  # e.g. "BTCUSDT"
            if self.on_agg_trade:
                await self.on_agg_trade(symbol, {
                    "price": float(data.get("p", 0)),
                    "quantity": float(data.get("q", 0)),
                    "trade_time": data.get("T"),
                    "is_buyer_maker": data.get("m"),
                })

        elif "@kline" in stream:
            kline = data.get("k", {})
            symbol = kline.get("s", "")
            if self.on_kline:
                await self.on_kline(symbol, {
                    "interval": kline.get("i"),
                    "open": float(kline.get("o", 0)),
                    "high": float(kline.get("h", 0)),
                    "low": float(kline.get("l", 0)),
                    "close": float(kline.get("c", 0)),
                    "volume": float(kline.get("v", 0)),
                    "is_closed": kline.get("x", False),
                    "open_time": kline.get("t"),
                    "close_time": kline.get("T"),
                })

    def get_health(self) -> dict:
        """Health check — returns per-connection stats."""
        now = time.monotonic()
        health = {}
        for conn_id, stats in self._stats.items():
            age = now - stats.last_message_at if stats.last_message_at else None
            health[f"conn-{conn_id}"] = {
                "messages": stats.messages_received,
                "reconnects": stats.reconnect_count,
                "consecutive_failures": stats.consecutive_failures,
                "last_message_age_s": round(age, 1) if age else None,
                "is_stale": age is not None and age > self.STALE_THRESHOLD,
            }
        return health


# ─── Snapshot Cache (in-memory, fed by WebSocket) ────────────────────────────

class SnapshotCache:
    """
    In-memory cache updated by WebSocket callbacks.

    The WebSocket feeds real-time prices into this cache.
    Every N seconds, the cache is flushed to the DB (snapshot_metrics).
    This decouples high-frequency WS messages from DB write throughput.
    """

    def __init__(self):
        self._prices: dict[str, float] = {}        # symbol → latest price
        self._volumes: dict[str, float] = {}        # symbol → rolling volume
        self._trade_counts: dict[str, int] = {}     # symbol → trade count (1min window)
        self._lock = asyncio.Lock()

    async def update_trade(self, symbol: str, data: dict):
        """Called on each aggTrade."""
        async with self._lock:
            self._prices[symbol] = data["price"]
            self._volumes[symbol] = self._volumes.get(symbol, 0) + data["quantity"]
            self._trade_counts[symbol] = self._trade_counts.get(symbol, 0) + 1

    async def update_kline(self, symbol: str, data: dict):
        """Called on each kline event."""
        async with self._lock:
            self._prices[symbol] = data["close"]

    async def flush_and_reset(self) -> dict:
        """Return current state and reset counters."""
        async with self._lock:
            snapshot = {
                "prices": dict(self._prices),
                "volumes": dict(self._volumes),
                "trade_counts": dict(self._trade_counts),
                "ts": datetime.now(timezone.utc).isoformat(),
            }
            self._volumes.clear()
            self._trade_counts.clear()
            return snapshot


# ─── Entrypoint ──────────────────────────────────────────────────────────────

async def run_ws_ingestion(symbols: list[str]):
    """
    Standalone entrypoint to run WebSocket ingestion.

    Usage:
      python -c "import asyncio; from ws_manager import run_ws_ingestion; \
        asyncio.run(run_ws_ingestion(['BTCUSDT', 'ETHUSDT']))"
    """
    cache = SnapshotCache()

    manager = BinanceWSManager(
        symbols=symbols,
        on_agg_trade=cache.update_trade,
        on_kline=cache.update_kline,
    )

    await manager.start()

    # Periodic flush to DB
    try:
        while True:
            await asyncio.sleep(settings.snapshot_tick_seconds)
            snapshot = await cache.flush_and_reset()
            logger.info(
                "Cache flush: %d symbols, %d trades",
                len(snapshot["prices"]),
                sum(snapshot["trade_counts"].values()),
            )
            # TODO: write snapshot to DB here
    except asyncio.CancelledError:
        await manager.stop()
