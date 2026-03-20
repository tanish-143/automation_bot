"""
CoinCap WebSocket Manager — Real-Time Price Data
==================================================

Strategy:
  • CoinCap provides a free WebSocket at wss://ws.coincap.io/prices?assets=...
  • Single connection for all assets — no key needed
  • Messages arrive as JSON: {"bitcoin": "67543.12", "ethereum": "3521.45", ...}
  • Reconnect with exponential backoff on disconnect

Data flow:
  WebSocket → parse → update in-memory snapshot cache → periodic DB flush
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import websockets

from config import settings

logger = logging.getLogger(__name__)

# CoinCap asset IDs for our tracked symbols
SYMBOL_TO_COINCAP: dict[str, str] = {
    "BTC/USDT": "bitcoin",
    "ETH/USDT": "ethereum",
    "SOL/USDT": "solana",
    "BNB/USDT": "binance-coin",
    "XRP/USDT": "xrp",
    "ADA/USDT": "cardano",
    "DOGE/USDT": "dogecoin",
    "AVAX/USDT": "avalanche",
    "DOT/USDT": "polkadot",
    "MATIC/USDT": "polygon",
    "LINK/USDT": "chainlink",
    "UNI/USDT": "uniswap",
    "ATOM/USDT": "cosmos",
    "LTC/USDT": "litecoin",
    "FIL/USDT": "filecoin",
}

COINCAP_TO_SYMBOL: dict[str, str] = {v: k for k, v in SYMBOL_TO_COINCAP.items()}


@dataclass
class StreamStats:
    """Tracks connection health."""
    connected_at: float | None = None
    last_message_at: float | None = None
    messages_received: int = 0
    reconnect_count: int = 0
    consecutive_failures: int = 0


class CoinCapWSManager:
    """
    Manages a single WebSocket connection to CoinCap for real-time prices.
    """

    MAX_RECONNECT_BACKOFF = 30.0
    STALE_THRESHOLD = 60

    def __init__(
        self,
        on_price_update: callable | None = None,
    ):
        self.on_price_update = on_price_update
        self._stats = StreamStats()
        self._task: asyncio.Task | None = None
        self._running = False

    def _build_ws_url(self) -> str:
        """Build CoinCap WebSocket URL with asset list."""
        assets = ",".join(SYMBOL_TO_COINCAP.values())
        return f"wss://ws.coincap.io/prices?assets={assets}"

    async def start(self):
        """Launch the WebSocket connection."""
        self._running = True
        self._task = asyncio.create_task(
            self._run_connection(),
            name="coincap-ws",
        )
        logger.info("CoinCap WebSocket manager started")

    async def stop(self):
        """Gracefully shut down."""
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("CoinCap WebSocket manager stopped")

    async def _run_connection(self):
        """
        Connection lifecycle with reconnect logic.
        Backoff: 1s → 2s → 4s → ... → capped at 30s
        """
        url = self._build_ws_url()
        backoff = 1.0

        while self._running:
            try:
                async with websockets.connect(
                    url,
                    ping_interval=30,
                    ping_timeout=10,
                    close_timeout=5,
                    max_size=2**20,
                ) as ws:
                    self._stats.connected_at = time.monotonic()
                    self._stats.consecutive_failures = 0
                    backoff = 1.0

                    logger.info("CoinCap WebSocket connected")

                    async for raw_msg in ws:
                        self._stats.last_message_at = time.monotonic()
                        self._stats.messages_received += 1

                        try:
                            data = json.loads(raw_msg)
                            await self._handle_message(data)
                        except (json.JSONDecodeError, KeyError) as e:
                            logger.debug("CoinCap bad message: %s", e)

            except websockets.ConnectionClosed as e:
                logger.warning("CoinCap WS closed: code=%s reason=%s", e.code, e.reason)
            except Exception as e:
                logger.error("CoinCap WS error: %s", e)

            self._stats.reconnect_count += 1
            self._stats.consecutive_failures += 1

            if self._stats.consecutive_failures >= 3:
                logger.critical(
                    "CoinCap WS: %d consecutive failures — possible outage",
                    self._stats.consecutive_failures,
                )

            if self._running:
                logger.info("CoinCap WS reconnecting in %.1fs", backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self.MAX_RECONNECT_BACKOFF)

    async def _handle_message(self, data: dict):
        """
        CoinCap sends: {"bitcoin": "67543.12", "ethereum": "3521.45", ...}
        Route each price update to the callback.
        """
        for coincap_id, price_str in data.items():
            our_symbol = COINCAP_TO_SYMBOL.get(coincap_id)
            if not our_symbol:
                continue

            try:
                price = float(price_str)
            except (ValueError, TypeError):
                continue

            if self.on_price_update:
                await self.on_price_update(our_symbol, {"price": price})

    def get_health(self) -> dict:
        """Health check stats."""
        now = time.monotonic()
        age = now - self._stats.last_message_at if self._stats.last_message_at else None
        return {
            "messages": self._stats.messages_received,
            "reconnects": self._stats.reconnect_count,
            "consecutive_failures": self._stats.consecutive_failures,
            "last_message_age_s": round(age, 1) if age else None,
            "is_stale": age is not None and age > self.STALE_THRESHOLD,
        }


# ─── Snapshot Cache (in-memory, fed by WebSocket) ────────────────────────────

class SnapshotCache:
    """
    In-memory cache updated by WebSocket callbacks.
    Every N seconds, the cache is flushed to the DB (snapshot_metrics).
    """

    def __init__(self):
        self._prices: dict[str, float] = {}
        self._update_counts: dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def update_price(self, symbol: str, data: dict):
        """Called on each price update from CoinCap."""
        async with self._lock:
            self._prices[symbol] = data["price"]
            self._update_counts[symbol] = self._update_counts.get(symbol, 0) + 1

    async def flush_and_reset(self) -> dict:
        """Return current state and reset counters."""
        async with self._lock:
            snapshot = {
                "prices": dict(self._prices),
                "update_counts": dict(self._update_counts),
                "ts": datetime.now(timezone.utc).isoformat(),
            }
            self._update_counts.clear()
            return snapshot


# ─── Main entry point ────────────────────────────────────────────────────────

async def main():
    """Run the CoinCap WebSocket manager with periodic DB flush."""
    from sqlalchemy import create_engine, text as sa_text

    cache = SnapshotCache()

    async def on_price(symbol: str, data: dict):
        await cache.update_price(symbol, data)

    manager = CoinCapWSManager(on_price_update=on_price)
    await manager.start()

    engine = create_engine(settings.database_url_sync)

    try:
        while True:
            await asyncio.sleep(settings.snapshot_tick_seconds)
            snapshot = await cache.flush_and_reset()
            prices = snapshot["prices"]

            if not prices:
                continue

            now = datetime.now(timezone.utc)

            with engine.begin() as conn:
                tracked = conn.execute(sa_text(
                    "SELECT symbol_id, symbol FROM symbols WHERE is_active = TRUE"
                )).fetchall()
                symbol_map = {row[1]: row[0] for row in tracked}

                for symbol, price in prices.items():
                    sid = symbol_map.get(symbol)
                    if not sid:
                        continue

                    conn.execute(sa_text("""
                        INSERT INTO snapshot_metrics (
                            ts, symbol_id, exchange, asset_class,
                            current_price, bid_price, ask_price, bid_ask_spread_bps
                        ) VALUES (
                            :ts, :sid, 'coincap', 'crypto',
                            :price, :price, :price, 0
                        )
                        ON CONFLICT (ts, symbol_id) DO UPDATE SET
                            current_price = EXCLUDED.current_price
                    """), {"ts": now, "sid": sid, "price": price})

            logger.info(
                "WS flush: %d prices (health: %s)",
                len(prices),
                manager.get_health(),
            )
    finally:
        await manager.stop()


if __name__ == "__main__":
    asyncio.run(main())
