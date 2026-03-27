from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone

import httpx

from config import settings

logger = logging.getLogger(__name__)

APP_TO_BINANCE_SYMBOL: dict[str, str] = {
    "BTC/USDT": "BTCUSDT",
    "ETH/USDT": "ETHUSDT",
    "SOL/USDT": "SOLUSDT",
    "BNB/USDT": "BNBUSDT",
    "XRP/USDT": "XRPUSDT",
    "ADA/USDT": "ADAUSDT",
    "DOGE/USDT": "DOGEUSDT",
    "AVAX/USDT": "AVAXUSDT",
    "DOT/USDT": "DOTUSDT",
    "MATIC/USDT": "MATICUSDT",
    "LINK/USDT": "LINKUSDT",
    "UNI/USDT": "UNIUSDT",
    "ATOM/USDT": "ATOMUSDT",
    "LTC/USDT": "LTCUSDT",
    "FIL/USDT": "FILUSDT",
}

BINANCE_TO_APP_SYMBOL: dict[str, str] = {v: k for k, v in APP_TO_BINANCE_SYMBOL.items()}


def tracked_app_symbols() -> list[str]:
    return list(APP_TO_BINANCE_SYMBOL)


def _tracked_exchange_symbols(app_symbols: list[str] | None = None) -> list[str]:
    symbols = app_symbols or tracked_app_symbols()
    return [APP_TO_BINANCE_SYMBOL[symbol] for symbol in symbols if symbol in APP_TO_BINANCE_SYMBOL]


def _symbols_param(symbols: list[str]) -> str:
    return json.dumps(symbols, separators=(",", ":"))


def _timestamp_to_iso(ts_ms: int | None) -> str:
    if not ts_ms:
        return datetime.now(timezone.utc).isoformat()
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()


def normalize_ticker_24h(ticker: dict) -> dict | None:
    exchange_symbol = ticker.get("symbol")
    app_symbol = BINANCE_TO_APP_SYMBOL.get(exchange_symbol)
    if not app_symbol:
        return None

    last_price = float(ticker.get("lastPrice") or 0)
    bid_price = float(ticker.get("bidPrice") or 0)
    ask_price = float(ticker.get("askPrice") or 0)
    mid_price = (bid_price + ask_price) / 2 if bid_price and ask_price else last_price
    spread_bps = ((ask_price - bid_price) / mid_price) * 10_000 if mid_price and ask_price and bid_price else 0.0

    return {
        "symbol": app_symbol,
        "exchange_symbol": exchange_symbol,
        "current_price": last_price,
        "price_change_pct_24h": float(ticker.get("priceChangePercent") or 0),
        "volume_24h": float(ticker.get("quoteVolume") or 0),
        "high_24h": float(ticker.get("highPrice") or 0),
        "low_24h": float(ticker.get("lowPrice") or 0),
        "bid_price": bid_price or last_price,
        "ask_price": ask_price or last_price,
        "bid_ask_spread_bps": spread_bps,
        "market_cap": 0.0,
        "market_cap_rank": None,
        "image": "",
        "last_updated": _timestamp_to_iso(ticker.get("closeTime")),
    }


async def fetch_ticker_24h_async(app_symbols: list[str] | None = None) -> list[dict]:
    exchange_symbols = _tracked_exchange_symbols(app_symbols)
    async with httpx.AsyncClient(base_url=settings.binance_rest_base, timeout=20.0) as client:
        response = await client.get(
            "/api/v3/ticker/24hr",
            params={"symbols": _symbols_param(exchange_symbols)},
        )
        response.raise_for_status()
        data = response.json()

    if isinstance(data, dict):
        data = [data]

    normalized = [item for ticker in data if (item := normalize_ticker_24h(ticker))]
    normalized.sort(key=lambda item: item["volume_24h"], reverse=True)
    return normalized


def fetch_ticker_24h_sync(app_symbols: list[str] | None = None) -> list[dict]:
    exchange_symbols = _tracked_exchange_symbols(app_symbols)
    with httpx.Client(base_url=settings.binance_rest_base, timeout=20.0) as client:
        response = client.get(
            "/api/v3/ticker/24hr",
            params={"symbols": _symbols_param(exchange_symbols)},
        )
        response.raise_for_status()
        data = response.json()

    if isinstance(data, dict):
        data = [data]

    normalized = [item for ticker in data if (item := normalize_ticker_24h(ticker))]
    normalized.sort(key=lambda item: item["volume_24h"], reverse=True)
    return normalized


async def fetch_candles_for_app_symbols_async(
    app_symbols: list[str] | None = None,
    *,
    interval: str,
    limit: int,
) -> dict[str, dict[str, list[float]]]:
    symbols = app_symbols or tracked_app_symbols()
    candle_map: dict[str, dict[str, list[float]]] = {}

    async with httpx.AsyncClient(base_url=settings.binance_rest_base, timeout=20.0) as client:
        async def fetch_one(app_symbol: str) -> tuple[str, dict[str, list[float]] | None]:
            exchange_symbol = APP_TO_BINANCE_SYMBOL.get(app_symbol)
            if not exchange_symbol:
                return app_symbol, None

            try:
                response = await client.get(
                    "/api/v3/klines",
                    params={"symbol": exchange_symbol, "interval": interval, "limit": limit},
                )
                response.raise_for_status()
                klines = response.json()
            except httpx.HTTPError as exc:
                logger.warning("Binance klines failed for %s: %s", app_symbol, exc)
                return app_symbol, None

            if not klines:
                return app_symbol, None

            return app_symbol, {
                "closes": [float(kline[4]) for kline in klines],
                "highs": [float(kline[2]) for kline in klines],
                "lows": [float(kline[3]) for kline in klines],
            }

        results = await asyncio.gather(*(fetch_one(symbol) for symbol in symbols))

    for app_symbol, candles in results:
        if candles:
            candle_map[app_symbol] = candles

    return candle_map