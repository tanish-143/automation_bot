"""
FastAPI Application — Market Scanner API
=========================================

Endpoints:
  GET  /scan/top-movers    — Top symbols ranked by composite score
  GET  /scan/alerts        — User's triggered alerts
  POST /scan/rules         — Save / update a detection rule
  GET  /scan/snapshot/{id} — Single symbol snapshot history
  GET  /health             — Liveness check
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from enum import Enum

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import httpx
from pydantic import BaseModel, field_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from alert_dispatcher import AlertDispatcher
from binance_client import fetch_candles_for_app_symbols_async, fetch_ticker_24h_async
from crypto.chandelier_exit import compute_ce_for_symbols
from config import settings
from db import get_db
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

# ─── App ──────────────────────────────────────────────────────────────────────

app = FastAPI(title="Market Scanner API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ─── Schemas ──────────────────────────────────────────────────────────────────

class AssetClass(str, Enum):
    CRYPTO = "crypto"
    STOCK = "stock"


class SessionFilter(str, Enum):
    ASIA = "asia"
    EUROPE = "europe"
    US = "us"


class TopMoverItem(BaseModel):
    symbol_id: int
    symbol: str
    exchange: str
    asset_class: str
    current_price: float
    price_change_pct_24h: float | None
    volume_24h: float | None
    volume_ratio: float | None
    realized_volatility: float | None
    volatility_percentile: float | None
    composite_score: float | None


class AlertItem(BaseModel):
    alert_id: int
    ts: datetime
    symbol: str
    exchange: str
    rule: str
    status: str
    trigger_price: float | None
    trigger_volume_ratio: float | None
    trigger_volatility: float | None
    message: str | None


class RuleCreate(BaseModel):
    """Payload for POST /scan/rules — create or update a user detection rule."""
    symbol_id: int | None = None          # null = global rule
    rule: str
    min_volume_ratio: float | None = None
    min_volatility: float | None = None
    min_price_change_pct: float | None = None
    max_spread_bps: float | None = None
    custom_expression: str | None = None
    is_enabled: bool = True

    @field_validator("rule")
    @classmethod
    def validate_rule(cls, v: str) -> str:
        allowed = {
            "volume_spike", "volatility_breakout",
            "spread_widening", "price_change_pct", "custom",
        }
        if v not in allowed:
            raise ValueError(f"rule must be one of {allowed}")
        return v

    @field_validator("custom_expression")
    @classmethod
    def validate_custom_expression(cls, v: str | None) -> str | None:
        if v is not None:
            forbidden = ["drop", "delete", "insert", "update", "alter", ";", "--"]
            lower = v.lower()
            for word in forbidden:
                if word in lower:
                    raise ValueError(f"Forbidden keyword in custom expression: {word}")
        return v


class RuleResponse(BaseModel):
    threshold_id: int
    message: str


# ─── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "ts": datetime.now(timezone.utc).isoformat()}


@app.get("/health/ready")
async def readiness(db: AsyncSession = Depends(get_db)):
    """Deep health check — verifies DB + Redis connectivity."""
    checks: dict = {"ts": datetime.now(timezone.utc).isoformat()}
    try:
        await db.execute(text("SELECT 1"))
        checks["db"] = "ok"
    except Exception as e:
        checks["db"] = f"error: {e}"
    try:
        import redis.asyncio as aioredis
        r = aioredis.from_url(settings.redis_url)
        await r.ping()
        await r.aclose()
        checks["redis"] = "ok"
    except Exception as e:
        checks["redis"] = f"error: {e}"
    checks["status"] = "ok" if checks.get("db") == "ok" and checks.get("redis") == "ok" else "degraded"
    return checks


@app.get("/scan/top-movers", response_model=list[TopMoverItem])
async def get_top_movers(
    asset_class: AssetClass | None = Query(None, description="Filter by crypto or stock"),
    session: SessionFilter | None = Query(None, description="Filter by trading session timezone"),
    limit: int = Query(50, ge=1, le=500),
    min_volume_ratio: float = Query(0.0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """
    Top movers ranked by composite score.

    Reads from `v_scanner_feed` (pre-computed percentiles + composite score).
    Supports filtering by asset class, session timezone, and minimum volume ratio.

    Index used: idx_snap_volume_ratio (ts DESC, asset_class, volume_ratio DESC)
    """
    conditions = ["volume_24h >= :min_vol"]
    params: dict = {"min_vol": settings.min_volume_floor_usd, "lim": limit}

    if asset_class:
        conditions.append("asset_class = :ac")
        params["ac"] = asset_class.value

    if min_volume_ratio > 0:
        conditions.append("volume_ratio >= :mvr")
        params["mvr"] = min_volume_ratio

    if session:
        session_hours = {"asia": (0, 8), "europe": (7, 16), "us": (13, 21)}
        start_h, end_h = session_hours[session.value]
        conditions.append(
            "EXTRACT(HOUR FROM ts) >= :sh AND EXTRACT(HOUR FROM ts) < :eh"
        )
        params["sh"] = start_h
        params["eh"] = end_h

    where = " AND ".join(conditions)
    query = text(f"""
        SELECT symbol_id, symbol, exchange, asset_class, current_price,
               price_change_pct_24h, volume_24h, volume_ratio,
               realized_volatility, volatility_pctile AS volatility_percentile,
               composite_score
        FROM v_scanner_feed
        WHERE {where}
        ORDER BY composite_score DESC NULLS LAST
        LIMIT :lim
    """)

    result = await db.execute(query, params)
    rows = result.mappings().all()
    return [TopMoverItem(**dict(r)) for r in rows]


@app.get("/scan/alerts", response_model=list[AlertItem])
async def get_alerts(
    user_id: int = Query(..., description="User ID"),
    status: str = Query("triggered", description="Alert status filter"),
    hours: int = Query(24, ge=1, le=168, description="Look-back window in hours"),
    limit: int = Query(100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
):
    """
    User's alerts within a time window.

    Index used: idx_alerts_user_status (user_id, status, ts DESC)
    """
    allowed_statuses = {"triggered", "acknowledged", "resolved", "expired"}
    if status not in allowed_statuses:
        raise HTTPException(400, f"status must be one of {allowed_statuses}")

    query = text("""
        SELECT a.alert_id, a.ts, s.symbol, s.exchange,
               CAST(a.rule AS TEXT) AS rule, CAST(a.status AS TEXT) AS status,
               a.trigger_price, a.trigger_volume_ratio,
               a.trigger_volatility, a.message
        FROM alerts a
        JOIN symbols s ON s.symbol_id = a.symbol_id
        WHERE a.user_id = :uid
          AND a.status = CAST(:st AS alert_status)
          AND a.ts >= NOW() - make_interval(hours => :hrs)
        ORDER BY a.ts DESC
        LIMIT :lim
    """)

    result = await db.execute(query, {
        "uid": user_id, "st": status, "hrs": hours, "lim": limit,
    })
    rows = result.mappings().all()
    return [AlertItem(**dict(r)) for r in rows]


@app.post("/scan/rules", response_model=RuleResponse)
async def save_rule(
    user_id: int = Query(..., description="User ID"),
    body: RuleCreate = ...,
    db: AsyncSession = Depends(get_db),
):
    """
    Create or update a detection rule (upsert on user_id + symbol_id + rule).

    Stores into `user_thresholds` table.
    """
    query = text("""
        INSERT INTO user_thresholds
            (user_id, symbol_id, rule, min_volume_ratio, min_volatility,
             min_price_change_pct, max_spread_bps, custom_expression, is_enabled,
             created_at, updated_at)
        VALUES
            (:uid, :sid, CAST(:rule AS alert_rule), :mvr, :mv, :mpc, :msb, :ce, :ie, NOW(), NOW())
        ON CONFLICT (user_id, symbol_id, rule) DO UPDATE SET
            min_volume_ratio     = EXCLUDED.min_volume_ratio,
            min_volatility       = EXCLUDED.min_volatility,
            min_price_change_pct = EXCLUDED.min_price_change_pct,
            max_spread_bps       = EXCLUDED.max_spread_bps,
            custom_expression    = EXCLUDED.custom_expression,
            is_enabled           = EXCLUDED.is_enabled,
            updated_at           = NOW()
        RETURNING threshold_id
    """)

    result = await db.execute(query, {
        "uid": user_id,
        "sid": body.symbol_id,
        "rule": body.rule,
        "mvr": body.min_volume_ratio,
        "mv": body.min_volatility,
        "mpc": body.min_price_change_pct,
        "msb": body.max_spread_bps,
        "ce": body.custom_expression,
        "ie": body.is_enabled,
    })
    await db.commit()
    row = result.fetchone()
    return RuleResponse(threshold_id=row[0], message="Rule saved")


@app.get("/scan/snapshot/{symbol_id}")
async def get_snapshot_history(
    symbol_id: int,
    hours: int = Query(24, ge=1, le=168),
    db: AsyncSession = Depends(get_db),
):
    """
    Snapshot metric history for a single symbol.

    Index used: idx_snap_symbol_ts (symbol_id, ts DESC)
    """
    query = text("""
        SELECT ts, current_price, price_change_pct_24h, volume_24h,
               volume_ratio, realized_volatility, volatility_percentile,
               atr_14, bid_ask_spread_bps
        FROM snapshot_metrics
        WHERE symbol_id = :sid
          AND ts >= NOW() - make_interval(hours => :hrs)
        ORDER BY ts DESC
    """)
    result = await db.execute(query, {"sid": symbol_id, "hrs": hours})
    return [dict(r._mapping) for r in result.all()]


@app.get("/scan/live-prices")
async def get_live_prices():
    """
    Fetch live prices for tracked symbols directly from Binance REST.
    Used by the frontend refresh button for instant data.
    """
    tickers = await fetch_ticker_24h_async()
    candles = await fetch_candles_for_app_symbols_async(
        [ticker["symbol"] for ticker in tickers],
        interval="1h",
        limit=24,
    )

    return [
        {
            **ticker,
            "sparkline": candles.get(ticker["symbol"], {}).get("closes", []),
        }
        for ticker in tickers
    ]


# ─── AI Trade Setup Analysis (Groq) ──────────────────────────────────────────

@app.get("/scan/ai-analysis")
async def get_ai_analysis(db: AsyncSession = Depends(get_db)):
    """
    Fetch live prices for tracked Binance pairs, enrich with
    volume_ratio from DB, optionally compute Chandelier Exit, then
    send to Groq AI for structured trade setup analysis.

    Returns JSON with long_setups, short_setups (each with limit entry, SL, TP).
    """
    from groq_ai import analyze_trade_setup

    if not settings.groq_api_key:
        raise HTTPException(503, "Groq API key not configured")

    try:
        tickers = await fetch_ticker_24h_async()
    except Exception as e:
        raise HTTPException(502, f"Binance fetch failed: {e}")

    # Load volume_ratio from DB for known symbols
    db_vol_ratios: dict[str, float] = {}
    try:
        result = await db.execute(text(
            "SELECT symbol, volume_ratio FROM v_scanner_feed"
        ))
        for row in result.mappings().all():
            db_vol_ratios[row["symbol"]] = float(row["volume_ratio"] or 0)
    except Exception:
        logger.warning("Could not load volume_ratios from DB, using Binance only")

    prices = []
    ce_candle_data = await fetch_candles_for_app_symbols_async(
        [ticker["symbol"] for ticker in tickers],
        interval="15m",
        limit=max(settings.ce_atr_period + 8, 32),
    )

    for ticker in tickers:
        symbol = ticker["symbol"]
        prices.append({
            "symbol": symbol,
            "current_price": ticker["current_price"],
            "price_change_pct_24h": ticker["price_change_pct_24h"],
            "volume_24h": ticker["volume_24h"],
            "volume_ratio": db_vol_ratios.get(symbol, 0),
        })

    # Compute Chandelier Exit from sparkline data
    ce_data: dict[str, dict] = {}
    if ce_candle_data:
        try:
            import sys
            sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "crypto"))
            from chandelier_exit import compute_ce_for_symbols
            ce_results = compute_ce_for_symbols(
                ce_candle_data,
                atr_length=settings.ce_atr_period,
                multiplier=settings.ce_atr_mult,
            )
            for sym, ce in ce_results.items():
                ce_data[sym] = {
                    "ce_dir": ce.ce_dir,
                    "ce_buySignal": ce.ce_buySignal,
                    "ce_sellSignal": ce.ce_sellSignal,
                    "longStop": ce.longStop,
                    "shortStop": ce.shortStop,
                }
        except Exception:
            logger.warning("Chandelier Exit computation failed, proceeding without CE")

    # Send to Groq AI
    analysis = await analyze_trade_setup(prices, ce_data if ce_data else None)

    return {"analysis": analysis, "coin_count": len(prices)}


# ─── Telegram Signal by Coin Category ────────────────────────────────────────

COIN_CATEGORIES: dict[str, list[str]] = {
    "meme": [
        "DOGEUSDT", "SHIBUSDT", "PEPEUSDT", "FLOKIUSDT", "BONKUSDT",
        "WIFUSDT", "MEMEUSDT", "BOMEUSDT", "TURBOUSDT", "PEOPLEUSDT",
    ],
    "regular": [
        "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
        "ADAUSDT", "AVAXUSDT", "DOTUSDT", "LINKUSDT", "LTCUSDT",
        "MATICUSDT", "UNIUSDT", "ATOMUSDT", "FILUSDT",
    ],
    "ai": [
        "FETUSDT", "RENDERUSDT", "TAOUSDT", "GRTUSDT", "NEARUSDT",
        "INJUSDT", "THETAUSDT", "ARUSDT", "WLDUSDT", "RLCUSDT",
    ],
}

CATEGORY_EMOJI = {"meme": "🐸", "regular": "💎", "ai": "🤖"}
CATEGORY_LABEL = {"meme": "Meme Coins", "regular": "Regular Coins", "ai": "AI Coins"}


class TelegramSignalRequest(BaseModel):
    category: str

    @field_validator("category")
    @classmethod
    def validate_category(cls, v: str) -> str:
        allowed = set(COIN_CATEGORIES.keys())
        if v not in allowed:
            raise ValueError(f"category must be one of {allowed}")
        return v


@app.post("/scan/telegram-signal")
async def send_telegram_signal(body: TelegramSignalRequest):
    """
    Fetch live Binance data for the chosen coin category
    and send a formatted signal with entry, stop-loss, and target profit to Telegram.
    """
    if not settings.telegram_bot_token or not settings.telegram_default_chat_id:
        raise HTTPException(503, "Telegram not configured")

    category = body.category
    symbols = COIN_CATEGORIES[category]
    emoji = CATEGORY_EMOJI[category]
    label = CATEGORY_LABEL[category]


    # Fetch 24h tickers for the category symbols
    try:
        async with httpx.AsyncClient(
            base_url=settings.binance_rest_base, timeout=20.0
        ) as client:
            resp = await client.get(
                "/api/v3/ticker/24hr",
                params={"symbols": json.dumps(symbols, separators=(",", ":"))},
            )
            resp.raise_for_status()
            raw_tickers = resp.json()
    except Exception as e:
        raise HTTPException(502, f"Binance fetch failed: {e}")

    if isinstance(raw_tickers, dict):
        raw_tickers = [raw_tickers]

    # Sort by absolute price change
    tickers = sorted(
        raw_tickers,
        key=lambda t: abs(float(t.get("priceChangePercent") or 0)),
        reverse=True,
    )

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"{emoji} <b>{label} Signal</b>",
        f"<i>{now_str}</i>",
        "",
    ]

    for i, t in enumerate(tickers, 1):
        sym = t.get("symbol", "")
        price = float(t.get("lastPrice") or 0)
        change = float(t.get("priceChangePercent") or 0)
        high = float(t.get("highPrice") or 0)
        low = float(t.get("lowPrice") or 0)
        vol = float(t.get("quoteVolume") or 0)

        if price <= 0:
            continue

        # ── Trading rules based on 24h range & momentum ──
        range_24h = high - low if high > low else price * 0.02
        is_long = change >= 0
        direction = "LONG 🟢" if is_long else "SHORT 🔴"

        if is_long:
            # Pullback entry near support, SL below 24h low, TP at range extension
            entry = price - (range_24h * 0.15)          # enter on ~15% pullback
            stop_loss = low - (range_24h * 0.10)         # SL 10% below 24h low
            tp1 = price + (range_24h * 0.50)             # TP1: 50% range extension
            tp2 = price + (range_24h * 1.00)             # TP2: full range extension
            tp3 = high + (range_24h * 0.50)              # TP3: breakout target
        else:
            # Short entry near resistance, SL above 24h high, TP at range drop
            entry = price + (range_24h * 0.15)
            stop_loss = high + (range_24h * 0.10)
            tp1 = price - (range_24h * 0.50)
            tp2 = price - (range_24h * 1.00)
            tp3 = low - (range_24h * 0.50)

        # Risk/reward ratio
        risk = abs(entry - stop_loss) if abs(entry - stop_loss) > 0 else 1
        rr1 = abs(tp1 - entry) / risk
        vol_m = vol / 1_000_000

        def fmt(v: float) -> str:
            if v >= 1000:
                return f"{v:.2f}"
            if v >= 1:
                return f"{v:.4f}"
            if v >= 0.01:
                return f"{v:.6f}"
            return f"{v:.8f}"

        lines.append(f"{'━' * 28}")
        lines.append(f"{i}. <b>{sym}</b>  {direction}")
        lines.append(f"   💰 Price: <code>{fmt(price)}</code>  ({change:+.2f}%)")
        lines.append(f"   📊 24h H/L: <code>{fmt(high)}</code> / <code>{fmt(low)}</code>")
        lines.append(f"   📈 Vol: <code>${vol_m:.1f}M</code>")
        lines.append("")
        lines.append(f"   ▸ Entry:     <code>{fmt(entry)}</code>")
        lines.append(f"   ▸ Stop Loss: <code>{fmt(stop_loss)}</code>")
        lines.append(f"   ▸ TP1:       <code>{fmt(tp1)}</code>")
        lines.append(f"   ▸ TP2:       <code>{fmt(tp2)}</code>")
        lines.append(f"   ▸ TP3:       <code>{fmt(tp3)}</code>")
        lines.append(f"   ▸ R:R →      <code>1:{rr1:.1f}</code>")
        lines.append("")

    lines.append(f"{'━' * 28}")
    lines.append("⚠️ <i>DYOR — Not financial advice</i>")

    text_msg = "\n".join(lines)

    # Send via Telegram Bot API
    url = f"https://api.telegram.org/bot{settings.telegram_bot_token}/sendMessage"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            tg_resp = await client.post(url, json={
                "chat_id": settings.telegram_default_chat_id,
                "text": text_msg,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            })
        if tg_resp.status_code >= 400:
            logger.warning("Telegram signal %d: %s", tg_resp.status_code, tg_resp.text[:200])
            raise HTTPException(502, "Telegram delivery failed")
    except httpx.HTTPError as e:
        raise HTTPException(502, f"Telegram request failed: {e}")

    return {
        "status": "sent",
        "category": category,
        "coins": len(tickers),
        "message": f"{label} signal sent to Telegram",
    }
