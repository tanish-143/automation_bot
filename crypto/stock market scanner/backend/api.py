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

import logging
from datetime import datetime, timezone
from enum import Enum

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, field_validator
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

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
async def health(db: AsyncSession = Depends(get_db)):
    await db.execute(text("SELECT 1"))
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
