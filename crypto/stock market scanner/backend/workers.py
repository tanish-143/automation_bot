"""
Celery Workers — Scheduled Ingestion & Scan Jobs
==================================================

Job structure:
  ┌──────────────────────────────────────────────────────────────────┐
  │  Beat scheduler (cron)                                          │
  │  ├── ingest_tickers      every 60s   fetch 24h ticker REST      │
  │  ├── ingest_candles      every 60s   fetch 1h/4h candles REST   │
  │  ├── run_scan_cycle      every 60s   compute metrics + rules    │
  │  └── cleanup_stale       every 300s  mark stale feeds           │
  │                                                                  │
  │  Each task is idempotent — safe to retry on failure.             │
  └──────────────────────────────────────────────────────────────────┘

Worker startup:
  celery -A workers.celery_app worker --loglevel=info --concurrency=4
  celery -A workers.celery_app beat   --loglevel=info
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from celery import Celery

from config import settings
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

# ─── Celery App ───────────────────────────────────────────────────────────────

celery_app = Celery(
    "scanner_workers",
    broker=settings.redis_url,
    backend=settings.redis_url,
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_acks_late=True,                # re-deliver if worker crashes mid-task
    worker_prefetch_multiplier=1,       # one task at a time per worker process
    task_reject_on_worker_lost=True,
    task_default_retry_delay=10,
    task_max_retries=3,

    # Beat schedule — cron-triggered periodic tasks
    beat_schedule={
        "ingest-tickers-every-60s": {
            "task": "workers.ingest_tickers",
            "schedule": 60.0,
        },
        "ingest-candles-every-60s": {
            "task": "workers.ingest_candles",
            "schedule": 60.0,
        },
        "run-scan-cycle-every-60s": {
            "task": "workers.run_scan_cycle",
            "schedule": 60.0,
        },
        "cleanup-stale-every-5m": {
            "task": "workers.cleanup_stale_feeds",
            "schedule": 300.0,
        },
    },
)


# ─── Rate Limiter (shared via Redis) ─────────────────────────────────────────

class BinanceRateLimiter:
    """
    Token-bucket rate limiter backed by Redis.

    Binance limits: 1200 request weight / minute for REST API.
    Each endpoint has a different weight; most ticker/candle calls = weight 1-5.

    Strategy: before each request, consume tokens. If bucket empty, sleep.
    """

    def __init__(self, redis_url: str, max_tokens: int = 1200, window_seconds: int = 60):
        import redis
        self.r = redis.from_url(redis_url)
        self.key = "binance:rate_limit"
        self.max_tokens = max_tokens
        self.window = window_seconds

    def acquire(self, weight: int = 1) -> bool:
        """Try to consume `weight` tokens. Returns True if allowed."""
        pipe = self.r.pipeline()
        now = int(datetime.now(timezone.utc).timestamp())
        window_start = now - self.window

        # Sliding window: remove old entries, count recent, add new
        pipe.zremrangebyscore(self.key, 0, window_start)
        pipe.zcard(self.key)
        results = pipe.execute()
        current_count = results[1]

        if current_count + weight > self.max_tokens:
            return False

        # Record this request (score = timestamp, member = unique id)
        import uuid
        pipe = self.r.pipeline()
        for _ in range(weight):
            pipe.zadd(self.key, {str(uuid.uuid4()): now})
        pipe.expire(self.key, self.window + 5)
        pipe.execute()
        return True

    def wait_and_acquire(self, weight: int = 1, max_wait: float = 5.0):
        """Block until tokens are available, up to max_wait seconds."""
        import time
        deadline = time.monotonic() + max_wait
        while time.monotonic() < deadline:
            if self.acquire(weight):
                return True
            time.sleep(0.2)
        raise RuntimeError(f"Rate limit: could not acquire {weight} tokens in {max_wait}s")


rate_limiter = BinanceRateLimiter(settings.redis_url, settings.binance_rate_limit_per_min)


# ─── Binance REST Client ─────────────────────────────────────────────────────

def _binance_get(path: str, params: dict | None = None, weight: int = 1) -> dict | list:
    """
    GET request to Binance REST API with rate limiting and retry.

    Retries on 429 (rate limited) and 5xx with exponential backoff.
    """
    import time
    import httpx

    rate_limiter.wait_and_acquire(weight)
    url = f"{settings.binance_rest_base}{path}"

    for attempt in range(4):
        try:
            resp = httpx.get(url, params=params, timeout=10.0)

            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 5))
                logger.warning("Binance 429 — backing off %ds", retry_after)
                time.sleep(retry_after)
                continue

            if resp.status_code >= 500:
                wait = 2 ** attempt
                logger.warning("Binance %d — retry in %ds", resp.status_code, wait)
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()

        except httpx.TimeoutException:
            if attempt < 3:
                time.sleep(2 ** attempt)
                continue
            raise

    raise RuntimeError(f"Binance API failed after 4 attempts: {path}")


# ─── Task: Ingest 24h Tickers ────────────────────────────────────────────────

@celery_app.task(name="workers.ingest_tickers", bind=True, max_retries=3)
def ingest_tickers(self):
    """
    Fetch all 24h ticker stats from Binance in ONE batch call.

    Binance endpoint: GET /api/v3/ticker/24hr  (weight: 40)
    Returns ~2000 symbols in a single response — no pagination needed.

    Flow:
      1. GET /api/v3/ticker/24hr  → all tickers
      2. Filter to active symbols in our DB
      3. Upsert into snapshot_metrics
    """
    try:
        tickers = _binance_get("/api/v3/ticker/24hr", weight=40)
        logger.info("Fetched %d tickers from Binance", len(tickers))

        # Bulk upsert into DB
        from sqlalchemy import create_engine, text as sa_text
        engine = create_engine(settings.database_url_sync)
        now = datetime.now(timezone.utc)

        with engine.begin() as conn:
            # Get our tracked symbols
            tracked = conn.execute(sa_text(
                "SELECT symbol_id, symbol FROM symbols WHERE is_active = TRUE AND exchange = 'binance'"
            )).fetchall()
            symbol_map = {row[1].replace("/", ""): row[0] for row in tracked}

            inserted = 0
            for t in tickers:
                sym = t.get("symbol", "")
                if sym not in symbol_map:
                    continue

                conn.execute(sa_text("""
                    INSERT INTO snapshot_metrics (
                        ts, symbol_id, exchange, asset_class,
                        current_price, price_change_pct_24h,
                        high_24h, low_24h, volume_24h
                    ) VALUES (
                        :ts, :sid, 'binance', 'crypto',
                        :price, :change_pct,
                        :high, :low, :vol
                    )
                    ON CONFLICT (ts, symbol_id) DO UPDATE SET
                        current_price = EXCLUDED.current_price,
                        price_change_pct_24h = EXCLUDED.price_change_pct_24h,
                        volume_24h = EXCLUDED.volume_24h
                """), {
                    "ts": now,
                    "sid": symbol_map[sym],
                    "price": float(t.get("lastPrice", 0)),
                    "change_pct": float(t.get("priceChangePercent", 0)),
                    "high": float(t.get("highPrice", 0)),
                    "low": float(t.get("lowPrice", 0)),
                    "vol": float(t.get("quoteVolume", 0)),
                })
                inserted += 1

        logger.info("Ingested %d ticker snapshots", inserted)
        return {"status": "ok", "count": inserted}

    except Exception as exc:
        logger.exception("ingest_tickers failed")
        raise self.retry(exc=exc, countdown=2 ** self.request.retries)


# ─── Task: Ingest Candles ────────────────────────────────────────────────────

@celery_app.task(name="workers.ingest_candles", bind=True, max_retries=3)
def ingest_candles(self):
    """
    Fetch latest 1h and 4h candles for tracked symbols.

    Strategy: batch symbols into groups to stay under rate limits.
    Each klines call = weight 1, so 200 symbols × 2 intervals = 400 weight.

    Flow:
      1. Get list of active symbols from DB
      2. For each symbol: GET /api/v3/klines?symbol=X&interval=1h&limit=2
      3. Upsert candle rows
    """
    try:
        from sqlalchemy import create_engine, text as sa_text
        engine = create_engine(settings.database_url_sync)

        with engine.begin() as conn:
            tracked = conn.execute(sa_text(
                "SELECT symbol_id, symbol FROM symbols "
                "WHERE is_active = TRUE AND exchange = 'binance' "
                "ORDER BY symbol_id LIMIT 200"
            )).fetchall()

            inserted = 0
            for symbol_id, symbol in tracked:
                binance_symbol = symbol.replace("/", "")

                for interval in ["1h", "4h"]:
                    try:
                        klines = _binance_get(
                            "/api/v3/klines",
                            params={"symbol": binance_symbol, "interval": interval, "limit": 2},
                            weight=1,
                        )
                    except Exception:
                        logger.warning("Failed klines for %s %s", binance_symbol, interval)
                        continue

                    for k in klines:
                        conn.execute(sa_text("""
                            INSERT INTO candles (ts, symbol_id, interval, open, high, low, close,
                                                 volume, quote_volume, trade_count, is_closed)
                            VALUES (:ts, :sid, :intv::candle_interval, :o, :h, :l, :c,
                                    :v, :qv, :tc, :closed)
                            ON CONFLICT (ts, symbol_id, interval) DO UPDATE SET
                                close = EXCLUDED.close,
                                high = GREATEST(candles.high, EXCLUDED.high),
                                low = LEAST(candles.low, EXCLUDED.low),
                                volume = EXCLUDED.volume,
                                is_closed = EXCLUDED.is_closed
                        """), {
                            "ts": datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc),
                            "sid": symbol_id,
                            "intv": interval,
                            "o": float(k[1]),
                            "h": float(k[2]),
                            "l": float(k[3]),
                            "c": float(k[4]),
                            "v": float(k[5]),
                            "qv": float(k[7]),
                            "tc": int(k[8]),
                            "closed": bool(k[6] < k[0] + 3_600_000),
                        })
                        inserted += 1

            logger.info("Ingested %d candle rows", inserted)
            return {"status": "ok", "count": inserted}

    except Exception as exc:
        logger.exception("ingest_candles failed")
        raise self.retry(exc=exc, countdown=2 ** self.request.retries)


# ─── Task: Run Scan Cycle ────────────────────────────────────────────────────

@celery_app.task(name="workers.run_scan_cycle", bind=True, max_retries=2)
def run_scan_cycle(self):
    """
    Main scan cycle — orchestrates the detection pipeline.

    Flow:
      1. DB: compute volume_ratios + realized_volatility (SQL functions)
      2. DB: read v_scanner_feed (snapshot + percentiles)
      3. App: run detection_rules.run_scan()
      4. App: dispatch alerts for any triggered rules
      5. DB: persist new alerts

    This is the core pipeline that ties ingestion → detection → alerting.
    """
    try:
        import sys
        import os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "crypto"))
        from detection_rules import (
            SnapshotRow, TradingSession,
            run_scan,
        )
        from sqlalchemy import create_engine, text as sa_text
        engine = create_engine(settings.database_url_sync)
        now = datetime.now(timezone.utc)

        with engine.begin() as conn:
            # Step 1: Update computed metrics
            conn.execute(sa_text("""
                UPDATE snapshot_metrics sm SET
                    volume_ratio = vr.volume_ratio
                FROM fn_compute_volume_ratios() vr
                WHERE sm.symbol_id = vr.symbol_id
                  AND sm.ts = (SELECT MAX(ts) FROM snapshot_metrics WHERE symbol_id = sm.symbol_id)
            """))

            conn.execute(sa_text("""
                UPDATE snapshot_metrics sm SET
                    realized_volatility = rv.realized_volatility
                FROM fn_compute_realized_volatility() rv
                WHERE sm.symbol_id = rv.symbol_id
                  AND sm.ts = (SELECT MAX(ts) FROM snapshot_metrics WHERE symbol_id = sm.symbol_id)
            """))

            # Step 2: Read scanner feed
            rows = conn.execute(sa_text(
                "SELECT * FROM v_scanner_feed"
            )).fetchall()

            snapshots = []
            for r in rows:
                snapshots.append(SnapshotRow(
                    symbol_id=r.symbol_id,
                    symbol=r.symbol,
                    exchange=r.exchange,
                    asset_class=r.asset_class,
                    ts=r.ts,
                    current_price=float(r.current_price),
                    price_change_pct_24h=float(r.price_change_pct_24h or 0),
                    volume_24h=float(r.volume_24h or 0),
                    volume_ratio=float(r.volume_ratio or 0),
                    realized_volatility=float(r.realized_volatility or 0),
                    volatility_percentile=float(r.volatility_pctile or 0),
                    atr_14=float(r.atr_14 or 0),
                    bid_ask_spread_bps=float(r.bid_ask_spread_bps or 0),
                ))

            # Step 3: Run detection rules
            # Determine current session
            hour = now.hour
            session = None
            if 0 <= hour < 8:
                session = TradingSession.ASIA
            elif 7 <= hour < 16:
                session = TradingSession.EUROPE
            elif 13 <= hour < 21:
                session = TradingSession.US

            results = run_scan(snapshots, session=session, now=now)

            # Step 4: Persist alerts & dispatch notifications
            total_alerts = 0
            for rule_name, alerts in results.items():
                for alert in alerts:
                    # Get all users subscribed to this symbol/rule
                    # (simplified: alert all users with matching thresholds)
                    users = conn.execute(sa_text("""
                        SELECT DISTINCT user_id FROM user_thresholds
                        WHERE rule = :rule::alert_rule
                          AND is_enabled = TRUE
                          AND (symbol_id IS NULL OR symbol_id = :sid)
                    """), {"rule": rule_name, "sid": alert.symbol_id}).fetchall()

                    for (user_id,) in users:
                        conn.execute(sa_text("""
                            SELECT fn_insert_alert(
                                :uid, :sid, :rule::alert_rule, :price,
                                :vol_ratio, :volatility, :spread, :msg, NULL
                            )
                        """), {
                            "uid": user_id,
                            "sid": alert.symbol_id,
                            "rule": rule_name,
                            "price": alert.trigger_price,
                            "vol_ratio": alert.trigger_volume_ratio,
                            "volatility": alert.trigger_volatility,
                            "spread": alert.trigger_spread_bps,
                            "msg": alert.message,
                        })
                        total_alerts += 1

                    # Dispatch notifications asynchronously
                    if alerts:
                        dispatch_alerts.delay([
                            {"symbol": a.symbol, "rule": a.rule.value, "message": a.message}
                            for a in alerts
                        ])

            logger.info("Scan cycle: %d snapshots, %d alerts", len(snapshots), total_alerts)
            return {"snapshots": len(snapshots), "alerts": total_alerts}

    except Exception as exc:
        logger.exception("run_scan_cycle failed")
        raise self.retry(exc=exc, countdown=10)


# ─── Task: Dispatch Alerts ───────────────────────────────────────────────────

@celery_app.task(name="workers.dispatch_alerts", bind=True, max_retries=3)
def dispatch_alerts(self, alert_payloads: list[dict]):
    """
    Fan-out alert notifications to all configured channels.
    Delegates to the alert_dispatcher module.
    """
    try:
        from alert_dispatcher import AlertDispatcher
        dispatcher = AlertDispatcher()

        for payload in alert_payloads:
            dispatcher.send_all(
                symbol=payload["symbol"],
                rule=payload["rule"],
                message=payload["message"],
            )

        return {"dispatched": len(alert_payloads)}

    except Exception as exc:
        logger.exception("dispatch_alerts failed")
        raise self.retry(exc=exc, countdown=2 ** self.request.retries)


# ─── Task: Cleanup Stale Feeds ──────────────────────────────────────────────

@celery_app.task(name="workers.cleanup_stale_feeds")
def cleanup_stale_feeds():
    """Mark symbols with no recent data as stale for dashboard visibility."""
    from sqlalchemy import create_engine, text as sa_text
    engine = create_engine(settings.database_url_sync)

    with engine.begin() as conn:
        result = conn.execute(sa_text("""
            SELECT COUNT(*) FROM v_stale_feeds
        """))
        stale_count = result.scalar()
        logger.info("Stale feed check: %d symbols without recent data", stale_count)
        return {"stale_symbols": stale_count}
