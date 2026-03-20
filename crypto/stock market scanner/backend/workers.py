"""
Celery Workers — Scheduled Ingestion & Scan Jobs (CoinGecko)
=============================================================

Job structure:
  ┌──────────────────────────────────────────────────────────────────┐
  │  Beat scheduler (cron)                                          │
  │  ├── ingest_tickers      every 90s   CoinGecko /coins/markets   │
  │  ├── run_scan_cycle      every 90s   compute metrics + rules    │
  │  └── cleanup_stale       every 300s  mark stale feeds           │
  │                                                                  │
  │  Each task is idempotent — safe to retry on failure.             │
  └──────────────────────────────────────────────────────────────────┘

Data source: CoinGecko Demo API (key sent via x-cg-demo-api-key header).
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone

from celery import Celery

from config import settings
from logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

# ─── Symbol Mapping ──────────────────────────────────────────────────────────
# Maps our DB symbol format (BTC/USDT) → CoinGecko coin ID

SYMBOL_TO_COINGECKO: dict[str, str] = {
    "BTC/USDT": "bitcoin",
    "ETH/USDT": "ethereum",
    "SOL/USDT": "solana",
    "BNB/USDT": "binancecoin",
    "XRP/USDT": "ripple",
    "ADA/USDT": "cardano",
    "DOGE/USDT": "dogecoin",
    "AVAX/USDT": "avalanche-2",
    "DOT/USDT": "polkadot",
    "MATIC/USDT": "matic-network",
    "LINK/USDT": "chainlink",
    "UNI/USDT": "uniswap",
    "ATOM/USDT": "cosmos",
    "LTC/USDT": "litecoin",
    "FIL/USDT": "filecoin",
}

COINGECKO_TO_SYMBOL: dict[str, str] = {v: k for k, v in SYMBOL_TO_COINGECKO.items()}


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
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_reject_on_worker_lost=True,
    task_default_retry_delay=10,
    task_max_retries=3,

    beat_schedule={
        "ingest-tickers-every-90s": {
            "task": "workers.ingest_tickers",
            "schedule": 90.0,
        },
        "run-scan-cycle-every-90s": {
            "task": "workers.run_scan_cycle",
            "schedule": 90.0,
        },
        "cleanup-stale-every-5m": {
            "task": "workers.cleanup_stale_feeds",
            "schedule": 300.0,
        },
    },
)


# ─── Rate Limiter (simple for CoinGecko free tier) ───────────────────────────

_last_request_time = 0.0
MIN_REQUEST_GAP = 2.5  # seconds between CoinGecko requests (free tier safe)


def _coingecko_get(path: str, params: dict | None = None) -> dict | list:
    """
    GET request to CoinGecko Demo API with rate limiting and retry.
    Sends API key via x-cg-demo-api-key header when configured.
    """
    import httpx
    global _last_request_time

    elapsed = time.monotonic() - _last_request_time
    if elapsed < MIN_REQUEST_GAP:
        time.sleep(MIN_REQUEST_GAP - elapsed)

    url = f"{settings.coingecko_rest_base}{path}"
    headers = {}
    if settings.coingecko_api_key:
        headers["x-cg-demo-api-key"] = settings.coingecko_api_key

    for attempt in range(3):
        try:
            _last_request_time = time.monotonic()
            resp = httpx.get(url, params=params, headers=headers, timeout=15.0)

            if resp.status_code == 429:
                wait = int(resp.headers.get("Retry-After", 30))
                logger.warning("CoinGecko 429 — backing off %ds", wait)
                time.sleep(wait)
                continue

            if resp.status_code >= 500:
                wait = 2 ** (attempt + 1)
                logger.warning("CoinGecko %d — retry in %ds", resp.status_code, wait)
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp.json()

        except httpx.TimeoutException:
            if attempt < 2:
                time.sleep(2 ** (attempt + 1))
                continue
            raise

    raise RuntimeError(f"CoinGecko API failed after 3 attempts: {path}")


# ─── Task: Ingest Tickers (CoinGecko) ────────────────────────────────────────

@celery_app.task(name="workers.ingest_tickers", bind=True, max_retries=3)
def ingest_tickers(self):
    """
    Fetch market data from CoinGecko /coins/markets endpoint.
    Single call returns price, 24h change, volume, high/low for all coins.
    """
    try:
        coin_ids = ",".join(SYMBOL_TO_COINGECKO.values())
        data = _coingecko_get("/coins/markets", params={
            "vs_currency": "usd",
            "ids": coin_ids,
            "order": "market_cap_desc",
            "per_page": 50,
            "page": 1,
            "sparkline": "false",
            "price_change_percentage": "24h",
        })
        logger.info("Fetched %d coins from CoinGecko", len(data))

        from sqlalchemy import create_engine, text as sa_text
        engine = create_engine(settings.database_url_sync)
        now = datetime.now(timezone.utc)

        with engine.begin() as conn:
            tracked = conn.execute(sa_text(
                "SELECT symbol_id, symbol FROM symbols WHERE is_active = TRUE"
            )).fetchall()
            symbol_map = {row[1]: row[0] for row in tracked}

            inserted = 0
            for coin in data:
                cg_id = coin.get("id", "")
                our_symbol = COINGECKO_TO_SYMBOL.get(cg_id)
                if not our_symbol or our_symbol not in symbol_map:
                    continue

                price = coin.get("current_price") or 0
                change_pct = coin.get("price_change_percentage_24h") or 0
                high = coin.get("high_24h") or 0
                low = coin.get("low_24h") or 0
                volume = coin.get("total_volume") or 0

                conn.execute(sa_text("""
                    INSERT INTO snapshot_metrics (
                        ts, symbol_id, exchange, asset_class,
                        current_price, price_change_pct_24h,
                        high_24h, low_24h, volume_24h,
                        bid_price, ask_price, bid_ask_spread_bps
                    ) VALUES (
                        :ts, :sid, 'coingecko', 'crypto',
                        :price, :change_pct,
                        :high, :low, :vol,
                        :price, :price, 0
                    )
                    ON CONFLICT (ts, symbol_id) DO UPDATE SET
                        current_price = EXCLUDED.current_price,
                        price_change_pct_24h = EXCLUDED.price_change_pct_24h,
                        high_24h = EXCLUDED.high_24h,
                        low_24h = EXCLUDED.low_24h,
                        volume_24h = EXCLUDED.volume_24h
                """), {
                    "ts": now,
                    "sid": symbol_map[our_symbol],
                    "price": price,
                    "change_pct": change_pct,
                    "high": high,
                    "low": low,
                    "vol": volume,
                })
                inserted += 1

        logger.info("Ingested %d ticker snapshots from CoinGecko", inserted)
        return {"status": "ok", "count": inserted}

    except Exception as exc:
        logger.exception("ingest_tickers failed")
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
                    users = conn.execute(sa_text("""
                        SELECT DISTINCT user_id FROM user_thresholds
                        WHERE rule = CAST(:rule AS alert_rule)
                          AND is_enabled = TRUE
                          AND (symbol_id IS NULL OR symbol_id = :sid)
                    """), {"rule": rule_name, "sid": alert.symbol_id}).fetchall()

                    for (user_id,) in users:
                        conn.execute(sa_text("""
                            SELECT fn_insert_alert(
                                :uid, :sid, CAST(:rule AS alert_rule), :price,
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
    """Fan-out alert notifications to all configured channels."""
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
