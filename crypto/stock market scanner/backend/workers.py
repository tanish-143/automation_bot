"""
Celery Workers — Scheduled Ingestion & Scan Jobs (Binance REST)
===============================================================

Job structure:
  ┌──────────────────────────────────────────────────────────────────┐
  │  Beat scheduler (cron)                                          │
    │  ├── ingest_tickers      every 90s   Binance /api/v3/ticker/24hr│
  │  ├── run_scan_cycle      every 90s   compute metrics + rules    │
  │  └── cleanup_stale       every 300s  mark stale feeds           │
  │                                                                  │
  │  Each task is idempotent — safe to retry on failure.             │
  └──────────────────────────────────────────────────────────────────┘

Data source: Binance REST API.
"""

from __future__ import annotations

import csv
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from celery import Celery

from binance_client import fetch_ticker_24h_sync
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


# ─── Task: Ingest Tickers (Binance REST) ─────────────────────────────────────

@celery_app.task(name="workers.ingest_tickers", bind=True, max_retries=3)
def ingest_tickers(self):
    """
    Fetch market data from Binance /api/v3/ticker/24hr.
    Single call returns price, 24h change, volume, bid/ask, and high/low.
    """
    try:
        data = fetch_ticker_24h_sync()
        logger.info("Fetched %d symbols from Binance REST", len(data))

        from sqlalchemy import create_engine, text as sa_text
        engine = create_engine(settings.database_url_sync)
        now = datetime.now(timezone.utc)

        with engine.begin() as conn:
            tracked = conn.execute(sa_text(
                "SELECT symbol_id, symbol FROM symbols WHERE is_active = TRUE"
            )).fetchall()
            symbol_map = {row[1]: row[0] for row in tracked}

            inserted = 0
            for ticker in data:
                our_symbol = ticker["symbol"]
                if our_symbol not in symbol_map:
                    continue

                price = ticker["current_price"]
                change_pct = ticker["price_change_pct_24h"]
                high = ticker["high_24h"]
                low = ticker["low_24h"]
                volume = ticker["volume_24h"]
                bid_price = ticker["bid_price"]
                ask_price = ticker["ask_price"]
                spread_bps = ticker["bid_ask_spread_bps"]

                conn.execute(sa_text("""
                    INSERT INTO snapshot_metrics (
                        ts, symbol_id, exchange, asset_class,
                        current_price, price_change_pct_24h,
                        high_24h, low_24h, volume_24h,
                        bid_price, ask_price, bid_ask_spread_bps
                    ) VALUES (
                        :ts, :sid, 'binance', 'crypto',
                        :price, :change_pct,
                        :high, :low, :vol,
                        :bid_price, :ask_price, :spread_bps
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
                    "bid_price": bid_price,
                    "ask_price": ask_price,
                    "spread_bps": spread_bps,
                })
                inserted += 1

        logger.info("Ingested %d ticker snapshots from Binance REST", inserted)
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
            snapshot_by_symbol_id = {snapshot.symbol_id: snapshot for snapshot in snapshots}

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
            #
            # The DB alert_rule enum only supports: volume_spike,
            # volatility_breakout, spread_widening, price_change_pct, custom.
            # Map scanner rule keys that don't exist in the enum.
            _RULE_TO_DB = {
                "volume_spike": "volume_spike",
                "volatility_breakout": "volatility_breakout",
                "combined": "volume_spike",          # highest-conviction combo → store as volume_spike
                "session_activity": "custom",         # informational → custom
                "composite_rank": "custom",           # ranking → custom
            }

            total_alerts = 0
            all_dispatched: list[dict] = []

            for rule_name, alerts in results.items():
                db_rule = _RULE_TO_DB.get(rule_name, "custom")

                for alert in alerts:
                    # Tag the message so the original rule is recoverable
                    msg = alert.message
                    if db_rule != rule_name:
                        msg = f"[{rule_name}] {msg}"

                    users = conn.execute(sa_text("""
                        SELECT DISTINCT user_id FROM user_thresholds
                        WHERE rule = CAST(:rule AS alert_rule)
                          AND is_enabled = TRUE
                          AND (symbol_id IS NULL OR symbol_id = :sid)
                    """), {"rule": db_rule, "sid": alert.symbol_id}).fetchall()

                    for (user_id,) in users:
                        conn.execute(sa_text("""
                            SELECT fn_insert_alert(
                                :uid, :sid, CAST(:rule AS alert_rule), :price,
                                :vol_ratio, :volatility, :spread, :msg, NULL
                            )
                        """), {
                            "uid": user_id,
                            "sid": alert.symbol_id,
                            "rule": db_rule,
                            "price": alert.trigger_price,
                            "vol_ratio": alert.trigger_volume_ratio,
                            "volatility": alert.trigger_volatility,
                            "spread": alert.trigger_spread_bps,
                            "msg": msg,
                        })
                        total_alerts += 1

                    snapshot = snapshot_by_symbol_id.get(alert.symbol_id)
                    all_dispatched.append({
                        "symbol": alert.symbol,
                        "rule": alert.rule.value,
                        "message": msg,
                        "trigger_price": alert.trigger_price,
                        "trigger_volume_ratio": alert.trigger_volume_ratio,
                        "trigger_volatility": alert.trigger_volatility,
                        "price_change_pct_24h": snapshot.price_change_pct_24h if snapshot else 0.0,
                        "atr_14": snapshot.atr_14 if snapshot else 0.0,
                    })

            # Dispatch all alerts in one batch
            if all_dispatched:
                dispatch_alerts.delay(all_dispatched)

            # Step 5: CSV export
            _export_alerts_csv(results, now)

            logger.info("Scan cycle: %d snapshots, %d alerts", len(snapshots), total_alerts)
            return {"snapshots": len(snapshots), "alerts": total_alerts}

    except Exception as exc:
        logger.exception("run_scan_cycle failed")
        raise self.retry(exc=exc, countdown=10)


# ─── CSV Alert Export ─────────────────────────────────────────────────────────

def _export_alerts_csv(
    results: dict[str, list],
    now: datetime,
) -> None:
    """Write all alerts from the latest scan cycle to a timestamped CSV."""
    export_dir = Path(settings.alerts_export_dir)
    try:
        export_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        logger.warning("Cannot create alerts export dir %s: %s", export_dir, e)
        return

    filename = now.strftime("scanner_alerts_%Y%m%d_%H%M.csv")
    filepath = export_dir / filename

    rows_written = 0
    try:
        with open(filepath, "w", newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["Timestamp", "Symbol", "Rule", "Message", "Price", "Change%", "VolRatio"])

            for rule_name, alerts in results.items():
                for a in alerts:
                    writer.writerow([
                        a.ts.strftime("%Y-%m-%d %H:%M:%S"),
                        a.symbol,
                        rule_name,
                        a.message,
                        f"{a.trigger_price:.4f}" if a.trigger_price else "",
                        "",  # Change% not stored on Alert directly
                        f"{a.trigger_volume_ratio:.2f}" if a.trigger_volume_ratio else "",
                    ])
                    rows_written += 1

        logger.info("CSV export: %d alerts → %s", rows_written, filepath)
    except OSError as e:
        logger.warning("CSV export failed: %s", e)


# ─── Task: Dispatch Alerts ───────────────────────────────────────────────────

@celery_app.task(name="workers.dispatch_alerts", bind=True, max_retries=3)
def dispatch_alerts(self, alert_payloads: list[dict]):
    """Fan-out alert notifications to all configured channels."""
    try:
        from alert_dispatcher import AlertDispatcher
        dispatcher = AlertDispatcher()

        dispatcher.send_batch(alert_payloads)

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
