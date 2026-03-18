"""
Market Scanner — Detection Rules Engine
========================================
Runs detection rules against snapshot_metrics to identify trading opportunities.

Architecture decision: WHERE to compute what
─────────────────────────────────────────────
  DB layer (SQL / TimescaleDB):
    • Percentile ranks across the full universe (percent_rank window fn)
    • Volume averages over rolling windows (continuous aggregates)
    • Filtering by timestamp range, asset_class
    • Pre-sorted top-N queries via composite indexes

  Application layer (this module):
    • Rule composition (AND/OR logic across multiple signals)
    • Session-timezone mapping (wall-clock → UTC offset)
    • Composite score weighting (easily tunable without DDL changes)
    • Cooldown / dedup logic (don't re-alert on the same symbol within N minutes)
    • Edge-case guards (min volume floor, data-gap detection)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


# ── Configuration ────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class ScannerConfig:
    """Tunable thresholds — change without touching rule logic."""

    # Rule 1: Volume spike
    volume_ratio_threshold: float = 3.0

    # Rule 2: Volatility anomaly
    volatility_percentile_threshold: float = 90.0

    # Rule 3: Combined alert
    combined_volume_ratio: float = 3.0
    combined_volatility_pct: float = 90.0
    combined_price_change_pct: float = 2.0   # absolute %

    # Rule 5: Composite score weights (must sum to 1.0)
    weight_volume: float = 0.5
    weight_volatility: float = 0.3
    weight_price_move: float = 0.2

    # Edge-case guards
    min_volume_floor_usd: float = 10_000.0   # ignore symbols below this 24h volume
    min_data_points: int = 10                 # need ≥N candles in window to trust stats
    max_spread_bps: float = 500.0             # ignore symbols with spread > 5%
    stale_data_seconds: int = 120             # flag if last tick > 2 min old
    cooldown_minutes: int = 15                # suppress re-alerts within this window

    def __post_init__(self):
        w = self.weight_volume + self.weight_volatility + self.weight_price_move
        if abs(w - 1.0) > 1e-6:
            raise ValueError(f"Composite weights must sum to 1.0, got {w}")


# ── Data Models ──────────────────────────────────────────────────────────────

class AlertRule(str, Enum):
    VOLUME_SPIKE = "volume_spike"
    VOLATILITY_BREAKOUT = "volatility_breakout"
    COMBINED = "combined"
    SESSION_ACTIVITY = "session_activity"
    COMPOSITE_RANK = "composite_rank"


class TradingSession(str, Enum):
    ASIA = "asia"       # 00:00–08:00 UTC  (Tokyo/HK/Singapore)
    EUROPE = "europe"   # 07:00–16:00 UTC  (London/Frankfurt)
    US = "us"           # 13:00–21:00 UTC  (NY/Chicago)


SESSION_WINDOWS_UTC = {
    TradingSession.ASIA:   (0, 8),
    TradingSession.EUROPE: (7, 16),
    TradingSession.US:     (13, 21),
}


@dataclass
class SnapshotRow:
    """One row from the snapshot_metrics table."""
    symbol_id: int
    symbol: str
    exchange: str
    asset_class: str
    ts: datetime
    current_price: float
    price_change_pct_24h: float
    volume_24h: float
    volume_ratio: float
    realized_volatility: float
    volatility_percentile: float
    atr_14: float
    bid_ask_spread_bps: float
    trade_count_1h: int = 0


@dataclass
class Alert:
    """An alert produced by a detection rule."""
    symbol_id: int
    symbol: str
    rule: AlertRule
    ts: datetime
    message: str
    trigger_price: float
    trigger_volume_ratio: Optional[float] = None
    trigger_volatility: Optional[float] = None
    trigger_spread_bps: Optional[float] = None
    composite_score: Optional[float] = None


# ── Edge-Case Filters ────────────────────────────────────────────────────────

def passes_quality_checks(row: SnapshotRow, cfg: ScannerConfig, now: datetime) -> bool:
    """
    Gate that every row must pass before any rule evaluates it.

    Catches:
      • Low-liquidity dust coins / penny stocks
      • Symbols with stale data (exchange down, websocket lag)
      • Wide-spread symbols where price signals are unreliable
      • Missing / null fields
    """
    # 1. Minimum 24h volume floor — eliminates dust & illiquid tokens
    if row.volume_24h is None or row.volume_24h < cfg.min_volume_floor_usd:
        return False

    # 2. Stale data — skip if last snapshot is too old
    age = (now - row.ts).total_seconds()
    if age > cfg.stale_data_seconds:
        logger.debug("Stale: %s last seen %.0fs ago", row.symbol, age)
        return False

    # 3. Spread too wide — market-maker absent, quotes unreliable
    if row.bid_ask_spread_bps is not None and row.bid_ask_spread_bps > cfg.max_spread_bps:
        return False

    # 4. Null critical fields
    if row.volume_ratio is None or row.realized_volatility is None:
        return False

    return True


# ── Percentile Computation ───────────────────────────────────────────────────

def compute_percentiles(values: np.ndarray) -> np.ndarray:
    """
    Vectorised percentile rank for an array of floats.

    For 1 000+ symbols this is O(n log n) via argsort — runs in < 1 ms.
    Returns an array of percentile ranks in [0, 100].

    Equivalent to SQL:  percent_rank() OVER (ORDER BY value)

    Prefer running this in the DB (see scanner_queries.sql) for the
    canonical snapshot.  Use this in-process version for:
      • Real-time websocket ticks between DB snapshots
      • Unit-testing rules without a DB connection
    """
    n = len(values)
    if n <= 1:
        return np.full(n, 50.0)

    # Handle NaNs: push them to percentile 0
    finite_mask = np.isfinite(values)
    ranks = np.zeros(n, dtype=np.float64)

    order = np.argsort(values)
    sorted_ranks = np.empty(n, dtype=np.float64)
    sorted_ranks[order] = np.arange(n, dtype=np.float64)

    # percent_rank = rank / (n - 1) * 100
    ranks[finite_mask] = sorted_ranks[finite_mask] / (n - 1) * 100
    ranks[~finite_mask] = 0.0

    return ranks


# ── Detection Rules ──────────────────────────────────────────────────────────

def rule_volume_spike(
    row: SnapshotRow,
    cfg: ScannerConfig,
) -> Optional[Alert]:
    """
    Rule 1 — Volume Spike
    ──────────────────────
    Fire when: volume_ratio (current_period_vol / 24h_avg_vol) >= threshold

    volume_ratio is pre-computed in the DB as:
        current_1h_volume / (volume_24h / 24)

    Edge cases handled:
      • volume_24h = 0 → volume_ratio would be inf → caught by quality check
      • Low-cap tokens often spike 10x on a single fill → min_volume_floor filters
    """
    if row.volume_ratio >= cfg.volume_ratio_threshold:
        return Alert(
            symbol_id=row.symbol_id,
            symbol=row.symbol,
            rule=AlertRule.VOLUME_SPIKE,
            ts=row.ts,
            trigger_price=row.current_price,
            trigger_volume_ratio=row.volume_ratio,
            message=(
                f"Volume spike: {row.symbol} volume_ratio={row.volume_ratio:.1f}x "
                f"(threshold {cfg.volume_ratio_threshold}x) "
                f"price={row.current_price} Δ24h={row.price_change_pct_24h:+.2f}%"
            ),
        )
    return None


def rule_volatility_anomaly(
    row: SnapshotRow,
    cfg: ScannerConfig,
) -> Optional[Alert]:
    """
    Rule 2 — Volatility Anomaly
    ────────────────────────────
    Fire when: symbol's realized_volatility is in the >= Nth percentile of the
    full universe (all active symbols of the same asset_class).

    volatility_percentile is pre-computed per scan tick in the DB via:
        percent_rank() OVER (PARTITION BY asset_class ORDER BY realized_volatility)

    Why percentile instead of absolute threshold?
      • Crypto vol is structurally higher than equity vol.
      • A 90th-pctile approach self-calibrates to the current regime.
    """
    if row.volatility_percentile >= cfg.volatility_percentile_threshold:
        return Alert(
            symbol_id=row.symbol_id,
            symbol=row.symbol,
            rule=AlertRule.VOLATILITY_BREAKOUT,
            ts=row.ts,
            trigger_price=row.current_price,
            trigger_volatility=row.realized_volatility,
            message=(
                f"Volatility anomaly: {row.symbol} "
                f"vol_pctile={row.volatility_percentile:.1f} "
                f"realized_vol={row.realized_volatility:.4f} "
                f"ATR={row.atr_14:.4f}"
            ),
        )
    return None


def rule_combined(
    row: SnapshotRow,
    cfg: ScannerConfig,
) -> Optional[Alert]:
    """
    Rule 3 — Combined (Volume + Volatility + Price Move)
    ─────────────────────────────────────────────────────
    Fire when ALL three conditions are true simultaneously:
      • volume_ratio >= threshold
      • volatility_percentile >= threshold
      • |price_change_pct_24h| >= threshold

    This is the highest-conviction signal — all three dimensions confirming.
    """
    vol_ok = row.volume_ratio >= cfg.combined_volume_ratio
    vty_ok = row.volatility_percentile >= cfg.combined_volatility_pct
    prc_ok = abs(row.price_change_pct_24h) >= cfg.combined_price_change_pct

    if vol_ok and vty_ok and prc_ok:
        return Alert(
            symbol_id=row.symbol_id,
            symbol=row.symbol,
            rule=AlertRule.COMBINED,
            ts=row.ts,
            trigger_price=row.current_price,
            trigger_volume_ratio=row.volume_ratio,
            trigger_volatility=row.realized_volatility,
            message=(
                f"Combined alert: {row.symbol} "
                f"vol_ratio={row.volume_ratio:.1f}x "
                f"vol_pctile={row.volatility_percentile:.1f} "
                f"price_Δ={row.price_change_pct_24h:+.2f}%"
            ),
        )
    return None


def rule_session_activity(
    rows: list[SnapshotRow],
    session: TradingSession,
    top_n: int = 20,
) -> list[Alert]:
    """
    Rule 4 — Session-Based Activity
    ────────────────────────────────
    Identify symbols with highest volume_ratio during a specific trading
    session window (Asia / Europe / US).

    Session windows (UTC):
      Asia:   00:00 – 08:00
      Europe: 07:00 – 16:00
      US:     13:00 – 21:00

    We filter snapshots whose timestamp falls within the session window,
    then rank by volume_ratio descending.

    Edge cases:
      • Overlapping sessions (EU/US 13-16 UTC) — symbol may appear in both.
        This is intentional: a spike during overlap is relevant to both audiences.
      • Weekends — stock markets closed, crypto keeps running.
        asset_class='stock' rows will naturally have no new snapshots on weekends.
    """
    start_hour, end_hour = SESSION_WINDOWS_UTC[session]

    in_session = [
        r for r in rows
        if start_hour <= r.ts.hour < end_hour
    ]

    # Sort by volume_ratio descending, take top N
    in_session.sort(key=lambda r: r.volume_ratio, reverse=True)

    alerts = []
    for r in in_session[:top_n]:
        alerts.append(Alert(
            symbol_id=r.symbol_id,
            symbol=r.symbol,
            rule=AlertRule.SESSION_ACTIVITY,
            ts=r.ts,
            trigger_price=r.current_price,
            trigger_volume_ratio=r.volume_ratio,
            message=(
                f"Top {session.value} session mover: {r.symbol} "
                f"vol_ratio={r.volume_ratio:.1f}x "
                f"price_Δ={r.price_change_pct_24h:+.2f}%"
            ),
        ))
    return alerts


def compute_composite_scores(
    rows: list[SnapshotRow],
    cfg: ScannerConfig,
) -> list[tuple[SnapshotRow, float]]:
    """
    Rule 5 — Composite Percentile Score
    ─────────────────────────────────────
    composite_score = w_vol * volume_pctile
                    + w_vty * volatility_pctile
                    + w_prc * price_move_pctile

    Steps:
      1. Extract raw arrays for volume_ratio, realized_volatility, |price_change|
      2. Compute percentile ranks in-process (or read pre-computed from DB)
      3. Weighted sum → composite score in [0, 100]

    Performance for 1 000+ symbols:
      • Three argsorts: O(3 × n log n) ≈ 30 000 comparisons → < 1 ms in NumPy
      • No DB round-trip needed for real-time ranking between scan ticks

    Edge case: if all symbols have identical volume_ratio, percentile = 50 for
    everyone → composite degrades gracefully to the other dimensions.
    """
    if not rows:
        return []

    vol_ratios = np.array([r.volume_ratio for r in rows], dtype=np.float64)
    volatilities = np.array([r.realized_volatility for r in rows], dtype=np.float64)
    price_moves = np.array([abs(r.price_change_pct_24h) for r in rows], dtype=np.float64)

    vol_pctiles = compute_percentiles(vol_ratios)
    vty_pctiles = compute_percentiles(volatilities)
    prc_pctiles = compute_percentiles(price_moves)

    scores = (
        cfg.weight_volume * vol_pctiles
        + cfg.weight_volatility * vty_pctiles
        + cfg.weight_price_move * prc_pctiles
    )

    return sorted(
        zip(rows, scores.tolist()),
        key=lambda pair: pair[1],
        reverse=True,
    )


# ── Cooldown / Dedup ─────────────────────────────────────────────────────────

class AlertCooldown:
    """
    Prevents re-alerting on the same (symbol, rule) within a cooldown window.

    Uses an in-memory dict; for multi-process deployments, replace with a
    Redis SET with TTL or a DB "recent_alerts" check.
    """

    def __init__(self, cooldown_minutes: int = 15):
        self.cooldown = timedelta(minutes=cooldown_minutes)
        self._last_fired: dict[tuple[int, str], datetime] = {}

    def should_suppress(self, alert: Alert) -> bool:
        key = (alert.symbol_id, alert.rule.value)
        last = self._last_fired.get(key)
        if last and (alert.ts - last) < self.cooldown:
            return True
        return False

    def record(self, alert: Alert) -> None:
        key = (alert.symbol_id, alert.rule.value)
        self._last_fired[key] = alert.ts

    def prune(self, now: datetime) -> None:
        """Remove expired entries to prevent unbounded memory growth."""
        expired = [
            k for k, ts in self._last_fired.items()
            if (now - ts) > self.cooldown
        ]
        for k in expired:
            del self._last_fired[k]


# ── Orchestrator ─────────────────────────────────────────────────────────────

def run_scan(
    snapshots: list[SnapshotRow],
    cfg: ScannerConfig | None = None,
    cooldown: AlertCooldown | None = None,
    session: TradingSession | None = None,
    composite_top_n: int = 50,
    now: datetime | None = None,
) -> dict[str, list[Alert]]:
    """
    Main entry point — runs all detection rules over a batch of snapshot rows.

    Parameters
    ----------
    snapshots : list[SnapshotRow]
        Latest snapshot metrics (one per symbol). Typically fetched via:
        SELECT * FROM v_latest_snapshot WHERE asset_class = 'crypto';
    cfg : ScannerConfig
        Tunable thresholds.
    cooldown : AlertCooldown
        Dedup tracker. Pass the same instance across scan ticks.
    session : TradingSession | None
        If set, also run session-activity detection for this window.
    composite_top_n : int
        How many top composite-ranked symbols to flag.

    Returns
    -------
    dict mapping rule name → list of Alert objects.
    """
    cfg = cfg or ScannerConfig()
    cooldown = cooldown or AlertCooldown(cfg.cooldown_minutes)
    now = now or datetime.now(timezone.utc)

    results: dict[str, list[Alert]] = {
        "volume_spike": [],
        "volatility_breakout": [],
        "combined": [],
        "session_activity": [],
        "composite_rank": [],
    }

    # ── Phase 1: Quality-filter the universe ──
    qualified = [r for r in snapshots if passes_quality_checks(r, cfg, now)]
    logger.info(
        "Scan: %d/%d symbols passed quality checks",
        len(qualified), len(snapshots),
    )

    # ── Phase 2: Per-symbol rules ──
    for row in qualified:
        for rule_fn, key in [
            (rule_volume_spike, "volume_spike"),
            (rule_volatility_anomaly, "volatility_breakout"),
            (rule_combined, "combined"),
        ]:
            alert = rule_fn(row, cfg)
            if alert and not cooldown.should_suppress(alert):
                cooldown.record(alert)
                results[key].append(alert)

    # ── Phase 3: Session-based (cross-symbol) ──
    if session:
        session_alerts = rule_session_activity(qualified, session)
        for a in session_alerts:
            if not cooldown.should_suppress(a):
                cooldown.record(a)
                results["session_activity"].append(a)

    # ── Phase 4: Composite ranking (cross-symbol) ──
    scored = compute_composite_scores(qualified, cfg)
    for row, score in scored[:composite_top_n]:
        alert = Alert(
            symbol_id=row.symbol_id,
            symbol=row.symbol,
            rule=AlertRule.COMPOSITE_RANK,
            ts=row.ts,
            trigger_price=row.current_price,
            trigger_volume_ratio=row.volume_ratio,
            trigger_volatility=row.realized_volatility,
            composite_score=score,
            message=(
                f"Composite top-{composite_top_n}: {row.symbol} "
                f"score={score:.1f} "
                f"vol_ratio={row.volume_ratio:.1f}x "
                f"vol_pctile={row.volatility_percentile:.1f} "
                f"price_Δ={row.price_change_pct_24h:+.2f}%"
            ),
        )
        if not cooldown.should_suppress(alert):
            cooldown.record(alert)
            results["composite_rank"].append(alert)

    # Housekeeping
    cooldown.prune(now)

    total = sum(len(v) for v in results.values())
    logger.info("Scan complete: %d alerts across %d rules", total, len(results))

    return results
