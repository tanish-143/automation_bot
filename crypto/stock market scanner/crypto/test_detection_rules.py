"""
Tests for detection_rules.py
Run with: pytest test_detection_rules.py -v
"""

from datetime import datetime, timezone

import numpy as np
import pytest

from detection_rules import (
    Alert,
    AlertCooldown,
    AlertRule,
    ScannerConfig,
    SnapshotRow,
    TradingSession,
    compute_composite_scores,
    compute_percentiles,
    passes_quality_checks,
    rule_combined,
    rule_session_activity,
    rule_volatility_anomaly,
    rule_volume_spike,
    run_scan,
)

NOW = datetime(2026, 3, 18, 14, 30, 0, tzinfo=timezone.utc)


def _make_row(**overrides) -> SnapshotRow:
    """Factory for SnapshotRow with sane defaults."""
    defaults = dict(
        symbol_id=1,
        symbol="BTC/USDT",
        exchange="binance",
        asset_class="crypto",
        ts=NOW,
        current_price=65000.0,
        price_change_pct_24h=1.5,
        volume_24h=500_000_000.0,
        volume_ratio=1.2,
        realized_volatility=0.65,
        volatility_percentile=50.0,
        atr_14=1200.0,
        bid_ask_spread_bps=2.0,
        trade_count_1h=5000,
    )
    defaults.update(overrides)
    return SnapshotRow(**defaults)


class TestQualityChecks:
    def test_passes_normal(self):
        row = _make_row()
        assert passes_quality_checks(row, ScannerConfig(), NOW)

    def test_rejects_low_volume(self):
        row = _make_row(volume_24h=500.0)
        assert not passes_quality_checks(row, ScannerConfig(), NOW)

    def test_rejects_null_volume(self):
        row = _make_row(volume_24h=None)
        assert not passes_quality_checks(row, ScannerConfig(), NOW)

    def test_rejects_stale_data(self):
        from datetime import timedelta
        stale_ts = NOW - timedelta(seconds=300)
        row = _make_row(ts=stale_ts)
        assert not passes_quality_checks(row, ScannerConfig(), NOW)

    def test_rejects_wide_spread(self):
        row = _make_row(bid_ask_spread_bps=600.0)
        assert not passes_quality_checks(row, ScannerConfig(), NOW)

    def test_rejects_null_volume_ratio(self):
        row = _make_row(volume_ratio=None)
        assert not passes_quality_checks(row, ScannerConfig(), NOW)


class TestPercentiles:
    def test_basic_ranking(self):
        values = np.array([10.0, 30.0, 20.0, 40.0])
        pctiles = compute_percentiles(values)
        # 10=0th, 20=33rd, 30=67th, 40=100th
        assert pctiles[0] == pytest.approx(0.0, abs=0.1)
        assert pctiles[3] == pytest.approx(100.0, abs=0.1)

    def test_single_element(self):
        pctiles = compute_percentiles(np.array([42.0]))
        assert pctiles[0] == 50.0

    def test_nan_handling(self):
        values = np.array([10.0, np.nan, 30.0, 20.0])
        pctiles = compute_percentiles(values)
        assert pctiles[1] == 0.0  # NaN gets 0th percentile

    def test_all_equal(self):
        values = np.array([5.0, 5.0, 5.0])
        pctiles = compute_percentiles(values)
        # argsort will assign one to 0, one to 50, one to 100
        assert np.mean(pctiles) == pytest.approx(50.0, abs=1.0)

    def test_1000_symbols(self):
        rng = np.random.default_rng(42)
        values = rng.lognormal(0, 1, size=1000)
        pctiles = compute_percentiles(values)
        assert pctiles.min() == pytest.approx(0.0, abs=0.2)
        assert pctiles.max() == pytest.approx(100.0, abs=0.2)
        assert np.median(pctiles) == pytest.approx(50.0, abs=1.0)


class TestRuleVolumeSpike:
    def test_fires_above_threshold(self):
        row = _make_row(volume_ratio=5.0)
        alert = rule_volume_spike(row, ScannerConfig())
        assert alert is not None
        assert alert.rule == AlertRule.VOLUME_SPIKE
        assert "5.0x" in alert.message

    def test_no_fire_below_threshold(self):
        row = _make_row(volume_ratio=2.5)
        assert rule_volume_spike(row, ScannerConfig()) is None

    def test_fires_at_exact_threshold(self):
        row = _make_row(volume_ratio=3.0)
        assert rule_volume_spike(row, ScannerConfig()) is not None


class TestRuleVolatilityAnomaly:
    def test_fires_above_90th(self):
        row = _make_row(volatility_percentile=95.0)
        alert = rule_volatility_anomaly(row, ScannerConfig())
        assert alert is not None
        assert alert.rule == AlertRule.VOLATILITY_BREAKOUT

    def test_no_fire_below_90th(self):
        row = _make_row(volatility_percentile=85.0)
        assert rule_volatility_anomaly(row, ScannerConfig()) is None


class TestRuleCombined:
    def test_fires_all_conditions_met(self):
        row = _make_row(
            volume_ratio=4.0,
            volatility_percentile=95.0,
            price_change_pct_24h=3.5,
        )
        alert = rule_combined(row, ScannerConfig())
        assert alert is not None
        assert alert.rule == AlertRule.COMBINED

    def test_no_fire_missing_one_condition(self):
        # Volume and volatility met, but price change too small
        row = _make_row(
            volume_ratio=4.0,
            volatility_percentile=95.0,
            price_change_pct_24h=0.5,
        )
        assert rule_combined(row, ScannerConfig()) is None

    def test_uses_absolute_price_change(self):
        # Large negative move should also trigger
        row = _make_row(
            volume_ratio=4.0,
            volatility_percentile=95.0,
            price_change_pct_24h=-5.0,
        )
        assert rule_combined(row, ScannerConfig()) is not None


class TestSessionActivity:
    def test_detects_asia_session(self):
        # Hour 3 UTC = Asia session
        rows = [
            _make_row(
                symbol_id=i,
                symbol=f"SYM{i}",
                ts=NOW.replace(hour=3),
                volume_ratio=float(i),
            )
            for i in range(1, 6)
        ]
        alerts = rule_session_activity(rows, TradingSession.ASIA, top_n=3)
        assert len(alerts) == 3
        assert alerts[0].trigger_volume_ratio == 5.0  # highest first

    def test_ignores_out_of_session(self):
        # Hour 22 UTC = not in any standard session
        rows = [_make_row(ts=NOW.replace(hour=22))]
        alerts = rule_session_activity(rows, TradingSession.ASIA)
        assert len(alerts) == 0


class TestCompositeScores:
    def test_ranking_order(self):
        rows = [
            _make_row(symbol_id=1, symbol="LOW", volume_ratio=1.0,
                      realized_volatility=0.1, price_change_pct_24h=0.1),
            _make_row(symbol_id=2, symbol="HIGH", volume_ratio=10.0,
                      realized_volatility=2.0, price_change_pct_24h=15.0),
        ]
        scored = compute_composite_scores(rows, ScannerConfig())
        # HIGH should rank above LOW
        assert scored[0][0].symbol == "HIGH"
        assert scored[0][1] > scored[1][1]

    def test_empty_input(self):
        assert compute_composite_scores([], ScannerConfig()) == []

    def test_weights_sum_validation(self):
        with pytest.raises(ValueError, match="sum to 1.0"):
            ScannerConfig(weight_volume=0.5, weight_volatility=0.5, weight_price_move=0.5)


class TestAlertCooldown:
    def test_suppresses_within_window(self):
        cd = AlertCooldown(cooldown_minutes=15)
        alert = Alert(
            symbol_id=1, symbol="BTC", rule=AlertRule.VOLUME_SPIKE,
            ts=NOW, message="test", trigger_price=65000,
        )
        cd.record(alert)
        assert cd.should_suppress(alert)

    def test_allows_after_window(self):
        from datetime import timedelta
        cd = AlertCooldown(cooldown_minutes=15)
        alert1 = Alert(
            symbol_id=1, symbol="BTC", rule=AlertRule.VOLUME_SPIKE,
            ts=NOW, message="test", trigger_price=65000,
        )
        cd.record(alert1)

        alert2 = Alert(
            symbol_id=1, symbol="BTC", rule=AlertRule.VOLUME_SPIKE,
            ts=NOW + timedelta(minutes=20), message="test", trigger_price=66000,
        )
        assert not cd.should_suppress(alert2)

    def test_different_rules_not_suppressed(self):
        cd = AlertCooldown(cooldown_minutes=15)
        a1 = Alert(
            symbol_id=1, symbol="BTC", rule=AlertRule.VOLUME_SPIKE,
            ts=NOW, message="test", trigger_price=65000,
        )
        cd.record(a1)

        a2 = Alert(
            symbol_id=1, symbol="BTC", rule=AlertRule.VOLATILITY_BREAKOUT,
            ts=NOW, message="test", trigger_price=65000,
        )
        assert not cd.should_suppress(a2)

    def test_prune_removes_expired(self):
        from datetime import timedelta
        cd = AlertCooldown(cooldown_minutes=15)
        alert = Alert(
            symbol_id=1, symbol="BTC", rule=AlertRule.VOLUME_SPIKE,
            ts=NOW - timedelta(minutes=30), message="test", trigger_price=65000,
        )
        cd.record(alert)
        cd.prune(NOW)
        assert len(cd._last_fired) == 0


class TestRunScan:
    def test_end_to_end(self):
        rows = [
            _make_row(symbol_id=1, symbol="BTC/USDT", volume_ratio=5.0,
                      volatility_percentile=95.0, price_change_pct_24h=4.0),
            _make_row(symbol_id=2, symbol="ETH/USDT", volume_ratio=1.0,
                      volatility_percentile=40.0, price_change_pct_24h=0.5),
            _make_row(symbol_id=3, symbol="DOGE/USDT", volume_ratio=8.0,
                      volatility_percentile=99.0, price_change_pct_24h=-6.0),
        ]
        results = run_scan(rows, session=TradingSession.US, now=NOW)

        # BTC and DOGE should trigger volume spike
        spike_symbols = {a.symbol for a in results["volume_spike"]}
        assert "BTC/USDT" in spike_symbols
        assert "DOGE/USDT" in spike_symbols
        assert "ETH/USDT" not in spike_symbols

        # BTC and DOGE should trigger combined
        combined_symbols = {a.symbol for a in results["combined"]}
        assert "BTC/USDT" in combined_symbols
        assert "DOGE/USDT" in combined_symbols

        # Composite rank should include all 3
        assert len(results["composite_rank"]) == 3
