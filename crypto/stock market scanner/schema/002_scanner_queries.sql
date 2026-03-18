-- ============================================================================
-- Scanner Detection Queries — DB-Layer Computations
-- Run these in TimescaleDB to feed the Python detection_rules engine.
-- ============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- Q1. LATEST SNAPSHOT WITH UNIVERSE PERCENTILES
-- This is the primary query the scanner calls every tick.
-- Computes percentile ranks across the full universe, partitioned by asset_class.
-- Result feeds directly into detection_rules.run_scan().
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_scanner_feed AS
WITH latest AS (
    -- Get the most recent snapshot per symbol (dedup)
    SELECT DISTINCT ON (symbol_id) *
    FROM snapshot_metrics
    ORDER BY symbol_id, ts DESC
),
ranked AS (
    SELECT
        l.*,
        s.symbol,
        -- Percentile ranks within each asset class
        (percent_rank() OVER (
            PARTITION BY l.asset_class
            ORDER BY l.volume_ratio
        ) * 100)::NUMERIC(5,2) AS volume_pctile,

        (percent_rank() OVER (
            PARTITION BY l.asset_class
            ORDER BY l.realized_volatility
        ) * 100)::NUMERIC(5,2) AS volatility_pctile,

        (percent_rank() OVER (
            PARTITION BY l.asset_class
            ORDER BY ABS(l.price_change_pct_24h)
        ) * 100)::NUMERIC(5,2) AS price_move_pctile,

        -- Composite score: 0.5*vol + 0.3*volatility + 0.2*price_move
        (
            0.5 * (percent_rank() OVER (
                PARTITION BY l.asset_class ORDER BY l.volume_ratio
            ) * 100)
          + 0.3 * (percent_rank() OVER (
                PARTITION BY l.asset_class ORDER BY l.realized_volatility
            ) * 100)
          + 0.2 * (percent_rank() OVER (
                PARTITION BY l.asset_class ORDER BY ABS(l.price_change_pct_24h)
            ) * 100)
        )::NUMERIC(5,2) AS composite_score

    FROM latest l
    JOIN symbols s ON s.symbol_id = l.symbol_id
    WHERE s.is_active = TRUE
)
SELECT * FROM ranked;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q2. VOLUME RATIO COMPUTATION
-- Computes volume_ratio = (current 1h volume) / (24h average hourly volume)
-- Run this periodically and UPDATE snapshot_metrics, or use in a scan function.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION fn_compute_volume_ratios()
RETURNS TABLE (
    symbol_id   INT,
    vol_1h      NUMERIC,
    vol_24h_avg NUMERIC,
    volume_ratio NUMERIC
) LANGUAGE sql STABLE AS $$
    WITH vol_1h AS (
        SELECT
            c.symbol_id,
            SUM(c.volume) AS vol_1h
        FROM candles c
        WHERE c.interval = '1m'
          AND c.ts >= NOW() - INTERVAL '1 hour'
        GROUP BY c.symbol_id
    ),
    vol_24h AS (
        SELECT
            c.symbol_id,
            SUM(c.volume) / 24.0 AS avg_hourly_vol  -- 24h total / 24 hours
        FROM candles c
        WHERE c.interval = '1h'
          AND c.ts >= NOW() - INTERVAL '24 hours'
        GROUP BY c.symbol_id
        HAVING SUM(c.volume) > 0   -- guard against division by zero
    )
    SELECT
        h.symbol_id,
        h.vol_1h,
        d.avg_hourly_vol,
        CASE
            WHEN d.avg_hourly_vol > 0
            THEN (h.vol_1h / d.avg_hourly_vol)::NUMERIC(10,4)
            ELSE NULL  -- insufficient history
        END AS volume_ratio
    FROM vol_1h h
    JOIN vol_24h d ON d.symbol_id = h.symbol_id;
$$;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q3. REALIZED VOLATILITY (annualized, from 1h close-to-close returns)
-- Uses the last 24 one-hour candles → stddev of log returns → annualize.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION fn_compute_realized_volatility()
RETURNS TABLE (
    symbol_id           INT,
    realized_volatility NUMERIC,
    data_points         INT
) LANGUAGE sql STABLE AS $$
    WITH hourly_closes AS (
        SELECT
            symbol_id,
            ts,
            close,
            LAG(close) OVER (PARTITION BY symbol_id ORDER BY ts) AS prev_close
        FROM candles
        WHERE interval = '1h'
          AND ts >= NOW() - INTERVAL '24 hours'
    ),
    log_returns AS (
        SELECT
            symbol_id,
            LN(close / prev_close) AS log_ret
        FROM hourly_closes
        WHERE prev_close > 0 AND close > 0
    )
    SELECT
        symbol_id,
        -- Annualize: hourly stddev × sqrt(24 hours × 365 days)
        (STDDEV(log_ret) * SQRT(24.0 * 365.0))::NUMERIC(10,6) AS realized_volatility,
        COUNT(*)::INT AS data_points
    FROM log_returns
    GROUP BY symbol_id
    HAVING COUNT(*) >= 10;  -- minimum data points guard
$$;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q4. TOP MOVERS BY VOLUME SPIKE (Rule 1 in pure SQL)
-- Direct DB query for dashboards / quick checks without the Python layer.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_volume_spikes AS
SELECT
    s.symbol,
    s.exchange,
    s.asset_class,
    sm.current_price,
    sm.volume_ratio,
    sm.volume_24h,
    sm.price_change_pct_24h,
    sm.bid_ask_spread_bps
FROM v_scanner_feed sm
JOIN symbols s ON s.symbol_id = sm.symbol_id
WHERE sm.volume_ratio >= 3.0
  AND sm.volume_24h >= 10000         -- min volume floor
  AND (sm.bid_ask_spread_bps IS NULL OR sm.bid_ask_spread_bps <= 500)
ORDER BY sm.volume_ratio DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q5. TOP VOLATILITY ANOMALIES (Rule 2 in pure SQL)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_volatility_anomalies AS
SELECT
    s.symbol,
    s.exchange,
    s.asset_class,
    sm.current_price,
    sm.realized_volatility,
    sm.volatility_pctile,
    sm.atr_14,
    sm.volume_ratio
FROM v_scanner_feed sm
JOIN symbols s ON s.symbol_id = sm.symbol_id
WHERE sm.volatility_pctile >= 90
  AND sm.volume_24h >= 10000
ORDER BY sm.volatility_pctile DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q6. COMBINED ALERT (Rule 3 in pure SQL)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_combined_alerts AS
SELECT
    s.symbol,
    s.exchange,
    sm.current_price,
    sm.volume_ratio,
    sm.volatility_pctile,
    sm.price_change_pct_24h,
    sm.composite_score
FROM v_scanner_feed sm
JOIN symbols s ON s.symbol_id = sm.symbol_id
WHERE sm.volume_ratio >= 3.0
  AND sm.volatility_pctile >= 90
  AND ABS(sm.price_change_pct_24h) >= 2.0
  AND sm.volume_24h >= 10000
ORDER BY sm.composite_score DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q7. SESSION-BASED TOP MOVERS (Rule 4)
-- Parameterized: pass session start/end hours.
-- Example: Asia session = hours 0–8 UTC
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION fn_session_top_movers(
    p_session_start_hour INT,
    p_session_end_hour   INT,
    p_asset_class        asset_class DEFAULT NULL,
    p_limit              INT DEFAULT 20
)
RETURNS TABLE (
    symbol        TEXT,
    exchange      TEXT,
    current_price NUMERIC,
    volume_ratio  NUMERIC,
    price_change  NUMERIC,
    volatility    NUMERIC
) LANGUAGE sql STABLE AS $$
    SELECT
        s.symbol,
        s.exchange,
        sm.current_price,
        sm.volume_ratio,
        sm.price_change_pct_24h,
        sm.realized_volatility
    FROM snapshot_metrics sm
    JOIN symbols s ON s.symbol_id = sm.symbol_id
    WHERE sm.ts >= NOW() - INTERVAL '24 hours'
      AND EXTRACT(HOUR FROM sm.ts) >= p_session_start_hour
      AND EXTRACT(HOUR FROM sm.ts) < p_session_end_hour
      AND sm.volume_24h >= 10000
      AND (p_asset_class IS NULL OR sm.asset_class = p_asset_class)
    ORDER BY sm.volume_ratio DESC
    LIMIT p_limit;
$$;

-- Usage:
-- SELECT * FROM fn_session_top_movers(0, 8);              -- Asia, all assets
-- SELECT * FROM fn_session_top_movers(13, 21, 'crypto');  -- US session, crypto only


-- ─────────────────────────────────────────────────────────────────────────────
-- Q8. COMPOSITE RANKING (Rule 5 in pure SQL)
-- Top N symbols by weighted composite score.
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_composite_rankings AS
SELECT
    s.symbol,
    s.exchange,
    s.asset_class,
    sm.current_price,
    sm.volume_ratio,
    sm.realized_volatility,
    sm.price_change_pct_24h,
    sm.volume_pctile,
    sm.volatility_pctile,
    sm.price_move_pctile,
    sm.composite_score
FROM v_scanner_feed sm
JOIN symbols s ON s.symbol_id = sm.symbol_id
WHERE sm.volume_24h >= 10000
ORDER BY sm.composite_score DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q9. DATA QUALITY: detect gaps and stale feeds
-- Symbols with no snapshot in the last 5 minutes (possible feed issue).
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW v_stale_feeds AS
SELECT
    s.symbol_id,
    s.symbol,
    s.exchange,
    MAX(sm.ts) AS last_seen,
    NOW() - MAX(sm.ts) AS staleness
FROM symbols s
LEFT JOIN snapshot_metrics sm ON sm.symbol_id = s.symbol_id
    AND sm.ts >= NOW() - INTERVAL '1 hour'
WHERE s.is_active = TRUE
GROUP BY s.symbol_id, s.symbol, s.exchange
HAVING MAX(sm.ts) IS NULL
    OR MAX(sm.ts) < NOW() - INTERVAL '5 minutes'
ORDER BY staleness DESC NULLS FIRST;


-- ─────────────────────────────────────────────────────────────────────────────
-- Q10. INSERT ALERT (called from application after rule fires)
-- ─────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION fn_insert_alert(
    p_user_id            INT,
    p_symbol_id          INT,
    p_rule               alert_rule,
    p_trigger_price      NUMERIC,
    p_trigger_vol_ratio  NUMERIC DEFAULT NULL,
    p_trigger_volatility NUMERIC DEFAULT NULL,
    p_trigger_spread     NUMERIC DEFAULT NULL,
    p_message            TEXT DEFAULT NULL,
    p_expires_at         TIMESTAMPTZ DEFAULT NULL
)
RETURNS BIGINT LANGUAGE sql AS $$
    INSERT INTO alerts (
        ts, user_id, symbol_id, rule, status,
        trigger_price, trigger_volume_ratio, trigger_volatility,
        trigger_spread_bps, message, expires_at
    ) VALUES (
        NOW(), p_user_id, p_symbol_id, p_rule, 'triggered',
        p_trigger_price, p_trigger_vol_ratio, p_trigger_volatility,
        p_trigger_spread, p_message,
        COALESCE(p_expires_at, NOW() + INTERVAL '24 hours')
    )
    RETURNING alert_id;
$$;
