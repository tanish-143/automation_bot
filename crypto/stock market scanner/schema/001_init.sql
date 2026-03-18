-- ============================================================================
-- Crypto / Stock Market Scanner — TimescaleDB Schema
-- Requires: PostgreSQL 15+ with TimescaleDB extension
-- ============================================================================

-- 0. Extensions
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS pg_trgm;          -- for symbol search

-- 1. Enum types
CREATE TYPE asset_class  AS ENUM ('crypto', 'stock');
CREATE TYPE candle_interval AS ENUM ('1m', '5m', '15m', '1h', '4h', '1d');
CREATE TYPE alert_status AS ENUM ('triggered', 'acknowledged', 'resolved', 'expired');
CREATE TYPE alert_rule   AS ENUM (
    'volume_spike',
    'volatility_breakout',
    'spread_widening',
    'price_change_pct',
    'custom'
);

-- ============================================================================
-- 2. SYMBOLS — master reference for every tradeable instrument
-- ============================================================================
CREATE TABLE symbols (
    symbol_id       SERIAL PRIMARY KEY,
    symbol          TEXT        NOT NULL,           -- e.g. BTC/USDT, AAPL
    exchange        TEXT        NOT NULL,           -- e.g. binance, nasdaq
    asset_class     asset_class NOT NULL,
    base_currency   TEXT,                           -- BTC, AAPL
    quote_currency  TEXT,                           -- USDT, USD
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
    listed_at       TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_symbol_exchange UNIQUE (symbol, exchange)
);

CREATE INDEX idx_symbols_asset_class ON symbols (asset_class);
CREATE INDEX idx_symbols_exchange    ON symbols (exchange);
CREATE INDEX idx_symbols_trgm        ON symbols USING gin (symbol gin_trgm_ops);

-- ============================================================================
-- 3. CANDLES — OHLCV data at multiple intervals (hypertable)
-- ============================================================================
CREATE TABLE candles (
    ts              TIMESTAMPTZ     NOT NULL,       -- candle open time (UTC)
    symbol_id       INT             NOT NULL REFERENCES symbols(symbol_id),
    interval        candle_interval NOT NULL,
    open            NUMERIC(24,8)   NOT NULL,
    high            NUMERIC(24,8)   NOT NULL,
    low             NUMERIC(24,8)   NOT NULL,
    close           NUMERIC(24,8)   NOT NULL,
    volume          NUMERIC(24,8)   NOT NULL,
    quote_volume    NUMERIC(24,8),                  -- volume in quote currency
    trade_count     INT,
    is_closed       BOOLEAN         NOT NULL DEFAULT FALSE,

    CONSTRAINT pk_candles PRIMARY KEY (ts, symbol_id, interval)
);

-- Convert to TimescaleDB hypertable; partition by ts, chunk every 1 day.
SELECT create_hypertable(
    'candles',
    'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

-- Primary lookup: latest candles for a symbol at a given interval
CREATE INDEX idx_candles_symbol_interval_ts
    ON candles (symbol_id, interval, ts DESC);

-- Retention: auto-drop 1m candles older than 30 days
SELECT add_retention_policy('candles', drop_after => INTERVAL '90 days',
    if_not_exists => TRUE);

-- ============================================================================
-- 4. CONTINUOUS AGGREGATES — pre-rolled 1h and 1d bars from 1m candles
--    (optional: use if you ingest only 1m and want higher intervals computed)
-- ============================================================================
-- Example: 1-hour bars from 1m candles
CREATE MATERIALIZED VIEW candles_1h
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', ts) AS ts,
    symbol_id,
    first(open, ts)           AS open,
    max(high)                 AS high,
    min(low)                  AS low,
    last(close, ts)           AS close,
    sum(volume)               AS volume,
    sum(quote_volume)         AS quote_volume,
    sum(trade_count)          AS trade_count
FROM candles
WHERE interval = '1m'
GROUP BY time_bucket('1 hour', ts), symbol_id
WITH NO DATA;

SELECT add_continuous_aggregate_policy('candles_1h',
    start_offset  => INTERVAL '3 hours',
    end_offset    => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour',
    if_not_exists => TRUE
);

-- ============================================================================
-- 5. SNAPSHOT METRICS — latest scanner state per symbol (hypertable)
--    One row per symbol per scan tick (e.g. every 10 s or 1 min).
-- ============================================================================
CREATE TABLE snapshot_metrics (
    ts                      TIMESTAMPTZ     NOT NULL,
    symbol_id               INT             NOT NULL REFERENCES symbols(symbol_id),
    exchange                TEXT            NOT NULL,
    asset_class             asset_class     NOT NULL,

    -- Price
    current_price           NUMERIC(24,8)   NOT NULL,
    price_change_pct_24h    NUMERIC(10,4),          -- e.g. -3.25 means -3.25%
    high_24h                NUMERIC(24,8),
    low_24h                 NUMERIC(24,8),

    -- Volume
    volume_24h              NUMERIC(24,8),
    volume_ratio            NUMERIC(10,4),          -- current / 24h avg

    -- Volatility
    realized_volatility     NUMERIC(10,6),          -- annualized, e.g. 0.85
    volatility_percentile   NUMERIC(5,2),           -- 0-100 within universe
    atr_14                  NUMERIC(24,8),           -- 14-period ATR

    -- Microstructure
    bid_price               NUMERIC(24,8),
    ask_price               NUMERIC(24,8),
    bid_ask_spread_bps      NUMERIC(10,4),           -- basis points

    -- Ranking helpers (computed per scan)
    volume_rank             INT,                     -- 1 = highest volume_ratio
    volatility_rank         INT,                     -- 1 = highest realized_vol

    CONSTRAINT pk_snapshot PRIMARY KEY (ts, symbol_id)
);

SELECT create_hypertable(
    'snapshot_metrics',
    'ts',
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists       => TRUE
);

-- Top movers: rank by volume_ratio or volatility within a time window
CREATE INDEX idx_snap_volume_ratio
    ON snapshot_metrics (ts DESC, asset_class, volume_ratio DESC NULLS LAST);

CREATE INDEX idx_snap_volatility
    ON snapshot_metrics (ts DESC, asset_class, realized_volatility DESC NULLS LAST);

CREATE INDEX idx_snap_price_change
    ON snapshot_metrics (ts DESC, asset_class, price_change_pct_24h DESC NULLS LAST);

-- Fast lookup for a single symbol's history
CREATE INDEX idx_snap_symbol_ts
    ON snapshot_metrics (symbol_id, ts DESC);

-- Retention: keep 1 year of snapshot data
SELECT add_retention_policy('snapshot_metrics', drop_after => INTERVAL '365 days',
    if_not_exists => TRUE);

-- ============================================================================
-- 6. USERS
-- ============================================================================
CREATE TABLE users (
    user_id         SERIAL PRIMARY KEY,
    email           TEXT        NOT NULL UNIQUE,
    display_name    TEXT,
    password_hash   TEXT        NOT NULL,
    is_active       BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================================
-- 7. WATCHLISTS — each user can have multiple named watchlists
-- ============================================================================
CREATE TABLE watchlists (
    watchlist_id    SERIAL PRIMARY KEY,
    user_id         INT         NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    name            TEXT        NOT NULL DEFAULT 'Default',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_user_watchlist_name UNIQUE (user_id, name)
);

CREATE TABLE watchlist_symbols (
    watchlist_id    INT NOT NULL REFERENCES watchlists(watchlist_id) ON DELETE CASCADE,
    symbol_id       INT NOT NULL REFERENCES symbols(symbol_id) ON DELETE CASCADE,
    added_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (watchlist_id, symbol_id)
);

-- ============================================================================
-- 8. USER THRESHOLDS — per-user alert thresholds (overrides defaults)
-- ============================================================================
CREATE TABLE user_thresholds (
    threshold_id            SERIAL PRIMARY KEY,
    user_id                 INT             NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    symbol_id               INT             REFERENCES symbols(symbol_id),  -- NULL = global default
    rule                    alert_rule      NOT NULL,
    min_volume_ratio        NUMERIC(10,4),          -- fire if volume_ratio >= this
    min_volatility          NUMERIC(10,6),
    min_price_change_pct    NUMERIC(10,4),
    max_spread_bps          NUMERIC(10,4),
    custom_expression       TEXT,                    -- for rule = 'custom'
    is_enabled              BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_user_symbol_rule UNIQUE (user_id, symbol_id, rule)
);

-- ============================================================================
-- 9. SUBSCRIPTIONS — how users want to be notified
-- ============================================================================
CREATE TYPE notify_channel AS ENUM ('email', 'webhook', 'telegram', 'discord', 'push');

CREATE TABLE subscriptions (
    subscription_id SERIAL PRIMARY KEY,
    user_id         INT             NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
    channel         notify_channel  NOT NULL,
    endpoint        TEXT            NOT NULL,       -- email addr, webhook URL, chat ID
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_user_channel_endpoint UNIQUE (user_id, channel, endpoint)
);

-- ============================================================================
-- 10. ALERTS — fired events
-- ============================================================================
CREATE TABLE alerts (
    alert_id        BIGSERIAL,
    ts              TIMESTAMPTZ     NOT NULL DEFAULT NOW(),
    user_id         INT             NOT NULL REFERENCES users(user_id),
    symbol_id       INT             NOT NULL REFERENCES symbols(symbol_id),
    rule            alert_rule      NOT NULL,
    status          alert_status    NOT NULL DEFAULT 'triggered',

    -- Snapshot at trigger time
    trigger_price           NUMERIC(24,8),
    trigger_volume_ratio    NUMERIC(10,4),
    trigger_volatility      NUMERIC(10,6),
    trigger_spread_bps      NUMERIC(10,4),
    message                 TEXT,

    acknowledged_at TIMESTAMPTZ,
    resolved_at     TIMESTAMPTZ,
    expires_at      TIMESTAMPTZ,           -- auto-expire stale alerts

    CONSTRAINT pk_alerts PRIMARY KEY (ts, alert_id)
);

SELECT create_hypertable(
    'alerts',
    'ts',
    chunk_time_interval => INTERVAL '7 days',
    if_not_exists       => TRUE
);

CREATE INDEX idx_alerts_user_status
    ON alerts (user_id, status, ts DESC);

CREATE INDEX idx_alerts_symbol_ts
    ON alerts (symbol_id, ts DESC);

-- Auto-expire alerts after 24h by default
SELECT add_retention_policy('alerts', drop_after => INTERVAL '180 days',
    if_not_exists => TRUE);

-- ============================================================================
-- 11. HELPER VIEWS — top movers, percentile ranking
-- ============================================================================

-- Latest snapshot per symbol (de-duplicated)
CREATE VIEW v_latest_snapshot AS
SELECT DISTINCT ON (symbol_id) *
FROM snapshot_metrics
ORDER BY symbol_id, ts DESC;

-- Top volume movers right now (crypto)
CREATE VIEW v_top_volume_crypto AS
SELECT
    s.symbol,
    sm.current_price,
    sm.price_change_pct_24h,
    sm.volume_24h,
    sm.volume_ratio,
    sm.realized_volatility,
    sm.volatility_percentile,
    sm.bid_ask_spread_bps,
    percent_rank() OVER (ORDER BY sm.volume_ratio)  AS volume_pctile,
    percent_rank() OVER (ORDER BY sm.realized_volatility) AS vol_pctile
FROM v_latest_snapshot sm
JOIN symbols s ON s.symbol_id = sm.symbol_id
WHERE sm.asset_class = 'crypto'
ORDER BY sm.volume_ratio DESC NULLS LAST;

-- Top volatility movers right now (all assets)
CREATE VIEW v_top_volatility AS
SELECT
    s.symbol,
    s.exchange,
    s.asset_class,
    sm.current_price,
    sm.realized_volatility,
    sm.volatility_percentile,
    sm.atr_14,
    sm.volume_ratio,
    percent_rank() OVER (
        PARTITION BY s.asset_class
        ORDER BY sm.realized_volatility
    ) AS vol_universe_pctile
FROM v_latest_snapshot sm
JOIN symbols s ON s.symbol_id = sm.symbol_id
ORDER BY sm.realized_volatility DESC NULLS LAST;

-- ============================================================================
-- 12. EXAMPLE QUERIES
-- ============================================================================

/*
-- A) Top 20 crypto by volume spike right now:
SELECT * FROM v_top_volume_crypto
LIMIT 20;

-- B) Symbols where volume_ratio > 3x AND volatility in top 5%:
SELECT *
FROM v_latest_snapshot
WHERE volume_ratio >= 3.0
  AND volatility_percentile >= 95
ORDER BY volume_ratio DESC;

-- C) Percentile rank of every symbol's volume_ratio in the last 5 minutes:
SELECT
    symbol_id,
    volume_ratio,
    percent_rank() OVER (ORDER BY volume_ratio) AS pctile
FROM snapshot_metrics
WHERE ts >= NOW() - INTERVAL '5 minutes'
ORDER BY pctile DESC;

-- D) User's triggered alerts in the last hour:
SELECT a.*, s.symbol, s.exchange
FROM alerts a
JOIN symbols s ON s.symbol_id = a.symbol_id
WHERE a.user_id = 1
  AND a.status = 'triggered'
  AND a.ts >= NOW() - INTERVAL '1 hour'
ORDER BY a.ts DESC;
*/
