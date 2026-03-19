-- Seed demo symbols
INSERT INTO symbols (symbol, exchange, asset_class, is_active) VALUES
  ('BTC/USDT', 'binance', 'crypto', true),
  ('ETH/USDT', 'binance', 'crypto', true),
  ('SOL/USDT', 'binance', 'crypto', true),
  ('BNB/USDT', 'binance', 'crypto', true),
  ('XRP/USDT', 'binance', 'crypto', true),
  ('ADA/USDT', 'binance', 'crypto', true),
  ('DOGE/USDT', 'binance', 'crypto', true),
  ('AVAX/USDT', 'binance', 'crypto', true),
  ('DOT/USDT', 'binance', 'crypto', true),
  ('MATIC/USDT', 'binance', 'crypto', true),
  ('LINK/USDT', 'binance', 'crypto', true),
  ('UNI/USDT', 'binance', 'crypto', true),
  ('ATOM/USDT', 'binance', 'crypto', true),
  ('LTC/USDT', 'binance', 'crypto', true),
  ('FIL/USDT', 'binance', 'crypto', true)
ON CONFLICT (symbol, exchange) DO NOTHING;

-- Seed a demo user
INSERT INTO users (email, display_name) VALUES ('demo@example.com', 'Demo User')
ON CONFLICT DO NOTHING;

-- Seed demo snapshot_metrics (recent data so the scanner feed works)
INSERT INTO snapshot_metrics (ts, symbol_id, exchange, asset_class, current_price, price_change_pct_24h, volume_24h, volume_ratio, realized_volatility, volatility_percentile, bid_price, ask_price, bid_ask_spread_bps)
SELECT
  now() - (interval '1 minute' * g),
  s.symbol_id,
  s.exchange,
  s.asset_class,
  CASE s.symbol
    WHEN 'BTC/USDT' THEN 67000 + (random() * 2000 - 1000)
    WHEN 'ETH/USDT' THEN 3500 + (random() * 200 - 100)
    WHEN 'SOL/USDT' THEN 145 + (random() * 20 - 10)
    WHEN 'BNB/USDT' THEN 580 + (random() * 30 - 15)
    WHEN 'XRP/USDT' THEN 0.62 + (random() * 0.05 - 0.025)
    WHEN 'ADA/USDT' THEN 0.45 + (random() * 0.03 - 0.015)
    WHEN 'DOGE/USDT' THEN 0.15 + (random() * 0.02 - 0.01)
    WHEN 'AVAX/USDT' THEN 35 + (random() * 5 - 2.5)
    WHEN 'DOT/USDT' THEN 7.5 + (random() * 1 - 0.5)
    WHEN 'MATIC/USDT' THEN 0.85 + (random() * 0.1 - 0.05)
    WHEN 'LINK/USDT' THEN 18 + (random() * 2 - 1)
    WHEN 'UNI/USDT' THEN 12 + (random() * 1.5 - 0.75)
    WHEN 'ATOM/USDT' THEN 9.5 + (random() * 1 - 0.5)
    WHEN 'LTC/USDT' THEN 85 + (random() * 10 - 5)
    WHEN 'FIL/USDT' THEN 6 + (random() * 1 - 0.5)
    ELSE 10 + random() * 5
  END,
  (random() * 10 - 5),                -- price_change_pct_24h
  (random() * 500000000 + 10000000),  -- volume_24h
  (random() * 8 + 0.5),              -- volume_ratio
  (random() * 0.08 + 0.01),          -- realized_volatility
  (random() * 100),                   -- volatility_percentile
  0, 0, (random() * 5)
FROM symbols s, generate_series(0, 59) g
WHERE s.is_active = true;

-- Seed a few demo alerts
INSERT INTO alerts (ts, user_id, symbol_id, rule, status, trigger_price, trigger_volume_ratio, trigger_volatility, message)
SELECT
  now() - (interval '5 minutes' * g),
  1,
  s.symbol_id,
  (ARRAY['volume_spike','volatility_anomaly','combined'])[1 + (g % 3)]::alert_rule,
  'triggered'::alert_status,
  CASE WHEN s.symbol = 'BTC/USDT' THEN 67500 WHEN s.symbol = 'ETH/USDT' THEN 3550 ELSE 100 END,
  (random() * 5 + 2),
  (random() * 0.05 + 0.02),
  'Demo alert — ' || s.symbol || ' detected anomaly'
FROM symbols s, generate_series(0, 4) g
WHERE s.symbol IN ('BTC/USDT', 'ETH/USDT', 'SOL/USDT');
