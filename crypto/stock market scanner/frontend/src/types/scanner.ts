/* ── Types for the scanner frontend ─────────────────────────────── */

export type AssetClass = 'crypto' | 'stock';
export type Session = 'all' | 'asia' | 'europe' | 'us';
export type Timeframe = '1m' | '5m' | '15m' | '1h' | '4h' | '1d';

export interface SymbolRow {
  symbol_id: number;
  symbol: string;
  exchange: string;
  asset_class: AssetClass;
  current_price: number;
  price_change_pct_24h: number | null;
  volume_24h: number | null;
  volume_ratio: number | null;
  realized_volatility: number | null;
  volatility_percentile: number | null;
  composite_score: number | null;
  /** Sparkline: last 24 close prices for mini chart */
  sparkline?: number[];
}

export interface AlertItem {
  alert_id: number;
  symbol_id: number;
  fired_at: string;
  symbol: string;
  exchange: string;
  rule_name: string;
  status: string;
  composite_score: number | null;
  volume_ratio: number | null;
  trigger_price: number | null;
  trigger_volatility: number | null;
  message: string | null;
}

export interface FilterState {
  session: Session;
  timeframe: Timeframe;
  volumeRatioMin: number;
  volatilityPctMin: number;
  minVolume: number;
  exchange: string;
  assetClass: AssetClass | 'all';
}

export interface SnapshotPoint {
  ts: string;
  current_price: number;
  volume_ratio: number | null;
  realized_volatility: number | null;
  volume_24h: number | null;
}
