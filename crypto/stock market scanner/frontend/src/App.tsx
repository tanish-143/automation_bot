/**
 * App — Three-panel layout for the market scanner.
 *
 * Desktop:  [FilterPanel 240px] | [Center: Scanner + Heatmap flex-1] | [AlertsFeed 280px]
 * Mobile:   Full-width stacked, filters hidden, alerts collapsed into badge.
 */

import { useEffect, useState, useCallback } from 'react';
import { useStore } from './store/scanner';
import { useWebSocket } from './hooks/useWebSocket';
import { api } from './lib/api';
import type { LivePrice } from './lib/api';
import { FilterPanel } from './components/filters/FilterPanel';
import { ScannerTable } from './components/scanner/ScannerTable';
import { Heatmap } from './components/heatmap/Heatmap';
import { AlertsFeed } from './components/alerts/AlertsFeed';
import { DetailModal } from './components/detail/DetailModal';
import { AiTradeSetup } from './components/ai/AiTradeSetup';

function App() {
  const setSymbols = useStore((s) => s.setSymbols);
  const setAlerts = useStore((s) => s.setAlerts);
  const wsConnected = useStore((s) => s.wsConnected);
  const alerts = useStore((s) => s.alerts);

  const [tab, setTab] = useState<'table' | 'heatmap'>('table');
  const [alertsOpen, setAlertsOpen] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [aiOpen, setAiOpen] = useState(false);

  // Connect WebSocket
  useWebSocket();

  // Initial data fetch — try DB-backed top-movers first, fall back to live Binance prices
  useEffect(() => {
    api.topMovers({}).then(setSymbols).catch(() => {
      // DB unavailable — seed store from live Binance data
      api.livePrices().then((prices) => {
        const rows = prices.map((p, i) => ({
          symbol_id: i + 1,
          symbol: p.symbol,
          exchange: 'binance',
          asset_class: 'crypto' as const,
          current_price: p.current_price,
          price_change_pct_24h: p.price_change_pct_24h,
          volume_24h: p.volume_24h,
          volume_ratio: null,
          realized_volatility: null,
          volatility_percentile: null,
          composite_score: null,
          sparkline: p.sparkline,
        }));
        setSymbols(rows);
        setLastUpdated(new Date());
      }).catch(console.error);
    });
    api.alerts({}).then(setAlerts).catch(() => {});
  }, [setSymbols, setAlerts]);

  // Refresh: fetch live prices from Binance and update store
  const handleRefresh = useCallback(async () => {
    if (refreshing) return;
    setRefreshing(true);
    try {
      const prices: LivePrice[] = await api.livePrices();
      const rows = prices.map((p, i) => ({
        symbol_id: i + 1,
        symbol: p.symbol,
        exchange: 'binance',
        asset_class: 'crypto' as const,
        current_price: p.current_price,
        price_change_pct_24h: p.price_change_pct_24h,
        volume_24h: p.volume_24h,
        volume_ratio: null,
        realized_volatility: null,
        volatility_percentile: null,
        composite_score: null,
        sparkline: p.sparkline,
      }));
      setSymbols(rows);
      setLastUpdated(new Date());
    } catch (err) {
      console.error('Refresh failed:', err);
    } finally {
      setRefreshing(false);
    }
  }, [refreshing, setSymbols]);

  return (
    <div className="flex h-screen overflow-hidden">
      {/* Left panel — filters (hidden on mobile) */}
      <FilterPanel />

      {/* Center — scanner content */}
      <main className="flex-1 flex flex-col min-w-0 overflow-hidden">
        {/* Top bar */}
        <header className="flex items-center justify-between px-4 py-2.5 border-b border-zinc-800 bg-zinc-900/80 backdrop-blur-sm">
          <div className="flex items-center gap-3">
            <h1 className="text-sm font-bold text-zinc-100 tracking-tight">
              <span className="text-indigo-400">⚡</span> Market Scanner
            </h1>
            <div
              className={`w-2 h-2 rounded-full ${wsConnected ? 'bg-emerald-500 animate-pulse' : 'bg-red-500'}`}
              title={wsConnected ? 'WebSocket connected' : 'WebSocket disconnected'}
            />
            {lastUpdated && (
              <span className="text-[10px] text-zinc-600 hidden sm:inline">
                Updated {lastUpdated.toLocaleTimeString()}
              </span>
            )}
          </div>

          <div className="flex items-center gap-2">
            {/* AI Trade Setup button */}
            <button
              onClick={() => setAiOpen(true)}
              className="flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-md
                bg-purple-600/20 text-purple-300 border border-purple-500/30
                hover:bg-purple-600/30 hover:border-purple-500/50 hover:text-purple-200
                transition-all duration-200 active:scale-95"
              title="AI Trade Setup Analysis (Groq)"
            >
              <span className="text-sm">🤖</span>
              AI Setup
            </button>

            {/* Refresh button */}
            <button
              onClick={handleRefresh}
              disabled={refreshing}
              className="flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-md
                bg-indigo-600/20 text-indigo-300 border border-indigo-500/30
                hover:bg-indigo-600/30 hover:border-indigo-500/50 hover:text-indigo-200
                disabled:opacity-50 disabled:cursor-not-allowed
                transition-all duration-200 active:scale-95"
              title="Fetch live prices from Binance"
            >
              <svg
                className={`w-3.5 h-3.5 ${refreshing ? 'animate-spin' : ''}`}
                fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
              >
                <path strokeLinecap="round" strokeLinejoin="round"
                  d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"
                />
              </svg>
              {refreshing ? 'Refreshing…' : 'Refresh'}
            </button>

            {/* View toggle */}
            <div className="flex items-center gap-0.5 bg-zinc-800/80 rounded-lg p-0.5 border border-zinc-700/50">
              <button
                onClick={() => setTab('table')}
                className={`text-xs px-3 py-1.5 rounded-md transition-all duration-200 ${
                  tab === 'table'
                    ? 'bg-zinc-600/80 text-zinc-100 shadow-sm'
                    : 'text-zinc-500 hover:text-zinc-300'
                }`}
              >
                📊 Table
              </button>
              <button
                onClick={() => setTab('heatmap')}
                className={`text-xs px-3 py-1.5 rounded-md transition-all duration-200 ${
                  tab === 'heatmap'
                    ? 'bg-zinc-600/80 text-zinc-100 shadow-sm'
                    : 'text-zinc-500 hover:text-zinc-300'
                }`}
              >
                🔥 Heatmap
              </button>
            </div>

            {/* Mobile alerts toggle */}
            <button
              onClick={() => setAlertsOpen((v) => !v)}
              className="lg:hidden relative text-zinc-400 hover:text-zinc-200 text-sm"
            >
              🔔
              {alerts.length > 0 && (
                <span className="absolute -top-1 -right-2 text-[9px] bg-rose-600 text-white rounded-full px-1 min-w-[14px] text-center">
                  {alerts.length > 99 ? '99+' : alerts.length}
                </span>
              )}
            </button>
          </div>
        </header>

        {/* Content area */}
        <div className="flex-1 overflow-y-auto">
          {tab === 'table' ? <ScannerTable /> : <Heatmap />}
        </div>
      </main>

      {/* Right panel — alerts feed (desktop: always visible, mobile: overlay) */}
      <aside
        className={`
          w-72 border-l border-zinc-800 bg-zinc-900/50 flex-shrink-0
          max-lg:fixed max-lg:inset-y-0 max-lg:right-0 max-lg:z-40 max-lg:shadow-xl
          max-lg:transition-transform max-lg:duration-200
          ${alertsOpen ? 'max-lg:translate-x-0' : 'max-lg:translate-x-full'}
        `}
      >
        <AlertsFeed />
      </aside>

      {/* Mobile overlay backdrop */}
      {alertsOpen && (
        <div
          className="fixed inset-0 bg-black/40 z-30 lg:hidden"
          onClick={() => setAlertsOpen(false)}
        />
      )}

      {/* Detail modal */}
      <DetailModal />

      {/* AI Trade Setup modal */}
      <AiTradeSetup open={aiOpen} onClose={() => setAiOpen(false)} />
    </div>
  );
}

export default App;
