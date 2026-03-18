/**
 * App — Three-panel layout for the market scanner.
 *
 * Desktop:  [FilterPanel 240px] | [Center: Scanner + Heatmap flex-1] | [AlertsFeed 280px]
 * Mobile:   Full-width stacked, filters hidden, alerts collapsed into badge.
 */

import { useEffect, useState } from 'react';
import { useStore } from './store/scanner';
import { useWebSocket } from './hooks/useWebSocket';
import { api } from './lib/api';
import { FilterPanel } from './components/filters/FilterPanel';
import { ScannerTable } from './components/scanner/ScannerTable';
import { Heatmap } from './components/heatmap/Heatmap';
import { AlertsFeed } from './components/alerts/AlertsFeed';
import { DetailModal } from './components/detail/DetailModal';

function App() {
  const setSymbols = useStore((s) => s.setSymbols);
  const setAlerts = useStore((s) => s.setAlerts);
  const wsConnected = useStore((s) => s.wsConnected);
  const alerts = useStore((s) => s.alerts);

  const [tab, setTab] = useState<'table' | 'heatmap'>('table');
  const [alertsOpen, setAlertsOpen] = useState(false);

  // Connect WebSocket
  useWebSocket();

  // Initial data fetch
  useEffect(() => {
    api.topMovers({}).then(setSymbols).catch(console.error);
    api.alerts({}).then(setAlerts).catch(console.error);
  }, [setSymbols, setAlerts]);

  return (
    <div className="flex h-screen overflow-hidden">
      {/* Left panel — filters (hidden on mobile) */}
      <FilterPanel />

      {/* Center — scanner content */}
      <main className="flex-1 flex flex-col min-w-0 overflow-hidden">
        {/* Top bar */}
        <header className="flex items-center justify-between px-4 py-2 border-b border-zinc-800 bg-zinc-900/80 backdrop-blur-sm">
          <div className="flex items-center gap-3">
            <h1 className="text-sm font-bold text-zinc-100 tracking-tight">Market Scanner</h1>
            <div
              className={`w-2 h-2 rounded-full ${wsConnected ? 'bg-emerald-500 animate-pulse' : 'bg-red-500'}`}
              title={wsConnected ? 'WebSocket connected' : 'WebSocket disconnected'}
            />
          </div>

          {/* View toggle */}
          <div className="flex items-center gap-1 bg-zinc-800 rounded p-0.5">
            <button
              onClick={() => setTab('table')}
              className={`text-xs px-3 py-1 rounded transition-colors ${
                tab === 'table' ? 'bg-zinc-700 text-zinc-100' : 'text-zinc-500 hover:text-zinc-300'
              }`}
            >
              Table
            </button>
            <button
              onClick={() => setTab('heatmap')}
              className={`text-xs px-3 py-1 rounded transition-colors ${
                tab === 'heatmap' ? 'bg-zinc-700 text-zinc-100' : 'text-zinc-500 hover:text-zinc-300'
              }`}
            >
              Heatmap
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
    </div>
  );
}

export default App;
