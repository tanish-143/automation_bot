/**
 * AlertsFeed — right-panel live alert stream.
 *
 * Features:
 *   • Auto-scrolls to bottom on new alerts
 *   • Color-coded by rule type
 *   • CSV export only on explicit button click
 *   • Caps display at 200 items for perf
 */

import { useRef, useEffect, useMemo, useCallback } from 'react';
import { useStore } from '../../store/scanner';
import type { AlertItem } from '../../types/scanner';

const RULE_COLORS: Record<string, string> = {
  volume_spike: 'text-blue-400',
  volatility_breakout: 'text-amber-400',
  spread_widening: 'text-rose-400',
  price_change_pct: 'text-purple-400',
  custom: 'text-emerald-400',
};

const RULE_ICONS: Record<string, string> = {
  volume_spike: '📈',
  volatility_breakout: '🌊',
  spread_widening: '⚡',
  price_change_pct: '🕐',
  custom: '🏆',
};

function formatTime(iso: string): string {
  const d = new Date(iso);
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function exportCsv(alerts: AlertItem[]) {
  const header = 'time,symbol,rule,volume_ratio,message\n';
  const rows = alerts
    .map(
      (a) =>
        `${a.ts},${a.symbol},${a.rule},${a.trigger_volume_ratio ?? ''},${(a.message ?? '').replace(/,/g, ';')}`,
    )
    .join('\n');
  const blob = new Blob([header + rows], { type: 'text/csv' });
  const url = URL.createObjectURL(blob);
  const link = document.createElement('a');
  link.href = url;
  link.download = `alerts_${Date.now()}.csv`;
  link.click();
  URL.revokeObjectURL(url);
}

export function AlertsFeed() {
  const alerts = useStore((s) => s.alerts);
  const scrollRef = useRef<HTMLDivElement>(null);

  // Auto-scroll on new alerts
  useEffect(() => {
    const el = scrollRef.current;
    if (el) el.scrollTop = el.scrollHeight;
  }, [alerts.length]);

  const visible = useMemo(() => alerts.slice(-200), [alerts]);

  const handleExport = useCallback(() => exportCsv(alerts), [alerts]);

  return (
    <div className="flex flex-col h-full">
      {/* Header */}
      <div className="flex items-center justify-between p-3 border-b border-zinc-800">
        <h3 className="text-xs font-semibold text-zinc-500 uppercase tracking-wider">
          Live Alerts
          <span className="ml-1.5 text-zinc-600">({alerts.length})</span>
        </h3>
        <button
          onClick={handleExport}
          disabled={alerts.length === 0}
          className="flex items-center gap-1.5 text-[11px] px-2.5 py-1 rounded-md
            bg-indigo-600/20 text-indigo-300 border border-indigo-500/30
            hover:bg-indigo-600/30 hover:border-indigo-500/50 hover:text-indigo-200
            disabled:opacity-30 disabled:cursor-not-allowed disabled:hover:bg-indigo-600/20
            transition-all duration-200 active:scale-95 font-medium"
          title={alerts.length === 0 ? 'No alerts to export' : `Export ${alerts.length} alerts as CSV`}
        >
          <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
            <path strokeLinecap="round" strokeLinejoin="round" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
          </svg>
          Export CSV
        </button>
      </div>

      {/* Feed */}
      <div ref={scrollRef} className="flex-1 overflow-y-auto p-2 space-y-1">
        {visible.length === 0 && (
          <div className="text-zinc-600 text-xs text-center mt-8">No alerts yet</div>
        )}
        {visible.map((alert, i) => (
          <AlertRow key={`${alert.alert_id}-${i}`} alert={alert} onClick={() => {}} />
        ))}
      </div>
    </div>
  );
}

function AlertRow({ alert, onClick }: { alert: AlertItem; onClick: () => void }) {
  const colorClass = RULE_COLORS[alert.rule] ?? 'text-zinc-400';
  const icon = RULE_ICONS[alert.rule] ?? '🔔';

  return (
    <button
      onClick={onClick}
      className="w-full text-left flex items-start gap-2 p-2 rounded hover:bg-zinc-800/60 transition-colors group"
    >
      <span className="text-sm leading-none mt-0.5">{icon}</span>
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-1.5">
          <span className="text-xs font-bold text-zinc-200 group-hover:text-white">
            {alert.symbol}
          </span>
          <span className={`text-[10px] font-mono ${colorClass}`}>{alert.rule}</span>
          <span className="text-[10px] text-zinc-600 ml-auto whitespace-nowrap">
            {formatTime(alert.ts)}
          </span>
        </div>
        {alert.message && (
          <div className="text-[10px] text-zinc-500 truncate mt-0.5">{alert.message}</div>
        )}
      </div>
    </button>
  );
}
