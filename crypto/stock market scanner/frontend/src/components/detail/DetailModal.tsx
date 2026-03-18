/**
 * DetailModal — overlay showing a TradingView-style mini chart
 * powered by lightweight-charts, plus key metrics for the selected symbol.
 */

import { useEffect, useRef, useCallback, useState } from 'react';
import { createChart, ColorType, AreaSeries, type IChartApi } from 'lightweight-charts';
import { useStore } from '../../store/scanner';
import { api } from '../../lib/api';
import type { SnapshotPoint } from '../../types/scanner';

export function DetailModal() {
  const detailOpen = useStore((s) => s.detailOpen);
  const selectedSymbolId = useStore((s) => s.selectedSymbolId);
  const closeDetail = useStore((s) => s.closeDetail);
  const symbols = useStore((s) => s.symbols);

  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);

  const [snapshots, setSnapshots] = useState<SnapshotPoint[]>([]);
  const [loading, setLoading] = useState(false);

  const row = symbols.find((s) => s.symbol_id === selectedSymbolId) ?? null;

  // Fetch snapshot data
  useEffect(() => {
    if (!selectedSymbolId || !detailOpen) return;
    let cancelled = false;
    setLoading(true);
    api
      .snapshot(selectedSymbolId)
      .then((data) => {
        if (!cancelled) setSnapshots(data);
      })
      .catch(() => {
        if (!cancelled) setSnapshots([]);
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [selectedSymbolId, detailOpen]);

  // Create / update chart
  useEffect(() => {
    if (!detailOpen || !chartContainerRef.current || snapshots.length === 0) return;

    // Dispose previous
    if (chartRef.current) {
      chartRef.current.remove();
      chartRef.current = null;
    }

    const chart = createChart(chartContainerRef.current, {
      width: chartContainerRef.current.clientWidth,
      height: 280,
      layout: {
        background: { type: ColorType.Solid, color: '#18181b' },
        textColor: '#a1a1aa',
      },
      grid: {
        vertLines: { color: '#27272a' },
        horzLines: { color: '#27272a' },
      },
      crosshair: { mode: 0 },
      rightPriceScale: { borderColor: '#3f3f46' },
      timeScale: { borderColor: '#3f3f46' },
    });

    const series = chart.addSeries(AreaSeries, {
      lineColor: '#3b82f6',
      topColor: 'rgba(59,130,246,0.3)',
      bottomColor: 'rgba(59,130,246,0.02)',
      lineWidth: 2,
    });

    const chartData = snapshots.map((s) => ({
      time: (new Date(s.ts).getTime() / 1000) as any,
      value: s.current_price,
    }));

    series.setData(chartData);
    chart.timeScale().fitContent();
    chartRef.current = chart;

    const handleResize = () => {
      if (chartContainerRef.current) {
        chart.applyOptions({ width: chartContainerRef.current.clientWidth });
      }
    };
    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      chart.remove();
      chartRef.current = null;
    };
  }, [detailOpen, snapshots]);

  // Close on Escape
  const handleKeyDown = useCallback(
    (e: KeyboardEvent) => {
      if (e.key === 'Escape') closeDetail();
    },
    [closeDetail],
  );

  useEffect(() => {
    if (detailOpen) {
      document.addEventListener('keydown', handleKeyDown);
      return () => document.removeEventListener('keydown', handleKeyDown);
    }
  }, [detailOpen, handleKeyDown]);

  if (!detailOpen || !row) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 backdrop-blur-sm">
      {/* Overlay click to close */}
      <div className="absolute inset-0" onClick={closeDetail} />

      <div className="relative z-10 bg-zinc-900 border border-zinc-700 rounded-lg shadow-xl w-full max-w-2xl mx-4">
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b border-zinc-800">
          <div>
            <span className="text-lg font-bold text-zinc-100">{row.symbol}</span>
            <span className="ml-2 text-sm text-zinc-500">{row.exchange}</span>
          </div>
          <button
            onClick={closeDetail}
            className="text-zinc-500 hover:text-zinc-200 text-xl leading-none px-2"
          >
            ✕
          </button>
        </div>

        {/* Chart */}
        <div className="p-4">
          {loading ? (
            <div className="h-[280px] flex items-center justify-center text-zinc-600 text-sm">
              Loading chart…
            </div>
          ) : snapshots.length === 0 ? (
            <div className="h-[280px] flex items-center justify-center text-zinc-600 text-sm">
              No snapshot data available
            </div>
          ) : (
            <div ref={chartContainerRef} className="w-full" />
          )}
        </div>

        {/* Metrics */}
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 p-4 border-t border-zinc-800">
          <Metric label="Price" value={fmt(row.current_price)} />
          <Metric
            label="24h Change"
            value={row.price_change_pct_24h != null ? `${row.price_change_pct_24h.toFixed(2)}%` : '—'}
            color={row.price_change_pct_24h != null && row.price_change_pct_24h >= 0 ? 'text-green-400' : 'text-red-400'}
          />
          <Metric label="Vol Ratio" value={row.volume_ratio != null ? `${row.volume_ratio.toFixed(2)}x` : '—'} />
          <Metric
            label="Score"
            value={row.composite_score != null ? row.composite_score.toFixed(0) : '—'}
            color={
              row.composite_score != null && row.composite_score >= 80
                ? 'text-emerald-400'
                : row.composite_score != null && row.composite_score >= 60
                  ? 'text-amber-400'
                  : 'text-zinc-300'
            }
          />
        </div>
      </div>
    </div>
  );
}

function Metric({ label, value, color }: { label: string; value: string; color?: string }) {
  return (
    <div>
      <div className="text-[10px] text-zinc-500 uppercase">{label}</div>
      <div className={`text-sm font-mono font-semibold ${color ?? 'text-zinc-200'}`}>{value}</div>
    </div>
  );
}

function fmt(n: number): string {
  if (n >= 1) return n.toLocaleString(undefined, { maximumFractionDigits: 2 });
  return n.toPrecision(4);
}
