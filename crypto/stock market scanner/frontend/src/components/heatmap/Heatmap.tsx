/**
 * Heatmap — grid of symbol tiles with shadow/glassmorphism popup on hover.
 *
 * Color scale:
 *   0–30  → dark cool (blue-gray)
 *   30–60 → neutral (slate)
 *   60–80 → warm (amber)
 *   80+   → hot (emerald/green glow)
 *
 * Tile size proportional to log(volume_24h) so high-volume symbols
 * are visually larger (treemap-like layout via CSS grid auto-fill).
 *
 * Hover: shows a floating popup card with detailed metrics, shadow & glass effect.
 */

import { memo, useMemo, useState, useRef, useCallback } from 'react';
import { useStore } from '../../store/scanner';
import type { SymbolRow } from '../../types/scanner';

function scoreToColor(score: number | null): string {
  if (score == null) return 'rgba(63,63,70,0.5)';
  const s = Math.max(0, Math.min(100, score));

  if (s >= 80) {
    const intensity = (s - 80) / 20;
    return `rgba(34,197,94,${0.3 + intensity * 0.5})`;
  }
  if (s >= 60) {
    const intensity = (s - 60) / 20;
    return `rgba(245,158,11,${0.2 + intensity * 0.4})`;
  }
  if (s >= 30) {
    const intensity = (s - 30) / 30;
    return `rgba(148,163,184,${0.1 + intensity * 0.2})`;
  }
  return `rgba(100,116,139,${0.08 + (s / 30) * 0.12})`;
}

function scoreToGlow(score: number | null): string {
  if (score == null) return 'none';
  if (score >= 80) return '0 0 20px rgba(34,197,94,0.3), 0 0 40px rgba(34,197,94,0.1)';
  if (score >= 60) return '0 0 15px rgba(245,158,11,0.2)';
  return 'none';
}

function scoreToTextColor(score: number | null): string {
  if (score == null) return '#71717a';
  if (score >= 80) return '#4ade80';
  if (score >= 60) return '#fbbf24';
  return '#a1a1aa';
}

function scoreToBorderColor(score: number | null): string {
  if (score == null) return 'rgba(63,63,70,0.3)';
  if (score >= 80) return 'rgba(34,197,94,0.3)';
  if (score >= 60) return 'rgba(245,158,11,0.2)';
  return 'rgba(63,63,70,0.2)';
}

function fmt(n: number | null | undefined, decimals = 2): string {
  if (n == null) return '—';
  if (Math.abs(n) >= 1_000_000_000) return `$${(n / 1_000_000_000).toFixed(1)}B`;
  if (Math.abs(n) >= 1_000_000) return `$${(n / 1_000_000).toFixed(1)}M`;
  if (Math.abs(n) >= 1_000) return `$${(n / 1_000).toFixed(1)}K`;
  return `$${n.toFixed(decimals)}`;
}

export function Heatmap() {
  const symbols = useStore((s) => s.symbols);
  const openDetail = useStore((s) => s.openDetail);

  const tiles = useMemo(
    () => [...symbols].sort((a, b) => (b.composite_score ?? 0) - (a.composite_score ?? 0)).slice(0, 100),
    [symbols],
  );

  const [hoveredSymbol, setHoveredSymbol] = useState<SymbolRow | null>(null);
  const [popupPos, setPopupPos] = useState<{ x: number; y: number }>({ x: 0, y: 0 });
  const containerRef = useRef<HTMLDivElement>(null);

  const handleTileHover = useCallback((row: SymbolRow, e: React.MouseEvent) => {
    const rect = containerRef.current?.getBoundingClientRect();
    if (!rect) return;
    const x = e.clientX - rect.left;
    const y = e.clientY - rect.top;
    setHoveredSymbol(row);
    setPopupPos({ x, y });
  }, []);

  const handleTileLeave = useCallback(() => {
    setHoveredSymbol(null);
  }, []);

  return (
    <div ref={containerRef} className="p-4 relative">
      {/* Header */}
      <div className="flex items-center justify-between mb-3">
        <h3 className="text-xs font-semibold text-zinc-500 uppercase tracking-wider">
          🔥 Heatmap — Top {tiles.length} by Composite Score
        </h3>
        <div className="flex items-center gap-3 text-[10px] text-zinc-600">
          <span className="flex items-center gap-1">
            <span className="w-3 h-3 rounded-sm" style={{ backgroundColor: 'rgba(100,116,139,0.15)' }} />
            Cold
          </span>
          <span className="flex items-center gap-1">
            <span className="w-3 h-3 rounded-sm" style={{ backgroundColor: 'rgba(148,163,184,0.25)' }} />
            Neutral
          </span>
          <span className="flex items-center gap-1">
            <span className="w-3 h-3 rounded-sm" style={{ backgroundColor: 'rgba(245,158,11,0.45)' }} />
            Warm
          </span>
          <span className="flex items-center gap-1">
            <span className="w-3 h-3 rounded-sm" style={{ backgroundColor: 'rgba(34,197,94,0.6)' }} />
            Hot
          </span>
        </div>
      </div>

      {/* Grid */}
      <div className="grid grid-cols-[repeat(auto-fill,minmax(88px,1fr))] gap-1.5">
        {tiles.map((row) => (
          <HeatmapTile
            key={row.symbol}
            row={row}
            onClick={() => openDetail(row.symbol_id)}
            onHover={handleTileHover}
            onLeave={handleTileLeave}
          />
        ))}
      </div>

      {/* Floating Popup Card */}
      {hoveredSymbol && (
        <HeatmapPopup row={hoveredSymbol} pos={popupPos} containerRef={containerRef} />
      )}
    </div>
  );
}

/* ── Heatmap Tile ──────────────────────────────────────────────── */

const HeatmapTile = memo(function HeatmapTile({
  row,
  onClick,
  onHover,
  onLeave,
}: {
  row: SymbolRow;
  onClick: () => void;
  onHover: (row: SymbolRow, e: React.MouseEvent) => void;
  onLeave: () => void;
}) {
  const bgColor = scoreToColor(row.composite_score);
  const textColor = scoreToTextColor(row.composite_score);
  const borderColor = scoreToBorderColor(row.composite_score);
  const glow = scoreToGlow(row.composite_score);
  const changePct = row.price_change_pct_24h;

  return (
    <button
      onClick={onClick}
      onMouseEnter={(e) => onHover(row, e)}
      onMouseMove={(e) => onHover(row, e)}
      onMouseLeave={onLeave}
      className="relative rounded-lg p-2 text-center cursor-pointer
        transition-all duration-200 ease-out
        hover:scale-110 hover:z-20
        active:scale-100"
      style={{
        backgroundColor: bgColor,
        border: `1px solid ${borderColor}`,
        boxShadow: glow,
      }}
    >
      <div className="text-[11px] font-bold truncate" style={{ color: textColor }}>
        {row.symbol.replace('/USDT', '').replace('/USD', '')}
      </div>
      <div
        className="text-[10px] font-mono font-semibold"
        style={{ color: changePct != null && changePct >= 0 ? '#4ade80' : '#f87171' }}
      >
        {changePct != null ? `${changePct >= 0 ? '+' : ''}${changePct.toFixed(1)}%` : '—'}
      </div>
      <div className="text-[9px] text-zinc-500 mt-0.5">
        {row.volume_ratio != null ? `${row.volume_ratio.toFixed(1)}x vol` : ''}
      </div>
    </button>
  );
});

/* ── Floating Popup Card ──────────────────────────────────────── */

function HeatmapPopup({
  row,
  pos,
  containerRef,
}: {
  row: SymbolRow;
  pos: { x: number; y: number };
  containerRef: React.RefObject<HTMLDivElement | null>;
}) {
  const containerRect = containerRef.current?.getBoundingClientRect();
  const popupW = 260;
  const popupH = 200;

  // Keep popup within container bounds
  let left = pos.x + 16;
  let top = pos.y - 20;

  if (containerRect) {
    if (left + popupW > containerRect.width) left = pos.x - popupW - 16;
    if (top + popupH > containerRect.height) top = containerRect.height - popupH - 8;
    if (top < 0) top = 8;
  }

  const changePct = row.price_change_pct_24h;
  const score = row.composite_score;

  return (
    <div
      className="absolute z-50 pointer-events-none animate-in fade-in-0 zoom-in-95"
      style={{ left, top }}
    >
      <div
        className="w-[260px] rounded-xl p-4 backdrop-blur-xl
          border border-zinc-600/40"
        style={{
          background: 'linear-gradient(135deg, rgba(24,24,27,0.95), rgba(39,39,42,0.9))',
          boxShadow: `
            0 4px 6px -1px rgba(0,0,0,0.4),
            0 10px 15px -3px rgba(0,0,0,0.3),
            0 20px 40px -4px rgba(0,0,0,0.5),
            inset 0 1px 0 rgba(255,255,255,0.05)
          `,
        }}
      >
        {/* Header */}
        <div className="flex items-center justify-between mb-3">
          <div>
            <div className="text-sm font-bold text-zinc-100">
              {row.symbol}
            </div>
            <div className="text-[10px] text-zinc-500">{row.exchange}</div>
          </div>
          {score != null && (
            <div
              className="text-xs font-bold px-2 py-1 rounded-md"
              style={{
                backgroundColor: score >= 80
                  ? 'rgba(34,197,94,0.2)'
                  : score >= 60
                    ? 'rgba(245,158,11,0.2)'
                    : 'rgba(63,63,70,0.5)',
                color: score >= 80 ? '#4ade80' : score >= 60 ? '#fbbf24' : '#a1a1aa',
              }}
            >
              Score {score.toFixed(0)}
            </div>
          )}
        </div>

        {/* Price */}
        <div className="mb-3">
          <div className="text-lg font-bold font-mono text-zinc-100">
            {fmt(row.current_price)}
          </div>
          <div
            className="text-sm font-mono font-semibold"
            style={{ color: changePct != null && changePct >= 0 ? '#4ade80' : '#f87171' }}
          >
            {changePct != null ? `${changePct >= 0 ? '▲' : '▼'} ${Math.abs(changePct).toFixed(2)}%` : '—'}
          </div>
        </div>

        {/* Metrics Grid */}
        <div className="grid grid-cols-2 gap-2">
          <PopupMetric label="Volume 24h" value={fmt(row.volume_24h)} />
          <PopupMetric
            label="Vol Ratio"
            value={row.volume_ratio != null ? `${row.volume_ratio.toFixed(1)}x` : '—'}
            highlight={(row.volume_ratio ?? 0) >= 3}
          />
          <PopupMetric
            label="Volatility"
            value={row.realized_volatility != null ? row.realized_volatility.toFixed(4) : '—'}
          />
          <PopupMetric
            label="Vol Pctile"
            value={row.volatility_percentile != null ? `${row.volatility_percentile.toFixed(0)}th` : '—'}
            highlight={(row.volatility_percentile ?? 0) >= 90}
          />
        </div>

        {/* Click hint */}
        <div className="mt-3 pt-2 border-t border-zinc-700/50 text-center">
          <span className="text-[10px] text-zinc-600">Click tile for detailed chart →</span>
        </div>
      </div>
    </div>
  );
}

function PopupMetric({
  label,
  value,
  highlight,
}: {
  label: string;
  value: string;
  highlight?: boolean;
}) {
  return (
    <div>
      <div className="text-[9px] text-zinc-500 uppercase tracking-wider">{label}</div>
      <div
        className={`text-xs font-mono font-semibold ${
          highlight ? 'text-amber-400' : 'text-zinc-300'
        }`}
      >
        {value}
      </div>
    </div>
  );
}
