/**
 * Heatmap — grid of symbol tiles colored by composite_score.
 *
 * Color scale:
 *   0–30  → dark cool (blue-gray)
 *   30–60 → neutral (slate)
 *   60–80 → warm (amber)
 *   80+   → hot (emerald/green glow)
 *
 * Tile size proportional to log(volume_24h) so high-volume symbols
 * are visually larger (treemap-like layout via CSS grid auto-fill).
 */

import { memo, useMemo } from 'react';
import { useStore } from '../../store/scanner';
import type { SymbolRow } from '../../types/scanner';

function scoreToColor(score: number | null): string {
  if (score == null) return 'rgba(63,63,70,0.5)';  // zinc-700/50
  // Clamp to 0-100
  const s = Math.max(0, Math.min(100, score));

  if (s >= 80) {
    // Hot: emerald glow
    const intensity = (s - 80) / 20;
    return `rgba(34,197,94,${0.3 + intensity * 0.5})`;
  }
  if (s >= 60) {
    // Warm: amber
    const intensity = (s - 60) / 20;
    return `rgba(245,158,11,${0.2 + intensity * 0.4})`;
  }
  if (s >= 30) {
    // Neutral: slate
    const intensity = (s - 30) / 30;
    return `rgba(148,163,184,${0.1 + intensity * 0.2})`;
  }
  // Cool: blue-gray
  return `rgba(100,116,139,${0.08 + (s / 30) * 0.12})`;
}

function scoreToTextColor(score: number | null): string {
  if (score == null) return '#71717a';
  if (score >= 80) return '#4ade80';
  if (score >= 60) return '#fbbf24';
  return '#a1a1aa';
}

export function Heatmap() {
  const symbols = useStore((s) => s.symbols);
  const openDetail = useStore((s) => s.openDetail);

  // Sort by composite_score descending for visual grouping
  const tiles = useMemo(
    () => [...symbols].sort((a, b) => (b.composite_score ?? 0) - (a.composite_score ?? 0)).slice(0, 100),
    [symbols],
  );

  return (
    <div className="p-3">
      <h3 className="text-xs font-semibold text-zinc-500 uppercase mb-2">
        Heatmap — Top 100 by Composite Score
      </h3>
      <div className="grid grid-cols-[repeat(auto-fill,minmax(72px,1fr))] gap-1">
        {tiles.map((row) => (
          <HeatmapTile key={row.symbol} row={row} onClick={() => openDetail(row.symbol_id)} />
        ))}
      </div>
    </div>
  );
}

const HeatmapTile = memo(function HeatmapTile({
  row,
  onClick,
}: {
  row: SymbolRow;
  onClick: () => void;
}) {
  const bgColor = scoreToColor(row.composite_score);
  const textColor = scoreToTextColor(row.composite_score);
  const changePct = row.price_change_pct_24h;

  return (
    <button
      onClick={onClick}
      className="relative rounded p-1.5 text-center transition-transform hover:scale-105 hover:z-10 cursor-pointer border border-transparent hover:border-zinc-600"
      style={{ backgroundColor: bgColor }}
    >
      <div className="text-[11px] font-bold truncate" style={{ color: textColor }}>
        {row.symbol.replace('/USDT', '').replace('/USD', '')}
      </div>
      <div
        className="text-[10px] font-mono"
        style={{ color: changePct != null && changePct >= 0 ? '#4ade80' : '#f87171' }}
      >
        {changePct != null ? `${changePct >= 0 ? '+' : ''}${changePct.toFixed(1)}%` : '—'}
      </div>
      <div className="text-[9px] text-zinc-500">
        {row.volume_ratio != null ? `${row.volume_ratio.toFixed(1)}x` : ''}
      </div>
    </button>
  );
});
