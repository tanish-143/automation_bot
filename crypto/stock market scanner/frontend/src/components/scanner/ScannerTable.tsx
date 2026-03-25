/**
 * ScannerTable — sortable table showing top movers.
 *
 * Performance strategy:
 *   • Each <SymbolRow> is React.memo — skips re-render if its SymbolRow
 *     object ref hasn't changed (Zustand produces new refs only for
 *     patched rows via batchUpdateSymbols).
 *   • Sorting is local via useMemo — no API call on sort change.
 *   • Click row → opens detail modal via store action.
 */

import { memo, useMemo, useState } from 'react';
import { useStore } from '../../store/scanner';
import { useFilteredSymbols } from '../../hooks/useFilteredSymbols';
import { Sparkline } from '../common/Sparkline';
import type { SymbolRow } from '../../types/scanner';
import clsx from 'clsx';

type SortKey = 'symbol' | 'current_price' | 'volume_ratio' | 'price_change_pct_24h' | 'realized_volatility' | 'composite_score';

function fmt(n: number | null | undefined, decimals = 2): string {
  if (n == null) return '—';
  if (Math.abs(n) >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`;
  if (Math.abs(n) >= 1_000) return `${(n / 1_000).toFixed(1)}K`;
  return n.toFixed(decimals);
}

function pctClass(v: number | null): string {
  if (v == null) return 'text-zinc-500';
  return v >= 0 ? 'text-emerald-400' : 'text-red-400';
}

export function ScannerTable() {
  const symbols = useFilteredSymbols();
  const openDetail = useStore((s) => s.openDetail);
  const [sortKey, setSortKey] = useState<SortKey>('composite_score');
  const [sortDesc, setSortDesc] = useState(true);

  const sorted = useMemo(() => {
    const copy = [...symbols];
    copy.sort((a, b) => {
      const av = a[sortKey] ?? -Infinity;
      const bv = b[sortKey] ?? -Infinity;
      return sortDesc ? (bv > av ? 1 : -1) : (av > bv ? 1 : -1);
    });
    return copy;
  }, [symbols, sortKey, sortDesc]);

  const toggleSort = (key: SortKey) => {
    if (sortKey === key) setSortDesc((d) => !d);
    else { setSortKey(key); setSortDesc(true); }
  };

  const sortIcon = (key: SortKey) =>
    sortKey === key ? (sortDesc ? ' ▼' : ' ▲') : '';

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-sm">
        <thead className="sticky top-0 bg-zinc-900 z-10">
          <tr className="text-zinc-500 text-xs uppercase tracking-wider">
            {[
              { key: 'symbol' as SortKey, label: 'Symbol' },
              { key: 'current_price' as SortKey, label: 'Price' },
              { key: 'price_change_pct_24h' as SortKey, label: '24h %' },
              { key: 'volume_ratio' as SortKey, label: 'Vol Ratio' },
              { key: 'realized_volatility' as SortKey, label: 'Volatility' },
              { key: 'composite_score' as SortKey, label: 'Score' },
            ].map(({ key, label }) => (
              <th
                key={key}
                onClick={() => toggleSort(key)}
                className="px-3 py-2 text-left cursor-pointer hover:text-zinc-300 select-none"
              >
                {label}{sortIcon(key)}
              </th>
            ))}
            <th className="px-3 py-2 text-left">Spark</th>
          </tr>
        </thead>
        <tbody>
          {sorted.map((row) => (
            <SymbolRowComponent
              key={row.symbol}
              row={row}
              onClick={() => openDetail(row.symbol_id)}
            />
          ))}
        </tbody>
      </table>
    </div>
  );
}

/* ── Memoized Row ──────────────────────────────────────────────── */

const SymbolRowComponent = memo(function SymbolRowComponent({
  row,
  onClick,
}: {
  row: SymbolRow;
  onClick: () => void;
}) {
  return (
    <tr
      onClick={onClick}
      className="border-b border-zinc-800/50 hover:bg-zinc-800/40 cursor-pointer transition-colors"
    >
      <td className="px-3 py-2">
        <div className="font-medium text-zinc-100">{row.symbol}</div>
        <div className="text-[10px] text-zinc-600">{row.exchange}</div>
      </td>
      <td className="px-3 py-2 font-mono text-zinc-200">${fmt(row.current_price, 2)}</td>
      <td className={clsx('px-3 py-2 font-mono', pctClass(row.price_change_pct_24h))}>
        {row.price_change_pct_24h != null ? `${row.price_change_pct_24h >= 0 ? '+' : ''}${row.price_change_pct_24h.toFixed(2)}%` : '—'}
      </td>
      <td className="px-3 py-2 font-mono">
        <span className={clsx(
          (row.volume_ratio ?? 0) >= 3 ? 'text-amber-400 font-bold' : 'text-zinc-300',
        )}>
          {fmt(row.volume_ratio, 1)}x
        </span>
      </td>
      <td className="px-3 py-2 font-mono text-zinc-300">
        {fmt(row.realized_volatility, 4)}
        {row.volatility_percentile != null && (
          <span className="text-[10px] text-zinc-500 ml-1">p{row.volatility_percentile.toFixed(0)}</span>
        )}
      </td>
      <td className="px-3 py-2">
        <ScoreBadge score={row.composite_score} />
      </td>
      <td className="px-3 py-2">
        <Sparkline
          data={row.sparkline ?? []}
          positive={row.price_change_pct_24h != null ? row.price_change_pct_24h >= 0 : undefined}
        />
      </td>
    </tr>
  );
});

function ScoreBadge({ score }: { score: number | null }) {
  if (score == null) return <span className="text-zinc-600">—</span>;
  const bg =
    score >= 80 ? 'bg-emerald-500/20 text-emerald-400' :
    score >= 60 ? 'bg-amber-500/20 text-amber-400' :
    'bg-zinc-700/50 text-zinc-400';
  return (
    <span className={`inline-block px-2 py-0.5 rounded text-xs font-bold ${bg}`}>
      {score.toFixed(1)}
    </span>
  );
}
