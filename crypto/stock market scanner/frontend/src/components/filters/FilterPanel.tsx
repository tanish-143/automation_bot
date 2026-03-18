import { useStore } from '../../store/scanner';
import type { Session, AssetClass } from '../../types/scanner';

const SESSIONS: { value: Session; label: string }[] = [
  { value: 'all', label: 'All' },
  { value: 'asia', label: '🌏 Asia' },
  { value: 'europe', label: '🌍 EU' },
  { value: 'us', label: '🌎 US' },
];

const EXCHANGES = ['all', 'binance', 'coinbase', 'nasdaq', 'nyse'];

export function FilterPanel() {
  const filters = useStore((s) => s.filters);
  const setFilters = useStore((s) => s.setFilters);

  return (
    <aside className="w-64 shrink-0 border-r border-zinc-800 p-4 space-y-5 overflow-y-auto bg-zinc-950 max-lg:hidden">
      <h2 className="text-sm font-semibold text-zinc-400 uppercase tracking-wider">Filters</h2>

      {/* Session / Timezone */}
      <fieldset>
        <legend className="text-xs text-zinc-500 mb-1.5">Session</legend>
        <div className="flex flex-wrap gap-1.5">
          {SESSIONS.map((s) => (
            <button
              key={s.value}
              onClick={() => setFilters({ session: s.value })}
              className={`px-2.5 py-1 rounded text-xs font-medium transition
                ${filters.session === s.value
                  ? 'bg-indigo-600 text-white'
                  : 'bg-zinc-800 text-zinc-400 hover:bg-zinc-700'}`}
            >
              {s.label}
            </button>
          ))}
        </div>
      </fieldset>

      {/* Timeframe */}
      <fieldset>
        <legend className="text-xs text-zinc-500 mb-1.5">Timeframe</legend>
        <div className="flex flex-wrap gap-1.5">
          {(['1m', '5m', '15m', '1h', '4h', '1d'] as const).map((tf) => (
            <button
              key={tf}
              onClick={() => setFilters({ timeframe: tf })}
              className={`px-2.5 py-1 rounded text-xs font-medium transition
                ${filters.timeframe === tf
                  ? 'bg-indigo-600 text-white'
                  : 'bg-zinc-800 text-zinc-400 hover:bg-zinc-700'}`}
            >
              {tf}
            </button>
          ))}
        </div>
      </fieldset>

      {/* Volume Spike Threshold */}
      <fieldset>
        <legend className="text-xs text-zinc-500 mb-1.5">
          Volume Spike ≥ <span className="text-indigo-400">{filters.volumeRatioMin}x</span>
        </legend>
        <input
          type="range"
          min={1} max={10} step={0.5}
          value={filters.volumeRatioMin}
          onChange={(e) => setFilters({ volumeRatioMin: +e.target.value })}
          className="w-full accent-indigo-500"
        />
        <div className="flex justify-between text-[10px] text-zinc-600">
          <span>1x</span><span>10x</span>
        </div>
      </fieldset>

      {/* Volatility Percentile */}
      <fieldset>
        <legend className="text-xs text-zinc-500 mb-1.5">
          Volatility ≥ <span className="text-indigo-400">{filters.volatilityPctMin}th</span> pctile
        </legend>
        <input
          type="range"
          min={50} max={99} step={1}
          value={filters.volatilityPctMin}
          onChange={(e) => setFilters({ volatilityPctMin: +e.target.value })}
          className="w-full accent-indigo-500"
        />
        <div className="flex justify-between text-[10px] text-zinc-600">
          <span>50th</span><span>99th</span>
        </div>
      </fieldset>

      {/* Min Volume */}
      <fieldset>
        <legend className="text-xs text-zinc-500 mb-1.5">Min 24h Volume ($)</legend>
        <input
          type="number"
          min={0} step={10000}
          value={filters.minVolume}
          onChange={(e) => setFilters({ minVolume: +e.target.value })}
          className="w-full bg-zinc-800 border border-zinc-700 rounded px-2 py-1 text-sm text-zinc-200"
        />
      </fieldset>

      {/* Exchange */}
      <fieldset>
        <legend className="text-xs text-zinc-500 mb-1.5">Exchange</legend>
        <select
          value={filters.exchange}
          onChange={(e) => setFilters({ exchange: e.target.value })}
          className="w-full bg-zinc-800 border border-zinc-700 rounded px-2 py-1 text-sm text-zinc-200"
        >
          {EXCHANGES.map((ex) => (
            <option key={ex} value={ex}>{ex === 'all' ? 'All Exchanges' : ex}</option>
          ))}
        </select>
      </fieldset>

      {/* Asset Class */}
      <fieldset>
        <legend className="text-xs text-zinc-500 mb-1.5">Asset Class</legend>
        <div className="flex gap-1.5">
          {(['all', 'crypto', 'stock'] as const).map((ac) => (
            <button
              key={ac}
              onClick={() => setFilters({ assetClass: ac as AssetClass | 'all' })}
              className={`px-2.5 py-1 rounded text-xs font-medium transition
                ${filters.assetClass === ac
                  ? 'bg-indigo-600 text-white'
                  : 'bg-zinc-800 text-zinc-400 hover:bg-zinc-700'}`}
            >
              {ac === 'all' ? 'All' : ac}
            </button>
          ))}
        </div>
      </fieldset>
    </aside>
  );
}
