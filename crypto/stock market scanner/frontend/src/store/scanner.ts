/**
 * Zustand Store — Single source of truth for scanner state.
 *
 * Why Zustand over Redux/Context:
 *   • Selective subscriptions: `useStore(s => s.symbols)` — only re-renders
 *     when `symbols` ref changes. Critical when WS pushes 10+ updates/sec.
 *   • No provider wrapper needed.
 *   • 1KB bundle, zero boilerplate.
 *
 * Real-time update strategy:
 *   WebSocket messages update individual rows via Map keyed by symbol.
 *   We shallow-merge the changed row, producing a new array ref only for
 *   the rows slice — so the table re-renders, but each <Row> component
 *   that uses React.memo skips re-render if its own props haven't changed.
 */

import { create } from 'zustand';
import type { SymbolRow, AlertItem, FilterState } from '../types/scanner';

interface ScannerStore {
  /* ── Data ── */
  symbols: SymbolRow[];
  symbolMap: Map<string, SymbolRow>;
  alerts: AlertItem[];
  selectedSymbolId: number | null;

  /* ── Filters ── */
  filters: FilterState;

  /* ── UI ── */
  detailOpen: boolean;
  wsConnected: boolean;

  /* ── Actions ── */
  setSymbols: (rows: SymbolRow[]) => void;
  updateSymbol: (symbol: string, patch: Partial<SymbolRow>) => void;
  batchUpdateSymbols: (updates: Array<{ symbol: string; patch: Partial<SymbolRow> }>) => void;
  addAlert: (alert: AlertItem) => void;
  setAlerts: (alerts: AlertItem[]) => void;
  setFilters: (f: Partial<FilterState>) => void;
  openDetail: (symbolId: number) => void;
  closeDetail: () => void;
  setWsConnected: (v: boolean) => void;
}

const DEFAULT_FILTERS: FilterState = {
  session: 'all',
  timeframe: '1h',
  volumeRatioMin: 1,
  volatilityPctMin: 50,
  minVolume: 10_000,
  exchange: 'all',
  assetClass: 'all',
};

export const useStore = create<ScannerStore>((set, get) => ({
  symbols: [],
  symbolMap: new Map(),
  alerts: [],
  selectedSymbolId: null,
  filters: DEFAULT_FILTERS,
  detailOpen: false,
  wsConnected: false,

  setSymbols: (rows) => {
    const map = new Map<string, SymbolRow>();
    rows.forEach((r) => map.set(r.symbol, r));
    set({ symbols: rows, symbolMap: map });
  },

  /**
   * Patch a single symbol row from a WS tick.
   * Only the changed row gets a new object ref → React.memo skips the rest.
   */
  updateSymbol: (symbol, patch) => {
    const { symbolMap, symbols } = get();
    const existing = symbolMap.get(symbol);
    if (!existing) return;

    const updated = { ...existing, ...patch };
    const newMap = new Map(symbolMap);
    newMap.set(symbol, updated);

    set({
      symbols: symbols.map((r) => (r.symbol === symbol ? updated : r)),
      symbolMap: newMap,
    });
  },

  /**
   * Batch update multiple symbols in one state transition.
   * Avoids N individual re-renders when WS flushes a batch.
   */
  batchUpdateSymbols: (updates) => {
    const { symbolMap, symbols } = get();
    const newMap = new Map(symbolMap);
    const changedSet = new Set<string>();

    for (const { symbol, patch } of updates) {
      const existing = newMap.get(symbol);
      if (existing) {
        newMap.set(symbol, { ...existing, ...patch });
        changedSet.add(symbol);
      }
    }

    set({
      symbols: symbols.map((r) =>
        changedSet.has(r.symbol) ? newMap.get(r.symbol)! : r,
      ),
      symbolMap: newMap,
    });
  },

  addAlert: (alert) =>
    set((s) => ({ alerts: [alert, ...s.alerts].slice(0, 500) })),

  setAlerts: (alerts) => set({ alerts }),

  setFilters: (f) =>
    set((s) => ({ filters: { ...s.filters, ...f } })),

  openDetail: (symbolId) =>
    set({ selectedSymbolId: symbolId, detailOpen: true }),

  closeDetail: () => set({ detailOpen: false }),

  setWsConnected: (v) => set({ wsConnected: v }),
}));
