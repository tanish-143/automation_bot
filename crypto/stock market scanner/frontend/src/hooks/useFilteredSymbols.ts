import { useMemo } from 'react';
import { useStore } from '../store/scanner';
import type { SymbolRow } from '../types/scanner';

export function useFilteredSymbols(): SymbolRow[] {
  const symbols = useStore((s) => s.symbols);
  const filters = useStore((s) => s.filters);

  return useMemo(() => {
    return symbols.filter((row) => {
      // Min 24h volume
      if (row.volume_24h != null && row.volume_24h < filters.minVolume) return false;

      // Volume ratio threshold (skip null — CoinGecko data won't have it)
      if (row.volume_ratio != null && row.volume_ratio < filters.volumeRatioMin) return false;

      // Volatility percentile threshold (skip null)
      if (row.volatility_percentile != null && row.volatility_percentile < filters.volatilityPctMin)
        return false;

      // Exchange
      if (filters.exchange !== 'all' && row.exchange !== filters.exchange) return false;

      // Asset class
      if (filters.assetClass !== 'all' && row.asset_class !== filters.assetClass) return false;

      return true;
    });
  }, [symbols, filters]);
}
