const API = import.meta.env.VITE_API_BASE ?? '';

async function get<T>(path: string, params?: Record<string, string | number>): Promise<T> {
  const url = new URL(`${API}${path}`, window.location.origin);
  if (params) {
    Object.entries(params).forEach(([k, v]) => {
      if (v !== undefined && v !== '' && v !== 'all') url.searchParams.set(k, String(v));
    });
  }
  const res = await fetch(url.toString());
  if (!res.ok) throw new Error(`API ${res.status}: ${await res.text()}`);
  return res.json();
}

export interface LivePrice {
  symbol: string;
  current_price: number;
  price_change_pct_24h: number;
  volume_24h: number;
  high_24h: number;
  low_24h: number;
  market_cap: number;
  market_cap_rank: number | null;
  sparkline: number[];
  image: string;
  last_updated: string;
}

export const api = {
  topMovers: (params: Record<string, string | number>) =>
    get<import('../types/scanner').SymbolRow[]>('/scan/top-movers', params),

  alerts: (params: Record<string, string | number> = {}) =>
    get<import('../types/scanner').AlertItem[]>('/scan/alerts', { user_id: 3, ...params }),

  snapshot: (symbolId: number, hours = 24) =>
    get<import('../types/scanner').SnapshotPoint[]>(`/scan/snapshot/${symbolId}`, { hours }),

  livePrices: () => get<LivePrice[]>('/scan/live-prices'),

  saveRule: async (userId: number, body: Record<string, unknown>) => {
    const res = await fetch(`${API}/scan/rules?user_id=${userId}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });
    if (!res.ok) throw new Error(`API ${res.status}`);
    return res.json();
  },
};
