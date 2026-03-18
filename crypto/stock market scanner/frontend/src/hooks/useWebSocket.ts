/**
 * useWebSocket — connects to the scanner WS endpoint and pushes
 * real-time price/volume updates into the Zustand store.
 *
 * Reconnect: exponential backoff 1s → 2s → 4s → cap 30s.
 * Batching: collects messages for 200ms then flushes as one store update
 *           to avoid per-tick re-renders.
 */

import { useEffect, useRef } from 'react';
import { useStore } from '../store/scanner';
import type { AlertItem, SymbolRow } from '../types/scanner';

const WS_URL = import.meta.env.VITE_WS_URL ?? `ws://${window.location.host}/ws/scanner`;
const MAX_BACKOFF = 30_000;
const BATCH_INTERVAL = 200; // ms — batch WS ticks before flushing to store

export function useWebSocket() {
  const batchUpdateSymbols = useStore((s) => s.batchUpdateSymbols);
  const addAlert = useStore((s) => s.addAlert);
  const setWsConnected = useStore((s) => s.setWsConnected);

  const wsRef = useRef<WebSocket | null>(null);
  const backoffRef = useRef(1000);
  const bufferRef = useRef<Array<{ symbol: string; patch: Partial<SymbolRow> }>>([]);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    let unmounted = false;

    // Flush buffered WS updates to store every BATCH_INTERVAL ms
    timerRef.current = setInterval(() => {
      const buf = bufferRef.current;
      if (buf.length === 0) return;
      bufferRef.current = [];
      batchUpdateSymbols(buf);
    }, BATCH_INTERVAL);

    function connect() {
      if (unmounted) return;
      const ws = new WebSocket(WS_URL);
      wsRef.current = ws;

      ws.onopen = () => {
        backoffRef.current = 1000;
        setWsConnected(true);
      };

      ws.onmessage = (ev) => {
        try {
          const msg = JSON.parse(ev.data);

          if (msg.type === 'price') {
            // { type: "price", symbol: "BTC/USDT", price: 65000, volume_ratio: 4.2, ... }
            bufferRef.current.push({
              symbol: msg.symbol,
              patch: {
                current_price: msg.price,
                price_change_pct_24h: msg.change_pct,
                volume_ratio: msg.volume_ratio,
              },
            });
          } else if (msg.type === 'alert') {
            addAlert(msg.alert as AlertItem);
          }
        } catch {
          // ignore malformed messages
        }
      };

      ws.onclose = () => {
        setWsConnected(false);
        if (unmounted) return;
        const delay = backoffRef.current;
        backoffRef.current = Math.min(delay * 2, MAX_BACKOFF);
        setTimeout(connect, delay);
      };

      ws.onerror = () => ws.close();
    }

    connect();

    return () => {
      unmounted = true;
      if (timerRef.current) clearInterval(timerRef.current);
      wsRef.current?.close();
    };
  }, [batchUpdateSymbols, addAlert, setWsConnected]);
}
