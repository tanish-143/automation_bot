import { useState, useRef, useEffect } from 'react';
import { api } from '../../lib/api';

type CoinCategory = 'meme' | 'regular' | 'ai';

const CATEGORIES: { key: CoinCategory; label: string; emoji: string; desc: string }[] = [
  { key: 'meme',    label: 'Meme Coins',    emoji: '🐸', desc: 'DOGE, SHIB, PEPE, FLOKI…' },
  { key: 'regular', label: 'Regular Coins',  emoji: '💎', desc: 'BTC, ETH, SOL, BNB, XRP…' },
  { key: 'ai',      label: 'AI Coins',       emoji: '🤖', desc: 'FET, RENDER, TAO, GRT…' },
];

export function TelegramSignal() {
  const [open, setOpen] = useState(false);
  const [sending, setSending] = useState<CoinCategory | null>(null);
  const [flash, setFlash] = useState<{ msg: string; ok: boolean } | null>(null);
  const ref = useRef<HTMLDivElement>(null);

  // Close dropdown on outside click
  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false);
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, []);

  const send = async (cat: CoinCategory) => {
    setSending(cat);
    setFlash(null);
    try {
      const result = await api.telegramSignal(cat);
      setFlash({ msg: result.message, ok: true });
    } catch {
      setFlash({ msg: 'Failed to send signal', ok: false });
    } finally {
      setSending(null);
      setTimeout(() => setFlash(null), 3000);
    }
  };

  return (
    <div ref={ref} className="relative">
      {/* Trigger button */}
      <button
        onClick={() => setOpen((v) => !v)}
        className="flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-md
          bg-sky-600/20 text-sky-300 border border-sky-500/30
          hover:bg-sky-600/30 hover:border-sky-500/50 hover:text-sky-200
          transition-all duration-200 active:scale-95"
        title="Send Telegram Signal"
      >
        <span className="text-sm">✈️</span>
        TG Signal
      </button>

      {/* Dropdown */}
      {open && (
        <div className="absolute right-0 top-full mt-1.5 z-50 w-64 rounded-lg border border-zinc-700/80
          bg-zinc-900/95 backdrop-blur-md shadow-2xl overflow-hidden">
          <div className="px-3 py-2 border-b border-zinc-800 text-[11px] text-zinc-500 uppercase tracking-wider">
            Send Telegram Signal
          </div>
          {CATEGORIES.map((cat) => {
            const isSending = sending === cat.key;
            return (
              <button
                key={cat.key}
                onClick={() => { send(cat.key); setOpen(false); }}
                disabled={sending !== null}
                className="w-full flex items-start gap-3 px-3 py-2.5 text-left
                  hover:bg-zinc-800/80 disabled:opacity-40 disabled:cursor-wait
                  transition-colors duration-150 border-b border-zinc-800/50 last:border-b-0"
              >
                <span className="text-lg mt-0.5">{cat.emoji}</span>
                <div className="min-w-0">
                  <div className="text-xs font-medium text-zinc-200 flex items-center gap-2">
                    {cat.label}
                    {isSending && (
                      <svg className="w-3 h-3 animate-spin text-sky-400" viewBox="0 0 24 24" fill="none">
                        <circle cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="3" className="opacity-25" />
                        <path d="M4 12a8 8 0 018-8" stroke="currentColor" strokeWidth="3" strokeLinecap="round" />
                      </svg>
                    )}
                  </div>
                  <div className="text-[10px] text-zinc-500 mt-0.5">{cat.desc}</div>
                </div>
              </button>
            );
          })}
        </div>
      )}

      {/* Toast */}
      {flash && (
        <div className={`absolute right-0 top-full mt-1.5 z-50 px-3 py-1.5 rounded-md text-xs font-medium
          shadow-lg whitespace-nowrap
          ${flash.ok
            ? 'bg-emerald-600/90 text-emerald-100 border border-emerald-500/40'
            : 'bg-red-600/90 text-red-100 border border-red-500/40'}`}
        >
          {flash.ok ? '✅' : '❌'} {flash.msg}
        </div>
      )}
    </div>
  );
}
