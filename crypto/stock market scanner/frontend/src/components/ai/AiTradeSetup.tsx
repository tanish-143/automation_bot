/**
 * AiTradeSetup — Full-screen modal showing Groq AI trade analysis.
 *
 * Features:
 *   • Fetches live prices → sends to Groq → renders markdown
 *   • Glassmorphism modal with shadow UI
 *   • Typewriter-style content reveal
 *   • Loading skeleton while AI processes
 */

import { useState, useCallback, useEffect } from 'react';
import { api } from '../../lib/api';

interface Props {
  open: boolean;
  onClose: () => void;
}

export function AiTradeSetup({ open, onClose }: Props) {
  const [analysis, setAnalysis] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [coinCount, setCoinCount] = useState(0);

  const fetchAnalysis = useCallback(async () => {
    setLoading(true);
    setError(null);
    setAnalysis(null);
    try {
      const result = await api.aiAnalysis();
      setAnalysis(result.analysis);
      setCoinCount(result.coin_count);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Analysis failed');
    } finally {
      setLoading(false);
    }
  }, []);

  // Auto-fetch when modal opens
  useEffect(() => {
    if (open && !analysis && !loading) {
      fetchAnalysis();
    }
  }, [open, analysis, loading, fetchAnalysis]);

  // Close on Escape
  useEffect(() => {
    if (!open) return;
    const handler = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    document.addEventListener('keydown', handler);
    return () => document.removeEventListener('keydown', handler);
  }, [open, onClose]);

  if (!open) return null;

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center">
      {/* Backdrop */}
      <div
        className="absolute inset-0 bg-black/60 backdrop-blur-sm"
        onClick={onClose}
      />

      {/* Modal */}
      <div
        className="relative z-10 w-full max-w-3xl max-h-[85vh] mx-4 rounded-2xl
          border border-zinc-700/50 overflow-hidden flex flex-col animate-in"
        style={{
          background: 'linear-gradient(135deg, rgba(15,15,20,0.98), rgba(24,24,30,0.95))',
          boxShadow: `
            0 0 0 1px rgba(99,102,241,0.1),
            0 8px 16px -4px rgba(0,0,0,0.5),
            0 20px 50px -8px rgba(0,0,0,0.6),
            0 0 80px -20px rgba(99,102,241,0.15)
          `,
        }}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-zinc-800/80">
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 rounded-lg bg-indigo-600/20 flex items-center justify-center">
              <span className="text-lg">🤖</span>
            </div>
            <div>
              <h2 className="text-sm font-bold text-zinc-100">AI Trade Setup</h2>
              <p className="text-[10px] text-zinc-500">
                Powered by Groq · Llama 3.3 70B
                {coinCount > 0 && <span> · {coinCount} coins analyzed</span>}
              </p>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <button
              onClick={fetchAnalysis}
              disabled={loading}
              className="flex items-center gap-1.5 text-xs px-3 py-1.5 rounded-md
                bg-indigo-600/20 text-indigo-300 border border-indigo-500/30
                hover:bg-indigo-600/30 hover:text-indigo-200
                disabled:opacity-50 disabled:cursor-not-allowed
                transition-all duration-200"
            >
              <svg
                className={`w-3 h-3 ${loading ? 'animate-spin' : ''}`}
                fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
              >
                <path strokeLinecap="round" strokeLinejoin="round"
                  d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"
                />
              </svg>
              {loading ? 'Analyzing…' : 'Re-analyze'}
            </button>
            <button
              onClick={onClose}
              className="text-zinc-500 hover:text-zinc-200 text-xl leading-none px-2 py-1
                hover:bg-zinc-800 rounded transition-colors"
            >
              ✕
            </button>
          </div>
        </div>

        {/* Content */}
        <div className="flex-1 overflow-y-auto px-6 py-4">
          {loading && <LoadingSkeleton />}
          {error && (
            <div className="flex items-center gap-3 p-4 rounded-lg bg-red-500/10 border border-red-500/20">
              <span className="text-red-400 text-lg">⚠️</span>
              <div>
                <div className="text-sm font-medium text-red-300">Analysis Failed</div>
                <div className="text-xs text-red-400/80 mt-0.5">{error}</div>
              </div>
            </div>
          )}
          {analysis && !loading && (
            <div className="ai-markdown prose prose-invert prose-sm max-w-none">
              <MarkdownRenderer content={analysis} />
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="px-6 py-3 border-t border-zinc-800/80 flex items-center justify-between">
          <span className="text-[10px] text-zinc-600">
            ⚡ AI analysis is for informational purposes only — not financial advice
          </span>
          <span className="text-[10px] text-zinc-600">
            {analysis && `${analysis.length} chars`}
          </span>
        </div>
      </div>
    </div>
  );
}

/* ── Markdown Renderer ──────────────────────────────────────────── */

function MarkdownRenderer({ content }: { content: string }) {
  const lines = content.split('\n');

  return (
    <div className="space-y-1">
      {lines.map((line, i) => (
        <MarkdownLine key={i} line={line} />
      ))}
    </div>
  );
}

function MarkdownLine({ line }: { line: string }) {
  // Heading ##
  if (line.startsWith('## ')) {
    return (
      <h2 className="text-base font-bold text-zinc-100 mt-5 mb-2 pb-1.5 border-b border-zinc-800/60">
        {line.replace('## ', '')}
      </h2>
    );
  }

  // Table header
  if (line.startsWith('| #') || line.startsWith('| -')) {
    if (line.startsWith('| -') || line.match(/^\|[\s-|]+$/)) {
      return null; // skip separator
    }
    const cells = line.split('|').filter(Boolean).map((c) => c.trim());
    return (
      <div className="grid gap-px bg-zinc-800/50 rounded-t-lg overflow-hidden mt-1"
        style={{ gridTemplateColumns: `repeat(${cells.length}, minmax(0, 1fr))` }}
      >
        {cells.map((cell, i) => (
          <div key={i} className="text-[10px] font-semibold text-zinc-400 uppercase tracking-wider px-2 py-1.5 bg-zinc-900">
            {cell}
          </div>
        ))}
      </div>
    );
  }

  // Table data row
  if (line.startsWith('| ') && line.includes('|')) {
    const cells = line.split('|').filter(Boolean).map((c) => c.trim());
    if (cells.length < 2) return <p className="text-xs text-zinc-400">{line}</p>;

    return (
      <div className="grid gap-px bg-zinc-800/30"
        style={{ gridTemplateColumns: `repeat(${cells.length}, minmax(0, 1fr))` }}
      >
        {cells.map((cell, i) => {
          // Color logic for Change%
          let color = 'text-zinc-300';
          if (cell.includes('%')) {
            const num = parseFloat(cell);
            if (!isNaN(num)) {
              color = num >= 0 ? 'text-emerald-400' : 'text-red-400';
            }
          }
          // Bold symbol column (usually column 1)
          if (i === 1 && cell.match(/^[A-Z]+/)) {
            color = 'text-zinc-100 font-semibold';
          }
          return (
            <div key={i} className={`text-xs font-mono px-2 py-1.5 bg-zinc-900/60 ${color}`}>
              {cell}
            </div>
          );
        })}
      </div>
    );
  }

  // Bullet list
  if (line.startsWith('- ')) {
    const text = line.slice(2);
    // Color market bias
    if (text.includes('BEARISH')) {
      return <div className="text-xs text-red-400 pl-3 py-0.5 border-l-2 border-red-500/30">📍 {text}</div>;
    }
    if (text.includes('BULLISH')) {
      return <div className="text-xs text-emerald-400 pl-3 py-0.5 border-l-2 border-emerald-500/30">📍 {text}</div>;
    }
    if (text.includes('NEUTRAL')) {
      return <div className="text-xs text-amber-400 pl-3 py-0.5 border-l-2 border-amber-500/30">📍 {text}</div>;
    }
    return <div className="text-xs text-zinc-300 pl-3 py-0.5 border-l-2 border-zinc-700">• {text}</div>;
  }

  // Parenthetical note (muted)
  if (line.startsWith('(') && line.endsWith(')')) {
    return <p className="text-[10px] text-zinc-600 italic">{line}</p>;
  }

  // Blank line
  if (line.trim() === '') return <div className="h-1" />;

  // Default paragraph
  return <p className="text-xs text-zinc-400">{line}</p>;
}

/* ── Loading Skeleton ──────────────────────────────────────────── */

function LoadingSkeleton() {
  return (
    <div className="space-y-4 animate-pulse">
      <div className="flex items-center gap-3 mb-6">
        <div className="w-8 h-8 rounded-lg bg-indigo-600/10" />
        <div>
          <div className="h-3 w-48 bg-zinc-800 rounded" />
          <div className="h-2 w-32 bg-zinc-800/60 rounded mt-2" />
        </div>
      </div>

      {/* Fake sections */}
      {[1, 2, 3].map((s) => (
        <div key={s} className="space-y-2">
          <div className="h-4 w-64 bg-zinc-800 rounded" />
          <div className="space-y-1">
            {[1, 2, 3].map((r) => (
              <div key={r} className="h-6 bg-zinc-800/40 rounded" />
            ))}
          </div>
        </div>
      ))}

      <div className="space-y-2 mt-6">
        <div className="h-4 w-32 bg-zinc-800 rounded" />
        <div className="h-3 w-full bg-zinc-800/40 rounded" />
        <div className="h-3 w-3/4 bg-zinc-800/40 rounded" />
        <div className="h-3 w-5/6 bg-zinc-800/40 rounded" />
      </div>

      <div className="flex items-center justify-center gap-2 mt-8 text-zinc-600 text-xs">
        <svg className="w-4 h-4 animate-spin" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
          <path strokeLinecap="round" strokeLinejoin="round"
            d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"
          />
        </svg>
        AI is analyzing market data…
      </div>
    </div>
  );
}
