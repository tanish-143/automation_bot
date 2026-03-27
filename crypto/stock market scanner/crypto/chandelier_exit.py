"""
Chandelier Exit — 15-minute candle indicator
=============================================

Computes the Chandelier Exit (CE) from 15m OHLC candles.

Parameters (matching TradingView defaults):
  ATR length  : 22
  Multiplier  : 3.0
  useClose    : True  (ATR based on close-to-close, not high-low)

Outputs per symbol:
  ce_dir         : 1 = long (price above long stop), -1 = short
  ce_buySignal   : True on the bar where direction flips to long
  ce_sellSignal  : True on the bar where direction flips to short
  longStop       : trailing stop for longs  (close - ATR * mult)
  shortStop      : trailing stop for shorts (close + ATR * mult)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

import numpy as np

logger = logging.getLogger(__name__)


@dataclass
class ChandelierResult:
    """Output of the Chandelier Exit calculation for a single symbol."""
    symbol: str
    ce_dir: int                   # 1 = long, -1 = short
    ce_buySignal: bool
    ce_sellSignal: bool
    longStop: float
    shortStop: float
    atr: float                    # latest ATR value


def _atr_from_closes(closes: np.ndarray, length: int) -> np.ndarray:
    """
    Compute ATR using close-to-close true range (useClose=True variant).

    True range when useClose=True:
        TR(i) = max(high(i), close(i-1)) - min(low(i), close(i-1))

    Since we only have close prices (CoinGecko doesn't give 15m OHLC easily),
    we approximate TR as |close(i) - close(i-1)|, then use RMA (Wilder's EMA).
    """
    if len(closes) < 2:
        return np.full(len(closes), 0.0)

    tr = np.abs(np.diff(closes))
    tr = np.insert(tr, 0, 0.0)  # first bar has no previous close

    # RMA (Wilder's smoothed moving average) = EMA with alpha = 1/length
    alpha = 1.0 / length
    atr = np.zeros(len(tr))
    atr[0] = tr[0]
    for i in range(1, len(tr)):
        atr[i] = alpha * tr[i] + (1 - alpha) * atr[i - 1]
    return atr


def _atr_from_ohlc(
    highs: np.ndarray,
    lows: np.ndarray,
    closes: np.ndarray,
    length: int,
) -> np.ndarray:
    """
    Standard ATR from OHLC using Wilder's smoothing.

    TR(i) = max(high(i) - low(i),
                |high(i) - close(i-1)|,
                |low(i)  - close(i-1)|)
    """
    n = len(closes)
    if n < 2:
        return np.full(n, 0.0)

    prev_close = np.roll(closes, 1)
    prev_close[0] = closes[0]

    tr = np.maximum(
        highs - lows,
        np.maximum(
            np.abs(highs - prev_close),
            np.abs(lows - prev_close),
        ),
    )

    alpha = 1.0 / length
    atr = np.zeros(n)
    atr[0] = tr[0]
    for i in range(1, n):
        atr[i] = alpha * tr[i] + (1 - alpha) * atr[i - 1]
    return atr


def compute_chandelier_exit(
    closes: np.ndarray,
    highs: Optional[np.ndarray] = None,
    lows: Optional[np.ndarray] = None,
    atr_length: int = 22,
    multiplier: float = 3.0,
    symbol: str = "",
) -> Optional[ChandelierResult]:
    """
    Compute Chandelier Exit from price arrays (oldest → newest).

    If highs/lows are provided, uses standard OHLC ATR.
    Otherwise falls back to close-to-close approximation.

    Requires at least `atr_length + 1` bars.
    """
    n = len(closes)
    if n < atr_length + 1:
        logger.debug("CE: not enough bars for %s (%d < %d)", symbol, n, atr_length + 1)
        return None

    # ATR
    if highs is not None and lows is not None:
        atr = _atr_from_ohlc(highs, lows, closes, atr_length)
    else:
        atr = _atr_from_closes(closes, atr_length)

    # Long / short stop arrays
    long_stop = np.zeros(n)
    short_stop = np.zeros(n)

    # Direction: 1 = long, -1 = short
    direction = np.ones(n, dtype=int)

    for i in range(atr_length, n):
        ls = closes[i] - atr[i] * multiplier
        ss = closes[i] + atr[i] * multiplier

        # Trail long stop: only move up
        if i > atr_length:
            if direction[i - 1] == 1:
                long_stop[i] = max(ls, long_stop[i - 1])
            else:
                long_stop[i] = ls
            # Trail short stop: only move down
            if direction[i - 1] == -1:
                short_stop[i] = min(ss, short_stop[i - 1])
            else:
                short_stop[i] = ss
        else:
            long_stop[i] = ls
            short_stop[i] = ss

        # Direction flip
        if closes[i] > short_stop[i - 1] if i > atr_length else False:
            direction[i] = 1
        elif closes[i] < long_stop[i - 1] if i > atr_length else False:
            direction[i] = -1
        else:
            direction[i] = direction[i - 1] if i > 0 else 1

    last = n - 1
    prev = last - 1

    ce_dir = int(direction[last])
    ce_buySignal = direction[last] == 1 and direction[prev] == -1
    ce_sellSignal = direction[last] == -1 and direction[prev] == 1

    return ChandelierResult(
        symbol=symbol,
        ce_dir=ce_dir,
        ce_buySignal=ce_buySignal,
        ce_sellSignal=ce_sellSignal,
        longStop=float(long_stop[last]),
        shortStop=float(short_stop[last]),
        atr=float(atr[last]),
    )


def compute_ce_for_symbols(
    candle_data: dict[str, dict],
    atr_length: int = 22,
    multiplier: float = 3.0,
) -> dict[str, ChandelierResult]:
    """
    Batch-compute Chandelier Exit for multiple symbols.

    Parameters
    ----------
    candle_data : dict
        {symbol: {"closes": [...], "highs": [...] (opt), "lows": [...] (opt)}}
    atr_length : int
        ATR lookback period (default 22).
    multiplier : float
        ATR multiplier (default 3.0).

    Returns
    -------
    dict mapping symbol → ChandelierResult (only symbols with enough data).
    """
    results: dict[str, ChandelierResult] = {}
    for symbol, data in candle_data.items():
        closes = np.array(data["closes"], dtype=np.float64)
        highs = np.array(data["highs"], dtype=np.float64) if "highs" in data else None
        lows = np.array(data["lows"], dtype=np.float64) if "lows" in data else None

        ce = compute_chandelier_exit(
            closes, highs, lows,
            atr_length=atr_length,
            multiplier=multiplier,
            symbol=symbol,
        )
        if ce is not None:
            results[symbol] = ce

    return results
