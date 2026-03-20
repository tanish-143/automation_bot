"""
Groq AI Trade Setup Analyzer
==============================

Sends live scanner data to Groq (Llama 3.3 70B) and returns
structured trade setup recommendations.

Flow:
  1. Receive live prices (from CoinGecko /coins/markets)
  2. Format as CSV alert data
  3. Send to Groq with the crypto analyst system prompt
  4. Return markdown analysis
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone

from config import settings

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """You are a crypto scanner analyst. I will paste CSV alert data from my trading scanner.

**Rules:**
1. Deduplicate — each symbol should appear only ONCE, using the most recent timestamp
2. Prioritize "combined" signals (price move + volume spike together) over single signals
3. Sort by signal strength: combined > volume_spike > volatility_anomaly
4. Within each category, sort by |Change%| × VolRatio (highest first)

**Output format (always follow this exactly):**

## 🔴 TOP SHORT CANDIDATES (combined signals, price falling + high volume)
| # | Symbol | Price | Change% | VolRatio | Signal | Note |
(Max 8 rows, only combined signals with Change% < -3% and VolRatio > 2x)

## 🟢 TOP LONG CANDIDATES (combined signals OR volatility up + volume)
| # | Symbol | Price | Change% | VolRatio | Signal | Note |
(Max 5 rows, only Change% > 3%)

## 📊 VOLUME-ONLY SPIKES (high volume but small price move — watch for breakout)
| # | Symbol | Price | Change% | VolRatio |
(Max 5 rows, VolRatio > 5x but |Change%| < 3%)

## ⚠️ EXTREME MOVERS (>15% move — avoid or scalp only)
List symbols with |Change%| > 15%

## 🎯 SUMMARY
- Market bias: [BEARISH/BULLISH/NEUTRAL]
- Best short: [symbol + reason in 10 words]
- Best long: [symbol + reason in 10 words]
- Key risk: [one sentence]

**Ignore:** USDTUSDT, USDCUSDT, USD1USDT (stablecoins)"""


def format_prices_as_csv(prices: list[dict]) -> str:
    """Convert live price data into CSV format for the AI prompt."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    lines = ["timestamp,symbol,price,change_pct_24h,volume_24h,volume_ratio,signal"]

    for p in prices:
        symbol = p.get("symbol", "").replace("/", "")
        price = p.get("current_price", 0)
        change = p.get("price_change_pct_24h", 0)
        volume = p.get("volume_24h", 0)
        vol_ratio = p.get("volume_ratio") or 0

        # Determine signal type based on metrics
        abs_change = abs(change) if change else 0
        if abs_change > 3 and vol_ratio > 2:
            signal = "combined"
        elif vol_ratio > 3:
            signal = "volume_spike"
        elif abs_change > 5:
            signal = "volatility_anomaly"
        else:
            signal = "normal"

        lines.append(
            f"{now},{symbol},{price:.4f},{change:.2f},{volume:.0f},{vol_ratio:.2f},{signal}"
        )

    return "\n".join(lines)


async def analyze_trade_setup(prices: list[dict]) -> str:
    """
    Send scanner data to Groq AI and return markdown trade analysis.

    Args:
        prices: List of live price dicts from CoinGecko.

    Returns:
        Markdown string with trade setup recommendations.
    """
    if not settings.groq_api_key:
        return "⚠️ Groq API key not configured. Set `SCANNER_GROQ_API_KEY` in .env"

    csv_data = format_prices_as_csv(prices)

    try:
        from groq import AsyncGroq

        client = AsyncGroq(api_key=settings.groq_api_key)

        response = await client.chat.completions.create(
            model=settings.groq_model,
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": f"Here is my latest scanner data:\n\n```csv\n{csv_data}\n```\n\nAnalyze and provide trade setups."},
            ],
            temperature=0.3,
            max_tokens=2000,
        )

        return response.choices[0].message.content or "No analysis generated."

    except Exception as e:
        logger.exception("Groq AI analysis failed")
        return f"⚠️ AI analysis failed: {e}"
