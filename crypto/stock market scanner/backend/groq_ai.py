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

**Output format — return ONLY valid JSON, no markdown fences, no extra text:**

{
  "long_setups": [
    {
      "symbol": "BTC/USDT",
      "limit_entry": 67000.00,
      "stop_loss": 65500.00,
      "take_profit": 70000.00,
      "signal": "combined",
      "volume_ratio": 4.2,
      "change_pct": 5.1,
      "confidence": "high",
      "note": "Strong volume + breakout above resistance"
    }
  ],
  "short_setups": [
    {
      "symbol": "ETH/USDT",
      "limit_entry": 3400.00,
      "stop_loss": 3550.00,
      "take_profit": 3100.00,
      "signal": "combined",
      "volume_ratio": 3.8,
      "change_pct": -6.2,
      "confidence": "high",
      "note": "Heavy selling volume + broke support"
    }
  ],
  "volume_only": [
    {
      "symbol": "SOL/USDT",
      "price": 150.00,
      "volume_ratio": 6.5,
      "change_pct": 1.2,
      "note": "Watch for breakout direction"
    }
  ],
  "extreme_movers": ["DOGE/USDT"],
  "summary": {
    "market_bias": "BEARISH",
    "best_long": "BTC — volume breakout above 67k with 4x vol",
    "best_short": "ETH — broke 3400 support on 3.8x volume",
    "key_risk": "High correlation across majors — one reversal moves everything"
  }
}

**Rules for setups:**
- long_setups: Max 5, only combined signals with Change% > 3% and VolRatio > 2x.
  limit_entry slightly below current price (buy the dip). SL below recent support. TP at 2:1+ R:R.
- short_setups: Max 8, only combined signals with Change% < -3% and VolRatio > 2x.
  limit_entry slightly above current price (sell the rally). SL above recent resistance. TP at 2:1+ R:R.
- volume_only: Max 5, VolRatio > 5x but |Change%| < 3%.
- extreme_movers: Symbols with |Change%| > 15%.
- Use the volume_ratio from the data — do NOT use 0 or default values.
- If Chandelier Exit data is provided, incorporate it: prefer longs where ce_dir=1, shorts where ce_dir=-1.

**Ignore stablecoins: USDTUSDT, USDCUSDT, USD1USDT**"""


def format_prices_as_csv(prices: list[dict], ce_data: dict | None = None) -> str:
    """Convert live price data into CSV format for the AI prompt.

    Args:
        prices: List of price dicts (must include volume_ratio from DB feed).
        ce_data: Optional dict of {symbol: {ce_dir, longStop, shortStop, ...}}.
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    header = "timestamp,symbol,price,change_pct_24h,volume_24h,volume_ratio,signal"
    if ce_data:
        header += ",ce_dir,longStop,shortStop"
    lines = [header]

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

        line = f"{now},{symbol},{price:.4f},{change:.2f},{volume:.0f},{vol_ratio:.2f},{signal}"

        if ce_data:
            sym_key = p.get("symbol", "")
            ce = ce_data.get(sym_key)
            if ce:
                line += f",{ce.get('ce_dir', 0)},{ce.get('longStop', 0):.4f},{ce.get('shortStop', 0):.4f}"
            else:
                line += ",0,0,0"

        lines.append(line)

    return "\n".join(lines)


async def analyze_trade_setup(prices: list[dict], ce_data: dict | None = None) -> str:
    """
    Send scanner data to Groq AI and return structured trade analysis.

    Args:
        prices: List of live price dicts (must include volume_ratio from DB).
        ce_data: Optional Chandelier Exit dict {symbol: {ce_dir, longStop, ...}}.

    Returns:
        JSON string with long_setups, short_setups (limit entry, SL, TP).
    """
    if not settings.groq_api_key:
        return "⚠️ Groq API key not configured. Set `SCANNER_GROQ_API_KEY` in .env"

    csv_data = format_prices_as_csv(prices, ce_data=ce_data)

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
