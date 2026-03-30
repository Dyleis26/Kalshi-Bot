"""
funding.py — Binance perpetual futures funding rate fetcher.

Uses the Binance USDT perpetuals API (public endpoint, no key required).
Returns the current 8-hour funding rate as a contrarian signal:

  rate > +threshold: market is net long (longs pay shorts)
    → overcrowded positioning → contrarian SHORT signal

  rate < -threshold: market is net short (shorts pay longs)
    → overcrowded positioning → contrarian LONG signal

  |rate| < threshold: balanced positioning → neutral

Funding rates cycle every 8 hours. Positive rate = longs paying = market
is leaning long. When a position becomes too crowded, mean-reversion is likely.

Typical thresholds:
  Neutral zone:  |rate| < 0.0001  (0.01% per 8h = ~11% annualized)
  Bear signal:   rate > 0.0003    (0.03% per 8h = ~33% annualized — very crowded long)
  Bull signal:   rate < -0.0001   (any net short position is unusual → contrarian long)

Cached 5 minutes.
"""

import logging
import requests
from datetime import datetime, timezone

logger = logging.getLogger("funding")

CACHE_TTL_SECS = 300   # 5-minute TTL — funding rate is stable between cycles
_cache: dict = {}

BINANCE_URL = "https://fapi.binance.com/fapi/v1/premiumIndex"


def get_funding_rate(symbol: str = "BTCUSDT") -> dict | None:
    """
    Fetch the current perpetual funding rate for the given symbol.

    Returns:
        {
          "symbol":       str,   # e.g. "BTCUSDT"
          "funding_rate": float, # positive → longs paying (market net long)
          "mark_price":   float,
          "fetched_at":   str,
        }
    Returns None on any error.
    """
    now_ts = datetime.now(timezone.utc).timestamp()
    if symbol in _cache and now_ts - _cache[symbol]["_ts"] < CACHE_TTL_SECS:
        return _cache[symbol]

    try:
        resp = requests.get(BINANCE_URL, params={"symbol": symbol}, timeout=10)
        if resp.status_code != 200:
            logger.warning(f"Binance funding rate HTTP {resp.status_code}")
            return None

        data = resp.json()
        rate = float(data.get("lastFundingRate", 0))
        mark = float(data.get("markPrice", 0))

        result = {
            "symbol":       symbol,
            "funding_rate": round(rate, 8),
            "mark_price":   round(mark, 2),
            "fetched_at":   datetime.now(timezone.utc).isoformat(),
            "_ts":          now_ts,
        }
        _cache[symbol] = result
        logger.debug(f"Funding rate {symbol}: {rate:+.6f} (mark={mark:.2f})")
        return result

    except Exception as e:
        logger.warning(f"Binance funding rate fetch error ({symbol}): {e}")
        return None


def get_funding_bias(rate: float, bull_threshold: float, bear_threshold: float) -> str:
    """
    Convert a funding rate to a contrarian directional bias.

    bull_threshold: rate must be <= this (negative, shorts paying) → bullish
    bear_threshold: rate must be >= this (positive, longs paying) → bearish
    """
    if rate <= bull_threshold:
        return "bull"   # Market net short → contrarian long
    if rate >= bear_threshold:
        return "bear"   # Market net long → contrarian short
    return "neutral"
