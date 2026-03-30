"""
equity.py — US equity futures trend fetcher (Yahoo Finance, no auth required).

Uses S&P 500 futures (ES=F) as a macro regime signal for BTC directional bias.
Nasdaq 100 futures (NQ=F) used as fallback if ES=F is unavailable.

Equity futures trade ~23h/day (Sun 6pm → Fri 5pm ET) and are a direct
(non-contrarian) leading indicator of risk-on/risk-off sentiment:

  ES trending UP   → risk-on → BTC bull tailwind   → add "bull" vote
  ES trending DOWN → risk-off → BTC bear headwind  → add "bear" vote
  Flat / no data   → neutral → no extra vote

Trend is computed as the % price change over the last LOOKBACK_BARS × 5-minute
bars (default: 3 bars = 15 minutes). This matches the BTC 15-minute window.

Typical thresholds:
  ±0.15% over 15 min — meaningful intraday move for equity futures
  ±0.25% over 30 min — use EQUITY_LOOKBACK_BARS=6 for 30-minute window

Cached 5 minutes.
"""

import logging
import requests
from datetime import datetime, timezone

logger = logging.getLogger("equity")

CACHE_TTL_SECS = 300  # 5-minute TTL — refreshes each BTC window
_cache: dict = {}

_YAHOO_URL = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
_HEADERS   = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}
_SYMBOLS = ["ES=F", "NQ=F"]   # S&P 500 futures primary, Nasdaq fallback


def _fetch_change(symbol: str, lookback_bars: int) -> float | None:
    """
    Fetch 5-minute bars for symbol and return % change over the last
    lookback_bars bars (e.g. 3 bars = 15 min, 6 bars = 30 min).
    Returns None on any error or insufficient data.
    """
    try:
        resp = requests.get(
            _YAHOO_URL.format(symbol=symbol),
            params={"interval": "5m", "range": "1d"},
            headers=_HEADERS,
            timeout=10,
        )
        if resp.status_code != 200:
            logger.warning(f"Yahoo Finance HTTP {resp.status_code} for {symbol}")
            return None
        data   = resp.json()
        result = data.get("chart", {}).get("result", [])
        if not result:
            return None
        closes = result[0].get("indicators", {}).get("quote", [{}])[0].get("close", [])
        closes = [c for c in closes if c is not None]
        if len(closes) < lookback_bars + 1:
            logger.warning(f"Equity {symbol}: not enough bars ({len(closes)}) for {lookback_bars}-bar lookback")
            return None
        then  = closes[-(lookback_bars + 1)]
        now   = closes[-1]
        return round((now - then) / then, 6)
    except Exception as e:
        logger.warning(f"Equity fetch error ({symbol}): {e}")
        return None


def get_equity_trend(threshold: float = 0.0015, lookback_bars: int = 3) -> dict | None:
    """
    Fetch US equity futures 15-minute trend and return a directional bias.

    Args:
        threshold:     minimum % move to count as directional (default 0.15%)
        lookback_bars: number of 5-min bars to look back (default 3 = 15 min)

    Returns:
        {
          "symbol":       str,   # "ES=F" or "NQ=F"
          "change_pct":   float, # % change over lookback window (e.g. 0.0018 = +0.18%)
          "lookback_min": int,   # lookback_bars × 5
          "bias":         str,   # "bull" | "bear" | "neutral"
          "fetched_at":   str,
        }
    Returns None if both sources fail.
    """
    now_ts = datetime.now(timezone.utc).timestamp()
    if "equity" in _cache and now_ts - _cache["equity"]["_ts"] < CACHE_TTL_SECS:
        return _cache["equity"]

    change = None
    used_symbol = None
    for symbol in _SYMBOLS:
        change = _fetch_change(symbol, lookback_bars)
        if change is not None:
            used_symbol = symbol
            break

    if change is None:
        return None

    if change >= threshold:
        bias = "bull"
    elif change <= -threshold:
        bias = "bear"
    else:
        bias = "neutral"

    lookback_min = lookback_bars * 5
    result = {
        "symbol":       used_symbol,
        "change_pct":   change,
        "lookback_min": lookback_min,
        "bias":         bias,
        "fetched_at":   datetime.now(timezone.utc).isoformat(),
        "_ts":          now_ts,
    }
    _cache["equity"] = result
    logger.info(
        f"Equity trend {used_symbol}: {change:+.4%} over {lookback_min}min → {bias}"
    )
    return result
