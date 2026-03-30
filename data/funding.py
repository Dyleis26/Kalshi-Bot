"""
funding.py — Perpetual futures funding rate fetcher (Bybit primary, OKX fallback).

Binance blocks certain VPS IPs with HTTP 451. Bybit and OKX both offer the same
funding rate data on public endpoints with no authentication required.

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

BYBIT_URL = "https://api.bybit.com/v5/market/tickers"
OKX_URL   = "https://www.okx.com/api/v5/public/funding-rate"

# Symbol mapping: internal (Binance-style) → exchange-specific
_BYBIT_SYMBOL = {"BTCUSDT": "BTCUSDT", "ETHUSDT": "ETHUSDT"}
_OKX_SYMBOL   = {"BTCUSDT": "BTC-USD-SWAP", "ETHUSDT": "ETH-USD-SWAP"}


def _fetch_bybit(symbol: str) -> float | None:
    """Fetch funding rate from Bybit V5. Returns float or None on error."""
    bybit_sym = _BYBIT_SYMBOL.get(symbol, symbol)
    try:
        resp = requests.get(
            BYBIT_URL,
            params={"category": "linear", "symbol": bybit_sym},
            timeout=10,
        )
        if resp.status_code != 200:
            logger.warning(f"Bybit funding rate HTTP {resp.status_code}")
            return None
        data = resp.json()
        items = data.get("result", {}).get("list", [])
        if not items:
            return None
        return float(items[0].get("fundingRate", 0))
    except Exception as e:
        logger.warning(f"Bybit funding rate error ({symbol}): {e}")
        return None


def _fetch_okx(symbol: str) -> float | None:
    """Fetch funding rate from OKX. Returns float or None on error."""
    okx_sym = _OKX_SYMBOL.get(symbol, symbol)
    try:
        resp = requests.get(
            OKX_URL,
            params={"instId": okx_sym},
            timeout=10,
        )
        if resp.status_code != 200:
            logger.warning(f"OKX funding rate HTTP {resp.status_code}")
            return None
        data = resp.json()
        items = data.get("data", [])
        if not items:
            return None
        return float(items[0].get("fundingRate", 0))
    except Exception as e:
        logger.warning(f"OKX funding rate error ({symbol}): {e}")
        return None


def get_funding_rate(symbol: str = "BTCUSDT") -> dict | None:
    """
    Fetch the current perpetual funding rate for the given symbol.
    Tries Bybit first, falls back to OKX if Bybit is unavailable.

    Returns:
        {
          "symbol":       str,   # e.g. "BTCUSDT"
          "funding_rate": float, # positive → longs paying (market net long)
          "source":       str,   # "bybit" or "okx"
          "fetched_at":   str,
        }
    Returns None on any error.
    """
    now_ts = datetime.now(timezone.utc).timestamp()
    if symbol in _cache and now_ts - _cache[symbol]["_ts"] < CACHE_TTL_SECS:
        return _cache[symbol]

    rate = _fetch_bybit(symbol)
    source = "bybit"

    if rate is None:
        rate = _fetch_okx(symbol)
        source = "okx"

    if rate is None:
        return None

    result = {
        "symbol":       symbol,
        "funding_rate": round(rate, 8),
        "source":       source,
        "fetched_at":   datetime.now(timezone.utc).isoformat(),
        "_ts":          now_ts,
    }
    _cache[symbol] = result
    logger.debug(f"Funding rate {symbol}: {rate:+.6f} (source={source})")
    return result


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
