"""
fng.py — Fear & Greed Index fetcher.

Uses the Alternative.me Fear & Greed API (free, no key required).
Returns current crypto market sentiment as a contrarian signal:

  0–25  (Extreme Fear)  → market oversold → contrarian BULL signal
  26–45 (Fear)          → mild bullish lean → neutral (not strong enough alone)
  46–54 (Neutral)       → no directional signal
  55–74 (Greed)         → mild bearish lean → neutral (not strong enough alone)
  75–100 (Extreme Greed) → market overbought → contrarian BEAR signal

Updated once daily by Alternative.me. Cached 1 hour.
"""

import logging
import requests
from datetime import datetime, timezone

logger = logging.getLogger("fng")

CACHE_TTL_SECS = 3600  # 1-hour TTL — updated daily, no need to refresh more often
_cache: dict = {}


def get_fng() -> dict | None:
    """
    Fetch the current Fear & Greed Index.

    Returns:
        {
          "value":          int,   # 0–100
          "classification": str,   # "Extreme Fear" | "Fear" | "Neutral" | "Greed" | "Extreme Greed"
          "bias":           str,   # "bull" | "bear" | "neutral"
          "fetched_at":     str,
        }
    Returns None on any error.
    """
    now_ts = datetime.now(timezone.utc).timestamp()
    if "fng" in _cache and now_ts - _cache["fng"]["_ts"] < CACHE_TTL_SECS:
        return _cache["fng"]

    try:
        resp = requests.get(
            "https://api.alternative.me/fng/?limit=1",
            timeout=10,
        )
        if resp.status_code != 200:
            logger.warning(f"F&G API HTTP {resp.status_code}")
            return None

        data = resp.json().get("data", [])
        if not data:
            return None

        entry = data[0]
        value = int(entry.get("value", 50))
        classification = entry.get("value_classification", "Neutral")
        bias = _to_bias(value)

        result = {
            "value":          value,
            "classification": classification,
            "bias":           bias,
            "fetched_at":     datetime.now(timezone.utc).isoformat(),
            "_ts":            now_ts,
        }
        _cache["fng"] = result
        logger.info(f"F&G Index: {value} ({classification}) → {bias}")
        return result

    except Exception as e:
        logger.warning(f"F&G fetch error: {e}")
        return None


def _to_bias(value: int) -> str:
    """Convert F&G value to contrarian directional bias."""
    if value <= 25:
        return "bull"   # Extreme fear → contrarian long
    if value >= 75:
        return "bear"   # Extreme greed → contrarian short
    return "neutral"
