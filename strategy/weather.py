"""
weather.py — Kalshi weather market signal strategy.

Compares NWS probability estimates to the current Kalshi YES price.
If there's meaningful edge (external probability diverges from Kalshi price by
at least WEATHER_EDGE_MIN), returns a direction to trade.

Decision logic:
  - BUY YES  (LONG):  NWS probability >> Kalshi YES price
  - BUY NO  (SHORT):  NWS probability << Kalshi YES price
  - NO TRADE:         Edge is too small to overcome fees

Kalshi weather markets ask things like:
  "Will NYC high temperature be above 55°F on March 29?"
  "Will it rain in Chicago on April 1?"

We parse the market title to extract the type (temperature / precipitation)
and threshold, then compute a probability from the NWS forecast.
"""

import re
import logging
from data.weather import get_forecast, high_temp_probability
from administration.config import (
    CONTRACT_PRICE_MIN, CONTRACT_PRICE_MAX, WEATHER_EDGE_MIN,
)

logger = logging.getLogger("strategy.weather")

LONG  = "long"
SHORT = "short"
NONE  = "none"


class WeatherStrategy:

    def decide(self, market: dict, lat: float, lng: float, city: str = "") -> dict:
        """
        Evaluate a Kalshi weather market and return a trade decision.

        Args:
            market:  Kalshi market dict (from get_market / get_markets_by_series)
            lat:     Latitude of the forecast city
            lng:     Longitude of the forecast city
            city:    Display name (for logging)

        Returns:
            {
                "direction":      "long" | "short" | "none",
                "confidence":     float (edge magnitude, 0.0–1.0),
                "external_prob":  float (NWS-derived probability of YES, 0.0–1.0),
                "kalshi_yes":     float (current Kalshi YES ask price),
                "edge":           float (external_prob - kalshi_yes),
                "reason":         str,
                "market_label":   str (formatted for Discord),
            }
        """
        title = market.get("title", market.get("subtitle", ""))
        yes_ask = float(market.get("yes_ask_dollars", 0.5))

        # Near-fair filter — same as crypto
        if not (CONTRACT_PRICE_MIN <= yes_ask <= CONTRACT_PRICE_MAX):
            return _no_trade(f"market too confident (YES={yes_ask:.2f})", yes_ask)

        forecast = get_forecast(lat, lng, city)
        if not forecast:
            return _no_trade("NWS forecast unavailable", yes_ask)

        # Determine what type of market this is and compute probability
        title_lower = title.lower()
        external_prob = None

        if "rain" in title_lower or "precip" in title_lower or "snow" in title_lower or "inch" in title_lower:
            external_prob = forecast["precip_pct"]
            metric = f"precip={external_prob*100:.0f}%"

        elif "high" in title_lower or "temp" in title_lower or "degree" in title_lower or "°" in title_lower:
            threshold = _parse_temp_threshold(title)
            if threshold is not None and forecast["high_temp_f"] is not None:
                external_prob = high_temp_probability(forecast["high_temp_f"], threshold)
                metric = f"NWS_high={forecast['high_temp_f']:.0f}°F vs threshold={threshold:.0f}°F → p={external_prob:.2f}"
            else:
                return _no_trade("could not parse temperature threshold from market title", yes_ask)

        if external_prob is None:
            return _no_trade("unrecognised weather market type", yes_ask)

        edge = external_prob - yes_ask
        market_label = _build_label(title, city)

        if edge >= WEATHER_EDGE_MIN:
            direction = LONG
            reason = f"NWS edge={edge:+.2f} ({metric}) — buying YES at {yes_ask:.2f}"
        elif edge <= -WEATHER_EDGE_MIN:
            direction = SHORT
            reason = f"NWS edge={edge:+.2f} ({metric}) — buying NO at {1-yes_ask:.2f}"
        else:
            direction = NONE
            reason = f"edge too small: {edge:+.2f} < ±{WEATHER_EDGE_MIN} ({metric})"

        logger.info(f"Weather [{city}]: {reason}")

        return {
            "direction":     direction,
            "confidence":    round(abs(edge), 4),
            "external_prob": round(external_prob, 4),
            "kalshi_yes":    round(yes_ask, 4),
            "edge":          round(edge, 4),
            "reason":        reason,
            "market_label":  market_label,
            # Crypto-compatible fields (empty) for trade log
            "rsi": 0, "macd": 0, "momentum": 0, "vwap": 0, "price": 0,
            "rsi_bias": None, "macd_bias": None, "momentum_bias": None, "vwap_bias": None,
        }


# ---------------------------------------------------------------------- #
#  Helpers                                                                 #
# ---------------------------------------------------------------------- #

def _parse_temp_threshold(title: str) -> float | None:
    """Extract a temperature threshold from a market title like 'above 55°F'."""
    match = re.search(r"(\d+(?:\.\d+)?)\s*°?\s*[fF]", title)
    if match:
        return float(match.group(1))
    match2 = re.search(r"(\d+(?:\.\d+)?)\s*degrees?", title, re.IGNORECASE)
    if match2:
        return float(match2.group(1))
    return None


def _build_label(title: str, city: str) -> str:
    """Build a short Discord-friendly label from the market title."""
    # Strip common boilerplate
    cleaned = re.sub(r"\?$", "", title).strip()
    # Truncate if too long
    if len(cleaned) > 50:
        cleaned = cleaned[:47] + "..."
    return f"Weather: {cleaned}"


def _no_trade(reason: str, yes_ask: float) -> dict:
    return {
        "direction":     NONE,
        "confidence":    0.0,
        "external_prob": 0.0,
        "kalshi_yes":    round(yes_ask, 4),
        "edge":          0.0,
        "reason":        reason,
        "market_label":  "Weather",
        "rsi": 0, "macd": 0, "momentum": 0, "vwap": 0, "price": 0,
        "rsi_bias": None, "macd_bias": None, "momentum_bias": None, "vwap_bias": None,
    }
