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
from data.weather import get_forecast, get_open_meteo, high_temp_probability
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

        nws = get_forecast(lat, lng, city)
        om  = get_open_meteo(lat, lng, city)   # Open-Meteo ECMWF (second source)

        if not nws and not om:
            return _no_trade("both NWS and Open-Meteo unavailable", yes_ask)

        # Determine what type of market this is and compute probability from each source
        title_lower = title.lower()
        nws_prob = om_prob = None

        if "rain" in title_lower or "precip" in title_lower or "snow" in title_lower or "inch" in title_lower:
            if nws:
                nws_prob = nws["precip_pct"]
            if om:
                om_prob = om["precip_pct"]

        elif "high" in title_lower or "temp" in title_lower or "degree" in title_lower or "°" in title_lower:
            direction_char, threshold = _parse_temp_threshold(title)
            if threshold is None:
                return _no_trade("could not parse temperature threshold from market title", yes_ask)
            if direction_char is None:
                return _no_trade("band/range market — skipping (only trade over/under markets)", yes_ask)

            if nws and nws.get("high_temp_f") is not None:
                raw = high_temp_probability(nws["high_temp_f"], threshold)
                nws_prob = raw if direction_char == ">" else (1.0 - raw)
            if om and om.get("high_temp_f") is not None:
                raw = high_temp_probability(om["high_temp_f"], threshold)
                om_prob = raw if direction_char == ">" else (1.0 - raw)

        if nws_prob is None and om_prob is None:
            return _no_trade("unrecognised weather market type", yes_ask)

        # Average both sources when available; fall back to whichever one has data
        if nws_prob is not None and om_prob is not None:
            external_prob = (nws_prob + om_prob) / 2.0
            nws_high = nws.get("high_temp_f") or 0
            om_high  = om.get("high_temp_f")  or 0
            if "precip" in title_lower or "rain" in title_lower:
                metric = f"precip NWS={nws_prob*100:.0f}% OM={om_prob*100:.0f}% avg={external_prob*100:.0f}%"
            else:
                metric = (
                    f"NWS={nws_high:.0f}°F→p={nws_prob:.2f} "
                    f"OM={om_high:.0f}°F→p={om_prob:.2f} avg={external_prob:.2f}"
                )
        elif nws_prob is not None:
            external_prob = nws_prob
            nws_high = nws.get("high_temp_f") or 0
            metric = f"NWS={nws_high:.0f}°F→p={nws_prob:.2f}"
        else:
            external_prob = om_prob
            om_high = om.get("high_temp_f") or 0
            metric = f"OM={om_high:.0f}°F→p={om_prob:.2f}"

        edge = external_prob - yes_ask
        market_label = _build_label(title, city)

        if edge >= WEATHER_EDGE_MIN:
            direction = LONG
            reason = f"Weather edge={edge:+.2f} ({metric}) — buying YES at {yes_ask:.2f}"
        elif edge <= -WEATHER_EDGE_MIN:
            direction = SHORT
            reason = f"Weather edge={edge:+.2f} ({metric}) — buying NO at {1-yes_ask:.2f}"
        else:
            direction = NONE
            reason = f"edge too small: {edge:+.2f} < ±{WEATHER_EDGE_MIN} ({metric})"

        logger.info(f"Weather [{city}]: {reason}")

        return {
            "direction":      direction,
            "confidence":     round(abs(edge), 4),
            "confidence_pct": round(min(abs(edge) / 0.5 * 100, 100.0), 1),
            "external_prob":  round(external_prob, 4),
            "kalshi_yes":     round(yes_ask, 4),
            "edge":           round(edge, 4),
            "reason":         reason,
            "market_label":   market_label,
            # Crypto-compatible fields (empty) for trade log
            "rsi": 0, "macd": 0, "momentum": 0, "vwap": 0, "price": 0,
            "rsi_bias": None, "macd_bias": None, "momentum_bias": None, "vwap_bias": None,
        }


# ---------------------------------------------------------------------- #
#  Helpers                                                                 #
# ---------------------------------------------------------------------- #

def _parse_temp_threshold(title: str) -> tuple:
    """
    Extract direction and temperature threshold from a market title.

    Returns (direction_char, threshold_f) where direction_char is '>' or '<'.
    Returns (None, threshold_f) for band/range markets (e.g. "68-69°").
    Returns (None, None) if no temperature found.

    Examples:
      "be >69° on Mar 30" → ('>', 69.0)
      "be <62° on Mar 30" → ('<', 62.0)
      "be 68-69° on Mar 30" → (None, 68.0)   ← band market, skip
    """
    # Over/under format: ">69°" or "<62°"
    match = re.search(r"([><])\s*(\d+(?:\.\d+)?)\s*°", title)
    if match:
        return (match.group(1), float(match.group(2)))

    # Degrees with F suffix: "69°F" or "69 degrees"
    match2 = re.search(r"(\d+(?:\.\d+)?)\s*°?\s*[fF]", title)
    if match2:
        # No direction found — check context
        if "above" in title.lower() or "high" in title.lower() or "over" in title.lower():
            return ('>', float(match2.group(1)))
        if "below" in title.lower() or "low" in title.lower() or "under" in title.lower():
            return ('<', float(match2.group(1)))
        return (None, float(match2.group(1)))

    # Bare degree sign (no F): "69°" without > or < → likely a band market
    match3 = re.search(r"(\d+(?:\.\d+)?)\s*°", title)
    if match3:
        return (None, float(match3.group(1)))

    return (None, None)


def _build_label(title: str, city: str) -> str:
    """Build a compact label like 'NYC >69' from the market title."""
    # Extract city abbreviation — look for 2-4 uppercase letters in title (e.g. 'NYC')
    city_match = re.search(r'\b([A-Z]{2,4})\b', title)
    city_abbr = city_match.group(1) if city_match else city.split()[0]

    # Extract direction+threshold: ">69" or "<62"
    thresh_match = re.search(r'([><])\s*(\d+(?:\.\d+)?)\s*°', title)
    if thresh_match:
        threshold = f"{thresh_match.group(1)}{int(float(thresh_match.group(2)))}"
        return f"{city_abbr} {threshold}"

    return f"{city_abbr} temp"


def _no_trade(reason: str, yes_ask: float) -> dict:
    return {
        "direction":      NONE,
        "confidence":     0.0,
        "confidence_pct": 0.0,
        "external_prob":  0.0,
        "kalshi_yes":     round(yes_ask, 4),
        "edge":           0.0,
        "reason":         reason,
        "market_label":   "Weather",
        "rsi": 0, "macd": 0, "momentum": 0, "vwap": 0, "price": 0,
        "rsi_bias": None, "macd_bias": None, "momentum_bias": None, "vwap_bias": None,
    }
