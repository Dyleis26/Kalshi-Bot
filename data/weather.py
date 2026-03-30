"""
weather.py — Weather forecast fetcher (NWS primary + Open-Meteo secondary).

Primary source:   National Weather Service (weather.gov) — US only, no key.
Secondary source: Open-Meteo (api.open-meteo.com) — global, free, ECMWF model.

Both sources are fetched for cross-validation. When both are available,
strategy/weather.py averages them to reduce single-model forecast error.

NWS flow:
  1. GET https://api.weather.gov/points/{lat},{lng}  → resolves to grid endpoint
  2. GET {forecast_url}                               → daily forecast periods

Open-Meteo flow:
  GET https://api.open-meteo.com/v1/forecast
      ?latitude={lat}&longitude={lng}&daily=temperature_2m_max,precipitation_probability_max
      &temperature_unit=fahrenheit&timezone=auto&forecast_days=1

Cached 10 minutes per source to avoid hammering on every 5-min poll.
"""

import logging
import requests
from datetime import datetime, timezone, date, timedelta
from typing import Optional

logger = logging.getLogger("weather")

# Cache so we don't hammer NWS on every 5-min poll
_cache: dict = {}
CACHE_TTL_SECS = 600   # 10-minute TTL


def get_forecast(lat: float, lng: float, city: str = "") -> Optional[dict]:
    """
    Fetch the current-day forecast for the given coordinates.

    Returns a dict:
        {
          "city":          str,               # e.g. "New York City"
          "high_temp_f":   float | None,      # Forecast high temperature (°F) for today
          "precip_pct":    float,             # Probability of precipitation (0.0–1.0)
          "short_desc":    str,               # "Partly Cloudy", "Rainy", etc.
          "period_name":   str,               # NWS period name ("Today", "Tonight", etc.)
          "fetched_at":    str,               # ISO timestamp
        }
    Returns None if the NWS API is unreachable or returns bad data.
    """
    cache_key = f"{lat:.4f},{lng:.4f}"
    now_ts = datetime.now(timezone.utc).timestamp()

    # Return cached data if still fresh
    if cache_key in _cache:
        entry = _cache[cache_key]
        if now_ts - entry["_fetched_ts"] < CACHE_TTL_SECS:
            return entry

    try:
        # Step 1: resolve grid endpoint from lat/lng
        resp = requests.get(
            f"https://api.weather.gov/points/{lat},{lng}",
            headers={"User-Agent": "KalshiBot/1.0 (contact@example.com)"},
            timeout=10,
        )
        if resp.status_code != 200:
            logger.warning(f"NWS points lookup failed: HTTP {resp.status_code}")
            return None

        props = resp.json().get("properties", {})
        forecast_url = props.get("forecast")
        if not forecast_url:
            logger.warning("NWS points response missing forecast URL")
            return None

        # Step 2: fetch daily forecast
        resp2 = requests.get(
            forecast_url,
            headers={"User-Agent": "KalshiBot/1.0 (contact@example.com)"},
            timeout=10,
        )
        if resp2.status_code != 200:
            logger.warning(f"NWS forecast fetch failed: HTTP {resp2.status_code}")
            return None

        periods = resp2.json().get("properties", {}).get("periods", [])
        if not periods:
            logger.warning("NWS forecast returned no periods")
            return None

        # Kalshi weather markets are for TOMORROW — find the first daytime period
        # whose startTime falls on tomorrow's date. Fall back to the first daytime
        # period if tomorrow's date can't be matched (e.g., late-night when NWS
        # hasn't posted tomorrow's forecast yet).
        tomorrow = date.today() + timedelta(days=1)
        period = None
        for p in periods:
            if not p.get("isDaytime", True):
                continue
            start_str = p.get("startTime", "")
            if start_str:
                try:
                    period_date = datetime.fromisoformat(start_str).date()
                    if period_date == tomorrow:
                        period = p
                        break
                except (ValueError, TypeError):
                    pass
        # Fallback: first daytime period (could be today — better than nothing)
        if period is None:
            for p in periods:
                if p.get("isDaytime", True):
                    period = p
                    break
        if period is None:
            period = periods[0]

        temp = period.get("temperature")
        precip_raw = period.get("probabilityOfPrecipitation", {})
        precip_val = precip_raw.get("value") if isinstance(precip_raw, dict) else precip_raw
        precip_pct = (float(precip_val) / 100.0) if precip_val is not None else 0.0

        result = {
            "city":         city or f"{lat:.2f},{lng:.2f}",
            "high_temp_f":  float(temp) if temp is not None else None,
            "precip_pct":   round(precip_pct, 4),
            "short_desc":   period.get("shortForecast", ""),
            "period_name":  period.get("name", ""),
            "fetched_at":   datetime.now(timezone.utc).isoformat(),
            "_fetched_ts":  now_ts,
        }

        _cache[cache_key] = result
        logger.info(
            f"NWS forecast [{city}]: high={temp}°F precip={precip_pct*100:.0f}% "
            f"({period.get('shortForecast', '')})"
        )
        return result

    except Exception as e:
        logger.warning(f"NWS forecast fetch error: {e}")
        return None


def get_open_meteo(lat: float, lng: float, city: str = "") -> Optional[dict]:
    """
    Fetch today's high temperature and precipitation probability from Open-Meteo (ECMWF model).

    Returns a dict with the same shape as get_forecast() for easy comparison:
        {
          "city":         str,
          "high_temp_f":  float | None,
          "precip_pct":   float,        # 0.0–1.0
          "short_desc":   str,
          "period_name":  str,
          "fetched_at":   str,
        }
    Returns None on error.
    """
    cache_key = f"om_{lat:.4f},{lng:.4f}"
    now_ts = datetime.now(timezone.utc).timestamp()
    if cache_key in _cache:
        entry = _cache[cache_key]
        if now_ts - entry["_fetched_ts"] < CACHE_TTL_SECS:
            return entry

    try:
        resp = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude":         lat,
                "longitude":        lng,
                "daily":            "temperature_2m_max,precipitation_probability_max",
                "temperature_unit": "fahrenheit",
                "timezone":         "auto",
                "forecast_days":    2,  # fetch 2 days so index [1] = tomorrow
            },
            timeout=10,
        )
        if resp.status_code != 200:
            logger.warning(f"Open-Meteo HTTP {resp.status_code}")
            return None

        data  = resp.json()
        daily = data.get("daily", {})
        temps   = daily.get("temperature_2m_max", [])
        precips = daily.get("precipitation_probability_max", [])

        # Kalshi weather markets are for TOMORROW — use index [1] if available, else [0]
        idx = 1 if len(temps) > 1 else 0
        high_f   = float(temps[idx])   if len(temps) > idx   and temps[idx]   is not None else None
        precip_p = float(precips[idx]) / 100.0 if len(precips) > idx and precips[idx] is not None else 0.0

        result = {
            "city":        city or f"{lat:.2f},{lng:.2f}",
            "high_temp_f": round(high_f, 1) if high_f is not None else None,
            "precip_pct":  round(precip_p, 4),
            "short_desc":  "Open-Meteo ECMWF",
            "period_name": "Today",
            "fetched_at":  datetime.now(timezone.utc).isoformat(),
            "_fetched_ts": now_ts,
        }
        _cache[cache_key] = result
        logger.info(
            f"Open-Meteo [{city}]: high={high_f}°F precip={precip_p*100:.0f}%"
        )
        return result

    except Exception as e:
        logger.warning(f"Open-Meteo fetch error: {e}")
        return None


def high_temp_probability(forecast_temp_f: float, threshold_f: float) -> float:
    """
    Estimate the probability that the high temperature exceeds threshold_f,
    given that NWS forecasts forecast_temp_f as the high.

    NWS point forecasts have a typical MAE of ~4–5°F. We model the error as
    normally distributed and compute P(actual > threshold | forecast = T).

    This is a fast approximation using a logistic sigmoid with scale ≈ 5°F.
    At (T == threshold): probability = 0.50.
    At (T = threshold + 10): probability ≈ 0.88.
    At (T = threshold - 10): probability ≈ 0.12.
    """
    from math import exp
    delta = forecast_temp_f - threshold_f
    p = 1.0 / (1.0 + exp(-delta / 5.0))   # logistic with scale 5°F ≈ NWS forecast error
    return round(p, 4)
