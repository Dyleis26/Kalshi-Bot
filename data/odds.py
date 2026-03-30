"""
odds.py — The Odds API sportsbook data fetcher.

Fetches consensus pre-game moneyline probabilities from major bookmakers
(Pinnacle, DraftKings, FanDuel) and returns vig-removed true win probability.

This replaces ESPN's default 0.50 when no moneyline data is available and
provides sharper pre-game edges than ESPN's own line data.

API:  https://the-odds-api.com/
Key:  ODDS_API_KEY in .env (free tier: 500 requests/month)

Sport keys:
  baseball/mlb     → baseball_mlb
  basketball/nba   → basketball_nba
  hockey/nhl       → icehockey_nhl

Cached 30 minutes to conserve free-tier quota.
"""

import logging
import requests
from datetime import datetime, timezone

logger = logging.getLogger("odds")

CACHE_TTL_SECS = 1800  # 30-minute cache — pre-game lines move slowly

_cache: dict = {}

BASE_URL = "https://api.the-odds-api.com/v4/sports"

# ESPN sport path → Odds API sport key
SPORT_KEY_MAP = {
    "baseball/mlb":   "baseball_mlb",
    "basketball/nba": "basketball_nba",
    "hockey/nhl":     "icehockey_nhl",
}

# Preferred bookmakers in priority order — Pinnacle has sharpest lines
PREFERRED_BOOKS = ["pinnacle", "draftkings", "fanduel", "betmgm", "bovada", "williamhill_us"]


def get_odds(espn_sport: str, api_key: str) -> list[dict]:
    """
    Fetch today's pre-game moneyline odds for the given sport.

    Returns a list of game dicts:
        {
          "home_team":    str,
          "away_team":    str,
          "home_win_pct": float,  # vig-removed true probability (0.0–1.0)
          "away_win_pct": float,
          "bookmaker":    str,    # source bookmaker key
        }
    Returns empty list when api_key is missing, sport is off-season, or on error.
    """
    if not api_key:
        return []

    sport_key = SPORT_KEY_MAP.get(espn_sport)
    if not sport_key:
        return []

    now_ts = datetime.now(timezone.utc).timestamp()
    if sport_key in _cache and now_ts - _cache[sport_key]["_ts"] < CACHE_TTL_SECS:
        return _cache[sport_key]["games"]

    try:
        resp = requests.get(
            f"{BASE_URL}/{sport_key}/odds",
            params={
                "apiKey":     api_key,
                "regions":    "us",
                "markets":    "h2h",
                "oddsFormat": "decimal",
            },
            timeout=10,
        )

        if resp.status_code == 401:
            logger.warning("Odds API: invalid API key")
            return []
        if resp.status_code == 422:
            # Off-season or no current events
            return []
        if resp.status_code != 200:
            logger.warning(f"Odds API HTTP {resp.status_code} for {sport_key}")
            return []

        games = [g for e in resp.json() if (g := _parse_event(e)) is not None]
        _cache[sport_key] = {"games": games, "_ts": now_ts}

        remaining = resp.headers.get("x-requests-remaining", "?")
        logger.info(f"Odds API {sport_key}: {len(games)} games, {remaining} requests left this month")
        return games

    except Exception as e:
        logger.warning(f"Odds API fetch error ({sport_key}): {e}")
        return []


def _parse_event(event: dict) -> dict | None:
    """Extract vig-removed probabilities from a single Odds API event."""
    home_team = event.get("home_team", "")
    away_team = event.get("away_team", "")
    bookmakers = event.get("bookmakers", [])
    if not bookmakers:
        return None

    # Pick the sharpest bookmaker available
    book_map = {b["key"]: b for b in bookmakers}
    selected = None
    for preferred in PREFERRED_BOOKS:
        if preferred in book_map:
            selected = book_map[preferred]
            break
    if not selected:
        selected = bookmakers[0]

    # Extract h2h market
    for market in selected.get("markets", []):
        if market.get("key") != "h2h":
            continue
        outcomes = {o["name"]: float(o.get("price", 2.0)) for o in market.get("outcomes", [])}

        home_dec = outcomes.get(home_team, 0.0)
        away_dec = outcomes.get(away_team, 0.0)
        if not home_dec or not away_dec:
            return None

        # Convert decimal odds to implied probability, then remove vig
        home_imp = 1.0 / home_dec
        away_imp = 1.0 / away_dec
        total = home_imp + away_imp
        if total <= 0:
            return None

        return {
            "home_team":    home_team,
            "away_team":    away_team,
            "home_win_pct": round(home_imp / total, 4),
            "away_win_pct": round(away_imp / total, 4),
            "bookmaker":    selected["key"],
        }

    return None


def find_matching_odds(odds_games: list[dict], home_abbr: str, away_abbr: str,
                       home_team: str, away_team: str) -> dict | None:
    """
    Find the odds entry matching the given team abbreviations/names.

    Scoring: 2 pts per team name match (substring). Returns best match
    with score ≥ 2 (at least one team matched on each side).
    Returns None if no match found.
    """
    home_words = {w.lower() for w in home_team.split() if len(w) > 2}
    away_words = {w.lower() for w in away_team.split() if len(w) > 2}
    home_lower = home_abbr.lower()
    away_lower = away_abbr.lower()

    best, best_score = None, 0
    for g in odds_games:
        g_home = g["home_team"].lower()
        g_away = g["away_team"].lower()
        score = 0
        if home_lower in g_home or any(w in g_home for w in home_words):
            score += 2
        if away_lower in g_away or any(w in g_away for w in away_words):
            score += 2
        if score > best_score:
            best_score, best = score, g

    return best if best_score >= 2 else None
