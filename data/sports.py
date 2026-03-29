"""
sports.py — ESPN live game data fetcher for MLB, NBA, and NHL.

Uses ESPN's unofficial public scoreboard API (no key required).
Returns win probabilities and team names for games happening today,
so strategy/sports.py can compare them to Kalshi market prices.

ESPN API endpoints:
  MLB: https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/scoreboard
  NBA: https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard
  NHL: https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/scoreboard

Cached for CACHE_TTL_SECS per sport to avoid repeated calls on every 5-min poll.
"""

import logging
import requests
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger("sports")

CACHE_TTL_SECS = 180   # 3-minute TTL — live win probabilities change quickly

_cache: dict = {}

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"

SPORT_PATHS = {
    "baseball/mlb": "MLB",
    "basketball/nba": "NBA",
    "hockey/nhl": "NHL",
}


def get_games(espn_sport: str) -> list[dict]:
    """
    Return a list of today's games for the given ESPN sport path.

    Each game dict:
        {
          "home_team":       str,   # Full name e.g. "LA Dodgers"
          "home_abbr":       str,   # Abbreviation e.g. "LAD"
          "away_team":       str,   # Full name e.g. "Chicago Cubs"
          "away_abbr":       str,   # Abbreviation e.g. "CHC"
          "home_win_pct":    float, # 0.0–1.0 win probability for home team
          "away_win_pct":    float, # 0.0–1.0 win probability for away team
          "status":          str,   # "pre", "in", "post"
          "display_clock":   str,   # "7:10 PM ET", "3rd Quarter", etc.
          "game_id":         str,   # ESPN game ID
          "start_time":      str,   # ISO UTC start time
        }

    Returns empty list on any error.
    """
    now_ts = datetime.now(timezone.utc).timestamp()

    if espn_sport in _cache:
        entry = _cache[espn_sport]
        if now_ts - entry["_ts"] < CACHE_TTL_SECS:
            return entry["games"]

    try:
        url = f"{ESPN_BASE}/{espn_sport}/scoreboard"
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            logger.warning(f"ESPN {espn_sport} HTTP {resp.status_code}")
            return []

        data = resp.json()
        events = data.get("events", [])
        games = []

        for event in events:
            try:
                game = _parse_event(event)
                if game:
                    games.append(game)
            except Exception as e:
                logger.debug(f"Failed to parse ESPN event: {e}")
                continue

        _cache[espn_sport] = {"games": games, "_ts": now_ts}
        sport_label = SPORT_PATHS.get(espn_sport, espn_sport)
        logger.info(f"ESPN {sport_label}: {len(games)} games fetched")
        return games

    except Exception as e:
        logger.warning(f"ESPN {espn_sport} fetch error: {e}")
        return []


def _parse_event(event: dict) -> Optional[dict]:
    """Extract structured game data from a single ESPN event dict."""
    competitions = event.get("competitions", [])
    if not competitions:
        return None
    comp = competitions[0]

    # Teams
    competitors = comp.get("competitors", [])
    if len(competitors) < 2:
        return None

    home = next((c for c in competitors if c.get("homeAway") == "home"), competitors[0])
    away = next((c for c in competitors if c.get("homeAway") == "away"), competitors[1])

    home_team = home.get("team", {}).get("displayName", "")
    home_abbr = home.get("team", {}).get("abbreviation", "")
    away_team = away.get("team", {}).get("displayName", "")
    away_abbr = away.get("team", {}).get("abbreviation", "")

    # Status
    status_obj = event.get("status", {})
    status_type = status_obj.get("type", {})
    status_name = status_type.get("name", "")   # STATUS_SCHEDULED, STATUS_IN_PROGRESS, STATUS_FINAL
    display_clock = status_type.get("shortDetail", "")

    if "FINAL" in status_name or "POSTPONED" in status_name:
        return None   # Game over — skip

    if "SCHEDULED" in status_name:
        status = "pre"
    elif "IN_PROGRESS" in status_name or "HALFTIME" in status_name:
        status = "in"
    else:
        status = "pre"

    # Win probabilities
    home_win_pct = 0.5
    away_win_pct = 0.5

    # Live in-game probability (most accurate)
    situation = comp.get("situation", {})
    if situation:
        home_odds = situation.get("homeTeamOdds", {})
        away_odds = situation.get("awayTeamOdds", {})
        if home_odds.get("winPercentage") is not None:
            home_win_pct = float(home_odds["winPercentage"]) / 100.0
            away_win_pct = 1.0 - home_win_pct

    # Pre-game moneyline odds (fallback for pre-game or when situation is absent)
    if home_win_pct == 0.5:
        odds_list = comp.get("odds", [])
        if odds_list:
            odds = odds_list[0]
            home_ml = odds.get("homeTeamOdds", {}).get("winPercentage")
            away_ml = odds.get("awayTeamOdds", {}).get("winPercentage")
            if home_ml is not None:
                home_win_pct = float(home_ml) / 100.0
                away_win_pct = 1.0 - home_win_pct
            elif away_ml is not None:
                away_win_pct = float(away_ml) / 100.0
                home_win_pct = 1.0 - away_win_pct

    # Start time
    start_time = event.get("date", "")

    return {
        "home_team":     home_team,
        "home_abbr":     home_abbr,
        "away_team":     away_team,
        "away_abbr":     away_abbr,
        "home_win_pct":  round(home_win_pct, 4),
        "away_win_pct":  round(away_win_pct, 4),
        "status":        status,
        "display_clock": display_clock,
        "game_id":       event.get("id", ""),
        "start_time":    start_time,
    }


def find_matching_game(games: list[dict], market_title: str) -> Optional[dict]:
    """
    Given a list of ESPN games and a Kalshi market title,
    find the game whose teams appear in the title.

    Kalshi titles look like:
      "Will the LA Dodgers win against the Chicago Cubs?"
      "Cubs @ Dodgers"
      "Boston Celtics vs New York Knicks"

    Matching: check if any team's abbreviation or partial name is in the title.
    Returns the best-matching game dict, or None.
    """
    title_lower = market_title.lower()

    best_game = None
    best_score = 0

    for game in games:
        score = 0
        home_abbr = game["home_abbr"].lower()
        away_abbr = game["away_abbr"].lower()
        home_words = [w.lower() for w in game["home_team"].split() if len(w) > 2]
        away_words = [w.lower() for w in game["away_team"].split() if len(w) > 2]

        if home_abbr in title_lower:
            score += 3
        if away_abbr in title_lower:
            score += 3
        for w in home_words:
            if w in title_lower:
                score += 1
        for w in away_words:
            if w in title_lower:
                score += 1

        if score > best_score:
            best_score = score
            best_game = game

    if best_score >= 2:
        return best_game
    return None
