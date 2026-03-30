"""
mlb_stats.py — MLB Stats API base-out state win expectancy.

Uses the official MLB Stats API (statsapi.mlb.com — free, no key required).
Fetches the live game feed and derives win probability from:
  - Current inning and half
  - Outs
  - Runners on base (base-out state)
  - Score differential

This provides sharper MLB in-game probabilities than the Gaussian scoring
model in data/sports.py, because it accounts for base-out state directly
(a team with bases loaded and 0 outs in the 8th leading by 1 is very
different from the same score with no runners and 2 outs).

Base-out win expectancy table: 2019–2023 MLB league average run expectancy
(outs × runners bitmask → expected runs remaining in the half-inning).

Cached 60 seconds per game — updates every at-bat.
"""

import logging
import requests
from datetime import date, datetime, timezone
from typing import Optional

logger = logging.getLogger("mlb_stats")

CACHE_TTL_SECS = 60   # 1-minute TTL — live game state changes every pitch
_cache: dict = {}

MLB_BASE = "https://statsapi.mlb.com/api/v1"

# Expected runs by base-out state: _RE[outs][runners_bitmask]
# runners_bitmask: bit 0 = 1B occupied, bit 1 = 2B, bit 2 = 3B
# Source: 2019-2023 MLB average run expectancy tables
_RE = [
    # outs = 0
    [0.544, 0.941, 1.170, 1.567, 1.456, 1.853, 2.082, 2.479],
    # outs = 1
    [0.291, 0.587, 0.716, 1.014, 0.925, 1.222, 1.351, 1.648],
    # outs = 2
    [0.112, 0.252, 0.324, 0.463, 0.448, 0.588, 0.659, 0.799],
]

# League average runs per full half-inning (0.45 runs/team/half)
_RUNS_PER_HALF = 0.45


def get_mlb_win_probability(home_abbr: str, away_abbr: str) -> Optional[tuple]:
    """
    Fetch live base-out state win probability for a MLB game.

    Looks up today's schedule for a game matching the given abbreviations,
    then fetches the live feed and computes (home_win_pct, away_win_pct).

    Returns (home_win_pct, away_win_pct) or None if the game is not live
    or the API is unavailable.
    """
    cache_key = f"{away_abbr}@{home_abbr}"
    now_ts = datetime.now(timezone.utc).timestamp()
    if cache_key in _cache and now_ts - _cache[cache_key]["_ts"] < CACHE_TTL_SECS:
        return _cache[cache_key]["result"]

    result = _fetch(home_abbr, away_abbr)
    _cache[cache_key] = {"result": result, "_ts": now_ts}
    return result


def _fetch(home_abbr: str, away_abbr: str) -> Optional[tuple]:
    """Internal: fetch today's schedule, match the game, pull live feed."""
    try:
        today_str = date.today().strftime("%Y-%m-%d")
        resp = requests.get(
            f"{MLB_BASE}/schedule",
            params={"sportId": 1, "date": today_str, "hydrate": "linescore,teams"},
            timeout=10,
        )
        if resp.status_code != 200:
            return None

        game_pk = _match_game(resp.json(), home_abbr.lower(), away_abbr.lower())
        if not game_pk:
            return None

        return _live_win_prob(game_pk)

    except Exception as e:
        logger.debug(f"MLB Stats lookup error ({away_abbr}@{home_abbr}): {e}")
        return None


def _match_game(schedule: dict, home_lower: str, away_lower: str) -> Optional[int]:
    """Find the gamePk matching home/away abbreviations from today's schedule."""
    for date_entry in schedule.get("dates", []):
        for game in date_entry.get("games", []):
            teams = game.get("teams", {})
            h_abbr = teams.get("home", {}).get("team", {}).get("abbreviation", "").lower()
            a_abbr = teams.get("away", {}).get("team", {}).get("abbreviation", "").lower()
            h_name = teams.get("home", {}).get("team", {}).get("name", "").lower()
            a_name = teams.get("away", {}).get("team", {}).get("name", "").lower()

            home_match = (home_lower == h_abbr or home_lower in h_name)
            away_match = (away_lower == a_abbr or away_lower in a_name)
            if home_match and away_match:
                return game.get("gamePk")
    return None


def _live_win_prob(game_pk: int) -> Optional[tuple]:
    """Fetch live feed for game_pk and compute base-out state win probability."""
    try:
        resp = requests.get(
            f"{MLB_BASE}.1/game/{game_pk}/feed/live",
            timeout=10,
        )
        if resp.status_code != 200:
            return None

        data      = resp.json()
        game_data = data.get("gameData", {})
        live_data = data.get("liveData", {})

        # Only process live games
        abstract_state = game_data.get("status", {}).get("abstractGameState", "")
        if abstract_state != "Live":
            return None

        linescore = live_data.get("linescore", {})
        offense   = linescore.get("offense", {})

        inning     = int(linescore.get("currentInning", 0))
        half       = linescore.get("inningHalf", "Top").lower()
        outs       = int(linescore.get("outs", 0))
        score_home = int(linescore.get("teams", {}).get("home", {}).get("runs", 0))
        score_away = int(linescore.get("teams", {}).get("away", {}).get("runs", 0))

        if inning == 0:
            return None

        # Runners bitmask: bit 0 = 1B, bit 1 = 2B, bit 2 = 3B
        runners = (
            (1 if offense.get("first")  else 0) |
            (2 if offense.get("second") else 0) |
            (4 if offense.get("third")  else 0)
        )

        # Expected runs from current base-out state for the batting team
        outs_idx = min(outs, 2)
        current_re = _RE[outs_idx][runners]

        # Determine which team is batting and compute expected final scores
        is_top = ("top" in half or "mid" in half)
        score_diff = score_home - score_away   # positive = home leading

        if is_top:
            # Away batting: away gets current_re this half, then future away half-innings
            # Home gets full future bottom half-innings
            future_away_halves = max(0, 9 - inning)     # away tops remaining after this
            future_home_halves = max(0, 9 - inning + 1) # home gets this bottom + future
            away_exp = current_re + future_away_halves * _RUNS_PER_HALF
            home_exp = future_home_halves * _RUNS_PER_HALF
        else:
            # Home batting: home gets current_re this half, then future home half-innings
            future_away_halves = max(0, 9 - inning)     # away gets future tops
            future_home_halves = max(0, 9 - inning)     # home: remaining bottoms after this
            away_exp = future_away_halves * _RUNS_PER_HALF
            home_exp = current_re + future_home_halves * _RUNS_PER_HALF

        # Net expected score for home: positive means home expected to win
        net = score_diff + home_exp - away_exp

        # Convert to win probability via logistic — uncertainty scales with remaining batting
        total_halves = max(future_home_halves + future_away_halves + 1, 0.5)
        sigma = max((total_halves * _RUNS_PER_HALF) ** 0.5, 0.30)

        from math import exp
        home_p = max(0.01, min(0.99, 1.0 / (1.0 + exp(-net / sigma))))

        logger.debug(
            f"MLB base-out [{game_pk}]: inn={inning} {'top' if is_top else 'bot'} "
            f"outs={outs} runners={runners:03b} score={score_home}-{score_away} "
            f"net={net:+.2f} σ={sigma:.2f} home_p={home_p:.3f}"
        )
        return (round(home_p, 4), round(1.0 - home_p, 4))

    except Exception as e:
        logger.debug(f"MLB live feed error (pk={game_pk}): {e}")
        return None
