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
                event["_espn_sport"] = espn_sport   # pass sport context to _parse_event
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
    home_team_id = str(home.get("team", {}).get("id", ""))
    away_team_id = str(away.get("team", {}).get("id", ""))

    # Team records — already embedded in ESPN scoreboard (total, home, road)
    home_record = home_home_record = ""
    away_record = away_road_record = ""
    for rec in home.get("records", []):
        rtype = rec.get("type", "")
        if rtype in ("total", "ytd"):   # ESPN uses "ytd" for NHL overall record
            home_record = rec.get("summary", "")
        elif rtype == "home":
            home_home_record = rec.get("summary", "")
    for rec in away.get("records", []):
        rtype = rec.get("type", "")
        if rtype in ("total", "ytd"):
            away_record = rec.get("summary", "")
        elif rtype == "road":
            away_road_record = rec.get("summary", "")

    # Status
    status_obj = event.get("status", {})
    status_type = status_obj.get("type", {})
    status_name  = status_type.get("name", "")
    short_detail = status_type.get("shortDetail", "")
    period       = int(status_obj.get("period", 0))
    clock_str    = status_obj.get("displayClock", "")  # "5:15" or "0:00"

    if "FINAL" in status_name or "POSTPONED" in status_name:
        return None

    if "SCHEDULED" in status_name:
        status = "pre"
    elif "IN_PROGRESS" in status_name or "HALFTIME" in status_name:
        status = "in"
    else:
        status = "pre"

    # Scores (available for in-game)
    score_home = _parse_score(home)
    score_away = _parse_score(away)
    score_diff = score_home - score_away   # positive = home leading
    score_validated = False   # will be set True if NHL API confirms
    score_mismatch  = False   # will be set True if sources disagree >1 goal (trade should skip)

    # Pre-game moneyline odds
    home_win_pct = 0.5
    away_win_pct = 0.5
    has_odds = False
    odds_list = comp.get("odds", [])
    if odds_list:
        odds = odds_list[0]
        home_ml = odds.get("homeTeamOdds", {}).get("winPercentage")
        away_ml = odds.get("awayTeamOdds", {}).get("winPercentage")
        if home_ml is not None:
            home_win_pct = float(home_ml) / 100.0
            away_win_pct = 1.0 - home_win_pct
            has_odds = True
        elif away_ml is not None:
            away_win_pct = float(away_ml) / 100.0
            home_win_pct = 1.0 - away_win_pct
            has_odds = True

    start_time = event.get("date", "")
    espn_sport_hint = event.get("_espn_sport", "")  # injected by get_games()

    # NHL live score cross-validation — compare ESPN score against official NHL API.
    # Off-by-one (broadcast delay): trust NHL API and correct.
    # Larger disagreement (>1 goal): flag as mismatch → strategy will skip the trade.
    if status == "in" and "nhl" in espn_sport_hint:
        try:
            from data.team_stats import get_nhl_live_scores
            nhl_scores = get_nhl_live_scores()
            nhl_key = f"{away_abbr}@{home_abbr}"
            if nhl_key in nhl_scores:
                nhl = nhl_scores[nhl_key]
                if nhl["score_home"] != score_home or nhl["score_away"] != score_away:
                    total_diff = abs(nhl["score_home"] - score_home) + abs(nhl["score_away"] - score_away)
                    if total_diff > 1:
                        # Large disagreement — likely a goal review, OT event, or API lag.
                        # Skip rather than guess which source is correct.
                        logger.warning(
                            f"NHL score mismatch [{nhl_key}]: ESPN={score_home}-{score_away} "
                            f"NHL_API={nhl['score_home']}-{nhl['score_away']} "
                            f"(diff={total_diff}) — marking mismatch, trade will be skipped"
                        )
                        score_mismatch = True
                    else:
                        # Off-by-one: minor broadcast lag → trust the official NHL API
                        logger.warning(
                            f"NHL score mismatch [{nhl_key}]: ESPN={score_home}-{score_away} "
                            f"NHL_API={nhl['score_home']}-{nhl['score_away']} — correcting"
                        )
                        score_home = nhl["score_home"]
                        score_away = nhl["score_away"]
                        score_diff = score_home - score_away
                        score_validated = True
                else:
                    score_validated = True
        except Exception as e:
            logger.debug(f"NHL score cross-validation error: {e}")

    return {
        "home_team":         home_team,
        "home_abbr":         home_abbr,
        "away_team":         away_team,
        "away_abbr":         away_abbr,
        "home_team_id":      home_team_id,
        "away_team_id":      away_team_id,
        "home_record":       home_record,
        "away_record":       away_record,
        "home_home_record":  home_home_record,
        "away_road_record":  away_road_record,
        "home_win_pct":      round(home_win_pct, 4),
        "away_win_pct":      round(away_win_pct, 4),
        "has_odds":          has_odds,
        "status":            status,
        "display_clock":     short_detail,
        "period":            period,
        "clock":             clock_str,
        "score_home":        score_home,
        "score_away":        score_away,
        "score_diff":        score_diff,
        "score_validated":   score_validated,
        "score_mismatch":    score_mismatch,
        "game_id":           event.get("id", ""),
        "start_time":        start_time,
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


# ---------------------------------------------------------------------- #
#  In-game win probability model                                           #
# ---------------------------------------------------------------------- #

# Scoring rates (points/goals per minute, total both teams)
_SCORING_RATE = {
    "basketball/nba": 4.6,   # ~220 pts/game ÷ 48 min
    "hockey/nhl":     0.16,  # ~5.8 goals/game ÷ 60 min (slightly conservative)
    "baseball/mlb":   None,  # inning-based; handled separately
}

# Period length in minutes per sport
_PERIOD_MINS = {
    "basketball/nba": 12,
    "hockey/nhl":     20,
    "baseball/mlb":   None,
}

# Total regular periods per sport
_TOTAL_PERIODS = {
    "basketball/nba": 4,
    "hockey/nhl":     3,
    "baseball/mlb":   9,   # innings
}


def compute_win_probability(game: dict, espn_sport: str) -> tuple:
    """
    Compute (home_win_pct, away_win_pct) for a live in-game match using a
    Gaussian random-walk model on score differential.

    Model:  P(home wins) = Φ(score_diff / σ)
            σ = sqrt(scoring_rate × minutes_remaining)

    Returns (home_win_pct, away_win_pct) floats in [0, 1].
    For pre-game or unknown state, returns (0.5, 0.5).
    """
    from math import sqrt, erf

    status     = game.get("status", "pre")
    score_diff = game.get("score_diff", 0)
    period     = game.get("period", 0)
    clock      = game.get("clock", "")

    if status != "in" or period == 0:
        return (game["home_win_pct"], game["away_win_pct"])

    rate = _SCORING_RATE.get(espn_sport)

    if rate is not None:
        # Continuous scoring sports (NBA / NHL)
        minutes_rem = _minutes_remaining(espn_sport, period, clock, game.get("display_clock", ""))
        if minutes_rem is None:
            return (game["home_win_pct"], game["away_win_pct"])
        # Minimum 0.5 min to avoid division by zero on final buzzer
        t = max(minutes_rem, 0.5)
        sigma = sqrt(rate * t)
        z = score_diff / sigma
        # Φ(z) = 0.5 * (1 + erf(z / sqrt(2)))
        home_p = max(0.01, min(0.99, 0.5 * (1.0 + erf(z / sqrt(2)))))
    else:
        # MLB: inning-based model (minutes_rem is irrelevant here)
        half_innings = _mlb_half_innings_remaining(period, game.get("display_clock", ""))
        if half_innings is None:
            return (game["home_win_pct"], game["away_win_pct"])
        # 0.45 runs per team per half-inning (league average ~4.5 runs/9 innings)
        runs_rate_per_half = 0.45
        t = max(half_innings * 1.0, 0.5)
        sigma = sqrt(runs_rate_per_half * 2 * t)   # both teams batting
        z = score_diff / sigma
        home_p = max(0.01, min(0.99, 0.5 * (1.0 + erf(z / sqrt(2)))))

    return (round(home_p, 4), round(1.0 - home_p, 4))


def _minutes_remaining(espn_sport: str, period: int, clock: str, short_detail: str) -> Optional[float]:
    """
    Compute total minutes of game time remaining for NBA/NHL.
    Returns None if we can't determine.
    """
    period_len  = _PERIOD_MINS.get(espn_sport)
    total_pds   = _TOTAL_PERIODS.get(espn_sport)

    if period_len is None or total_pds is None:
        return None   # MLB handled separately

    # Parse clock "M:SS" → decimal minutes
    clock_mins = _parse_clock(clock)
    if clock_mins is None:
        return None

    # Minutes left in current period + full future periods
    periods_left = max(0, total_pds - period)
    mins = clock_mins + periods_left * period_len

    # NHL: add 5-min OT expectation weight when tied and near end of regulation
    if espn_sport == "hockey/nhl" and period == total_pds:
        # If tied going into 3rd → ~25% chance of OT (~5 min extra)
        # Already captured in the model via larger sigma — no explicit adjustment needed
        pass

    return mins


def _mlb_half_innings_remaining(inning: int, short_detail: str) -> Optional[int]:
    """
    Compute half-innings remaining from current inning and short_detail.
    "Top 5th" → (9-5)*2 + 2 = 10 half-innings
    "Bot 5th" → (9-5)*2 + 1 = 9 half-innings
    Extra innings (inning > 9): only the current half-inning remains (sudden death).
    """
    detail = short_detail.lower()
    if "top" in detail or "mid" in detail:
        half = 2   # home still has bottom of this inning
    elif "bot" in detail or "end" in detail:
        half = 1   # only future innings remain (this half is in progress / done)
    else:
        return None

    if inning >= 9:
        # Extra innings: no guaranteed future innings — treat as 1 half-inning remaining
        return half
    remaining = (9 - inning) * 2 + half
    return max(remaining, 0)


def _parse_clock(clock: str) -> Optional[float]:
    """Parse "M:SS" clock string into decimal minutes. Returns None on failure."""
    if not clock or ':' not in clock:
        return None
    try:
        parts = clock.split(':')
        return int(parts[0]) + int(parts[1]) / 60.0
    except (ValueError, IndexError):
        return None


def get_nba_momentum(game_id: str) -> dict | None:
    """
    Fetch recent scoring momentum for a live NBA game using the ESPN Summary API.

    Analyzes the last ~3 minutes of play-by-play to detect scoring runs.
    Returns win probability adjustments for each team.

    Returns:
        {
          "home_pts_recent": int,    # Home team points in last ~3 minutes
          "away_pts_recent": int,    # Away team points in last ~3 minutes
          "home_adj":        float,  # +0.03 if home on run, -0.03 if away on run, else 0.0
          "away_adj":        float,  # Mirror of home_adj
        }
    Returns None on error or insufficient data.
    """
    if not game_id:
        return None

    cache_key = f"nba_mom_{game_id}"
    now_ts = datetime.now(timezone.utc).timestamp()
    if cache_key in _cache:
        entry = _cache[cache_key]
        if now_ts - entry["_ts"] < 60:   # 60-second TTL for momentum
            return entry.get("momentum")

    try:
        url = f"{ESPN_BASE}/basketball/nba/summary"
        resp = requests.get(url, params={"event": game_id}, timeout=10)
        if resp.status_code != 200:
            return None

        data = resp.json()
        plays = data.get("plays", [])
        if not plays:
            return None

        # Take the last N plays (roughly 3 minutes of game time)
        recent = plays[-20:]
        home_pts = 0
        away_pts = 0

        for play in recent:
            if not play.get("scoringPlay", False):
                continue
            team = play.get("team", {})
            home_away = team.get("homeAway", "")
            # Parse points from the play description
            play_pts = _extract_points(play.get("text", ""))
            if home_away == "home":
                home_pts += play_pts
            elif home_away == "away":
                away_pts += play_pts

        # Compute adjustment: if one team outscored the other by 8+ in recent plays
        diff = home_pts - away_pts
        if diff >= 8:
            home_adj, away_adj = 0.03, -0.03
        elif diff <= -8:
            home_adj, away_adj = -0.03, 0.03
        else:
            home_adj, away_adj = 0.0, 0.0

        result = {
            "home_pts_recent": home_pts,
            "away_pts_recent": away_pts,
            "home_adj":        home_adj,
            "away_adj":        away_adj,
        }
        _cache[cache_key] = {"momentum": result, "_ts": now_ts}
        logger.debug(
            f"NBA momentum [{game_id}]: home_recent={home_pts} away_recent={away_pts} "
            f"home_adj={home_adj:+.2f}"
        )
        return result

    except Exception as e:
        logger.debug(f"NBA momentum fetch error ({game_id}): {e}")
        return None


def _extract_points(play_text: str) -> int:
    """Extract point value from an ESPN play description (e.g. 'Three Point Jumper' → 3)."""
    text = play_text.lower()
    if "three point" in text or "3-pt" in text or "3pt" in text:
        return 3
    if "free throw" in text:
        return 1
    # Default to 2 for any other scoring play
    return 2


def _parse_score(competitor: dict) -> int:
    """Parse score from ESPN competitor dict. Returns 0 on failure."""
    try:
        return int(competitor.get("score", 0) or 0)
    except (ValueError, TypeError):
        return 0
