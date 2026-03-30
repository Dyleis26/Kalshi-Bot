"""
team_stats.py — Team records, L10 form, and H2H data from multiple sources.

Sources by sport:
  NHL: api-web.nhle.com (official NHL API)
       - Live score cross-validation (prevents wrong ESPN score reads)
       - Standings with L10, home/road splits built in
  MLB: statsapi.mlb.com (official MLB Stats API)
       - Standings with home/away splits
  NBA: ESPN team schedule endpoint — L10 computed from last 10 completed games
  H2H: ESPN summary/seasonseries — all sports (current season series record)

All results are cached to minimize outbound API calls:
  Standings:    1-hour TTL
  ESPN L10:     30-minute TTL
  H2H:          1-hour TTL
  NHL live:     30-second TTL (score cross-validation)
"""

import logging
import requests
from datetime import datetime, timezone, date, timedelta
from typing import Optional

logger = logging.getLogger("team_stats")

_cache: dict = {}

STANDINGS_TTL = 3600   # 1 h
L10_TTL       = 1800   # 30 min
H2H_TTL       = 3600   # 1 h
NHL_LIVE_TTL  = 30     # 30 s — tight TTL for score cross-validation

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports"
NHL_API   = "https://api-web.nhle.com/v1"
MLB_API   = "https://statsapi.mlb.com/api/v1"


# --------------------------------------------------------------------------- #
#  NHL                                                                          #
# --------------------------------------------------------------------------- #

def get_nhl_standings() -> dict:
    """
    Fetch current NHL standings from the official NHL API.

    Returns dict keyed by team abbreviation:
      {
        "NYR": {
          "record":      "48-21-3",   # W-L-OTL
          "l10":         "7-2-1",     # last 10 W-L-OTL
          "home":        "26-9-1",    # home W-L-OTL
          "road":        "22-12-2",   # road W-L-OTL
          "season_pct":  0.6944,
          "l10_pct":     0.75,
          "home_pct":    0.7361,
          "road_pct":    0.6389,
        }
      }
    Returns {} on error.
    """
    cache_key = "nhl_standings"
    now_ts = datetime.now(timezone.utc).timestamp()
    if cache_key in _cache and now_ts - _cache[cache_key]["_ts"] < STANDINGS_TTL:
        return _cache[cache_key]["data"]

    try:
        resp = requests.get(f"{NHL_API}/standings/now", timeout=10)
        if resp.status_code != 200:
            logger.warning(f"NHL standings HTTP {resp.status_code}")
            return {}

        result = {}
        for s in resp.json().get("standings", []):
            abbr = s.get("teamAbbrev", {}).get("default", "")
            if not abbr:
                continue

            w    = s.get("wins", 0);      l    = s.get("losses", 0);    otl  = s.get("otLosses", 0)
            l10w = s.get("l10Wins", 0);   l10l = s.get("l10Losses", 0); l10o = s.get("l10OtLosses", 0)
            hw   = s.get("homeWins", 0);  hl   = s.get("homeLosses", 0); hotl = s.get("homeOtLosses", 0)
            rw   = s.get("roadWins", 0);  rl   = s.get("roadLosses", 0); rotl = s.get("roadOtLosses", 0)

            gp   = max(w + l + otl, 1)
            l10  = max(l10w + l10l + l10o, 1)
            hgp  = max(hw + hl + hotl, 1)
            rgp  = max(rw + rl + rotl, 1)

            result[abbr] = {
                "record":     f"{w}-{l}-{otl}",
                "l10":        f"{l10w}-{l10l}-{l10o}",
                "home":       f"{hw}-{hl}-{hotl}",
                "road":       f"{rw}-{rl}-{rotl}",
                "season_pct": round((w + 0.5 * otl) / gp, 4),
                "l10_pct":    round((l10w + 0.5 * l10o) / l10, 4),
                "home_pct":   round((hw + 0.5 * hotl) / hgp, 4),
                "road_pct":   round((rw + 0.5 * rotl) / rgp, 4),
            }

        _cache[cache_key] = {"data": result, "_ts": now_ts}
        logger.info(f"NHL standings fetched: {len(result)} teams")
        return result

    except Exception as e:
        logger.warning(f"NHL standings fetch error: {e}")
        return {}


def get_nhl_live_scores() -> dict:
    """
    Fetch current NHL live scores from the official NHL API.
    Used to cross-validate ESPN score data and correct stale/wrong reads.

    Returns dict keyed by "AWAY@HOME" abbreviation:
      {
        "CHI@NJ": {
          "score_home": 5,
          "score_away": 3,
          "period":     3,
          "clock":      "3:21",
          "state":      "LIVE",   # LIVE, OFF, FUT, PRE
        }
      }
    Returns {} on error.
    """
    cache_key = "nhl_live_scores"
    now_ts = datetime.now(timezone.utc).timestamp()
    if cache_key in _cache and now_ts - _cache[cache_key]["_ts"] < NHL_LIVE_TTL:
        return _cache[cache_key]["data"]

    try:
        resp = requests.get(f"{NHL_API}/score/now", timeout=10)
        if resp.status_code != 200:
            return {}

        result = {}
        for game in resp.json().get("games", []):
            home   = game.get("homeTeam", {})
            away   = game.get("awayTeam", {})
            h_abbr = home.get("abbrev", "")
            a_abbr = away.get("abbrev", "")
            if not h_abbr or not a_abbr:
                continue
            clock = game.get("clock", {})
            result[f"{a_abbr}@{h_abbr}"] = {
                "score_home": home.get("score", 0),
                "score_away": away.get("score", 0),
                "period":     game.get("period", 0),
                "clock":      clock.get("timeRemaining", ""),
                "state":      game.get("gameState", ""),
            }

        _cache[cache_key] = {"data": result, "_ts": now_ts}
        return result

    except Exception as e:
        logger.warning(f"NHL live score fetch error: {e}")
        return {}


# --------------------------------------------------------------------------- #
#  MLB                                                                          #
# --------------------------------------------------------------------------- #

def get_mlb_standings() -> dict:
    """
    Fetch current MLB standings from the official MLB Stats API.

    Returns dict keyed by team abbreviation:
      {
        "SEA": {
          "record":     "3-0",
          "home":       "3-0",
          "road":       "0-0",
          "season_pct": 1.000,
          "home_pct":   1.000,
          "road_pct":   0.000,
        }
      }
    Returns {} on error.
    """
    cache_key = "mlb_standings"
    now_ts = datetime.now(timezone.utc).timestamp()
    if cache_key in _cache and now_ts - _cache[cache_key]["_ts"] < STANDINGS_TTL:
        return _cache[cache_key]["data"]

    try:
        for season in [date.today().year, date.today().year - 1]:
            resp = requests.get(
                f"{MLB_API}/standings",
                params={
                    "leagueId": "103,104",
                    "season": season,
                    "standingsTypes": "regularSeason",
                    "hydrate": "team",
                },
                timeout=10,
            )
            if resp.status_code == 200 and resp.json().get("records"):
                break
        else:
            return {}

        result = {}
        for division in resp.json().get("records", []):
            for tr in division.get("teamRecords", []):
                team = tr.get("team", {})
                abbr = team.get("abbreviation", "")
                if not abbr:
                    continue

                w = tr.get("wins", 0)
                l = tr.get("losses", 0)
                gp = max(w + l, 1)

                hw = hl = aw = al = 0
                for split in tr.get("records", {}).get("splitRecords", []):
                    stype = split.get("type", "")
                    if stype == "home":
                        hw, hl = split.get("wins", 0), split.get("losses", 0)
                    elif stype == "away":
                        aw, al = split.get("wins", 0), split.get("losses", 0)

                result[abbr] = {
                    "record":     f"{w}-{l}",
                    "home":       f"{hw}-{hl}",
                    "road":       f"{aw}-{al}",
                    "season_pct": round(w / gp, 4),
                    "home_pct":   round(hw / max(hw + hl, 1), 4),
                    "road_pct":   round(aw / max(aw + al, 1), 4),
                }

        _cache[cache_key] = {"data": result, "_ts": now_ts}
        logger.info(f"MLB standings fetched: {len(result)} teams")
        return result

    except Exception as e:
        logger.warning(f"MLB standings fetch error: {e}")
        return {}


# --------------------------------------------------------------------------- #
#  ESPN — L10 and H2H (all sports)                                             #
# --------------------------------------------------------------------------- #

def get_espn_l10(team_id: str, espn_sport: str) -> Optional[tuple]:
    """
    Compute last-10-games W-L record for a team from ESPN's schedule endpoint.

    Args:
        team_id:     ESPN team ID (from scoreboard competitor.team.id)
        espn_sport:  e.g. "basketball/nba"

    Returns (wins, losses) tuple for last ≤10 completed games, or None on error.
    """
    if not team_id:
        return None

    cache_key = f"l10_{espn_sport}_{team_id}"
    now_ts = datetime.now(timezone.utc).timestamp()
    if cache_key in _cache and now_ts - _cache[cache_key]["_ts"] < L10_TTL:
        return _cache[cache_key]["result"]

    try:
        resp = requests.get(
            f"{ESPN_BASE}/{espn_sport}/teams/{team_id}/schedule",
            timeout=10,
        )
        if resp.status_code != 200:
            return None

        completed = []
        for event in resp.json().get("events", []):
            comp = event.get("competitions", [{}])[0]
            if "FINAL" not in comp.get("status", {}).get("type", {}).get("name", ""):
                continue
            for c in comp.get("competitors", []):
                if str(c.get("team", {}).get("id", "")) == str(team_id):
                    completed.append(bool(c.get("winner", False)))
                    break

        last10 = completed[-10:]
        wins   = sum(1 for w in last10 if w)
        result = (wins, len(last10) - wins)
        _cache[cache_key] = {"result": result, "_ts": now_ts}
        return result

    except Exception as e:
        logger.debug(f"ESPN L10 error ({team_id}): {e}")
        return None


def get_espn_h2h(event_id: str, espn_sport: str) -> Optional[str]:
    """
    Fetch the current-season series (H2H) summary for a game from ESPN.

    Returns a short string like "LAC leads 2-0", "Series tied 1-1",
    or None if unavailable.
    """
    if not event_id:
        return None

    cache_key = f"h2h_{espn_sport}_{event_id}"
    now_ts = datetime.now(timezone.utc).timestamp()
    if cache_key in _cache and now_ts - _cache[cache_key]["_ts"] < H2H_TTL:
        return _cache[cache_key]["result"]

    try:
        resp = requests.get(
            f"{ESPN_BASE}/{espn_sport}/summary",
            params={"event": event_id},
            timeout=10,
        )
        if resp.status_code != 200:
            return None

        summary = None
        for ss in resp.json().get("seasonseries", []):
            s = ss.get("shortSummary") or ss.get("summary") or ss.get("description")
            if s:
                summary = s
                break

        _cache[cache_key] = {"result": summary, "_ts": now_ts}
        return summary

    except Exception as e:
        logger.debug(f"ESPN H2H error ({event_id}): {e}")
        return None


# --------------------------------------------------------------------------- #
#  Helpers                                                                      #
# --------------------------------------------------------------------------- #

def format_record(wins: int, losses: int, otl: int = 0) -> str:
    """Format a W-L or W-L-OTL record string."""
    if otl:
        return f"{wins}-{losses}-{otl}"
    return f"{wins}-{losses}"
