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

STANDINGS_TTL   = 3600   # 1 h
L10_TTL         = 1800   # 30 min
H2H_TTL         = 3600   # 1 h
NHL_LIVE_TTL    = 30     # 30 s — tight TTL for score cross-validation
NHL_GOALIE_TTL  = 300    # 5 min — goalie starters don't change mid-game
INJURY_TTL      = 1800   # 30 min
MLB_WIND_TTL    = 1800   # 30 min — NWS wind forecast

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
                "game_id":    game.get("id", 0),
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


# --------------------------------------------------------------------------- #
#  NHL Goalie                                                                   #
# --------------------------------------------------------------------------- #

def get_nhl_starting_goalies(away_abbr: str, home_abbr: str) -> dict:
    """
    Fetch the starting goalies for an NHL game via the official NHL API.

    Uses /v1/score/now to find the game_id, then /v1/gamecenter/{id}/boxscore
    to read the first (starting) goalie for each team.

    Returns:
        {
          "home_goalie": "Andrei Vasilevskiy",
          "away_goalie": "Jake Oettinger",
        }
    Returns {} on any error or if game not yet started.
    """
    cache_key = f"nhl_goalies_{away_abbr}@{home_abbr}"
    now_ts = datetime.now(timezone.utc).timestamp()
    if cache_key in _cache and now_ts - _cache[cache_key]["_ts"] < NHL_GOALIE_TTL:
        return _cache[cache_key]["data"]

    # Get game_id from live scores dict (reuses the 30s-TTL live score fetch)
    scores = get_nhl_live_scores()
    nhl_key = f"{away_abbr}@{home_abbr}"
    game_info = scores.get(nhl_key, {})
    game_id = game_info.get("game_id")
    if not game_id:
        return {}

    try:
        resp = requests.get(f"{NHL_API}/gamecenter/{game_id}/boxscore", timeout=10)
        if resp.status_code != 200:
            return {}

        data = resp.json()
        home_team = data.get("homeTeam", {})
        away_team = data.get("awayTeam", {})

        def _first_goalie_name(team_dict: dict) -> str:
            goalies = team_dict.get("goalies", [])
            if not goalies:
                return ""
            g = goalies[0]
            name = g.get("name", {})
            return name.get("default", "") or f"{g.get('firstName',{}).get('default','')} {g.get('lastName',{}).get('default','')}".strip()

        result = {
            "home_goalie": _first_goalie_name(home_team),
            "away_goalie": _first_goalie_name(away_team),
        }
        _cache[cache_key] = {"data": result, "_ts": now_ts}
        return result

    except Exception as e:
        logger.debug(f"NHL goalie fetch error ({nhl_key}): {e}")
        return {}


# --------------------------------------------------------------------------- #
#  Injury Feed                                                                  #
# --------------------------------------------------------------------------- #

# ESPN sport path → (sport, league) for the injuries endpoint
_ESPN_INJURY_PATH = {
    "baseball/mlb":   ("baseball",    "mlb"),
    "basketball/nba": ("basketball",  "nba"),
    "hockey/nhl":     ("hockey",      "nhl"),
}


def get_espn_injuries(team_id: str, espn_sport: str) -> list[str]:
    """
    Fetch the current injury report for a team from ESPN.

    Returns a list of strings like ["Aaron Judge (OUT)", "Giancarlo Stanton (Doubtful)"]
    for players with OUT, Doubtful, or Questionable status.
    Returns [] on error or if no notable injuries.
    """
    if not team_id:
        return []

    cache_key = f"injuries_{espn_sport}_{team_id}"
    now_ts = datetime.now(timezone.utc).timestamp()
    if cache_key in _cache and now_ts - _cache[cache_key]["_ts"] < INJURY_TTL:
        return _cache[cache_key]["data"]

    sport_info = _ESPN_INJURY_PATH.get(espn_sport)
    if not sport_info:
        return []
    sport, league = sport_info
    today = date.today()
    # NBA/NHL seasons span calendar years (Oct–Jun). In Jan–Jun the season year is the prior year.
    # MLB season runs Apr–Oct so current year is always correct.
    if league in ("nba", "nhl") and today.month <= 6:
        year = today.year - 1
    else:
        year = today.year

    try:
        resp = requests.get(
            f"https://sports.core.api.espn.com/v2/sports/{sport}/leagues/{league}"
            f"/seasons/{year}/teams/{team_id}/injuries",
            params={"limit": 50},
            timeout=10,
        )
        if resp.status_code != 200:
            _cache[cache_key] = {"data": [], "_ts": now_ts}
            return []

        injuries = []
        for item in resp.json().get("items", []):
            status = item.get("status", "")
            if status.lower() not in ("out", "doubtful", "questionable", "day-to-day"):
                continue
            athlete = item.get("athlete", {})
            name    = athlete.get("displayName", athlete.get("shortName", "Unknown"))
            injuries.append(f"{name} ({status})")

        _cache[cache_key] = {"data": injuries, "_ts": now_ts}
        return injuries

    except Exception as e:
        logger.debug(f"ESPN injury fetch error (team={team_id}, sport={espn_sport}): {e}")
        _cache[cache_key] = {"data": [], "_ts": now_ts}
        return []


# --------------------------------------------------------------------------- #
#  MLB Ballpark Wind                                                            #
# --------------------------------------------------------------------------- #

# MLB team abbreviation → (lat, lng) for each home ballpark
_MLB_BALLPARKS: dict[str, tuple[float, float]] = {
    "ARI": (33.445, -112.067),   # Chase Field, Phoenix
    "ATL": (33.890,  -84.468),   # Truist Park, Atlanta
    "BAL": (39.284,  -76.622),   # Oriole Park, Baltimore
    "BOS": (42.347,  -71.097),   # Fenway Park, Boston
    "CHC": (41.948,  -87.656),   # Wrigley Field, Chicago
    "CWS": (41.830,  -87.634),   # Guaranteed Rate Field, Chicago
    "CIN": (39.097,  -84.507),   # Great American Ball Park, Cincinnati
    "CLE": (41.496,  -81.685),   # Progressive Field, Cleveland
    "COL": (39.756, -104.994),   # Coors Field, Denver
    "DET": (42.339,  -83.048),   # Comerica Park, Detroit
    "HOU": (29.757,  -95.356),   # Minute Maid Park, Houston
    "KC":  (39.051,  -94.480),   # Kauffman Stadium, Kansas City
    "LAA": (33.800, -117.883),   # Angel Stadium, Anaheim
    "LAD": (34.074, -118.240),   # Dodger Stadium, Los Angeles
    "MIA": (25.778,  -80.220),   # loanDepot Park, Miami
    "MIL": (43.028,  -87.971),   # American Family Field, Milwaukee
    "MIN": (44.981,  -93.278),   # Target Field, Minneapolis
    "NYM": (40.757,  -73.846),   # Citi Field, New York
    "NYY": (40.829,  -73.926),   # Yankee Stadium, New York
    "PHI": (39.906,  -75.166),   # Citizens Bank Park, Philadelphia
    "PIT": (40.447,  -80.006),   # PNC Park, Pittsburgh
    "SD":  (32.707, -117.157),   # Petco Park, San Diego
    "SF":  (37.778, -122.389),   # Oracle Park, San Francisco
    "SEA": (47.591, -122.333),   # T-Mobile Park, Seattle
    "STL": (38.623,  -90.193),   # Busch Stadium, St. Louis
    "TB":  (27.768,  -82.653),   # Tropicana Field (indoor — wind irrelevant)
    "TEX": (32.747,  -97.083),   # Globe Life Field (retractable roof)
    "WSH": (38.873,  -77.007),   # Nationals Park, Washington DC
    # Toronto and Oakland not covered (NWS US-only)
}

# Parks with roofs — wind data not meaningful
_INDOOR_PARKS = {"TB", "TEX", "MIA", "MIL", "HOU", "MIN", "ARI"}


def get_mlb_ballpark_wind(home_abbr: str) -> dict:
    """
    Fetch current wind conditions at an MLB ballpark using NWS hourly forecast.

    Returns:
        {
          "wind_mph":    float,   # Wind speed in mph
          "wind_dir":    str,     # Direction e.g. "NE", "SW"
          "is_high":     bool,    # True if wind_mph >= 15 (affects scoring significantly)
          "is_indoor":   bool,    # True if park has roof (wind irrelevant)
        }
    Returns {} on error or unsupported team (Toronto, Oakland).
    """
    if home_abbr in _INDOOR_PARKS:
        return {"wind_mph": 0.0, "wind_dir": "N/A", "is_high": False, "is_indoor": True}

    coords = _MLB_BALLPARKS.get(home_abbr)
    if not coords:
        return {}  # Team not in lookup (TOR, OAK) — NWS US-only

    lat, lng = coords
    cache_key = f"mlb_wind_{home_abbr}"
    now_ts = datetime.now(timezone.utc).timestamp()
    if cache_key in _cache and now_ts - _cache[cache_key]["_ts"] < MLB_WIND_TTL:
        return _cache[cache_key]["data"]

    try:
        # Step 1: resolve NWS grid from lat/lng
        pts_resp = requests.get(
            f"https://api.weather.gov/points/{lat:.4f},{lng:.4f}",
            headers={"User-Agent": "KalshiBot/1.0"},
            timeout=10,
        )
        if pts_resp.status_code != 200:
            return {}

        props = pts_resp.json().get("properties", {})
        hourly_url = props.get("forecastHourly")
        if not hourly_url:
            return {}

        # Step 2: fetch hourly forecast
        fc_resp = requests.get(
            hourly_url,
            headers={"User-Agent": "KalshiBot/1.0"},
            timeout=10,
        )
        if fc_resp.status_code != 200:
            return {}

        periods = fc_resp.json().get("properties", {}).get("periods", [])
        if not periods:
            return {}

        # Use the first (current or upcoming) period
        p = periods[0]
        wind_str = p.get("windSpeed", "0 mph")   # e.g. "12 mph" or "12 to 17 mph"
        wind_dir = p.get("windDirection", "")

        # Parse mph — take the high end if it's a range
        import re
        nums = re.findall(r"\d+", wind_str)
        wind_mph = float(max(int(n) for n in nums)) if nums else 0.0

        result = {
            "wind_mph":  wind_mph,
            "wind_dir":  wind_dir,
            "is_high":   wind_mph >= 15,
            "is_indoor": False,
        }
        _cache[cache_key] = {"data": result, "_ts": now_ts}
        return result

    except Exception as e:
        logger.debug(f"MLB wind fetch error ({home_abbr}): {e}")
        return {}
