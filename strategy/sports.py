"""
sports.py — Kalshi sports market signal strategy.

Pre-game: compares ESPN pre-game moneyline to Kalshi YES price.
In-game:  uses a Gaussian random-walk scoring model (score_diff + time remaining)
          to compute live win probability, then compares to Kalshi price.

In-game edge sources:
  - Market lag: Kalshi prices often update slowly vs live game state
  - Comeback value: trailing teams are sometimes oversold on Kalshi
  - Normalization: late-close games should trade near 0.50; if they don't, edge exists

Price range is wider for in-game (SPORTS_CONTRACT_PRICE_MIN/MAX) because a
team winning 79-62 in the 3rd quarter legitimately has YES > 0.80.
"""

import logging
from data.sports import get_games, find_matching_game, compute_win_probability, get_nba_momentum
from data.odds import get_odds, find_matching_odds
from data.mlb_stats import get_mlb_win_probability
from data.team_stats import (
    get_nhl_standings, get_mlb_standings,
    get_espn_l10, get_espn_h2h,
    get_nhl_starting_goalies, get_espn_injuries, get_mlb_ballpark_wind,
)
from administration.config import (
    CONTRACT_PRICE_MIN, CONTRACT_PRICE_MAX,
    SPORTS_EDGE_MIN, SPORTS_CONTRACT_PRICE_MIN, SPORTS_CONTRACT_PRICE_MAX,
    ODDS_API_KEY,
)

logger = logging.getLogger("strategy.sports")

LONG  = "long"
SHORT = "short"
NONE  = "none"


class SportsStrategy:

    def decide(self, market: dict, espn_sport: str, sport_label: str = "") -> dict:
        """
        Evaluate a Kalshi sports market and return a trade decision.

        Args:
            market:       Kalshi market dict (from get_market / get_markets_by_series)
            espn_sport:   ESPN sport path, e.g. "baseball/mlb"
            sport_label:  Short label e.g. "MLB" for logging and Discord

        Returns:
            {
                "direction":      "long" | "short" | "none",
                "confidence":     float (edge magnitude),
                "external_prob":  float (ESPN win probability for the YES outcome),
                "kalshi_yes":     float (current Kalshi YES ask price),
                "edge":           float (external_prob - kalshi_yes),
                "reason":         str,
                "market_label":   str (formatted for Discord, e.g. "MLB: Cubs to WIN"),
            }
        """
        title   = market.get("title", market.get("subtitle", ""))
        yes_sub = market.get("yes_sub_title", "")   # e.g. "Denver" or "Golden State"
        yes_ask = float(market.get("yes_ask_dollars", 0.5))

        # Fetch today's ESPN games for this sport
        games = get_games(espn_sport)
        if not games:
            return _no_trade("no ESPN games found today", yes_ask, sport_label, title)

        # Match the Kalshi market to a specific ESPN game
        game = find_matching_game(games, title)
        if not game:
            return _no_trade(f"no ESPN game matched to '{title[:60]}'", yes_ask, sport_label, title)

        is_live = (game.get("status") == "in")

        # Score mismatch safety check — skip if NHL sources disagree by >1 goal
        if game.get("score_mismatch"):
            return _no_trade(
                f"NHL score mismatch between ESPN and NHL API — skipping until resolved",
                yes_ask, sport_label, title,
            )

        # Price range filter: wider for in-game (live markets can be 0.20–0.80+)
        price_min = SPORTS_CONTRACT_PRICE_MIN if is_live else CONTRACT_PRICE_MIN
        price_max = SPORTS_CONTRACT_PRICE_MAX if is_live else CONTRACT_PRICE_MAX
        if not (price_min <= yes_ask <= price_max):
            status_tag = "in-game" if is_live else "pre-game"
            return _no_trade(f"market too confident ({status_tag} YES={yes_ask:.2f})", yes_ask, sport_label, title)

        # Win probability source (in priority order):
        #   In-game MLB   → MLB Stats API base-out state model (most accurate)
        #   In-game NBA   → Gaussian model + play-by-play momentum adjustment
        #   In-game NHL   → Gaussian scoring model
        #   Pre-game      → The Odds API (sharp sportsbook lines) → ESPN fallback
        line_movement = 0.0  # only populated for pre-game via Odds API
        if is_live:
            if espn_sport == "baseball/mlb":
                mlb_result = get_mlb_win_probability(game["home_abbr"], game["away_abbr"])
                if mlb_result:
                    home_p, away_p = mlb_result
                    prob_source = (
                        f"MLBStats(score={game['score_home']}-{game['score_away']} "
                        f"{game.get('display_clock','')})"
                    )
                else:
                    home_p, away_p = compute_win_probability(game, espn_sport)
                    prob_source = (
                        f"model(score={game['score_home']}-{game['score_away']} "
                        f"diff={game['score_diff']:+d} {game.get('display_clock','')})"
                    )
            else:
                home_p, away_p = compute_win_probability(game, espn_sport)
                prob_source = (
                    f"model(score={game['score_home']}-{game['score_away']} "
                    f"diff={game['score_diff']:+d} {game.get('display_clock','')})"
                )
        else:
            # Pre-game: try The Odds API first (sharp sportsbook lines), fall back to ESPN
            home_p = away_p = None
            odds_source = None
            if ODDS_API_KEY:
                odds_games = get_odds(espn_sport, ODDS_API_KEY)
                if odds_games:
                    odds_match = find_matching_odds(
                        odds_games, game["home_abbr"], game["away_abbr"],
                        game["home_team"], game["away_team"]
                    )
                    if odds_match:
                        home_p = odds_match["home_win_pct"]
                        away_p = odds_match["away_win_pct"]
                        odds_source = odds_match["bookmaker"]

            if home_p is None:
                # Fall back to ESPN pre-game moneyline
                if not game.get("has_odds", False):
                    return _no_trade("no pre-game odds (Odds API + ESPN unavailable)", yes_ask, sport_label, title)
                home_p = game["home_win_pct"]
                away_p = game["away_win_pct"]
                prob_source = "ESPN pre-game"
                line_movement = 0.0
            else:
                prob_source = f"OddsAPI/{odds_source}"
                line_movement = odds_match.get("line_movement", 0.0) if odds_match else 0.0

        # Determine which team is the YES outcome
        yes_team_win_pct = _resolve_yes_team_probability(
            game, title, yes_sub, home_p=home_p, away_p=away_p
        )
        if yes_team_win_pct is None:
            return _no_trade(f"could not resolve YES team from '{title[:60]}'", yes_ask, sport_label, title)

        # NBA in-game: apply play-by-play momentum adjustment (±0.03)
        momentum_tag = ""
        if is_live and espn_sport == "basketball/nba":
            mom = get_nba_momentum(game.get("game_id", ""))
            if mom:
                # Determine if the YES team is home or away
                # Use the same resolution logic: yes_team_win_pct == home_p → YES is home
                yes_is_home = abs(yes_team_win_pct - home_p) < 0.005
                adj = mom["home_adj"] if yes_is_home else mom["away_adj"]
                if adj != 0.0:
                    yes_team_win_pct = max(0.01, min(0.99, yes_team_win_pct + adj))
                    tag_dir = "+" if adj > 0 else ""
                    momentum_tag = (
                        f" mom({tag_dir}{adj:+.2f} "
                        f"h={mom['home_pts_recent']} a={mom['away_pts_recent']})"
                    )

        edge = yes_team_win_pct - yes_ask
        market_label = _build_label(sport_label, title, game, yes_sub)

        # ------------------------------------------------------------------ #
        #  Team records, L10 form, and H2H                                    #
        # ------------------------------------------------------------------ #
        home_record = game.get("home_record", "")
        away_record = game.get("away_record", "")
        home_home_record = game.get("home_home_record", "")
        away_road_record = game.get("away_road_record", "")
        home_l10 = away_l10 = ""
        h2h_series = None

        # Pull official standings (NHL / MLB) for richer L10 + splits
        if espn_sport == "hockey/nhl":
            standings = get_nhl_standings()
            hst = standings.get(game.get("home_abbr", ""), {})
            ast = standings.get(game.get("away_abbr", ""), {})
            if hst:
                home_record      = hst["record"]
                home_home_record = hst["home"]
                home_l10         = hst["l10"]
            if ast:
                away_record      = ast["record"]
                away_road_record = ast["road"]
                away_l10         = ast["l10"]
        elif espn_sport == "baseball/mlb":
            standings = get_mlb_standings()
            hst = standings.get(game.get("home_abbr", ""), {})
            ast = standings.get(game.get("away_abbr", ""), {})
            if hst:
                home_record      = hst["record"]
                home_home_record = hst["home"]
            if ast:
                away_record      = ast["record"]
                away_road_record = ast["road"]
            # MLB L10 via ESPN schedule
            hl10 = get_espn_l10(game.get("home_team_id", ""), espn_sport)
            al10 = get_espn_l10(game.get("away_team_id", ""), espn_sport)
            home_l10 = f"{hl10[0]}-{hl10[1]}" if hl10 else ""
            away_l10 = f"{al10[0]}-{al10[1]}" if al10 else ""
        else:
            # NBA — L10 from ESPN schedule
            hl10 = get_espn_l10(game.get("home_team_id", ""), espn_sport)
            al10 = get_espn_l10(game.get("away_team_id", ""), espn_sport)
            home_l10 = f"{hl10[0]}-{hl10[1]}" if hl10 else ""
            away_l10 = f"{al10[0]}-{al10[1]}" if al10 else ""

        # H2H season series (all sports via ESPN summary)
        h2h_series = get_espn_h2h(game.get("game_id", ""), espn_sport)

        # ------------------------------------------------------------------ #
        #  Additional context: goalies, injuries, wind, line movement         #
        # ------------------------------------------------------------------ #

        # NHL goalie confirmation (in-game and pre-game)
        goalie_tag = ""
        if espn_sport == "hockey/nhl":
            goalies = get_nhl_starting_goalies(game.get("away_abbr", ""), game.get("home_abbr", ""))
            if goalies.get("home_goalie") or goalies.get("away_goalie"):
                goalie_tag = (
                    f" | goalies: {game.get('home_abbr','')}={goalies.get('home_goalie','?')} "
                    f"{game.get('away_abbr','')}={goalies.get('away_goalie','?')}"
                )

        # Injury report (all sports, pre-game only — in-game injuries already reflected in score)
        home_injuries = away_injuries = []
        if not is_live:
            home_injuries = get_espn_injuries(game.get("home_team_id", ""), espn_sport)
            away_injuries = get_espn_injuries(game.get("away_team_id", ""), espn_sport)
            if home_injuries or away_injuries:
                inj_parts = []
                if home_injuries:
                    inj_parts.append(f"{game.get('home_abbr','')}: {', '.join(home_injuries[:3])}")
                if away_injuries:
                    inj_parts.append(f"{game.get('away_abbr','')}: {', '.join(away_injuries[:3])}")
                logger.info(f"Sports [{sport_label}] injuries: {' | '.join(inj_parts)}")

        # MLB ballpark wind
        wind_tag = ""
        if espn_sport == "baseball/mlb":
            wind = get_mlb_ballpark_wind(game.get("home_abbr", ""))
            if wind and not wind.get("is_indoor"):
                wind_tag = f" | wind: {wind.get('wind_mph',0):.0f}mph {wind.get('wind_dir','')}"
                if wind.get("is_high"):
                    wind_tag += " ⚠️HIGH"

        # Line movement (pre-game only, from Odds API)
        line_tag = ""
        if line_movement and abs(line_movement) >= 0.03:
            direction_str = "toward home" if line_movement > 0 else "away from home"
            line_tag = f" | line moved {line_movement:+.2f} ({direction_str})"

        # Log context line for all pre-game trades
        if not is_live:
            logger.info(
                f"Sports [{sport_label}] context: "
                f"home {game.get('home_abbr','')} {home_record} (home {home_home_record}, L10 {home_l10}) | "
                f"away {game.get('away_abbr','')} {away_record} (road {away_road_record}, L10 {away_l10}) | "
                f"H2H: {h2h_series or 'n/a'}{goalie_tag}{wind_tag}{line_tag}"
            )
        elif goalie_tag or wind_tag:
            logger.info(f"Sports [{sport_label}] context: {game.get('home_abbr','')}-{game.get('away_abbr','')}{goalie_tag}{wind_tag}")

        src = prob_source + momentum_tag
        if edge >= SPORTS_EDGE_MIN:
            direction = LONG
            reason = (
                f"edge={edge:+.2f} ({src}, "
                f"p={yes_team_win_pct:.2f} Kalshi={yes_ask:.2f}) — buying YES"
            )
        elif edge <= -SPORTS_EDGE_MIN:
            direction = SHORT
            reason = (
                f"edge={edge:+.2f} ({src}, "
                f"p={yes_team_win_pct:.2f} Kalshi={yes_ask:.2f}) — buying NO"
            )
        else:
            direction = NONE
            reason = (
                f"edge too small: {edge:+.2f} < ±{SPORTS_EDGE_MIN} "
                f"({src} p={yes_team_win_pct:.2f} Kalshi={yes_ask:.2f})"
            )

        logger.info(f"Sports [{sport_label}]: {reason}")

        return {
            "direction":         direction,
            "confidence":        round(abs(edge), 4),
            "confidence_pct":    round(min(abs(edge) / 0.5 * 100, 100.0), 1),
            "external_prob":     round(yes_team_win_pct, 4),
            "kalshi_yes":        round(yes_ask, 4),
            "edge":              round(edge, 4),
            "reason":            reason,
            "market_label":      market_label,
            "is_ingame":         is_live,
            "game_score":        f"{game['score_home']}-{game['score_away']}",
            "game_period":       game.get("period", 0),
            "game_clock":        game.get("clock", ""),
            "score_validated":   game.get("score_validated", False),
            # Team records and form
            "home_record":       home_record,
            "away_record":       away_record,
            "home_home_record":  home_home_record,
            "away_road_record":  away_road_record,
            "home_l10":          home_l10,
            "away_l10":          away_l10,
            "h2h_series":        h2h_series,
            # Crypto-compatible empty fields for trade log
            "rsi": 0, "macd": 0, "momentum": 0, "vwap": 0, "price": 0,
            "rsi_bias": None, "macd_bias": None, "momentum_bias": None, "vwap_bias": None,
        }


# ---------------------------------------------------------------------- #
#  Helpers                                                                 #
# ---------------------------------------------------------------------- #

def _resolve_yes_team_probability(game: dict, title: str, yes_sub: str = "",
                                   home_p: float = None, away_p: float = None) -> float | None:
    """
    Determine which team is the YES outcome, then return the win probability for that team.

    home_p / away_p: pre-computed probabilities (in-game model or ESPN pre-game).
    Falls back to game["home_win_pct"] / game["away_win_pct"] if not supplied.
    """
    hp = home_p if home_p is not None else game["home_win_pct"]
    ap = away_p if away_p is not None else game["away_win_pct"]

    home_abbr  = game["home_abbr"].lower()
    away_abbr  = game["away_abbr"].lower()
    home_words = [w.lower() for w in game["home_team"].split() if len(w) > 2]
    away_words = [w.lower() for w in game["away_team"].split() if len(w) > 2]

    if yes_sub:
        sub_lower = yes_sub.lower()
        home_match = (home_abbr in sub_lower or any(w in sub_lower for w in home_words))
        away_match = (away_abbr in sub_lower or any(w in sub_lower for w in away_words))
        if home_match and not away_match:
            return hp
        if away_match and not home_match:
            return ap

    # Fallback: first team mentioned in title is YES
    title_lower = title.lower()
    home_pos = _first_mention(title_lower, [home_abbr] + home_words)
    away_pos = _first_mention(title_lower, [away_abbr] + away_words)

    if home_pos is None and away_pos is None:
        return None
    if home_pos is None:
        return ap
    if away_pos is None:
        return hp
    return hp if home_pos <= away_pos else ap


def _first_mention(text: str, tokens: list) -> int | None:
    """Return the character index of the first token found in text, or None."""
    positions = []
    for tok in tokens:
        idx = text.find(tok)
        if idx != -1:
            positions.append(idx)
    return min(positions) if positions else None


def _build_label(sport_label: str, title: str, game: dict, yes_sub: str = "") -> str:
    """Build a short Discord-friendly label, e.g. 'NBA - GSW WIN'."""
    # Match yes_sub to home or away team to find the abbreviation
    if yes_sub:
        sub_lower = yes_sub.lower()
        home_words = [w.lower() for w in game["home_team"].split() if len(w) > 2]
        away_words = [w.lower() for w in game["away_team"].split() if len(w) > 2]
        home_match = (game["home_abbr"].lower() in sub_lower or
                      any(w in sub_lower for w in home_words))
        away_match = (game["away_abbr"].lower() in sub_lower or
                      any(w in sub_lower for w in away_words))
        if home_match and not away_match:
            return f"{sport_label} - {game['home_abbr']} WIN"
        if away_match and not home_match:
            return f"{sport_label} - {game['away_abbr']} WIN"

    # Fallback: first team abbreviation from title position
    title_lower = title.lower()
    home_pos = _first_mention(title_lower, [game["home_abbr"].lower()] +
                              [w.lower() for w in game["home_team"].split() if len(w) > 2])
    away_pos = _first_mention(title_lower, [game["away_abbr"].lower()] +
                              [w.lower() for w in game["away_team"].split() if len(w) > 2])
    if home_pos is not None and (away_pos is None or home_pos <= away_pos):
        return f"{sport_label} - {game['home_abbr']} WIN"
    if away_pos is not None:
        return f"{sport_label} - {game['away_abbr']} WIN"

    return f"{sport_label} - {game['home_abbr']} vs {game['away_abbr']}"


def _no_trade(reason: str, yes_ask: float, sport_label: str, title: str) -> dict:
    logger.info(f"Sports [{sport_label}]: skip — {reason}")
    return {
        "direction":      NONE,
        "confidence":     0.0,
        "confidence_pct": 0.0,
        "external_prob":  0.0,
        "kalshi_yes":     round(yes_ask, 4),
        "edge":          0.0,
        "reason":        reason,
        "market_label":  sport_label or "Sports",
        "is_ingame":     False,
        "rsi": 0, "macd": 0, "momentum": 0, "vwap": 0, "price": 0,
        "rsi_bias": None, "macd_bias": None, "momentum_bias": None, "vwap_bias": None,
    }
