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
from data.odds import get_odds, find_matching_odds, force_invalidate
from data.mlb_stats import get_mlb_win_probability
from data.team_stats import (
    get_nhl_standings, get_mlb_standings,
    get_espn_l10, get_espn_h2h,
    get_nhl_starting_goalies, get_espn_injuries, get_mlb_ballpark_wind,
)
from administration.config import (
    CONTRACT_PRICE_MIN, CONTRACT_PRICE_MAX,
    SPORTS_EDGE_MIN, SPORTS_SHORT_EDGE_MIN,
    SPORTS_PREGAME_EDGE_MIN, SPORTS_PREGAME_VOTE_MIN,
    SPORTS_PREGAME_VOTE_CONFIDENCE, SPORTS_PREGAME_CONFIDENCE_EDGE,
    SPORTS_PREGAME_SHORT, SPORTS_SHORT_MAX_NO_PRICE,
    SPORTS_CONTRACT_PRICE_MIN, SPORTS_CONTRACT_PRICE_MAX,
    SPORTS_PREGAME_PRICE_MIN, SPORTS_PREGAME_PRICE_MAX,
    ODDS_API_KEY,
)

logger = logging.getLogger("strategy.sports")

LONG  = "long"
SHORT = "short"
NONE  = "none"

# Tracks known OUT players per game so we can detect newly-scratched players
# and trigger an Odds API cache refresh before the edge is computed.
_known_out_players: dict = {}  # {"{home}|{away}": set_of_out_player_strings}

# Fix #3: tracks YES-team score deficit per game across scans (for momentum filter).
# {game_id: [yes_team_deficit, ...]}  — positive = YES team winning, negative = trailing.
_game_deficit_history: dict = {}

# Lag detection: tracks last known score_diff and Kalshi YES price per game.
# When score changes but Kalshi price hasn't moved, a lag window is open.
# {game_id: {"score_diff": int, "kalshi_yes": float}}
_last_game_state: dict = {}

# ESPN uses shortened abbreviations; some NHL API endpoints return longer codes.
# Map ESPN abbr → NHL API abbr so standings lookups don't return empty dicts.
_NHL_ABBR_ESPN_TO_API: dict = {
    "TB":  "TBL",   # Tampa Bay Lightning
    "NJ":  "NJD",   # New Jersey Devils
    "LA":  "LAK",   # Los Angeles Kings
    "SJ":  "SJS",   # San Jose Sharks
    "CB":  "CBJ",   # Columbus Blue Jackets
}


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

        # Price range filter: widest for in-game, moderate for pre-game
        # In-game: 0.20–0.80 (live score gives real edge even at extreme prices)
        # Pre-game: 0.40–0.70 (model less certain; edge filter does the real work)
        if is_live:
            price_min, price_max = SPORTS_CONTRACT_PRICE_MIN, SPORTS_CONTRACT_PRICE_MAX
        else:
            price_min, price_max = SPORTS_PREGAME_PRICE_MIN, SPORTS_PREGAME_PRICE_MAX
        if not (price_min <= yes_ask <= price_max):
            status_tag = "in-game" if is_live else "pre-game"
            return _no_trade(f"price outside range ({status_tag} YES={yes_ask:.2f}, range {price_min:.2f}–{price_max:.2f})", yes_ask, sport_label, title)

        # Early-game 0-0 filter: when no score exists the Gaussian model returns p=0.50,
        # making any "edge" pure pre-game Kalshi mispricing — not live information.
        # Skip the first half of the game when tied 0-0.
        if is_live and game.get("score_home", 0) == 0 and game.get("score_away", 0) == 0:
            _total_periods = {"baseball/mlb": 9, "basketball/nba": 4, "hockey/nhl": 3}
            _tp = _total_periods.get(espn_sport, 4)
            if game.get("period", 1) <= _tp // 2:
                return _no_trade(
                    f"tied 0-0 in period {game.get('period',1)} — no live score edge yet",
                    yes_ask, sport_label, title,
                )

        # Injury pre-check (pre-game only): fetch OUT players for both teams before the
        # odds call. If a player is newly OUT since the last check, invalidate the Odds API
        # cache so get_odds() below fetches a fresh line rather than the stale cached one.
        if not is_live and ODDS_API_KEY:
            _hi = get_espn_injuries(game.get("home_team_id", ""), espn_sport)
            _ai = get_espn_injuries(game.get("away_team_id", ""), espn_sport)
            _game_key = f"{game['home_abbr']}|{game['away_abbr']}"
            _out_now  = {p for p in (_hi + _ai) if "(out)" in p.lower() or "injur" in p.lower()}
            _prev_out = _known_out_players.get(_game_key, set())
            _new_out  = _out_now - _prev_out
            if _new_out:
                logger.info(
                    f"Sports [{sport_label}]: injury alert {_game_key} — "
                    f"new OUT: {', '.join(_new_out)} — refreshing Odds API"
                )
                force_invalidate(espn_sport)
            _known_out_players[_game_key] = _out_now

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
        yes_team_win_pct, yes_is_home = _resolve_yes_team_probability(
            game, title, yes_sub, home_p=home_p, away_p=away_p
        )
        if yes_team_win_pct is None:
            return _no_trade(f"could not resolve YES team from '{title[:60]}'", yes_ask, sport_label, title)

        # NBA in-game: apply play-by-play momentum adjustment (±0.03)
        momentum_tag = ""
        if is_live and espn_sport == "basketball/nba":
            mom = get_nba_momentum(game.get("game_id", ""))
            if mom:
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
            _h_abbr = game.get("home_abbr", "")
            _a_abbr = game.get("away_abbr", "")
            hst = standings.get(_NHL_ABBR_ESPN_TO_API.get(_h_abbr, _h_abbr), {})
            ast = standings.get(_NHL_ABBR_ESPN_TO_API.get(_a_abbr, _a_abbr), {})
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

        # ------------------------------------------------------------------ #
        #  Pre-game winner prediction vote score                              #
        # ------------------------------------------------------------------ #
        vote_score = 0
        vote_detail = ""
        if not is_live:
            vote_score, vote_detail = _pregame_vote_score(
                yes_team_win_pct=yes_team_win_pct,
                home_p=home_p,
                home_l10=home_l10,
                away_l10=away_l10,
                home_home_record=home_home_record,
                away_road_record=away_road_record,
                h2h_series=h2h_series,
                line_movement=line_movement,
                game=game,
                yes_is_home=yes_is_home,
            )
            logger.info(
                f"Sports [{sport_label}]: pre-game votes {vote_score}/6 — {vote_detail}"
            )

        # Three-tier pre-game threshold — confidence is the primary signal:
        #   Tier 1 (confidence pick): 4+ votes AND model > 52% → needs just 0.02 edge
        #   Tier 2 (vote-backed):     3+ votes                  → needs 0.07 edge
        #   Tier 3 (edge-only):       any                       → needs 0.12 edge
        using_confidence_tier = False
        if not is_live and vote_score >= SPORTS_PREGAME_VOTE_CONFIDENCE and yes_team_win_pct > 0.52:
            active_threshold = SPORTS_PREGAME_CONFIDENCE_EDGE
            using_confidence_tier = True
        elif not is_live and vote_score >= SPORTS_PREGAME_VOTE_MIN and edge > 0:
            active_threshold = SPORTS_PREGAME_EDGE_MIN
        else:
            active_threshold = SPORTS_EDGE_MIN

        # Lag detection: when score just changed but Kalshi price hasn't moved,
        # a temporary mispricing window is open. Lower threshold by 1/3 to enter earlier.
        lag_detected = False
        if is_live:
            game_id = game.get("game_id", "")
            cur_score_diff = game.get("score_diff", 0)
            last_state = _last_game_state.get(game_id)
            if last_state is not None:
                score_changed = cur_score_diff != last_state["score_diff"]
                kalshi_flat   = abs(yes_ask - last_state["kalshi_yes"]) < 0.02
                if score_changed and kalshi_flat and abs(edge) > 0.05:
                    lag_detected = True
                    active_threshold = round(active_threshold * 0.67, 3)
                    logger.info(
                        f"Sports [{sport_label}]: lag detected — score {last_state['score_diff']:+d}→{cur_score_diff:+d}, "
                        f"Kalshi flat at {yes_ask:.2f} — threshold lowered to {active_threshold:.3f}"
                    )
            _last_game_state[game_id] = {"score_diff": cur_score_diff, "kalshi_yes": yes_ask}

        src = prob_source + momentum_tag
        if lag_detected:
            src += " [LAG]"
        if edge >= active_threshold:
            direction = LONG
            if using_confidence_tier:
                reason = (
                    f"confidence pick — votes {vote_score}/6, "
                    f"p={yes_team_win_pct:.2f} Kalshi={yes_ask:.2f} edge={edge:+.2f}"
                )
            else:
                reason = (
                    f"edge={edge:+.2f} ({src}, "
                    f"p={yes_team_win_pct:.2f} Kalshi={yes_ask:.2f}) — buying YES"
                )
                if not is_live and vote_score > 0:
                    reason += f" | votes {vote_score}/6"
        elif edge <= -active_threshold:
            # Fix #2: don't buy NO when our own model says YES team wins.
            if yes_team_win_pct > 0.50:
                logger.info(
                    f"Sports [{sport_label}]: model-direction conflict — "
                    f"edge={edge:+.2f} but model p={yes_team_win_pct:.2f} > 0.50 — skip NO"
                )
                return _no_trade(
                    f"model-direction conflict: edge={edge:+.2f} "
                    f"(model p={yes_team_win_pct:.2f} agrees YES wins) — skip",
                    yes_ask, sport_label, title
                )

            # Short-direction quality filters: focus on confident winners, not
            # catching underdogs or fighting Kalshi on small margins.
            no_ask_est = round(1.0 - yes_ask, 2)

            # Block pre-game SHORTs entirely — only LONG on confirmed pre-game winners
            if not is_live and not SPORTS_PREGAME_SHORT:
                return _no_trade(
                    f"pre-game SHORT disabled — only LONG on confirmed winners "
                    f"(edge={edge:+.2f} p={yes_team_win_pct:.2f})",
                    yes_ask, sport_label, title
                )

            # Block if NO price is too expensive — bad risk/reward
            # (paying >65¢ for a max $1.00 return on an already-cheap NO)
            if no_ask_est > SPORTS_SHORT_MAX_NO_PRICE:
                return _no_trade(
                    f"SHORT blocked — NO price {no_ask_est:.2f} > max {SPORTS_SHORT_MAX_NO_PRICE:.2f} "
                    f"(bad risk/reward: pay {no_ask_est:.2f} to win {1-no_ask_est:.2f})",
                    yes_ask, sport_label, title
                )

            # In-game SHORT requires larger edge than LONG
            short_threshold = SPORTS_SHORT_EDGE_MIN
            if lag_detected:
                short_threshold = round(short_threshold * 0.67, 3)
            if abs(edge) < short_threshold:
                return _no_trade(
                    f"SHORT edge {edge:+.2f} below in-game SHORT threshold ±{short_threshold} "
                    f"(requires higher confidence than LONG)",
                    yes_ask, sport_label, title
                )

            direction = SHORT
            reason = (
                f"edge={edge:+.2f} ({src}, "
                f"p={yes_team_win_pct:.2f} Kalshi={yes_ask:.2f}) — buying NO"
            )
        else:
            direction = NONE
            reason = (
                f"edge too small: {edge:+.2f} < ±{active_threshold} "
                f"({src} p={yes_team_win_pct:.2f} Kalshi={yes_ask:.2f})"
            )
            if not is_live:
                reason += f" | votes {vote_score}/6"

        # Fix #3: in-game momentum block for NBA/NHL LONG trades.
        # If the YES team has trailed for 10+ consecutive scans AND the deficit
        # is the same or worse, the market knows something the static model doesn't.
        if (direction == LONG and is_live and
                espn_sport in ("basketball/nba", "hockey/nhl")):
            game_id = game.get("game_id", "")
            if game_id:
                # Determine YES team deficit: positive = winning, negative = trailing
                yes_deficit = game.get("score_diff", 0) if yes_is_home else -game.get("score_diff", 0)

                hist = _game_deficit_history.setdefault(game_id, [])
                hist.append(yes_deficit)
                if len(hist) > 20:
                    hist.pop(0)

                if len(hist) >= 10:
                    recent = hist[-10:]
                    all_trailing = all(d < 0 for d in recent)
                    worsening = recent[-1] <= recent[0]  # deficit same or grew
                    if all_trailing and worsening:
                        logger.info(
                            f"Sports [{sport_label}]: momentum block — "
                            f"YES team trailing {abs(yes_deficit)} for 10+ scans "
                            f"(deficit {recent[0]:+d} → {recent[-1]:+d})"
                        )
                        return _no_trade(
                            f"momentum block: trailing {abs(yes_deficit)} for 10+ scans "
                            f"({recent[0]:+d} → {recent[-1]:+d})",
                            yes_ask, sport_label, title
                        )

        logger.info(f"Sports [{sport_label}]: {reason}")

        # Confidence score blends edge magnitude and vote conviction:
        #   Pre-game: 50% from vote score (0–6) + 50% from edge (capped at 30¢)
        #   In-game:  edge-only (no vote system for live markets)
        if not is_live and vote_score > 0:
            vote_component = (vote_score / 6) * 50
            edge_component = min(abs(edge) * 100, 30) * (50 / 30)
            _conf_pct = round(min(vote_component + edge_component, 99.0), 1)
        else:
            _conf_pct = round(min(abs(edge) * 100, 99.0), 1)

        return {
            "direction":         direction,
            "confidence":        round(abs(edge), 4),
            "confidence_pct":    _conf_pct,
            "external_prob":     round(yes_team_win_pct, 4),
            "kalshi_yes":        round(yes_ask, 4),
            "edge":              round(edge, 4),
            "reason":            reason,
            "market_label":      market_label,
            "is_ingame":         is_live,
            "game_id":           game.get("game_id", ""),
            "home_score":        game.get("score_home", 0),
            "away_score":        game.get("score_away", 0),
            "game_score":        f"{game['score_home']}-{game['score_away']}",
            "game_period":       game.get("period", 0),
            "game_clock":        game.get("clock", ""),
            "score_validated":   game.get("score_validated", False),
            "prob_source":       src,
            # Vote conviction data (pre-game only)
            "vote_score":        vote_score,
            "vote_detail":       vote_detail,
            "confidence_tier":   "confidence" if using_confidence_tier else ("vote" if vote_score >= SPORTS_PREGAME_VOTE_MIN else "edge"),
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
                                   home_p: float = None, away_p: float = None
                                   ) -> tuple[float, bool] | tuple[None, None]:
    """
    Determine which team is the YES outcome, then return (win_probability, yes_is_home).

    home_p / away_p: pre-computed probabilities (in-game model or ESPN pre-game).
    Falls back to game["home_win_pct"] / game["away_win_pct"] if not supplied.

    Returns (probability, yes_is_home) so callers know which team YES represents
    without having to compare floating-point probabilities (which is ambiguous at 50/50).
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
            return hp, True
        if away_match and not home_match:
            return ap, False

    # Fallback: first team mentioned in title is YES
    title_lower = title.lower()
    home_pos = _first_mention(title_lower, [home_abbr] + home_words)
    away_pos = _first_mention(title_lower, [away_abbr] + away_words)

    if home_pos is None and away_pos is None:
        return None, None
    if home_pos is None:
        return ap, False
    if away_pos is None:
        return hp, True
    if home_pos <= away_pos:
        return hp, True
    return ap, False


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


def _parse_l10_wins(l10: str) -> int | None:
    """Parse wins from an L10 string like '7-3' or '6-2-2' (NHL W-L-OTL)."""
    if not l10:
        return None
    try:
        return int(l10.split("-")[0])
    except (ValueError, IndexError):
        return None


def _parse_venue_wp(record: str) -> float | None:
    """Parse win% from a venue record like '21-16' or '23-13-1'."""
    if not record:
        return None
    parts = record.split("-")
    try:
        wins   = int(parts[0])
        losses = int(parts[1])
        ot     = int(parts[2]) if len(parts) >= 3 else 0
        total  = wins + losses + ot
        return round(wins / total, 4) if total > 0 else None
    except (ValueError, IndexError):
        return None


def _pregame_vote_score(yes_team_win_pct: float, home_p: float,
                         home_l10: str, away_l10: str,
                         home_home_record: str, away_road_record: str,
                         h2h_series: str | None, line_movement: float,
                         game: dict, yes_is_home: bool = True) -> tuple[int, str]:
    """
    Compute a 0–6 winner-prediction score for the YES team in a pre-game market.

    Signals:
      +2  Bookmaker implied probability > 0.55  (strong favorite)
      +1  Better L10 form (more wins in last 10 games)
      +1  Better venue win% (home team's home record vs away team's road record)
      +1  H2H season series leader
      +1  Opening line moved ≥0.03 toward this team

    Returns (score, detail_string).
    """
    score = 0
    parts = []

    # 1. Bookmaker implied probability > 0.55 (+2 — strong favorite signal)
    if yes_team_win_pct > 0.55:
        score += 2
        parts.append(f"fav({yes_team_win_pct:.2f})")

    # 2. Better L10 form (+1)
    yes_l10 = home_l10 if yes_is_home else away_l10
    opp_l10 = away_l10 if yes_is_home else home_l10
    y_wins = _parse_l10_wins(yes_l10)
    o_wins = _parse_l10_wins(opp_l10)
    if y_wins is not None and o_wins is not None and y_wins > o_wins:
        score += 1
        parts.append(f"L10({yes_l10}>{opp_l10})")

    # 3. Better venue win% (+1)
    yes_venue = home_home_record if yes_is_home else away_road_record
    opp_venue = away_road_record if yes_is_home else home_home_record
    y_wp = _parse_venue_wp(yes_venue)
    o_wp = _parse_venue_wp(opp_venue)
    if y_wp is not None and o_wp is not None and y_wp > o_wp:
        score += 1
        parts.append(f"venue({y_wp:.0%}>{o_wp:.0%})")

    # 4. H2H season series leader (+1)
    if h2h_series and "leads" in h2h_series.lower():
        yes_abbr  = (game.get("home_abbr", "") if yes_is_home else game.get("away_abbr", "")).lower()
        yes_words = [w.lower() for w in (game.get("home_team", "") if yes_is_home
                     else game.get("away_team", "")).split() if len(w) > 2]
        if yes_abbr in h2h_series.lower() or any(w in h2h_series.lower() for w in yes_words):
            score += 1
            parts.append("h2h")

    # 5. Line movement toward YES team (+1)
    if yes_is_home and line_movement >= 0.03:
        score += 1
        parts.append(f"line+{line_movement:.2f}")
    elif not yes_is_home and line_movement <= -0.03:
        score += 1
        parts.append(f"line{line_movement:.2f}")

    return score, (", ".join(parts) if parts else "no signals")


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
