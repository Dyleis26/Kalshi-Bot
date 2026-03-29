"""
sports.py — Kalshi sports market signal strategy.

Compares ESPN win-probability estimates to the current Kalshi YES price.
If there's meaningful edge (ESPN probability diverges from Kalshi price by
at least SPORTS_EDGE_MIN), returns a direction to trade.

Decision logic:
  - BUY YES  (LONG):  ESPN win probability >> Kalshi YES price
  - BUY NO  (SHORT):  ESPN win probability << Kalshi YES price
  - NO TRADE:         Edge too small, no ESPN game found, or near-fair filter fails

Kalshi sports markets ask:
  "Will the LA Dodgers win against the Chicago Cubs?"
  "Will the Boston Celtics beat the New York Knicks?"
  "Will the Boston Bruins win?"

We match the Kalshi market title to ESPN game data using team name matching,
then extract the win probability for the favoured team.
"""

import logging
from data.sports import get_games, find_matching_game
from administration.config import (
    CONTRACT_PRICE_MIN, CONTRACT_PRICE_MAX, SPORTS_EDGE_MIN,
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
        title = market.get("title", market.get("subtitle", ""))
        yes_ask = float(market.get("yes_ask_dollars", 0.5))

        # Near-fair filter — same as crypto
        if not (CONTRACT_PRICE_MIN <= yes_ask <= CONTRACT_PRICE_MAX):
            return _no_trade(f"market too confident (YES={yes_ask:.2f})", yes_ask, sport_label, title)

        # Fetch today's ESPN games for this sport
        games = get_games(espn_sport)
        if not games:
            return _no_trade("no ESPN games found today", yes_ask, sport_label, title)

        # Match the Kalshi market to a specific ESPN game
        game = find_matching_game(games, title)
        if not game:
            return _no_trade(f"no ESPN game matched to '{title[:60]}'", yes_ask, sport_label, title)

        # Determine which team the Kalshi market is asking about (the YES outcome).
        # Kalshi titles usually start with "Will [team] win..." — we find which team
        # appears first in the title and treat that as the YES team.
        yes_team_win_pct = _resolve_yes_team_probability(game, title)
        if yes_team_win_pct is None:
            return _no_trade(f"could not resolve YES team from '{title[:60]}'", yes_ask, sport_label, title)

        edge = yes_team_win_pct - yes_ask
        market_label = _build_label(sport_label, title, game)

        if edge >= SPORTS_EDGE_MIN:
            direction = LONG
            reason = (
                f"ESPN edge={edge:+.2f} "
                f"({game['home_abbr']} vs {game['away_abbr']}, "
                f"ESPN={yes_team_win_pct:.2f} Kalshi={yes_ask:.2f}) — buying YES"
            )
        elif edge <= -SPORTS_EDGE_MIN:
            direction = SHORT
            reason = (
                f"ESPN edge={edge:+.2f} "
                f"({game['home_abbr']} vs {game['away_abbr']}, "
                f"ESPN={yes_team_win_pct:.2f} Kalshi={yes_ask:.2f}) — buying NO"
            )
        else:
            direction = NONE
            reason = (
                f"edge too small: {edge:+.2f} < ±{SPORTS_EDGE_MIN} "
                f"(ESPN={yes_team_win_pct:.2f} Kalshi={yes_ask:.2f})"
            )

        logger.info(f"Sports [{sport_label}]: {reason}")

        return {
            "direction":     direction,
            "confidence":    round(abs(edge), 4),
            "external_prob": round(yes_team_win_pct, 4),
            "kalshi_yes":    round(yes_ask, 4),
            "edge":          round(edge, 4),
            "reason":        reason,
            "market_label":  market_label,
            # Crypto-compatible empty fields for trade log
            "rsi": 0, "macd": 0, "momentum": 0, "vwap": 0, "price": 0,
            "rsi_bias": None, "macd_bias": None, "momentum_bias": None, "vwap_bias": None,
        }


# ---------------------------------------------------------------------- #
#  Helpers                                                                 #
# ---------------------------------------------------------------------- #

def _resolve_yes_team_probability(game: dict, title: str) -> float | None:
    """
    Determine which team is the YES outcome in the Kalshi market title,
    then return ESPN's win probability for that team.

    Kalshi titles: "Will the LA Dodgers beat the Cubs?" → YES = Dodgers win
    If we can't determine YES team, return 0.5 (neutral) so the near-fair
    filter naturally blocks the trade.
    """
    title_lower = title.lower()

    home_abbr = game["home_abbr"].lower()
    away_abbr = game["away_abbr"].lower()
    home_words = [w.lower() for w in game["home_team"].split() if len(w) > 2]
    away_words = [w.lower() for w in game["away_team"].split() if len(w) > 2]

    # Score each team by how prominent they are in the title (first mention wins)
    home_pos = _first_mention(title_lower, [home_abbr] + home_words)
    away_pos = _first_mention(title_lower, [away_abbr] + away_words)

    if home_pos is None and away_pos is None:
        return None

    # The team mentioned first is typically the YES team ("Will [team] win...")
    if home_pos is None:
        yes_is_home = False
    elif away_pos is None:
        yes_is_home = True
    else:
        yes_is_home = (home_pos <= away_pos)

    return game["home_win_pct"] if yes_is_home else game["away_win_pct"]


def _first_mention(text: str, tokens: list) -> int | None:
    """Return the character index of the first token found in text, or None."""
    positions = []
    for tok in tokens:
        idx = text.find(tok)
        if idx != -1:
            positions.append(idx)
    return min(positions) if positions else None


def _build_label(sport_label: str, title: str, game: dict) -> str:
    """
    Build a short Discord-friendly label like "MLB: Cubs to WIN"
    from the market title and matched ESPN game.
    """
    import re
    # Try to extract team name from title: "Will [team] win/beat..."
    match = re.search(
        r"will (?:the )?(.+?)\s+(?:win|beat|defeat|cover)",
        title, re.IGNORECASE
    )
    if match:
        team = match.group(1).strip()
        if len(team) <= 30:
            return f"{sport_label}: {team} to WIN"

    # Fallback: use abbreviations
    return f"{sport_label}: {game['home_abbr']} vs {game['away_abbr']}"


def _no_trade(reason: str, yes_ask: float, sport_label: str, title: str) -> dict:
    logger.info(f"Sports [{sport_label}]: skip — {reason}")
    return {
        "direction":     NONE,
        "confidence":    0.0,
        "external_prob": 0.0,
        "kalshi_yes":    round(yes_ask, 4),
        "edge":          0.0,
        "reason":        reason,
        "market_label":  sport_label or "Sports",
        "rsi": 0, "macd": 0, "momentum": 0, "vwap": 0, "price": 0,
        "rsi_bias": None, "macd_bias": None, "momentum_bias": None, "vwap_bias": None,
    }
