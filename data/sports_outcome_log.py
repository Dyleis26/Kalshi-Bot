"""
sports_outcome_log.py — Sports prediction accuracy tracker.

Records every game evaluated by the sports strategy regardless of whether
a trade fires. This builds a ground-truth dataset for measuring model
accuracy:

  1. How accurate is our external probability vs actual outcome?
  2. What edge sizes actually convert vs miss?
  3. Are we better pre-game or in-game?
  4. Which sports/sources produce the most reliable edge?

CSV: data/storage/sports_predictions.csv

Columns:
  timestamp      — UTC time of evaluation "YYYY-MM-DD HH:MM:SS"
  sport          — MLB / NBA / NHL
  game_id        — ESPN game ID
  title          — Kalshi market title
  ticker         — Kalshi market ticker
  is_live        — 1 if in-game, 0 if pre-game
  period         — current period / inning
  home_score     — home team score at eval time
  away_score     — away team score at eval time
  yes_team       — which team YES pays out on
  model_prob     — our model's win probability for YES team
  kalshi_yes     — Kalshi YES ask price
  edge           — model_prob - kalshi_yes
  direction      — LONG / SHORT / NONE
  traded         — 1 if trade placed, 0 otherwise
  trade_result   — win / loss / open / '' (filled at settlement)
  pnl            — PnL at settlement (filled when known)
  prob_source    — OddsAPI / ESPN / H2H etc
"""

import csv
import os
import threading
from datetime import datetime, timezone

STORAGE_DIR    = os.path.join(os.path.dirname(__file__), "storage")
OUTCOMES_FILE  = os.path.join(STORAGE_DIR, "sports_predictions.csv")

COLUMNS = [
    "timestamp", "sport", "game_id", "title", "ticker", "is_live",
    "period", "home_score", "away_score", "yes_team",
    "model_prob", "kalshi_yes", "edge", "direction", "traded",
    "confidence_pct", "vote_score", "vote_detail", "confidence_tier",
    "home_record", "away_record", "home_l10", "away_l10", "h2h_series",
    "trade_result", "pnl", "prob_source",
]

_lock = threading.Lock()


def _ensure_header():
    if not os.path.exists(OUTCOMES_FILE):
        os.makedirs(STORAGE_DIR, exist_ok=True)
        with open(OUTCOMES_FILE, "w", newline="") as f:
            csv.writer(f).writerow(COLUMNS)


def log_evaluation(
    sport: str,
    game: dict,
    ticker: str,
    title: str,
    yes_team: str,
    model_prob: float,
    kalshi_yes: float,
    edge: float,
    direction: str,
    traded: bool,
    prob_source: str = "",
    confidence_pct: float = 0.0,
    vote_score: int = 0,
    vote_detail: str = "",
    confidence_tier: str = "",
    home_record: str = "",
    away_record: str = "",
    home_l10: str = "",
    away_l10: str = "",
    h2h_series: str = "",
):
    """
    Record one game evaluation. Called by sports.py for every game that
    passes the initial filters (ticker match, price range, ESPN match).

    game — the dict from data/sports.py (has game_id, period, home_score,
           away_score, score_diff, is_live, etc.)
    """
    try:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        row = {
            "timestamp":       now,
            "sport":           sport,
            "game_id":         game.get("game_id", ""),
            "title":           title[:80],
            "ticker":          ticker,
            "is_live":         1 if game.get("is_live") else 0,
            "period":          game.get("period", ""),
            "home_score":      game.get("home_score", ""),
            "away_score":      game.get("away_score", ""),
            "yes_team":        yes_team[:40] if yes_team else "",
            "model_prob":      round(float(model_prob), 4) if model_prob is not None else "",
            "kalshi_yes":      round(float(kalshi_yes), 4),
            "edge":            round(float(edge), 4),
            "direction":       direction,
            "traded":          1 if traded else 0,
            "confidence_pct":  round(float(confidence_pct), 1),
            "vote_score":      vote_score,
            "vote_detail":     vote_detail[:80] if vote_detail else "",
            "confidence_tier": confidence_tier,
            "home_record":     home_record,
            "away_record":     away_record,
            "home_l10":        home_l10,
            "away_l10":        away_l10,
            "h2h_series":      h2h_series[:60] if h2h_series else "",
            "trade_result":    "",
            "pnl":             "",
            "prob_source":     prob_source[:30] if prob_source else "",
        }
        with _lock:
            _ensure_header()
            with open(OUTCOMES_FILE, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=COLUMNS)
                writer.writerow(row)
    except Exception:
        pass


def update_result(ticker: str, result: str, pnl: float, game_id: str = ""):
    """
    Update trade_result and pnl for a resolved trade.
    Matches on ticker (most-recent unresolved row). game_id is optional.
    """
    try:
        with _lock:
            if not os.path.exists(OUTCOMES_FILE):
                return
            with open(OUTCOMES_FILE, "r", newline="") as f:
                reader = csv.DictReader(f)
                all_rows = list(reader)

            # Update the last unresolved row for this ticker
            updated = False
            for r in reversed(all_rows):
                if r["ticker"] == ticker and r["trade_result"] in ("", "open"):
                    r["traded"] = "1"
                    r["trade_result"] = result
                    r["pnl"] = round(float(pnl), 2) if result not in ("open", "") else ""
                    updated = True
                    break

            if updated:
                with open(OUTCOMES_FILE, "w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=COLUMNS)
                    writer.writeheader()
                    writer.writerows(all_rows)
    except Exception:
        pass
