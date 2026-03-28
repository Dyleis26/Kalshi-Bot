import pandas as pd
from strategy.signals import evaluate
from administration.config import MIN_CONFIDENCE, KELLY_FRACTION, MIN_BET, FORCE_TRADE, VWAP_MIN_DISTANCE_PCT, NEWS_ENABLED
from administration.logger import log_signal
from administration.news import NewsContext


# Trade directions
LONG  = "long"   # Buy YES (price goes Up)
SHORT = "short"  # Buy NO  (price goes Down)
NONE  = "none"   # No trade


class Strategy:
    def __init__(self):
        pass

    # ------------------------------------------------------------------ #
    #  Entry Decision                                                      #
    # ------------------------------------------------------------------ #

    def decide(self, df_1h: pd.DataFrame, df_15m: pd.DataFrame) -> dict:
        """
        Evaluate all 4 signals and return a trade decision.

        Returns:
            {
                "direction": 'long' | 'short' | 'none',
                "confidence": int (0-4 signals agreeing),
                "signals": dict (full signal snapshot),
                "reason": str
            }
        """
        signals = evaluate(df_1h, df_15m)
        biases = [
            signals["rsi_bias"],
            signals["macd_bias"],
            signals["momentum_bias"],
            signals["vwap_bias"],   # All 4 signals get equal weight
        ]

        bull_count = biases.count("bull")
        bear_count = biases.count("bear")

        if FORCE_TRADE:
            # Majority vote: need strict majority (3-1 or 4-0), or a clear tiebreaker on 2-2
            if bull_count > bear_count:
                direction = LONG
                reason = f"Force/majority — bull={bull_count} bear={bear_count}"
            elif bear_count > bull_count:
                direction = SHORT
                reason = f"Force/majority — bull={bull_count} bear={bear_count}"
            else:
                # 2-2 tie: use momentum as tiebreaker (most reactive signal).
                # In a 4-signal 2-2 tie all signals must be directional — a neutral
                # momentum would produce a 3-1 split, not a tie — so the bear/bull
                # branches below are exhaustive and the NONE branch is a safety net only.
                if signals["momentum_bias"] == "bull":
                    direction = LONG
                    reason = f"Force/tie — momentum tiebreaker bull"
                elif signals["momentum_bias"] == "bear":
                    direction = SHORT
                    reason = f"Force/tie — momentum tiebreaker bear"
                else:
                    direction = NONE   # Defensive: not reachable with current 4-signal setup
                    reason = f"Force/tie — no tiebreaker (mom={signals['momentum_bias']} rsi={signals['rsi']:.1f})"
        elif bull_count >= MIN_CONFIDENCE:
            direction = LONG
            reason = "All 4 signals bullish"
        elif bear_count >= MIN_CONFIDENCE:
            direction = SHORT
            reason = "All 4 signals bearish"
        else:
            direction = NONE
            reason = f"No confluence — bull={bull_count} bear={bear_count} neutral={biases.count('neutral')}"

        # VWAP filter: direction must align with which side of VWAP price is on,
        # and price must be far enough from VWAP to have a meaningful edge.
        if direction != NONE:
            price = signals["price"]
            vwap  = signals["vwap"]
            vwap_diff = price - vwap
            vwap_distance_pct = abs(vwap_diff) / price if price > 0 else 0
            vwap_aligned = (direction == LONG and vwap_diff > 0) or \
                           (direction == SHORT and vwap_diff < 0)
            if not vwap_aligned or vwap_distance_pct < VWAP_MIN_DISTANCE_PCT:
                direction = NONE
                reason = (f"VWAP filter blocked — distance={vwap_distance_pct:.4f} "
                          f"({'aligned' if vwap_aligned else 'misaligned'})")

        # News filter: block trades where high-confidence news directly
        # contradicts the technical direction. Medium/neutral news is logged only.
        if direction != NONE and NEWS_ENABLED:
            news = NewsContext.load()
            if news:
                news_conflicts = (
                    (direction == LONG  and news["bias"] == "bearish") or
                    (direction == SHORT and news["bias"] == "bullish")
                )
                if news_conflicts and news["confidence"] == "high":
                    direction = NONE
                    reason = (
                        f"News blocked ({news['bias']} high conf, score={news['score']:+d}) "
                        f"— {news['reason'][:80]}"
                    )

        log_signal(
            rsi=signals["rsi"],
            macd=signals["macd"],
            momentum=signals["momentum"],
            vwap_diff=signals["price"] - signals["vwap"],
            decision=direction
        )

        return {
            "direction": direction,
            "confidence": max(bull_count, bear_count),
            "signals": signals,
            "reason": reason,
        }

    # ------------------------------------------------------------------ #
    #  Position Sizing                                                     #
    # ------------------------------------------------------------------ #

    def size(self, confidence: int = 3) -> float:
        """
        Confidence-based sizing:
          4-0 all signals agree  → $15 (MAX_BET)
          3-1 strong majority    → $10 (MIN_BET)
          2-x tiebreaker         → $5
        """
        if confidence >= 4:
            return MIN_BET * 1.5   # $15
        elif confidence == 3:
            return MIN_BET         # $10
        else:
            return MIN_BET * 0.5   # $5

