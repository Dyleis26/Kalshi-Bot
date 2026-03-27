import pandas as pd
from strategy.signals import evaluate
from administration.config import MIN_CONFIDENCE, KELLY_FRACTION, MIN_BET, FORCE_TRADE
from administration.logger import log_signal


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
                # 2-2 tie: use momentum as tiebreaker (most reactive), then RSI
                # If neither provides a clear direction, skip — don't guess
                if signals["momentum_bias"] == "bear":
                    direction = SHORT
                    reason = f"Force/tie — mom tiebreaker bear"
                elif signals["momentum_bias"] == "bull":
                    direction = LONG
                    reason = f"Force/tie — mom tiebreaker bull"
                elif signals["rsi_bias"] == "bear":
                    direction = SHORT
                    reason = f"Force/tie — rsi tiebreaker bear"
                elif signals["rsi_bias"] == "bull":
                    direction = LONG
                    reason = f"Force/tie — rsi tiebreaker bull"
                else:
                    direction = NONE   # All signals flat — skip rather than guess
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

