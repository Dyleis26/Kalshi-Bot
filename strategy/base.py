import pandas as pd
from strategy.signals import evaluate
from administration.config import (
    MIN_CONFIDENCE, KELLY_FRACTION, MIN_BET,
    STREAK_MACD_CONFIRM,
)
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

    def decide(self, df_1h: pd.DataFrame, df_15m: pd.DataFrame, asset: str = "BTC") -> dict:
        """
        Streak mean-reversion strategy for BTC 15M Kalshi markets.

        Core signal: after N consecutive closes in one direction, bet on the
        opposite (mean reversion). Validated out-of-sample: 68% WR at N=2.

        Optional MACD confirmation (STREAK_MACD_CONFIRM=True): only trade
        when MACD histogram also agrees with the mean-reversion direction.
        Validated out-of-sample: 79% WR when both streak and MACD agree,
        at reduced frequency (~11% of windows vs ~33%).

        Confidence levels:
          2/2 (streak + MACD agree)  → HIGH confidence (100%)
          1/1 (streak only, no MACD) → BASE confidence (50%)
        """
        signals = evaluate(df_1h, df_15m)
        streak_b = signals["streak_bias"]
        macd_b   = signals["macd_bias"]

        if streak_b == "neutral":
            direction = NONE
            reason    = f"No streak — macd={macd_b}"
            confidence     = 0
            confidence_pct = 0.0
        else:
            # MACD confirms when it agrees with the mean-reversion direction
            macd_confirms = (macd_b == streak_b)

            if STREAK_MACD_CONFIRM and not macd_confirms:
                direction      = NONE
                reason         = f"Streak={streak_b} but MACD={macd_b} — no confirm"
                confidence     = 1
                confidence_pct = 50.0
            else:
                direction      = LONG if streak_b == "bull" else SHORT
                conf_label     = "streak+macd" if macd_confirms else "streak"
                closes         = df_15m["close"].values
                n_consec       = _count_streak(closes)
                reason         = (
                    f"{conf_label} — {n_consec} consecutive "
                    f"{'drops' if streak_b == 'bull' else 'rises'}, "
                    f"macd={macd_b}"
                )
                confidence     = 2 if macd_confirms else 1
                confidence_pct = 100.0 if macd_confirms else 50.0

        log_signal(
            rsi=signals["rsi"],
            macd=signals["macd"],
            momentum=signals["momentum"],
            vwap_diff=signals["price"] - signals["vwap"],
            decision=direction
        )

        bull_count = 1 if direction == LONG  else 0
        bear_count = 1 if direction == SHORT else 0

        return {
            "direction":      direction,
            "confidence":     confidence,
            "confidence_pct": confidence_pct,
            "signals":        signals,
            "reason":         reason,
            "bull_votes":     bull_count,
            "bear_votes":     bear_count,
            "funding_rate":   None,
            "fng_value":      None,
            "news_bias":      None,
            "news_score":     None,
            "equity_bias":    None,
            "equity_change":  None,
        }


def _count_streak(closes) -> int:
    """Count how many consecutive candles moved in the same direction as the most recent."""
    if len(closes) < 2:
        return 0
    going_up = closes[-1] > closes[-2]
    count = 1
    for i in range(len(closes)-2, 0, -1):
        if (closes[i] > closes[i-1]) == going_up:
            count += 1
        else:
            break
    return count

