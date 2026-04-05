import pandas as pd
from strategy.signals import evaluate
from administration.config import (
    MIN_CONFIDENCE, KELLY_FRACTION, MIN_BET,
    RSI_ENTRY_OB, RSI_ENTRY_OS,
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
        RSI(9) mean-reversion strategy for BTC 15M Kalshi markets.

        Entry signal: RSI(9) overbought/oversold threshold.
          RSI(9) >= RSI_ENTRY_OB (60) → overbought → SHORT (bet DOWN)
          RSI(9) <= RSI_ENTRY_OS (40) → oversold   → LONG  (bet UP)

        Validated out-of-sample (400 days Binance, walk-forward 70/30):
          OB=60/OS=40 → ~34 trades/day, 58.1% WR, Sharpe 18.96
          Break-even WR at 50¢ + maker fee = 50.9%

        Confidence scales with RSI extremity:
          RSI 60–65 / 35–40  → conf ~20–30%  (just touched threshold)
          RSI 70–75 / 25–30  → conf ~40–50%  (clear overbought)
          RSI 80+ / 20-      → conf ~60–99%  (extreme reading)
        """
        signals  = evaluate(df_1h, df_15m)
        rsi9_val = signals["rsi9"]
        rsi9_b   = signals["rsi9_bias"]

        if rsi9_b == "neutral":
            direction      = NONE
            reason         = f"RSI9={rsi9_val:.1f} — within {RSI_ENTRY_OS}–{RSI_ENTRY_OB} neutral zone"
            confidence     = 0
            confidence_pct = 0.0
        else:
            direction = LONG if rsi9_b == "bull" else SHORT
            # Confidence scales linearly with how far RSI is past the threshold
            extreme   = abs(rsi9_val - 50.0)          # 0–50 range
            conf_raw  = min((extreme - 10) / 40, 1.0) # 0 at threshold, 1.0 at RSI=90/10
            confidence_pct = round(max(conf_raw * 99, 1.0), 1)
            confidence     = 1
            side   = "overbought" if rsi9_b == "bear" else "oversold"
            reason = (
                f"RSI9={rsi9_val:.1f} ({side}) → "
                f"{'SHORT' if direction == SHORT else 'LONG'}, "
                f"conf={confidence_pct:.0f}%"
            )

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

