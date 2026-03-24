import pandas as pd
from strategy.signals import evaluate
from administration.config import MIN_CONFIDENCE, KELLY_FRACTION, MAX_BET, MIN_BET
from administration.logger import log_signal


# Trade directions
LONG  = "long"   # Buy YES (price goes Up)
SHORT = "short"  # Buy NO  (price goes Down)
NONE  = "none"   # No trade


class Strategy:
    def __init__(self, win_rate: float = 0.57):
        """
        win_rate: assumed win probability used for Kelly sizing.
        Start conservative at 57% — update after backtesting.
        """
        self.win_rate = win_rate

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
            signals["vwap_bias"],
        ]

        bull_count = biases.count("bull")
        bear_count = biases.count("bear")

        if bull_count == MIN_CONFIDENCE:
            direction = LONG
            reason = "All 4 signals bullish"
        elif bear_count == MIN_CONFIDENCE:
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

    def size(self, capital: float = None, contract_price: float = None, size_multiplier: float = 1.0) -> float:
        """
        Flat $10 per trade during testing.
        Kelly sizing can be enabled later once real win rate is established.
        """
        return MIN_BET

    # ------------------------------------------------------------------ #
    #  Contract Direction                                                  #
    # ------------------------------------------------------------------ #

    def contract_side(self, direction: str) -> str:
        """
        Map trade direction to Kalshi contract side.
        LONG  → buy YES on the "Up" market
        SHORT → buy YES on the "Down" market (or buy NO on "Up")
        """
        if direction == LONG:
            return "yes"
        elif direction == SHORT:
            return "no"
        return None
