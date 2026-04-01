import pandas as pd
from strategy.signals import evaluate
from administration.config import (
    MIN_CONFIDENCE, KELLY_FRACTION, MIN_BET, FORCE_TRADE, NEWS_ENABLED,
    FUNDING_RATE_BULL_THRESHOLD, FUNDING_RATE_BEAR_THRESHOLD,
    FNG_BULL_MAX, FNG_BEAR_MIN,
    EQUITY_TREND_ENABLED, EQUITY_TREND_THRESHOLD, EQUITY_LOOKBACK_BARS,
)
from administration.logger import log_signal
from administration.news import NewsContext
from data.funding import get_funding_rate, get_funding_bias
from data.fng import get_fng
from data.equity import get_equity_trend


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
        Evaluate all 4 signals and return a trade decision.

        Args:
            asset: "BTC" or "ETH" — used to fetch the correct funding rate symbol.

        Returns:
            {
                "direction": 'long' | 'short' | 'none',
                "confidence": int (0-8 signals agreeing),
                "signals": dict (full signal snapshot),
                "reason": str
            }
        """
        signals = evaluate(df_1h, df_15m)

        # Core technical signals (5 votes)
        biases = [
            signals["rsi_bias"],      # RSI slope: rising = bull, falling = bear
            signals["macd_bias"],     # MACD histogram direction
            signals["momentum_bias"], # 15M price momentum
            signals["vwap_bias"],     # Mean-reversion: above VWAP = bear, below = bull
            signals["bb_bias"],       # Bollinger Bands: at upper = bear, at lower = bull
        ]

        # Funding rate: contrarian signal — crowded positioning reverses (per-asset)
        funding_data = get_funding_rate(f"{asset}USDT")
        funding_b = (
            get_funding_bias(
                funding_data["funding_rate"],
                FUNDING_RATE_BULL_THRESHOLD,
                FUNDING_RATE_BEAR_THRESHOLD,
            )
            if funding_data else "neutral"
        )

        # Fear & Greed: contrarian signal — extreme readings only
        fng_data = get_fng()
        if fng_data:
            fv = fng_data["value"]
            fng_b = "bull" if fv <= FNG_BULL_MAX else ("bear" if fv >= FNG_BEAR_MIN else "neutral")
        else:
            fng_b = "neutral"

        # Equity futures: direct (non-contrarian) macro regime signal
        equity_data = None
        equity_b    = "neutral"
        if EQUITY_TREND_ENABLED:
            equity_data = get_equity_trend(EQUITY_TREND_THRESHOLD, EQUITY_LOOKBACK_BARS)
            if equity_data:
                equity_b = equity_data["bias"]

        # Add 3 extra votes (8 total); majority still determines direction
        biases += [funding_b, fng_b, equity_b]

        num_votes  = len(biases)
        bull_count = biases.count("bull")
        bear_count = biases.count("bear")

        eq_tag = ""
        if equity_data:
            eq_tag = f" eq={equity_b}({equity_data['change_pct']:+.2%}/{equity_data['lookback_min']}m)"
        extra_tag = f" [fund={funding_b} fng={fng_b}({fng_data['value'] if fng_data else '?'}){eq_tag}]"

        if FORCE_TRADE:
            # Majority vote across 8 signals; tiebreaker when bull == bear
            if bull_count > bear_count:
                direction = LONG
                reason = f"Force/majority — bull={bull_count} bear={bear_count}{extra_tag}"
            elif bear_count > bull_count:
                direction = SHORT
                reason = f"Force/majority — bull={bull_count} bear={bear_count}{extra_tag}"
            else:
                # 3-3 tie: use momentum as tiebreaker (most reactive signal)
                if signals["momentum_bias"] == "bull":
                    direction = LONG
                    reason = f"Force/tie — momentum tiebreaker bull{extra_tag}"
                elif signals["momentum_bias"] == "bear":
                    direction = SHORT
                    reason = f"Force/tie — momentum tiebreaker bear{extra_tag}"
                else:
                    # Final fallback: VWAP side
                    vwap_side = signals["vwap_bias"]
                    direction = LONG if vwap_side == "bull" else SHORT
                    reason = f"Force/tie — VWAP fallback ({vwap_side}){extra_tag}"
        elif bull_count >= MIN_CONFIDENCE:
            direction = LONG
            reason = f"Confluence — {bull_count}/{num_votes} bullish{extra_tag}"
        elif bear_count >= MIN_CONFIDENCE:
            direction = SHORT
            reason = f"Confluence — {bear_count}/{num_votes} bearish{extra_tag}"
        else:
            direction = NONE
            reason = f"No confluence — bull={bull_count} bear={bear_count}{extra_tag}"

        # News filter: block trades where high-confidence news directly
        # contradicts the technical direction. Medium/neutral news is logged only.
        news = None
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
            "direction":      direction,
            "confidence":     max(bull_count, bear_count),
            "confidence_pct": round(max(bull_count, bear_count) / num_votes * 100, 1),
            "signals":        signals,
            "reason":         reason,
            "bull_votes":     bull_count,
            "bear_votes":     bear_count,
            "funding_rate":   funding_data["funding_rate"] if funding_data else None,
            "fng_value":      fng_data["value"] if fng_data else None,
            "news_bias":      news["bias"] if news else None,
            "news_score":     news["score"] if news else None,
            "equity_bias":    equity_data["bias"]       if equity_data else None,
            "equity_change":  equity_data["change_pct"] if equity_data else None,
        }

    # ------------------------------------------------------------------ #
    #  Position Sizing                                                     #
    # ------------------------------------------------------------------ #

    def size(self, confidence: int = 3) -> float:
        """Returns MIN_BET floor ($3.00). Used by backtest only — paper.py uses dynamic capital-based sizing."""
        return MIN_BET

