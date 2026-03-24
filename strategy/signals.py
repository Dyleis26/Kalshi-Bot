import numpy as np
import pandas as pd
import administration.config as cfg
from administration.config import RSI_PERIOD


# ------------------------------------------------------------------ #
#  RSI                                                                 #
# ------------------------------------------------------------------ #

def rsi(df: pd.DataFrame, period: int = RSI_PERIOD) -> float:
    """
    Calculate RSI on the 'close' column.
    Returns the latest RSI value.
    """
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(com=period - 1, min_periods=period).mean()
    avg_loss = loss.ewm(com=period - 1, min_periods=period).mean()

    rs = avg_gain / avg_loss.replace(0, np.nan)
    rsi_series = 100 - (100 / (1 + rs))
    val = float(rsi_series.iloc[-1])
    return round(val if not np.isnan(val) else 50.0, 4)  # 50 = neutral on insufficient data


def rsi_bias(rsi_val: float) -> str:
    """
    Returns 'bull', 'bear', or 'neutral' based on 1H RSI value.
    Neutral = no trade zone.
    """
    if rsi_val > cfg.RSI_BULL:
        return "bull"
    elif rsi_val < cfg.RSI_BEAR:
        return "bear"
    return "neutral"


# ------------------------------------------------------------------ #
#  MACD                                                                #
# ------------------------------------------------------------------ #

def macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9) -> float:
    """
    Calculate MACD histogram on the 'close' column.
    Returns the latest histogram value (positive = bullish, negative = bearish).
    """
    ema_fast = df["close"].ewm(span=fast, adjust=False).mean()
    ema_slow = df["close"].ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return round(float(histogram.iloc[-1]), 6)


def macd_bias(histogram: float) -> str:
    """Returns 'bull', 'bear' based on MACD histogram direction."""
    if histogram > 0:
        return "bull"
    elif histogram < 0:
        return "bear"
    return "neutral"


# ------------------------------------------------------------------ #
#  Momentum                                                            #
# ------------------------------------------------------------------ #

def momentum(df: pd.DataFrame, lookback: int = 2) -> float:
    """
    Price % change over the last N 15M candles.
    Returns positive for upward momentum, negative for downward.
    """
    if len(df) < lookback + 1:
        return 0.0
    start = df["close"].iloc[-(lookback + 1)]
    end = df["close"].iloc[-1]
    return round((end - start) / start, 6)


def momentum_bias(mom: float) -> str:
    """Returns 'bull', 'bear', or 'neutral' based on momentum threshold."""
    if mom >= cfg.MOMENTUM_MIN:
        return "bull"
    elif mom <= -cfg.MOMENTUM_MIN:
        return "bear"
    return "neutral"


# ------------------------------------------------------------------ #
#  VWAP                                                                #
# ------------------------------------------------------------------ #

def vwap(df: pd.DataFrame) -> float:
    """
    Calculate intraday VWAP using typical price × volume.
    Returns the latest VWAP value.
    """
    typical = (df["high"] + df["low"] + df["close"]) / 3
    cumulative_vol = df["volume"].cumsum()
    cumulative_tp_vol = (typical * df["volume"]).cumsum()
    vwap_series = cumulative_tp_vol / cumulative_vol.replace(0, np.nan)
    return round(float(vwap_series.iloc[-1]), 4)


def vwap_bias(current_price: float, vwap_val: float) -> str:
    """Returns 'bull' if price is above VWAP, 'bear' if below."""
    if current_price > vwap_val:
        return "bull"
    elif current_price < vwap_val:
        return "bear"
    return "neutral"


# ------------------------------------------------------------------ #
#  All Signals                                                         #
# ------------------------------------------------------------------ #

def evaluate(df_1h: pd.DataFrame, df_15m: pd.DataFrame) -> dict:
    """
    Run all 4 signals and return a full snapshot.

    Returns:
        {
            "rsi":       float,
            "macd":      float,
            "momentum":  float,
            "vwap":      float,
            "price":     float,
            "rsi_bias":      'bull'|'bear'|'neutral',
            "macd_bias":     'bull'|'bear'|'neutral',
            "momentum_bias": 'bull'|'bear'|'neutral',
            "vwap_bias":     'bull'|'bear'|'neutral',
        }
    """
    rsi_val = rsi(df_1h)
    macd_val = macd(df_1h)
    mom_val = momentum(df_15m)
    vwap_val = vwap(df_15m)
    price = float(df_15m["close"].iloc[-1])

    return {
        "rsi":           rsi_val,
        "macd":          macd_val,
        "momentum":      mom_val,
        "vwap":          vwap_val,
        "price":         price,
        "rsi_bias":      rsi_bias(rsi_val),
        "macd_bias":     macd_bias(macd_val),
        "momentum_bias": momentum_bias(mom_val),
        "vwap_bias":     vwap_bias(price, vwap_val),
    }
