"""
kalshi_market_log.py — BTC Kalshi market observer.

Logs every BTC 15-minute market window to a CSV regardless of whether a
trade is placed. This builds a historical record of real Kalshi opening
prices so we can:

  1. Answer "how often does YES open between 45-55 cents?"
  2. Backtest with actual entry prices instead of a hardcoded $0.50
  3. Measure the relationship between opening price and outcome
  4. Validate strategy win rates against real market data

CSV: data/storage/btc_kalshi_markets.csv

Columns:
  window        — UTC window start time (e.g. "2026-04-04 15:30")
  ticker        — Kalshi market ticker (e.g. "KXBTC15M-26APR0415")
  yes_ask       — YES ask price at observation time (0.01 – 0.99)
  no_ask        — NO ask price at observation time
  btc_price     — BTC/USD spot price (from Kraken) at observation time
  traded        — 1 if the bot placed a trade this window, 0 otherwise
  direction     — 'long' / 'short' / '' if not traded
  outcome       — 'win' / 'loss' / '' — filled in when trade resolves
  btc_open      — BTC price at start of window (filled at close)
  btc_close     — BTC price at end of window (filled at close)
  move_pct      — (btc_close - btc_open) / btc_open (filled at close)

Outcomes for untraded windows are computed post-hoc via join with Kraken
15m OHLCV data using the window timestamp as the key.
"""

import os
import threading
import pandas as pd
from datetime import datetime, timezone

STORAGE_DIR  = os.path.join(os.path.dirname(__file__), "storage")
MARKETS_FILE = os.path.join(STORAGE_DIR, "btc_kalshi_markets.csv")

COLUMNS = [
    "window", "ticker", "yes_ask", "no_ask", "btc_price",
    "traded", "direction", "outcome", "btc_open", "btc_close", "move_pct",
]

_lock = threading.Lock()


def _load() -> pd.DataFrame:
    if not os.path.exists(MARKETS_FILE):
        return pd.DataFrame(columns=COLUMNS)
    return pd.read_csv(MARKETS_FILE)


def _save(df: pd.DataFrame):
    os.makedirs(STORAGE_DIR, exist_ok=True)
    df.to_csv(MARKETS_FILE, index=False)


def current_window_str() -> str:
    """Return the current 15-minute window start as 'YYYY-MM-DD HH:MM' UTC."""
    now = datetime.now(timezone.utc)
    minute = now.minute - (now.minute % 15)
    return now.replace(minute=minute, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M")


def log_open(ticker: str, yes_ask: float, no_ask: float, btc_price: float):
    """
    Record the opening observation for the current 15-minute window.
    Called once per window at evaluation time, before the strategy decision.
    If the window is already logged (duplicate poll), this is a no-op.
    """
    window = current_window_str()
    with _lock:
        df = _load()
        if not df.empty and (df["window"] == window).any():
            return   # already logged this window
        row = {col: "" for col in COLUMNS}
        row.update({
            "window":    window,
            "ticker":    ticker or "",
            "yes_ask":   round(yes_ask, 4),
            "no_ask":    round(no_ask, 4),
            "btc_price": round(btc_price, 2),
            "traded":    0,
            "direction": "",
            "outcome":   "",
            "btc_open":  "",
            "btc_close": "",
            "move_pct":  "",
        })
        df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
        _save(df)


def log_trade(window: str, direction: str):
    """Mark that a trade was placed in this window."""
    with _lock:
        df = _load()
        if df.empty:
            return
        mask = df["window"] == window
        if not mask.any():
            return
        df.loc[mask, "traded"]    = 1
        df.loc[mask, "direction"] = direction
        _save(df)


def log_outcome(window: str, result: str, btc_open: float, btc_close: float):
    """
    Record the trade outcome and final BTC prices for a window.
    Called when a trade resolves (win/loss).
    """
    move = round((btc_close - btc_open) / btc_open, 6) if btc_open else ""
    with _lock:
        df = _load()
        if df.empty:
            return
        mask = df["window"] == window
        if not mask.any():
            return
        df.loc[mask, "outcome"]   = result
        df.loc[mask, "btc_open"]  = round(btc_open, 2)  if btc_open  else ""
        df.loc[mask, "btc_close"] = round(btc_close, 2) if btc_close else ""
        df.loc[mask, "move_pct"]  = move
        _save(df)


def summary() -> dict:
    """Quick stats on collected market data."""
    df = _load()
    if df.empty:
        return {"windows_logged": 0}

    total   = len(df)
    traded  = int((df["traded"] == 1).sum())
    priced  = df[df["yes_ask"] != ""]
    in_band = len(priced[(priced["yes_ask"].astype(float) >= 0.45) &
                         (priced["yes_ask"].astype(float) <= 0.55)])

    return {
        "windows_logged": total,
        "windows_traded": traded,
        "pct_traded":     round(traded / total, 3) if total else 0,
        "pct_in_45_55":   round(in_band / len(priced), 3) if len(priced) else 0,
        "yes_ask_mean":   round(priced["yes_ask"].astype(float).mean(), 4) if len(priced) else 0,
        "yes_ask_median": round(priced["yes_ask"].astype(float).median(), 4) if len(priced) else 0,
    }
