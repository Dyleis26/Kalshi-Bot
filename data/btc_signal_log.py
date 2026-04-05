"""
btc_signal_log.py — BTC 15-minute signal history logger.

Records every BTC window evaluation to a CSV regardless of whether a trade
fires. This builds a complete signal history for strategy validation:

  1. Measure true win-rate of streak signal (not just traded windows)
  2. Check MACD filter effectiveness — how many signals does it block?
  3. Validate RSI/momentum values at trade vs no-trade windows
  4. Post-hoc label outcomes by joining with BTCUSD_15m.csv on window

CSV: data/storage/btc_signal_history.csv

Columns:
  window         — UTC window start time "YYYY-MM-DD HH:MM"
  streak_bias    — bull / bear / neutral
  macd_bias      — bull / bear / neutral
  rsi            — RSI value (float)
  macd           — MACD histogram value (float)
  momentum       — momentum value (float)
  vwap_diff      — price - VWAP (float)
  direction      — LONG / SHORT / NONE (what the strategy decided)
  traded         — 1 if a trade was placed this window, 0 otherwise
  confidence_pct — 0 / 50 / 100
  reason         — strategy reason string
"""

import csv
import os
import threading
from datetime import datetime, timezone

STORAGE_DIR  = os.path.join(os.path.dirname(__file__), "storage")
SIGNAL_FILE  = os.path.join(STORAGE_DIR, "btc_signal_history.csv")

COLUMNS = [
    "window", "streak_bias", "macd_bias",
    "rsi", "macd", "momentum", "vwap_diff",
    "direction", "traded", "confidence_pct", "reason",
]

_lock = threading.Lock()
_last_logged_window: str = ""   # in-memory dedupe — avoids reading entire CSV each window


def current_window() -> str:
    now = datetime.now(timezone.utc)
    minute = now.minute - (now.minute % 15)
    return now.replace(minute=minute, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M")


def _ensure_header():
    if not os.path.exists(SIGNAL_FILE):
        os.makedirs(STORAGE_DIR, exist_ok=True)
        with open(SIGNAL_FILE, "w", newline="") as f:
            csv.writer(f).writerow(COLUMNS)


def log_window(decision: dict, traded: bool = False):
    """
    Record a BTC window evaluation.

    decision — the dict returned by strategy.decide()
    traded   — True if a trade was placed this window
    """
    global _last_logged_window
    try:
        win     = current_window()
        signals = decision.get("signals", {})
        price   = float(signals.get("price", 0) or 0)
        vwap    = float(signals.get("vwap", 0) or 0)
        row = {
            "window":         win,
            "streak_bias":    signals.get("streak_bias", ""),
            "macd_bias":      signals.get("macd_bias", ""),
            "rsi":            round(float(signals.get("rsi", 0) or 0), 4),
            "macd":           round(float(signals.get("macd", 0) or 0), 6),
            "momentum":       round(float(signals.get("momentum", 0) or 0), 6),
            "vwap_diff":      round(price - vwap, 4) if price and vwap else "",
            "direction":      decision.get("direction", ""),
            "traded":         1 if traded else 0,
            "confidence_pct": decision.get("confidence_pct", 0),
            "reason":         decision.get("reason", ""),
        }
        with _lock:
            if win == _last_logged_window:
                return   # already logged this window — no file I/O needed
            _ensure_header()
            with open(SIGNAL_FILE, "a", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=COLUMNS)
                writer.writerow(row)
            _last_logged_window = win
    except Exception:
        pass


def mark_traded(window: str):
    """Update the traded flag for a window after a trade fires."""
    try:
        with _lock:
            if not os.path.exists(SIGNAL_FILE):
                return
            rows = []
            updated = False
            with open(SIGNAL_FILE, "r", newline="") as f:
                reader = csv.DictReader(f)
                for r in reader:
                    if r["window"] == window and r["traded"] == "0":
                        r["traded"] = "1"
                        updated = True
                    rows.append(r)
            if updated:
                with open(SIGNAL_FILE, "w", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=COLUMNS)
                    writer.writeheader()
                    writer.writerows(rows)
    except Exception:
        pass
