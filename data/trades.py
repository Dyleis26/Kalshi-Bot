import os
import uuid
import threading
import pandas as pd
from datetime import datetime, timezone

STORAGE_DIR = os.path.join(os.path.dirname(__file__), "storage")
TRADES_FILE = os.path.join(STORAGE_DIR, "trades.csv")

# All columns stored per trade — in order
COLUMNS = [
    # Identity
    "trade_id",           # Unique ID for this trade
    "mode",               # 'paper' or 'live'
    "asset",              # 'BTC', 'ETH', 'SOL', 'XRP', 'DOGE'

    # Entry
    "entry_time",         # UTC timestamp when trade was placed
    "direction",          # 'long' (UP) or 'short' (DOWN)
    "btc_price_entry",    # Asset price at moment of entry

    # Contract details
    "contracts",          # Number of contracts requested
    "contracts_filled",   # Number of contracts actually filled (may be less than requested on live)
    "contract_price_pct", # Contract price as % (e.g. 50.0)
    "cost",               # Total dollar cost including entry fee
    "possible_payout",    # Max payout if win

    # Signal snapshot at entry
    "rsi_1h",             # 1H RSI value at entry
    "macd_1h",            # 1H MACD histogram value at entry
    "momentum_15m",       # 15M momentum (price % change over last 2 candles)
    "vwap_15m",           # 15M VWAP value at entry
    "vwap_diff",          # BTC price minus VWAP at entry (positive = above)
    "rsi_bias",           # 'bull' or 'bear'
    "macd_bias",          # 'bull' or 'bear'
    "momentum_bias",      # 'bull' or 'bear'
    "vwap_bias",          # 'bull' or 'bear'

    # Window candle (the 15M candle the trade resolves on)
    "window_open",        # BTC open price at start of the 15M window
    "window_high",        # BTC high during the 15M window
    "window_low",         # BTC low during the 15M window
    "window_close",       # BTC close price at end of the 15M window
    "window_volume",      # BTC volume during the 15M window
    "window_move_pct",    # % price change during the window (close/open - 1)
    "window_direction",   # 'up' or 'down' — what BTC actually did

    # Execution
    "exit_time",          # UTC timestamp when trade resolved
    "duration_secs",      # Seconds from entry to resolution

    # Result
    "result",             # 'win' or 'loss'
    "pnl",                # Net PnL after all fees
    "fee_paid",           # Total fee paid (entry + exit)

    # Portfolio state after trade
    "capital_after",      # Capital balance after this trade
    "cash_after",         # Cash balance after this trade
    "total_after",        # Total portfolio value after this trade
]


class TradeLog:
    def __init__(self, mode: str = "paper"):
        os.makedirs(STORAGE_DIR, exist_ok=True)
        self.mode = mode
        self._lock = threading.Lock()  # Serialize all CSV reads/writes across threads

    # ------------------------------------------------------------------ #
    #  Public                                                              #
    # ------------------------------------------------------------------ #

    def reset(self):
        """Archive the current trades CSV and start a clean slate for the new session."""
        with self._lock:
            if os.path.exists(TRADES_FILE):
                ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
                archive = os.path.join(STORAGE_DIR, f"trades_{ts}.csv")
                os.rename(TRADES_FILE, archive)

    def open_trade(self, direction: str, contracts: int, contracts_filled: int,
                   contract_price_pct: float,
                   cost: float, possible_payout: float, btc_price: float,
                   signals: dict, asset: str = "BTC") -> str:
        """
        Record a trade entry. Returns a unique trade_id to reference at close.

        Args:
            direction:           'long' or 'short'
            contracts:           number of contracts requested
            contracts_filled:    number of contracts actually filled
            contract_price_pct:  price as percentage (e.g. 50.0)
            cost:                total cost including fees
            possible_payout:     max payout if win
            btc_price:           live BTC price at entry
            signals:             full signal snapshot from strategy.signals.evaluate()
        """
        trade_id = str(uuid.uuid4())[:8]
        entry_time = datetime.now(timezone.utc).isoformat()

        row = {col: None for col in COLUMNS}
        row.update({
            "trade_id":           trade_id,
            "mode":               self.mode,
            "asset":              asset,
            "entry_time":         entry_time,
            "direction":          direction,
            "btc_price_entry":    round(btc_price, 2),
            "contracts":          contracts,
            "contracts_filled":   contracts_filled,
            "contract_price_pct": contract_price_pct,
            "cost":               cost,
            "possible_payout":    possible_payout,
            "rsi_1h":             round(signals.get("rsi", 0), 4),
            "macd_1h":            round(signals.get("macd", 0), 6),
            "momentum_15m":       round(signals.get("momentum", 0), 6),
            "vwap_15m":           round(signals.get("vwap", 0), 2),
            "vwap_diff":          round(signals.get("price", 0) - signals.get("vwap", 0), 2),
            "rsi_bias":           signals.get("rsi_bias"),
            "macd_bias":          signals.get("macd_bias"),
            "momentum_bias":      signals.get("momentum_bias"),
            "vwap_bias":          signals.get("vwap_bias"),
        })

        self._append_row(row)
        return trade_id

    def close_trade(self, trade_id: str, result: str, pnl: float, fee_paid: float,
                    btc_candle: dict, portfolio: dict):
        """
        Update an existing trade record with resolution data.

        Args:
            trade_id:    ID returned by open_trade()
            result:      'win' or 'loss'
            pnl:         net PnL after fees
            fee_paid:    total fees paid
            btc_candle:  the resolved 15M candle dict (open, high, low, close, volume)
            portfolio:   portfolio summary dict after trade
        """
        exit_time = datetime.now(timezone.utc)
        window_open  = btc_candle.get("open", 0)
        window_close = btc_candle.get("close", 0)
        move_pct = round((window_close - window_open) / window_open, 6) if window_open else 0

        updates = {
            "window_open":      round(window_open, 2),
            "window_high":      round(btc_candle.get("high", 0), 2),
            "window_low":       round(btc_candle.get("low", 0), 2),
            "window_close":     round(window_close, 2),
            "window_volume":    round(btc_candle.get("volume", 0), 4),
            "window_move_pct":  move_pct,
            "window_direction": "up" if move_pct >= 0 else "down",
            "exit_time":        exit_time.isoformat(),
            "duration_secs":    None,  # computed below under lock
            "result":           result,
            "pnl":              round(pnl, 4),
            "fee_paid":         round(fee_paid, 4),
            "capital_after":    portfolio.get("capital", 0),
            "cash_after":       portfolio.get("cash", 0),
            "total_after":      portfolio.get("total", 0),
        }

        with self._lock:
            df = self._load()
            if df.empty:
                return
            idx = df.index[df["trade_id"] == trade_id]
            if idx.empty:
                return
            i = idx[0]
            entry_time = pd.to_datetime(df.at[i, "entry_time"], utc=True)
            updates["duration_secs"] = round((exit_time - entry_time).total_seconds())
            for col, val in updates.items():
                df.loc[i, col] = val
            df.to_csv(TRADES_FILE, index=False)

    def load(self) -> pd.DataFrame:
        """Load the full trade history as a DataFrame."""
        return self._load()

    def summary(self) -> dict:
        """Quick stats from trade history."""
        df = self._load()
        if df.empty or "result" not in df.columns:
            return {"total_trades": 0}

        closed = df.dropna(subset=["result"])
        wins   = closed[closed["result"] == "win"]
        losses = closed[closed["result"] == "loss"]

        return {
            "total_trades":  len(closed),
            "wins":          len(wins),
            "losses":        len(losses),
            "win_rate":      round(len(wins) / len(closed), 4) if len(closed) > 0 else 0,
            "total_pnl":     round(closed["pnl"].sum(), 2),
            "avg_win":       round(wins["pnl"].mean(), 2) if not wins.empty else 0,
            "avg_loss":      round(losses["pnl"].mean(), 2) if not losses.empty else 0,
            "avg_duration":  round(closed["duration_secs"].mean(), 1) if "duration_secs" in closed else 0,
            "avg_move_pct":  round(closed["window_move_pct"].mean() * 100, 4) if "window_move_pct" in closed else 0,
        }

    # ------------------------------------------------------------------ #
    #  Private                                                             #
    # ------------------------------------------------------------------ #

    def _append_row(self, row: dict):
        with self._lock:
            file_exists = os.path.exists(TRADES_FILE)
            pd.DataFrame([{col: row.get(col) for col in COLUMNS}]).to_csv(
                TRADES_FILE, mode="a", header=not file_exists, index=False
            )

    def _load(self) -> pd.DataFrame:
        if not os.path.exists(TRADES_FILE):
            return pd.DataFrame(columns=COLUMNS)
        df = pd.read_csv(TRADES_FILE)
        # Prevent string columns from being inferred as float64 when all values are null,
        # which would cause FutureWarning (soon an error) when close_trade writes strings back.
        str_cols = [
            "trade_id", "mode", "asset", "direction", "entry_time", "exit_time",
            "result", "window_direction", "rsi_bias", "macd_bias", "momentum_bias", "vwap_bias",
        ]
        for col in str_cols:
            if col in df.columns:
                df[col] = df[col].astype(object)
        return df
