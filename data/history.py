import os
import pandas as pd
from data.kraken import KrakenFeed
from administration.config import INTERVALS, CANDLE_LIMIT

SYMBOL = "BTCUSD"

STORAGE_DIR = os.path.join(os.path.dirname(__file__), "storage")


class History:
    def __init__(self):
        os.makedirs(STORAGE_DIR, exist_ok=True)
        self.feed = KrakenFeed()

    # ------------------------------------------------------------------ #
    #  Public                                                              #
    # ------------------------------------------------------------------ #

    def load(self, interval):
        """
        Load candles for an interval.
        Pulls from Kraken if no local file exists, otherwise loads local
        and appends any missing candles since last save.
        """
        path = self._path(interval)
        if not os.path.exists(path):
            df = self.feed.get_candles(interval, limit=CANDLE_LIMIT)
            self._save(df, path)
            return df

        df = self._read(path)
        df = self._update(df, interval)
        self._save(df, path)
        return df

    def load_all(self):
        """Load 1H and 15M candles and return as a dict."""
        return {
            "1h": self.load(INTERVALS["trend"]),
            "15m": self.load(INTERVALS["entry"]),
        }

    def append(self, candle: dict, interval: str):
        """Append a single live candle to the local CSV (called by WebSocket handler)."""
        path = self._path(interval)
        row = pd.DataFrame([candle])[["time", "open", "high", "low", "close", "volume"]]
        if not os.path.exists(path):
            row.to_csv(path, index=False)
            return
        df = self._read(path)
        # Avoid duplicate candles
        if not df.empty and df["time"].iloc[-1] == row["time"].iloc[0]:
            df.iloc[-1] = row.iloc[0]
        else:
            df = pd.concat([df, row], ignore_index=True)
        self._save(df, path)

    # ------------------------------------------------------------------ #
    #  Private                                                             #
    # ------------------------------------------------------------------ #

    def _update(self, df, interval):
        """Fetch candles newer than the last saved candle and append them."""
        if df.empty:
            return self.feed.get_candles(interval, limit=CANDLE_LIMIT)

        fresh = self.feed.get_candles(interval, limit=CANDLE_LIMIT)
        last_saved = df["time"].iloc[-1]
        new_rows = fresh[fresh["time"] > last_saved]

        if new_rows.empty:
            return df
        return pd.concat([df, new_rows], ignore_index=True)

    def _path(self, interval):
        filename = f"{SYMBOL}_{interval}.csv"
        return os.path.join(STORAGE_DIR, filename)

    def _save(self, df, path):
        df.to_csv(path, index=False)

    def _read(self, path):
        df = pd.read_csv(path, parse_dates=["time"])
        return df
