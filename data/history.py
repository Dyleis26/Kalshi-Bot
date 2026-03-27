import os
import threading
import pandas as pd
from administration.config import INTERVALS, CANDLE_LIMIT

STORAGE_DIR = os.path.join(os.path.dirname(__file__), "storage")

# Candles to fetch on catch-up (not full CANDLE_LIMIT — just enough to cover realistic gaps)
_CATCHUP_LIMIT = 20


class History:
    def __init__(self, asset: str = "BTC", feed=None):
        """
        feed: shared KrakenFeed instance — accepts its REST get_candles() method.
              If None, creates its own (legacy path, avoids import cycle on standalone use).
        """
        self.asset = asset
        self._lock = threading.Lock()
        os.makedirs(STORAGE_DIR, exist_ok=True)
        if feed is not None:
            self.feed = feed
        else:
            from data.kraken import KrakenFeed
            self.feed = KrakenFeed()
        # Cache the last-written candle time per interval to avoid full CSV reads in append()
        self._last_written: dict = {}

    # ------------------------------------------------------------------ #
    #  Public                                                              #
    # ------------------------------------------------------------------ #

    def load(self, interval: str) -> pd.DataFrame:
        """
        Load candles for an interval.
        Pulls from Kraken if no local file exists, otherwise loads local
        and appends any missing candles since last save.
        Trims to CANDLE_LIMIT rows and seeds _last_written cache.
        """
        with self._lock:
            path = self._path(interval)
            if not os.path.exists(path):
                df = self.feed.get_candles(interval, asset=self.asset, limit=CANDLE_LIMIT)
                df = df.tail(CANDLE_LIMIT)
                self._save(df, path)
            else:
                df = self._read(path)
                df = self._update(df, interval)
                df = df.tail(CANDLE_LIMIT)
                self._save(df, path)

            # Seed the append cache so the first live candle doesn't duplicate
            if not df.empty:
                self._last_written[interval] = pd.Timestamp(df["time"].iloc[-1])
            return df

    def load_all(self) -> dict:
        """Load 1H and 15M candles and return as a dict."""
        return {
            "1h":  self.load(INTERVALS["trend"]),
            "15m": self.load(INTERVALS["entry"]),
        }

    def append(self, candle: dict, interval: str):
        """
        Append a single live candle to the local CSV.
        Uses an in-memory cache to detect duplicates without reading the full file.
        Only falls back to a read-modify-write when the same candle arrives twice
        (backfill + WS overlap on reconnect).
        """
        path = self._path(interval)
        row = pd.DataFrame([candle])[["time", "open", "high", "low", "close", "volume"]]
        candle_time = pd.Timestamp(row["time"].iloc[0])

        with self._lock:
            last = self._last_written.get(interval)

            if last is not None and last == candle_time:
                # Same candle arrived again — update the last row in place
                df = self._read(path)
                if not df.empty:
                    df.iloc[-1] = row.iloc[0]
                    self._save(df, path)
                return

            # New candle — append directly without reading the whole file
            file_exists = os.path.exists(path)
            row.to_csv(path, mode="a", header=not file_exists, index=False)
            self._last_written[interval] = candle_time

    # ------------------------------------------------------------------ #
    #  Private                                                             #
    # ------------------------------------------------------------------ #

    def _update(self, df: pd.DataFrame, interval: str) -> pd.DataFrame:
        """Fetch only the candles newer than last saved and append them."""
        if df.empty:
            return self.feed.get_candles(interval, asset=self.asset, limit=CANDLE_LIMIT)

        # Fetch a small window — enough to cover realistic bot downtime
        fresh = self.feed.get_candles(interval, asset=self.asset, limit=_CATCHUP_LIMIT)
        if fresh.empty:
            return df

        last_saved = pd.Timestamp(df["time"].iloc[-1])
        new_rows = fresh[fresh["time"].apply(pd.Timestamp) > last_saved]

        if new_rows.empty:
            return df
        return pd.concat([df, new_rows], ignore_index=True)

    def _path(self, interval: str) -> str:
        return os.path.join(STORAGE_DIR, f"{self.asset}USD_{interval}.csv")

    def _save(self, df: pd.DataFrame, path: str):
        df.to_csv(path, index=False)

    def _read(self, path: str) -> pd.DataFrame:
        return pd.read_csv(path, parse_dates=["time"])
