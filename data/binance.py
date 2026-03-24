import pandas as pd
from binance.client import Client
from binance import ThreadedWebsocketManager
from administration.config import (
    BINANCE_API_KEY, BINANCE_API_SECRET,
    SYMBOL, INTERVALS, CANDLE_LIMIT
)


class BinanceFeed:
    def __init__(self):
        self.client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
        self.bm = None
        self.streams = {}
        self.latest_price = None

    # ------------------------------------------------------------------ #
    #  REST — Historical Candles                                           #
    # ------------------------------------------------------------------ #

    def get_candles(self, interval, limit=CANDLE_LIMIT):
        """Fetch historical candles for a given interval via REST."""
        raw = self.client.get_klines(
            symbol=SYMBOL,
            interval=interval,
            limit=limit
        )
        return self._to_dataframe(raw)

    def get_trend_candles(self):
        """1H candles for RSI + MACD trend filter."""
        return self.get_candles(INTERVALS["trend"])

    def get_entry_candles(self):
        """15M candles for Momentum + VWAP entry signal."""
        return self.get_candles(INTERVALS["entry"])

    # ------------------------------------------------------------------ #
    #  WebSocket — Live Streams                                            #
    # ------------------------------------------------------------------ #

    def start_streams(self, on_1h=None, on_15m=None, on_1m=None):
        """
        Start live WebSocket streams.
        Callbacks receive a parsed candle dict when a candle closes.
        on_1m receives every tick (live price updates).
        """
        self.bm = ThreadedWebsocketManager(
            api_key=BINANCE_API_KEY,
            api_secret=BINANCE_API_SECRET
        )
        self.bm.start()

        if on_1h:
            self.streams["1h"] = self.bm.start_kline_socket(
                callback=self._make_candle_handler(on_1h, closed_only=True),
                symbol=SYMBOL,
                interval=INTERVALS["trend"]
            )

        if on_15m:
            self.streams["15m"] = self.bm.start_kline_socket(
                callback=self._make_candle_handler(on_15m, closed_only=True),
                symbol=SYMBOL,
                interval=INTERVALS["entry"]
            )

        if on_1m:
            self.streams["1m"] = self.bm.start_kline_socket(
                callback=self._make_ticker_handler(on_1m),
                symbol=SYMBOL,
                interval=INTERVALS["ticker"]
            )

    def stop_streams(self):
        """Cleanly shut down all WebSocket streams."""
        if self.bm:
            for key in self.streams.values():
                self.bm.stop_socket(key)
            self.bm.close()
            self.streams = {}

    # ------------------------------------------------------------------ #
    #  Handlers                                                            #
    # ------------------------------------------------------------------ #

    def _make_candle_handler(self, callback, closed_only=True):
        """Returns a handler that fires callback only on closed candles."""
        def handler(msg):
            if msg.get("e") == "error":
                return
            candle = msg.get("k", {})
            if closed_only and not candle.get("x", False):
                return  # Candle not yet closed
            callback(self._parse_candle(candle))
        return handler

    def _make_ticker_handler(self, callback):
        """Returns a handler that fires on every 1M tick with latest price."""
        def handler(msg):
            if msg.get("e") == "error":
                return
            candle = msg.get("k", {})
            price = float(candle.get("c", 0))
            if price:
                self.latest_price = price
                callback(price)
        return handler

    # ------------------------------------------------------------------ #
    #  Parsers                                                             #
    # ------------------------------------------------------------------ #

    def _to_dataframe(self, raw):
        """Convert raw Binance kline list to a clean DataFrame."""
        df = pd.DataFrame(raw, columns=[
            "time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades",
            "taker_buy_base", "taker_buy_quote", "ignore"
        ])
        df["time"] = pd.to_datetime(df["time"], unit="ms")
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)
        return df[["time", "open", "high", "low", "close", "volume"]].copy()

    def _parse_candle(self, k):
        """Parse a single WebSocket kline dict into a clean dict."""
        return {
            "time": pd.to_datetime(k["t"], unit="ms"),
            "open": float(k["o"]),
            "high": float(k["h"]),
            "low": float(k["l"]),
            "close": float(k["c"]),
            "volume": float(k["v"]),
            "closed": k.get("x", False)
        }
