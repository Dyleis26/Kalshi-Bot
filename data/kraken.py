import json
import time
import threading
import requests
import websocket
import pandas as pd
from administration.config import CANDLE_LIMIT
from administration.logger import get as get_logger

logger = get_logger("kraken")

REST_URL = "https://api.kraken.com/0/public"
WS_URL   = "wss://ws.kraken.com/v2"

# Kraken interval codes for REST API (minutes)
INTERVAL_MAP = {
    "1h":  60,
    "15m": 15,
    "1m":  1,
}

SYMBOL_REST = "XBTUSD"   # Kraken REST symbol for BTC/USD
SYMBOL_WS   = "BTC/USD"  # Kraken WebSocket symbol


class KrakenFeed:
    def __init__(self):
        self.latest_price = None
        self._ws = None
        self._ws_thread = None
        self._running = False
        self._callbacks = {}

    # ------------------------------------------------------------------ #
    #  REST — Historical Candles                                           #
    # ------------------------------------------------------------------ #

    def get_candles(self, interval: str, limit: int = CANDLE_LIMIT) -> pd.DataFrame:
        """Fetch historical OHLCV candles via Kraken REST API."""
        minutes = INTERVAL_MAP.get(interval, 60)
        # Kraken returns up to 720 candles; calculate since timestamp for limit
        since = int(time.time()) - (minutes * 60 * limit)
        try:
            r = requests.get(f"{REST_URL}/OHLC", params={
                "pair":     SYMBOL_REST,
                "interval": minutes,
                "since":    since,
            }, timeout=10)
            r.raise_for_status()
            data = r.json()
            if data.get("error"):
                logger.error(f"Kraken REST error: {data['error']}")
                return pd.DataFrame()
            # Result key is the pair name (e.g. 'XXBTZUSD')
            pair_key = list(data["result"].keys())[0]
            raw = data["result"][pair_key]
            return self._to_dataframe(raw)
        except Exception as e:
            logger.error(f"Failed to fetch {interval} candles: {e}")
            return pd.DataFrame()

    def get_trend_candles(self) -> pd.DataFrame:
        """1H candles for RSI + MACD trend filter."""
        return self.get_candles("1h")

    def get_entry_candles(self) -> pd.DataFrame:
        """15M candles for Momentum + VWAP entry signal."""
        return self.get_candles("15m")

    # ------------------------------------------------------------------ #
    #  WebSocket — Live Streams                                            #
    # ------------------------------------------------------------------ #

    def start_streams(self, on_1h=None, on_15m=None, on_1m=None):
        """
        Start live WebSocket streams for 1H, 15M candles and 1M ticker.
        Candle callbacks fire only on closed candles.
        Ticker callback fires on every price update.
        """
        self._callbacks = {
            "1h":  on_1h,
            "15m": on_15m,
            "1m":  on_1m,
        }
        self._running = True
        self._ws_thread = threading.Thread(target=self._run_ws, daemon=True)
        self._ws_thread.start()
        logger.info("Kraken WebSocket streams starting...")

    def stop_streams(self):
        """Cleanly shut down WebSocket."""
        self._running = False
        if self._ws:
            self._ws.close()
        logger.info("Kraken WebSocket stopped.")

    # ------------------------------------------------------------------ #
    #  WebSocket Internals                                                 #
    # ------------------------------------------------------------------ #

    def _run_ws(self):
        self._ws = websocket.WebSocketApp(
            WS_URL,
            on_open=self._on_open,
            on_message=self._on_message,
            on_error=self._on_error,
            on_close=self._on_close,
        )
        while self._running:
            try:
                self._ws.run_forever(ping_interval=30, ping_timeout=10)
            except Exception as e:
                logger.warning(f"WebSocket disconnected: {e} — reconnecting in 5s")
                time.sleep(5)

    def _on_open(self, ws):
        logger.info("Kraken WebSocket connected.")
        # Subscribe to OHLC for 1H, 15M and ticker for live price
        subscriptions = [
            {"method": "subscribe", "params": {"channel": "ohlc", "symbol": [SYMBOL_WS], "interval": 60}},
            {"method": "subscribe", "params": {"channel": "ohlc", "symbol": [SYMBOL_WS], "interval": 15}},
            {"method": "subscribe", "params": {"channel": "ticker", "symbol": [SYMBOL_WS]}},
        ]
        for sub in subscriptions:
            ws.send(json.dumps(sub))

    def _on_message(self, ws, message):
        try:
            msg = json.loads(message)
            channel = msg.get("channel")
            msg_type = msg.get("type")

            if channel == "ohlc" and msg_type == "update":
                self._handle_ohlc(msg)
            elif channel == "ticker" and msg_type == "update":
                self._handle_ticker(msg)
        except Exception as e:
            logger.warning(f"WebSocket message error: {e}")

    def _on_error(self, ws, error):
        logger.error(f"Kraken WebSocket error: {error}")

    def _on_close(self, ws, code, msg):
        logger.warning(f"Kraken WebSocket closed: {code} {msg}")

    def _handle_ohlc(self, msg):
        """Process a closed OHLC candle and fire the appropriate callback."""
        data = msg.get("data", [{}])[0]
        interval = data.get("interval")
        # Only fire on confirmed (closed) candles
        if not data.get("confirm", False):
            return

        candle = {
            "time":   pd.to_datetime(data["timestamp"]),
            "open":   float(data["open"]),
            "high":   float(data["high"]),
            "low":    float(data["low"]),
            "close":  float(data["close"]),
            "volume": float(data["volume"]),
            "closed": True,
        }

        if interval == 60 and self._callbacks.get("1h"):
            self._callbacks["1h"](candle)
        elif interval == 15 and self._callbacks.get("15m"):
            self._callbacks["15m"](candle)

    def _handle_ticker(self, msg):
        """Process a live ticker update and fire the 1M price callback."""
        data = msg.get("data", [{}])[0]
        price = float(data.get("last", 0))
        if price:
            self.latest_price = price
            if self._callbacks.get("1m"):
                self._callbacks["1m"](price)

    # ------------------------------------------------------------------ #
    #  Parser                                                              #
    # ------------------------------------------------------------------ #

    def _to_dataframe(self, raw: list) -> pd.DataFrame:
        """Convert Kraken REST OHLC list to a clean DataFrame."""
        df = pd.DataFrame(raw, columns=[
            "time", "open", "high", "low", "close", "vwap", "volume", "count"
        ])
        df = df.copy()
        df["time"] = pd.to_datetime(df["time"].astype(int), unit="s")
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = df[col].astype(float)
        return df[["time", "open", "high", "low", "close", "volume"]].reset_index(drop=True)
