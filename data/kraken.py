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
        # Candle close detection: track last seen timestamp per interval
        self._last_ts: dict = {}
        self._last_data: dict = {}

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
        # Kraken v2 only allows ONE ohlc interval per connection — subscribe to 15M only.
        # 1H data is refreshed from REST in the heartbeat (RSI/MACD are slow indicators).
        subscriptions = [
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
        """
        Detect closed candles by watching for a timestamp change.
        Kraken WebSocket v2 sends confirm=null on every update, so we cannot
        rely on the confirm field. Instead: when the candle timestamp changes,
        the previous candle just closed — fire the callback with its final data.
        """
        data = msg.get("data", [{}])[0]
        interval = data.get("interval")
        candle_ts = data.get("interval_begin")  # ISO string: candle open time

        if interval not in self._last_ts:
            # First update for this interval — initialise, don't fire yet
            self._last_ts[interval] = candle_ts
            self._last_data[interval] = data
            return

        if candle_ts != self._last_ts[interval]:
            # Candle rolled over — previous candle is now closed
            closed = self._last_data[interval]
            candle = {
                "time":   pd.to_datetime(closed["interval_begin"], utc=True).tz_convert(None),
                "open":   float(closed["open"]),
                "high":   float(closed["high"]),
                "low":    float(closed["low"]),
                "close":  float(closed["close"]),
                "volume": float(closed["volume"]),
            }
            logger.debug(f"Candle closed: interval={interval} ts={closed['interval_begin']} close={closed['close']}")
            if interval == 60 and self._callbacks.get("1h"):
                self._callbacks["1h"](candle)
            elif interval == 15 and self._callbacks.get("15m"):
                self._callbacks["15m"](candle)
            self._last_ts[interval] = candle_ts

        # Always update latest candle data
        self._last_data[interval] = data

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
        df.loc[:, "time"] = pd.to_datetime(df["time"].astype(int), unit="s")
        for col in ["open", "high", "low", "close", "volume"]:
            df.loc[:, col] = df[col].astype(float)
        return df[["time", "open", "high", "low", "close", "volume"]].reset_index(drop=True)
