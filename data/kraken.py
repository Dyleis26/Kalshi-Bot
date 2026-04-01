import json
import time
import threading
import requests
import websocket
import pandas as pd
from datetime import datetime, timezone
from administration.config import CANDLE_LIMIT, ASSETS
from administration.logger import get as get_logger
from administration.security import rate_limited_call

logger = get_logger("kraken")

REST_URL = "https://api.kraken.com/0/public"
WS_URL   = "wss://ws.kraken.com/v2"

# Kraken interval codes for REST API (minutes)
INTERVAL_MAP = {
    "1h":  60,
    "15m": 15,
    "1m":  1,
}

# Map WebSocket symbol → asset name (e.g. "BTC/USD" → "BTC")
WS_TO_ASSET = {cfg["kraken_ws"]: asset for asset, cfg in ASSETS.items()}


class KrakenFeed:
    def __init__(self):
        self.latest_prices: dict = {asset: None for asset in ASSETS}  # per-asset live price
        self._ws = None
        self._ws_thread = None
        self._running = False
        self._on_15m = None   # callback(asset, candle)
        self._on_tick = None  # callback(asset, price)
        # Candle close detection: keyed by (ws_symbol, interval)
        self._last_ts: dict = {}
        self._last_data: dict = {}

    # ------------------------------------------------------------------ #
    #  REST — Historical Candles                                           #
    # ------------------------------------------------------------------ #

    def get_candles(self, interval: str, asset: str = "BTC", limit: int = CANDLE_LIMIT) -> pd.DataFrame:
        """Fetch historical OHLCV candles via Kraken REST API."""
        minutes = INTERVAL_MAP.get(interval, 60)
        symbol_rest = ASSETS[asset]["kraken_rest"]
        since = int(time.time()) - (minutes * 60 * limit)
        try:
            def _call():
                r = requests.get(f"{REST_URL}/OHLC", params={
                    "pair":     symbol_rest,
                    "interval": minutes,
                    "since":    since,
                }, timeout=10)
                r.raise_for_status()
                return r.json()
            data = rate_limited_call("kraken", _call)
            if data.get("error"):
                logger.error(f"Kraken REST error ({asset}): {data['error']}")
                return pd.DataFrame()
            pair_key = next(k for k in data["result"] if k != "last")
            raw = data["result"][pair_key]
            return self._to_dataframe(raw)
        except Exception as e:
            logger.error(f"Failed to fetch {interval} candles for {asset}: {e}")
            return pd.DataFrame()

    def get_trend_candles(self, asset: str = "BTC") -> pd.DataFrame:
        """1H candles for RSI + MACD trend filter."""
        return self.get_candles("1h", asset)

    def get_entry_candles(self, asset: str = "BTC") -> pd.DataFrame:
        """15M candles for Momentum + VWAP entry signal."""
        return self.get_candles("15m", asset)

    # ------------------------------------------------------------------ #
    #  WebSocket — Live Streams                                            #
    # ------------------------------------------------------------------ #

    def start_streams(self, on_15m=None, on_tick=None):
        """
        Start live WebSocket streams for all assets (15M OHLC + ticker).
        on_15m(asset, candle) fires on each closed 15M candle.
        on_tick(asset, price) fires on every ticker price update.
        """
        self._on_15m = on_15m
        self._on_tick = on_tick
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
        # Only subscribe BTC — it's the only live-traded crypto asset.
        # ETH/SOL/XRP/DOGE remain in ASSETS for backtesting only.
        btc_symbol = [ASSETS["BTC"]["kraken_ws"]]
        # ONE ohlc interval per connection — 15M only. 1H refreshed via REST in heartbeat.
        subscriptions = [
            {"method": "subscribe", "params": {"channel": "ohlc",   "symbol": btc_symbol, "interval": 15}},
            {"method": "subscribe", "params": {"channel": "ticker",  "symbol": btc_symbol}},
        ]
        for sub in subscriptions:
            ws.send(json.dumps(sub))
        # Backfill any candles missed during a disconnect
        self._backfill_missed_candles()

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

    def _backfill_missed_candles(self):
        """
        On reconnect, fetch the last 5 closed 15M candles via REST for BTC
        and fire _on_15m for any candle newer than what we last saw on the WS.
        Prevents missed signals when the WS drops for up to ~75 minutes.
        """
        if not self._on_15m:
            return
        # tz-naive UTC to match Kraken REST candle timestamps
        now_utc = pd.Timestamp(datetime.now(timezone.utc).replace(tzinfo=None))
        for asset in ["BTC"]:   # Only backfill live-traded asset
            try:
                df = self.get_candles("15m", asset=asset, limit=5)
                if df.empty:
                    continue
                ws_symbol = ASSETS[asset]["kraken_ws"]
                key = (ws_symbol, 15)
                last_seen = self._last_ts.get(key)  # interval_begin string from WS
                for _, row in df.iterrows():
                    # Skip candles that haven't closed yet (current open candle from REST)
                    candle_close_time = pd.Timestamp(row["time"]) + pd.Timedelta(minutes=15)
                    if candle_close_time > now_utc:
                        continue
                    # Convert REST timestamp to comparable string (ISO8601 UTC)
                    row_ts = pd.Timestamp(row["time"], tz="UTC").isoformat()
                    if last_seen is None or row_ts > last_seen:
                        candle = {
                            "time":   row["time"],
                            "open":   row["open"],
                            "high":   row["high"],
                            "low":    row["low"],
                            "close":  row["close"],
                            "volume": row["volume"],
                        }
                        logger.info(f"Backfill: firing missed candle for {asset} at {row['time']}")
                        self._on_15m(asset, candle)
                        # Update seen timestamp so repeated reconnects don't re-fire
                        self._last_ts[key] = row_ts
                        last_seen = row_ts
            except Exception as e:
                logger.warning(f"Backfill failed for {asset}: {e}")

    def _handle_ohlc(self, msg):
        """
        Detect closed candles by watching for a timestamp change, per symbol.
        When the candle timestamp changes, the previous candle just closed.
        """
        data = msg.get("data", [{}])[0]
        ws_symbol = data.get("symbol")
        interval  = data.get("interval")
        candle_ts = data.get("interval_begin")
        asset     = WS_TO_ASSET.get(ws_symbol)

        if not asset:
            return

        key = (ws_symbol, interval)

        if key not in self._last_ts or key not in self._last_data:
            # First live message for this key (or backfill set _last_ts without _last_data)
            self._last_ts[key]   = candle_ts
            self._last_data[key] = data
            return

        if candle_ts != self._last_ts[key]:
            closed = self._last_data[key]
            candle = {
                "time":   pd.to_datetime(closed["interval_begin"], utc=True).tz_convert(None),
                "open":   float(closed["open"]),
                "high":   float(closed["high"]),
                "low":    float(closed["low"]),
                "close":  float(closed["close"]),
                "volume": float(closed["volume"]),
            }
            logger.debug(f"Candle closed: {asset} interval={interval} close={closed['close']}")
            if interval == 15 and self._on_15m:
                self._on_15m(asset, candle)
            self._last_ts[key] = candle_ts

        self._last_data[key] = data

    def _handle_ticker(self, msg):
        """Process a live ticker update and fire the price callback."""
        data = msg.get("data", [{}])[0]
        ws_symbol = data.get("symbol")
        asset     = WS_TO_ASSET.get(ws_symbol)
        price     = float(data.get("last", 0))
        if asset and price:
            self.latest_prices[asset] = price
            if self._on_tick:
                self._on_tick(asset, price)

    # ------------------------------------------------------------------ #
    #  Parser                                                              #
    # ------------------------------------------------------------------ #

    def _to_dataframe(self, raw: list) -> pd.DataFrame:
        """Convert Kraken REST OHLC list to a clean DataFrame."""
        df = pd.DataFrame(raw, columns=[
            "time", "open", "high", "low", "close", "vwap", "volume", "count"
        ])
        df = df.assign(
            time=pd.to_datetime(df["time"].astype(int), unit="s"),
            open=pd.to_numeric(df["open"]),
            high=pd.to_numeric(df["high"]),
            low=pd.to_numeric(df["low"]),
            close=pd.to_numeric(df["close"]),
            volume=pd.to_numeric(df["volume"]),
        )
        return df[["time", "open", "high", "low", "close", "volume"]].reset_index(drop=True)
