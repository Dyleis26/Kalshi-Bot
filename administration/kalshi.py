import time
import base64
import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from administration.config import KALSHI_API_KEY, KALSHI_KEY_PATH, ASSETS, SLOTS
from administration.logger import log_error
from administration.security import rate_limited_call

BASE_URL   = "https://trading-api.kalshi.com/trade-api/v2"
DEMO_URL   = "https://demo-api.kalshi.co/trade-api/v2"
PUBLIC_URL = "https://api.elections.kalshi.com/trade-api/v2"  # No auth required — real live data


class KalshiClient:
    def __init__(self, paper: bool = True):
        """
        paper=True  → uses Kalshi demo environment (paper trading)
        paper=False → uses live trading environment
        """
        self.base = DEMO_URL if paper else BASE_URL
        self.paper = paper
        self.session = requests.Session()
        self._private_key = self._load_key()

    # ------------------------------------------------------------------ #
    #  Auth                                                                #
    # ------------------------------------------------------------------ #

    def _load_key(self):
        """Load RSA private key from PEM file."""
        with open(KALSHI_KEY_PATH, "rb") as f:
            return serialization.load_pem_private_key(f.read(), password=None)

    def _sign(self, method: str, path: str) -> dict:
        """
        Build Kalshi RSA-SHA256 auth headers.
        Signature = RSA-SHA256(private_key, timestamp + method + full_path)
        full_path includes the base path prefix (e.g. /trade-api/v2/markets)
        """
        from urllib.parse import urlparse
        base_path = urlparse(self.base).path  # e.g. "/trade-api/v2"
        timestamp = str(int(time.time() * 1000))
        message = (timestamp + method.upper() + base_path + path).encode("utf-8")
        signature = self._private_key.sign(message, padding.PKCS1v15(), hashes.SHA256())
        sig_b64 = base64.b64encode(signature).decode("utf-8")

        return {
            "KALSHI-ACCESS-KEY": KALSHI_API_KEY,
            "KALSHI-ACCESS-SIGNATURE": sig_b64,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "Content-Type": "application/json",
        }

    def _get_public(self, path: str, params: dict = None):
        """Unauthenticated GET against the public Kalshi API (real live market data)."""
        def call():
            r = requests.get(PUBLIC_URL + path, params=params, timeout=10)
            r.raise_for_status()
            return r.json()
        return rate_limited_call("kalshi", call)

    def _get(self, path: str, params: dict = None):
        def call():
            r = self.session.get(self.base + path, headers=self._sign("GET", path), params=params, timeout=10)
            r.raise_for_status()
            return r.json()
        return rate_limited_call("kalshi", call)

    def _post(self, path: str, body: dict):
        def call():
            r = self.session.post(self.base + path, headers=self._sign("POST", path), json=body, timeout=10)
            r.raise_for_status()
            return r.json()
        return rate_limited_call("kalshi", call)

    def _delete(self, path: str):
        def call():
            r = self.session.delete(self.base + path, headers=self._sign("DELETE", path), timeout=10)
            r.raise_for_status()
            return r.json()
        return rate_limited_call("kalshi", call)

    # ------------------------------------------------------------------ #
    #  Market                                                              #
    # ------------------------------------------------------------------ #

    def get_market_for_asset(self, asset: str) -> dict | None:
        """
        Find the active 15-minute Up/Down market for the given asset.
        Uses the public API (no auth) so all 5 assets return real live data.
        Returns the market dict or None if not found.
        """
        series = ASSETS.get(asset, {}).get("kalshi_series")
        if not series:
            return None
        try:
            data = self._get_public("/markets", params={"status": "open", "series_ticker": series})
            markets = data.get("markets", [])
            if markets:
                return markets[0]
            return None
        except Exception as e:
            log_error(f"Failed to fetch {asset} market", e)
            return None

    def get_btc_market(self):
        """Convenience wrapper — returns BTC 15M market."""
        return self.get_market_for_asset("BTC")

    def get_markets_by_series(self, series_prefix: str, max_close_hours: float = 36.0,
                               game_date_filter: bool = False) -> list:
        """
        Return open markets for a given series_ticker that are relevant to trade now.

        game_date_filter=True  (sports): parse game date from ticker (e.g. KXNBAGAME-26MAR29...)
                                          and only return today's games with active pricing.
        game_date_filter=False: filter by close_time within max_close_hours.

        Returns a list of market dicts (may be empty).
        """
        import re
        from datetime import datetime, timezone, timedelta, date as dt_date

        from zoneinfo import ZoneInfo
        now = datetime.now(timezone.utc)
        # Use Eastern Time for game date — Kalshi tickers use ET dates (e.g. 26MAR30 = March 30 ET)
        today = datetime.now(ZoneInfo("America/New_York")).date()
        cutoff = now + timedelta(hours=max_close_hours)

        _MONTH = {'JAN':1,'FEB':2,'MAR':3,'APR':4,'MAY':5,'JUN':6,
                  'JUL':7,'AUG':8,'SEP':9,'OCT':10,'NOV':11,'DEC':12}

        try:
            data = self._get(
                "/markets",
                params={"status": "open", "series_ticker": series_prefix, "limit": 100},
            )
            markets = data.get("markets", [])
            result = []
            for m in markets:
                if game_date_filter:
                    # Sports: skip unpriced markets (game not yet posted or already finished)
                    yes_ask = float(m.get("yes_ask_dollars") or 0)
                    if yes_ask <= 0:
                        continue
                    # Parse game date from ticker: KXNBAGAME-26MAR29GSWDEN-DEN → 26MAR29
                    ticker = m.get("ticker", "")
                    dm = re.search(r'(\d{2})(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(\d{2})',
                                   ticker)
                    if not dm:
                        continue
                    game_date = dt_date(2000 + int(dm.group(1)),
                                        _MONTH[dm.group(2)],
                                        int(dm.group(3)))
                    if game_date != today:
                        continue
                else:
                    # Weather: filter by close_time within max_close_hours
                    close_str = m.get("close_time") or m.get("expiration_time", "")
                    if not close_str:
                        continue
                    try:
                        close_dt = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
                    except ValueError:
                        continue
                    if not (now <= close_dt <= cutoff):
                        continue

                result.append(m)
            return result
        except Exception as e:
            log_error(f"Failed to fetch markets for series {series_prefix!r}", e)
            return []

    def get_open_markets_by_category(self, limit: int = 20) -> list:
        """
        Debug helper — return a sample of open markets to discover available series.
        Run this once to find valid series_ticker values for sports/weather slots.
        """
        try:
            data = self._get_public("/markets", params={"status": "open", "limit": limit})
            markets = data.get("markets", [])
            return [
                {"ticker": m.get("ticker"), "series": m.get("series_ticker"),
                 "title": m.get("title"), "close_time": m.get("close_time")}
                for m in markets
            ]
        except Exception as e:
            log_error("Failed to fetch open markets", e)
            return []

    def get_market(self, ticker: str) -> dict:
        """Fetch a single market by ticker using the public API (real live data, no auth)."""
        try:
            data = self._get_public(f"/markets/{ticker}")
            return data.get("market")
        except Exception as e:
            log_error(f"Failed to fetch market {ticker}", e)
            return None

    def get_market_result(self, ticker: str, retries: int = 6, delay: int = 10) -> str | None:
        """
        Poll until a market has settled and return 'yes' or 'no'.
        Waits up to retries * delay seconds (default 60s, 6 polls × 10s).
        First check is immediate — most markets settle within 5-10s of window close.
        Returns None if it never settles in time.
        """
        for i in range(retries):
            m = self.get_market(ticker)
            if m and m.get("result") in ("yes", "no"):
                return m["result"]
            if i < retries - 1:
                time.sleep(delay)
        return None

    def get_orderbook(self, ticker: str):
        """Fetch the current orderbook for a market ticker."""
        try:
            return self._get(f"/markets/{ticker}/orderbook")
        except Exception as e:
            log_error(f"Failed to fetch orderbook for {ticker}", e)
            return None

    def get_market_price(self, ticker: str, side: str = "yes") -> float:
        """
        Return the best ask price (0.0–1.0) for the given side ('yes' or 'no').
        Reads market fields directly — demo API orderbooks are always empty.
        Falls back to last_price if the directional ask has no seller (price = 1.0).
        Returns 1.0 to signal "skip this trade" on API error or truly no price.
        """
        market = self.get_market(ticker)
        if not market:
            return 1.0  # API error — skip trade

        field = "yes_ask_dollars" if side == "yes" else "no_ask_dollars"
        price = float(market.get(field, 0.0))

        # 0.0 means no ask exists; 1.0 means no seller at a real price — both invalid
        if 0.0 < price < 1.0:
            return round(price, 4)

        # No directional ask — fall back to last traded price (only if meaningful)
        last = float(market.get("last_price_dollars", 0.0))
        if last >= 0.05:
            fair = last if side == "yes" else round(1.0 - last, 4)
            return round(fair, 4)

        return 1.0  # No price data at all — skip trade

    def get_market_prices(self, ticker: str) -> tuple:
        """
        Return (yes_ask, no_ask) for a market in a single API call.
        Used for market-alignment direction: yes_ask > 0.50 means market favours UP.
        Returns (1.0, 1.0) on any error so callers can treat it as 'no price'.
        """
        market = self.get_market(ticker)
        if not market:
            return 1.0, 1.0

        yes = float(market.get("yes_ask_dollars", 0.0))
        no  = float(market.get("no_ask_dollars",  0.0))

        # Fall back to last_price when ask is missing
        if not (0.0 < yes < 1.0) or not (0.0 < no < 1.0):
            last = float(market.get("last_price_dollars", 0.0))
            if last >= 0.05:
                yes = round(last, 4)
                no  = round(1.0 - last, 4)
            else:
                return 1.0, 1.0

        return round(yes, 4), round(no, 4)

    # ------------------------------------------------------------------ #
    #  Orders                                                              #
    # ------------------------------------------------------------------ #

    def place_limit_order(self, ticker: str, side: str, count: int, price_cents: int) -> dict:
        """
        Place a limit buy order on Kalshi.

        Args:
            ticker:       Market ticker (e.g. 'KXBTC-15M-...')
            side:         'yes' or 'no'
            count:        Number of contracts
            price_cents:  Limit price in cents (1-99)

        Returns:
            Order dict from Kalshi API or None on failure
        """
        try:
            body = {
                "ticker": ticker,
                "action": "buy",
                "side": side,
                "type": "limit",
                "count": count,
                "limit_price": price_cents,
            }
            result = self._post("/orders", body)
            return result.get("order")
        except Exception as e:
            log_error(f"Failed to place limit order on {ticker}", e)
            return None

    def sell_position(self, ticker: str, side: str, count: int, price_cents: int) -> dict | None:
        """
        Sell (exit) contracts already held in a position.

        Args:
            ticker:       Market ticker
            side:         'yes' or 'no' — the side we own
            count:        Number of contracts to sell
            price_cents:  Minimum sell price in cents (1-99). Use a low value (e.g. 1)
                          to guarantee a fill on stop-loss; use current market price for
                          trailing-profit exits where we want a fair execution.

        Returns:
            Order dict from Kalshi API or None on failure
        """
        try:
            body = {
                "ticker": ticker,
                "action": "sell",
                "side": side,
                "type": "limit",
                "count": count,
                "limit_price": price_cents,
            }
            result = self._post("/orders", body)
            return result.get("order")
        except Exception as e:
            log_error(f"Failed to place sell order on {ticker}", e)
            return None

    def wait_for_fill(self, order_id: str, timeout_secs: int = None) -> dict | None:
        """
        Poll until an order is fully executed or the timeout is exceeded.
        Cancels the order if it has not filled by the deadline.

        Returns the filled order dict, or None if canceled/timeout.
        """
        from administration.config import ORDER_TIMEOUT_SECS
        if timeout_secs is None:
            timeout_secs = ORDER_TIMEOUT_SECS
        deadline = time.monotonic() + timeout_secs
        while time.monotonic() < deadline:
            order = self.get_order(order_id)
            if not order:
                return None
            status = order.get("status")
            if status == "executed":
                return order
            if status == "canceled":
                return None
            time.sleep(5)
        self.cancel_order(order_id)
        return None

    def cancel_order(self, order_id: str) -> bool:
        """Cancel an open order. Returns True on success."""
        try:
            self._delete(f"/orders/{order_id}")
            return True
        except Exception as e:
            log_error(f"Failed to cancel order {order_id}", e)
            return False

    def get_order(self, order_id: str) -> dict:
        """Fetch the current state of an order."""
        try:
            data = self._get(f"/orders/{order_id}")
            return data.get("order")
        except Exception as e:
            log_error(f"Failed to fetch order {order_id}", e)
            return None

    # ------------------------------------------------------------------ #
    #  Portfolio                                                           #
    # ------------------------------------------------------------------ #

    def get_balance(self) -> float:
        """Return available Kalshi account balance in dollars."""
        try:
            data = self._get("/portfolio/balance")
            cents = data.get("balance", 0)
            return round(cents / 100, 2)
        except Exception as e:
            log_error("Failed to fetch Kalshi balance", e)
            return 0.0

    def get_positions(self) -> list:
        """Return all open positions."""
        try:
            data = self._get("/portfolio/positions")
            return data.get("market_positions", [])
        except Exception as e:
            log_error("Failed to fetch positions", e)
            return []
