import time
import base64
import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
from administration.config import KALSHI_API_KEY, KALSHI_KEY_PATH, ASSETS
from administration.logger import log_error
from administration.security import rate_limited_call

BASE_URL = "https://trading-api.kalshi.com/trade-api/v2"
DEMO_URL = "https://demo-api.kalshi.co/trade-api/v2"


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
        Returns the market dict or None if not found.
        """
        series = ASSETS.get(asset, {}).get("kalshi_series")
        if not series:
            return None
        try:
            data = self._get("/markets", params={"status": "open", "series_ticker": series})
            markets = data.get("markets", [])
            # The 15M directional series has exactly one open market at a time;
            # title is e.g. "BTC price up in next 15 mins?" — just return the first open market.
            if markets:
                return markets[0]
            return None
        except Exception as e:
            log_error(f"Failed to fetch {asset} market", e)
            return None

    def get_btc_market(self):
        """Convenience wrapper — returns BTC 15M market."""
        return self.get_market_for_asset("BTC")

    def get_market(self, ticker: str) -> dict:
        """Fetch a single market by ticker (works for open, closed, and settled)."""
        try:
            data = self._get(f"/markets/{ticker}")
            return data.get("market")
        except Exception as e:
            log_error(f"Failed to fetch market {ticker}", e)
            return None

    def get_market_result(self, ticker: str, retries: int = 6, delay: int = 10) -> str | None:
        """
        Poll until a market has settled and return 'yes' or 'no'.
        Waits up to retries * delay seconds (default 60s).
        Returns None if it never settles in time.
        """
        import time
        for _ in range(retries):
            m = self.get_market(ticker)
            if m and m.get("result") in ("yes", "no"):
                return m["result"]
            time.sleep(delay)
        return None

    def get_orderbook(self, ticker: str):
        """Fetch the current orderbook for a market ticker."""
        try:
            return self._get(f"/markets/{ticker}/orderbook")
        except Exception as e:
            log_error(f"Failed to fetch orderbook for {ticker}", e)
            return None

    def get_market_price(self, ticker: str) -> float:
        """
        Return the best YES ask price (0.0 to 1.0) for a market.
        Returns 1.0 (outside filter range) when no asks exist so the trade is skipped.
        """
        book = self.get_orderbook(ticker)
        if not book:
            return 1.0  # API error or stale ticker — skip trade, don't assume 50¢
        asks = book.get("orderbook", {}).get("asks", [])
        if asks:
            return round(asks[0][0] / 100, 4)
        return 1.0  # No asks = no liquidity; 1.0 forces the contract price filter to skip

    # ------------------------------------------------------------------ #
    #  Orders                                                              #
    # ------------------------------------------------------------------ #

    def place_limit_order(self, ticker: str, side: str, count: int, price_cents: int) -> dict:
        """
        Place a limit order on Kalshi.

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
