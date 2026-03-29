import os
from pathlib import Path
from dotenv import load_dotenv

# Always load .env from the project root regardless of where the script is run from
load_dotenv(Path(__file__).parent.parent / ".env")

# --- API Keys ---
KALSHI_API_KEY       = os.getenv("KALSHI_API_KEY")
CRYPTOPANIC_API_KEY  = os.getenv("CRYPTOPANIC_API_KEY", "")
NEWSAPI_KEY          = os.getenv("NEWSAPI_KEY", "")
KALSHI_KEY_PATH = os.getenv(
    "KALSHI_KEY_PATH",
    str(Path(__file__).parent / "kalshi_key.pem")
)

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

# --- Assets ---
ASSETS = {
    "BTC":  {"kraken_rest": "XBTUSD",  "kraken_ws": "BTC/USD",  "kalshi_series": "KXBTC15M"},
    "ETH":  {"kraken_rest": "ETHUSD",  "kraken_ws": "ETH/USD",  "kalshi_series": "KXETH15M"},
    "SOL":  {"kraken_rest": "SOLUSD",  "kraken_ws": "SOL/USD",  "kalshi_series": "KXSOL15M"},
    "XRP":  {"kraken_rest": "XRPUSD",  "kraken_ws": "XRP/USD",  "kalshi_series": "KXXRP15M"},
    "DOGE": {"kraken_rest": "XDGUSD",  "kraken_ws": "DOGE/USD", "kalshi_series": "KXDOGE15M"},
}
NUM_SLOTS = len(ASSETS)  # 5 — one capital slot per asset

# --- Market ---
INTERVALS = {
    "trend": "1h",    # RSI + MACD filter
    "entry": "15m",   # Momentum + VWAP signal
    "ticker": "1m",   # Live price for order placement
}
CANDLE_LIMIT = 200    # Historical candles to load on startup

# --- Strategy Thresholds ---
RSI_BULL = 53         # 1H RSI above this = bullish bias (tighter neutral zone)
RSI_BEAR = 47         # 1H RSI below this = bearish bias
RSI_PERIOD = 14
MOMENTUM_MIN = 0.0005   # Minimum 0.05% price move to be directional
MOMENTUM_LOOKBACK = 3   # Candles to look back for momentum (3 × 15m = 45 min)
MACD_MIN = 0.0001       # Neutral deadband: histogram must exceed 0.01% of price to count
                        # Normalized by current price in signals.py so it works across all assets
MIN_CONFIDENCE = 4    # All 4 signals must agree to enter
FORCE_TRADE = True    # Data collection mode: majority vote, trades every window

# --- Execution ---
LIMIT_ORDER_OFFSET = 0.02   # Place limit 2 cents below ask
ORDER_TIMEOUT_SECS = 120    # Cancel unfilled orders after 2 minutes
MAX_TRADES_PER_HOUR = 4     # 4 per hour per asset (paper.py multiplies by NUM_SLOTS)

# --- Portfolio ---
STARTING_BALANCE = float(os.getenv("STARTING_BALANCE", "1000.00"))
CASH_SPLIT = 0.50           # 50% of portfolio always in cash
CAPITAL_SPLIT = 0.50        # 50% available for trading
PROFIT_TO_CASH = 0.50       # 50% of each profit goes to cash

# --- Risk ---
DAILY_LOSS_LIMIT = 1.00     # Data collection: disabled (100% loss required to halt)
KELLY_FRACTION = 0.20       # Use 20% of full Kelly
MAX_BET = 10.00             # Kelly max — near-fair prices (YES 0.45–0.55)
MIN_BET = 3.00              # Kelly floor — market very confident (YES outside 0.35–0.65)
MAX_LOSING_STREAK = 999     # Data collection: disabled
LOSING_STREAK_REDUCTION = 1.0   # Data collection: no size reduction

# --- Bet Sizing (two tiers) ---
# $10 when YES is within 10 cents of 0.50 (genuine uncertainty zone)
# $5 when YES is 0.35–0.40 or 0.60–0.65 (market leaning but still tradeable)
BET_NEAR_FAIR   = 10.00   # YES 0.40–0.60: best EV zone
BET_SLIGHT_LEAN = 10.00   # (same as near-fair — merged into $10 tier)
BET_MOD_LEAN    =  5.00   # YES 0.35–0.65: market leaning, smaller bet
BET_STRONG_LEAN =  5.00   # (same as mod-lean — merged into $5 tier)

# --- Kalshi Contract Price Filter (near-fair zone) ---
# Only trade when YES is in this range — outside it the payout asymmetry makes
# positive EV mathematically impossible even with accurate signals.
# YES=0.65 requires 68%+ accuracy to break even; YES=0.50 requires only ~52%.
CONTRACT_PRICE_MIN = 0.35
CONTRACT_PRICE_MAX = 0.65

# --- News Context ---
NEWS_ENABLED         = True    # Toggle the news sentiment filter on/off
NEWS_MAX_AGE_SECS    = 86400   # 24h — NewsAPI free tier has ~24h delay; use yesterday's sentiment
NEWS_HIGH_CONFIDENCE = 8       # Score threshold for "high" confidence bias
NEWS_MED_CONFIDENCE  = 3       # Score threshold for "medium" confidence bias

# --- Intra-Window Position Management ---
# Stop-loss: sell the contract immediately if it drops to this value.
# At 0.25, a NO bought at 0.50 has lost half its value — cut and save the rest.
STOP_LOSS_PRICE  = 0.33   # Tightened from 0.25 — cuts failed contrarian bets earlier

# Trailing profit: once the contract reaches this value, arm a trailing exit.
# Exits when the price drops TRAILING_BUFFER cents below the observed peak.
# Buffer prevents premature exits on single-poll noise (e.g. armed at 0.75, dips
# to 0.74 then recovers — without buffer we'd exit; with buffer we stay in).
TRAILING_TRIGGER = 0.75
TRAILING_BUFFER  = 0.05   # Require 5-cent drop from peak before exiting

# --- Correlated-sweep protection ---
SWEEP_COOLOFF_LOSSES  = 3     # Losses in one window that trigger a 1-window cooloff
CONSEC_LOSS_THRESHOLD = 2     # Consecutive losses on one asset before bet reduction kicks in
CONSEC_LOSS_REDUCTION = 0.50  # Bet multiplier when asset is on a losing streak

# --- Kalshi Fees ---
KALSHI_MAKER_FEE = 0.0175   # Maker fee coefficient (limit orders)
KALSHI_TAKER_FEE = 0.07     # Taker fee coefficient (market orders)

