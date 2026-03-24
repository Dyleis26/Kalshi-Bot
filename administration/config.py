import os
from pathlib import Path
from dotenv import load_dotenv

# Always load .env from the project root regardless of where the script is run from
load_dotenv(Path(__file__).parent.parent / ".env")

# --- API Keys ---
KALSHI_API_KEY = os.getenv("KALSHI_API_KEY")
KALSHI_KEY_PATH = os.getenv(
    "KALSHI_KEY_PATH",
    str(Path(__file__).parent / "kalshi_key.pem")
)

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

# --- Market ---
INTERVALS = {
    "trend": "1h",    # RSI + MACD filter
    "entry": "15m",   # Momentum + VWAP signal
    "ticker": "1m",   # Live price for order placement
}
CANDLE_LIMIT = 200    # Historical candles to load on startup

# --- Strategy Thresholds ---
RSI_BULL = 55         # 1H RSI above this = bullish bias
RSI_BEAR = 45         # 1H RSI below this = bearish bias
RSI_PERIOD = 14
MOMENTUM_MIN = 0.0005 # Minimum 0.05% price move over last 2 candles
MIN_CONFIDENCE = 4    # All 4 signals must agree to enter
FORCE_TRADE = True    # Data collection mode: majority vote, trades every window

# --- Execution ---
LIMIT_ORDER_OFFSET = 0.02   # Place limit 2 cents below ask
ORDER_TIMEOUT_SECS = 120    # Cancel unfilled orders after 2 minutes
MAX_TRADES_PER_HOUR = 4     # 4 per hour = every 15M window (data collection)

# --- Portfolio ---
STARTING_BALANCE = float(os.getenv("STARTING_BALANCE", "1000.00"))
CASH_SPLIT = 0.50           # 50% of portfolio always in cash
CAPITAL_SPLIT = 0.50        # 50% available for trading
PROFIT_TO_CASH = 0.50       # 50% of each profit goes to cash
PROFIT_TO_CAPITAL = 0.50    # 50% of each profit stays in capital

# --- Risk ---
DAILY_LOSS_LIMIT = 1.00     # Data collection: disabled (100% loss required to halt)
KELLY_FRACTION = 0.20       # Use 20% of full Kelly
MAX_BET = 50.00             # Hard cap per trade in dollars
MIN_BET = 10.00             # Minimum trade size in dollars
MAX_LOSING_STREAK = 999     # Data collection: disabled
LOSING_STREAK_REDUCTION = 1.0   # Data collection: no size reduction

# --- Kalshi Fees ---
KALSHI_MAKER_FEE = 0.0175   # Maker fee coefficient (limit orders)
KALSHI_TAKER_FEE = 0.07     # Taker fee coefficient (market orders)

# --- No-Trade Windows (minutes before/after macro events) ---
NO_TRADE_BUFFER_MINS = 30
