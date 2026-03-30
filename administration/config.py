import os
from pathlib import Path
from dotenv import load_dotenv

# Always load .env from the project root regardless of where the script is run from
load_dotenv(Path(__file__).parent.parent / ".env")

# --- API Keys ---
KALSHI_API_KEY       = os.getenv("KALSHI_API_KEY")
CRYPTOPANIC_API_KEY  = os.getenv("CRYPTOPANIC_API_KEY", "")
NEWSAPI_KEY          = os.getenv("NEWSAPI_KEY", "")
ODDS_API_KEY         = os.getenv("ODDS_API_KEY", "")   # The Odds API — free tier 500 req/month
KALSHI_KEY_PATH = os.getenv(
    "KALSHI_KEY_PATH",
    str(Path(__file__).parent / "kalshi_key.pem")
)

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL")

# --- Assets (kept for backtest / optimizer — unchanged) ---
ASSETS = {
    "BTC":  {"kraken_rest": "XBTUSD",  "kraken_ws": "BTC/USD",  "kalshi_series": "KXBTC15M"},
    "ETH":  {"kraken_rest": "ETHUSD",  "kraken_ws": "ETH/USD",  "kalshi_series": "KXETH15M"},
    "SOL":  {"kraken_rest": "SOLUSD",  "kraken_ws": "SOL/USD",  "kalshi_series": "KXSOL15M"},
    "XRP":  {"kraken_rest": "XRPUSD",  "kraken_ws": "XRP/USD",  "kalshi_series": "KXXRP15M"},
    "DOGE": {"kraken_rest": "XDGUSD",  "kraken_ws": "DOGE/USD", "kalshi_series": "KXDOGE15M"},
}

# --- Live Trading Slots (5 market slots used by paper.py / live trader) ---
# BTC: 15-min crypto Up/Down (Kraken WebSocket driven)
# WEATHER/MLB/NBA/NHL: external-probability arbitrage (5-min poll driven)
#
# NOTE: Kalshi series prefixes for sports/weather are guesses based on known
# Kalshi conventions. Run `python -c "from administration.kalshi import KalshiClient;
# c=KalshiClient(); print(c.get_open_markets_by_category())"` to verify live tickers,
# then update the series fields below.
SLOTS = {
    "BTC": {
        "type":        "crypto",
        "label":       "BTC",
        "series":      "KXBTC15M",
        "kraken_rest": "XBTUSD",
        "kraken_ws":   "BTC/USD",
    },
    "WEATHER": {
        "type":       "weather",
        "label":      "Weather",
        "series":     "KXHIGHNY",     # Kalshi NYC daily high-temperature series
        "city":       "New York City",
        "lat":        40.7128,
        "lng":        -74.0060,
    },
    "MLB": {
        "type":             "sports",
        "label":            "MLB",
        "series":           "KXMLBGAME",  # Kalshi MLB game-winner series
        "espn_sport":       "baseball/mlb",
        "game_date_filter": True,          # filter by game date in ticker, not close_time
    },
    "NBA": {
        "type":             "sports",
        "label":            "NBA",
        "series":           "KXNBAGAME",  # Kalshi NBA game-winner series
        "espn_sport":       "basketball/nba",
        "game_date_filter": True,
    },
    "NHL": {
        "type":             "sports",
        "label":            "NHL",
        "series":           "KXNHLGAME",  # Kalshi NHL game-winner series
        "espn_sport":       "hockey/nhl",
        "game_date_filter": True,
    },
}
NUM_SLOTS = len(SLOTS)  # 5 — one capital slot per market type

# --- Non-crypto slot settings ---
MARKET_EVAL_INTERVAL_SECS = 120   # Poll weather/sports slots every 2 minutes (faster in-game edge capture)
# Weather close_time is ~30h away (next-day markets); sports close_time is weeks away (settlement).
# Sports slots use game_date_filter (ticker date) instead — this value only matters for weather.
SPORTS_EDGE_MIN            = 0.08  # Need ≥8% edge over Kalshi YES price to enter
SPORTS_CONTRACT_PRICE_MIN  = 0.20  # Broader range for in-game (pre-game uses 0.35)
SPORTS_CONTRACT_PRICE_MAX  = 0.80  # In-game favorites can be 0.80+ and still have edge
SPORTS_INGAME_COOLOFF_MINS = 20    # Minimum minutes between re-entries on same live market
INGAME_STALE_MARKET_SECS   = 600   # Skip in-game market if Kalshi YES price unchanged >10 min
WEATHER_EDGE_MIN           = 0.10  # Need ≥10% edge over Kalshi YES price to enter
MARKET_MAX_CLOSE_HOURS     = 36.0  # Weather markets close ~30h away (next-day forecast)

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
STARTING_BALANCE = float(os.getenv("STARTING_BALANCE", "500.00"))
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

# --- Bet Sizing (percentage-based, dynamic) ---
# Each trade = BET_PCT_OF_SLOT × slot_capital
#            = BET_PCT_OF_SLOT × (portfolio.capital × SLOT_CAPITAL_PCT)
#
# With $250 starting balance:
#   capital = $125  →  slot_capital = $12.50  →  bet = $3.13
#
# After every trade portfolio.capital updates automatically, so bet size
# self-adjusts without any separate rebalance step:
#   win  → capital grows  → next bet slightly larger
#   loss → capital shrinks → next bet slightly smaller
#   cash is NEVER used for losses (record_loss touches capital only)
#
SLOT_CAPITAL_PCT = 0.10   # 10% of capital pool per slot (capital ÷ NUM_SLOTS)
BET_PCT_OF_SLOT  = 0.50   # 50% of slot's capital allocation per trade

# --- Kalshi Contract Price Filter (near-fair zone) ---
# Only trade when YES is in this range — outside it the payout asymmetry makes
# positive EV mathematically impossible even with accurate signals.
# YES=0.65 requires 68%+ accuracy to break even; YES=0.50 requires only ~52%.
CONTRACT_PRICE_MIN = 0.35
CONTRACT_PRICE_MAX = 0.65
CONTRACT_BUY_MIN   = 0.05  # Hard floor on actual purchase price — blocks near-zero contracts
CONTRACT_BUY_MAX   = 0.95  # Hard cap on actual purchase price — blocks illiquid NO asks (e.g. no_ask=0.98)

# --- Equity Futures Trend (direct signal for BTC macro regime) ---
EQUITY_TREND_ENABLED   = True   # Toggle equity futures signal on/off
EQUITY_TREND_THRESHOLD = 0.0015 # ±0.15% over lookback window = directional signal
EQUITY_LOOKBACK_BARS   = 3      # 3 × 5-min bars = 15-minute window (matches BTC window)

# --- News Context ---
NEWS_ENABLED         = True    # Toggle the news sentiment filter on/off
NEWS_MAX_AGE_SECS    = 172800  # 48h — NewsAPI free tier ~24-26h delay; 48h window reliably captures yesterday's articles
NEWS_HIGH_CONFIDENCE = 8       # Score threshold for "high" confidence bias
NEWS_MED_CONFIDENCE  = 3       # Score threshold for "medium" confidence bias

# --- Intra-Window Position Management ---
# Stop-loss: sell the contract immediately if it drops to this value.
# At 0.25, a NO bought at 0.50 has lost half its value — cut and save the rest.
STOP_LOSS_PRICE  = 0.00   # DISABLED — let trades run to settlement for signal accuracy testing

# Trailing profit: DISABLED — let trades run to settlement for signal accuracy testing
TRAILING_TRIGGER = 1.01   # Never triggers (contract price never exceeds 1.0)
TRAILING_BUFFER  = 0.05

# --- Correlated-sweep protection ---
SWEEP_COOLOFF_LOSSES  = 3     # Losses in one window that trigger a 1-window cooloff
CONSEC_LOSS_THRESHOLD = 2     # Consecutive losses on one asset before bet reduction kicks in
CONSEC_LOSS_REDUCTION = 0.50  # Bet multiplier when asset is on a losing streak

# --- Funding Rate (Binance perpetual — contrarian signal for crypto) ---
# Positive rate = market net long (longs pay shorts) → contrarian SHORT
# Negative rate = market net short (shorts pay longs) → contrarian LONG
FUNDING_RATE_BULL_THRESHOLD = -0.0001   # rate <= this → bull signal (crowded short)
FUNDING_RATE_BEAR_THRESHOLD =  0.0003   # rate >= this → bear signal (crowded long)

# --- Fear & Greed Index (Alternative.me — contrarian signal for crypto) ---
# Only extreme readings count as signals (single vote in the bias pool)
FNG_BULL_MAX =  25   # value <= this (Extreme Fear) → contrarian bull
FNG_BEAR_MIN =  75   # value >= this (Extreme Greed) → contrarian bear

# --- Kalshi Fees ---
KALSHI_MAKER_FEE = 0.0175   # Maker fee coefficient (limit orders)
KALSHI_TAKER_FEE = 0.07     # Taker fee coefficient (market orders)

