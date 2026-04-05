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
# MLB/NBA/NHL: external-probability arbitrage (5-min poll driven)
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
NUM_SLOTS = len(SLOTS)  # 4 — BTC, MLB, NBA, NHL

# --- Non-crypto slot settings ---
MARKET_EVAL_INTERVAL_SECS  = 30    # Poll sports slots every 30s — faster in-game lag capture
# Sports close_time is weeks away (settlement); game_date_filter (ticker date) is used instead.
SPORTS_EDGE_MIN            = 0.12  # In-game LONG edge threshold (edge-only tier)
SPORTS_SHORT_EDGE_MIN      = 0.20  # In-game SHORT requires larger edge — higher confidence bar
# Pre-game entry: three-tier system — confidence > vote-backed > edge-only
# Tier 1 — Confidence pick:  votes ≥ SPORTS_PREGAME_VOTE_CONFIDENCE AND model > 52% → edge ≥ 0.02
# Tier 2 — Vote-backed:      votes ≥ SPORTS_PREGAME_VOTE_MIN                       → edge ≥ 0.07
# Tier 3 — Edge-only:        any votes                                              → edge ≥ 0.12
SPORTS_PREGAME_VOTE_CONFIDENCE   = 4     # High-confidence tier: model picks winner, minimal edge required
SPORTS_PREGAME_CONFIDENCE_EDGE   = 0.02  # Edge floor for confidence-tier trades (model just needs to beat Kalshi)
SPORTS_PREGAME_EDGE_MIN          = 0.07  # Edge threshold for vote-backed tier (3+ votes)
SPORTS_PREGAME_VOTE_MIN          = 3     # Min votes (out of 6) to unlock vote-backed threshold
                                         # Votes: implied_prob>0.55(+2), L10(+1), venue_record(+1), H2H(+1), line_move(+1)
SPORTS_PREGAME_SHORT       = False # Disable pre-game SHORT — only LONG on confident pre-game winners
SPORTS_SHORT_MAX_NO_PRICE  = 0.65  # Block SHORT if NO ask > 0.65 — paying >65¢ for max $0.35 return is bad math
SPORTS_CONTRACT_PRICE_MIN  = 0.35  # In-game price floor (raised from 0.20 — no more extreme underdog buys)
SPORTS_CONTRACT_PRICE_MAX  = 0.80  # In-game price ceiling
SPORTS_PREGAME_PRICE_MIN   = 0.40  # Pre-game floor (raised from 0.35 — tighter near-fair zone)
SPORTS_PREGAME_PRICE_MAX   = 0.70  # Pre-game ceiling
SPORTS_INGAME_COOLOFF_MINS = 20    # Minimum minutes between re-entries on same live market
SPORTS_MAX_GAMES_PER_SLOT  = 5     # Max unique game matchups per sports slot per day
SPORTS_MAX_TRADES_PER_GAME = 2     # Max trades per game matchup (raised from 1 — allows second in-game entry)
SPORTS_DAILY_BUDGET_PCT    = 0.75  # Each sport slot spends up to 75% of its slot capital per day
INGAME_STALE_MARKET_SECS   = 600   # Skip in-game market if Kalshi YES price unchanged >10 min
MARKET_MAX_CLOSE_HOURS     = 48.0  # Sports markets: look 48h ahead — covers tomorrow's full schedule

# --- Market ---
INTERVALS = {
    "trend": "1h",    # RSI + MACD filter
    "entry": "15m",   # Momentum + VWAP signal
    "ticker": "1m",   # Live price for order placement
}
CANDLE_LIMIT = 200    # Historical candles to load on startup

# --- Strategy Thresholds ---
RSI_PERIOD = 14
MOMENTUM_MIN = 0.001    # Minimum 0.10% price move to be directional (was 0.05% — too noisy)
MOMENTUM_LOOKBACK = 3   # Candles to look back for momentum (3 × 15m = 45 min)
MACD_MIN = 0.0003       # Neutral deadband: histogram must exceed 0.03% of price to count
                        # Normalized by current price in signals.py so it works across all assets
VWAP_MIN_PCT = 0.002    # Block trade if price is >0.20% from VWAP (already overextended)
# --- BTC Streak Mean-Reversion Strategy ---
# After STREAK_LENGTH consecutive closes in one direction, bet the reverse.
# STREAK_MACD_CONFIRM=True: also require MACD histogram to agree (higher WR, lower frequency).
#   STREAK_LENGTH=2, STREAK_MACD_CONFIRM=False → ~33% windows, ~68% WR (out-of-sample)
#   STREAK_LENGTH=2, STREAK_MACD_CONFIRM=True  → ~11% windows, ~79% WR (out-of-sample)
STREAK_LENGTH      = 2     # Consecutive same-direction closes required to trigger
STREAK_MACD_CONFIRM = True   # Require MACD to confirm (True = higher WR, lower frequency)

MIN_CONFIDENCE = 2    # Minimum confidence to place trade (1 = streak alone, 2 = streak+MACD)
FORCE_TRADE = False

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
# BTC:    confidence-scaled % of slot_capital, hard-capped at BTC_MAX_BET
#           conf < 50%  → BTC_BET_PCT_LOW  (10%) of slot_capital
#           conf 50-74% → BTC_BET_PCT_MID  (15%) of slot_capital
#           conf 75%+   → BTC_BET_PCT_HIGH (20%) of slot_capital
#           hard cap: min(calculated, BTC_MAX_BET)
# Sports: daily budget = SPORTS_DAILY_BUDGET_PCT × slot_capital per sport slot
#         per-game bet = daily_budget / min(N_tradeable_now, remaining_capacity)
#         — fractions up when fewer good games available, spreads thin when many
#
# With $500 starting balance:
#   capital = $250  →  slot_capital = $62.50
#                                   →  BTC bet (50-74% conf) = $9.38, capped at $25
#                                   →  BTC bet (75%+ conf)   = $12.50, capped at $25
#                                   →  Sports daily budget = $46.88 (75%)
#                                   →  Sports per-game cap = $15.63 (25% of slot)
#
# After every trade portfolio.capital updates automatically, so bet size
# self-adjusts without any separate rebalance step:
#   win  → capital grows  → next bet slightly larger
#   loss → capital shrinks → next bet slightly smaller
#   cash is NEVER used for losses (record_loss touches capital only)
#
SLOT_CAPITAL_PCT        = 0.25   # 25% of capital pool per slot (4 slots)
BET_PCT_OF_SLOT         = 0.50   # Sports only: 50% of slot capital = daily budget
BTC_BET_PCT_LOW         = 0.10   # BTC conf <50%:  10% of slot capital
BTC_BET_PCT_MID         = 0.15   # BTC conf 50-74%: 15% of slot capital
BTC_BET_PCT_HIGH        = 0.20   # BTC conf 75%+:  20% of slot capital
BTC_MAX_BET             = 25.00  # Hard dollar cap on any single BTC trade
SPORTS_MAX_BET_PCT      = 0.25   # Hard cap: no single sports game bet exceeds 25% of slot capital

# --- Kalshi Contract Price Filter (near-fair zone) ---
# Only trade when YES is in this range — outside it the payout asymmetry makes
# positive EV mathematically impossible even with accurate signals.
# YES=0.65 requires 68%+ accuracy to break even; YES=0.50 requires only ~52%.
CONTRACT_PRICE_MIN = 0.52
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

