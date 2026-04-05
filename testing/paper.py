import time
import math
import os
import json
import signal
import threading
import pandas as pd
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from administration.portfolio import Portfolio
from administration.monitor import Monitor
from administration.discord import Discord
from administration.config import (
    STARTING_BALANCE, MAX_TRADES_PER_HOUR, KALSHI_MAKER_FEE, FORCE_TRADE, SLOTS,
    CONTRACT_PRICE_MIN, CONTRACT_PRICE_MAX, CONTRACT_BUY_MIN, CONTRACT_BUY_MAX, NEWS_ENABLED,
    SLOT_CAPITAL_PCT, BET_PCT_OF_SLOT, NUM_SLOTS,
    BTC_BET_PCT_LOW, BTC_BET_PCT_MID, BTC_BET_PCT_HIGH, BTC_MAX_BET,
    STOP_LOSS_PRICE, TRAILING_TRIGGER, TRAILING_BUFFER,
    SWEEP_COOLOFF_LOSSES, CONSEC_LOSS_THRESHOLD, CONSEC_LOSS_REDUCTION,
    MARKET_EVAL_INTERVAL_SECS, MARKET_MAX_CLOSE_HOURS, SPORTS_INGAME_COOLOFF_MINS,
    INGAME_STALE_MARKET_SECS, SPORTS_MAX_GAMES_PER_SLOT, SPORTS_MAX_TRADES_PER_GAME,
    SPORTS_DAILY_BUDGET_PCT, SPORTS_MAX_BET_PCT,
)
from administration.news import NewsContext
from administration.kalshi import KalshiClient
from data.kraken import KrakenFeed
from data.history import History
from data.trades import TradeLog
from strategy.base import Strategy, LONG, SHORT, NONE
from strategy.sports import SportsStrategy
from administration.logger import get as get_logger, log_trade, log_halt
import data.kalshi_market_log as market_log
import data.btc_signal_log as signal_log
import data.sports_outcome_log as sports_log

logger = get_logger("paper")

# Paths for persisting crypto trade windows and open positions across restarts
_TRADE_STATE_FILE      = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", ".btc_last_trade.json")
_OPEN_TRADES_FILE      = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", ".open_trades.json")
_SPORTS_STATE_FILE     = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", ".sports_state.json")
_PORTFOLIO_STATE_FILE  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", ".portfolio_state.json")
_SESSION_STATE_FILE    = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", ".session_state.json")


class Trader:
    """
    Live and paper trading across 4 market slots:
      - BTC:  15-min crypto Up/Down driven by Kraken WebSocket
      - MLB:  ESPN win probability vs Kalshi MLB market (2-min poll)
      - NBA:  ESPN win probability vs Kalshi NBA market (2-min poll)
      - NHL:  ESPN win probability vs Kalshi NHL market (2-min poll)

    Sports capital model: each slot has a daily budget = SPORTS_DAILY_BUDGET_PCT × slot_capital.
    Budget is spread across up to SPORTS_MAX_GAMES_PER_SLOT unique games; sizing fractions up
    when fewer good games are found. One trade per game matchup (no re-entries).

    live=False → paper mode: simulates orders, uses Kalshi demo API
    live=True  → live mode: places real orders on Kalshi, syncs balance at startup
    """

    def __init__(self, live: bool = False, starting_balance: float = STARTING_BALANCE):
        self.live      = live
        self.portfolio = Portfolio(starting_balance)  # always fresh on restart
        self.monitor   = Monitor()
        self.discord   = Discord(paper=not live)
        self.trade_log = TradeLog(mode="live" if live else "paper")
        self.kalshi    = KalshiClient(paper=not live)

        # Crypto-specific components (BTC only)
        self.feed     = KrakenFeed()
        self.strategy = Strategy()
        self.btc_state = {
            "df_1h":   pd.DataFrame(),
            "df_15m":  pd.DataFrame(),
            "price":   0.0,
            "history": History("BTC", feed=self.feed),
        }

        # Non-crypto strategy instances
        self._sports_strategy = SportsStrategy()

        # BTC signal evaluation lock — prevents concurrent strategy.decide() calls
        # on the WebSocket thread (each decide() makes up to 3 blocking HTTP requests).
        self._eval_lock = threading.Lock()

        # Per-slot rate limiting and state tracking
        self.trades_this_hour  = {k: 0 for k in SLOTS}
        self.hour_window_start = {k: time.monotonic() for k in SLOTS}

        # Sports budget tracking — resets at UTC midnight
        _sports_slots = [k for k, v in SLOTS.items() if v["type"] == "sports"]
        # Games bet per slot today (replaces old session_trade_counts)
        self._sport_games_bet: dict = {k: 0 for k in _sports_slots}
        # Dollars spent from each sport slot's daily budget today
        self._sport_budget_spent: dict = {k: 0.0 for k in _sports_slots}
        # Slot capital snapshot taken at day-start (budget baseline doesn't drift intra-day)
        _initial_slot_cap = self.portfolio.capital * SLOT_CAPITAL_PCT
        self._sport_budget_snap: dict = {k: _initial_slot_cap for k in _sports_slots}

        # Kalshi ticker cache (15s TTL — BTC crypto slot)
        self._ticker_cache: dict = {
            "BTC": {"ticker": None, "ts": 0.0},
        }
        # File lock for safe open-trades file writes from multiple threads
        self._file_lock = threading.Lock()

        # Session stats
        self.session_wins   = 0
        self.session_losses = 0
        self.session_pnl    = 0.0
        self._open_stake    = 0.0
        self._last_1h_refresh = 0.0

        # One-trade-per-window/market: stores the last traded ticker per slot (crypto: datetime; others: unused)
        self._last_trade_key: dict = {k: None for k in SLOTS}
        # Session is always fresh on restart — no state restored from disk
        # Accumulates every ticker traded this session per slot
        self._traded_tickers: dict = {k: set() for k in SLOTS}
        # In-game re-entry cooloff: {game_key: last_entry_monotonic_time}
        self._ingame_trade_times: dict = {}
        # Per-game daily trade count: {game_key: count} — resets at UTC midnight
        self._game_trade_counts: dict = {}

        # Correlated-sweep protection
        self._consec_losses:        dict = {k: 0 for k in SLOTS}
        self._tracked_windows:      dict = {}
        self._sweep_cooloff_window       = None  # skip BTC trades in this 15M window


        # Last non-crypto poll timestamp
        self._last_market_poll = 0.0

        # Stale market filter: track last YES price change time per ticker
        # Format: {ticker: {"price": float, "last_changed": monotonic_time}}
        self._market_price_seen: dict = {}

        # In-game price direction filter: track last 3 yes_ask values per ticker
        # Used to reject trades where Kalshi is actively moving against our direction
        # Format: {ticker: [(monotonic_time, yes_ask), ...]}  (max 3 entries)
        self._market_price_history: dict = {}

        self.running      = False
        self._stopped     = False
        self._lock        = threading.Lock()
        self._ready_at    = None
        self._last_reset_date = None
        # UTC midnight session cap reset (separate from ET-based portfolio day reset)
        self._last_session_reset_utc = None

    # ------------------------------------------------------------------ #
    #  Lifecycle                                                           #
    # ------------------------------------------------------------------ #

    def start(self):
        mode_str = "LIVE" if self.live else "paper"
        logger.info(f"Trader starting [{mode_str}] (4 slots: BTC + MLB + NBA + NHL)...")
        self.discord.start()

        if self.live:
            live_balance = self.kalshi.get_balance()
            if live_balance > 0:
                self.portfolio = Portfolio(live_balance)
                logger.info(f"Live mode: synced Kalshi balance — ${live_balance:.2f}")
            else:
                logger.warning("Live mode: could not fetch Kalshi balance — using STARTING_BALANCE")

        self.trade_log.reset()

        # Load BTC historical candles
        logger.info("Loading history for BTC...")
        data = self.btc_state["history"].load_all()
        self.btc_state["df_1h"]  = data["1h"]
        self.btc_state["df_15m"] = data["15m"]

        self.monitor.set_connected("kraken",  True)
        self.monitor.set_connected("kalshi",  True)
        self.monitor.set_connected("discord", self.discord.is_ready())

        self.discord.bot_started(self.portfolio.total)
        self.running   = True
        self._ready_at = time.monotonic() + 10

        signal.signal(signal.SIGTERM, lambda s, f: self.stop("SIGTERM"))

        self.feed.start_streams(
            on_15m=self._on_15m_candle,
            on_tick=self._on_tick,
        )

        logger.info("Trader running. BTC driven by WebSocket; MLB/NBA/NHL polled every 2 min.")

        # Clear all persisted position state on every restart — no open trades carry over.
        # This keeps every session fully isolated for clean testing.
        self._clear_position_state()

        if NEWS_ENABLED:
            t = threading.Thread(
                target=NewsContext.fetch, args=[["BTC"]], daemon=True
            )
            t.start()

        self._heartbeat()

    def stop(self, reason: str = "Manual stop"):
        self.running = False
        self.feed.stop_streams()
        if not self._stopped:
            self._stopped = True
            self.discord.bot_stopped(self.portfolio.total)
        self.discord.stop()
        logger.info(f"Trader stopped: {reason}")

    # ------------------------------------------------------------------ #
    #  WebSocket Handlers (BTC)                                          #
    # ------------------------------------------------------------------ #

    def _on_15m_candle(self, asset: str, candle: dict):
        if asset != "BTC":
            return
        state = self.btc_state
        with self._lock:
            row = pd.DataFrame([candle])[["time", "open", "high", "low", "close", "volume"]]
            candle_time = row["time"].iloc[0]
            df = state["df_15m"]
            if not df.empty and pd.Timestamp(df["time"].iloc[-1]) == pd.Timestamp(candle_time):
                state["df_15m"].iloc[-1] = row.iloc[0]
            else:
                state["df_15m"] = pd.concat([df, row], ignore_index=True).tail(300)
        state["history"].append(candle, "15m")
        # Run evaluation in a background thread so blocking HTTP requests inside
        # strategy.decide() (funding rate, F&G, equity futures) don't stall the
        # WebSocket callback and cause missed ticks or WS timeouts.
        t = threading.Thread(target=self._evaluate_crypto_bg, daemon=True)
        t.start()

    def _evaluate_crypto_bg(self):
        """Non-blocking wrapper for _evaluate_crypto: skips if evaluation already in progress."""
        if not self._eval_lock.acquire(blocking=False):
            return  # Previous candle's evaluation is still running — skip this one
        try:
            self._evaluate_crypto()
        finally:
            self._eval_lock.release()

    def _on_tick(self, asset: str, price: float):
        if asset == "BTC":
            with self._lock:
                self.btc_state["price"] = price

    # ------------------------------------------------------------------ #
    #  Crypto Evaluation (BTC)                                            #
    # ------------------------------------------------------------------ #

    def _get_crypto_state(self, slot_key: str) -> dict:
        """Return the per-asset state dict for a crypto slot (BTC only)."""
        return self.btc_state

    def _evaluate_crypto(self):
        if time.monotonic() < self._ready_at:
            return
        if not FORCE_TRADE and not self.portfolio.can_trade():
            return
        if not self._within_trade_limit("BTC"):
            return

        # Sweep-loss cooloff: skip one 15M window after SWEEP_COOLOFF_LOSSES+ simultaneous losses
        _ev_now = datetime.now(timezone.utc)
        _ev_min = _ev_now.minute - (_ev_now.minute % 15)
        eval_window = _ev_now.replace(minute=_ev_min, second=0, microsecond=0, tzinfo=None)
        with self._lock:
            cooloff = self._sweep_cooloff_window
        if cooloff is not None and eval_window <= cooloff:
            logger.info(f"BTC: sweep-loss cooloff active — skipping {eval_window.strftime('%H:%M')} window")
            self._release_trade_slot("BTC")
            return

        state = self.btc_state
        if len(state["df_1h"]) < 35 or len(state["df_15m"]) < 10:
            return

        try:
            latest_candle_time = pd.to_datetime(
                state["df_15m"]["time"].iloc[-1], format="mixed"
            )
            if latest_candle_time.tzinfo is None:
                latest_candle_time = latest_candle_time.replace(tzinfo=timezone.utc)
            age_minutes = (datetime.now(timezone.utc) - latest_candle_time).total_seconds() / 60
            if age_minutes > 30:
                return
        except Exception:
            return

        with self._lock:
            df_1h_snap  = state["df_1h"].copy()
            df_15m_snap = state["df_15m"].copy()

        decision = self.strategy.decide(df_1h_snap, df_15m_snap, asset="BTC")

        # Log every window to the market observer. Kalshi sometimes returns 1.0
        # for the new market in the first ~30s after :00/:15/:30/:45. If that
        # happens, spawn a background thread to retry after 30s.
        def _try_log_market_open(btc_price: float, attempts: int = 3):
            for _ in range(attempts):
                time.sleep(30)
                try:
                    # Refetch the ticker — the old window's ticker always returns 1.0
                    m = self.kalshi.get_market_for_asset("BTC")
                    t = m.get("ticker") if m else None
                    if not t:
                        continue
                    yes, no = self.kalshi.get_market_prices(t)
                    if 0.05 <= yes <= 0.95:
                        market_log.log_open(t, yes, no, btc_price)
                        return
                except Exception:
                    return

        try:
            _obs_ticker = self._get_ticker_for_slot("BTC")
            if _obs_ticker:
                _obs_yes, _obs_no = self.kalshi.get_market_prices(_obs_ticker)
                _obs_btc = float(state.get("price", 0))
                if 0.05 <= _obs_yes <= 0.95:
                    market_log.log_open(_obs_ticker, _obs_yes, _obs_no, _obs_btc)
                else:
                    # Market transitioning — retry in background with fresh ticker
                    _t = threading.Thread(
                        target=_try_log_market_open,
                        args=(_obs_btc,),
                        daemon=True
                    )
                    _t.start()
        except Exception:
            pass

        direction = decision["direction"]
        self.monitor.record_signal(direction, decision["signals"])

        # Log every BTC window to signal history (trade=False for now; marked
        # True later if _execute_crypto_trade actually places an order).
        signal_log.log_window(decision, traded=False)

        if direction == NONE:
            logger.info(f"BTC: no trade — {decision['reason']}")
            return

        t = threading.Thread(
            target=self._execute_crypto_trade,
            args=["BTC", direction, decision]
        )
        t.daemon = True
        t.start()

    def _get_ticker_for_slot(self, slot_key: str) -> str | None:
        """Return the current Kalshi ticker for the BTC crypto slot with 15s TTL cache."""
        now = time.monotonic()
        cache = self._ticker_cache[slot_key]
        if cache["ticker"] and (now - cache["ts"]) < 15:
            return cache["ticker"]
        m = self.kalshi.get_market_for_asset(slot_key)
        ticker = m.get("ticker") if m else None
        self._ticker_cache[slot_key] = {"ticker": ticker, "ts": now}
        return ticker

    def _execute_crypto_trade(self, slot_key: str, direction: str, decision: dict):
        """Execute a BTC 15-minute Up/Down trade with full contrarian + near-fair logic."""
        # Extract and enrich signals with decision metadata for trade storage
        signals        = dict(decision["signals"])
        confidence     = decision["confidence"]
        confidence_pct = decision["confidence_pct"]
        signals.update({
            "bull_votes":    decision.get("bull_votes"),
            "bear_votes":    decision.get("bear_votes"),
            "funding_rate":  decision.get("funding_rate"),
            "fng_value":     decision.get("fng_value"),
            "news_bias":     decision.get("news_bias"),
            "news_score":    decision.get("news_score"),
            "equity_bias":   decision.get("equity_bias"),
            "equity_change": decision.get("equity_change"),
        })

        # 07:00 UTC blackout: European cash open floods volume and invalidates overnight signals.
        # Data shows 0% win rate and -$42.70 PnL in this hour across all sessions.
        _now_cw = datetime.now(timezone.utc)
        if _now_cw.hour == 7:
            logger.info(f"{slot_key}: skipped — 07:00 UTC blackout (European open volatility)")
            self._release_trade_slot(slot_key)
            return

        # BTC crash regime filter: if BTC has dropped >3% over the last 6 hours, signals built
        # on pre-crash data are unreliable. All-time record: <$67k regime 42% WR, -$96 PnL.
        try:
            _df1h = self.btc_state.get("df_1h")
            if _df1h is not None and len(_df1h) >= 7:
                _close_now  = float(_df1h["close"].iloc[-1])
                _close_6h   = float(_df1h["close"].iloc[-7])
                _pct_change = (_close_now - _close_6h) / _close_6h
                if _pct_change <= -0.03:
                    logger.info(
                        f"{slot_key}: skipped — BTC crash regime "
                        f"({_pct_change:+.2%} over 6h, signals unreliable)"
                    )
                    self._release_trade_slot(slot_key)
                    return
        except Exception:
            pass

        _cw_min = _now_cw.minute - (_now_cw.minute % 15)
        current_window = _now_cw.replace(minute=_cw_min, second=0, microsecond=0, tzinfo=None)
        with self._lock:
            last_key = self._last_trade_key[slot_key]
        if last_key == current_window:
            logger.info(f"{slot_key}: already traded this window ({current_window.strftime('%H:%M')}) — skipping")
            self._release_trade_slot(slot_key)
            return

        kalshi_ticker = self._get_ticker_for_slot(slot_key)
        if not kalshi_ticker:
            logger.info(f"{slot_key}: no Kalshi market found — skipping trade")
            self._release_trade_slot(slot_key)
            return

        # Retry loop: wait up to 14 min for a real price
        retry_start  = time.monotonic()
        WINDOW_SECS  = 14 * 60
        attempt      = 0
        yes_price, no_price = 1.0, 1.0
        while time.monotonic() - retry_start < WINDOW_SECS:
            fresh_ticker = self._get_ticker_for_slot(slot_key) or kalshi_ticker
            yes_price, no_price = self.kalshi.get_market_prices(fresh_ticker)
            if 0.05 <= yes_price <= 0.95:
                kalshi_ticker = fresh_ticker
                break
            attempt += 1
            remaining = WINDOW_SECS - (time.monotonic() - retry_start)
            if remaining < 30:
                break
            logger.info(f"{slot_key}: Kalshi not priced (yes={yes_price:.2f}) — retrying in 30s (attempt {attempt})")
            time.sleep(30)

        if not (0.05 <= yes_price <= 0.95):
            logger.info(f"{slot_key}: no real Kalshi price available — skipping trade")
            self._release_trade_slot(slot_key)
            return

        if not (CONTRACT_PRICE_MIN <= yes_price <= CONTRACT_PRICE_MAX):
            logger.info(
                f"{slot_key}: market too confident (yes={yes_price:.2f}) — "
                f"outside near-fair zone [{CONTRACT_PRICE_MIN:.2f}, {CONTRACT_PRICE_MAX:.2f}], skipping"
            )
            self._release_trade_slot(slot_key)
            return

        market_direction = LONG if yes_price >= 0.50 else SHORT
        is_contrarian    = (market_direction != direction)

        if is_contrarian:
            price_above_vwap = signals["price"] > signals["vwap"]
            if direction == SHORT and not price_above_vwap:
                logger.info(
                    f"{slot_key}: contrarian SHORT skipped — price below VWAP "
                    f"({signals['price']:.2f} < {signals['vwap']:.2f}), no mean-reversion setup"
                )
                self._release_trade_slot(slot_key)
                return
            if direction == LONG and price_above_vwap:
                logger.info(
                    f"{slot_key}: contrarian LONG skipped — price above VWAP "
                    f"({signals['price']:.2f} > {signals['vwap']:.2f}), no mean-reversion setup"
                )
                self._release_trade_slot(slot_key)
                return

            # Block only when RSI slope actively opposes the direction; neutral RSI passes.
            rsi_opposes = (
                (direction == SHORT and signals["rsi_bias"] == "bull") or
                (direction == LONG  and signals["rsi_bias"] == "bear")
            )
            if rsi_opposes:
                logger.info(
                    f"{slot_key}: contrarian {direction.upper()} skipped — "
                    f"RSI slope ({signals['rsi_bias']}) actively opposes direction"
                )
                self._release_trade_slot(slot_key)
                return

            logger.info(
                f"{slot_key}: contrarian — signal={direction} vs market yes={yes_price:.2f} "
                f"(buying {'YES at discount' if direction == LONG else 'NO at discount'})"
            )

        vwap_pos   = "above" if signals["price"] > signals["vwap"] else "below"
        equity_str = ""
        if signals.get("equity_change") is not None:
            equity_str = f" | equity={signals['equity_change']:+.2%}({signals['equity_bias']})"
        logger.info(
            f"{slot_key}: signal-context | rsi={signals['rsi']:.1f}({signals['rsi_bias']}) | "
            f"price {vwap_pos} vwap ({signals['price']:.2f} vs {signals['vwap']:.2f}){equity_str}"
        )

        side           = "yes" if direction == LONG else "no"
        contract_price = yes_price if direction == LONG else no_price

        # Confidence-scaled BTC sizing: smaller bets at low confidence, larger at high.
        # Rebalances automatically after every trade since portfolio.capital is live.
        with self._lock:
            slot_capital = self.portfolio.capital * SLOT_CAPITAL_PCT
            consec = self._consec_losses[slot_key]
        if confidence_pct >= 75.0:
            btc_pct = BTC_BET_PCT_HIGH
        elif confidence_pct >= 50.0:
            btc_pct = BTC_BET_PCT_MID
        else:
            btc_pct = BTC_BET_PCT_LOW
        size = round(min(slot_capital * btc_pct, BTC_MAX_BET), 2)
        if consec >= CONSEC_LOSS_THRESHOLD:
            size = round(size * CONSEC_LOSS_REDUCTION, 2)
            logger.info(f"{slot_key}: cooldown active ({consec} consecutive losses) — bet reduced to ${size:.2f}")
        logger.debug(f"{slot_key}: sizing — capital=${self.portfolio.capital:.2f} slot=${slot_capital:.2f} bet=${size:.2f}")

        contracts = math.floor(size / contract_price)
        if contracts < 1:
            self._release_trade_slot(slot_key)
            return

        side_label   = "UP" if direction == LONG else "DOWN"
        market_label = f"{slot_key} {side_label}"

        # Compute settlement window before order placement (timing matters)
        _now = datetime.now(timezone.utc)
        _m   = _now.minute - (_now.minute % 15)
        settlement_open = _now.replace(minute=_m, second=0, microsecond=0, tzinfo=None)

        # Mark this window as traded in the market log
        try:
            market_log.log_trade(market_log.current_window_str(), direction)
        except Exception:
            pass

        self._place_and_monitor(
            slot_key=slot_key,
            slot_type="crypto",
            direction=direction,
            signals=signals,
            contracts=contracts,
            contract_price=contract_price,
            kalshi_ticker=kalshi_ticker,
            market_label=market_label,
            trade_key=current_window,
            settlement_open=settlement_open,
            bet_size=size,
            confidence_pct=confidence_pct,
        )

    # ------------------------------------------------------------------ #
    #  Non-Crypto Market Slot Evaluation (2-min poll)                     #
    # ------------------------------------------------------------------ #

    def _poll_market_slots(self):
        """
        Evaluate all non-crypto market slots.
        Called from _heartbeat() every MARKET_EVAL_INTERVAL_SECS seconds.
        Each slot runs in its own thread to avoid blocking each other.
        """
        for slot_key, slot_cfg in SLOTS.items():
            if slot_cfg["type"] == "crypto":
                continue
            t = threading.Thread(
                target=self._evaluate_market_slot,
                args=[slot_key, slot_cfg],
                daemon=True,
            )
            t.start()

    def _evaluate_market_slot(self, slot_key: str, slot_cfg: dict):
        """
        Evaluate a single non-crypto market slot:
        1. Discover open Kalshi markets closing within MARKET_MAX_CLOSE_HOURS
        2. Evaluate ALL markets, collect candidates that pass all guards
        3. Sort candidates by confidence descending (priority queue — best edge first)
        4. Execute the highest-confidence trade
        """
        if time.monotonic() < self._ready_at:
            return

        # Budget guard: check games bet cap and remaining daily budget
        with self._lock:
            games_bet_today  = self._sport_games_bet.get(slot_key, 0)
            budget_spent     = self._sport_budget_spent.get(slot_key, 0.0)
            budget_snap      = self._sport_budget_snap.get(slot_key, self.portfolio.capital * SLOT_CAPITAL_PCT)
        daily_budget     = round(budget_snap * SPORTS_DAILY_BUDGET_PCT, 2)
        budget_remaining = round(daily_budget - budget_spent, 2)

        if games_bet_today >= SPORTS_MAX_GAMES_PER_SLOT:
            logger.info(f"{slot_key}: game cap reached ({games_bet_today}/{SPORTS_MAX_GAMES_PER_SLOT}) — skipping slot")
            return
        if budget_remaining < 1.00:
            logger.info(f"{slot_key}: daily budget exhausted (${budget_spent:.2f}/${daily_budget:.2f}) — skipping slot")
            return

        if not self._within_trade_limit(slot_key):
            return
        if not FORCE_TRADE and not self.portfolio.can_trade():
            self._release_trade_slot(slot_key)
            return

        series           = slot_cfg["series"]
        game_date_filter = slot_cfg.get("game_date_filter", False)
        markets          = self.kalshi.get_markets_by_series(
            series,
            max_close_hours=MARKET_MAX_CLOSE_HOURS,
            game_date_filter=game_date_filter,
        )

        if not markets:
            filter_desc = "today's games" if game_date_filter else f"within {MARKET_MAX_CLOSE_HOURS}h"
            logger.info(f"{slot_key}: no open markets found for series={series!r} ({filter_desc})")
            self._release_trade_slot(slot_key)
            return

        slot_type = slot_cfg["type"]

        # --- Phase 1: evaluate ALL markets, collect valid candidates ---
        candidates = []
        now_mono = time.monotonic()

        # Evict stale market price tracking entries (older than 4 hours) — prevents unbounded growth
        _evict_before = now_mono - 14400
        stale_tickers = [t for t, v in self._market_price_seen.items()
                         if v.get("last_changed", 0) < _evict_before]
        for t in stale_tickers:
            self._market_price_seen.pop(t, None)

        for market in markets:
            ticker = market.get("ticker")
            if not ticker:
                continue

            yes_ask = float(market.get("yes_ask_dollars", 0.5))
            if not (0.05 <= yes_ask <= 0.95):
                continue

            # Price direction history: track last 3 yes_ask observations per ticker
            _ph = self._market_price_history.setdefault(ticker, [])
            _ph.append((now_mono, yes_ask))
            if len(_ph) > 3:
                _ph.pop(0)

            # Stale market filter: skip in-game markets whose Kalshi price hasn't
            # changed in > INGAME_STALE_MARKET_SECS (price is stuck, not a real edge).
            seen = self._market_price_seen.get(ticker)
            if seen:
                if seen["price"] != yes_ask:
                    self._market_price_seen[ticker] = {"price": yes_ask, "last_changed": now_mono}
            else:
                self._market_price_seen[ticker] = {"price": yes_ask, "last_changed": now_mono}

            # Get signal from external source
            if slot_type == "sports":
                decision = self._sports_strategy.decide(
                    market=market,
                    espn_sport=slot_cfg["espn_sport"],
                    sport_label=slot_cfg["label"],
                )
            else:
                continue

            direction = decision["direction"]
            is_ingame = decision.get("is_ingame", False)

            if direction == NONE:
                logger.info(f"{slot_key} [{ticker}]: no trade — {decision['reason']}")
                continue

            # Log this evaluation to the sports outcome tracker.
            # Only fires for non-NONE decisions (edge exceeded threshold, ESPN matched).
            # traded=False for now; updated to True if the trade actually executes.
            _log_game = {
                "game_id":    decision.get("game_id", ""),
                "is_live":    is_ingame,
                "period":     decision.get("game_period", ""),
                "home_score": decision.get("home_score", ""),
                "away_score": decision.get("away_score", ""),
                "score_diff": (decision.get("home_score", 0) or 0) -
                               (decision.get("away_score", 0) or 0),
            }
            sports_log.log_evaluation(
                sport=slot_cfg["label"],
                game=_log_game,
                ticker=ticker,
                title=decision.get("market_label", ""),
                yes_team=decision.get("market_label", ""),
                model_prob=decision.get("external_prob", 0.0),
                kalshi_yes=decision.get("kalshi_yes", yes_ask),
                edge=decision.get("edge", 0.0),
                direction=direction,
                traded=False,
                prob_source=decision.get("prob_source", ""),
                confidence_pct=decision.get("confidence_pct", 0.0),
                vote_score=decision.get("vote_score", 0),
                vote_detail=decision.get("vote_detail", ""),
                confidence_tier=decision.get("confidence_tier", ""),
                home_record=decision.get("home_record", ""),
                away_record=decision.get("away_record", ""),
                home_l10=decision.get("home_l10", ""),
                away_l10=decision.get("away_l10", ""),
                h2h_series=decision.get("h2h_series", ""),
            )

            # Stale market guard (apply only to in-game sports markets)
            if is_ingame and slot_type == "sports":
                stale_info = self._market_price_seen.get(ticker, {})
                age = now_mono - stale_info.get("last_changed", now_mono)
                if age > INGAME_STALE_MARKET_SECS:
                    logger.info(
                        f"{slot_key} [{ticker}]: stale market — YES={yes_ask:.2f} "
                        f"unchanged {int(age//60)}m — skipping"
                    )
                    continue

            # Post-decision re-entry guard for sports
            if slot_type == "sports":
                # Game-level key prevents duplicate trades on both team markets
                # (e.g. BOSATL-BOS and BOSATL-ATL map to BOSATL)
                game_key = ticker.rsplit('-', 1)[0] if ticker.count('-') >= 2 else ticker

                with self._lock:
                    # Per-game daily cap: don't over-concentrate on one matchup
                    game_count = self._game_trade_counts.get(game_key, 0)
                    if game_count >= SPORTS_MAX_TRADES_PER_GAME:
                        logger.info(
                            f"{slot_key}: game cap reached on {game_key} "
                            f"({game_count}/{SPORTS_MAX_TRADES_PER_GAME}) — skipping"
                        )
                        continue

                    # In-game cooloff: min N minutes between re-entries on same live game
                    cooloff_secs = SPORTS_INGAME_COOLOFF_MINS * 60 if is_ingame else 86400
                    last_t = self._ingame_trade_times.get(game_key, 0)
                    since  = now_mono - last_t
                    if since < cooloff_secs:
                        tag = f"{int(since//60)}m/{SPORTS_INGAME_COOLOFF_MINS}m" if is_ingame else "pre-game once-only"
                        logger.info(f"{slot_key}: re-entry blocked on {ticker} ({tag} cooloff)")
                        continue

                # Open-trades game guard: block if ANY open trade already exists for
                # this same game matchup (same game_key), regardless of direction or
                # ticker side. Prevents both YES-A and NO-B on the same game.
                _open_now = {}
                try:
                    with self._file_lock:
                        with open(_OPEN_TRADES_FILE) as _ot_f:
                            _open_now = json.load(_ot_f)
                except Exception:
                    pass
                _game_already_open = False
                _opp = False
                for _td in _open_now.values():
                    _open_ticker = _td.get("kalshi_ticker", "")
                    _open_gk = (
                        _open_ticker.rsplit("-", 1)[0]
                        if _open_ticker.count("-") >= 2
                        else _open_ticker
                    )
                    if _open_gk == game_key:
                        _game_already_open = True
                    if _td.get("kalshi_ticker") == ticker and _td.get("direction") != direction:
                        _opp = True
                if _game_already_open:
                    logger.info(
                        f"{slot_key} [{ticker}]: skipping — open trade already "
                        f"exists for matchup {game_key}"
                    )
                    continue
                if _opp:
                    logger.info(
                        f"{slot_key} [{ticker}]: skipping — already hold "
                        f"opposite side on this market"
                    )
                    continue

            # In-game price direction filter: reject if Kalshi is actively moving
            # against our intended direction (drift ≥ 0.06 over last 3 observations ≈ 2 min).
            # Prevents entering trades where the market is already pricing us out in real time.
            if is_ingame:
                _ph = self._market_price_history.get(ticker, [])
                if len(_ph) >= 3:
                    _drift = _ph[-1][1] - _ph[0][1]
                    if direction == LONG and _drift <= -0.06:
                        logger.info(
                            f"{slot_key} [{ticker}]: skip — price trending against LONG "
                            f"({_drift:+.3f} over {len(_ph)} obs)"
                        )
                        continue
                    if direction == SHORT and _drift >= 0.06:
                        logger.info(
                            f"{slot_key} [{ticker}]: skip — price trending against SHORT "
                            f"({_drift:+.3f} over {len(_ph)} obs)"
                        )
                        continue

            no_ask         = float(market.get("no_ask_dollars", 0.5))
            contract_price = yes_ask if direction == LONG else no_ask

            if not (CONTRACT_BUY_MIN <= contract_price <= CONTRACT_BUY_MAX):
                logger.info(
                    f"{slot_key} [{ticker}]: no trade — contract price {contract_price:.2f} "
                    f"outside buy range [{CONTRACT_BUY_MIN:.2f}, {CONTRACT_BUY_MAX:.2f}]"
                )
                continue

            close_str = market.get("close_time") or market.get("expiration_time", "")
            settlement_open = None
            if close_str:
                try:
                    close_dt = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
                    settlement_open = close_dt.replace(tzinfo=None)
                except ValueError:
                    pass

            # Within-cycle dedup: if a market from the same game is already in candidates
            # (e.g. YES-CLE and NO-LAD both pass guards before any trade is placed),
            # skip the lower-confidence one — only one game per matchup per scan cycle.
            _cand_game_key = (
                ticker.rsplit("-", 1)[0] if ticker.count("-") >= 2 else ticker
            )
            if any(c.get("game_key") == _cand_game_key for c in candidates):
                logger.info(
                    f"{slot_key} [{ticker}]: skipping — same matchup already "
                    f"in candidates this cycle ({_cand_game_key})"
                )
                continue

            candidates.append({
                "market":          market,
                "ticker":          ticker,
                "decision":        decision,
                "direction":       direction,
                "is_ingame":       is_ingame,
                "contract_price":  contract_price,
                "settlement_open": settlement_open,
                "confidence":      decision.get("confidence", 0.0),
                "market_label":    decision.get("market_label", slot_key),
                "game_key":        _cand_game_key,
            })

        # --- Phase 2: compute budget-based sizing, sort by confidence, execute top candidate ---
        candidates.sort(key=lambda c: c["confidence"], reverse=True)

        # Budget-aware sizing: remaining budget / min(N_tradeable, remaining_capacity)
        # Fractions up when fewer good games found; spreads across up to max_games.
        n_tradeable       = len(candidates)
        remaining_capacity = SPORTS_MAX_GAMES_PER_SLOT - games_bet_today
        if n_tradeable > 0 and remaining_capacity > 0:
            per_game_size = round(budget_remaining / min(n_tradeable, remaining_capacity), 2)
        else:
            per_game_size = 0.0
        # Hard cap: no single game exceeds 25% of slot capital regardless of budget math
        _slot_cap_now = self.portfolio.capital * SLOT_CAPITAL_PCT
        _max_per_game = round(_slot_cap_now * SPORTS_MAX_BET_PCT, 2)
        per_game_size = min(per_game_size, _max_per_game)

        traded = False

        for c in candidates:
            contract_price = c["contract_price"]
            size = per_game_size
            contracts = math.floor(size / contract_price)
            if contracts < 1:
                logger.info(f"{slot_key} [{c['ticker']}]: sizing too small (${size:.2f} / ${contract_price:.2f}) — skipping")
                continue

            # Slot already claimed via _within_trade_limit — don't release it
            self._place_and_monitor(
                slot_key=slot_key,
                slot_type=slot_type,
                direction=c["direction"],
                signals=c["decision"],
                contracts=contracts,
                contract_price=contract_price,
                kalshi_ticker=c["ticker"],
                market_label=c["market_label"],
                trade_key=c["ticker"],
                settlement_open=c["settlement_open"],
                bet_size=size,
                confidence_pct=c["decision"].get("confidence_pct", 0.0),
            )
            # Mark the sports outcome log row as actually traded
            sports_log.update_result(ticker=c["ticker"], result="open", pnl=0.0)
            traded = True
            break  # one trade per slot per poll cycle

        if not traded:
            self._release_trade_slot(slot_key)

    # ------------------------------------------------------------------ #
    #  Shared Trade Execution Path                                         #
    # ------------------------------------------------------------------ #

    def _place_and_monitor(
        self,
        slot_key: str,
        slot_type: str,
        direction: str,
        signals: dict,
        contracts: int,
        contract_price: float,
        kalshi_ticker: str,
        market_label: str,
        trade_key,              # current_window (crypto) or ticker (sports/weather)
        settlement_open,        # datetime (naive UTC) or None
        bet_size: float = 0.0,  # clean dollar amount before fees, for Discord display
        confidence_pct: float = 0.0,  # model confidence (0.0–100.0%)
    ):
        """
        Place the order (real or simulated), record the trade, and spawn
        the position monitor thread. Shared by crypto and non-crypto slots.
        """
        side = "yes" if direction == LONG else "no"

        # --- Live order placement ---
        contracts_filled = contracts
        if self.live:
            order = self.kalshi.place_limit_order(
                kalshi_ticker, side, contracts,
                price_cents=round(contract_price * 100),
            )
            if not order:
                logger.error(f"{slot_key}: order placement failed — skipping")
                self._release_trade_slot(slot_key)
                return
            filled_order = self.kalshi.wait_for_fill(order["order_id"])
            if not filled_order:
                logger.warning(f"{slot_key}: order expired unfilled — skipping")
                self._release_trade_slot(slot_key)
                return
            contracts_filled = filled_order.get("filled_count", contracts)
            if contracts_filled < 1:
                logger.warning(f"{slot_key}: zero contracts filled — skipping")
                self._release_trade_slot(slot_key)
                return
            contracts = contracts_filled

        actual_cost = contracts * contract_price
        fee_entry   = KALSHI_MAKER_FEE * contracts * min(contract_price, 1 - contract_price)
        total_cost  = round(actual_cost + fee_entry, 2)
        payout      = round(contracts * 1.00 - fee_entry, 2)
        price_pct   = contract_price * 100
        live_price  = self._get_crypto_state(slot_key)["price"] if slot_type == "crypto" else 0.0

        with self._lock:
            self._open_stake = round(self._open_stake + total_cost, 2)
            portfolio_after  = round(self.portfolio.total - self._open_stake, 2)
            self._last_trade_key[slot_key] = trade_key
            if slot_type != "crypto":
                self._traded_tickers[slot_key].add(trade_key)
                self._sport_games_bet[slot_key] = self._sport_games_bet.get(slot_key, 0) + 1
                # Record budget spend (actual cost, not including fees — fees come from capital)
                _actual_cost = contracts * contract_price
                self._sport_budget_spent[slot_key] = round(
                    self._sport_budget_spent.get(slot_key, 0.0) + _actual_cost, 4
                )
            if slot_key == "BTC":
                try:
                    with open(_TRADE_STATE_FILE, "w") as _f:
                        json.dump({"BTC": trade_key.isoformat()}, _f)
                    logger.info(f"BTC: trade state persisted ({trade_key.strftime('%H:%M')} window)")
                except Exception as exc:
                    logger.warning(f"BTC: could not persist trade state to disk: {exc}")
            # Record ingame trade time for cooloff + per-game count tracking
            if slot_type == "sports":
                _gk = trade_key.rsplit('-', 1)[0] if trade_key.count('-') >= 2 else trade_key
                self._ingame_trade_times[_gk] = time.monotonic()
                self._game_trade_counts[_gk] = self._game_trade_counts.get(_gk, 0) + 1

        # Persist sports session/game counts to disk so they survive restarts
        if slot_type == "sports":
            self._save_sports_state()

        slot_cfg = SLOTS[slot_key]
        trade_id = self.trade_log.open_trade(
            direction=direction,
            contracts=contracts,
            contracts_filled=contracts_filled,
            contract_price_pct=price_pct,
            confidence_pct=confidence_pct,
            cost=total_cost,
            possible_payout=payout,
            btc_price=live_price,
            signals=signals,
            asset=slot_key,
            slot_type=slot_type,
            market_label=market_label,
            kalshi_ticker=kalshi_ticker,
        )

        # Persist open trade to disk so monitor threads can be resumed after restart
        _settlement_iso = settlement_open.isoformat() if settlement_open is not None else None
        self._save_open_trade(trade_id, {
            "slot_key":       slot_key,
            "slot_type":      slot_type,
            "direction":      direction,
            "contracts":      contracts,
            "contract_price": contract_price,
            "price_pct":      price_pct,
            "confidence_pct": confidence_pct,
            "kalshi_ticker":  kalshi_ticker,
            "market_label":   market_label,
            "settlement_open_iso": _settlement_iso,
            "entry_time_iso": datetime.now(timezone.utc).isoformat(),
        })

        log_trade(direction, contract_price, total_cost, confidence_pct=confidence_pct,
                  slot_type=slot_type, market_label=market_label)
        if slot_type == "crypto":
            signal_log.mark_traded(signal_log.current_window())
        self.monitor.record_order_placed()
        self.discord.buy(
            direction=direction,
            contracts=contracts,
            contracts_filled=contracts_filled,
            price_pct=price_pct,
            cost=total_cost,
            payout=payout,
            portfolio_total=portfolio_after,
            market_label=market_label,
            bet_size=bet_size,
            session_wins=self.session_wins,
            session_losses=self.session_losses,
            session_pnl=self.session_pnl,
        )

        # Seconds until settlement
        if settlement_open is not None and slot_type == "crypto":
            # Crypto: fire at next 15M boundary
            _now = datetime.now(timezone.utc)
            seconds_into_window = (_now.minute % 15) * 60 + _now.second + _now.microsecond / 1e6
            seconds_until_settlement = max(5.0, (15 * 60) - seconds_into_window + 5)
        elif settlement_open is not None:
            # Non-crypto: fire at market close time
            _now = datetime.now(timezone.utc)
            close_aware = settlement_open.replace(tzinfo=timezone.utc)
            seconds_until_settlement = max(10.0, (close_aware - _now).total_seconds() + 10)
        else:
            seconds_until_settlement = 900.0  # fallback: 15 min

        t = threading.Thread(
            target=self._monitor_position,
            args=[slot_key, slot_type, direction, contracts, contract_price, price_pct,
                  trade_id, kalshi_ticker, settlement_open, seconds_until_settlement,
                  market_label, confidence_pct]
        )
        t.daemon = True
        t.start()

    # ------------------------------------------------------------------ #
    #  Position Monitor                                                    #
    # ------------------------------------------------------------------ #

    def _monitor_position(self, slot_key: str, slot_type: str,
                          direction: str, contracts: int,
                          contract_price: float, price_pct: float, trade_id: str,
                          kalshi_ticker: str, settlement_open, seconds_until_settlement: float,
                          market_label: str = "", confidence_pct: float = 0.0):
        """
        Poll the Kalshi contract price every 10s and exit early if:
          - Stop-loss: contract price drops to <= STOP_LOSS_PRICE
          - Trailing profit: once price >= TRAILING_TRIGGER, exit on any drop of TRAILING_BUFFER
        Falls through to _resolve_trade() if neither fires.
        """
        side           = "yes" if direction == LONG else "no"
        high_water     = contract_price
        trailing_armed = False
        # Crypto: 10s — need fast stop-loss / trailing-profit response.
        # Sports: 120s — stop-loss and trailing are disabled; only checking for early Kalshi
        #         settlement. 300+ API calls per game at 30s is wasteful; 2-min is sufficient.
        poll_interval  = 10 if slot_type == "crypto" else 120
        deadline       = time.monotonic() + seconds_until_settlement

        # Stop-loss threshold: 50% drop from entry (relative), capped at STOP_LOSS_PRICE.
        # Prevents immediate stop-out when entry price is already below STOP_LOSS_PRICE
        # (e.g. sports NO contracts priced at 0.20-0.30 when YES market is heavily favored).
        stop_threshold = min(STOP_LOSS_PRICE, contract_price * 0.50)

        while self.running:
            remaining = deadline - time.monotonic()
            if remaining <= poll_interval:
                if remaining > 0:
                    time.sleep(remaining)
                break

            time.sleep(poll_interval)

            # Refresh ticker for crypto (market rolls every 15 min)
            fresh_ticker = kalshi_ticker
            if slot_type == "crypto":
                fresh_ticker = self._get_ticker_for_slot(slot_key) or kalshi_ticker

            # Sports: check for early Kalshi resolution (close_time may be days away
            # but markets resolve within minutes of game end)
            if slot_type != "crypto" and kalshi_ticker:
                early_result = self.kalshi.get_market_result(kalshi_ticker, retries=1, delay=0)
                if early_result is not None:
                    logger.info(
                        f"{slot_key}: early settlement — Kalshi resolved {kalshi_ticker} → {early_result.upper()}"
                    )
                    break

            current_price = self.kalshi.get_market_price(fresh_ticker, side)
            if not (0.05 < current_price < 0.95):
                continue

            if current_price > high_water:
                high_water = current_price

            if not trailing_armed and high_water >= TRAILING_TRIGGER:
                trailing_armed = True
                logger.info(
                    f"{slot_key}: trailing-profit armed — {side.upper()} peaked at {high_water:.2f}"
                )

            if trailing_armed and current_price < high_water - TRAILING_BUFFER:
                logger.info(
                    f"{slot_key}: trailing-profit EXIT — peaked {high_water:.2f} → now {current_price:.2f}"
                )
                self._exit_early(slot_key, slot_type, direction, contracts, contract_price,
                                 current_price, trade_id, "trailing-profit", fresh_ticker,
                                 market_label=market_label, confidence_pct=confidence_pct)
                return

            if current_price <= stop_threshold:
                logger.info(
                    f"{slot_key}: stop-loss EXIT — {side.upper()} at {current_price:.2f} "
                    f"(<= {stop_threshold:.2f})"
                )
                self._exit_early(slot_key, slot_type, direction, contracts, contract_price,
                                 current_price, trade_id, "stop-loss", fresh_ticker,
                                 market_label=market_label, confidence_pct=confidence_pct)
                return

        if self.running:
            logger.info(f"{slot_key}: settling trade_id={trade_id}")
            try:
                self._resolve_trade(slot_key, slot_type, direction, contracts, contract_price,
                                    price_pct, trade_id, kalshi_ticker, settlement_open,
                                    market_label=market_label, confidence_pct=confidence_pct)
            except Exception as exc:
                logger.exception(f"{slot_key}: _resolve_trade crashed — trade_id={trade_id}: {exc}")

    def _exit_early(self, slot_key: str, slot_type: str, direction: str, contracts: int,
                    contract_price: float, exit_price: float,
                    trade_id: str, reason: str, kalshi_ticker: str = None,
                    market_label: str = "", confidence_pct: float = 0.0):
        """Resolve a trade early at exit_price (stop-loss or trailing profit)."""
        if self.live and kalshi_ticker:
            side = "yes" if direction == LONG else "no"
            sell_cents = 1 if reason == "stop-loss" else max(1, round(exit_price * 100))
            self.kalshi.sell_position(kalshi_ticker, side, contracts, sell_cents)

        actual_cost   = contracts * contract_price
        exit_proceeds = contracts * exit_price
        fee_entry     = KALSHI_MAKER_FEE * contracts * min(contract_price, 1 - contract_price)
        fee_exit      = KALSHI_MAKER_FEE * contracts * min(exit_price,     1 - exit_price)
        fee_paid      = round(fee_entry + fee_exit, 4)
        pnl           = round(exit_proceeds - actual_cost - fee_paid, 4)
        win           = pnl > 0
        total_cost    = round(actual_cost + fee_entry, 2)
        live_price    = self._get_crypto_state(slot_key)["price"] if slot_type == "crypto" else 0.0

        with self._lock:
            self._open_stake = max(0.0, round(self._open_stake - total_cost, 2))
            last_candle = {}
            if slot_type == "crypto":
                df = self._get_crypto_state(slot_key)["df_15m"]
                last_candle = df.iloc[-1].to_dict() if not df.empty else {}

        if not market_label:
            market_label = self._derive_label(slot_key, direction, None)

        if win:
            with self._lock:
                self.session_wins += 1
                self.session_pnl   = round(self.session_pnl + pnl, 4)
                self.portfolio.record_win(pnl)
                self.portfolio.save(_PORTFOLIO_STATE_FILE)
                s_wins, s_losses, s_pnl = self.session_wins, self.session_losses, self.session_pnl
                port_total   = self.portfolio.total
                port_summary = self.portfolio.summary()
                self._consec_losses[slot_key] = 0
            log_trade(direction, contract_price, actual_cost, result="win", pnl=pnl,
                      confidence_pct=confidence_pct, slot_type=slot_type, market_label=market_label)
            self._save_session_state()
            self.monitor.record_trade_result("win")
            self.trade_log.close_trade(trade_id, "win", pnl, fee_paid, last_candle, port_summary)
            self.discord.sell_win(
                direction=direction, contracts=contracts, contracts_filled=contracts,
                price_pct=contract_price * 100, pnl=pnl, portfolio_total=port_total,
                market_label=market_label,
                session_wins=s_wins, session_losses=s_losses, session_pnl=s_pnl,
            )
        else:
            with self._lock:
                self.session_losses += 1
                self.session_pnl     = round(self.session_pnl + pnl, 4)
                self.portfolio.record_loss(abs(pnl))
                self.portfolio.save(_PORTFOLIO_STATE_FILE)
                s_wins, s_losses, s_pnl = self.session_wins, self.session_losses, self.session_pnl
                port_total   = self.portfolio.total
                port_summary = self.portfolio.summary()
                self._consec_losses[slot_key] = min(self._consec_losses[slot_key] + 1, 10)
            log_trade(direction, contract_price, actual_cost, result="loss", pnl=pnl,
                      confidence_pct=confidence_pct, slot_type=slot_type, market_label=market_label)
            self._save_session_state()
            self.monitor.record_trade_result("loss")
            self.trade_log.close_trade(trade_id, "loss", pnl, fee_paid, last_candle, port_summary)
            self.discord.sell_loss(
                direction=direction, contracts=contracts, contracts_filled=contracts,
                price_pct=contract_price * 100, pnl=pnl, portfolio_total=port_total,
                market_label=market_label,
                session_wins=s_wins, session_losses=s_losses, session_pnl=s_pnl,
            )

        logger.info(
            f"{slot_key}: {reason} | exit={exit_price:.2f} entry={contract_price:.2f} | pnl=${pnl:+.2f}"
        )
        self._remove_open_trade(trade_id)

        with self._lock:
            can_trade    = self.portfolio.can_trade()
            port_halt    = self.portfolio.total
        if not FORCE_TRADE and not can_trade:
            halt_reason = "Daily loss limit reached"
            log_halt(halt_reason)
            self.monitor.set_halt(True, halt_reason)
            if not self._stopped:
                self._stopped = True
                self.discord.bot_stopped(port_halt)

    def _resolve_trade(self, slot_key: str, slot_type: str,
                       direction: str, contracts: int,
                       contract_price: float, price_pct: float,
                       trade_id: str, kalshi_ticker: str = None,
                       settlement_open=None, market_label: str = "",
                       confidence_pct: float = 0.0):
        """Resolve a trade at Kalshi settlement."""
        kalshi_result = None
        if kalshi_ticker:
            kalshi_result = self.kalshi.get_market_result(kalshi_ticker)
            if kalshi_result:
                logger.info(f"Kalshi settled {slot_key} {kalshi_ticker}: {kalshi_result.upper()}")
            else:
                logger.warning(f"{slot_key}: Kalshi settlement unavailable")

        # For crypto: look up the settlement candle from Kraken data
        last_candle = {}
        went_up     = None
        if slot_type == "crypto":
            with self._lock:
                df = self._get_crypto_state(slot_key)["df_15m"]
                if not df.empty and len(df) >= 2:
                    last = None
                    if settlement_open is not None:
                        target    = pd.Timestamp(settlement_open).floor("min")
                        df_times  = pd.to_datetime(df["time"], format="mixed").dt.floor("min")
                        match     = df[df_times == target]
                        if not match.empty:
                            last = match.iloc[0]
                    if last is None:
                        if settlement_open is not None:
                            target_ts = pd.Timestamp(settlement_open)
                            past  = df[pd.to_datetime(df["time"], format="mixed") <= target_ts]
                            last  = past.iloc[-1] if not past.empty else df.iloc[-1]
                        else:
                            last = df.iloc[-1]
                    went_up     = last["close"] > last["open"]
                    last_candle = last.to_dict()

        # Determine win
        if kalshi_result:
            kalshi_side = "yes" if direction == LONG else "no"
            win = (kalshi_result == kalshi_side)
        elif went_up is not None:
            # Kraken fallback for crypto only
            win = (direction == LONG and went_up) or (direction == SHORT and not went_up)
        else:
            # Non-crypto without Kalshi result — treat as loss (conservative)
            logger.warning(f"{slot_key}: no settlement data — recording as loss")
            win = False

        actual_cost = contracts * contract_price
        fee_one_leg = KALSHI_MAKER_FEE * contracts * min(contract_price, 1 - contract_price)
        total_cost  = round(actual_cost + fee_one_leg, 2)
        live_price  = self._get_crypto_state(slot_key)["price"] if slot_type == "crypto" else 0.0

        with self._lock:
            self._open_stake = max(0.0, round(self._open_stake - total_cost, 2))
        self._remove_open_trade(trade_id)

        if not market_label:
            market_label = self._derive_label(slot_key, direction, None)

        if win:
            fee_paid = round(fee_one_leg, 4)   # settlement exit fee = 0 (min(1.0, 0.0) = 0)
            gross    = contracts * 1.00
            pnl      = round(gross - actual_cost - fee_paid, 4)
            with self._lock:
                self.session_wins += 1
                self.session_pnl   = round(self.session_pnl + pnl, 4)
                self.portfolio.record_win(pnl)
                self.portfolio.save(_PORTFOLIO_STATE_FILE)
                s_wins, s_losses, s_pnl = self.session_wins, self.session_losses, self.session_pnl
                port_total   = self.portfolio.total
                port_summary = self.portfolio.summary()
                self._consec_losses[slot_key] = 0
                if settlement_open is not None and slot_type == "crypto":
                    self._tracked_windows.setdefault(
                        settlement_open, {"wins": 0, "losses": 0}
                    )["wins"] += 1
            log_trade(direction, contract_price, actual_cost, result="win", pnl=pnl,
                      confidence_pct=confidence_pct, slot_type=slot_type, market_label=market_label)
            try:
                if slot_type == "crypto" and settlement_open is not None:
                    _w = settlement_open.strftime("%Y-%m-%d %H:%M")
                    market_log.log_outcome(_w, "win", float(last_candle.get("open", 0)), float(last_candle.get("close", 0)))
                elif slot_type == "sports" and kalshi_ticker:
                    sports_log.update_result(ticker=kalshi_ticker, result="win", pnl=pnl)
            except Exception:
                pass
            self._save_session_state()
            self.monitor.record_trade_result("win")
            self.trade_log.close_trade(trade_id, "win", pnl, fee_paid, last_candle, port_summary)
            self.discord.sell_win(
                direction=direction, contracts=contracts, contracts_filled=contracts,
                price_pct=contract_price * 100, pnl=pnl, portfolio_total=port_total,
                market_label=market_label,
                session_wins=s_wins, session_losses=s_losses, session_pnl=s_pnl,
            )
        else:
            fee_paid = round(fee_one_leg, 4)
            pnl      = round(-actual_cost - fee_paid, 4)
            with self._lock:
                self.session_losses += 1
                self.session_pnl     = round(self.session_pnl + pnl, 4)
                self.portfolio.record_loss(abs(pnl))
                self.portfolio.save(_PORTFOLIO_STATE_FILE)
                s_wins, s_losses, s_pnl = self.session_wins, self.session_losses, self.session_pnl
                port_total   = self.portfolio.total
                port_summary = self.portfolio.summary()
                self._consec_losses[slot_key] = min(self._consec_losses[slot_key] + 1, 10)
                if settlement_open is not None and slot_type == "crypto":
                    _wr = self._tracked_windows.setdefault(
                        settlement_open, {"wins": 0, "losses": 0}
                    )
                    _wr["losses"] += 1
                    if _wr["losses"] >= SWEEP_COOLOFF_LOSSES:
                        _next_w = settlement_open + timedelta(minutes=15)
                        if self._sweep_cooloff_window is None or _next_w > self._sweep_cooloff_window:
                            self._sweep_cooloff_window = _next_w
                            logger.info(
                                f"Sweep-loss cooloff: {_wr['losses']} losses in "
                                f"{settlement_open.strftime('%H:%M')} window — "
                                f"skipping {_next_w.strftime('%H:%M')} window"
                            )
                    _cutoff = settlement_open - timedelta(minutes=30)
                    for _k in [k for k in self._tracked_windows if k < _cutoff]:
                        del self._tracked_windows[_k]
            log_trade(direction, contract_price, actual_cost, result="loss", pnl=pnl,
                      confidence_pct=confidence_pct, slot_type=slot_type, market_label=market_label)
            try:
                if slot_type == "crypto" and settlement_open is not None:
                    _w = settlement_open.strftime("%Y-%m-%d %H:%M")
                    market_log.log_outcome(_w, "loss", float(last_candle.get("open", 0)), float(last_candle.get("close", 0)))
                elif slot_type == "sports" and kalshi_ticker:
                    sports_log.update_result(ticker=kalshi_ticker, result="loss", pnl=pnl)
            except Exception:
                pass
            self._save_session_state()
            self.monitor.record_trade_result("loss")
            self.trade_log.close_trade(trade_id, "loss", pnl, fee_paid, last_candle, port_summary)
            self.discord.sell_loss(
                direction=direction, contracts=contracts, contracts_filled=contracts,
                price_pct=contract_price * 100, pnl=pnl, portfolio_total=port_total,
                market_label=market_label,
                session_wins=s_wins, session_losses=s_losses, session_pnl=s_pnl,
            )

        with self._lock:
            can_trade       = self.portfolio.can_trade()
            port_total_halt = self.portfolio.total
        if not FORCE_TRADE and not can_trade:
            reason = "Daily loss limit reached"
            log_halt(reason)
            self.monitor.set_halt(True, reason)
            if not self._stopped:
                self._stopped = True
                self.discord.bot_stopped(port_total_halt)

    # ------------------------------------------------------------------ #
    #  Helpers                                                             #
    # ------------------------------------------------------------------ #

    def _derive_label(self, slot_key: str, direction: str, trade_key) -> str:
        """
        Reconstruct the market_label for _exit_early / _resolve_trade.
        For crypto: '{ASSET} UP' or '{ASSET} DOWN'.
        For sports: the label is embedded in the trade log;
        we build a fallback here from the slot name and direction.
        """
        if slot_key == "BTC":
            return f"BTC {'UP' if direction == LONG else 'DOWN'}"
        slot_label = SLOTS[slot_key]["label"]
        direction_word = "YES" if direction == LONG else "NO"
        return f"{slot_label}: {direction_word}"

    def _within_trade_limit(self, slot_key: str) -> bool:
        with self._lock:
            now = time.monotonic()
            if now - self.hour_window_start[slot_key] >= 3600:
                self.trades_this_hour[slot_key]  = 0
                self.hour_window_start[slot_key] = now
            if self.trades_this_hour[slot_key] < MAX_TRADES_PER_HOUR:
                self.trades_this_hour[slot_key] += 1
                return True
            return False

    def _release_trade_slot(self, slot_key: str):
        with self._lock:
            self.trades_this_hour[slot_key] = max(0, self.trades_this_hour[slot_key] - 1)

    def _save_sports_state(self):
        """Persist sports game counts and budget spend to disk (same-day restoration after restart)."""
        with self._lock:
            gb    = dict(self._sport_games_bet)
            bs    = dict(self._sport_budget_spent)
            gc    = dict(self._game_trade_counts)
            bsnap = dict(self._sport_budget_snap)
        with self._file_lock:
            try:
                state = {
                    "date_utc":     datetime.now(timezone.utc).date().isoformat(),
                    "games_bet":    gb,
                    "budget_spent": bs,
                    "game_counts":  gc,
                    "budget_snap":  bsnap,
                }
                with open(_SPORTS_STATE_FILE, "w") as _f:
                    json.dump(state, _f)
            except Exception as exc:
                logger.warning(f"Could not save sports state: {exc}")

    def _save_session_state(self):
        """Persist session W/L counters and consecutive-loss streak to disk (same-day restoration)."""
        with self._lock:
            wins   = self.session_wins
            losses = self.session_losses
            pnl    = self.session_pnl
            consec = dict(self._consec_losses)
        with self._file_lock:
            try:
                state = {
                    "date_utc":      datetime.now(timezone.utc).date().isoformat(),
                    "session_wins":  wins,
                    "session_losses": losses,
                    "session_pnl":   pnl,
                    "consec_losses": consec,
                }
                with open(_SESSION_STATE_FILE, "w") as _f:
                    json.dump(state, _f)
            except Exception as exc:
                logger.warning(f"Could not save session state: {exc}")

    # ------------------------------------------------------------------ #
    #  Heartbeat                                                           #
    # ------------------------------------------------------------------ #

    def _heartbeat(self):
        try:
            elapsed = 0
            while self.running:
                time.sleep(10)
                elapsed += 10

                # Non-crypto slots: poll every MARKET_EVAL_INTERVAL_SECS (2 min)
                now = time.monotonic()
                if now - self._last_market_poll >= MARKET_EVAL_INTERVAL_SECS:
                    self._last_market_poll = now
                    self._poll_market_slots()

                if elapsed < 900:
                    continue
                elapsed = 0
                self.monitor.print_status()

                # Portfolio breakdown — logged every heartbeat so cash/capital are always visible
                with self._lock:
                    p = self.portfolio
                    slot_cap     = round(p.capital * SLOT_CAPITAL_PCT, 2)
                    btc_max_bet  = round(min(slot_cap * BTC_BET_PCT_HIGH, BTC_MAX_BET), 2)
                    sport_budget = round(slot_cap * SPORTS_DAILY_BUDGET_PCT, 2)
                if p.total > 0:
                    logger.info(
                        f"PORTFOLIO | total=${p.total:.2f} | "
                        f"cash=${p.cash:.2f} ({p.cash/p.total*100:.1f}%) [locked] | "
                        f"capital=${p.capital:.2f} ({p.capital/p.total*100:.1f}%) | "
                        f"per-slot=${slot_cap:.2f} | BTC-max=${btc_max_bet:.2f} | "
                        f"sports-budget=${sport_budget:.2f}/slot | pnl=${p.daily_pnl:+.2f}"
                    )
                else:
                    logger.warning("PORTFOLIO | total=$0.00 — portfolio fully depleted")

                if NEWS_ENABLED:
                    t = threading.Thread(
                        target=NewsContext.fetch, args=[["BTC"]], daemon=True
                    )
                    t.start()

                # Refresh BTC 1H candles every 15 min
                if now - self._last_1h_refresh >= 900:
                    fresh_btc_1h = self.btc_state["history"].load("1h")
                    with self._lock:
                        self.btc_state["df_1h"] = fresh_btc_1h
                    self._last_1h_refresh = now

                if self._is_new_day():
                    self.portfolio.reset_day()

                # UTC midnight: reset per-slot session trade caps
                _utc_today = datetime.now(timezone.utc).date()
                if self._last_session_reset_utc is None:
                    self._last_session_reset_utc = _utc_today
                elif _utc_today != self._last_session_reset_utc:
                    self._last_session_reset_utc = _utc_today
                    # Re-snapshot slot capital for new day's budget baseline
                    _new_snap = self.portfolio.capital * SLOT_CAPITAL_PCT
                    with self._lock:
                        _sports_slots = [k for k, v in SLOTS.items() if v["type"] == "sports"]
                        self._sport_games_bet    = {k: 0   for k in _sports_slots}
                        self._sport_budget_spent = {k: 0.0 for k in _sports_slots}
                        self._sport_budget_snap  = {k: _new_snap for k in _sports_slots}
                        self._game_trade_counts  = {}
                        self._ingame_trade_times = {}
                    self._save_sports_state()
                    logger.info(
                        f"UTC midnight: sports budgets reset — new snap=${_new_snap:.2f}/slot "
                        f"(daily budget ${_new_snap * SPORTS_DAILY_BUDGET_PCT:.2f}/slot)"
                    )

        except (KeyboardInterrupt, SystemExit):
            self.stop("Keyboard interrupt")

    def _is_new_day(self) -> bool:
        now_et = datetime.now(ZoneInfo("America/New_York"))
        today  = now_et.date()
        if self._last_reset_date is None:
            self._last_reset_date = today
            return False
        if today != self._last_reset_date:
            self._last_reset_date = today
            return True
        return False

    # ------------------------------------------------------------------ #
    #  Open Trade Persistence                                              #
    # ------------------------------------------------------------------ #

    def _save_open_trade(self, trade_id: str, data: dict):
        """Write a newly opened trade to disk so it can be resumed after restart."""
        with self._file_lock:
            try:
                existing = {}
                try:
                    with open(_OPEN_TRADES_FILE) as _f:
                        existing = json.load(_f)
                except (FileNotFoundError, json.JSONDecodeError):
                    pass
                existing[trade_id] = data
                with open(_OPEN_TRADES_FILE, "w") as _f:
                    json.dump(existing, _f)
            except Exception as exc:
                logger.warning(f"Could not save open trade {trade_id}: {exc}")

    def _remove_open_trade(self, trade_id: str):
        """Remove a settled trade from the open trades file."""
        with self._file_lock:
            try:
                existing = {}
                try:
                    with open(_OPEN_TRADES_FILE) as _f:
                        existing = json.load(_f)
                except (FileNotFoundError, json.JSONDecodeError):
                    pass
                existing.pop(trade_id, None)
                with open(_OPEN_TRADES_FILE, "w") as _f:
                    json.dump(existing, _f)
            except Exception as exc:
                logger.warning(f"Could not remove open trade {trade_id}: {exc}")

    def _clear_position_state(self):
        """
        Wipe all persisted position files on startup so every restart begins
        with zero open trades and no carry-over state.
        """
        files_to_clear = [
            (_OPEN_TRADES_FILE,    "{}"),     # open trade monitoring threads
            (_TRADE_STATE_FILE,    "{}"),     # last BTC window traded (prevents same-window re-entry)
            (_SPORTS_STATE_FILE,   "{}"),     # sports game counts / daily budget
            (_SESSION_STATE_FILE,  "{}"),     # session W/L counters
        ]
        cleared = []
        for path, empty in files_to_clear:
            try:
                with open(path, "w") as _f:
                    _f.write(empty)
                cleared.append(os.path.basename(path))
            except Exception:
                pass
        if cleared:
            logger.info(f"Position state cleared on startup: {', '.join(cleared)}")

    def _resume_open_trades(self):
        """
        On startup, read open_trades.json and resume monitoring threads for any
        trades that were alive when the bot last died. Handles orphaned positions
        from deploys or crashes.
        """
        try:
            with open(_OPEN_TRADES_FILE) as _f:
                trades = json.load(_f)
        except FileNotFoundError:
            return
        except Exception as exc:
            logger.warning(f"Could not load open trades file: {exc}")
            return

        if not trades:
            return

        now = datetime.now(timezone.utc)
        resumed = 0
        stale   = 0

        for trade_id, td in list(trades.items()):
            try:
                entry_time = datetime.fromisoformat(td["entry_time_iso"]).replace(tzinfo=timezone.utc)
                # Only resume trades entered within the last 6 hours
                if (now - entry_time).total_seconds() > 21600:
                    stale += 1
                    self._remove_open_trade(trade_id)
                    continue

                settlement_open = None
                if td.get("settlement_open_iso"):
                    settlement_open = datetime.fromisoformat(td["settlement_open_iso"])

                # Compute remaining time until settlement
                if settlement_open is not None:
                    close_aware = settlement_open.replace(tzinfo=timezone.utc)
                    secs_remaining = max(30.0, (close_aware - now).total_seconds() + 10)
                else:
                    secs_remaining = 60.0

                slot_key  = td["slot_key"]
                slot_type = td["slot_type"]
                logger.info(
                    f"Resuming open trade {trade_id}: {slot_key} {td['direction']} "
                    f"@ {td['contract_price']:.2f} — {int(secs_remaining)}s until settlement"
                )
                t = threading.Thread(
                    target=self._monitor_position,
                    args=[
                        slot_key, slot_type, td["direction"],
                        td["contracts"], td["contract_price"], td["price_pct"],
                        trade_id, td["kalshi_ticker"], settlement_open,
                        secs_remaining, td["market_label"], td["confidence_pct"],
                    ]
                )
                t.daemon = True
                t.start()
                resumed += 1

            except Exception as exc:
                logger.warning(f"Could not resume open trade {trade_id}: {exc}")

        if resumed or stale:
            logger.info(f"Open trades on startup: resumed={resumed} stale_removed={stale}")
