import time
import math
import signal
import threading
import pandas as pd
from datetime import datetime, timezone, timedelta
from administration.portfolio import Portfolio
from administration.monitor import Monitor
from administration.discord import Discord
from administration.config import (
    STARTING_BALANCE, MAX_TRADES_PER_HOUR, KALSHI_MAKER_FEE, FORCE_TRADE, SLOTS,
    CONTRACT_PRICE_MIN, CONTRACT_PRICE_MAX, NEWS_ENABLED,
    BET_NEAR_FAIR, BET_SLIGHT_LEAN, BET_MOD_LEAN, BET_STRONG_LEAN,
    STOP_LOSS_PRICE, TRAILING_TRIGGER, TRAILING_BUFFER,
    SWEEP_COOLOFF_LOSSES, CONSEC_LOSS_THRESHOLD, CONSEC_LOSS_REDUCTION,
    MARKET_EVAL_INTERVAL_SECS, MARKET_MAX_CLOSE_HOURS,
)
from administration.news import NewsContext
from administration.kalshi import KalshiClient
from data.kraken import KrakenFeed
from data.history import History
from data.trades import TradeLog
from strategy.base import Strategy, LONG, SHORT, NONE
from strategy.weather import WeatherStrategy
from strategy.sports import SportsStrategy
from administration.logger import get as get_logger, log_trade, log_halt

logger = get_logger("paper")


class Trader:
    """
    Live and paper trading across 5 market slots:
      - BTC:     15-min crypto Up/Down driven by Kraken WebSocket
      - WEATHER: NWS probability vs Kalshi weather market (5-min poll)
      - MLB:     ESPN win probability vs Kalshi MLB market (5-min poll)
      - NBA:     ESPN win probability vs Kalshi NBA market (5-min poll)
      - NHL:     ESPN win probability vs Kalshi NHL market (5-min poll)

    live=False → paper mode: simulates orders, uses Kalshi demo API
    live=True  → live mode: places real orders on Kalshi, syncs balance at startup
    """

    def __init__(self, live: bool = False, starting_balance: float = STARTING_BALANCE):
        self.live      = live
        self.portfolio = Portfolio(starting_balance)
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
        self._weather_strategy = WeatherStrategy()
        self._sports_strategy  = SportsStrategy()

        # Per-slot rate limiting and state tracking
        self.trades_this_hour  = {k: 0 for k in SLOTS}
        self.hour_window_start = {k: time.monotonic() for k in SLOTS}

        # Kalshi ticker cache (15s TTL — only used for BTC/crypto slot)
        self._ticker_cache: dict = {"BTC": {"ticker": None, "ts": 0.0}}

        # Session stats
        self.session_wins   = 0
        self.session_losses = 0
        self.session_pnl    = 0.0
        self._open_stake    = 0.0
        self._last_1h_refresh = 0.0

        # One-trade-per-window/market: stores the last traded ticker per slot
        self._last_trade_key: dict = {k: None for k in SLOTS}

        # Correlated-sweep protection
        self._consec_losses:        dict = {k: 0 for k in SLOTS}
        self._tracked_windows:      dict = {}
        self._sweep_cooloff_window       = None  # skip BTC trades in this 15M window

        # Last non-crypto poll timestamp
        self._last_market_poll = 0.0

        self.running      = False
        self._stopped     = False
        self._lock        = threading.Lock()
        self._ready_at    = None
        self._last_reset_date = None

    # ------------------------------------------------------------------ #
    #  Lifecycle                                                           #
    # ------------------------------------------------------------------ #

    def start(self):
        mode_str = "LIVE" if self.live else "paper"
        logger.info(f"Trader starting [{mode_str}] (5 slots: BTC + Weather + MLB + NBA + NHL)...")
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

        logger.info("Trader running. BTC driven by WebSocket; Weather/MLB/NBA/NHL polled every 5 min.")

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
    #  WebSocket Handlers (BTC only)                                      #
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
        self._evaluate_crypto()

    def _on_tick(self, asset: str, price: float):
        if asset == "BTC":
            self.btc_state["price"] = price

    # ------------------------------------------------------------------ #
    #  BTC Crypto Evaluation                                               #
    # ------------------------------------------------------------------ #

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
            decision = self.strategy.decide(
                state["df_1h"].copy(),
                state["df_15m"].copy(),
            )

        direction = decision["direction"]
        self.monitor.record_signal(direction, decision["signals"])

        if direction == NONE:
            return

        t = threading.Thread(
            target=self._execute_crypto_trade,
            args=[direction, decision["signals"], decision["confidence"]]
        )
        t.daemon = True
        t.start()

    def _get_btc_ticker(self) -> str | None:
        """Return the current Kalshi BTC ticker with 15-second TTL cache."""
        now = time.monotonic()
        cache = self._ticker_cache["BTC"]
        if cache["ticker"] and (now - cache["ts"]) < 15:
            return cache["ticker"]
        m = self.kalshi.get_market_for_asset("BTC")
        ticker = m.get("ticker") if m else None
        self._ticker_cache["BTC"] = {"ticker": ticker, "ts": now}
        return ticker

    def _execute_crypto_trade(self, direction: str, signals: dict, confidence: int = 3):
        """Execute a BTC 15-minute Up/Down trade with full contrarian + near-fair logic."""
        slot_key = "BTC"

        # One-trade-per-window: block duplicate entries in the same 15M window
        _now_cw = datetime.now(timezone.utc)
        _cw_min = _now_cw.minute - (_now_cw.minute % 15)
        current_window = _now_cw.replace(minute=_cw_min, second=0, microsecond=0, tzinfo=None)
        with self._lock:
            last_key = self._last_trade_key[slot_key]
        if last_key == current_window:
            logger.info(f"BTC: already traded this window ({current_window.strftime('%H:%M')}) — skipping")
            self._release_trade_slot(slot_key)
            return

        kalshi_ticker = self._get_btc_ticker()
        if not kalshi_ticker:
            logger.info("BTC: no Kalshi market found — skipping trade")
            self._release_trade_slot(slot_key)
            return

        # Retry loop: wait up to 14 min for a real price
        retry_start  = time.monotonic()
        WINDOW_SECS  = 14 * 60
        attempt      = 0
        yes_price, no_price = 1.0, 1.0
        while time.monotonic() - retry_start < WINDOW_SECS:
            fresh_ticker = self._get_btc_ticker() or kalshi_ticker
            yes_price, no_price = self.kalshi.get_market_prices(fresh_ticker)
            if 0.05 <= yes_price <= 0.95:
                kalshi_ticker = fresh_ticker
                break
            attempt += 1
            remaining = WINDOW_SECS - (time.monotonic() - retry_start)
            if remaining < 30:
                break
            logger.info(f"BTC: Kalshi not priced (yes={yes_price:.2f}) — retrying in 30s (attempt {attempt})")
            time.sleep(30)

        if not (0.05 <= yes_price <= 0.95):
            logger.info("BTC: no real Kalshi price available — skipping trade")
            self._release_trade_slot(slot_key)
            return

        if not (CONTRACT_PRICE_MIN <= yes_price <= CONTRACT_PRICE_MAX):
            logger.info(
                f"BTC: market too confident (yes={yes_price:.2f}) — "
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
                    f"BTC: contrarian SHORT skipped — price below VWAP "
                    f"({signals['price']:.2f} < {signals['vwap']:.2f}), no mean-reversion setup"
                )
                self._release_trade_slot(slot_key)
                return
            if direction == LONG and price_above_vwap:
                logger.info(
                    f"BTC: contrarian LONG skipped — price above VWAP "
                    f"({signals['price']:.2f} > {signals['vwap']:.2f}), no mean-reversion setup"
                )
                self._release_trade_slot(slot_key)
                return

            rsi_agrees = (
                (direction == SHORT and signals["rsi_bias"] == "bear") or
                (direction == LONG  and signals["rsi_bias"] == "bull")
            )
            if not rsi_agrees:
                logger.info(
                    f"BTC: contrarian {direction.upper()} skipped — "
                    f"RSI={signals['rsi']:.1f} ({signals['rsi_bias']}) opposes direction"
                )
                self._release_trade_slot(slot_key)
                return

            logger.info(
                f"BTC: contrarian — signal={direction} vs market yes={yes_price:.2f} "
                f"(buying {'YES at discount' if direction == LONG else 'NO at discount'})"
            )

        vwap_pos = "above" if signals["price"] > signals["vwap"] else "below"
        logger.info(
            f"BTC: signal-context | rsi={signals['rsi']:.1f}({signals['rsi_bias']}) | "
            f"price {vwap_pos} vwap ({signals['price']:.2f} vs {signals['vwap']:.2f})"
        )

        side           = "yes" if direction == LONG else "no"
        contract_price = yes_price if direction == LONG else no_price

        # Kelly-optimal sizing
        distance = abs(contract_price - 0.50)
        if distance <= 0.05:
            size = BET_NEAR_FAIR
        elif distance <= 0.10:
            size = BET_SLIGHT_LEAN
        elif distance <= 0.15:
            size = BET_MOD_LEAN
        else:
            size = BET_STRONG_LEAN

        with self._lock:
            consec = self._consec_losses[slot_key]
        if consec >= CONSEC_LOSS_THRESHOLD:
            size = round(size * CONSEC_LOSS_REDUCTION, 2)
            logger.info(f"BTC: cooldown active ({consec} consecutive losses) — bet reduced to ${size:.2f}")

        contracts = math.floor(size / contract_price)
        if contracts < 1:
            self._release_trade_slot(slot_key)
            return

        side_label   = "UP" if direction == LONG else "DOWN"
        market_label = f"BTC {side_label}"

        # Compute settlement window before order placement (timing matters)
        _now = datetime.now(timezone.utc)
        _m   = _now.minute - (_now.minute % 15)
        settlement_open = _now.replace(minute=_m, second=0, microsecond=0, tzinfo=None)

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
        )

    # ------------------------------------------------------------------ #
    #  Non-Crypto Market Slot Evaluation (5-min poll)                     #
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
        2. For each market, get external signal (NWS / ESPN)
        3. If edge >= threshold, execute trade
        """
        if time.monotonic() < self._ready_at:
            return
        if not self._within_trade_limit(slot_key):
            return
        if not FORCE_TRADE and not self.portfolio.can_trade():
            self._release_trade_slot(slot_key)
            return

        series   = slot_cfg["series"]
        markets  = self.kalshi.get_markets_by_series(series, max_close_hours=MARKET_MAX_CLOSE_HOURS)

        if not markets:
            logger.info(f"{slot_key}: no open markets found for series={series!r} within {MARKET_MAX_CLOSE_HOURS}h")
            self._release_trade_slot(slot_key)
            return

        slot_type = slot_cfg["type"]
        traded = False

        for market in markets:
            ticker = market.get("ticker")
            if not ticker:
                continue

            # One-trade-per-market: don't re-enter the same game/event twice
            with self._lock:
                if self._last_trade_key[slot_key] == ticker:
                    logger.info(f"{slot_key}: already traded market {ticker} — skipping")
                    continue

            # Get signal from external source
            if slot_type == "weather":
                decision = self._weather_strategy.decide(
                    market=market,
                    lat=slot_cfg["lat"],
                    lng=slot_cfg["lng"],
                    city=slot_cfg.get("city", ""),
                )
            elif slot_type == "sports":
                decision = self._sports_strategy.decide(
                    market=market,
                    espn_sport=slot_cfg["espn_sport"],
                    sport_label=slot_cfg["label"],
                )
            else:
                continue

            direction    = decision["direction"]
            market_label = decision.get("market_label", slot_key)

            if direction == NONE:
                logger.info(f"{slot_key} [{ticker}]: no trade — {decision['reason']}")
                continue

            yes_ask = float(market.get("yes_ask_dollars", 0.5))
            no_ask  = float(market.get("no_ask_dollars",  0.5))
            if not (0.05 <= yes_ask <= 0.95):
                continue

            contract_price = yes_ask if direction == LONG else no_ask

            # Kelly sizing — same tiers as crypto
            distance = abs(contract_price - 0.50)
            if distance <= 0.05:
                size = BET_NEAR_FAIR
            elif distance <= 0.10:
                size = BET_SLIGHT_LEAN
            elif distance <= 0.15:
                size = BET_MOD_LEAN
            else:
                size = BET_STRONG_LEAN

            with self._lock:
                consec = self._consec_losses[slot_key]
            if consec >= CONSEC_LOSS_THRESHOLD:
                size = round(size * CONSEC_LOSS_REDUCTION, 2)
                logger.info(f"{slot_key}: cooldown active ({consec} consecutive losses) — bet reduced to ${size:.2f}")

            contracts = math.floor(size / contract_price)
            if contracts < 1:
                continue

            # Use market close_time as proxy for settlement
            close_str = market.get("close_time") or market.get("expiration_time", "")
            settlement_open = None
            if close_str:
                try:
                    close_dt = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
                    settlement_open = close_dt.replace(tzinfo=None)
                except ValueError:
                    pass

            # Slot already claimed via _within_trade_limit — don't release it
            self._place_and_monitor(
                slot_key=slot_key,
                slot_type=slot_type,
                direction=direction,
                signals=decision,
                contracts=contracts,
                contract_price=contract_price,
                kalshi_ticker=ticker,
                market_label=market_label,
                trade_key=ticker,
                settlement_open=settlement_open,
            )
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
        trade_key,          # current_window (crypto) or ticker (sports/weather)
        settlement_open,    # datetime (naive UTC) or None
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
        payout      = round(contracts * 1.00 - 2 * fee_entry, 2)
        price_pct   = contract_price * 100
        live_price  = self.btc_state["price"] if slot_type == "crypto" else 0.0

        with self._lock:
            self._open_stake = round(self._open_stake + total_cost, 2)
            portfolio_after  = round(self.portfolio.total - self._open_stake, 2)
            self._last_trade_key[slot_key] = trade_key

        slot_cfg = SLOTS[slot_key]
        trade_id = self.trade_log.open_trade(
            direction=direction,
            contracts=contracts,
            contracts_filled=contracts_filled,
            contract_price_pct=price_pct,
            cost=total_cost,
            possible_payout=payout,
            btc_price=live_price,
            signals=signals,
            asset=slot_key,
            slot_type=slot_type,
            market_label=market_label,
        )

        log_trade(direction, live_price, total_cost)
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
                  trade_id, kalshi_ticker, settlement_open, seconds_until_settlement, market_label]
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
                          market_label: str = ""):
        """
        Poll the Kalshi contract price every 10s and exit early if:
          - Stop-loss: contract price drops to <= STOP_LOSS_PRICE
          - Trailing profit: once price >= TRAILING_TRIGGER, exit on any drop of TRAILING_BUFFER
        Falls through to _resolve_trade() if neither fires.
        """
        side           = "yes" if direction == LONG else "no"
        high_water     = contract_price
        trailing_armed = False
        poll_interval  = 10
        deadline       = time.monotonic() + seconds_until_settlement

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
                fresh_ticker = self._get_btc_ticker() or kalshi_ticker

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
                                 market_label=market_label)
                return

            if current_price <= STOP_LOSS_PRICE:
                logger.info(
                    f"{slot_key}: stop-loss EXIT — {side.upper()} at {current_price:.2f} "
                    f"(<= {STOP_LOSS_PRICE})"
                )
                self._exit_early(slot_key, slot_type, direction, contracts, contract_price,
                                 current_price, trade_id, "stop-loss", fresh_ticker,
                                 market_label=market_label)
                return

        if self.running:
            self._resolve_trade(slot_key, slot_type, direction, contracts, contract_price,
                                price_pct, trade_id, kalshi_ticker, settlement_open,
                                market_label=market_label)

    def _exit_early(self, slot_key: str, slot_type: str, direction: str, contracts: int,
                    contract_price: float, exit_price: float,
                    trade_id: str, reason: str, kalshi_ticker: str = None,
                    market_label: str = ""):
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
        live_price    = self.btc_state["price"] if slot_type == "crypto" else 0.0

        with self._lock:
            self._open_stake = max(0.0, round(self._open_stake - total_cost, 2))
            last_candle = {}
            if slot_type == "crypto":
                df = self.btc_state["df_15m"]
                last_candle = df.iloc[-1].to_dict() if not df.empty else {}

        if not market_label:
            market_label = self._derive_label(slot_key, direction, None)

        if win:
            with self._lock:
                self.session_wins += 1
                self.session_pnl   = round(self.session_pnl + pnl, 4)
                self.portfolio.record_win(pnl)
                s_wins, s_losses, s_pnl = self.session_wins, self.session_losses, self.session_pnl
                port_total   = self.portfolio.total
                port_summary = self.portfolio.summary()
                self._consec_losses[slot_key] = 0
            log_trade(direction, live_price, actual_cost, result="win", pnl=pnl)
            self.monitor.record_trade_result("win")
            self.trade_log.close_trade(trade_id, "win", pnl, fee_paid, last_candle, port_summary)
            self.discord.sell_win(
                direction=direction, contracts=contracts, contracts_filled=contracts,
                price_pct=exit_price * 100, pnl=pnl, portfolio_total=port_total,
                market_label=market_label,
                session_wins=s_wins, session_losses=s_losses, session_pnl=s_pnl,
            )
        else:
            with self._lock:
                self.session_losses += 1
                self.session_pnl     = round(self.session_pnl + pnl, 4)
                self.portfolio.record_loss(abs(pnl))
                s_wins, s_losses, s_pnl = self.session_wins, self.session_losses, self.session_pnl
                port_total   = self.portfolio.total
                port_summary = self.portfolio.summary()
                self._consec_losses[slot_key] = min(self._consec_losses[slot_key] + 1, 10)
            log_trade(direction, live_price, actual_cost, result="loss", pnl=pnl)
            self.monitor.record_trade_result("loss")
            self.trade_log.close_trade(trade_id, "loss", pnl, fee_paid, last_candle, port_summary)
            self.discord.sell_loss(
                direction=direction, contracts=contracts, contracts_filled=contracts,
                price_pct=exit_price * 100, pnl=pnl, portfolio_total=port_total,
                market_label=market_label,
                session_wins=s_wins, session_losses=s_losses, session_pnl=s_pnl,
            )

        logger.info(
            f"{slot_key}: {reason} | exit={exit_price:.2f} entry={contract_price:.2f} | pnl=${pnl:+.2f}"
        )

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
                       settlement_open=None, market_label: str = ""):
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
                df = self.btc_state["df_15m"]
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
        live_price  = self.btc_state["price"] if slot_type == "crypto" else 0.0

        with self._lock:
            self._open_stake = max(0.0, round(self._open_stake - total_cost, 2))

        if not market_label:
            market_label = self._derive_label(slot_key, direction, None)

        if win:
            fee_paid = round(fee_one_leg * 2, 4)
            gross    = contracts * 1.00
            pnl      = round(gross - actual_cost - fee_paid, 4)
            with self._lock:
                self.session_wins += 1
                self.session_pnl   = round(self.session_pnl + pnl, 4)
                self.portfolio.record_win(pnl)
                s_wins, s_losses, s_pnl = self.session_wins, self.session_losses, self.session_pnl
                port_total   = self.portfolio.total
                port_summary = self.portfolio.summary()
                self._consec_losses[slot_key] = 0
                if settlement_open is not None and slot_type == "crypto":
                    self._tracked_windows.setdefault(
                        settlement_open, {"wins": 0, "losses": 0}
                    )["wins"] += 1
            log_trade(direction, live_price, actual_cost, result="win", pnl=pnl)
            self.monitor.record_trade_result("win")
            self.trade_log.close_trade(trade_id, "win", pnl, fee_paid, last_candle, port_summary)
            self.discord.sell_win(
                direction=direction, contracts=contracts, contracts_filled=contracts,
                price_pct=price_pct, pnl=pnl, portfolio_total=port_total,
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
            log_trade(direction, live_price, actual_cost, result="loss", pnl=pnl)
            self.monitor.record_trade_result("loss")
            self.trade_log.close_trade(trade_id, "loss", pnl, fee_paid, last_candle, port_summary)
            self.discord.sell_loss(
                direction=direction, contracts=contracts, contracts_filled=contracts,
                price_pct=price_pct, pnl=pnl, portfolio_total=port_total,
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
        For crypto: 'BTC UP' or 'BTC DOWN'.
        For sports/weather: the label is embedded in the trade log;
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

    # ------------------------------------------------------------------ #
    #  Heartbeat                                                           #
    # ------------------------------------------------------------------ #

    def _heartbeat(self):
        try:
            elapsed = 0
            while self.running:
                time.sleep(10)
                elapsed += 10

                # Non-crypto slots: poll every MARKET_EVAL_INTERVAL_SECS (5 min)
                now = time.monotonic()
                if now - self._last_market_poll >= MARKET_EVAL_INTERVAL_SECS:
                    self._last_market_poll = now
                    self._poll_market_slots()

                if elapsed < 900:
                    continue
                elapsed = 0
                self.monitor.print_status()

                if NEWS_ENABLED:
                    t = threading.Thread(
                        target=NewsContext.fetch, args=[["BTC"]], daemon=True
                    )
                    t.start()

                # Refresh BTC 1H candles every 15 min
                if now - self._last_1h_refresh >= 900:
                    fresh_1h = self.btc_state["history"].load("1h")
                    with self._lock:
                        self.btc_state["df_1h"] = fresh_1h
                    self._last_1h_refresh = now

                if self._is_new_day():
                    self.portfolio.reset_day()

        except (KeyboardInterrupt, SystemExit):
            self.stop("Keyboard interrupt")

    def _is_new_day(self) -> bool:
        now_et = datetime.now(timezone.utc) - timedelta(hours=4)
        today  = now_et.date()
        if self._last_reset_date is None:
            self._last_reset_date = today
            return False
        if today != self._last_reset_date:
            self._last_reset_date = today
            return True
        return False
