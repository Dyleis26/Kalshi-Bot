import time
import math
import signal
import threading
import pandas as pd
from datetime import datetime, timezone
from administration.portfolio import Portfolio
from administration.monitor import Monitor
from administration.discord import Discord
from administration.config import (
    STARTING_BALANCE, MAX_TRADES_PER_HOUR, KALSHI_MAKER_FEE, FORCE_TRADE, ASSETS,
    CONTRACT_PRICE_MIN, CONTRACT_PRICE_MAX,
)
from administration.kalshi import KalshiClient
from data.kraken import KrakenFeed
from data.history import History
from data.trades import TradeLog
from strategy.base import Strategy, LONG, SHORT, NONE
from administration.logger import get as get_logger, log_trade, log_halt

logger = get_logger("paper")


class PaperTrader:
    """
    Simulated live trading across 5 crypto assets using real Kraken price data.
    Each asset runs independently with its own data, signals, and trade resolution.
    """

    def __init__(self, starting_balance: float = STARTING_BALANCE):
        self.portfolio = Portfolio(starting_balance)
        self.strategy  = Strategy()
        self.feed      = KrakenFeed()
        self.monitor   = Monitor()
        self.discord   = Discord(paper=True)
        self.trade_log = TradeLog(mode="paper")
        self.kalshi = KalshiClient(paper=True)   # demo — ticker discovery & settlement

        # Per-asset state: dataframes, live price, history manager
        self.assets: dict = {
            asset: {
                "df_1h":   pd.DataFrame(),
                "df_15m":  pd.DataFrame(),
                "price":   0.0,
                "history": History(asset),
            }
            for asset in ASSETS
        }

        # Trade rate limiting (per asset)
        self.trades_this_hour  = {asset: 0 for asset in ASSETS}
        self.hour_window_start = {asset: time.monotonic() for asset in ASSETS}

        # Kalshi ticker cache (2-minute TTL — 15M markets rotate every 15 min)
        self._ticker_cache: dict = {asset: {"ticker": None, "ts": 0.0} for asset in ASSETS}

        # Session stats — reset every time the bot restarts
        self.session_wins   = 0
        self.session_losses = 0
        self.session_pnl    = 0.0
        self._open_stake    = 0.0  # Sum of costs for all currently open trades
        self._last_1h_refresh = 0.0  # monotonic time of last 1H candle refresh

        self.running   = False
        self._stopped  = False  # Guard against double bot_stopped Discord messages
        self._lock     = threading.Lock()
        self._ready_at = None

    # ------------------------------------------------------------------ #
    #  Lifecycle                                                           #
    # ------------------------------------------------------------------ #

    def start(self):
        logger.info("Paper trader starting (5 assets)...")
        self.discord.start()

        # Fresh start — wipe trades from any previous session
        self.trade_log.reset()

        # Load historical candles for all assets
        for asset, state in self.assets.items():
            logger.info(f"Loading history for {asset}...")
            data = state["history"].load_all()
            state["df_1h"]  = data["1h"]
            state["df_15m"] = data["15m"]

        self.monitor.set_connected("kraken",  True)
        self.monitor.set_connected("kalshi",  True)
        self.monitor.set_connected("discord", self.discord.is_ready())

        self.discord.bot_started(self.portfolio.total)
        self.running   = True
        self._ready_at = time.monotonic() + 60  # 1-minute warmup

        # Graceful shutdown on SIGTERM (e.g. kill, systemd, task runner)
        signal.signal(signal.SIGTERM, lambda s, f: self.stop("SIGTERM"))

        self.feed.start_streams(
            on_15m=self._on_15m_candle,
            on_tick=self._on_tick,
        )

        logger.info("Paper trader running. Waiting for signals on BTC, ETH, SOL, XRP, DOGE...")
        self._heartbeat()

    def stop(self, reason: str = "Manual stop"):
        self.running = False
        self.feed.stop_streams()
        if not self._stopped:
            self._stopped = True
            self.discord.bot_stopped(self.portfolio.total)
        self.discord.stop()
        logger.info(f"Paper trader stopped: {reason}")

    # ------------------------------------------------------------------ #
    #  WebSocket Handlers                                                  #
    # ------------------------------------------------------------------ #

    def _on_15m_candle(self, asset: str, candle: dict):
        if asset not in self.assets:
            return
        with self._lock:
            row = pd.DataFrame([candle])[["time", "open", "high", "low", "close", "volume"]]
            state = self.assets[asset]
            candle_time = row["time"].iloc[0]
            df = state["df_15m"]
            if not df.empty and pd.Timestamp(df["time"].iloc[-1]) == pd.Timestamp(candle_time):
                # Same candle arrived twice (backfill + WS, or WS reconnect) — update in place
                state["df_15m"].iloc[-1] = row.iloc[0]
            else:
                state["df_15m"] = pd.concat([df, row], ignore_index=True).tail(300)
            state["history"].append(candle, "15m")
        self._evaluate(asset)

    def _on_tick(self, asset: str, price: float):
        if asset in self.assets:
            self.assets[asset]["price"] = price

    # ------------------------------------------------------------------ #
    #  Signal Evaluation & Trade Execution                                 #
    # ------------------------------------------------------------------ #

    def _evaluate(self, asset: str):
        if time.monotonic() < self._ready_at:
            return
        if not FORCE_TRADE and not self.portfolio.can_trade():
            return
        if not self._within_trade_limit(asset):
            return

        state = self.assets[asset]
        if len(state["df_1h"]) < 35 or len(state["df_15m"]) < 10:
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

        self._simulate_trade(asset, direction, decision)

    def _get_kalshi_ticker(self, asset: str) -> str | None:
        """Return the current Kalshi ticker for an asset, using a 2-minute TTL cache."""
        now = time.monotonic()
        cache = self._ticker_cache[asset]
        if cache["ticker"] and (now - cache["ts"]) < 120:
            return cache["ticker"]
        m = self.kalshi.get_market_for_asset(asset)
        ticker = m.get("ticker") if m else None
        self._ticker_cache[asset] = {"ticker": ticker, "ts": now}
        return ticker

    def _simulate_trade(self, asset: str, direction: str, decision: dict):
        kalshi_ticker = self._get_kalshi_ticker(asset)

        if kalshi_ticker:
            contract_price = self.kalshi.get_market_price(kalshi_ticker)
            if contract_price >= 1.0:
                logger.info(f"{asset}: no Kalshi asks — skipping trade")
                return
            if not (CONTRACT_PRICE_MIN <= contract_price <= CONTRACT_PRICE_MAX):
                logger.info(
                    f"{asset}: contract price {contract_price:.2f} outside "
                    f"[{CONTRACT_PRICE_MIN:.2f}, {CONTRACT_PRICE_MAX:.2f}] — skipping trade"
                )
                return
        else:
            contract_price = 0.50

        size = self.strategy.size()

        contracts = math.floor(size / contract_price)
        if contracts < 1:
            return

        actual_cost      = contracts * contract_price
        fee_entry        = KALSHI_MAKER_FEE * actual_cost * (1 - contract_price)
        total_cost       = round(actual_cost + fee_entry, 2)
        # Net payout if win = gross - settlement fee (same formula as entry fee)
        payout           = round(contracts * 1.00 - fee_entry, 2)
        price_pct        = contract_price * 100
        live_price = self.assets[asset]["price"]
        with self._lock:
            self._open_stake = round(self._open_stake + total_cost, 2)
            portfolio_after  = round(self.portfolio.total - self._open_stake, 2)

        trade_id = self.trade_log.open_trade(
            direction=direction,
            contracts=contracts,
            contracts_filled=contracts,   # paper: always fully filled; live: use order response
            contract_price_pct=price_pct,
            cost=total_cost,
            possible_payout=payout,
            btc_price=live_price,
            signals=decision["signals"],
            asset=asset,
        )

        log_trade(direction, live_price, total_cost)
        self.monitor.record_order_placed()
        self.discord.buy(
            direction=direction,
            contracts=contracts,
            contracts_filled=contracts,   # paper: always fully filled; live: use order response
            price_pct=price_pct,
            cost=total_cost,
            payout=payout,
            portfolio_total=portfolio_after,
            asset=asset,
            session_wins=self.session_wins,
            session_losses=self.session_losses,
            session_pnl=self.session_pnl,
        )

        # Compute the settlement candle's open time (15M boundary at moment of trade entry).
        # Passed to _resolve_trade so it looks up the correct candle instead of df_15m.iloc[-1].
        from datetime import datetime as _dt, timezone as _tz
        _now = _dt.now(_tz.utc)
        _m = _now.minute - (_now.minute % 15)
        settlement_open = _now.replace(minute=_m, second=0, microsecond=0, tzinfo=None)

        t = threading.Timer(
            15 * 60,
            self._resolve_trade,
            args=[asset, direction, contracts, contract_price, price_pct, trade_id,
                  kalshi_ticker, settlement_open]
        )
        t.daemon = True  # Dies with the process — no ghost resolutions after restart
        t.start()

    def _resolve_trade(self, asset: str, direction: str, contracts: int,
                       contract_price: float, price_pct: float,
                       trade_id: str, kalshi_ticker: str = None,
                       settlement_open=None):

        # Kalshi settlement (ground truth)
        kalshi_result = None
        if kalshi_ticker:
            kalshi_result = self.kalshi.get_market_result(kalshi_ticker)
            if kalshi_result:
                logger.info(f"Kalshi settled {asset} {kalshi_ticker}: {kalshi_result.upper()}")
            else:
                logger.warning(f"{asset}: Kalshi settlement unavailable — falling back to Kraken")

        state = self.assets[asset]
        with self._lock:
            df = state["df_15m"]
            if df.empty or len(df) < 2:
                return
            # Find the specific settlement candle by its open time rather than blindly using
            # the latest candle (which may already be the NEXT 15M candle due to timing).
            last = None
            if settlement_open is not None:
                target = pd.Timestamp(settlement_open)
                match = df[df["time"].apply(pd.Timestamp) == target]
                if not match.empty:
                    last = match.iloc[0]
            if last is None:
                last = df.iloc[-1]  # fallback
            went_up    = last["close"] > last["open"]
            last_candle = last.to_dict()

        if kalshi_result:
            kalshi_side = "yes" if direction == LONG else "no"
            win = (kalshi_result == kalshi_side)
        else:
            win = (direction == LONG and went_up) or (direction == SHORT and not went_up)

        actual_cost  = contracts * contract_price
        fee_one_leg  = KALSHI_MAKER_FEE * actual_cost * (1 - contract_price)
        total_cost   = round(actual_cost + fee_one_leg, 2)
        live_price   = state["price"]
        with self._lock:
            self._open_stake = max(0.0, round(self._open_stake - total_cost, 2))

        if win:
            # Kalshi charges fees at both entry AND settlement on winning trades
            fee_paid = round(fee_one_leg * 2, 4)
            gross    = contracts * 1.00
            pnl      = round(gross - actual_cost - fee_paid, 4)
            with self._lock:
                self.session_wins += 1
                self.session_pnl   = round(self.session_pnl + pnl, 4)
                self.portfolio.record_win(pnl)
                s_wins, s_losses, s_pnl = self.session_wins, self.session_losses, self.session_pnl
                port_total = self.portfolio.total
                port_summary = self.portfolio.summary()
            log_trade(direction, live_price, actual_cost, result="win", pnl=pnl)
            self.monitor.record_trade_result("win")
            self.trade_log.close_trade(trade_id, "win", pnl, fee_paid, last_candle, port_summary)
            self.discord.sell_win(
                direction=direction,
                contracts=contracts,
                contracts_filled=contracts,
                price_pct=price_pct,
                pnl=pnl,
                portfolio_total=port_total,
                asset=asset,
                session_wins=s_wins,
                session_losses=s_losses,
                session_pnl=s_pnl,
            )
        else:
            # On a loss: only the entry fee was paid (no settlement fee)
            fee_paid = round(fee_one_leg, 4)
            pnl      = round(-actual_cost - fee_paid, 4)
            with self._lock:
                self.session_losses += 1
                self.session_pnl     = round(self.session_pnl + pnl, 4)
                self.portfolio.record_loss(abs(pnl))
                s_wins, s_losses, s_pnl = self.session_wins, self.session_losses, self.session_pnl
                port_total = self.portfolio.total
                port_summary = self.portfolio.summary()
            log_trade(direction, live_price, actual_cost, result="loss", pnl=pnl)
            self.monitor.record_trade_result("loss")
            self.trade_log.close_trade(trade_id, "loss", pnl, fee_paid, last_candle, port_summary)
            self.discord.sell_loss(
                direction=direction,
                contracts=contracts,
                contracts_filled=contracts,
                price_pct=price_pct,
                pnl=abs(pnl),
                portfolio_total=port_total,
                asset=asset,
                session_wins=s_wins,
                session_losses=s_losses,
                session_pnl=s_pnl,
            )

        with self._lock:
            can_trade = self.portfolio.can_trade()
            port_total_halt = self.portfolio.total
        if not FORCE_TRADE and not can_trade:
            reason = "Daily loss limit reached"
            log_halt(reason)
            self.monitor.set_halt(True, reason)
            if not self._stopped:
                self._stopped = True
                self.discord.bot_stopped(port_total_halt)

    # ------------------------------------------------------------------ #
    #  Guards                                                              #
    # ------------------------------------------------------------------ #

    def _within_trade_limit(self, asset: str) -> bool:
        """Check hourly trade limit and pre-increment atomically to prevent races."""
        with self._lock:
            now = time.monotonic()
            if now - self.hour_window_start[asset] >= 3600:
                self.trades_this_hour[asset]  = 0
                self.hour_window_start[asset] = now
            if self.trades_this_hour[asset] < MAX_TRADES_PER_HOUR:
                self.trades_this_hour[asset] += 1
                return True
            return False

    # ------------------------------------------------------------------ #
    #  Heartbeat                                                           #
    # ------------------------------------------------------------------ #

    def _heartbeat(self):
        try:
            while self.running:
                time.sleep(900)
                self.monitor.print_status()
                # Refresh 1H candles from REST at most once per hour (4 heartbeat cycles)
                now = time.monotonic()
                if now - self._last_1h_refresh >= 3600:
                    for asset, state in self.assets.items():
                        fresh_1h = state["history"].load("1h")
                        with self._lock:
                            state["df_1h"] = fresh_1h
                    self._last_1h_refresh = now
                if self._is_new_day():
                    self.portfolio.reset_day()
        except (KeyboardInterrupt, SystemExit):
            self.stop("Keyboard interrupt")

    def _is_new_day(self) -> bool:
        # Reset at midnight ET. EDT=UTC-4, EST=UTC-5.
        # Use UTC-4 year-round — close enough, avoids tzdata dependency.
        from datetime import timedelta
        now_et = datetime.now(timezone.utc) - timedelta(hours=4)
        return now_et.hour == 0 and now_et.minute < 15
