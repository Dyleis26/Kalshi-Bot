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
        self.kalshi      = KalshiClient(paper=True)   # demo — ticker discovery & settlement
        self.kalshi_live = KalshiClient(paper=False)  # live — read-only price lookups

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

        # Kalshi market tickers (cached at startup)
        self.kalshi_tickers: dict = {asset: None for asset in ASSETS}

        # Session stats — reset every time the bot restarts
        self.session_wins   = 0
        self.session_losses = 0
        self.session_pnl    = 0.0
        self._open_stake    = 0.0  # Sum of costs for all currently open trades

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

        # Load historical candles for all assets
        for asset, state in self.assets.items():
            logger.info(f"Loading history for {asset}...")
            data = state["history"].load_all()
            state["df_1h"]  = data["1h"]
            state["df_15m"] = data["15m"]

        # Recover any orphaned trades from previous sessions
        self._recover_orphaned_trades()

        # Cache Kalshi market tickers for all assets
        for asset in ASSETS:
            m = self.kalshi.get_market_for_asset(asset)
            self.kalshi_tickers[asset] = m.get("ticker") if m else None
            if self.kalshi_tickers[asset]:
                logger.info(f"Kalshi ticker for {asset}: {self.kalshi_tickers[asset]}")
            else:
                logger.warning(f"No Kalshi ticker found for {asset} — will fall back to Kraken")

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
            state["df_15m"] = pd.concat([state["df_15m"], row], ignore_index=True).tail(300)
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

    def _simulate_trade(self, asset: str, direction: str, decision: dict):
        # Fetch the current open market ticker fresh from the live API on every trade.
        # The 15M series has a new market every 15 minutes — the startup cache goes stale
        # after the first candle. kalshi_live (paper=False) has real orderbooks.
        live_market = self.kalshi_live.get_market_for_asset(asset)
        kalshi_ticker = live_market.get("ticker") if live_market else None

        if kalshi_ticker:
            contract_price = self.kalshi_live.get_market_price(kalshi_ticker)
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
        with self._lock:
            self.trades_this_hour[asset] += 1

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

        threading.Timer(
            15 * 60,
            self._resolve_trade,
            args=[asset, direction, contracts, contract_price, price_pct, trade_id, kalshi_ticker]
        ).start()

    def _resolve_trade(self, asset: str, direction: str, contracts: int,
                       contract_price: float, price_pct: float,
                       trade_id: str, kalshi_ticker: str = None):

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
            if state["df_15m"].empty or len(state["df_15m"]) < 2:
                return
            last       = state["df_15m"].iloc[-1]
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
    #  Startup Recovery                                                   #
    # ------------------------------------------------------------------ #

    def _recover_orphaned_trades(self):
        """
        Scan trades.csv at startup for open trades older than 15 minutes
        and retroactively resolve them using Kraken 15M candle data.
        """
        df = self.trade_log.load()
        if df.empty:
            return

        open_trades = df[df["result"].isna() & df["entry_time"].notna()]
        if open_trades.empty:
            return

        now = datetime.now(timezone.utc)
        recovered = 0

        # Pre-fetch candles once per asset to avoid redundant REST calls
        candle_cache: dict = {}

        for _, row in open_trades.iterrows():
            trade_id       = row["trade_id"]
            asset          = str(row["asset"])
            direction      = str(row["direction"])
            contracts      = int(row["contracts"])
            contract_price = float(row["contract_price_pct"]) / 100.0

            entry_time = pd.to_datetime(row["entry_time"], utc=True)
            age_mins   = (now - entry_time).total_seconds() / 60

            if age_mins < 16:  # Not yet resolvable — trade window may still be open
                continue

            if asset not in ASSETS:
                logger.warning(f"Recovery: unknown asset '{asset}' for trade {trade_id} — skipping")
                continue

            if asset not in candle_cache:
                fetched = self.feed.get_candles("15m", asset=asset, limit=200)
                if fetched.empty:
                    logger.warning(f"Recovery: no candles for {asset} — skipping {trade_id}")
                    continue
                fetched["time"] = pd.to_datetime(fetched["time"])
                candle_cache[asset] = fetched
            candles = candle_cache[asset]

            # Settlement candle opens at the 15M boundary at/after entry_time
            entry_dt = entry_time.to_pydatetime()
            m = entry_dt.minute - (entry_dt.minute % 15)
            settlement_open = entry_dt.replace(minute=m, second=0, microsecond=0)
            settlement_open_naive = settlement_open.replace(tzinfo=None)

            match = candles[candles["time"] == settlement_open_naive]
            if match.empty:
                # Try the next 15M window
                nxt = settlement_open_naive + pd.Timedelta(minutes=15)
                match = candles[candles["time"] == nxt]
            if match.empty:
                logger.warning(f"Recovery: no candle at {settlement_open_naive} for {asset} {trade_id} — skipping")
                continue

            candle = match.iloc[0].to_dict()
            went_up = float(candle["close"]) > float(candle["open"])
            win = (direction == LONG and went_up) or (direction == SHORT and not went_up)

            actual_cost = contracts * contract_price
            fee_one_leg = KALSHI_MAKER_FEE * actual_cost * (1 - contract_price)

            if win:
                fee_paid = round(fee_one_leg * 2, 4)
                pnl      = round(contracts * 1.00 - actual_cost - fee_paid, 4)
                result   = "win"
            else:
                fee_paid = round(fee_one_leg, 4)
                pnl      = round(-actual_cost - fee_paid, 4)
                result   = "loss"

            self.trade_log.close_trade(
                trade_id, result, pnl, fee_paid, candle,
                {"capital": 0, "cash": 0, "total": 0},
            )
            logger.info(
                f"Recovered orphaned trade {trade_id}: {asset} {direction} → {result} "
                f"(PnL: {pnl:+.4f}, candle open={candle['open']:.4f} close={candle['close']:.4f})"
            )
            recovered += 1

        if recovered:
            logger.info(f"Startup recovery: {recovered} orphaned trade(s) resolved.")
        else:
            logger.info("Startup recovery: no orphaned trades found.")

    # ------------------------------------------------------------------ #
    #  Guards                                                              #
    # ------------------------------------------------------------------ #

    def _within_trade_limit(self, asset: str) -> bool:
        now = time.monotonic()
        if now - self.hour_window_start[asset] >= 3600:
            self.trades_this_hour[asset]  = 0
            self.hour_window_start[asset] = now
        return self.trades_this_hour[asset] < MAX_TRADES_PER_HOUR

    # ------------------------------------------------------------------ #
    #  Heartbeat                                                           #
    # ------------------------------------------------------------------ #

    def _heartbeat(self):
        try:
            while self.running:
                time.sleep(900)
                self.monitor.print_status()
                # Refresh 1H candles from REST for all assets
                for asset, state in self.assets.items():
                    fresh_1h = state["history"].load("1h")
                    with self._lock:
                        state["df_1h"] = fresh_1h
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
