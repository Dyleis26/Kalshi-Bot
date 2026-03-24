import time
import math
import threading
import pandas as pd
from datetime import datetime, timezone
from administration.portfolio import Portfolio
from administration.monitor import Monitor
from administration.discord import Discord
from administration.config import (
    STARTING_BALANCE, MAX_TRADES_PER_HOUR, KALSHI_MAKER_FEE, FORCE_TRADE
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
    Simulated live trading using real Kraken price data.
    No real orders are placed — everything is tracked internally.
    """

    def __init__(self, starting_balance: float = STARTING_BALANCE):
        self.portfolio = Portfolio(starting_balance)
        self.strategy = Strategy()
        self.feed = KrakenFeed()
        self.history = History()
        self.monitor = Monitor()
        self.discord = Discord(paper=True)
        self.trade_log = TradeLog(mode="paper")
        self.kalshi = KalshiClient(paper=False)  # live URL for real market settlement data

        # Live candle buffers
        self.df_1h: pd.DataFrame = pd.DataFrame()
        self.df_15m: pd.DataFrame = pd.DataFrame()
        self.live_price: float = 0.0

        # State
        self.running = False
        self.trades_this_hour = 0
        self.hour_window_start = time.monotonic()
        self._lock = threading.Lock()
        self._ready_at = None  # set on start, trades blocked for first 60s

    # ------------------------------------------------------------------ #
    #  Lifecycle                                                           #
    # ------------------------------------------------------------------ #

    def start(self):
        logger.info("Paper trader starting...")
        self.discord.start()

        data = self.history.load_all()
        self.df_1h = data["1h"]
        self.df_15m = data["15m"]

        self.monitor.set_connected("kraken", True)
        self.monitor.set_connected("kalshi", True)
        self.monitor.set_connected("discord", self.discord.is_ready())

        self.discord.bot_started(self.portfolio.total)
        self.running = True
        self._ready_at = time.monotonic() + 60  # 1-minute warmup before first trade

        self.feed.start_streams(
            on_1h=self._on_1h_candle,
            on_15m=self._on_15m_candle,
            on_1m=self._on_tick,
        )

        logger.info("Paper trader running. Waiting for signals...")
        self._heartbeat()

    def stop(self, reason: str = "Manual stop"):
        self.running = False
        self.feed.stop_streams()
        self.discord.bot_stopped(self.portfolio.total)
        self.discord.stop()
        logger.info(f"Paper trader stopped: {reason}")

    # ------------------------------------------------------------------ #
    #  WebSocket Handlers                                                  #
    # ------------------------------------------------------------------ #

    def _on_1h_candle(self, candle: dict):
        with self._lock:
            row = pd.DataFrame([candle])[["time", "open", "high", "low", "close", "volume"]]
            self.df_1h = pd.concat([self.df_1h, row], ignore_index=True).tail(300)
            self.history.append(candle, "1h")

    def _on_15m_candle(self, candle: dict):
        with self._lock:
            row = pd.DataFrame([candle])[["time", "open", "high", "low", "close", "volume"]]
            self.df_15m = pd.concat([self.df_15m, row], ignore_index=True).tail(300)
            self.history.append(candle, "15m")
        self._evaluate()

    def _on_tick(self, price: float):
        self.live_price = price

    # ------------------------------------------------------------------ #
    #  Signal Evaluation & Trade Execution                                 #
    # ------------------------------------------------------------------ #

    def _evaluate(self):
        if time.monotonic() < self._ready_at:
            return
        if not FORCE_TRADE and not self.portfolio.can_trade():
            return
        if not self._within_trade_limit():
            return
        if len(self.df_1h) < 35 or len(self.df_15m) < 10:
            return

        with self._lock:
            decision = self.strategy.decide(self.df_1h.copy(), self.df_15m.copy())

        direction = decision["direction"]
        self.monitor.record_signal(direction, decision["signals"])

        if direction == NONE:
            return

        self._simulate_trade(direction, decision)

    def _simulate_trade(self, direction: str, decision: dict):
        contract_price = 0.50  # Paper trade assumption
        size = self.strategy.size()

        # Calculate contracts and costs
        contracts = math.floor(size / contract_price)
        if contracts < 1:
            return

        actual_cost = contracts * contract_price
        fee_entry = KALSHI_MAKER_FEE * actual_cost * (1 - contract_price)
        total_cost = round(actual_cost + fee_entry, 2)
        payout = round(contracts * 1.00, 2)
        price_pct = contract_price * 100

        # Portfolio value after buy (cash + remaining capital - cost)
        portfolio_after_buy = round(self.portfolio.total - total_cost, 2)

        # Log trade entry
        trade_id = self.trade_log.open_trade(
            direction=direction,
            contracts=contracts,
            contract_price_pct=price_pct,
            cost=total_cost,
            possible_payout=payout,
            btc_price=self.live_price,
            signals=decision["signals"],
        )

        # Grab the Kalshi market ticker that covers the upcoming outcome window
        kalshi_ticker = self.kalshi.get_btc_market_ticker()
        if kalshi_ticker:
            logger.info(f"Outcome market ticker: {kalshi_ticker}")
        else:
            logger.warning("Could not fetch Kalshi market ticker — will fall back to Kraken for resolution")

        log_trade(direction, self.live_price, total_cost)
        self.monitor.record_order_placed()
        self.trades_this_hour += 1

        self.discord.buy(
            direction=direction,
            contracts=contracts,
            price_pct=price_pct,
            cost=total_cost,
            payout=payout,
            portfolio_total=portfolio_after_buy,
        )

        threading.Timer(
            15 * 60,
            self._resolve_trade,
            args=[direction, contracts, contract_price, price_pct, trade_id, kalshi_ticker]
        ).start()

    def _resolve_trade(self, direction: str, contracts: int,
                       contract_price: float, price_pct: float, trade_id: str,
                       kalshi_ticker: str = None):
        # --- Determine outcome via Kalshi settlement (ground truth) ---
        kalshi_result = None
        if kalshi_ticker:
            kalshi_result = self.kalshi.get_market_result(kalshi_ticker)
            if kalshi_result:
                logger.info(f"Kalshi settled {kalshi_ticker}: {kalshi_result.upper()}")
            else:
                logger.warning(f"Kalshi settlement not available for {kalshi_ticker} — falling back to Kraken")

        with self._lock:
            if self.df_15m.empty or len(self.df_15m) < 2:
                return
            last = self.df_15m.iloc[-1]
            went_up = last["close"] > last["open"]
            btc_candle = last.to_dict()

        if kalshi_result:
            # Ground truth: use Kalshi's actual settlement
            kalshi_side = "yes" if direction == LONG else "no"
            win = (kalshi_result == kalshi_side)
        else:
            # Fallback: estimate from Kraken candle direction
            win = (direction == LONG and went_up) or (direction == SHORT and not went_up)

        actual_cost = contracts * contract_price
        fee = KALSHI_MAKER_FEE * actual_cost * (1 - contract_price)

        if win:
            gross = contracts * 1.00
            pnl = round(gross - actual_cost - fee, 4)
            self.portfolio.record_win(pnl)
            log_trade(direction, self.live_price, actual_cost, result="win", pnl=pnl)
            self.monitor.record_trade_result("win")
            self.trade_log.close_trade(trade_id, "win", pnl, fee, btc_candle, self.portfolio.summary())
            self.discord.sell_win(
                direction=direction,
                contracts=contracts,
                price_pct=price_pct,
                pnl=pnl,
                portfolio_total=self.portfolio.total,
            )
        else:
            pnl = round(-(actual_cost) - fee, 4)
            self.portfolio.record_loss(abs(pnl))
            log_trade(direction, self.live_price, actual_cost, result="loss", pnl=pnl)
            self.monitor.record_trade_result("loss")
            self.trade_log.close_trade(trade_id, "loss", pnl, fee, btc_candle, self.portfolio.summary())
            self.discord.sell_loss(
                direction=direction,
                contracts=contracts,
                price_pct=price_pct,
                pnl=abs(pnl),
                portfolio_total=self.portfolio.total,
            )

        if not self.portfolio.can_trade():
            reason = "Daily loss limit reached"
            log_halt(reason)
            self.monitor.set_halt(True, reason)
            self.discord.bot_stopped(self.portfolio.total)

    # ------------------------------------------------------------------ #
    #  Guards                                                              #
    # ------------------------------------------------------------------ #

    def _within_trade_limit(self) -> bool:
        now = time.monotonic()
        if now - self.hour_window_start >= 3600:
            self.trades_this_hour = 0
            self.hour_window_start = now
        return self.trades_this_hour < MAX_TRADES_PER_HOUR

    # ------------------------------------------------------------------ #
    #  Heartbeat                                                           #
    # ------------------------------------------------------------------ #

    def _heartbeat(self):
        try:
            while self.running:
                time.sleep(900)
                self.monitor.print_status()
                # Refresh 1H candles from REST — WebSocket only subscribes to 15M
                fresh_1h = self.history.load("1h")
                with self._lock:
                    self.df_1h = fresh_1h
                if self._is_new_day():
                    self.portfolio.reset_day()
        except KeyboardInterrupt:
            self.stop("Keyboard interrupt")

    def _is_new_day(self) -> bool:
        now = datetime.now(timezone.utc)
        return now.hour == 0 and now.minute < 15
