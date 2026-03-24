import time
import math
import threading
import pandas as pd
from datetime import datetime, timezone
from administration.portfolio import Portfolio
from administration.monitor import Monitor
from administration.discord import Discord
from administration.config import (
    STARTING_BALANCE, MAX_TRADES_PER_HOUR, KALSHI_MAKER_FEE, FORCE_TRADE, ASSETS
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
        self.kalshi    = KalshiClient(paper=False)

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

        # Trade rate limiting (shared across all assets)
        self.trades_this_hour  = 0
        self.hour_window_start = time.monotonic()

        self.running   = False
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

        self.monitor.set_connected("kraken",  True)
        self.monitor.set_connected("kalshi",  True)
        self.monitor.set_connected("discord", self.discord.is_ready())

        self.discord.bot_started(self.portfolio.total)
        self.running   = True
        self._ready_at = time.monotonic() + 60  # 1-minute warmup

        self.feed.start_streams(
            on_15m=self._on_15m_candle,
            on_tick=self._on_tick,
        )

        logger.info("Paper trader running. Waiting for signals on BTC, ETH, SOL, XRP, DOGE...")
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
        if not self._within_trade_limit():
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
        contract_price = 0.50
        size           = self.strategy.size()

        contracts = math.floor(size / contract_price)
        if contracts < 1:
            return

        actual_cost      = contracts * contract_price
        fee_entry        = KALSHI_MAKER_FEE * actual_cost * (1 - contract_price)
        total_cost       = round(actual_cost + fee_entry, 2)
        payout           = round(contracts * 1.00, 2)
        price_pct        = contract_price * 100
        live_price       = self.assets[asset]["price"]
        portfolio_after  = round(self.portfolio.total - total_cost, 2)

        trade_id = self.trade_log.open_trade(
            direction=direction,
            contracts=contracts,
            contract_price_pct=price_pct,
            cost=total_cost,
            possible_payout=payout,
            btc_price=live_price,
            signals=decision["signals"],
            asset=asset,
        )

        # Capture Kalshi market ticker for ground-truth settlement
        kalshi_ticker = None
        try:
            m = self.kalshi.get_market_for_asset(asset)
            kalshi_ticker = m.get("ticker") if m else None
            if kalshi_ticker:
                logger.info(f"{asset} outcome market: {kalshi_ticker}")
            else:
                logger.warning(f"{asset}: no Kalshi ticker found — will fall back to Kraken")
        except Exception:
            pass

        log_trade(direction, live_price, total_cost)
        self.monitor.record_order_placed()
        self.trades_this_hour += 1

        self.discord.buy(
            direction=direction,
            contracts=contracts,
            price_pct=price_pct,
            cost=total_cost,
            payout=payout,
            portfolio_total=portfolio_after,
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

        actual_cost = contracts * contract_price
        fee         = KALSHI_MAKER_FEE * actual_cost * (1 - contract_price)
        live_price  = state["price"]

        if win:
            gross = contracts * 1.00
            pnl   = round(gross - actual_cost - fee, 4)
            self.portfolio.record_win(pnl)
            log_trade(direction, live_price, actual_cost, result="win", pnl=pnl)
            self.monitor.record_trade_result("win")
            self.trade_log.close_trade(trade_id, "win", pnl, fee, last_candle, self.portfolio.summary())
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
            log_trade(direction, live_price, actual_cost, result="loss", pnl=pnl)
            self.monitor.record_trade_result("loss")
            self.trade_log.close_trade(trade_id, "loss", pnl, fee, last_candle, self.portfolio.summary())
            self.discord.sell_loss(
                direction=direction,
                contracts=contracts,
                price_pct=price_pct,
                pnl=abs(pnl),
                portfolio_total=self.portfolio.total,
            )

        if not FORCE_TRADE and not self.portfolio.can_trade():
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
            self.trades_this_hour  = 0
            self.hour_window_start = now
        return self.trades_this_hour < MAX_TRADES_PER_HOUR * len(ASSETS)

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
        except KeyboardInterrupt:
            self.stop("Keyboard interrupt")

    def _is_new_day(self) -> bool:
        now = datetime.now(timezone.utc)
        return now.hour == 0 and now.minute < 15
