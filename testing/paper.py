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
    CONTRACT_PRICE_MIN, CONTRACT_PRICE_MAX, NEWS_ENABLED,
    BET_NEAR_FAIR, BET_SLIGHT_LEAN, BET_MOD_LEAN, BET_STRONG_LEAN,
    STOP_LOSS_PRICE, TRAILING_TRIGGER,
)
from administration.news import NewsContext
from administration.kalshi import KalshiClient
from data.kraken import KrakenFeed
from data.history import History
from data.trades import TradeLog
from strategy.base import Strategy, LONG, SHORT, NONE
from administration.logger import get as get_logger, log_trade, log_halt

logger = get_logger("paper")


class Trader:
    """
    Live and paper trading across 5 crypto assets using real Kraken price data.
    Each asset runs independently with its own data, signals, and trade resolution.

    live=False  → paper mode: simulates orders, uses Kalshi demo API for market data
    live=True   → live mode:  places real orders on Kalshi, syncs balance at startup
    """

    def __init__(self, live: bool = False, starting_balance: float = STARTING_BALANCE):
        self.live      = live
        self.portfolio = Portfolio(starting_balance)
        self.strategy  = Strategy()
        self.feed      = KrakenFeed()
        self.monitor   = Monitor()
        self.discord   = Discord(paper=not live)
        self.trade_log = TradeLog(mode="live" if live else "paper")
        self.kalshi    = KalshiClient(paper=not live)

        # Per-asset state: dataframes, live price, history manager
        # All History instances share the same KrakenFeed (REST calls only — thread-safe)
        self.assets: dict = {
            asset: {
                "df_1h":   pd.DataFrame(),
                "df_15m":  pd.DataFrame(),
                "price":   0.0,
                "history": History(asset, feed=self.feed),
            }
            for asset in ASSETS
        }

        # Trade rate limiting (per asset)
        self.trades_this_hour  = {asset: 0 for asset in ASSETS}
        self.hour_window_start = {asset: time.monotonic() for asset in ASSETS}

        # Kalshi ticker cache (15s TTL — short so retry loop always gets a fresh ticker
        # in case the market rolls to the next window during a 30s retry sleep)
        self._ticker_cache: dict = {asset: {"ticker": None, "ts": 0.0} for asset in ASSETS}

        # Session stats — reset every time the bot restarts
        self.session_wins   = 0
        self.session_losses = 0
        self.session_pnl    = 0.0
        self._open_stake    = 0.0  # Sum of costs for all currently open trades
        self._last_1h_refresh = 0.0  # monotonic time of last 1H candle refresh

        self.running          = False
        self._stopped         = False  # Guard against double bot_stopped Discord messages
        self._lock            = threading.Lock()
        self._ready_at        = None
        self._last_reset_date = None   # Tracks last daily reset date (ET) for _is_new_day()
        self._last_trade_window: dict = {asset: None for asset in ASSETS}  # Last window traded per asset

    # ------------------------------------------------------------------ #
    #  Lifecycle                                                           #
    # ------------------------------------------------------------------ #

    def start(self):
        mode_str = "LIVE" if self.live else "paper"
        logger.info(f"Trader starting [{mode_str}] (5 assets)...")
        self.discord.start()

        # In live mode: sync portfolio balance from the real Kalshi account.
        # Ensures we're sizing bets against our actual available funds.
        if self.live:
            live_balance = self.kalshi.get_balance()
            if live_balance > 0:
                self.portfolio = Portfolio(live_balance)
                logger.info(f"Live mode: synced Kalshi balance — ${live_balance:.2f}")
            else:
                logger.warning("Live mode: could not fetch Kalshi balance — using STARTING_BALANCE")

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
        self._ready_at = time.monotonic() + 10  # 10s — enough for WebSocket connect + backfill

        # Graceful shutdown on SIGTERM (e.g. kill, systemd, task runner)
        signal.signal(signal.SIGTERM, lambda s, f: self.stop("SIGTERM"))

        self.feed.start_streams(
            on_15m=self._on_15m_candle,
            on_tick=self._on_tick,
        )

        logger.info("Paper trader running. Waiting for signals on BTC, ETH, SOL, XRP, DOGE...")

        # Prime the news context in background so the first window has data
        # without blocking the main thread during startup
        if NEWS_ENABLED:
            t = threading.Thread(target=NewsContext.fetch, args=[list(ASSETS.keys())], daemon=True)
            t.start()

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
        state = self.assets[asset]  # dict structure is fixed at init — safe outside lock
        with self._lock:
            row = pd.DataFrame([candle])[["time", "open", "high", "low", "close", "volume"]]
            candle_time = row["time"].iloc[0]
            df = state["df_15m"]
            if not df.empty and pd.Timestamp(df["time"].iloc[-1]) == pd.Timestamp(candle_time):
                # Same candle arrived twice (backfill + WS, or WS reconnect) — update in place
                state["df_15m"].iloc[-1] = row.iloc[0]
            else:
                state["df_15m"] = pd.concat([df, row], ignore_index=True).tail(300)
        # Append to CSV outside the paper lock — History has its own internal lock
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

        # Data freshness check: don't trade on stale candles.
        # load_all() fetches current data on startup, so this should always pass.
        # Guards against edge cases where REST fetch lagged or CSV was very old.
        try:
            latest_candle_time = pd.to_datetime(
                state["df_15m"]["time"].iloc[-1], format="mixed"
            )
            if latest_candle_time.tzinfo is None:
                latest_candle_time = latest_candle_time.replace(tzinfo=timezone.utc)
            age_minutes = (datetime.now(timezone.utc) - latest_candle_time).total_seconds() / 60
            if age_minutes > 30:
                return  # Data too stale — wait for next live candle
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

        t = threading.Thread(target=self._execute_trade, args=[asset, direction, decision["signals"], decision["confidence"]])
        t.daemon = True
        t.start()

    def _get_kalshi_ticker(self, asset: str) -> str | None:
        """Return the current Kalshi ticker for an asset, using a 15-second TTL cache."""
        now = time.monotonic()
        cache = self._ticker_cache[asset]
        if cache["ticker"] and (now - cache["ts"]) < 15:
            return cache["ticker"]
        m = self.kalshi.get_market_for_asset(asset)
        ticker = m.get("ticker") if m else None
        self._ticker_cache[asset] = {"ticker": ticker, "ts": now}
        return ticker

    def _execute_trade(self, asset: str, direction: str, signals: dict, confidence: int = 3):
        # One-trade-per-window: compute the current 15M window boundary and block
        # re-entry if this asset already has a trade open in this window.
        _now_cw = datetime.now(timezone.utc)
        _cw_min = _now_cw.minute - (_now_cw.minute % 15)
        current_window = _now_cw.replace(minute=_cw_min, second=0, microsecond=0, tzinfo=None)
        with self._lock:
            last_window = self._last_trade_window[asset]
        if last_window == current_window:
            logger.info(f"{asset}: already traded this window ({current_window.strftime('%H:%M')}) — skipping")
            self._release_trade_slot(asset)
            return

        kalshi_ticker = self._get_kalshi_ticker(asset)

        if not kalshi_ticker:
            logger.info(f"{asset}: no Kalshi market found — skipping trade")
            self._release_trade_slot(asset)
            return

        # Retry every 30s for up to 14 minutes (window is 15 min; stop before it closes).
        # Always fetch BOTH yes and no prices in one call so we can use the live
        # market probability (yes_ask) to determine the optimal direction.
        retry_start = time.monotonic()
        WINDOW_SECS = 14 * 60  # 14 minutes
        attempt = 0
        yes_price, no_price = 1.0, 1.0
        while time.monotonic() - retry_start < WINDOW_SECS:
            fresh_ticker = self._get_kalshi_ticker(asset) or kalshi_ticker
            yes_price, no_price = self.kalshi.get_market_prices(fresh_ticker)
            if 0.05 <= yes_price <= 0.95:
                kalshi_ticker = fresh_ticker
                break
            attempt += 1
            remaining = WINDOW_SECS - (time.monotonic() - retry_start)
            if remaining < 30:
                break
            logger.info(f"{asset}: Kalshi market not yet priced (price={yes_price:.2f}) — retrying in 30s (attempt {attempt})")
            time.sleep(30)

        if not (0.05 <= yes_price <= 0.95):
            logger.info(f"{asset}: no real Kalshi price available — skipping trade")
            self._release_trade_slot(asset)
            return

        # Near-fair filter: only trade when YES is in the viable payout zone.
        # Outside [0.35, 0.65] the market is highly confident and break-even accuracy
        # exceeds ~68% — unreachable with technical signals. Skip these windows.
        if not (CONTRACT_PRICE_MIN <= yes_price <= CONTRACT_PRICE_MAX):
            logger.info(
                f"{asset}: market too confident (yes={yes_price:.2f}) — "
                f"outside near-fair zone [{CONTRACT_PRICE_MIN:.2f}, {CONTRACT_PRICE_MAX:.2f}], skipping"
            )
            self._release_trade_slot(asset)
            return

        # Determine market direction and whether this is a contrarian trade.
        market_direction = LONG if yes_price >= 0.50 else SHORT
        is_contrarian = (market_direction != direction)

        if is_contrarian:
            # VWAP confirmation: price must be stretched in the direction of the trade.
            # Contrarian SHORT needs price ABOVE VWAP (stretched high → mean reversion down).
            # Contrarian LONG needs price BELOW VWAP (stretched low → mean reversion up).
            # Without this, we can be shorting a price already below VWAP — no edge.
            price_above_vwap = signals["price"] > signals["vwap"]
            if direction == SHORT and not price_above_vwap:
                logger.info(
                    f"{asset}: contrarian SHORT skipped — price below VWAP "
                    f"({signals['price']:.2f} < {signals['vwap']:.2f}), no mean-reversion setup"
                )
                self._release_trade_slot(asset)
                return
            if direction == LONG and price_above_vwap:
                logger.info(
                    f"{asset}: contrarian LONG skipped — price above VWAP "
                    f"({signals['price']:.2f} > {signals['vwap']:.2f}), no mean-reversion setup"
                )
                self._release_trade_slot(asset)
                return

            # RSI must agree: the 1H trend should support the trade direction.
            # A contrarian SHORT with bullish 1H RSI is fighting two levels of trend.
            # "neutral" RSI (47–53) is not sufficient conviction to oppose the market.
            rsi_agrees = (
                (direction == SHORT and signals["rsi_bias"] == "bear") or
                (direction == LONG  and signals["rsi_bias"] == "bull")
            )
            if not rsi_agrees:
                logger.info(
                    f"{asset}: contrarian {direction.upper()} skipped — "
                    f"RSI={signals['rsi']:.1f} ({signals['rsi_bias']}) opposes direction"
                )
                self._release_trade_slot(asset)
                return

            logger.info(
                f"{asset}: contrarian — signal={direction} vs market yes={yes_price:.2f} "
                f"(buying {'YES at discount' if direction == LONG else 'NO at discount'})"
            )

        # Log signal context at every entry to enable future backtesting of filter thresholds.
        vwap_pos = "above" if signals["price"] > signals["vwap"] else "below"
        logger.info(
            f"{asset}: signal-context | rsi={signals['rsi']:.1f}({signals['rsi_bias']}) | "
            f"price {vwap_pos} vwap ({signals['price']:.2f} vs {signals['vwap']:.2f})"
        )

        side = "yes" if direction == LONG else "no"
        contract_price = yes_price if direction == LONG else no_price

        # Kelly-optimal sizing: bet more when YES is near 0.50 (best EV), less when market is confident
        distance = abs(contract_price - 0.50)
        if distance <= 0.05:
            size = BET_NEAR_FAIR      # YES 0.45–0.55
        elif distance <= 0.10:
            size = BET_SLIGHT_LEAN    # YES 0.40–0.60
        elif distance <= 0.15:
            size = BET_MOD_LEAN       # YES 0.35–0.65
        else:
            size = BET_STRONG_LEAN    # YES outside 0.35–0.65

        contracts = math.floor(size / contract_price)
        if contracts < 1:
            self._release_trade_slot(asset)
            return

        # --- Live order placement ---
        # Paper mode skips this block and assumes a perfect fill at contract_price.
        # Live mode places a real limit order and waits for fill confirmation.
        contracts_filled = contracts
        if self.live:
            order = self.kalshi.place_limit_order(
                kalshi_ticker, side, contracts,
                price_cents=round(contract_price * 100),
            )
            if not order:
                logger.error(f"{asset}: order placement failed — skipping")
                self._release_trade_slot(asset)
                return
            filled_order = self.kalshi.wait_for_fill(order["order_id"])
            if not filled_order:
                logger.warning(f"{asset}: order expired unfilled — skipping")
                self._release_trade_slot(asset)
                return
            contracts_filled = filled_order.get("filled_count", contracts)
            if contracts_filled < 1:
                logger.warning(f"{asset}: zero contracts filled — skipping")
                self._release_trade_slot(asset)
                return
            contracts = contracts_filled

        actual_cost      = contracts * contract_price
        fee_entry        = KALSHI_MAKER_FEE * contracts * min(contract_price, 1 - contract_price)
        total_cost       = round(actual_cost + fee_entry, 2)
        # Net payout if win = gross - entry fee - settlement fee (both legs)
        payout           = round(contracts * 1.00 - 2 * fee_entry, 2)
        price_pct        = contract_price * 100
        live_price = self.assets[asset]["price"]
        with self._lock:
            self._open_stake = round(self._open_stake + total_cost, 2)
            portfolio_after  = round(self.portfolio.total - self._open_stake, 2)
            self._last_trade_window[asset] = current_window  # one-trade-per-window

        trade_id = self.trade_log.open_trade(
            direction=direction,
            contracts=contracts,
            contracts_filled=contracts_filled,
            contract_price_pct=price_pct,
            cost=total_cost,
            possible_payout=payout,
            btc_price=live_price,
            signals=signals,
            asset=asset,
        )

        log_trade(direction, live_price, total_cost)
        self.monitor.record_order_placed()
        self.discord.buy(
            direction=direction,
            contracts=contracts,
            contracts_filled=contracts,
            price_pct=price_pct,
            cost=total_cost,
            payout=payout,
            portfolio_total=portfolio_after,
            asset=asset,
            session_wins=self.session_wins,
            session_losses=self.session_losses,
            session_pnl=self.session_pnl,
        )

        # Compute the settlement candle's open time and exact seconds until settlement.
        # Fire at the next 15M boundary (not 15 min from now) so we don't poll late
        # when the trade was entered partway through a window.
        _now = datetime.now(timezone.utc)
        _m = _now.minute - (_now.minute % 15)
        settlement_open = _now.replace(minute=_m, second=0, microsecond=0, tzinfo=None)
        seconds_into_window = (_now.minute % 15) * 60 + _now.second + _now.microsecond / 1e6
        seconds_until_settlement = max(5.0, (15 * 60) - seconds_into_window + 5)  # +5s buffer

        t = threading.Thread(
            target=self._monitor_position,
            args=[asset, direction, contracts, contract_price, price_pct, trade_id,
                  kalshi_ticker, settlement_open, seconds_until_settlement]
        )
        t.daemon = True  # Dies with the process — no ghost resolutions after restart
        t.start()

    def _monitor_position(self, asset: str, direction: str, contracts: int,
                          contract_price: float, price_pct: float, trade_id: str,
                          kalshi_ticker: str, settlement_open, seconds_until_settlement: float):
        """
        Poll the Kalshi contract price every 30s after entry and exit early if:
          - Stop-loss: contract price drops to <= STOP_LOSS_PRICE (default 0.25)
          - Trailing profit: once contract price >= TRAILING_TRIGGER (default 0.75),
            sell immediately on any drop back below the observed peak.
        Falls through to normal Kalshi settlement resolution if neither fires.
        """
        side = "yes" if direction == LONG else "no"
        high_water     = contract_price
        trailing_armed = False
        poll_interval  = 30
        deadline       = time.monotonic() + seconds_until_settlement

        while self.running:
            remaining = deadline - time.monotonic()
            if remaining <= poll_interval:
                # Close enough to settlement — hand off to the resolver
                if remaining > 0:
                    time.sleep(remaining)
                break

            time.sleep(poll_interval)

            fresh_ticker  = self._get_kalshi_ticker(asset) or kalshi_ticker
            if not fresh_ticker:
                continue
            current_price = self.kalshi.get_market_price(fresh_ticker, side)

            # Skip stale/invalid prices (demo API commonly returns 0.0 or 1.0 mid-window)
            if not (0.05 < current_price < 0.95):
                continue

            # Update high water mark
            if current_price > high_water:
                high_water = current_price

            # Arm trailing profit once the contract has reached the trigger threshold
            if not trailing_armed and high_water >= TRAILING_TRIGGER:
                trailing_armed = True
                logger.info(
                    f"{asset}: trailing-profit armed — {side.upper()} peaked at {high_water:.2f}"
                )

            # Trailing profit exit: any drop below the peak locks in gains
            if trailing_armed and current_price < high_water:
                logger.info(
                    f"{asset}: trailing-profit EXIT — peaked {high_water:.2f} → now {current_price:.2f}"
                )
                self._exit_early(asset, direction, contracts, contract_price,
                                 current_price, trade_id, "trailing-profit",
                                 kalshi_ticker=fresh_ticker)
                return

            # Stop-loss exit: contract lost most of its value — cut remaining risk
            if current_price <= STOP_LOSS_PRICE:
                logger.info(
                    f"{asset}: stop-loss EXIT — {side.upper()} at {current_price:.2f} "
                    f"(<= {STOP_LOSS_PRICE})"
                )
                self._exit_early(asset, direction, contracts, contract_price,
                                 current_price, trade_id, "stop-loss",
                                 kalshi_ticker=fresh_ticker)
                return

        if self.running:
            # No early exit triggered — resolve at Kalshi settlement
            self._resolve_trade(asset, direction, contracts, contract_price, price_pct,
                                trade_id, kalshi_ticker, settlement_open)

    def _exit_early(self, asset: str, direction: str, contracts: int,
                    contract_price: float, exit_price: float,
                    trade_id: str, reason: str, kalshi_ticker: str = None):
        """
        Resolve a trade early at exit_price (stop-loss or trailing profit).
        PnL = sale proceeds − entry cost − entry fee − exit fee.

        In live mode: places a real sell order before recording the result.
        Stop-loss uses an aggressive price (1 cent) to guarantee a fill.
        Trailing-profit uses the observed exit price for a fair execution.
        """
        # --- Live sell order ---
        if self.live and kalshi_ticker:
            side = "yes" if direction == LONG else "no"
            if reason == "stop-loss":
                sell_cents = 1  # Guarantee fill — we're cutting losses, price is irrelevant
            else:
                sell_cents = max(1, round(exit_price * 100))
            self.kalshi.sell_position(kalshi_ticker, side, contracts, sell_cents)

        actual_cost   = contracts * contract_price
        exit_proceeds = contracts * exit_price
        fee_entry     = KALSHI_MAKER_FEE * contracts * min(contract_price, 1 - contract_price)
        fee_exit      = KALSHI_MAKER_FEE * contracts * min(exit_price,     1 - exit_price)
        fee_paid      = round(fee_entry + fee_exit, 4)
        pnl           = round(exit_proceeds - actual_cost - fee_paid, 4)
        win           = pnl > 0
        total_cost    = round(actual_cost + fee_entry, 2)
        live_price    = self.assets[asset]["price"]

        with self._lock:
            self._open_stake = max(0.0, round(self._open_stake - total_cost, 2))
            df = self.assets[asset]["df_15m"]
            last_candle = df.iloc[-1].to_dict() if not df.empty else {}

        if win:
            with self._lock:
                self.session_wins += 1
                self.session_pnl   = round(self.session_pnl + pnl, 4)
                self.portfolio.record_win(pnl)
                s_wins, s_losses, s_pnl = self.session_wins, self.session_losses, self.session_pnl
                port_total   = self.portfolio.total
                port_summary = self.portfolio.summary()
            log_trade(direction, live_price, actual_cost, result="win", pnl=pnl)
            self.monitor.record_trade_result("win")
            self.trade_log.close_trade(trade_id, "win", pnl, fee_paid, last_candle, port_summary)
            self.discord.sell_win(
                direction=direction, contracts=contracts, contracts_filled=contracts,
                price_pct=exit_price * 100, pnl=pnl, portfolio_total=port_total,
                asset=asset, session_wins=s_wins, session_losses=s_losses, session_pnl=s_pnl,
            )
        else:
            with self._lock:
                self.session_losses += 1
                self.session_pnl     = round(self.session_pnl + pnl, 4)
                self.portfolio.record_loss(abs(pnl))
                s_wins, s_losses, s_pnl = self.session_wins, self.session_losses, self.session_pnl
                port_total   = self.portfolio.total
                port_summary = self.portfolio.summary()
            log_trade(direction, live_price, actual_cost, result="loss", pnl=pnl)
            self.monitor.record_trade_result("loss")
            self.trade_log.close_trade(trade_id, "loss", pnl, fee_paid, last_candle, port_summary)
            self.discord.sell_loss(
                direction=direction, contracts=contracts, contracts_filled=contracts,
                price_pct=exit_price * 100, pnl=pnl, portfolio_total=port_total,
                asset=asset, session_wins=s_wins, session_losses=s_losses, session_pnl=s_pnl,
            )

        logger.info(
            f"{asset}: {reason} | exit={exit_price:.2f} entry={contract_price:.2f} | pnl=${pnl:+.2f}"
        )

        with self._lock:
            can_trade = self.portfolio.can_trade()
            port_halt = self.portfolio.total
        if not FORCE_TRADE and not can_trade:
            halt_reason = "Daily loss limit reached"
            log_halt(halt_reason)
            self.monitor.set_halt(True, halt_reason)
            if not self._stopped:
                self._stopped = True
                self.discord.bot_stopped(port_halt)

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
            # Floor to minute to handle any sub-second precision in WS-sourced timestamps.
            last = None
            if settlement_open is not None:
                target = pd.Timestamp(settlement_open).floor("min")
                df_times = pd.to_datetime(df["time"], format="mixed").dt.floor("min")
                match = df[df_times == target]
                if not match.empty:
                    last = match.iloc[0]
            if last is None:
                if settlement_open is not None:
                    # Fallback: most recent candle at or before settlement_open.
                    # Avoids accidentally using a future candle if the next window has
                    # already arrived by the time this timer fires.
                    target_ts = pd.Timestamp(settlement_open)
                    past = df[pd.to_datetime(df["time"], format="mixed") <= target_ts]
                    last = past.iloc[-1] if not past.empty else df.iloc[-1]
                else:
                    last = df.iloc[-1]
            went_up    = last["close"] > last["open"]
            last_candle = last.to_dict()

        if kalshi_result:
            kalshi_side = "yes" if direction == LONG else "no"
            win = (kalshi_result == kalshi_side)
        else:
            win = (direction == LONG and went_up) or (direction == SHORT and not went_up)

        actual_cost  = contracts * contract_price
        fee_one_leg  = KALSHI_MAKER_FEE * contracts * min(contract_price, 1 - contract_price)
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
                pnl=pnl,   # negative; discord.py applies abs() for display
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

    def _release_trade_slot(self, asset: str):
        """Return a pre-claimed trade slot when a trade is ultimately skipped."""
        with self._lock:
            self.trades_this_hour[asset] = max(0, self.trades_this_hour[asset] - 1)

    # ------------------------------------------------------------------ #
    #  Heartbeat                                                           #
    # ------------------------------------------------------------------ #

    def _heartbeat(self):
        try:
            elapsed = 0
            while self.running:
                time.sleep(10)         # Short sleep so SIGTERM exits within 10s
                elapsed += 10
                if elapsed < 900:
                    continue
                elapsed = 0
                self.monitor.print_status()
                # Refresh news context in background — HTTP calls can take up to 20s
                # and must not block the heartbeat or 1H candle refresh
                if NEWS_ENABLED:
                    t = threading.Thread(target=NewsContext.fetch, args=[list(ASSETS.keys())], daemon=True)
                    t.start()
                # Refresh 1H candles from REST every 15 min (matches heartbeat cadence)
                # Keeps RSI/MACD trend filter at most ~15 min stale instead of ~60 min
                now = time.monotonic()
                if now - self._last_1h_refresh >= 900:
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
        # Returns True exactly once per calendar day (ET), tracking the last reset date.
        # Robust to heartbeat timing — won't miss midnight if heartbeat skips a window.
        from datetime import timedelta
        now_et = datetime.now(timezone.utc) - timedelta(hours=4)
        today = now_et.date()
        if self._last_reset_date is None:
            self._last_reset_date = today  # Initialize on first call — no reset
            return False
        if today != self._last_reset_date:
            self._last_reset_date = today
            return True
        return False
