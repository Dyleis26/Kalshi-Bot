import math
import pandas as pd
from administration.portfolio import Portfolio
from administration.config import (
    STARTING_BALANCE, KALSHI_TAKER_FEE, KALSHI_MAKER_FEE,
    SLOT_CAPITAL_PCT, BET_PCT_OF_SLOT,
)
from strategy.base import Strategy, LONG, SHORT, NONE
from testing.metrics import calculate, print_summary
from administration.logger import get as get_logger

logger = get_logger("backtest")

# Minimum candles needed before signals are reliable
MIN_1H_CANDLES = 35    # RSI(14) + MACD(26) need ~35 candles to stabilise
MIN_15M_CANDLES = 10   # VWAP + momentum need at least 10 candles


class Backtest:
    def __init__(self, df_1h: pd.DataFrame, df_15m: pd.DataFrame,
                 starting_balance: float = STARTING_BALANCE,
                 use_maker_fee: bool = True):
        """
        Args:
            df_1h:             Historical 1H candles (time, open, high, low, close, volume)
            df_15m:            Historical 15M candles (same format)
            starting_balance:  Starting portfolio balance
            use_maker_fee:     True = maker (limit order) fees, False = taker fees
        """
        self.df_1h = df_1h.reset_index(drop=True)
        self.df_15m = df_15m.reset_index(drop=True)
        self.portfolio = Portfolio(starting_balance)
        self.strategy = Strategy()
        self.fee_rate = KALSHI_MAKER_FEE if use_maker_fee else KALSHI_TAKER_FEE
        self.trades = []

    # ------------------------------------------------------------------ #
    #  Run                                                                 #
    # ------------------------------------------------------------------ #

    def run(self) -> dict:
        """
        Replay historical data through the strategy.
        For each 15M candle, evaluate signals and simulate a trade.
        Returns metrics dict.
        """
        logger.info(f"Backtest starting — {len(self.df_15m)} 15M candles, "
                    f"{len(self.df_1h)} 1H candles")

        self.portfolio.reset_day()
        skipped = 0
        current_day = None

        for i in range(MIN_15M_CANDLES, len(self.df_15m) - 1):
            candle = self.df_15m.iloc[i]

            # Match 1H candles up to this point in time
            candle_time = candle["time"]

            # Daily reset — mirrors paper/live behavior
            candle_date = pd.Timestamp(candle_time).date()
            if current_day is None:
                current_day = candle_date
            elif candle_date != current_day:
                current_day = candle_date
                self.portfolio.reset_day()

            df_1h_window = self.df_1h[pd.to_datetime(self.df_1h["time"]) <= pd.Timestamp(candle_time)]

            if len(df_1h_window) < MIN_1H_CANDLES:
                skipped += 1
                continue

            df_15m_window = self.df_15m.iloc[: i + 1]

            # Check portfolio can trade
            if not self.portfolio.can_trade():
                logger.info("Portfolio halted — stopping backtest early.")
                break

            # Get signal decision
            decision = self.strategy.decide(df_1h_window, df_15m_window)
            direction = decision["direction"]

            if direction == NONE:
                continue

            # Simulate trade — use same dynamic sizing as paper.py
            contract_price = self._estimate_contract_price(direction, candle)
            size = round(self.portfolio.capital * SLOT_CAPITAL_PCT * BET_PCT_OF_SLOT, 2)
            if math.floor(size / contract_price) < 1:
                skipped += 1
                continue

            # Determine outcome on the next candle (i+1)
            outcome = self._resolve_outcome(direction, i)

            # Calculate PnL
            pnl = self._calculate_pnl(outcome, size, contract_price)

            # Record with portfolio
            if outcome == "win":
                self.portfolio.record_win(pnl)
            else:
                self.portfolio.record_loss(abs(pnl))

            trade = {
                "time":       str(candle_time),
                "direction":  direction,
                "result":     outcome,
                "size":       size,
                "price":      contract_price,
                "pnl":        pnl,
                "capital":    self.portfolio.capital,
                "cash":       self.portfolio.cash,
            }
            self.trades.append(trade)

        logger.info(f"Backtest complete — {len(self.trades)} trades, {skipped} skipped")
        metrics = calculate(self.trades)
        print_summary(metrics)
        return metrics

    # ------------------------------------------------------------------ #
    #  Private                                                             #
    # ------------------------------------------------------------------ #

    def _resolve_outcome(self, direction: str, candle_idx: int):
        """
        Kalshi BTC 15M Up/Down: signals fire on close of candle i, trade
        resolves on the NEXT candle (i+1). Compare next candle's close to open.
        WIN if direction matches actual price movement of the next window.
        """
        candle = self.df_15m.iloc[candle_idx + 1]
        open_price = candle["open"]
        close_price = candle["close"]

        went_up = close_price > open_price

        if direction == LONG and went_up:
            return "win"
        elif direction == SHORT and not went_up:
            return "win"
        else:
            return "loss"

    def _estimate_contract_price(self, direction: str, candle) -> float:
        """
        Estimate contract price. In a real market this is the YES ask price.
        For backtesting, we assume near-50 cent pricing as a conservative default.

        Note: paper.py skips markets where YES is outside [CONTRACT_PRICE_MIN, CONTRACT_PRICE_MAX]
        (default 0.35–0.65). This filter is not simulated here since historical Kalshi prices
        are not available — backtest trade counts will be higher than live trading.
        """
        return 0.50

    def _calculate_pnl(self, outcome: str, size: float, contract_price: float) -> float:
        """
        Calculate net PnL after Kalshi fees.

        Fee formula: fee_rate × contracts × min(price, 1 - price)
        Settlement exit fee = 0 because min(1.0, 0.0) = 0.

        Win:  contracts × $1.00 - size - fee_entry
        Loss: -size - fee_entry
        """
        contracts = math.floor(size / contract_price)
        actual_cost = contracts * contract_price
        fee = self.fee_rate * contracts * min(contract_price, 1 - contract_price)

        if outcome == "win":
            gross = contracts * 1.00
            return round(gross - actual_cost - fee, 4)
        else:
            return round(-actual_cost - fee, 4)
