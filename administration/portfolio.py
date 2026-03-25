from administration.config import (
    STARTING_BALANCE, CASH_SPLIT, CAPITAL_SPLIT,
    PROFIT_TO_CASH, PROFIT_TO_CAPITAL,
    DAILY_LOSS_LIMIT, MAX_LOSING_STREAK, LOSING_STREAK_REDUCTION,
    MIN_BET
)


class Portfolio:
    def __init__(self, starting_balance=None):
        balance = starting_balance or STARTING_BALANCE
        self.cash = round(balance * CASH_SPLIT, 2)
        self.capital = round(balance * CAPITAL_SPLIT, 2)

        # Daily tracking — reset each session
        self.capital_at_day_start = self.capital
        self.daily_pnl = 0.0

        # Streak tracking
        self.losing_streak = 0
        self.is_halted = False

        # Trade history
        self.trades = []

    # ------------------------------------------------------------------ #
    #  Properties                                                          #
    # ------------------------------------------------------------------ #

    @property
    def total(self):
        return round(self.cash + self.capital, 2)

    @property
    def daily_loss_limit(self):
        """50% of capital at start of day."""
        return round(self.capital_at_day_start * DAILY_LOSS_LIMIT, 2)

    @property
    def size_multiplier(self):
        """Reduces bet size by 50% when on a losing streak."""
        if self.losing_streak >= MAX_LOSING_STREAK:
            return LOSING_STREAK_REDUCTION
        return 1.0

    # ------------------------------------------------------------------ #
    #  Trade Results                                                       #
    # ------------------------------------------------------------------ #

    def record_win(self, profit: float):
        """
        On a win: split profit 50% to cash, 50% to capital.
        Reset losing streak.
        """
        to_cash = round(profit * PROFIT_TO_CASH, 2)
        to_capital = round(profit - to_cash, 2)  # Avoid rounding drift from independent splits

        self.cash += to_cash
        self.capital += to_capital
        self.daily_pnl += profit
        self.losing_streak = 0

        self.trades.append({"result": "win", "amount": profit})
        return to_cash, to_capital

    def record_loss(self, loss: float):
        """
        On a loss: deduct from capital only. Cash is never touched.
        Increment losing streak. Check halt condition.
        """
        self.capital = round(self.capital - abs(loss), 2)
        self.daily_pnl -= abs(loss)
        self.losing_streak += 1

        self.trades.append({"result": "loss", "amount": -abs(loss)})
        self._check_halt()
        return self.capital

    # ------------------------------------------------------------------ #
    #  Guards                                                              #
    # ------------------------------------------------------------------ #

    def can_trade(self):
        """Returns True if bot is allowed to place a new trade."""
        if self.is_halted:
            return False
        if self.capital < MIN_BET:
            return False
        return True

    def reset_day(self):
        """Call at the start of each trading day."""
        self.capital_at_day_start = self.capital
        self.daily_pnl = 0.0
        self.is_halted = False

    # ------------------------------------------------------------------ #
    #  Summary                                                             #
    # ------------------------------------------------------------------ #

    def summary(self):
        return {
            "total": self.total,
            "cash": self.cash,
            "capital": self.capital,
            "daily_pnl": round(self.daily_pnl, 2),
            "daily_loss_limit": self.daily_loss_limit,
            "losing_streak": self.losing_streak,
            "size_multiplier": self.size_multiplier,
            "halted": self.is_halted,
            "trades": len(self.trades),
        }

    # ------------------------------------------------------------------ #
    #  Private                                                             #
    # ------------------------------------------------------------------ #

    def _check_halt(self):
        """Halt trading if daily loss exceeds the daily loss limit."""
        if self.daily_pnl <= -self.daily_loss_limit:
            self.is_halted = True
