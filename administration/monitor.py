import time
import threading
from datetime import datetime, timezone
from administration.logger import get as get_logger

logger = get_logger("monitor")


class Monitor:
    def __init__(self):
        self._lock = threading.Lock()
        self.start_time = time.monotonic()
        self.start_dt = datetime.now(timezone.utc)

        # Connection status
        self.kraken_connected = False
        self.kalshi_connected = False
        self.discord_connected = False

        # Session stats
        self.trades_today = 0
        self.wins_today = 0
        self.losses_today = 0
        self.signals_evaluated = 0
        self.signals_fired = 0
        self.orders_placed = 0
        self.orders_cancelled = 0

        # Current state
        self.open_positions = []
        self.last_signal = None
        self.last_trade_time = None
        self.is_halted = False
        self.halt_reason = None

    # ------------------------------------------------------------------ #
    #  Connection Status                                                   #
    # ------------------------------------------------------------------ #

    def set_connected(self, service: str, status: bool):
        with self._lock:
            if service == "kraken":
                self.kraken_connected = status
            elif service == "kalshi":
                self.kalshi_connected = status
            elif service == "discord":
                self.discord_connected = status
        logger.info(f"{service.upper()} connection: {'OK' if status else 'LOST'}")

    def all_connected(self) -> bool:
        return self.kraken_connected and self.kalshi_connected

    # ------------------------------------------------------------------ #
    #  Event Tracking                                                      #
    # ------------------------------------------------------------------ #

    def record_signal(self, direction: str, signals: dict):
        with self._lock:
            self.signals_evaluated += 1
            self.last_signal = {
                "time": datetime.now(timezone.utc).isoformat(),
                "direction": direction,
                "signals": signals,
            }
            if direction != "none":
                self.signals_fired += 1

    def record_order_placed(self):
        with self._lock:
            self.orders_placed += 1
            self.last_trade_time = datetime.now(timezone.utc).isoformat()

    def record_order_cancelled(self):
        with self._lock:
            self.orders_cancelled += 1

    def record_trade_result(self, result: str):
        with self._lock:
            self.trades_today += 1
            if result == "win":
                self.wins_today += 1
            else:
                self.losses_today += 1

    def set_halt(self, halted: bool, reason: str = None):
        with self._lock:
            self.is_halted = halted
            self.halt_reason = reason if halted else None
        if halted:
            logger.warning(f"Bot halted: {reason}")

    def update_positions(self, positions: list):
        with self._lock:
            self.open_positions = positions

    # ------------------------------------------------------------------ #
    #  Summary                                                             #
    # ------------------------------------------------------------------ #

    def uptime(self) -> str:
        elapsed = int(time.monotonic() - self.start_time)
        h, remainder = divmod(elapsed, 3600)
        m, s = divmod(remainder, 60)
        return f"{h:02d}:{m:02d}:{s:02d}"

    def win_rate(self) -> float:
        if self.trades_today == 0:
            return 0.0
        return round(self.wins_today / self.trades_today, 4)

    def status(self) -> dict:
        return {
            "uptime":             self.uptime(),
            "started":            self.start_dt.strftime("%Y-%m-%d %H:%M:%S UTC"),
            "halted":             self.is_halted,
            "halt_reason":        self.halt_reason,
            "connections": {
                "kraken":         self.kraken_connected,
                "kalshi":         self.kalshi_connected,
                "discord":        self.discord_connected,
            },
            "today": {
                "trades":         self.trades_today,
                "wins":           self.wins_today,
                "losses":         self.losses_today,
                "win_rate":       f"{self.win_rate() * 100:.1f}%",
                "signals_eval":   self.signals_evaluated,
                "signals_fired":  self.signals_fired,
                "orders_placed":  self.orders_placed,
                "orders_cancelled": self.orders_cancelled,
            },
            "open_positions":     len(self.open_positions),
            "last_trade":         self.last_trade_time,
            "last_signal":        self.last_signal,
        }

    def print_status(self):
        s = self.status()
        logger.info(
            f"STATUS | uptime={s['uptime']} | "
            f"entries={s['today']['orders_placed']} | "
            f"settled={s['today']['trades']} ({s['today']['wins']}W-{s['today']['losses']}L) | "
            f"win_rate={s['today']['win_rate']} | "
            f"halted={s['halted']}"
        )
