import logging
import os
from datetime import datetime, timezone

LOGS_DIR = os.path.join(os.path.dirname(__file__), "..", "logs")


def _setup():
    os.makedirs(LOGS_DIR, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # --- Specific file handlers (trades + errors only) ---
    # General bot output goes to stdout, captured by systemd → bot.log on VPS,
    # or printed to terminal during local development. No separate bot_*.log file.
    trade_handler = logging.FileHandler(os.path.join(LOGS_DIR, f"trades_{today}.log"))
    trade_handler.setFormatter(fmt)
    trade_handler.setLevel(logging.INFO)

    error_handler = logging.FileHandler(os.path.join(LOGS_DIR, f"errors_{today}.log"))
    error_handler.setFormatter(fmt)
    error_handler.setLevel(logging.ERROR)

    # --- Console handler (stdout → systemd on VPS, terminal locally) ---
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(fmt)
    console_handler.setLevel(logging.INFO)

    # --- Root logger ---
    root = logging.getLogger()
    root.setLevel(logging.DEBUG)
    root.addHandler(console_handler)

    # --- Trade logger (separate file) ---
    trade_log = logging.getLogger("trade")
    trade_log.addHandler(trade_handler)

    # --- Error logger (separate file) ---
    error_log = logging.getLogger("error")
    error_log.addHandler(error_handler)


_setup()


# ------------------------------------------------------------------ #
#  Public helpers                                                      #
# ------------------------------------------------------------------ #

def get(name="bot"):
    return logging.getLogger(name)


def log_trade(direction: str, price: float, size: float, confidence_pct: float = 0.0,
              result: str = None, pnl: float = None):
    """Log a trade entry or result to the trade log."""
    logger = logging.getLogger("trade")
    if result is None:
        logger.info(
            f"ENTRY | {direction.upper()} | price={price:.4f} | size=${size:.2f} "
            f"| conf={confidence_pct:.1f}%"
        )
    else:
        logger.info(
            f"RESULT | {result.upper()} | direction={direction.upper()} "
            f"| price={price:.4f} | size=${size:.2f} | pnl=${pnl:+.2f} "
            f"| conf={confidence_pct:.1f}%"
        )


def log_signal(rsi: float, macd: float, momentum: float, vwap_diff: float, decision: str):
    """Log signal values and the resulting decision."""
    logger = logging.getLogger("bot")
    logger.debug(
        f"SIGNAL | RSI={rsi:.2f} | MACD={macd:.4f} | "
        f"MOM={momentum:+.4f} | VWAP_diff={vwap_diff:+.4f} | => {decision.upper()}"
    )


def log_halt(reason: str):
    """Log when the bot halts trading."""
    logger = logging.getLogger("bot")
    logger.warning(f"HALT | {reason}")


def log_error(message: str, exc: Exception = None):
    """Log an error with optional exception."""
    logger = logging.getLogger("error")
    if exc:
        logger.error(f"{message} | {type(exc).__name__}: {exc}", exc_info=True)
    else:
        logger.error(message)
