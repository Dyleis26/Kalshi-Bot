"""
main.py — Entry point for the Kalshi BTC 15M bot.

Modes:
  python -m administration.main paper      → Paper trade (default)
  python -m administration.main backtest   → Run backtest on historical data
  python -m administration.main optimize   → Run RBI optimizer
"""

import os
import sys
import atexit
from administration.security import validate_env, kill
from administration.logger import get as get_logger, log_error

logger = get_logger("main")

PID_FILE = "/tmp/kalshi_bot.pid"


def _acquire_pid_lock():
    """Prevent multiple bot instances. Exits if another instance is already running."""
    if os.path.exists(PID_FILE):
        with open(PID_FILE) as f:
            old_pid = int(f.read().strip())
        try:
            os.kill(old_pid, 0)  # 0 = just check existence
            logger.error(f"Bot already running (PID {old_pid}). Stop it first.")
            sys.exit(1)
        except ProcessLookupError:
            pass  # Stale PID file from a crashed run — safe to continue
    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))
    atexit.register(lambda: os.path.exists(PID_FILE) and os.remove(PID_FILE))


def run_paper():
    from testing.paper import PaperTrader
    trader = PaperTrader()
    try:
        trader.start()
    except Exception as e:
        log_error("Paper trader crashed", e)
        trader.stop("Crash")
        kill()
        raise


def run_backtest():
    from data.history import History
    from testing.backtest import Backtest
    from testing.metrics import print_summary

    logger.info("Loading historical data...")
    h = History()
    data = h.load_all()

    logger.info("Running backtest...")
    bt = Backtest(df_1h=data["1h"], df_15m=data["15m"])
    metrics = bt.run()
    print_summary(metrics)


def run_optimizer():
    from data.history import History
    from testing.optimizer import Optimizer

    logger.info("Loading historical data for optimizer...")
    h = History()
    data = h.load_all()

    logger.info("Starting RBI optimizer...")
    opt = Optimizer(df_1h=data["1h"], df_15m=data["15m"])
    top = opt.run(top_n=5, rank_by="sharpe")

    if top:
        logger.info(f"Best config: {top[0]['params']}")
    else:
        logger.warning("Optimizer found no valid configs.")


def main():
    mode = sys.argv[1].lower() if len(sys.argv) > 1 else "paper"

    logger.info(f"Starting bot in [{mode}] mode")

    try:
        validate_env()
    except ValueError as e:
        logger.error(f"Environment setup incomplete: {e}")
        sys.exit(1)

    if mode == "paper":
        _acquire_pid_lock()
        run_paper()
    elif mode == "backtest":
        run_backtest()
    elif mode == "optimize":
        run_optimizer()
    else:
        print(f"Unknown mode: {mode}")
        print("Usage: python -m administration.main [paper|backtest|optimize]")
        sys.exit(1)


if __name__ == "__main__":
    main()
