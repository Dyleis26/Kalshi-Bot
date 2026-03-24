"""
main.py — Entry point for the Kalshi BTC 15M bot.

Modes:
  python -m administration.main paper      → Paper trade (default)
  python -m administration.main backtest   → Run backtest on historical data
  python -m administration.main optimize   → Run RBI optimizer
"""

import sys
from administration.security import validate_env, kill
from administration.logger import get as get_logger, log_error

logger = get_logger("main")


def run_paper():
    from testing.paper import PaperTrader
    trader = PaperTrader()
    try:
        trader.start()
    except Exception as e:
        log_error("Paper trader crashed", e)
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
