import json
import os
import itertools
import pandas as pd
from copy import deepcopy
from administration import config as cfg
from testing.backtest import Backtest
from administration.logger import get as get_logger

logger = get_logger("optimizer")

CONFIGS_DIR = os.path.join(os.path.dirname(__file__), "..", "strategy", "configs")

# ------------------------------------------------------------------ #
#  Parameter Grid                                                      #
# ------------------------------------------------------------------ #

DEFAULT_GRID = {
    "RSI_BULL":      [52, 55, 58, 60],
    "RSI_BEAR":      [40, 42, 45, 48],
    "MOMENTUM_MIN":  [0.0002, 0.0005, 0.001, 0.002],
    "KELLY_FRACTION":[0.10, 0.15, 0.20, 0.25],
}


class Optimizer:
    def __init__(self, df_1h: pd.DataFrame, df_15m: pd.DataFrame):
        self.df_1h = df_1h
        self.df_15m = df_15m
        os.makedirs(CONFIGS_DIR, exist_ok=True)

    # ------------------------------------------------------------------ #
    #  Run                                                                 #
    # ------------------------------------------------------------------ #

    def run(self, grid: dict = None, top_n: int = 5, rank_by: str = "sharpe") -> list:
        """
        Grid search over all parameter combinations.

        Args:
            grid:     Parameter grid dict (uses DEFAULT_GRID if None)
            top_n:    Number of top configs to return and save
            rank_by:  Metric to rank by: 'sharpe', 'total_pnl', 'win_rate'

        Returns:
            List of top N config dicts with their metrics, ranked best-first
        """
        grid = grid or DEFAULT_GRID
        combinations = list(itertools.product(*grid.values()))
        keys = list(grid.keys())
        total = len(combinations)

        logger.info(f"RBI Optimizer starting — {total} combinations to test")

        results = []

        for idx, combo in enumerate(combinations, 1):
            params = dict(zip(keys, combo))
            self._apply_params(params)

            try:
                bt = Backtest(self.df_1h.copy(), self.df_15m.copy())
                metrics = bt.run()

                if metrics["total_trades"] < 10:
                    continue  # Not enough trades to be meaningful

                results.append({
                    "params":  params,
                    "metrics": metrics,
                    "score":   metrics.get(rank_by, 0),
                })

                if idx % 10 == 0:
                    logger.info(f"Progress: {idx}/{total} combos tested")

            except Exception as e:
                logger.warning(f"Combo {idx} failed: {params} — {e}")

        self._restore_defaults()

        if not results:
            logger.warning("No valid results from optimizer.")
            return []

        # Sort best first
        results.sort(key=lambda x: x["score"], reverse=True)
        top = results[:top_n]

        self._save_configs(top, rank_by)
        self._print_top(top)
        return top

    # ------------------------------------------------------------------ #
    #  Private                                                             #
    # ------------------------------------------------------------------ #

    def _apply_params(self, params: dict):
        """Temporarily override config values for this backtest run."""
        for key, val in params.items():
            setattr(cfg, key, val)

    def _restore_defaults(self):
        """Restore original config values after all runs."""
        cfg.RSI_BULL       = 55
        cfg.RSI_BEAR       = 45
        cfg.MOMENTUM_MIN   = 0.0005
        cfg.KELLY_FRACTION = 0.20

    def _save_configs(self, top: list, rank_by: str):
        """Save top configs to JSON files in strategy/configs/."""
        for i, result in enumerate(top, 1):
            filename = f"config_rank{i}_{rank_by}.json"
            path = os.path.join(CONFIGS_DIR, filename)
            data = {
                "rank":    i,
                "rank_by": rank_by,
                "params":  result["params"],
                "metrics": result["metrics"],
            }
            with open(path, "w") as f:
                json.dump(data, f, indent=2)
        logger.info(f"Saved top {len(top)} configs to strategy/configs/")

    def _print_top(self, top: list):
        print("\n" + "=" * 60)
        print("  RBI OPTIMIZER — TOP RESULTS")
        print("=" * 60)
        for i, r in enumerate(top, 1):
            p = r["params"]
            m = r["metrics"]
            print(f"\n  Rank #{i}")
            print(f"    RSI Bull/Bear:  {p['RSI_BULL']} / {p['RSI_BEAR']}")
            print(f"    Momentum Min:   {p['MOMENTUM_MIN']}")
            print(f"    Kelly Fraction: {p['KELLY_FRACTION']}")
            print(f"    Sharpe:         {m['sharpe']:.4f}")
            print(f"    Win Rate:       {m['win_rate_pct']}")
            print(f"    Total PnL:      ${m['total_pnl']:+.2f}")
            print(f"    Trades:         {m['total_trades']}")
        print("\n" + "=" * 60 + "\n")


def load_best_config(rank_by: str = "sharpe") -> dict:
    """Load the top-ranked saved config from strategy/configs/."""
    path = os.path.join(CONFIGS_DIR, f"config_rank1_{rank_by}.json")
    if not os.path.exists(path):
        return {}
    with open(path) as f:
        return json.load(f)


def apply_config(config: dict):
    """Apply a saved config's params to the live config module."""
    params = config.get("params", {})
    for key, val in params.items():
        if hasattr(cfg, key):
            setattr(cfg, key, val)
    logger.info(f"Applied config: {params}")
