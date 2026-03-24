import numpy as np
from typing import List


def calculate(trades: List[dict]) -> dict:
    """
    Calculate all performance metrics from a list of trade results.

    Each trade dict must have:
        { "pnl": float, "result": "win"|"loss", "size": float }

    Returns a full metrics dict.
    """
    if not trades:
        return _empty()

    pnls = [t["pnl"] for t in trades]
    wins = [t for t in trades if t["result"] == "win"]
    losses = [t for t in trades if t["result"] == "loss"]

    total_trades = len(trades)
    total_pnl = sum(pnls)
    win_rate = len(wins) / total_trades if total_trades > 0 else 0

    avg_win = np.mean([t["pnl"] for t in wins]) if wins else 0
    avg_loss = abs(np.mean([t["pnl"] for t in losses])) if losses else 0

    profit_factor = (
        sum(t["pnl"] for t in wins) / abs(sum(t["pnl"] for t in losses))
        if losses and sum(t["pnl"] for t in losses) != 0 else float("inf")
    )

    sharpe = _sharpe(pnls)
    max_dd = _max_drawdown(pnls)
    ev = total_pnl / total_trades if total_trades > 0 else 0

    return {
        "total_trades":   total_trades,
        "wins":           len(wins),
        "losses":         len(losses),
        "win_rate":       round(win_rate, 4),
        "win_rate_pct":   f"{win_rate * 100:.2f}%",
        "total_pnl":      round(total_pnl, 2),
        "avg_win":        round(avg_win, 2),
        "avg_loss":       round(avg_loss, 2),
        "profit_factor":  round(profit_factor, 4),
        "sharpe":         round(sharpe, 4),
        "max_drawdown":   round(max_dd, 2),
        "max_drawdown_pct": f"{max_dd:.2f}",
        "ev_per_trade":   round(ev, 4),
        "expectancy":     f"${ev:+.4f} per trade",
    }


def print_summary(metrics: dict):
    """Pretty print metrics to console."""
    print("\n" + "=" * 50)
    print("  PERFORMANCE METRICS")
    print("=" * 50)
    print(f"  Trades:         {metrics['total_trades']} ({metrics['wins']}W / {metrics['losses']}L)")
    print(f"  Win Rate:       {metrics['win_rate_pct']}")
    print(f"  Total PnL:      ${metrics['total_pnl']:+.2f}")
    print(f"  Avg Win:        ${metrics['avg_win']:.2f}")
    print(f"  Avg Loss:       ${metrics['avg_loss']:.2f}")
    print(f"  Profit Factor:  {metrics['profit_factor']:.2f}")
    print(f"  Sharpe Ratio:   {metrics['sharpe']:.4f}")
    print(f"  Max Drawdown:   ${metrics['max_drawdown']:.2f}")
    print(f"  EV per Trade:   {metrics['expectancy']}")
    print("=" * 50 + "\n")


# ------------------------------------------------------------------ #
#  Private                                                             #
# ------------------------------------------------------------------ #

def _sharpe(pnls: List[float], risk_free: float = 0.0) -> float:
    """Annualised Sharpe ratio (assumes ~96 trades per day at 15M intervals)."""
    if len(pnls) < 2:
        return 0.0
    arr = np.array(pnls)
    mean = np.mean(arr) - risk_free
    std = np.std(arr, ddof=1)
    if std == 0:
        return 0.0
    # Annualise: 96 15-min periods per day × 365 days
    return float(mean / std * np.sqrt(96 * 365))


def _max_drawdown(pnls: List[float]) -> float:
    """Maximum peak-to-trough drawdown in dollars."""
    if not pnls:
        return 0.0
    cumulative = np.cumsum(pnls)
    peak = np.maximum.accumulate(cumulative)
    drawdown = peak - cumulative
    return float(np.max(drawdown))


def _empty() -> dict:
    return {
        "total_trades": 0, "wins": 0, "losses": 0,
        "win_rate": 0, "win_rate_pct": "0.00%",
        "total_pnl": 0, "avg_win": 0, "avg_loss": 0,
        "profit_factor": 0, "sharpe": 0,
        "max_drawdown": 0, "max_drawdown_pct": "0.00",
        "ev_per_trade": 0, "expectancy": "$0.0000 per trade",
    }
