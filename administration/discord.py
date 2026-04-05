import threading
import requests
from datetime import datetime
from zoneinfo import ZoneInfo
from administration.config import DISCORD_WEBHOOK_URL
from administration.logger import get as get_logger

logger = get_logger("discord")

BLUE  = 0x3498DB
GREEN = 0x2ECC71
RED   = 0xE74C3C

ET = ZoneInfo("America/New_York")


class Discord:
    def __init__(self, paper: bool = True):
        self.url = DISCORD_WEBHOOK_URL
        self.ready = bool(self.url)
        self.mode = "Paper" if paper else "Live"

    def start(self):
        if not self.ready:
            logger.warning("Discord webhook URL not set — alerts disabled.")
            return
        logger.info("Discord webhook ready.")

    def stop(self):
        pass

    def is_ready(self) -> bool:
        return self.ready

    # ------------------------------------------------------------------ #
    #  Core Send                                                           #
    # ------------------------------------------------------------------ #

    def _send(self, title: str, color: int, description: str):
        if not self.ready:
            return
        embed = {
            "title":       title,
            "description": description,
            "color":       color,
            "footer":      {"text": self._footer()},
        }
        def _post():
            try:
                r = requests.post(self.url, json={"embeds": [embed]}, timeout=5)
                if r.status_code == 429:
                    retry_after = r.json().get("retry_after", "?")
                    logger.warning(f"Discord rate limited — retry_after={retry_after}s")
                elif r.status_code not in (200, 204):
                    logger.warning(f"Discord send failed: HTTP {r.status_code} — {r.text[:200]}")
            except Exception as e:
                logger.warning(f"Discord send failed: {e}")
        threading.Thread(target=_post, daemon=True).start()

    # ------------------------------------------------------------------ #
    #  Notifications                                                       #
    # ------------------------------------------------------------------ #

    def bot_started(self, portfolio_total: float):
        self._send(
            title="🚀 Bot Started",
            color=BLUE,
            description="\n".join([
                f"**Mode:** {self.mode}",
                f"**Portfolio:** ${portfolio_total:.2f}",
            ])
        )

    def bot_stopped(self, portfolio_total: float):
        self._send(
            title="🛑 Bot Stopped",
            color=RED,
            description="\n".join([
                f"**Mode:** {self.mode}",
                f"**Portfolio:** ${portfolio_total:.2f}",
            ])
        )

    def buy(self, direction: str, contracts: int, contracts_filled: int, price_pct: float,
            cost: float, payout: float, portfolio_total: float,
            market_label: str = "BTC UP", bet_size: float = 0.0,
            session_wins: int = 0, session_losses: int = 0, session_pnl: float = 0.0):
        pnl_sign = "+" if session_pnl >= 0 else "-"
        fill_str = (f"{contracts_filled}/{contracts}"
                    if contracts_filled != contracts
                    else f"{contracts_filled}")
        display_cost = bet_size if bet_size > 0 else cost
        self._send(
            title=f"🚀 BUY: {market_label}",
            color=BLUE,
            description="\n".join([
                f"**Contracts:** {fill_str} @ {price_pct:.0f}¢",
                f"**Cost:** ${display_cost:.2f}",
                f"**Payout:** ${payout:.2f}",
                f"**Portfolio:** ${portfolio_total:.2f}",
                "",
                f"**Record:** {session_wins}W - {session_losses}L",
                f"**Total P&L:** {pnl_sign}${abs(session_pnl):.2f}",
            ])
        )

    def sell_win(self, direction: str, contracts: int, contracts_filled: int,
                 price_pct: float, pnl: float, portfolio_total: float,
                 market_label: str = "BTC UP",
                 session_wins: int = 0, session_losses: int = 0, session_pnl: float = 0.0):
        pnl_sign = "+" if session_pnl >= 0 else "-"
        fill_str = (f"{contracts_filled}/{contracts}"
                    if contracts_filled != contracts
                    else f"{contracts_filled}")
        self._send(
            title=f"✅ SELL: {market_label}",
            color=GREEN,
            description="\n".join([
                f"**Contracts:** {fill_str} @ {price_pct:.0f}¢",
                f"**Result:** Win",
                f"**P&L:** +${pnl:.2f}",
                f"**Portfolio:** ${portfolio_total:.2f}",
                "",
                f"**Record:** {session_wins}W - {session_losses}L",
                f"**Total P&L:** {pnl_sign}${abs(session_pnl):.2f}",
            ])
        )

    def sell_loss(self, direction: str, contracts: int, contracts_filled: int,
                  price_pct: float, pnl: float, portfolio_total: float,
                  market_label: str = "BTC UP",
                  session_wins: int = 0, session_losses: int = 0, session_pnl: float = 0.0):
        pnl_sign = "+" if session_pnl >= 0 else "-"
        fill_str = (f"{contracts_filled}/{contracts}"
                    if contracts_filled != contracts
                    else f"{contracts_filled}")
        self._send(
            title=f"❌ SELL: {market_label}",
            color=RED,
            description="\n".join([
                f"**Contracts:** {fill_str} @ {price_pct:.0f}¢",
                f"**Result:** Loss",
                f"**P&L:** -${abs(pnl):.2f}",
                f"**Portfolio:** ${portfolio_total:.2f}",
                "",
                f"**Record:** {session_wins}W - {session_losses}L",
                f"**Total P&L:** {pnl_sign}${abs(session_pnl):.2f}",
            ])
        )

    # ------------------------------------------------------------------ #
    #  Footer                                                              #
    # ------------------------------------------------------------------ #

    def _footer(self) -> str:
        now = datetime.now(ET)
        return f"{self.mode}  ·  {_format_date(now)}  ·  {_format_time(now)} ET"


# ------------------------------------------------------------------ #
#  Helpers                                                             #
# ------------------------------------------------------------------ #

def _ordinal(day: int) -> str:
    if 11 <= day <= 13:
        return f"{day}th"
    return f"{day}{['th','st','nd','rd','th','th','th','th','th','th'][day % 10]}"


def _format_date(dt: datetime) -> str:
    return dt.strftime(f"%A, %B {_ordinal(dt.day)}")


def _format_time(dt: datetime) -> str:
    return dt.strftime("%I:%M %p").lstrip("0")
