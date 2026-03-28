"""
news.py — Pre-window news sentiment context.

Fetches recent crypto and macro headlines from CryptoPanic and NewsAPI,
scores sentiment, and writes a cached JSON report that strategy/base.py
reads before each trade decision.

Flow:
  1. PaperTrader._heartbeat() calls NewsContext.fetch() every ~15 min.
  2. fetch() hits both APIs, scores headlines, writes data/news_context.json.
  3. Strategy.decide() calls NewsContext.load() (no network — reads cached file).
  4. High-confidence news that conflicts with the technical direction blocks the trade.
"""

import json
import logging
import requests
from datetime import datetime, timezone, timedelta
from pathlib import Path

from administration.config import (
    CRYPTOPANIC_API_KEY, NEWSAPI_KEY,
    NEWS_ENABLED, NEWS_MAX_AGE_SECS,
    NEWS_HIGH_CONFIDENCE, NEWS_MED_CONFIDENCE,
)

logger = logging.getLogger("news")

_CONTEXT_PATH = Path(__file__).parent.parent / "data" / "news_context.json"

# Keywords that push sentiment bullish (+1 each)
_BULLISH_KEYWORDS = [
    "rally", "surge", "soar", "jump", "rise", "bullish", "adoption",
    "etf", "approval", "approved", "partnership", "upgrade", "breakout",
    "all-time high", "ath", "record high", "milestone", "recovery",
    "gains", "positive", "buy", "accumulate", "launch", "integration",
    "institutional", "inflow", "listing",
]

# Keywords that push sentiment bearish (-1 each)
_BEARISH_KEYWORDS = [
    "ban", "banned", "hack", "hacked", "exploit", "crash", "dump",
    "crackdown", "sanction", "sanctions", "lawsuit", "investigation",
    "fraud", "collapse", "bearish", "plunge", "drop", "fear", "panic",
    "sell-off", "selloff", "breach", "vulnerability", "scam", "ponzi",
    "bankrupt", "insolvency", "liquidation", "outflow", "delist",
]

# High-impact names that multiply keyword score by 1.5 when present
_AMPLIFIERS = [
    "trump", "fed ", "federal reserve", "sec ", "cftc", "congress",
    "senate", "white house", "treasury", "blackrock", "fidelity",
]


class NewsContext:

    @staticmethod
    def fetch(assets: list) -> dict:
        """
        Fetch news from CryptoPanic + NewsAPI, score sentiment, write cache.
        Safe to call from a background thread — only writes one JSON file.
        Returns the report dict.
        """
        if not NEWS_ENABLED:
            return {}

        score = 0
        headlines = []
        sources_used = []
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=NEWS_MAX_AGE_SECS)

        # ------------------------------------------------------------------ #
        #  CryptoPanic — crypto-specific news + community vote scores         #
        # ------------------------------------------------------------------ #
        if CRYPTOPANIC_API_KEY:
            try:
                currencies = ",".join(assets)
                url = (
                    f"https://cryptopanic.com/api/v1/posts/"
                    f"?auth_token={CRYPTOPANIC_API_KEY}"
                    f"&currencies={currencies}&public=true"
                )
                resp = requests.get(url, timeout=10)
                if resp.status_code == 200:
                    for post in resp.json().get("results", []):
                        pub_str = post.get("published_at", "")
                        if not pub_str:
                            continue
                        pub = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
                        if pub < cutoff:
                            continue
                        # Community votes give direct signal strength
                        votes = post.get("votes", {})
                        post_score = votes.get("positive", 0) - votes.get("negative", 0)
                        # Keyword score on top of vote score
                        title = post.get("title", "").lower()
                        post_score += _keyword_score(title)
                        score += post_score
                        if post.get("title"):
                            headlines.append(post["title"])
                    sources_used.append("cryptopanic")
                else:
                    logger.warning(f"CryptoPanic HTTP {resp.status_code}")
            except Exception as e:
                logger.warning(f"CryptoPanic fetch failed: {e}")

        # ------------------------------------------------------------------ #
        #  NewsAPI — broad macro + political news                             #
        # ------------------------------------------------------------------ #
        if NEWSAPI_KEY:
            try:
                query = "bitcoin OR ethereum OR crypto OR solana OR ripple OR XRP"
                url = (
                    f"https://newsapi.org/v2/everything"
                    f"?q={query}&sortBy=publishedAt&pageSize=20"
                    f"&language=en&apiKey={NEWSAPI_KEY}"
                )
                resp = requests.get(url, timeout=10)
                if resp.status_code == 200:
                    for article in resp.json().get("articles", []):
                        pub_str = article.get("publishedAt", "")
                        if not pub_str:
                            continue
                        pub = datetime.fromisoformat(pub_str.replace("Z", "+00:00"))
                        if pub < cutoff:
                            continue
                        title = (article.get("title") or "").lower()
                        desc  = (article.get("description") or "").lower()
                        text  = title + " " + desc
                        kw = _keyword_score(text)
                        # Amplify when a high-impact name is present
                        if any(amp in text for amp in _AMPLIFIERS):
                            kw = int(kw * 1.5) if kw != 0 else kw
                        score += kw
                        if article.get("title"):
                            headlines.append(article["title"])
                    sources_used.append("newsapi")
                else:
                    logger.warning(f"NewsAPI HTTP {resp.status_code}")
            except Exception as e:
                logger.warning(f"NewsAPI fetch failed: {e}")

        # ------------------------------------------------------------------ #
        #  Score → bias + confidence                                          #
        # ------------------------------------------------------------------ #
        if score >= NEWS_HIGH_CONFIDENCE:
            bias, confidence = "bullish", "high"
        elif score >= NEWS_MED_CONFIDENCE:
            bias, confidence = "bullish", "medium"
        elif score <= -NEWS_HIGH_CONFIDENCE:
            bias, confidence = "bearish", "high"
        elif score <= -NEWS_MED_CONFIDENCE:
            bias, confidence = "bearish", "medium"
        else:
            bias, confidence = "neutral", "low"

        reason = _build_reason(score, headlines)

        report = {
            "timestamp":  datetime.now(timezone.utc).isoformat(),
            "bias":       bias,
            "confidence": confidence,
            "score":      score,
            "reason":     reason,
            "headlines":  headlines[:5],
            "sources":    sources_used,
        }

        try:
            _CONTEXT_PATH.parent.mkdir(parents=True, exist_ok=True)
            _CONTEXT_PATH.write_text(json.dumps(report, indent=2))
            logger.info(
                f"News context: bias={bias} confidence={confidence} "
                f"score={score:+d} sources={sources_used} headlines={len(headlines)}"
            )
        except Exception as e:
            logger.warning(f"Failed to write news_context.json: {e}")

        return report

    @staticmethod
    def load() -> dict | None:
        """
        Load cached news report from disk. Returns None if missing or stale.
        No network I/O — safe to call inside the strategy decision loop.
        """
        if not NEWS_ENABLED or not _CONTEXT_PATH.exists():
            return None
        try:
            report = json.loads(_CONTEXT_PATH.read_text())
            ts  = datetime.fromisoformat(report["timestamp"])
            age = (datetime.now(timezone.utc) - ts).total_seconds()
            if age > NEWS_MAX_AGE_SECS:
                logger.debug(f"News context stale ({int(age)}s old) — ignoring")
                return None
            report["age_secs"] = int(age)
            return report
        except Exception as e:
            logger.warning(f"Failed to read news_context.json: {e}")
            return None


# ------------------------------------------------------------------ #
#  Helpers                                                             #
# ------------------------------------------------------------------ #

def _keyword_score(text: str) -> int:
    """Count bullish keywords (+1 each) minus bearish keywords (-1 each)."""
    score = 0
    for kw in _BULLISH_KEYWORDS:
        if kw in text:
            score += 1
    for kw in _BEARISH_KEYWORDS:
        if kw in text:
            score -= 1
    return score


def _build_reason(score: int, headlines: list) -> str:
    direction = "bullish" if score > 0 else "bearish" if score < 0 else "neutral"
    if not headlines:
        return f"No recent headlines within window (score={score:+d})"
    top = headlines[0][:100]
    return f"score={score:+d} ({direction}) — \"{top}\""
