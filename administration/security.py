import os
import time
import threading
from collections import deque
from administration.logger import log_halt, log_error

# Kill switch state — shared across threads
_killed = threading.Event()

# Rate limit tracker: max requests per window
_rate_limits = {
    "binance": {"max": 1200, "window": 60, "calls": deque()},
    "kalshi":  {"max": 60,   "window": 60, "calls": deque()},
}
_rate_lock = threading.Lock()


# ------------------------------------------------------------------ #
#  Kill Switch                                                         #
# ------------------------------------------------------------------ #

def kill():
    """Immediately halt all bot activity."""
    _killed.set()
    log_halt("KILL SWITCH ACTIVATED — all trading stopped.")


def revive():
    """Re-enable bot activity after a kill."""
    _killed.clear()


def is_killed():
    """Returns True if the kill switch is active."""
    return _killed.is_set()


def require_alive():
    """Raise an exception if the kill switch is active."""
    if is_killed():
        raise RuntimeError("Kill switch is active. Bot is halted.")


# ------------------------------------------------------------------ #
#  Rate Limiter                                                        #
# ------------------------------------------------------------------ #

def check_rate_limit(service: str):
    """
    Returns True if the service call is allowed.
    Returns False if the rate limit would be exceeded.
    Automatically cleans up expired timestamps.
    """
    if service not in _rate_limits:
        return True

    cfg = _rate_limits[service]
    now = time.monotonic()
    window = cfg["window"]
    max_calls = cfg["max"]

    with _rate_lock:
        calls = cfg["calls"]
        # Remove calls outside the current window
        while calls and now - calls[0] > window:
            calls.popleft()

        if len(calls) >= max_calls:
            log_error(f"Rate limit hit for {service}: {len(calls)}/{max_calls} calls in {window}s")
            return False

        calls.append(now)
        return True


def rate_limited_call(service: str, fn, *args, **kwargs):
    """
    Execute fn only if rate limit allows.
    Raises RuntimeError if rate limit is exceeded.
    """
    require_alive()
    if not check_rate_limit(service):
        raise RuntimeError(f"Rate limit exceeded for {service}. Skipping call.")
    return fn(*args, **kwargs)


# ------------------------------------------------------------------ #
#  Credential Validation                                               #
# ------------------------------------------------------------------ #

def validate_env():
    """
    Check that all required config values are set.
    Raises ValueError listing any missing keys.
    """
    from administration import config as cfg
    checks = {
        "KALSHI_API_KEY":    cfg.KALSHI_API_KEY,
        "KALSHI_KEY_PATH":   cfg.KALSHI_KEY_PATH,
        "DISCORD_WEBHOOK_URL": cfg.DISCORD_WEBHOOK_URL,
    }
    missing = [key for key, val in checks.items() if not val]
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
