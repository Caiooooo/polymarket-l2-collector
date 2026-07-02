"""
Polymarket Gamma API — market discovery and asset ID resolution.

Separated from WS logic so it can be tested and reused independently.
"""

from __future__ import annotations

import json
import time
from datetime import datetime
from typing import Any

import aiohttp
import pytz

from .config import load_settings
from .get_asset_id import get_asset_id_async
from .logger_config import get_logger

logger = get_logger("market_discovery")

# Month name lookup (lowercase) for 1h URL generation
_MONTH_NAMES = {
    1: "january", 2: "february", 3: "march", 4: "april",
    5: "may", 6: "june", 7: "july", 8: "august",
    9: "september", 10: "october", 11: "november", 12: "december",
}
_COIN_NAMES = {"btc": "bitcoin", "eth": "ethereum", "sol": "solana", "xrp": "xrp"}


def _build_event_slug(coin: str, interval: str, window_ts: int) -> str:
    """Build the Polymarket event slug for a given coin/interval/window.

    Examples:
        >>> _build_event_slug("btc", "5m", 1765359900)
        'btc-updown-5m-1765359900'
        >>> _build_event_slug("eth", "15m", 1765359900)
        'eth-updown-15m-1765359900'
    """
    if interval in ("5m", "15m"):
        return f"{coin}-updown-{interval}-{window_ts}"
    else:
        # 1h slugs use ET-based naming
        utc_time = datetime.fromtimestamp(window_ts, tz=pytz.UTC)
        et_tz = pytz.timezone("US/Eastern")
        et_time = utc_time.astimezone(et_tz)

        month = _MONTH_NAMES[et_time.month]
        day = et_time.day
        hour_12 = et_time.hour % 12 or 12
        period = "pm" if et_time.hour >= 12 else "am"
        coin_name = _COIN_NAMES.get(coin, coin)
        return f"{coin_name}-up-or-down-{month}-{day}-{hour_12}{period}-et"


def _build_event_url(coin: str, interval: str, window_ts: int) -> str:
    """Build the full polymarket.com/event/ URL."""
    slug = _build_event_slug(coin, interval, window_ts)
    return f"https://polymarket.com/event/{slug}"


def parse_assets_from_markets(markets: list[dict[str, Any]]) -> dict[str, str]:
    """Parse market list from Gamma API into {direction: asset_id} map.

    Returns a dict like ``{"up": "123...", "down": "456..."}``.
    Returns an empty dict if parsing fails.
    """
    if not markets:
        return {}

    market = markets[0]
    clob_token_ids = market.get("clobTokenIds", [])
    outcomes = market.get("outcomes", [])

    # Defensive parsing — Gamma API can return JSON-as-string
    if isinstance(clob_token_ids, str):
        try:
            clob_token_ids = json.loads(clob_token_ids)
        except (json.JSONDecodeError, TypeError):
            clob_token_ids = []
    if isinstance(outcomes, str):
        try:
            outcomes = json.loads(outcomes)
        except (json.JSONDecodeError, TypeError):
            outcomes = []

    result: dict[str, str] = {}
    for outcome, token_id in zip(outcomes, clob_token_ids):
        if outcome in ("Up", "Down"):
            result[outcome.lower()] = token_id

    logger.info(
        "Market: %s | Up: %s | Down: %s",
        market.get("question", "N/A"),
        result.get("up", "-")[:12],
        result.get("down", "-")[:12],
    )
    return result


async def resolve_assets(
    coin: str,
    interval: str,
    target_timestamp: int | None = None,
    session: aiohttp.ClientSession | None = None,
) -> dict[str, str]:
    """Resolve asset IDs for a coin/interval/window via Gamma API.

    Args:
        coin: Coin symbol (e.g. "btc", "eth").
        interval: Window interval ("5m", "15m", "1h").
        target_timestamp: Window start Unix timestamp.  Defaults to now.
        session: Optional shared aiohttp session (recommended for reuse).

    Returns:
        Dict like ``{"up": "asset_id", "down": "asset_id"}``, or empty dict
        if the market could not be resolved.
    """
    settings = load_settings()
    ts = int(target_timestamp) if target_timestamp is not None else int(time.time())
    gap_seconds = settings.interval_seconds(interval)
    window_ts = (ts // gap_seconds) * gap_seconds

    url = _build_event_url(coin, interval, window_ts)
    markets = await get_asset_id_async(url, session=session)

    if not markets:
        logger.warning("No market data for %s %s @ %s", coin, interval, window_ts)
        return {}

    return parse_assets_from_markets(markets)
