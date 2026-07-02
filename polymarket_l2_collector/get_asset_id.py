"""
Gamma API HTTP client — async (aiohttp) and sync (requests) variants.

CLI usage::

    python -m polymarket_l2_collector.get_asset_id btc-updown-5m-1765359900
"""

from __future__ import annotations

import json
import sys
from typing import Any

import aiohttp
import requests

from .logger_config import get_logger

logger = get_logger("get_asset_id")


# ── Async (used by WebSocket collector) ────────────────────────────


async def get_market_info_by_slug_async(
    slug: str,
    session: aiohttp.ClientSession | None = None,
) -> list[dict[str, Any]] | None:
    """Fetch market info from Gamma API by slug.

    Args:
        slug: Event slug (e.g. "btc-updown-5m-1765359900") or full URL.
        session: Optional shared aiohttp session.

    Returns:
        List of market dicts, or ``None`` on failure.
    """
    if slug.startswith("http"):
        slug = slug.split("/")[-1]

    url = f"https://gamma-api.polymarket.com/events?slug={slug}"

    close_session = session is None
    if close_session:
        session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))

    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                return None
            data: list[dict] = await resp.json()
            if not data:
                return None

            markets = data[0].get("markets", [])
            for m in markets:
                for field in ("outcomes", "outcomePrices", "clobTokenIds"):
                    val = m.get(field)
                    if isinstance(val, str):
                        try:
                            m[field] = json.loads(val)
                        except (json.JSONDecodeError, TypeError):
                            pass
            return markets
    except Exception as exc:
        logger.debug("Gamma API error for slug %s: %s", slug, exc)
        return None
    finally:
        if close_session:
            await session.close()


async def get_asset_id_async(
    url_or_slug: str,
    session: aiohttp.ClientSession | None = None,
) -> list[dict[str, Any]] | None:
    """Convenience alias for async market lookup."""
    return await get_market_info_by_slug_async(url_or_slug, session=session)


# ── Sync (CLI use) ─────────────────────────────────────────────────


def get_market_info_by_slug(slug: str) -> list[dict[str, Any]] | None:
    """Fetch market info synchronously (CLI only)."""
    if slug.startswith("http"):
        slug = slug.split("/")[-1]

    url = f"https://gamma-api.polymarket.com/events?slug={slug}"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        if not data:
            print(f"Not found: {slug}")
            return None
        markets = data[0].get("markets", [])
        for m in markets:
            for field in ("outcomes", "outcomePrices", "clobTokenIds"):
                val = m.get(field)
                if isinstance(val, str):
                    try:
                        m[field] = json.loads(val)
                    except (json.JSONDecodeError, TypeError):
                        pass
        return markets
    except requests.exceptions.RequestException as exc:
        print(f"API request failed: {exc}")
        return None


def search_markets(keyword: str) -> None:
    """Search events by keyword."""
    url = "https://gamma-api.polymarket.com/events?limit=10"
    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        events = resp.json()
    except requests.exceptions.RequestException as exc:
        print(f"API request failed: {exc}")
        return

    matching = [e for e in events if keyword.lower() in e.get("title", "").lower()]
    if not matching:
        print(f"No events matching '{keyword}'")
        return

    print(f"\nFound {len(matching)} events:\n")
    for idx, event in enumerate(matching, 1):
        print(f"  [{idx}] {event.get('title', 'N/A')}")
        print(f"       Slug: {event.get('slug', 'N/A')}")
        print(f"       URL: https://polymarket.com/event/{event.get('slug', 'N/A')}")
        markets = event.get("markets", [])
        if markets:
            for m in markets[:2]:
                tids = m.get("clobTokenIds", [])
                if tids:
                    print(f"       Token ID: {tids[0]}")
        print()


# ── CLI entry point ────────────────────────────────────────────────


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python -m polymarket_l2_collector.get_asset_id <slug|url>")
        print("  python -m polymarket_l2_collector.get_asset_id --search <keyword>")
        sys.exit(1)

    if sys.argv[1] == "--search":
        if len(sys.argv) < 3:
            print("Please provide a search keyword")
            sys.exit(1)
        search_markets(sys.argv[2])
    else:
        get_market_info_by_slug(sys.argv[1])


if __name__ == "__main__":
    main()
