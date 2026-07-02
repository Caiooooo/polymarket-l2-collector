"""
REST snapshot backfill module for polymarket-l2-collector.

Fills data gaps by fetching historical orderbook snapshots from
Polymarket's CLOB REST API.

CLI::

    polymarket-backfill [--data-dir data] [--no-dry-run]
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import time
from typing import Any

import aiohttp

from .config import load_settings
from .file_cache import _build_file_path, flush_all_caches, save_book
from .logger_config import get_logger
from .market_discovery import resolve_assets
from .window_metadata import scan_data_quality, touch_message

logger = get_logger("rest_snapshot")

_REST_URL = "https://clob.polymarket.com/orderbook"

# Gap-window pattern from scan_data_quality
# Example:
#   interval=5m coin=btc type=orderbooks direction=up: gap between 1000 and 1600
#   (delta=600s, expected≈300s, tolerance=60s, ~1 window(s) missing)
_GAP_PATTERN = re.compile(
    r"interval=(?P<interval>\S+) "
    r"coin=(?P<coin>\S+) "
    r"type=(?P<data_type>\S+) "
    r"direction=(?P<direction>\w+): "
    r"gap between (?P<start_ts>\d+) and (?P<end_ts>\d+) "
    r"\(delta=\d+s, expected[=≈](?P<expected>\d+)s,"
)


def _parse_gap_message(gap_msg: str) -> dict[str, Any] | None:
    """Parse a single gap-window string from ``scan_data_quality``.

    Returns a dict with keys *interval*, *coin*, *data_type*, *direction*,
    *start_ts*, *end_ts*, *expected_seconds*, or ``None`` if the message
    doesn't match the expected format.
    """
    m = _GAP_PATTERN.search(gap_msg)
    if not m:
        return None
    return {
        "interval": m.group("interval"),
        "coin": m.group("coin"),
        "data_type": m.group("data_type"),
        "direction": m.group("direction"),
        "start_ts": int(m.group("start_ts")),
        "end_ts": int(m.group("end_ts")),
        "expected_seconds": int(m.group("expected")),
    }


def _missing_timestamps(start_ts: int, end_ts: int, expected: int) -> list[int]:
    """Compute the list of missing window start timestamps within a gap.

    Given two existing window timestamps and the expected interval in seconds,
    returns the timestamps of every window that should exist between them
    but is missing.
    """
    missing: list[int] = []
    ts = start_ts + expected
    while ts < end_ts:
        missing.append(ts)
        ts += expected
    return missing


async def fetch_snapshot(
    asset_id: str,
    session: aiohttp.ClientSession | None = None,
) -> dict | None:
    """Fetch the current orderbook snapshot for *asset_id*.

    Makes a ``GET`` request to
    ``https://clob.polymarket.com/orderbook?asset_id={asset_id}``.

    Args:
        asset_id: The Polymarket token ID to fetch.
        session: Optional shared aiohttp session (recommended for reuse).

    Returns:
        A dict with keys ``bids``, ``asks``, ``timestamp`` (int ms),
        or ``None`` on any failure.
    """
    url = f"{_REST_URL}?asset_id={asset_id}"
    close_session = session is None
    if close_session:
        session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))

    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                logger.warning(
                    "REST API returned status %d for asset_id=%s",
                    resp.status,
                    asset_id,
                )
                return None
            data = await resp.json()
            return {
                "bids": data.get("bids", []),
                "asks": data.get("asks", []),
                "timestamp": int(time.time() * 1000),
            }
    except Exception as exc:
        logger.warning(
            "REST API request failed for asset_id=%s: %s",
            asset_id,
            exc,
        )
        return None
    finally:
        if close_session:
            await session.close()


async def _backfill_single_window(
    data_dir: str,
    interval: str,
    coin: str,
    direction: str,
    window_ts: int,
    session: aiohttp.ClientSession,
) -> None:
    """Backfill a single window by resolving its asset ID and
    fetching/saving a REST snapshot.
    """
    assets = await resolve_assets(coin, interval, target_timestamp=window_ts, session=session)
    if not assets or direction not in assets:
        raise ValueError(
            f"No asset ID found for {coin}/{interval}/{direction} @ {window_ts}"
        )

    asset_id = assets[direction]
    snapshot = await fetch_snapshot(asset_id, session=session)
    if snapshot is None:
        raise RuntimeError(f"Failed to fetch snapshot for asset_id={asset_id}")

    now_ms = str(int(time.time() * 1000))
    row = {
        "bids": snapshot["bids"],
        "asks": snapshot["asks"],
        "local_timestamp": now_ms,
        "timestamp": str(snapshot["timestamp"]),
        "asset_price": 0.0,
        "window_open_ts": window_ts,
    }

    fp = _build_file_path(data_dir, interval, coin, "orderbooks", window_ts, direction)
    touch_message(interval, coin, "orderbooks", direction, window_ts, int(time.time() * 1000))
    save_book([row], fp)


async def backfill_gaps(
    data_dir: str,
    interval: str,
    coin: str,
    direction: str = "up",
    dry_run: bool = True,
) -> list[dict]:
    """Scan for data gaps in a specific (interval, coin, direction) and
    optionally backfill them from the REST API.

    Args:
        data_dir: Root data directory.
        interval: Window interval (``"5m"``, ``"15m"``, ``"1h"``).
        coin: Coin symbol (``"btc"``, ``"eth"``, etc.).
        direction: Market direction (``"up"`` or ``"down"``).
        dry_run: If ``True`` (default), only report without fetching.

    Returns:
        A list of result dicts, one per considered window, each with keys
        ``interval``, ``coin``, ``direction``, ``window_ts``, ``status``.
    """
    report = scan_data_quality(data_dir)
    results: list[dict] = []

    gap_list = report.get("gap_windows", [])
    matched_gaps: list[dict[str, Any]] = []
    for gap_msg in gap_list:
        parsed = _parse_gap_message(gap_msg)
        if parsed is None:
            continue
        if (
            parsed["interval"] == interval
            and parsed["coin"] == coin
            and parsed["direction"] == direction
            and parsed["data_type"] == "orderbooks"
        ):
            matched_gaps.append(parsed)

    if not matched_gaps:
        logger.info(
            "No gaps found",
            extra={"interval": interval, "coin": coin, "direction": direction},
        )
        return results

    now = int(time.time())
    interval_seconds = load_settings().interval_seconds(interval)
    # Only backfill windows whose end + 2x interval buffer is in the past
    completion_buffer = 2 * interval_seconds

    for gap in matched_gaps:
        missing = _missing_timestamps(
            gap["start_ts"], gap["end_ts"], gap["expected_seconds"]
        )

        for window_ts in missing:
            window_end = window_ts + interval_seconds
            if now <= window_end + completion_buffer:
                result = {
                    "interval": interval,
                    "coin": coin,
                    "direction": direction,
                    "window_ts": window_ts,
                    "status": "skipped_too_recent",
                }
                results.append(result)
                logger.info(
                    "Skipping window (too recent)",
                    extra={
                        "coin": coin,
                        "interval": interval,
                        "direction": direction,
                        "window": window_ts,
                    },
                )
                continue

            fp = _build_file_path(data_dir, interval, coin, "orderbooks", window_ts, direction)
            if os.path.exists(fp):
                result = {
                    "interval": interval,
                    "coin": coin,
                    "direction": direction,
                    "window_ts": window_ts,
                    "status": "skipped_already_exists",
                }
                results.append(result)
                logger.info(
                    "Skipping window (already has data)",
                    extra={
                        "coin": coin,
                        "interval": interval,
                        "direction": direction,
                        "window": window_ts,
                    },
                )
                continue

            if dry_run:
                result = {
                    "interval": interval,
                    "coin": coin,
                    "direction": direction,
                    "window_ts": window_ts,
                    "status": "dry_run_would_fetch",
                }
                results.append(result)
                logger.info(
                    "Would fetch snapshot for window",
                    extra={
                        "coin": coin,
                        "interval": interval,
                        "direction": direction,
                        "window": window_ts,
                    },
                )
                continue

            try:
                async with aiohttp.ClientSession(
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as session:
                    await _backfill_single_window(
                        data_dir, interval, coin, direction, window_ts, session
                    )
                result = {
                    "interval": interval,
                    "coin": coin,
                    "direction": direction,
                    "window_ts": window_ts,
                    "status": "backfilled",
                }
                results.append(result)
                logger.info(
                    "Backfilled window",
                    extra={
                        "coin": coin,
                        "interval": interval,
                        "direction": direction,
                        "window": window_ts,
                    },
                )
            except Exception as exc:
                result = {
                    "interval": interval,
                    "coin": coin,
                    "direction": direction,
                    "window_ts": window_ts,
                    "status": f"error: {exc}",
                }
                results.append(result)
                logger.error(
                    "Failed to backfill window",
                    extra={
                        "coin": coin,
                        "interval": interval,
                        "direction": direction,
                        "window": window_ts,
                        "error": str(exc),
                    },
                )

    return results


async def backfill_all_gaps(
    data_dir: str = "data",
    dry_run: bool = True,
) -> dict[str, list[dict]]:
    """Iterate over all (interval, coin, direction) pairs from config and
    call ``backfill_gaps`` for each.

    Args:
        data_dir: Root data directory.
        dry_run: If ``True`` (default), only report without fetching.

    Returns:
        A dict keyed by ``"{interval}/{coin}/{direction}"`` with lists of
        result dicts.
    """
    settings = load_settings()
    all_results: dict[str, list[dict]] = {}

    for interval in settings.intervals:
        for coin in settings.coins:
            for direction in settings.directions:
                key = f"{interval}/{coin}/{direction}"
                gaps = await backfill_gaps(
                    data_dir, interval, coin, direction, dry_run=dry_run
                )
                if gaps:
                    all_results[key] = gaps

    if not dry_run and all_results:
        flush_all_caches()

    return all_results


def main() -> None:
    """CLI entry point (``polymarket-backfill`` command)."""
    parser = argparse.ArgumentParser(
        description="Backfill Polymarket L2 data gaps from REST API"
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Root data directory (default: data)",
    )
    parser.add_argument(
        "--no-dry-run",
        action="store_true",
        help="Actually fetch and save snapshots (default is dry-run)",
    )
    args = parser.parse_args()

    dry_run = not args.no_dry_run
    asyncio.run(backfill_all_gaps(data_dir=args.data_dir, dry_run=dry_run))


if __name__ == "__main__":
    main()
