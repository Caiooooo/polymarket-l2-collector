"""
Format raw WebSocket messages into the canonical Parquet row format.

Separated from WS logic for testability.
"""

from __future__ import annotations

import time
from typing import Any

from .binance_price import current_prices
from .logger_config import get_logger

logger = get_logger("formatter")


def _get_asset_price(coin: str) -> float:
    """Look up Binance midprice for *coin*."""
    symbol = f"{coin.upper()}USDT"
    info = current_prices.get(symbol)
    return info.get("mid", 0.0) if info else 0.0


def format_orderbook(
    raw_messages: list[dict[str, Any]],
    asset_to_coin: dict[str, str],
    window_open_ts: int | None = None,
) -> list[dict[str, Any]]:
    """Format a batch of raw orderbook WS messages into rows.

    Skips messages whose *asset_id* is not in *asset_to_coin*.
    """
    now_ms = str(int(time.time() * 1000))
    rows: list[dict[str, Any]] = []

    for item in raw_messages:
        asset_id = item.get("asset_id", "")
        coin_tag = asset_to_coin.get(asset_id)
        if not coin_tag or "_" not in coin_tag:
            continue

        coin_name = coin_tag.split("_")[0]
        rows.append(
            {
                "bids": item.get("bids", []),
                "asks": item.get("asks", []),
                "local_timestamp": now_ms,
                "timestamp": item.get("timestamp", now_ms),
                "asset_price": _get_asset_price(coin_name),
                "window_open_ts": window_open_ts,
            }
        )

    return rows


def format_trade(
    raw_messages: list[dict[str, Any]],
    asset_to_coin: dict[str, str],
    window_open_ts: int | None = None,
) -> list[dict[str, Any]]:
    """Format a batch of raw trade WS messages into rows."""
    now_ms = str(int(time.time() * 1000))
    rows: list[dict[str, Any]] = []

    for item in raw_messages:
        asset_id = item.get("asset_id", "")
        coin_tag = asset_to_coin.get(asset_id)
        if not coin_tag or "_" not in coin_tag:
            continue

        coin_name = coin_tag.split("_")[0]
        rows.append(
            {
                "price": item.get("price", "0"),
                "size": item.get("size", "0"),
                "side": item.get("side", "").lower(),
                "local_timestamp": now_ms,
                "timestamp": item.get("timestamp", now_ms),
                "asset_price": _get_asset_price(coin_name),
                "window_open_ts": window_open_ts,
            }
        )

    return rows
