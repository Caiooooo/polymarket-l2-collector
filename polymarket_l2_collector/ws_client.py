"""
WebSocket connection lifecycle — connect, subscribe, receive, heartbeat.

This module is independent of the window-switching logic in
:mod:`collector`; it handles a single WS connection at a time.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from typing import Any

import websockets

from .config import load_settings
from .logger_config import get_logger

logger = get_logger("ws_client")


# ── Subscription helpers ───────────────────────────────────────────


def build_asset_id_list(assets_by_coin: dict[str, dict[str, Any]], directions: list[str]) -> list[str]:
    """Flatten asset IDs for the configured *directions* from the assets dict.

    *assets_by_coin* has the shape::

        {"btc": {"5m": {"up": "id1", "down": "id2"}, "15m": ...}, ...}
    """
    ids: list[str] = []
    for coin, intervals in assets_by_coin.items():
        for interval_data in intervals.values():
            if not isinstance(interval_data, dict):
                continue
            for d in directions:
                aid = interval_data.get(d)
                if aid:
                    ids.append(aid)
    return ids


async def subscribe(websocket: websockets.WebSocketClientProtocol, asset_ids: list[str]) -> None:
    """Send a ``market`` subscription message for *asset_ids*."""
    if not asset_ids:
        logger.warning("No asset IDs to subscribe — skipping subscription")
        return
    msg = {"type": "market", "assets_ids": asset_ids}
    logger.info("Subscribing to %d asset(s) …", len(asset_ids))
    await websocket.send(json.dumps(msg))


# ── Heartbeat ──────────────────────────────────────────────────────


async def send_ping_loop(websocket: websockets.WebSocketClientProtocol, interval: float = 10.0) -> None:
    """Send text ``PING`` every *interval* seconds to keep the WS alive."""
    try:
        while True:
            await asyncio.sleep(interval)
            await websocket.send("PING")
    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as exc:
        logger.error("PING loop error: %s", exc)


# ── Message receive loop ───────────────────────────────────────────


async def receive_loop(
    websocket: websockets.WebSocketClientProtocol,
    on_book: Callable[[dict[str, Any]], None],
    on_trade: Callable[[dict[str, Any]], None],
    should_save: Callable[[], bool],
    touch_activity: Callable[[], None] | None = None,
    recv_timeout: float = 1.0,
) -> None:
    """Receive WS messages and dispatch to *on_book* / *on_trade*.

    Loops until the connection is closed.  *should_save* is checked
    before every dispatch — if it returns ``False`` the message is
    received but dropped.
    """
    while True:
        try:
            raw = await asyncio.wait_for(websocket.recv(), timeout=recv_timeout)
        except asyncio.TimeoutError:
            continue

        if raw == "PONG":
            continue

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            logger.debug("Ignoring non-JSON message: %s", raw[:120])
            continue

        if not isinstance(data, (list, dict)):
            logger.debug("Ignoring unexpected type %s", type(data).__name__)
            continue

        if touch_activity:
            touch_activity()

        if not should_save():
            continue

        messages = data if isinstance(data, list) else [data]
        for item in messages:
            event = item.get("event_type")
            if event == "book":
                on_book(item)
            elif event == "last_trade_price":
                on_trade(item)


# ── Connect ────────────────────────────────────────────────────────


async def connect_and_subscribe(
    asset_ids: list[str],
) -> websockets.WebSocketClientProtocol:
    """Open a connection and subscribe to *asset_ids*.

    Returns the connected (subscribed) websocket.
    """
    settings = load_settings()
    ws = await websockets.connect(
        settings.ws_url,
        max_size=settings.ws_max_size,
    )
    await subscribe(ws, asset_ids)
    logger.info("WS connected & subscribed (%d assets)", len(asset_ids))
    return ws


# ── Cleanup ────────────────────────────────────────────────────────


async def close_ws(
    ws: websockets.WebSocketClientProtocol | None,
    tasks: list[asyncio.Task] | None = None,
) -> None:
    """Close the websocket and cancel its background tasks."""
    if ws is not None:
        try:
            await ws.close()
        except Exception:
            pass
    if tasks:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
