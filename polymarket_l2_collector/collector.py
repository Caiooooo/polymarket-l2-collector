"""
Parameterised window-based WebSocket data collector.

Replaces the duplicated ``poly_ws_5min.py`` / ``poly_ws_15min.py`` with
a single ``Collector`` class that takes interval, coins, directions, etc.
as parameters.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from typing import Any

from .config import load_settings
from .data_formatter import format_orderbook, format_trade
from .file_cache import save_book, save_trades
from .logger_config import get_logger
from .market_discovery import resolve_assets
from .window_metadata import mark_complete, mark_failed, record_disconnect, touch_message
from .ws_client import (
    build_asset_id_list,
    close_ws,
    connect_and_subscribe,
    receive_loop,
    send_ping_loop,
)

logger = get_logger("collector")


def _build_file_path(
    data_dir: str,
    interval: str,
    coin: str,
    direction: str,
    data_type: str,
    window_ts: int,
) -> str:
    return f"{data_dir}/{interval}/{coin}/{data_type}/{window_ts}{direction}.parquet"


class Collector:
    """Real-time data collector for a single time interval.

    Manages per-window WS connections: connects at the start of each
    window, switches at the boundary, and skips saving the first
    (partial) window on startup.

    Usage::

        collector = Collector(interval="5m", coins=["btc", "eth"])
        await collector.run()
    """

    def __init__(
        self,
        interval: str,
        coins: list[str] | None = None,
        directions: list[str] | None = None,
        data_dir: str | None = None,
        touch_activity: Callable[[], None] | None = None,
        wallet: Any | None = None,
    ) -> None:
        settings = load_settings()
        self.interval = interval
        self.interval_seconds = settings.interval_seconds(interval)
        self.coins = coins or settings.coins
        self.directions = directions or settings.directions
        self.data_dir = data_dir or settings.data_dir
        self._touch_activity = touch_activity
        self._wallet = wallet

        # WS lifecycle state
        self._current_ws: Any = None
        self._current_tasks: list[asyncio.Task] = []
        self._current_asset_to_coin: dict[str, str] = {}

        self._next_ws: Any = None
        self._next_tasks: list[asyncio.Task] = []
        self._next_asset_to_coin: dict[str, str] = {}

        self._saving_enabled = False
        self._retry_count = 0

    # ── Asset resolution ───────────────────────────────────────────

    async def _resolve_assets_for_window(
        self, window_start: int
    ) -> dict[str, dict[str, Any]]:
        """Fetch asset IDs for every configured coin at *window_start*.

        Returns the nested dict expected by ``build_asset_id_list``.
        """
        assets: dict[str, dict[str, Any]] = {}
        for coin in self.coins:
            coin_assets = await resolve_assets(coin, self.interval, target_timestamp=window_start)
            if coin_assets:
                assets[coin] = {self.interval: coin_assets}
            else:
                logger.warning(
                    "No assets for %s @ %s",
                    coin,
                    window_start,
                    extra={"coin": coin, "interval": self.interval, "window": window_start},
                )
        return assets

    def _build_asset_to_coin(self, assets: dict[str, dict[str, Any]]) -> dict[str, str]:
        """Build asset_id → "COIN_direction_interval" lookup from the assets dict."""
        mapping: dict[str, str] = {}
        for coin, intervals in assets.items():
            for interval_key, data in intervals.items():
                if not isinstance(data, dict):
                    continue
                for direction in self.directions:
                    aid = data.get(direction)
                    if aid:
                        coin_tag = f"{coin.upper()}_{direction}_{interval_key}"
                        mapping[aid] = coin_tag
        return mapping

    # ── Book / trade save helpers ──────────────────────────────────

    def _make_save_book(self, asset_to_coin: dict[str, str], window_open_ts: int):
        """Return a closure that saves a single book message."""
        data_dir = self.data_dir
        interval = self.interval
        directions = self.directions

        def _save(data: dict[str, Any]) -> None:
            coin_tag = asset_to_coin.get(data.get("asset_id", ""))
            if not coin_tag or "_" not in coin_tag:
                return
            coin, direction, _ = coin_tag.lower().split("_", 2)
            if direction not in directions:
                return
            ts_ms = int(data.get("timestamp", 0))
            touch_message(interval, coin, "orderbooks", direction, window_open_ts, ts_ms)
            rows = format_orderbook([data], asset_to_coin, window_open_ts=window_open_ts)
            if not rows:
                return
            fp = _build_file_path(data_dir, interval, coin, direction, "orderbooks", window_open_ts)
            save_book(rows, fp)

        return _save

    def _make_save_trade(self, asset_to_coin: dict[str, str], window_open_ts: int):
        """Return a closure that saves a single trade message."""
        data_dir = self.data_dir
        interval = self.interval
        directions = self.directions

        def _save(data: dict[str, Any]) -> None:
            coin_tag = asset_to_coin.get(data.get("asset_id", ""))
            if not coin_tag or "_" not in coin_tag:
                return
            coin, direction, _ = coin_tag.lower().split("_", 2)
            if direction not in directions:
                return
            ts_ms = int(data.get("timestamp", 0))
            touch_message(interval, coin, "trades", direction, window_open_ts, ts_ms)
            rows = format_trade([data], asset_to_coin, window_open_ts=window_open_ts)
            if not rows:
                return
            fp = _build_file_path(data_dir, interval, coin, direction, "trades", window_open_ts)
            save_trades(rows, fp)

        return _save

    def _should_save(self) -> bool:
        return self._saving_enabled

    # ── Connection lifecycle ───────────────────────────────────────

    async def _wallet_noop_ping(self) -> None:
        """No-op ping loop for wallet mode (ping handled by WalletService)."""
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            pass

    async def _start_ws(
        self, window_open_ts: int
    ) -> tuple:
        """Connect and subscribe for *window_open_ts*.

        Returns ``(websocket, asset_to_coin, [recv_task, ping_task])``.
        When *wallet* is set, *websocket* is ``None`` (wallet manages
        connections internally).
        """
        assets = await self._resolve_assets_for_window(window_open_ts)
        asset_to_coin = self._build_asset_to_coin(assets)
        asset_ids = build_asset_id_list(assets, self.directions)

        on_book = self._make_save_book(asset_to_coin, window_open_ts)
        on_trade = self._make_save_trade(asset_to_coin, window_open_ts)

        if self._wallet is not None:
            await self._wallet.subscribe(asset_ids)
            recv_task = asyncio.create_task(
                receive_loop(
                    recv_fn=self._wallet.recv,
                    on_book=on_book,
                    on_trade=on_trade,
                    should_save=self._should_save,
                    touch_activity=self._touch_activity,
                )
            )
            ping_task = asyncio.create_task(self._wallet_noop_ping())
            return None, asset_to_coin, [recv_task, ping_task]

        ws = await connect_and_subscribe(asset_ids)

        recv_task = asyncio.create_task(
            receive_loop(
                ws,
                on_book=on_book,
                on_trade=on_trade,
                should_save=self._should_save,
                touch_activity=self._touch_activity,
            )
        )
        ping_task = asyncio.create_task(send_ping_loop(ws))

        return ws, asset_to_coin, [recv_task, ping_task]

    # ── Run ────────────────────────────────────────────────────────

    async def run(self) -> None:
        """Main loop — runs forever, managing window transitions."""
        startup_ts = int(time.time())
        current_window_start = (startup_ts // self.interval_seconds) * self.interval_seconds
        save_start_ts = current_window_start + self.interval_seconds
        self._saving_enabled = False
        logger.info(
            "Startup protection: saving starts at %s",
            save_start_ts,
            extra={"interval": self.interval, "save_start_ts": save_start_ts},
        )

        while True:
            try:
                logger.info("Connecting …", extra={"interval": self.interval})
                (
                    self._current_ws,
                    self._current_asset_to_coin,
                    self._current_tasks,
                ) = await self._start_ws(current_window_start)

                self._next_ws = None
                self._next_tasks = []
                next_window_start = current_window_start + self.interval_seconds
                next_switch_ts = next_window_start

                logger.info(
                    "Next window switch",
                    extra={
                        "interval": self.interval,
                        "next_window_ts": next_switch_ts,
                        "seconds_remaining": next_switch_ts - int(time.time()),
                    },
                )

                while True:
                    now = int(time.time())

                    if not self._saving_enabled and now >= save_start_ts:
                        self._saving_enabled = True
                        logger.info("Saving enabled", extra={"interval": self.interval})

                    # Window boundary → transition
                    if now >= next_switch_ts:
                        if self._next_ws is None:
                            logger.info(
                                "Connecting next window",
                                extra={
                                    "interval": self.interval,
                                    "window_ts": next_window_start,
                                },
                            )
                            (
                                self._next_ws,
                                self._next_asset_to_coin,
                                self._next_tasks,
                            ) = await self._start_ws(next_window_start)

                        # Mark the completed window in metadata
                        for coin in self.coins:
                            for d in self.directions:
                                mark_complete(self.interval, coin, "orderbooks", d, current_window_start)
                                mark_complete(self.interval, coin, "trades", d, current_window_start)

                        await close_ws(self._current_ws, self._current_tasks)

                        self._current_ws = self._next_ws
                        self._current_asset_to_coin = self._next_asset_to_coin
                        self._current_tasks = self._next_tasks

                        self._next_ws = None
                        self._next_tasks = []

                        current_window_start = next_window_start
                        next_window_start = current_window_start + self.interval_seconds
                        next_switch_ts = next_window_start
                        self._retry_count = 0
                        logger.info(
                            "Switched window",
                            extra={
                                "interval": self.interval,
                                "window_ts": current_window_start,
                            },
                        )

                    # Detect unexpected WS closure
                    if self._current_ws is not None:
                        closed = getattr(self._current_ws, "closed", None)
                        close_code = getattr(self._current_ws, "close_code", None)
                        if closed is True or close_code is not None:
                            raise RuntimeError(f"WS disconnected (close_code={close_code})")

                    await asyncio.sleep(0.5)

            except Exception as exc:
                logger.error(
                    "Connection error",
                    extra={"interval": self.interval, "error": str(exc)[:200]},
                )
                # Mark current window as failed and record disconnect
                err_msg = str(exc)[:200]
                for coin in self.coins:
                    for d in self.directions:
                        mark_failed(self.interval, coin, "orderbooks", d, current_window_start, err_msg)
                        mark_failed(self.interval, coin, "trades", d, current_window_start, err_msg)
                        record_disconnect(self.interval, coin, "orderbooks", d, current_window_start)
                        record_disconnect(self.interval, coin, "trades", d, current_window_start)
                await close_ws(self._current_ws, self._current_tasks)
                await close_ws(self._next_ws, self._next_tasks)
                self._current_ws = None
                self._current_tasks = []
                self._next_ws = None
                self._next_tasks = []

                self._retry_count += 1
                delay = min(5 * self._retry_count, 60)
                logger.info(
                    "Retrying",
                    extra={
                        "interval": self.interval,
                        "delay_seconds": delay,
                        "attempt": self._retry_count,
                    },
                )
                await asyncio.sleep(delay)
