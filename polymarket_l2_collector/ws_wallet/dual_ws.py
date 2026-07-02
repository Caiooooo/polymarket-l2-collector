"""Dual WebSocket connection lifecycle manager with health checks and failover."""
from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING

from polymarket_l2_collector.ws_client import (
    close_ws,
    connect_and_subscribe,
    send_ping_loop,
)
from polymarket_l2_collector.logger_config import get_logger

if TYPE_CHECKING:
    import websockets

logger = get_logger("dual_ws")

TAGS = ["primary", "secondary"]


class DualWsManager:
    """Manages two independent WS connections (primary + secondary).

    Provides health checks, automatic failover, and reconnection.
    Used by WalletService (Task 4).
    """

    def __init__(
        self,
        primary_timeout: float | None = None,
        secondary_timeout: float | None = None,
    ) -> None:
        from polymarket_l2_collector.config import load_settings

        settings = load_settings()
        self._primary_timeout = (
            primary_timeout if primary_timeout is not None else settings.wallet_primary_timeout
        )
        self._secondary_timeout = (
            secondary_timeout
            if secondary_timeout is not None
            else settings.wallet_secondary_timeout
        )

        self._ws: dict[str, websockets.WebSocketClientProtocol | None] = {
            "primary": None,
            "secondary": None,
        }
        self._tasks: dict[str, list[asyncio.Task]] = {
            "primary": [],
            "secondary": [],
        }
        self._last_msg_time: dict[str, float] = {
            "primary": 0.0,
            "secondary": 0.0,
        }
        self._msg_count: dict[str, int] = {
            "primary": 0,
            "secondary": 0,
        }
        self._active: str = "primary"
        self._asset_ids: list[str] = []

    # ── Public API ──────────────────────────────────────────────────

    async def connect(self, asset_ids: list[str]) -> None:
        """Open both primary and secondary WS connections.

        If the secondary connection fails, the manager continues in
        degraded mode (primary only). The primary **must** succeed.
        """
        self._asset_ids = asset_ids
        for tag in TAGS:
            try:
                ws, tasks = await self._connect_one(tag, asset_ids)
                self._ws[tag] = ws
                self._tasks[tag] = tasks
                self._last_msg_time[tag] = time.time()
                logger.info("DualWsManager: %s connected", tag)
            except Exception:
                logger.warning("DualWsManager: %s connection failed", tag, exc_info=True)
                self._ws[tag] = None
                self._tasks[tag] = []
                if tag == "primary":
                    raise

    def ws(self, tag: str) -> websockets.WebSocketClientProtocol | None:
        """Return the WS connection for *tag*, or None if not connected."""
        return self._ws.get(tag)

    @property
    def active_tag(self) -> str:
        """Return the currently active tag ("primary" | "secondary")."""
        return self._active

    def switch(self) -> None:
        """Swap the active tag without reconnecting."""
        self._active = "secondary" if self._active == "primary" else "primary"
        logger.info("DualWsManager: switched active to %s", self._active)

    def touch(self, tag: str) -> None:
        """Record message activity for the given *tag*."""
        self._last_msg_time[tag] = time.time()
        self._msg_count[tag] += 1

    async def health_check(self) -> str | None:
        """Check connection health and reconnect if stale.

        Returns the tag that was switched to (if primary was stale and we
        failed over to secondary), or None if no switch occurred.
        """
        switched_to: str | None = None
        now = time.time()

        # Check primary
        if (
            self._ws["primary"] is not None
            and now - self._last_msg_time["primary"] > self._primary_timeout
        ):
            logger.warning("DualWsManager: primary stale, switching to secondary")
            self.switch()
            switched_to = self._active
            await self._reconnect_one("primary", self._asset_ids)

        # Check secondary
        if (
            self._ws["secondary"] is not None
            and now - self._last_msg_time["secondary"] > self._secondary_timeout
        ):
            logger.warning("DualWsManager: secondary stale, reconnecting")
            await self._reconnect_one("secondary", self._asset_ids)

        return switched_to

    async def close(self) -> None:
        """Close both WS connections and clear all state."""
        for tag in TAGS:
            await close_ws(self._ws[tag], self._tasks[tag])
            self._ws[tag] = None
            self._tasks[tag] = []
            self._last_msg_time[tag] = 0.0
            self._msg_count[tag] = 0
        self._active = "primary"
        self._asset_ids = []
        logger.info("DualWsManager: closed")

    # ── Internal helpers ────────────────────────────────────────────

    async def _connect_one(
        self,
        tag: str,
        asset_ids: list[str],
    ) -> tuple[websockets.WebSocketClientProtocol, list[asyncio.Task]]:
        """Open a single connection, subscribe, and start a ping loop."""
        ws = await connect_and_subscribe(asset_ids)
        ping_task = asyncio.create_task(send_ping_loop(ws))
        ping_task.set_name(f"{tag}-ping")
        return ws, [ping_task]

    async def _reconnect_one(self, tag: str, asset_ids: list[str]) -> None:
        """Close the old connection for *tag* and open a new one."""
        await close_ws(self._ws[tag], self._tasks[tag])
        self._ws[tag] = None
        self._tasks[tag] = []
        try:
            ws, tasks = await self._connect_one(tag, asset_ids)
            self._ws[tag] = ws
            self._tasks[tag] = tasks
            self._last_msg_time[tag] = time.time()
            logger.info("DualWsManager: %s reconnected", tag)
        except Exception:
            logger.error("DualWsManager: %s reconnection failed", tag, exc_info=True)
