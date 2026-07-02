"""High-level facade for consuming the dual-WS data stream.

WalletService owns DualWsManager (connections) + Verifier (comparison)
and runs background reader/verify loops.
"""
from __future__ import annotations

import asyncio
import json
from typing import Any

from polymarket_l2_collector.config import load_settings
from polymarket_l2_collector.logger_config import get_logger
from polymarket_l2_collector.ws_wallet.dual_ws import DualWsManager
from polymarket_l2_collector.ws_wallet.verifier import Verifier

logger = get_logger("wallet")


class WalletService:
    """High-level facade for the dual-WS data stream.

    Owns DualWsManager (connections) + Verifier (data comparison),
    provides a single ``recv()`` interface, and runs background
    reader/verify loops.
    """

    def __init__(self) -> None:
        settings = load_settings()
        self._manager = DualWsManager(
            primary_timeout=settings.wallet_primary_timeout,
            secondary_timeout=settings.wallet_secondary_timeout,
        )
        self._verifier = Verifier(
            divergence_pct=settings.wallet_switch_on_divergence,
        )
        self._verify_interval = settings.wallet_verify_interval
        self._queue: asyncio.Queue[dict] = asyncio.Queue(maxsize=2048)
        self._asset_ids: list[str] = []
        self._connected = False
        self._running = False
        self._reader_task: asyncio.Task[None] | None = None
        self._verify_task: asyncio.Task[None] | None = None

    # ── Public API ──────────────────────────────────────────────────

    async def subscribe(self, asset_ids: list[str]) -> None:
        """Subscribe to *asset_ids* via both WS connections.

        First call connects and starts background loops.  Subsequent
        calls accumulate new IDs (deduplicating) and re-subscribe.
        """
        if not self._connected:
            self._asset_ids = list(asset_ids)
            await self._manager.connect(asset_ids)
            self._running = True
            self._connected = True
            self._reader_task = asyncio.create_task(self._reader_loop())
            self._verify_task = asyncio.create_task(self._verify_loop())
        else:
            self._accumulate_ids(asset_ids)
            await self._resubscribe_both()

    async def recv(self, timeout: float = 1.0) -> dict | None:
        """Return the next output message, or *None* on timeout."""
        try:
            return await asyncio.wait_for(self._queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    @property
    def active_ws_tag(self) -> str:
        """The tag (``"primary"`` | ``"secondary"``) currently in use."""
        return self._manager.active_tag

    async def reconnect_active(self) -> None:
        """Reconnect the currently active WS connection."""
        await self._manager._reconnect_one(self._manager.active_tag, self._asset_ids)  # noqa: SLF001

    async def close(self) -> None:
        """Shut down background loops and close both WS connections."""
        self._running = False
        tasks_to_cancel: list[asyncio.Task[None]] = []
        if self._reader_task is not None:
            self._reader_task.cancel()
            tasks_to_cancel.append(self._reader_task)
        if self._verify_task is not None:
            self._verify_task.cancel()
            tasks_to_cancel.append(self._verify_task)
        if tasks_to_cancel:
            await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
        await self._manager.close()
        self._connected = False

    # ── Internal helpers ────────────────────────────────────────────

    def _accumulate_ids(self, asset_ids: list[str]) -> None:
        existing = set(self._asset_ids)
        for aid in asset_ids:
            if aid not in existing:
                self._asset_ids.append(aid)
                existing.add(aid)

    async def _resubscribe_both(self) -> None:
        from polymarket_l2_collector.ws_client import subscribe as ws_subscribe  # noqa: PLC0415

        for tag in ("primary", "secondary"):
            ws = self._manager.ws(tag)
            if ws is not None:
                await ws_subscribe(ws, self._asset_ids)

    async def _reader_loop(self) -> None:
        """Read from both WS connections, feed the verifier, push active data to output."""
        tags = ("primary", "secondary")
        while self._running:
            for tag in tags:
                ws = self._manager.ws(tag)
                if ws is None:
                    continue
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=0.5)
                except asyncio.TimeoutError:
                    continue
                except Exception:
                    continue

                if raw == "PONG":
                    continue

                parsed = self._parse(raw)
                if parsed is None:
                    continue

                if tag == "primary":
                    self._verifier.feed_primary(parsed)
                else:
                    self._verifier.feed_secondary(parsed)

                if tag == self._manager.active_tag:
                    self._manager.touch(tag)
                    await self._queue.put(parsed)

    async def _verify_loop(self) -> None:
        """Periodically tick the verifier and handle verdicts."""
        while self._running:
            await asyncio.sleep(self._verify_interval)
            try:
                verdict = self._verifier.tick()
            except Exception:
                continue
            if verdict is None:
                continue

            if verdict.action == "switch":
                self._manager.switch()
            elif verdict.action == "warn":
                pass  # log only

            if self._verifier.is_degraded:
                asyncio.create_task(self.reconnect_active())

    def _parse(self, raw: Any) -> dict | None:
        """Parse raw WS data into a dict, returning None for invalid input."""
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8", errors="replace")
        if not isinstance(raw, str):
            return None
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return None
        if not isinstance(data, dict):
            return None
        return data
