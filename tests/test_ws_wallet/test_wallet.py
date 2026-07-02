"""Tests for WalletService facade. (Config test also in this file.)"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# ── Config defaults (from Task 1) ─────────────────────────────
from polymarket_l2_collector.config import load_settings
from polymarket_l2_collector.ws_wallet import WalletService


def test_wallet_config_defaults():
    settings = load_settings()
    assert settings.wallet_primary_timeout == 60
    assert settings.wallet_secondary_timeout == 120
    assert settings.wallet_verify_interval == 1.0
    assert settings.wallet_switch_on_divergence == 50.0
    assert settings.chain_verify_enabled is False


# ── WalletService tests ────────────────────────────────────────


@pytest.mark.asyncio
async def test_subscribe_calls_dual_connect():
    with (
        patch("polymarket_l2_collector.ws_wallet.wallet.DualWsManager", autospec=True) as mock_mgr,
        patch("polymarket_l2_collector.ws_wallet.wallet.Verifier", autospec=True),
    ):
        mgr_instance = mock_mgr.return_value
        mgr_instance.connect = AsyncMock()
        mgr_instance.ws = MagicMock(return_value=AsyncMock())

        wallet = WalletService()
        await wallet.subscribe(["a1", "a2"])

        mgr_instance.connect.assert_called_once_with(["a1", "a2"])

        # Cleanup background tasks
        wallet._running = False
        for t in (wallet._reader_task, wallet._verify_task):
            if t is not None:
                t.cancel()
        await asyncio.gather(
            *[t for t in (wallet._reader_task, wallet._verify_task) if t is not None],
            return_exceptions=True,
        )


@pytest.mark.asyncio
async def test_recv_returns_from_queue():
    with (
        patch("polymarket_l2_collector.ws_wallet.wallet.DualWsManager", autospec=True),
        patch("polymarket_l2_collector.ws_wallet.wallet.Verifier", autospec=True),
    ):
        wallet = WalletService()
        test_msg = {"event_type": "book", "asset_id": "a1"}
        wallet._queue.put_nowait(test_msg)

        msg = await wallet.recv(timeout=0.5)
        assert msg is not None
        assert msg["event_type"] == "book"


@pytest.mark.asyncio
async def test_close_cleans_up():
    with (
        patch("polymarket_l2_collector.ws_wallet.wallet.DualWsManager", autospec=True) as mock_mgr,
        patch("polymarket_l2_collector.ws_wallet.wallet.Verifier", autospec=True),
    ):
        mgr_instance = mock_mgr.return_value
        mgr_instance.close = AsyncMock()

        wallet = WalletService()
        wallet._manager = mgr_instance
        wallet._running = True

        # Create real asyncio tasks so close() can cancel + gather them
        async def _noop() -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                pass

        wallet._reader_task = asyncio.create_task(_noop())
        wallet._verify_task = asyncio.create_task(_noop())

        await wallet.close()
        mgr_instance.close.assert_called_once()
        assert wallet._reader_task.cancelled()
        assert wallet._verify_task.cancelled()


@pytest.mark.asyncio
async def test_active_ws_tag_reflects_manager():
    with (
        patch("polymarket_l2_collector.ws_wallet.wallet.DualWsManager", autospec=True) as mock_mgr,
        patch("polymarket_l2_collector.ws_wallet.wallet.Verifier", autospec=True),
    ):
        mgr_instance = mock_mgr.return_value
        mgr_instance.active_tag = "secondary"

        wallet = WalletService()
        wallet._manager = mgr_instance
        assert wallet.active_ws_tag == "secondary"
