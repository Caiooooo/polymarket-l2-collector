"""Tests for DualWsManager — dual WS connection lifecycle manager."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from polymarket_l2_collector.ws_wallet.dual_ws import DualWsManager


@pytest.fixture
def mock_ws():
    ws = AsyncMock()
    ws.recv = AsyncMock(return_value='{"event_type": "book", "bids": [], "asks": []}')
    return ws


@pytest.mark.asyncio
async def test_connect_creates_two_connections(mock_ws):
    with patch(
        "polymarket_l2_collector.ws_wallet.dual_ws.connect_and_subscribe",
        new=AsyncMock(return_value=mock_ws),
    ) as mock_connect:
        mgr = DualWsManager()
        await mgr.connect(["asset_1", "asset_2"])
        assert mock_connect.call_count == 2
        assert mgr.active_tag == "primary"
        assert mgr.ws("primary") is not None
        assert mgr.ws("secondary") is not None


@pytest.mark.asyncio
async def test_switch_changes_active_tag(mock_ws):
    with patch(
        "polymarket_l2_collector.ws_wallet.dual_ws.connect_and_subscribe",
        new=AsyncMock(return_value=mock_ws),
    ):
        mgr = DualWsManager()
        await mgr.connect(["asset_1"])
        assert mgr.active_tag == "primary"
        mgr.switch()
        assert mgr.active_tag == "secondary"


@pytest.mark.asyncio
async def test_switch_does_not_close_or_reconnect(mock_ws):
    with patch(
        "polymarket_l2_collector.ws_wallet.dual_ws.connect_and_subscribe",
        new=AsyncMock(return_value=mock_ws),
    ):
        mgr = DualWsManager()
        await mgr.connect(["asset_1"])
        primary_before = mgr.ws("primary")
        mgr.switch()
        assert mgr.ws("primary") is primary_before


@pytest.mark.asyncio
async def test_health_check_primary_stale_triggers_switch(mock_ws):
    with patch(
        "polymarket_l2_collector.ws_wallet.dual_ws.connect_and_subscribe",
        new=AsyncMock(return_value=mock_ws),
    ):
        mgr = DualWsManager(primary_timeout=0.1)
        await mgr.connect(["asset_1"])
        mgr._last_msg_time["primary"] = 0.0
        await mgr.health_check()
        assert mgr.active_tag == "secondary"


@pytest.mark.asyncio
async def test_health_check_secondary_stale_reconnects(mock_ws):
    with patch(
        "polymarket_l2_collector.ws_wallet.dual_ws.connect_and_subscribe",
        new=AsyncMock(return_value=mock_ws),
    ) as mock_connect:
        mgr = DualWsManager(secondary_timeout=0.1)
        await mgr.connect(["asset_1"])
        mgr._last_msg_time["secondary"] = 0.0
        await mgr.health_check()
        assert mock_connect.call_count > 2


@pytest.mark.asyncio
async def test_close_cleans_up_both_connections(mock_ws):
    with patch(
        "polymarket_l2_collector.ws_wallet.dual_ws.connect_and_subscribe",
        new=AsyncMock(return_value=mock_ws),
    ):
        with patch(
            "polymarket_l2_collector.ws_wallet.dual_ws.close_ws",
            new=AsyncMock(),
        ) as mock_close:
            mgr = DualWsManager()
            await mgr.connect(["asset_1"])
            await mgr.close()
            assert mock_close.call_count == 2


@pytest.mark.asyncio
async def test_connect_failure_secondary_starts_degraded(mock_ws):
    with patch(
        "polymarket_l2_collector.ws_wallet.dual_ws.connect_and_subscribe",
        new=AsyncMock(side_effect=[mock_ws, ConnectionError("fail")]),
    ):
        mgr = DualWsManager()
        await mgr.connect(["asset_1"])
        assert mgr.active_tag == "primary"
        assert mgr.ws("secondary") is None
