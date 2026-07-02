"""Tests for ChainVerifyWorker — offline HyperSync verification."""
from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from polymarket_l2_collector.ws_wallet.chain_verify import ChainVerifyWorker


@pytest.mark.asyncio
async def test_skip_when_disabled():
    worker = ChainVerifyWorker(enabled=False)
    result = await worker.verify_window("5m", "btc", "orderbooks", "up", 1000, "/tmp/test.parquet")
    assert result is None


def test_verify_result_structure():
    worker = ChainVerifyWorker(enabled=True)
    with patch.object(worker, "_query_hypersync", new=AsyncMock(return_value=[])):
        result = asyncio.run(worker.verify_window("5m", "btc", "trades", "up", 1000, "/tmp/none.parquet"))
        assert result is not None
        assert "ws_trade_count" in result
        assert "onchain_trade_count" in result
        assert "completeness_pct" in result
        assert "verified_at" in result
