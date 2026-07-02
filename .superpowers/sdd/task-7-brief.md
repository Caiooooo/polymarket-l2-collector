# Task 7: ChainVerifyWorker — 离线链上验证

## Context

Offline verification module that uses HyperSync to compare WS-collected trades against on-chain OrderFilled events. Optional (gated by CHAIN_VERIFY_ENABLED). Runs outside the real-time data path.

## Files
- Create: `polymarket_l2_collector/ws_wallet/chain_verify.py`
- Create: `tests/test_ws_wallet/test_chain_verify.py`

## Interfaces

```python
class ChainVerifyWorker:
    def __init__(self, enabled: bool | None = None): ...
    async def verify_window(
        self,
        interval: str,
        coin: str,
        data_type: str,
        direction: str,
        window_ts: int,
        parquet_path: str,
    ) -> dict[str, Any] | None: ...
```

### verify_window return dict:
```python
{
    "ws_trade_count": int,
    "onchain_trade_count": int,
    "completeness_pct": float,
    "ws_total_usd": 0.0,
    "onchain_total_usd": float,
    "verified_at": str,  # ISO-8601
    "status": "verified" | "incomplete",
}
```

## Implementation Details

### Constants
- `_POLYGON_BLOCK_TIME = 2.0` — average Polygon block time
- `_CTF_EXCHANGE_V2 = "0xe111180000d2663c0091e4f400237545b87b996b"` — CTF Exchange V2 address
- `_CONFIRMATION_BUFFER = 120` — seconds to wait before chain data is final

### __init__
- If `enabled` is None, load from `settings.chain_verify_enabled`
- Store `_enabled` flag

### verify_window
- If not enabled → return None
- If `now - window_end < _CONFIRMATION_BUFFER` → return None (too recent)
- Count WS trades: `_count_parquet_trades(parquet_path)`
- Query HyperSync: `await _query_hypersync(window_ts, window_end, coin)`
- Compute completeness: `min(ws_count / onchain_count, 1.0) * 100` (if onchain > 0)
- If completeness < 90% → status = "incomplete"
- Write to .meta.json via `_write_chain_meta(parquet_path, result)`
- Return result dict

### _count_parquet_trades
- If file doesn't exist → 0
- Use `pd.read_parquet(path)`, return len(df)
- Exception-safe → return 0

### _query_hypersync (simplified)
- Try importing hypersync/eth_utils → if ImportError → return []
- Check `HYPERSYNC_API` env var → if not set → return []
- Build approximate block range:
  - `from_block = max(0, int(window_ts / 2.0) - 10_000_000)`  # rough estimate
  - `to_block = from_block + int((window_end - window_ts) / 2.0) + 100`
- Build Query with ORDERFILLED_TOPIC for CTF Exchange V2
- Stream logs, return as list of dicts with {"usd_amount": "0"}
- Exception-safe → return []
- NOTE: This is a simplified implementation — real filtering by coin/asset_id is not needed for the initial version

### _write_chain_meta
- Read existing `.meta.json` file
- Add `chain_verify` key with result dict
- Write back with json.dump
- Exception-safe (silent on failure)

## Test Code

Write `tests/test_ws_wallet/test_chain_verify.py`:

```python
"""Tests for ChainVerifyWorker — offline HyperSync verification."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from polymarket_l2_collector.ws_wallet.chain_verify import ChainVerifyWorker


def test_skip_when_disabled():
    worker = ChainVerifyWorker(enabled=False)
    result = worker.verify_window("5m", "btc", "orderbooks", "up", 1000, "/tmp/test.parquet")
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
```

## Steps

1. Write tests → fail (no chain_verify.py)
2. Run: `uv run pytest tests/test_ws_wallet/test_chain_verify.py -v` → FAIL
3. Implement chain_verify.py
4. Run tests → PASS
5. Run full suite: `uv run pytest tests/ -v --tb=short` → ALL PASS
6. Commit: `git commit -m "feat: add ChainVerifyWorker for offline HyperSync verification"`

## Report

Write to `/home/debian/pmdata/polymarket-l2-collector/.superpowers/sdd/task-7-report.md`
