# Task 4: WalletService — 高层面向外接口

## Context

WalletService is the high-level facade that Collector uses to consume the dual-WS data stream. It owns DualWsManager (connections) + Verifier (comparison), and runs background reader/verify loops.

## Files
- Create: `polymarket_l2_collector/ws_wallet/wallet.py`
- Modify: `tests/test_ws_wallet/test_wallet.py` (append tests)
- Modify: `polymarket_l2_collector/ws_wallet/__init__.py` (add WalletService to exports)

## Interfaces

**Consumes (already built in Tasks 1-3):**
- `DualWsManager` — `connect(ids)`, `ws(tag)`, `active_tag`, `switch()`, `touch(tag)`, `health_check()`, `close()`
- `Verifier` — `feed_primary(msg)`, `feed_secondary(msg)`, `tick() -> Verdict | None`, `is_degraded`
- Config: `wallet_primary_timeout`, `wallet_secondary_timeout`, `wallet_verify_interval`, `wallet_switch_on_divergence`

**Produces:**
```python
class WalletService:
    async def subscribe(self, asset_ids: list[str]) -> None
    async def recv(self, timeout: float = 1.0) -> dict | None
    @property
    def active_ws_tag(self) -> str
    async def reconnect_active(self) -> None
    async def close(self) -> None
```

## Implementation

### `__init__`:
- Load settings, create `DualWsManager` (with timeouts) and `Verifier` (with divergence_pct)
- Create `asyncio.Queue(maxsize=2048)` for output messages
- Track `_connected`, `_running`, `_reader_task`, `_verify_task`

### `subscribe(asset_ids)`:
- First call (not connected): save asset_ids, call `manager.connect(asset_ids)`, start `_reader_loop` and `_verify_loop` tasks
- Subsequent calls: accumulate asset_ids (dedup), re-subscribe both WS via `ws_client.subscribe(ws, all_ids)`
- Use `from ..ws_client import subscribe as ws_subscribe` for the re-subscribe call

### `recv(timeout)`:
- `await asyncio.wait_for(self._queue.get(), timeout=timeout)` → return msg or None

### `_reader_loop`:
```python
while self._running:
    for tag in ("primary", "secondary"):
        ws = self._manager.ws(tag)
        if ws is None: continue
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=0.5)
        except asyncio.TimeoutError:
            continue
        except Exception as exc:
            continue
        
        if raw == "PONG": continue
        
        parsed = self._parse(raw)
        if parsed is None: continue
        
        if tag == "primary":
            self._verifier.feed_primary(parsed)
        else:
            self._verifier.feed_secondary(parsed)
        
        if tag == self._manager.active_tag:
            self._manager.touch(tag)
            await self._queue.put(parsed)
```

### `_verify_loop`:
```python
while self._running:
    await asyncio.sleep(self._verify_interval)
    try:
        verdict = self._verifier.tick()
    except:
        continue
    if verdict is None: continue
    
    if verdict.action == "switch":
        self._manager.switch()
    elif verdict.action == "warn":
        pass  # log only
    
    if self._verifier.is_degraded:
        asyncio.create_task(self.reconnect_active())
```

### `_parse(raw)`:
- Parse JSON string to dict, return None for non-JSON or non-dict

### `close()`:
- Set `_running = False`, cancel reader/verify tasks, call `manager.close()`

### `reconnect_active()`:
- Reconnect the currently active WS via `manager._reconnect_one(tag, subscribed_ids)`

## Test Code

Append to existing `tests/test_ws_wallet/test_wallet.py` (replacing the minimal config test). The config test stays, add these new tests:

```python
"""Tests for WalletService facade. (Config test also in this file.)"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from polymarket_l2_collector.ws_wallet import WalletService

# ── Config defaults (from Task 1) ─────────────────────────────

from polymarket_l2_collector.config import load_settings

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
        patch("polymarket_l2_collector.ws_wallet.wallet.DualWsManager", autospec=True) as MockMgr,
        patch("polymarket_l2_collector.ws_wallet.wallet.Verifier", autospec=True),
    ):
        mgr_instance = MockMgr.return_value
        mgr_instance.connect = AsyncMock()
        mgr_instance.ws = MagicMock(return_value=AsyncMock())

        wallet = WalletService()
        await wallet.subscribe(["a1", "a2"])

        mgr_instance.connect.assert_called_once_with(["a1", "a2"])


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
        patch("polymarket_l2_collector.ws_wallet.wallet.DualWsManager", autospec=True) as MockMgr,
        patch("polymarket_l2_collector.ws_wallet.wallet.Verifier", autospec=True),
    ):
        mgr_instance = MockMgr.return_value
        mgr_instance.close = AsyncMock()

        wallet = WalletService()
        wallet._manager = mgr_instance
        wallet._running = True
        wallet._reader_task = AsyncMock()
        wallet._verify_task = AsyncMock()

        await wallet.close()
        mgr_instance.close.assert_called_once()


@pytest.mark.asyncio
async def test_active_ws_tag_reflects_manager():
    with (
        patch("polymarket_l2_collector.ws_wallet.wallet.DualWsManager", autospec=True) as MockMgr,
        patch("polymarket_l2_collector.ws_wallet.wallet.Verifier", autospec=True),
    ):
        mgr_instance = MockMgr.return_value
        mgr_instance.active_tag = "secondary"

        wallet = WalletService()
        wallet._manager = mgr_instance
        assert wallet.active_ws_tag == "secondary"
```

## Steps

1. Write tests (update test_wallet.py with both config test and new tests)
2. Run tests: `uv run pytest tests/test_ws_wallet/test_wallet.py -v` → FAIL
3. Implement WalletService in wallet.py
4. Update `__init__.py` to export `WalletService`
5. Run tests → PASS
6. Run full suite: `uv run pytest tests/ -v --tb=short` → ALL PASS
7. Commit: `git commit -m "feat: add WalletService facade with reader/verify loops"`

## Report

Write to `/home/debian/pmdata/polymarket-l2-collector/.superpowers/sdd/task-4-report.md`
