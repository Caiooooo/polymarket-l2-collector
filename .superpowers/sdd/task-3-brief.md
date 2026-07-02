# Task 3: DualWsManager — 双 WS 连接生命周期管理

## Context

Manages two independent WS connections (primary + secondary) with health checks, automatic failover, and reconnection. Used by WalletService (Task 4).

## Files
- Create: `polymarket_l2_collector/ws_wallet/dual_ws.py`
- Create: `tests/test_ws_wallet/test_dual_ws.py`

## Interfaces Consumed
- `config.py` fields: `wallet_primary_timeout`, `wallet_secondary_timeout`
- `ws_client.py`: `connect_and_subscribe(asset_ids: list[str]) -> WebSocketClientProtocol`, `close_ws(ws, tasks)`, `send_ping_loop(ws)`
- Config: `ws_url`, `ws_max_size`

## Interface Produced

```python
class DualWsManager:
    def __init__(self, primary_timeout=None, secondary_timeout=None): ...
    async def connect(self, asset_ids: list[str]) -> None: ...
    def ws(self, tag: str) -> WebSocketClientProtocol | None: ...
    @property
    def active_tag(self) -> str: ...  # "primary" | "secondary"
    def switch(self) -> None: ...
    def touch(self, tag: str) -> None: ...
    async def health_check(self) -> str | None: ...  # returns switched-to tag
    async def _reconnect_one(self, tag: str, asset_ids: list[str]) -> None: ...
    async def close(self) -> None: ...
```

## Implementation Details

### Connection
- `connect()`: iterate `["primary", "secondary"]`, call `_connect_one()` for each. If secondary fails, continue (primary must succeed).
- `_connect_one(tag, asset_ids)`: call `connect_and_subscribe(asset_ids)`, start `send_ping_loop(ws)` as a task. Return `(ws, [ping_task])`.
- Store ws + tasks in `self._ws[tag]` and `self._tasks[tag]`.

### Health Check
- `health_check()`: called periodically (by WalletService). Check time since `_last_msg_time[tag]`:
  - Primary > primary_timeout → switch to secondary, reconnect primary via `_reconnect_one`
  - Secondary > secondary_timeout → reconnect secondary
- `_reconnect_one(tag, asset_ids)`: close old ws, call _connect_one.

### Switch
- Just changes `_active` pointer. No reconnect.

### Touch
- `touch(tag)`: record `time.time()` as `_last_msg_time[tag]`, increment `_msg_count[tag]`.

### Close
- Close both connections via `close_ws()`, clear state.

## Test Code

Write tests in `tests/test_ws_wallet/test_dual_ws.py`:

```python
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
    with patch("polymarket_l2_collector.ws_wallet.dual_ws.connect_and_subscribe", new=AsyncMock(return_value=mock_ws)) as mock_connect:
        mgr = DualWsManager()
        await mgr.connect(["asset_1", "asset_2"])
        assert mock_connect.call_count == 2
        assert mgr.active_tag == "primary"
        assert mgr.ws("primary") is not None
        assert mgr.ws("secondary") is not None

@pytest.mark.asyncio
async def test_switch_changes_active_tag(mock_ws):
    with patch("polymarket_l2_collector.ws_wallet.dual_ws.connect_and_subscribe", new=AsyncMock(return_value=mock_ws)):
        mgr = DualWsManager()
        await mgr.connect(["asset_1"])
        assert mgr.active_tag == "primary"
        mgr.switch()
        assert mgr.active_tag == "secondary"

@pytest.mark.asyncio
async def test_switch_does_not_close_or_reconnect(mock_ws):
    with patch("polymarket_l2_collector.ws_wallet.dual_ws.connect_and_subscribe", new=AsyncMock(return_value=mock_ws)):
        mgr = DualWsManager()
        await mgr.connect(["asset_1"])
        primary_before = mgr.ws("primary")
        mgr.switch()
        assert mgr.ws("primary") is primary_before

@pytest.mark.asyncio
async def test_health_check_primary_stale_triggers_switch(mock_ws):
    with patch("polymarket_l2_collector.ws_wallet.dual_ws.connect_and_subscribe", new=AsyncMock(return_value=mock_ws)):
        mgr = DualWsManager(primary_timeout=0.1)
        await mgr.connect(["asset_1"])
        mgr._last_msg_time["primary"] = 0.0
        await mgr.health_check()
        assert mgr.active_tag == "secondary"

@pytest.mark.asyncio
async def test_health_check_secondary_stale_reconnects(mock_ws):
    with patch("polymarket_l2_collector.ws_wallet.dual_ws.connect_and_subscribe", new=AsyncMock(return_value=mock_ws)) as mock_connect:
        mgr = DualWsManager(secondary_timeout=0.1)
        await mgr.connect(["asset_1"])
        mgr._last_msg_time["secondary"] = 0.0
        await mgr.health_check()
        assert mock_connect.call_count > 2

@pytest.mark.asyncio
async def test_close_cleans_up_both_connections(mock_ws):
    with patch("polymarket_l2_collector.ws_wallet.dual_ws.connect_and_subscribe", new=AsyncMock(return_value=mock_ws)):
        with patch("polymarket_l2_collector.ws_wallet.dual_ws.close_ws", new=AsyncMock()) as mock_close:
            mgr = DualWsManager()
            await mgr.connect(["asset_1"])
            await mgr.close()
            assert mock_close.call_count == 2

@pytest.mark.asyncio
async def test_connect_failure_secondary_starts_degraded(mock_ws):
    with patch("polymarket_l2_collector.ws_wallet.dual_ws.connect_and_subscribe", new=AsyncMock(side_effect=[mock_ws, ConnectionError("fail")])):
        mgr = DualWsManager()
        await mgr.connect(["asset_1"])
        assert mgr.active_tag == "primary"
        assert mgr.ws("secondary") is None
```

## Steps

1. Write tests first
2. Run `uv run pytest tests/test_ws_wallet/test_dual_ws.py -v` → FAIL
3. Implement DualWsManager in `dual_ws.py`
4. Run tests → PASS
5. Run full suite: `uv run pytest tests/ -v --tb=short` → ALL PASS
6. Commit: `git commit -m "feat: add DualWsManager with health checks and failover"`

## Report Contract

Write to `/home/debian/pmdata/polymarket-l2-collector/.superpowers/sdd/task-3-report.md`
