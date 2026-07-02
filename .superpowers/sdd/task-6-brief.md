# Task 6: Collector + main.py 集成 WalletService

## Context

Wire WalletService into Collector and main.py. Collector stops creating WS connections directly and instead uses wallet.subscribe()/recv(). main.py creates the wallet and passes it to all Collectors.

## Files
- Modify: `polymarket_l2_collector/collector.py`
- Modify: `polymarket_l2_collector/main.py`

## Interfaces

**Consumes:** `WalletService` (Task 4), updated `receive_loop` (Task 5)

### Collector changes

**1. Constructor** — add `wallet: Any | None = None` parameter, store as `self._wallet`

**2. New method** — no-op ping loop for wallet mode:
```python
async def _wallet_noop_ping(self) -> None:
    """No-op ping loop for wallet mode (ping handled by WalletService)."""
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
```

**3. _start_ws** — when `self._wallet` is not None:
   - Call `self._wallet.subscribe(asset_ids)` instead of `connect_and_subscribe(asset_ids)`
   - Create receive_loop with `recv_fn=self._wallet.recv` instead of passing websocket
   - Return `None` for websocket (so `close_ws` skips the WS close but still cancels tasks)

Full _start_ws when wallet is set:
```python
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
```

The existing code path (when wallet is None) stays exactly the same.

**4. close_ws** — No changes needed! When `_current_ws` is None (wallet mode), existing `close_ws` handles it:
```python
if ws is not None:  # ← already guarded
```

### main.py changes

**1. Import** — add `from .ws_wallet import WalletService`

**2. _wrap_collector** — add `wallet: WalletService | None = None` parameter, pass to Collector constructor

**3. _run_session** — create one WalletService at the start of each session:
```python
wallet = WalletService()

tasks = [
    asyncio.create_task(_wrap_binance(killer), name="binance"),
]
for interval in settings.intervals:
    tasks.append(
        asyncio.create_task(
            _wrap_collector(interval, killer, wallet=wallet),
            name=f"poly_{interval}"
        )
    )
```

**4. Session cleanup** — after `done, pending = await asyncio.wait(...)` and pending task cleanup:
```python
# NEW: close wallet
try:
    await wallet.close()
except Exception as exc:
    logger.error("Wallet close error: %s", exc)
```

## Steps

1. Read current `collector.py` and `main.py` to understand exact code
2. Apply Collector changes (constructor, _wallet_noop_ping, _start_ws)
3. Apply main.py changes (import, _wrap_collector, _run_session, cleanup)
4. Run full suite: `uv run pytest tests/ -v --tb=short` → ALL PASS
5. Commit: `feat: integrate WalletService into Collector and main loop`

## Report

Write to `/home/debian/pmdata/polymarket-l2-collector/.superpowers/sdd/task-6-report.md`
