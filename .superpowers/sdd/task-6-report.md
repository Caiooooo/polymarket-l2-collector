# Task 6 Report: Collector + main.py Integration

## Changes Made

### `polymarket_l2_collector/collector.py`
1. **Constructor** — added `wallet: Any | None = None` parameter, stored as `self._wallet`
2. **`_wallet_noop_ping()`** — new no-op ping coroutine that awaits `asyncio.Event().wait()` until cancelled (preserves the `[recv_task, ping_task]` tuple shape for cleanup)
3. **`_start_ws()`** — when `self._wallet` is not None, calls `wallet.subscribe(asset_ids)` and creates `receive_loop` with `recv_fn=self._wallet.recv` instead of opening a raw WS connection; returns `None` as the websocket so `close_ws` skips WS close but still cancels tasks

### `polymarket_l2_collector/main.py`
1. **Import** — added `from .ws_wallet import WalletService`
2. **`_wrap_collector`** — added `wallet: WalletService | None = None` parameter, passed to `Collector(..., wallet=wallet)`
3. **`_run_session`** — creates a single `WalletService()` at session start; passes it to all `_wrap_collector` calls so all intervals share the same wallet
4. **Session cleanup** — calls `await wallet.close()` (with error handling) after pending tasks are cancelled and before cache flush

## Verification

- All 110 tests pass
- Existing behaviour (wallet=None) is completely unchanged — no regressions
- Wallet mode flows through the same window-switch logic with `None` websocket
- `close_ws(None, tasks)` gracefully handles the None websocket (already guarded in ws_client.py)
