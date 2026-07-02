# Task 4: WalletService — Complete

## Status
**Completed successfully**

## Steps

1. **Wrote tests** (TDD first): Updated `tests/test_ws_wallet/test_wallet.py` with 4 new WalletService tests while keeping the existing config test.
   - `test_subscribe_calls_dual_connect` — verifies `subscribe()` calls `manager.connect()`
   - `test_recv_returns_from_queue` — verifies `recv()` pops from the internal queue
   - `test_close_cleans_up` — verifies `close()` cancels tasks and calls `manager.close()`
   - `test_active_ws_tag_reflects_manager` — verifies `active_ws_tag` delegates to manager

2. **Ran tests** → FAIL (expected, WalletService didn't exist yet)

3. **Implemented** `polymarket_l2_collector/ws_wallet/wallet.py`:
   - `WalletService.__init__` — loads settings, creates `DualWsManager`, `Verifier`, and `asyncio.Queue(maxsize=2048)`
   - `subscribe(asset_ids)` — first call connects + starts `_reader_loop` / `_verify_loop` tasks; subsequent calls accumulate IDs (dedup) and re-subscribe both WS via `ws_client.subscribe`
   - `recv(timeout)` — `wait_for` on the queue, returns `None` on timeout
   - `active_ws_tag` — delegates to `manager.active_tag`
   - `reconnect_active()` — calls `manager._reconnect_one(active_tag, ids)`
   - `close()` — sets `_running = False`, cancels reader/verify tasks, calls `manager.close()`
   - `_reader_loop()` — polls both WS (0.5s timeout), feeds verifier, pushes active-WS data to queue
   - `_verify_loop()` — ticks verifier at configured interval, handles `switch`/`warn`/degraded verdicts
   - `_parse(raw)` — JSON decode with type safety

4. **Updated** `polymarket_l2_collector/ws_wallet/__init__.py` — added `WalletService` import and export

5. **Ran tests** → PASS (5/5)

6. **Full suite** → **110 passed in 2.85s** (5 new + 105 existing, none broken)

## Deviations from brief spec

- **Test cleanup**: `test_subscribe_calls_dual_connect` needed explicit background task cleanup (sets `_running = False`, cancels reader/verify tasks, gathers) to avoid hanging in pytest-asyncio strict mode. The background tasks run infinite `while self._running` loops and must be cleaned up.
- **`test_close_cleans_up`**: Used real `asyncio.Task` objects instead of `AsyncMock` because `asyncio.gather` requires real task/coroutine objects, not mocks.

## Files changed/created

- `polymarket_l2_collector/ws_wallet/wallet.py` (new, 159 lines)
- `polymarket_l2_collector/ws_wallet/__init__.py` (modified)
- `tests/test_ws_wallet/test_wallet.py` (modified)

## Commit

`25ea7de` on `main` — `feat: add WalletService facade with reader/verify loops`
