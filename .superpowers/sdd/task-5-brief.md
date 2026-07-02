# Task 5: ws_client.py 改造 — receive_loop 接受 callable

## Context

Modify the existing `receive_loop` function in `ws_client.py` to accept an optional `recv_fn` callable, allowing Collector to inject `WalletService.recv()` instead of a raw websocket. This is a backward-compatible change.

## Files
- Modify: `polymarket_l2_collector/ws_client.py`

## Changes

### Signature change

Old:
```python
async def receive_loop(
    websocket: websockets.WebSocketClientProtocol,
    on_book: Callable[[dict[str, Any]], None],
    on_trade: Callable[[dict[str, Any]], None],
    should_save: Callable[[], bool],
    touch_activity: Callable[[], None] | None = None,
    recv_timeout: float = 1.0,
) -> None:
```

New:
```python
async def receive_loop(
    websocket: websockets.WebSocketClientProtocol | None = None,
    on_book: Callable[[dict[str, Any]], None] | None = None,
    on_trade: Callable[[dict[str, Any]], None] | None = None,
    should_save: Callable[[], bool] | None = None,
    touch_activity: Callable[[], None] | None = None,
    recv_timeout: float = 1.0,
    recv_fn: Callable[[float], Awaitable[dict | None]] | None = None,
) -> None:
```

### Logic change

The receive loop body changes to:

```python
while True:
    try:
        if recv_fn is not None:
            raw_or_dict = await recv_fn(recv_timeout)
            if raw_or_dict is None:
                continue
            data = raw_or_dict  # recv_fn returns parsed dicts
        else:
            raw = await asyncio.wait_for(websocket.recv(), timeout=recv_timeout)
            if raw == "PONG":
                continue
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                logger.debug("Ignoring non-JSON message: %s", raw[:120])
                continue
    except asyncio.TimeoutError:
        continue
    except websockets.exceptions.ConnectionClosed:
        break
```

Then continue with the same dispatch logic (type check, touch_activity, should_save, dispatch to on_book/on_trade).

### Important:
- Add `Awaitable` to the typing imports (`from collections.abc import Awaitable`)
- Make `on_book`, `on_trade`, `should_save` all `None` by default (they can be None when recv_fn is used — the dispatch has `if event == "book" and on_book` guards already)
- The existing test `tests/test_smoke.py` calls `receive_loop` with positional args — the change to `websocket=None` default means positional usage still works
- **Backward compatible**: when `recv_fn` is None, behavior is identical to before

## Steps

1. Read the current `ws_client.py` to understand the exact code
2. Apply the changes
3. Run full suite: `uv run pytest tests/ -v --tb=short` → ALL PASS (should not break anything since it's backward compatible)
4. Commit: `refactor: receive_loop accepts optional recv_fn for WalletService`

## Report

Write to `/home/debian/pmdata/polymarket-l2-collector/.superpowers/sdd/task-5-report.md`
