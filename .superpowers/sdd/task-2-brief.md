# Task 2: Verifier — RingBuffer 实时数据校验引擎

## Context

Pure data structure, no async, no network. First module of the ws_wallet package. Used by WalletService (Task 4) to compare primary vs secondary WS data.

## Files
- Create: `polymarket_l2_collector/ws_wallet/verifier.py`
- Create: `tests/test_ws_wallet/test_verifier.py`

## Interfaces

Classes to implement:

```python
@dataclass
class Verdict:
    ts_sec: int
    max_bid_diff_pct: float
    max_ask_diff_pct: float
    action: str  # "ok" | "warn" | "switch"

@dataclass
class Bucket:
    ts_sec: int
    primary: list[dict]
    secondary: list[dict]
    def top5_prices(self, side: str) -> list[float]: ...
    def compare(self, divergence_pct: float = 50.0) -> Verdict | None: ...

class Verifier:
    def __init__(self, divergence_pct: float = 50.0): ...
    def feed_primary(self, msg: dict) -> None: ...
    def feed_secondary(self, msg: dict) -> None: ...
    def tick(self) -> Verdict | None: ...
    @property
    def is_degraded(self) -> bool: ...
```

## Implementation Details

### Bucket
- `top5_prices(side)`: iterate primary messages in reverse (most recent first), extract first 5 price values from `msg[side]` list, return as `list[float]`. If `side` is "bids" the prices naturally descend; if "asks" they ascend — we just take top 5 as-is.
- `compare(divergence_pct)`: if either list is empty → None. Get top5 bids/asks from both primary and secondary. Use inner `_max_diff(a, b)` helper: min(len(a), len(b)) pairs, `abs(a[i] - b[i]) / max(abs(a[i]), 0.0001) * 100`. Max of bid_diff and ask_diff. Action: < 10% → "ok", < divergence_pct → "warn", else → "switch".

### Verifier
- `_buckets`: dict[int, Bucket] keyed by 1s-granularity timestamp.
- `feed_primary/feed_secondary(msg)`: extract ts from `int(msg["timestamp"]) // 1000`, create/append to bucket.
- `tick()`: find buckets where `ts < now_sec - 2` AND both primary and secondary have data. Sort by ts, pop each, call compare. Return worst verdict (switch > warn > ok). Increment `_warn_count` on each warn verdict.
- `is_degraded`: `_warn_count > 300` (~5 min at 1 tick/s).

## Test Code

Write `tests/test_ws_wallet/test_verifier.py` with these tests (use `_book_msg` helper):

```python
def _book_msg(bids=None, asks=None, ts_ms=None):
    return {
        "asset_id": "123",
        "bids": bids or [{"price": "100.0", "size": "1.0"}],
        "asks": asks or [{"price": "101.0", "size": "1.0"}],
        "timestamp": str(ts_ms or int(time.time() * 1000)),
    }
```

### TestBucket class:
1. `test_top5_bids_returns_prices` — 3 bids → 3 prices
2. `test_top5_bids_max_five` — 10 bids → 5 prices
3. `test_top5_asks_ascending` — 3 asks → 3 prices
4. `test_compare_identical_returns_none` — same data → None
5. `test_compare_large_divergence_returns_switch` — 100 vs 80 → switch
6. `test_compare_moderate_divergence_returns_warn` — 100 vs 95 → warn
7. `test_empty_bucket_returns_none` — no data → None
8. `test_no_secondary_returns_none` — no secondary → None

### TestVerifier class:
1. `test_feed_and_tick_ok` — same data → None
2. `test_divergent_primary_secondary_triggers_warn` — different data → warn/switch
3. `test_is_degraded_after_many_warns` — _warn_count=301 → True

## Steps

1. Write tests first (they will fail since verifier.py doesn't exist)
2. Run tests: `uv run pytest tests/test_ws_wallet/test_verifier.py -v` → FAIL
3. Implement Verifier/Bucket/Verdict in verifier.py
4. Run tests: `uv run pytest tests/test_ws_wallet/test_verifier.py -v` → PASS
5. Run full suite: `uv run pytest tests/ -v --tb=short` → ALL PASS
6. Commit

## Commit Message
```
feat: add Verifier with RingBuffer bucket comparison
```

## Report Contract

Write report to `/home/debian/pmdata/polymarket-l2-collector/.superpowers/sdd/task-2-report.md`:
1. Status
2. Each step completed
3. Test output
4. Commit hash
5. Concerns
