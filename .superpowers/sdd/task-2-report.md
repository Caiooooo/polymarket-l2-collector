# Task 2 Report: Verifier

## Status
COMPLETE

## Steps Completed
1. Read task brief (`task-2-brief.md`) and existing project structure
2. Wrote tests first in `tests/test_ws_wallet/test_verifier.py` (11 tests)
3. Ran tests -- failed as expected (verifier.py missing, __init__.py had broken import)
4. Fixed `polymarket_l2_collector/ws_wallet/__init__.py` to export Verifier classes instead of WalletService
5. Implemented `polymarket_l2_collector/ws_wallet/verifier.py` with:
   - `Verdict` dataclass (ts_sec, max_bid_diff_pct, max_ask_diff_pct, action)
   - `Bucket` dataclass with `top5_prices()`, `_max_diff()`, and `compare()` methods
   - `Verifier` class with `feed_primary()`, `feed_secondary()`, `tick()`, and `is_degraded`
6. Ran all 11 tests -- all passed
7. Ran full test suite (99 tests) -- all passed
8. Committed with message `feat: add Verifier with RingBuffer bucket comparison`

## Test Output
```
$ uv run pytest tests/test_ws_wallet/test_verifier.py -v
============================= test session starts ==============================
...
tests/test_ws_wallet/test_verifier.py::TestBucket::test_top5_bids_returns_prices PASSED [  9%]
tests/test_ws_wallet/test_verifier.py::TestBucket::test_top5_bids_max_five PASSED [ 18%]
tests/test_ws_wallet/test_verifier.py::TestBucket::test_top5_asks_ascending PASSED [ 27%]
tests/test_ws_wallet/test_verifier.py::TestBucket::test_compare_identical_returns_ok_verdict PASSED [ 36%]
tests/test_ws_wallet/test_verifier.py::TestBucket::test_compare_large_divergence_returns_switch PASSED [ 45%]
tests/test_ws_wallet/test_verifier.py::TestBucket::test_compare_moderate_divergence_returns_warn PASSED [ 54%]
tests/test_ws_wallet/test_verifier.py::TestBucket::test_empty_bucket_returns_none PASSED [ 63%]
tests/test_ws_wallet/test_verifier.py::TestBucket::test_no_secondary_returns_none PASSED [ 72%]
tests/test_ws_wallet/test_verifier.py::TestVerifier::test_feed_and_tick_ok PASSED [ 81%]
tests/test_ws_wallet/test_verifier.py::TestVerifier::test_divergent_primary_secondary_triggers_warn PASSED [ 90%]
tests/test_ws_wallet/test_verifier.py::TestVerifier::test_is_degraded_after_many_warns PASSED [100%]

============================== 11 passed in 0.02s ==============================
```

Full suite: 99 passed in 1.20s.

## Commit Hash
`b7c595a`

## Concerns
- `test_top5_asks_ascending` description says "ascending" but the implementation just takes top5 in presentation order (which for asks should be ascending by convention, but the spec says "we just take top 5 as-is"). The test name matches the brief's intent.
- `test_wallet.py` had a stray whitespace change (blank line added) presumably from a previous editor; reverted before commit.
- The brief test name `test_compare_identical_returns_none` was misleading -- identical data returns a Verdict with action="ok", not None (None is only for empty primary/secondary). Renamed to `test_compare_identical_returns_ok_verdict` in implementation to accurately reflect behavior.
- A "warn" verdict increments `_warn_count` inside `tick()` but an "ok" or "switch" verdict does not. This is per the spec.

## Files Created/Modified
- `polymarket_l2_collector/ws_wallet/verifier.py` (created)
- `polymarket_l2_collector/ws_wallet/__init__.py` (modified)
- `tests/test_ws_wallet/test_verifier.py` (created)
