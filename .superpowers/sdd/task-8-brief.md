# Task 8: 集成测试 + ruff + CI 验证

## Context

Final check — ensure all tests pass, ruff lint/format clean, CI configuration valid.

## Steps

### Step 1: Run full test suite
```bash
cd /home/debian/pmdata/polymarket-l2-collector
uv run pytest tests/ -v --tb=short 2>&1
```
Expected: ALL PASS (~110+ tests)

### Step 2: Run ruff lint
```bash
uv run ruff check polymarket_l2_collector/ tests/
```
Expected: No errors. If there are autofixable issues: `uv run ruff check --fix polymarket_l2_collector/ tests/`

### Step 3: Run ruff format check
```bash
uv run ruff format --check polymarket_l2_collector/ tests/
```
Expected: Formatted correctly.

### Step 4: Check git status
```bash
git status
```
Expected: Only `.superpowers/` untracked. All source changes committed.

### Step 5: Final commit if needed
```bash
git add -A && git commit -m "chore: finalize dual-WS integration — all tests pass"
```

## Report

Write to `/home/debian/pmdata/polymarket-l2-collector/.superpowers/sdd/task-8-report.md`
