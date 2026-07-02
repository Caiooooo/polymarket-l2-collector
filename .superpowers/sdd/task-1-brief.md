# Task 1: Config 新增字段 + ws_wallet 包脚手架

## Context

This is the first task for adding dual-WS WalletService to polymarket-l2-collector (v0.2.0). We're scaffolding the module structure and adding config fields that all later tasks depend on.

## Files
- Create: `polymarket_l2_collector/ws_wallet/__init__.py`
- Modify: `polymarket_l2_collector/config.py`
- Create: `tests/test_ws_wallet/__init__.py`
- Create: `tests/test_ws_wallet/test_wallet.py`

## Steps

### Step 1: Create package directories
```bash
mkdir -p polymarket_l2_collector/ws_wallet
mkdir -p tests/test_ws_wallet
```

### Step 2: Create package init files

Write `polymarket_l2_collector/ws_wallet/__init__.py`:
```python
"""Dual-WebSocket wallet module — primary/backup WS with data verification."""
from __future__ import annotations
from .wallet import WalletService
__all__ = ["WalletService"]
```

Write `tests/test_ws_wallet/__init__.py` — empty file.

### Step 3: Add wallet config fields to Settings

In `polymarket_l2_collector/config.py`, append these fields inside the `Settings` dataclass before the closing. The existing pattern for simple env-var fields is direct assignment with `os.getenv` (see `ws_url`, `ws_max_size` etc.):

```python
    # ── Wallet / Dual-WS ──────────────────────────────────────────
    wallet_primary_timeout: int = int(os.getenv("WALLET_PRIMARY_TIMEOUT", "60"))
    wallet_secondary_timeout: int = int(os.getenv("WALLET_SECONDARY_TIMEOUT", "120"))
    wallet_verify_interval: float = float(os.getenv("WALLET_VERIFY_INTERVAL", "1.0"))
    wallet_switch_on_divergence: float = float(os.getenv("WALLET_SWITCH_ON_DIVERGENCE", "50.0"))

    # ── Chain verify ──────────────────────────────────────────────
    chain_verify_enabled: bool = os.getenv("CHAIN_VERIFY_ENABLED", "false").lower() in ("1", "true", "yes")
```

### Step 4: Write config defaults test

Write `tests/test_ws_wallet/test_wallet.py`:
```python
"""Tests for WalletService config defaults."""
from __future__ import annotations
from polymarket_l2_collector.config import load_settings


def test_wallet_config_defaults():
    settings = load_settings()
    assert settings.wallet_primary_timeout == 60
    assert settings.wallet_secondary_timeout == 120
    assert settings.wallet_verify_interval == 1.0
    assert settings.wallet_switch_on_divergence == 50.0
    assert settings.chain_verify_enabled is False
```

### Step 5: Run test
```bash
cd /home/debian/pmdata/polymarket-l2-collector
uv run pytest tests/test_ws_wallet/test_wallet.py -v
```
Expected: PASS.

### Step 6: Commit
```bash
git add polymarket_l2_collector/ws_wallet/__init__.py polymarket_l2_collector/config.py tests/test_ws_wallet/
git commit -m "feat: add ws_wallet package scaffold and wallet config fields"
```

## Report Contract

After completing all steps, write a report to `/home/debian/pmdata/polymarket-l2-collector/.superpowers/sdd/task-1-report.md` containing:
1. Status (DONE / DONE_WITH_CONCERNS / NEEDS_CONTEXT / BLOCKED)
2. Each step completed and its result
3. Test output (copy-paste from the terminal)
4. Commit hash
5. Any concerns
