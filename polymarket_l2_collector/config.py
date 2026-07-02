"""
Centralised configuration loaded from environment variables.

Allows every component to be configured via .env or environment without
hard-coded globals scattered across modules.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _csv_list(value: str | None, default: list[str]) -> list[str]:
    if not value:
        return default
    return [v.strip().lower() for v in value.split(",") if v.strip()]


@dataclass
class Settings:
    # ── Coins & intervals ──────────────────────────────────────────
    coins: list[str] = field(default_factory=lambda: _csv_list(os.getenv("COINS"), ["btc", "eth"]))
    intervals: list[str] = field(default_factory=lambda: _csv_list(os.getenv("INTERVALS"), ["5m", "15m"]))
    directions: list[str] = field(default_factory=lambda: _csv_list(os.getenv("DIRECTIONS"), ["up"]))

    # ── Polymarket WebSocket ───────────────────────────────────────
    ws_url: str = os.getenv("WS_URL", "wss://ws-subscriptions-clob.polymarket.com/ws/market")
    ws_max_size: int = int(os.getenv("WS_MAX_SIZE", "524288"))  # 512 KB

    # ── Data paths ─────────────────────────────────────────────────
    data_dir: str = os.getenv("DATA_DIR", "data")

    # ── Flush thresholds ───────────────────────────────────────────
    flush_threshold_trades: int = int(os.getenv("FLUSH_THRESHOLD_TRADES", "50"))
    flush_threshold_book: int = int(os.getenv("FLUSH_THRESHOLD_BOOK", "30"))
    max_cached_windows: int = int(os.getenv("MAX_CACHED_WINDOWS", "30"))

    # ── Logging ────────────────────────────────────────────────────
    log_level: str = os.getenv("LOG_LEVEL", "INFO")

    # ── Memory protection ──────────────────────────────────────────
    memory_soft_limit_mb: int = int(os.getenv("MEMORY_SOFT_LIMIT_MB", "300"))
    memory_hard_limit_mb: int = int(os.getenv("MEMORY_HARD_LIMIT_MB", "400"))

    # ── Health check ───────────────────────────────────────────────
    binance_stale_seconds: int = int(os.getenv("BINANCE_STALE_SECONDS", "300"))
    poly_ws_stale_seconds: int = int(os.getenv("POLY_WS_STALE_SECONDS", "600"))
    health_check_interval: int = int(os.getenv("HEALTH_CHECK_INTERVAL", "30"))

    # ── Daily restart ──────────────────────────────────────────────
    restart_hour: int = int(os.getenv("RESTART_HOUR", "3"))
    restart_minute: int = int(os.getenv("RESTART_MINUTE", "0"))

    # ── Wallet / Dual-WS ──────────────────────────────────────────
    wallet_primary_timeout: int = int(os.getenv("WALLET_PRIMARY_TIMEOUT", "60"))
    wallet_secondary_timeout: int = int(os.getenv("WALLET_SECONDARY_TIMEOUT", "120"))
    wallet_verify_interval: float = float(os.getenv("WALLET_VERIFY_INTERVAL", "1.0"))
    wallet_switch_on_divergence: float = float(os.getenv("WALLET_SWITCH_ON_DIVERGENCE", "50.0"))

    # ── Chain verify ──────────────────────────────────────────────
    chain_verify_enabled: bool = os.getenv("CHAIN_VERIFY_ENABLED", "false").lower() in ("1", "true", "yes")

    # ── Derived helpers ────────────────────────────────────────────
    @property
    def data_path(self) -> Path:
        return Path(self.data_dir)

    def interval_seconds(self, interval: str) -> int:
        """Convert interval string ('5m' / '15m') to seconds."""
        mapping = {"5m": 5 * 60, "15m": 15 * 60, "1h": 60 * 60}
        return mapping[interval]


_settings: Settings | None = None


def load_settings() -> Settings:
    """Load (or return cached) settings.

    Call once at startup.  Subsequent calls return the same instance.
    """
    global _settings
    if _settings is None:
        # Try loading .env via python-dotenv if available
        try:
            from dotenv import load_dotenv

            load_dotenv()
        except ImportError:
            pass
        _settings = Settings()
    return _settings
