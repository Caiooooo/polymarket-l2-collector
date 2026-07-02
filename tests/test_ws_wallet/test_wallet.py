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
