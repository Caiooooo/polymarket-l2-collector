"""
Legacy asset utility module — re-exports from ``market_discovery``.

Kept for backward compatibility with older scripts; new code should
import directly from ``market_discovery``.
"""

from __future__ import annotations

from .market_discovery import resolve_assets

__all__ = ["get_assets", "resolve_assets"]

# Maintain the old async signature for callers that import via asset_utils
get_assets = resolve_assets
