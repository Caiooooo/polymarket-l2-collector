#!/usr/bin/env python3
"""
Extract asset IDs from Gamma API JSON response files.

Usage:
    python polymarket_l2_collector/extract_asset_id.py response.json
    echo '{"outcomes":["Up","Down"],...}' | python polymarket_l2_collector/extract_asset_id.py
"""

from __future__ import annotations

import json
import sys
from typing import Any


def extract_asset_ids(market_data: Any) -> None:
    """Pretty-print asset IDs from market data."""
    if isinstance(market_data, list):
        if not market_data:
            print("Empty data")
            return
        market_data = market_data[0]

    question = market_data.get("question", "N/A")
    slug = market_data.get("slug", "N/A")
    outcomes_str = market_data.get("outcomes", "[]")
    token_ids_str = market_data.get("clobTokenIds", "[]")

    if isinstance(outcomes_str, str):
        outcomes: list[str] = json.loads(outcomes_str)
    else:
        outcomes = outcomes_str

    if isinstance(token_ids_str, str):
        clob_token_ids: list[str] = json.loads(token_ids_str)
    else:
        clob_token_ids = token_ids_str

    print("=" * 80)
    print(f"Market: {question}")
    print(f"Slug: {slug}")
    print("=" * 80)
    print("\nAsset IDs:\n")
    for outcome, token_id in zip(outcomes, clob_token_ids):
        print(f'  {outcome:6s} = "{token_id[:16]}…{token_id[-8:]}"')

    print("\nConfig snippet:\n")
    print("ASSET_IDS = [")
    for outcome, token_id in zip(outcomes, clob_token_ids):
        print(f'    "{token_id}",  # {outcome}')
    print("]")


def main() -> None:
    if len(sys.argv) > 1:
        with open(sys.argv[1]) as f:
            data = json.load(f)
        extract_asset_ids(data)
    else:
        print("Paste JSON (Ctrl+D to end):")
        data = json.load(sys.stdin)
        extract_asset_ids(data)


if __name__ == "__main__":
    main()
