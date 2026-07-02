"""ChainVerifyWorker — offline HyperSync chain verification.

Compares WS-collected trades against on-chain OrderFilled events using
HyperSync.  This module is OPTIONAL — it gracefully returns empty results
when the hypersync package is not installed or HYPERSYNC_API is unset.
"""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from polymarket_l2_collector.config import load_settings

# ── Constants ────────────────────────────────────────────────────────────────
_POLYGON_BLOCK_TIME = 2.0
_CTF_EXCHANGE_V2 = "0xe111180000d2663c0091e4f400237545b87b996b"
_CONFIRMATION_BUFFER = 120


class ChainVerifyWorker:
    """Offline verification worker that compares WS data against on-chain events."""

    def __init__(self, enabled: bool | None = None):
        if enabled is None:
            enabled = load_settings().chain_verify_enabled
        self._enabled = enabled

    # ── Public API ───────────────────────────────────────────────────────────

    async def verify_window(
        self,
        interval: str,
        coin: str,
        data_type: str,
        direction: str,
        window_ts: int,
        parquet_path: str,
    ) -> dict[str, Any] | None:
        """Verify a single window against on-chain data.

        Returns None when verification is disabled or the window is too
        recent (within the confirmation buffer).  Otherwise returns a
        dict with comparison metrics.
        """
        if not self._enabled:
            return None

        window_end = window_ts + self._parse_interval_seconds(interval)
        now = int(time.time())
        if now - window_end < _CONFIRMATION_BUFFER:
            return None

        ws_count = self._count_parquet_trades(parquet_path)
        onchain_trades = await self._query_hypersync(window_ts, window_end, coin)
        onchain_count = len(onchain_trades)

        if onchain_count > 0:
            completeness = min(ws_count / onchain_count, 1.0) * 100
        else:
            completeness = 0.0

        status = "verified" if completeness >= 90 else "incomplete"

        # Compute on-chain total USD (simplified: all amounts are "0" for now)
        onchain_total = sum(float(t.get("usd_amount", 0)) for t in onchain_trades)

        result: dict[str, Any] = {
            "ws_trade_count": ws_count,
            "onchain_trade_count": onchain_count,
            "completeness_pct": completeness,
            "ws_total_usd": 0.0,
            "onchain_total_usd": onchain_total,
            "verified_at": self._iso_now(),
            "status": status,
        }

        self._write_chain_meta(parquet_path, result)
        return result

    # ── Internal helpers ────────────────────────────────────────────────────

    @staticmethod
    def _parse_interval_seconds(interval: str) -> int:
        """Convert '5m', '15m' or '1h' to seconds."""
        mapping = {"5m": 300, "15m": 900, "1h": 3600}
        return mapping.get(interval, 300)

    @staticmethod
    def _iso_now() -> str:
        return datetime.now(timezone.utc).isoformat() + "Z"

    @staticmethod
    def _count_parquet_trades(path: str) -> int:
        """Count rows in a parquet file.  Returns 0 on any failure."""
        try:
            df = pd.read_parquet(path)
            return len(df)
        except Exception:
            return 0

    @staticmethod
    async def _query_hypersync(
        window_ts: int,
        window_end: int,
        coin: str,  # noqa: ARG004 — placeholder for future filtering
    ) -> list[dict[str, Any]]:
        """Query HyperSync for OrderFilled events in the given window.

        Simplified implementation: returns [] when the hypersync package
        is not installed, ``HYPERSYNC_API`` is not set, or on any error.
        """
        # fmt: off
        try:
            import hypersync  # noqa: F401
            from eth_utils import event_abi_to_log_topic  # noqa: F401
            from hypersync import (
                ColumnMapping,
                DataType,
                LogSelection,
                LogSelectionRequest,
                TransactionField,
            )
            from hypersync.client import HyperSyncClient
        except ImportError:
            return []

        api_url = os.environ.get("HYPERSYNC_API")
        if not api_url:
            return []

        try:
            from_block = max(0, int(window_ts / 2.0) - 10_000_000)
            to_block = from_block + int((window_end - window_ts) / 2.0) + 100

            client = HyperSyncClient(api_url)

            # OrderFilled event topic for CTF Exchange V2
            order_filled_abi = {
                "anonymous": False,
                "inputs": [
                    {"indexed": False, "internalType": "address", "name": "maker", "type": "address"},
                    {"indexed": False, "internalType": "address", "name": "taker", "type": "address"},
                    {"indexed": False, "internalType": "uint256", "name": "makerAmount", "type": "uint256"},
                    {"indexed": False, "internalType": "uint256", "name": "takerAmount", "type": "uint256"},
                ],
                "name": "OrderFilled",
                "type": "event",
            }
            topic = event_abi_to_log_topic(order_filled_abi)

            selectors = [
                LogSelection(
                    address=[_CTF_EXCHANGE_V2],
                    topics=[topic],
                )
            ]

            query = LogSelectionRequest(
                from_block=from_block,
                to_block=to_block,
                logs=selectors,
                field_selection=TransactionField(
                    block_number=True,
                    from_address=True,
                ),
                column_mapping=ColumnMapping(
                    decoded_log={
                        "maker": DataType.ADDRESS,
                        "taker": DataType.ADDRESS,
                        "makerAmount": DataType.INT256,
                        "takerAmount": DataType.INT256,
                    },
                ),
                decode_logs=True,
            )

            result = client.get_logs(query)
            logs_data: list[dict[str, Any]] = []
            for log in result.data.logs:
                logs_data.append({
                    "usd_amount": "0",  # simplified
                    "block_number": log.block_number,
                })
            return logs_data

        except Exception:
            return []

    @staticmethod
    def _write_chain_meta(parquet_path: str, result: dict[str, Any]) -> None:
        """Append chain_verify metadata to the companion .meta.json file.

        Reads existing meta if present, adds a ``chain_verify`` key, and
        writes back.  Silently ignores any IO or serialisation error.
        """
        try:
            meta_path = Path(parquet_path).with_suffix(".meta.json")
            meta: dict[str, Any] = {}
            if meta_path.exists():
                with open(meta_path) as f:
                    meta = json.load(f)
            meta["chain_verify"] = result
            with open(meta_path, "w") as f:
                json.dump(meta, f, indent=2)
        except Exception:
            pass
