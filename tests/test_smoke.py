"""
Smoke tests — verify the core data pipeline (format → cache → Parquet)
works end-to-end with synthetic messages.

These tests do NOT connect to any real WebSocket or API.
"""

import tempfile
from pathlib import Path

import pandas as pd

from polymarket_l2_collector.data_formatter import format_orderbook, format_trade
from polymarket_l2_collector.file_cache import (
    _build_file_path,
    flush_all_caches,
    restore_from_parquet,
    save_book,
    save_trades,
)


class TestOrderbookPipeline:
    """Format → save → flush → read-back pipeline for orderbook data."""

    def setup_method(self):
        self.tmpdir = tempfile.mkdtemp()
        self.asset_to_coin = {
            "111": "BTC_up_5m",
            "222": "ETH_up_5m",
        }

    def _path(self, interval, coin, window_ts, direction="up"):
        return _build_file_path(self.tmpdir, interval, coin, "orderbooks", window_ts, direction)

    def test_format_and_save_book_single_message(self):
        """Single orderbook message → format → save → flush → readable."""
        raw = [
            {
                "asset_id": "111",
                "event_type": "book",
                "bids": [{"price": "0.48", "size": "30.0"}],
                "asks": [{"price": "0.52", "size": "25.0"}],
                "timestamp": "1765359900123",
            }
        ]
        rows = format_orderbook(raw, self.asset_to_coin, window_open_ts=1765359900)
        assert len(rows) == 1
        assert rows[0]["window_open_ts"] == 1765359900

        fp = self._path("5m", "btc", 1765359900)
        save_book(rows, fp)
        flush_all_caches()

        assert Path(fp).exists()
        df = pd.read_parquet(fp)
        assert len(df) == 1
        # Data is stored compressed (p/s) — restore to compare
        restored = restore_from_parquet(df.to_dict("records"))
        assert restored[0]["bids"][0]["price"] == 0.48


class TestTradePipeline:
    """Format → save → flush → read-back pipeline for trade data."""

    def setup_method(self):
        self.tmpdir = tempfile.mkdtemp()
        self.asset_to_coin = {
            "111": "BTC_up_5m",
        }

    def _path(self, interval, coin, window_ts, direction="up"):
        return _build_file_path(self.tmpdir, interval, coin, "trades", window_ts, direction)

    def test_format_and_save_trade_single_message(self):
        raw = [
            {
                "asset_id": "111",
                "event_type": "last_trade_price",
                "price": "0.50",
                "size": "100.0",
                "side": "BUY",
                "timestamp": "1765359901123",
            }
        ]
        rows = format_trade(raw, self.asset_to_coin, window_open_ts=1765359900)
        assert len(rows) == 1
        assert rows[0]["side"] == "buy"

        fp = self._path("5m", "btc", 1765359900)
        save_trades(rows, fp)
        flush_all_caches()

        assert Path(fp).exists()
        df = pd.read_parquet(fp)
        assert len(df) == 1
        restored = restore_from_parquet(df.to_dict("records"))
        assert restored[0]["price"] == 0.50
        assert restored[0]["size"] == 100.0
        assert restored[0]["side"] == "buy"


class TestAppendPipeline:
    """Multiple flushes to the same window should append, not overwrite."""

    def setup_method(self):
        self.tmpdir = tempfile.mkdtemp()
        self.asset_to_coin = {"111": "BTC_up_5m"}

    def test_append_to_same_window(self):
        fp = _build_file_path(self.tmpdir, "5m", "btc", "orderbooks", 1765359900, "up")

        # First flush: 1 message
        raw1 = [
            {
                "asset_id": "111",
                "bids": [{"price": "0.48", "size": "30.0"}],
                "asks": [{"price": "0.52", "size": "25.0"}],
                "timestamp": "1765359900123",
            }
        ]
        rows1 = format_orderbook(raw1, self.asset_to_coin, window_open_ts=1765359900)
        save_book(rows1, fp)
        flush_all_caches()

        # Second flush: another message
        raw2 = [
            {
                "asset_id": "111",
                "bids": [{"price": "0.49", "size": "20.0"}],
                "asks": [{"price": "0.53", "size": "15.0"}],
                "timestamp": "1765359901123",
            }
        ]
        rows2 = format_orderbook(raw2, self.asset_to_coin, window_open_ts=1765359900)
        save_book(rows2, fp)
        flush_all_caches()

        df = pd.read_parquet(fp)
        assert len(df) == 2
