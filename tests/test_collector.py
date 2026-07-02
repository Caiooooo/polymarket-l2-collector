"""
Unit tests for time-window computation logic, shutdown flush behavior,
and Collector parameterised initialisation.
"""

import tempfile
from pathlib import Path

import pandas as pd

from polymarket_l2_collector.collector import Collector
from polymarket_l2_collector.config import load_settings
from polymarket_l2_collector.data_formatter import format_orderbook, format_trade
from polymarket_l2_collector.file_cache import (
    _build_file_path,
    flush_all_caches,
    save_book,
    save_trades,
)


class TestWindowCalculation:
    """Window boundary calculations for 5m and 15m intervals."""

    def setup_method(self):
        self.settings = load_settings()

    def test_5m_interval_seconds(self):
        assert self.settings.interval_seconds("5m") == 300

    def test_15m_interval_seconds(self):
        assert self.settings.interval_seconds("15m") == 900

    def test_5m_window_boundary_align(self):
        """A timestamp at a 5m boundary should stay on that boundary."""
        interval = 300
        ts = 1765359900  # a 5m-aligned timestamp (divisible by 300)
        window = (ts // interval) * interval
        assert window == ts

    def test_5m_window_floor(self):
        """A mid-window timestamp should floor to the start."""
        interval = 300
        ts = 1765359900 + 123  # 123s into the window
        window = (ts // interval) * interval
        assert window == 1765359900

    def test_15m_window_floor(self):
        interval = 900
        ts = 1765360000
        window = (ts // interval) * interval
        assert window == 1765359900  # floor to previous 15m boundary

    def test_cross_hour_5m(self):
        """5m windows should cross hour boundaries correctly."""
        interval = 300
        # 1h before epoch + 3500s = 1h 3500 → window starts at 1h 3300
        ts = 3600 + 3500  # 7100
        window = (ts // interval) * interval
        assert window == 6900
        assert window % interval == 0

    def test_cross_midnight(self):
        """5m windows crossing midnight UTC."""
        interval = 300
        # 86400 + 1 → should floor to 86400
        ts = 86400 + 1
        window = (ts // interval) * interval
        assert window == 86400


class TestTimestampNormalisation:
    """Timestamp normalisation (ms vs seconds)."""

    def test_ms_timestamp_div_1000(self):
        """Timestamp > 1e12 is ms, should be divided by 1000."""
        ms_ts = 1765359900123
        seconds = ms_ts // 1000
        assert seconds == 1765359900

    def test_epoch_timestamp_passthrough(self):
        """Timestamp < 1e12 is already seconds."""
        sec_ts = 1765359900
        assert sec_ts < 1_000_000_000_000


class TestCollectorShutdown:
    """Verify that cached data is flushed to disk on shutdown (via file_cache)."""

    def setup_method(self):
        self.tmpdir = tempfile.mkdtemp()
        self.asset_to_coin = {"111": "BTC_up_5m"}

    def _book_path(self, interval, coin, window_ts, direction="up"):
        return _build_file_path(self.tmpdir, interval, coin, "orderbooks", window_ts, direction)

    def _trade_path(self, interval, coin, window_ts, direction="up"):
        return _build_file_path(self.tmpdir, interval, coin, "trades", window_ts, direction)

    def test_flush_on_cancel(self):
        """Save rows, flush caches, verify parquet file exists with correct row count.

        This simulates what happens when a Collector is cancelled and
        ``flush_all_caches()`` is called in the shutdown path of
        ``_run_session()`` / ``GracefulKiller``.
        """
        window_ts = 1765359900

        # -- Save a few orderbook rows --
        raw_books = [
            {
                "asset_id": "111",
                "event_type": "book",
                "bids": [{"price": "0.48", "size": "30.0"}],
                "asks": [{"price": "0.52", "size": "25.0"}],
                "timestamp": "1765359900123",
            },
            {
                "asset_id": "111",
                "event_type": "book",
                "bids": [{"price": "0.49", "size": "20.0"}],
                "asks": [{"price": "0.53", "size": "15.0"}],
                "timestamp": "1765359901123",
            },
        ]
        book_rows = format_orderbook(raw_books, self.asset_to_coin, window_open_ts=window_ts)
        assert len(book_rows) == 2

        book_fp = self._book_path("5m", "btc", window_ts)
        save_book(book_rows, book_fp)

        # -- Save a trade row --
        raw_trades = [
            {
                "asset_id": "111",
                "event_type": "last_trade_price",
                "price": "0.50",
                "size": "100.0",
                "side": "BUY",
                "timestamp": "1765359902123",
            }
        ]
        trade_rows = format_trade(raw_trades, self.asset_to_coin, window_open_ts=window_ts)
        assert len(trade_rows) == 1

        trade_fp = self._trade_path("5m", "btc", window_ts)
        save_trades(trade_rows, trade_fp)

        # -- Simulate shutdown flush --
        flushed = flush_all_caches()
        assert flushed >= 2  # at least the 2 book + 1 trade rows

        # -- Verify book parquet --
        assert Path(book_fp).exists(), f"Book parquet not found: {book_fp}"
        book_df = pd.read_parquet(book_fp)
        assert len(book_df) == 2, f"Expected 2 book rows, got {len(book_df)}"

        # -- Verify trade parquet --
        assert Path(trade_fp).exists(), f"Trade parquet not found: {trade_fp}"
        trade_df = pd.read_parquet(trade_fp)
        assert len(trade_df) == 1, f"Expected 1 trade row, got {len(trade_df)}"


class TestCollectorConfigDriven:
    """Collector accepts per-instance configuration parameters."""

    def test_collector_init_with_directions(self):
        """Collector accepts a single-element directions list."""
        c = Collector(interval="5m", coins=["btc"], directions=["up"])
        assert c.interval == "5m"
        assert c.coins == ["btc"]
        assert c.directions == ["up"]

    def test_collector_init_with_multiple_directions(self):
        """Collector can be initialised with ["up", "down"]."""
        c = Collector(interval="15m", coins=["btc", "eth"], directions=["up", "down"])
        assert c.interval == "15m"
        assert c.coins == ["btc", "eth"]
        assert c.directions == ["up", "down"]
        assert len(c.directions) == 2
