"""
Unit tests for window metadata tracking.
"""

import json
import tempfile
from pathlib import Path

from polymarket_l2_collector.window_metadata import (
    _metadata_path,
    get_or_create_meta,
    mark_complete,
    mark_failed,
    record_disconnect,
    record_flush,
    scan_data_quality,
    touch_message,
    write_metadata,
)


class TestWindowMeta:
    """Window meta lifecycle."""

    def _clear_meta(self):
        from polymarket_l2_collector import window_metadata
        window_metadata._metadata.clear()

    def setup_method(self):
        self._clear_meta()

    def test_create_and_get(self):
        meta = get_or_create_meta("5m", "btc", "orderbooks", "up", 1765359900)
        assert meta.interval == "5m"
        assert meta.coin == "btc"
        assert meta.data_type == "orderbooks"
        assert meta.direction == "up"
        assert meta.window_ts == 1765359900
        assert meta.message_count == 0
        assert meta.status == "active"

    def test_touch_message_increments_count(self):
        touch_message("5m", "btc", "orderbooks", "up", 1765359900, 1765359900123)
        meta = get_or_create_meta("5m", "btc", "orderbooks", "up", 1765359900)
        assert meta.message_count == 1
        assert meta.first_message_time == 1765359900123
        assert meta.last_message_time == 1765359900123

    def test_touch_message_updates_last(self):
        touch_message("5m", "btc", "orderbooks", "up", 1765359900, 1000)
        touch_message("5m", "btc", "orderbooks", "up", 1765359900, 2000)
        meta = get_or_create_meta("5m", "btc", "orderbooks", "up", 1765359900)
        assert meta.message_count == 2
        assert meta.first_message_time == 1000
        assert meta.last_message_time == 2000

    def test_record_flush(self):
        record_flush("5m", "btc", "orderbooks", "up", 1765359900)
        meta = get_or_create_meta("5m", "btc", "orderbooks", "up", 1765359900)
        assert meta.flush_count == 1

    def test_record_disconnect(self):
        record_disconnect("15m", "eth", "trades", "down", 1765360800)
        meta = get_or_create_meta("15m", "eth", "trades", "down", 1765360800)
        assert meta.disconnect_count == 1

    def test_mark_failed(self):
        mark_failed("5m", "btc", "orderbooks", "up", 1765359900, "connection lost")
        meta = get_or_create_meta("5m", "btc", "orderbooks", "up", 1765359900)
        assert meta.status == "failed"
        assert "connection lost" in meta.error

    def test_mark_complete(self):
        mark_complete("5m", "btc", "orderbooks", "up", 1765359900)
        meta = get_or_create_meta("5m", "btc", "orderbooks", "up", 1765359900)
        assert meta.status == "complete"
        assert "window_end_utc" in meta.__dict__
        assert meta.window_end_utc != ""

    def test_get_or_create_reuses(self):
        meta1 = get_or_create_meta("5m", "btc", "orderbooks", "up", 1765359900)
        meta2 = get_or_create_meta("5m", "btc", "orderbooks", "up", 1765359900)
        assert meta1 is meta2


class TestMetadataWrite:
    """Writing companion .meta.json files."""

    def _clear_meta(self):
        from polymarket_l2_collector import window_metadata
        window_metadata._metadata.clear()

    def setup_method(self):
        self._clear_meta()
        self.tmpdir = tempfile.mkdtemp()

    def _parquet_path(self, name: str) -> str:
        return str(Path(self.tmpdir) / name)

    def test_write_metadata(self):
        pp = self._parquet_path("data/5m/btc/orderbooks/1765359900up.parquet")
        Path(pp).parent.mkdir(parents=True, exist_ok=True)

        # Record some messages first
        touch_message("5m", "btc", "orderbooks", "up", 1765359900, 1765359900123)
        record_flush("5m", "btc", "orderbooks", "up", 1765359900)
        mark_complete("5m", "btc", "orderbooks", "up", 1765359900)

        write_metadata(pp)
        mp = _metadata_path(pp)
        assert Path(mp).exists()

        with open(mp) as f:
            data = json.load(f)
        assert data["interval"] == "5m"
        assert data["coin"] == "btc"
        assert data["message_count"] == 1
        assert data["flush_count"] == 1
        assert data["status"] == "complete"
        assert data["window_ts"] == 1765359900

    def test_write_metadata_empty_window_skipped(self):
        """A window with 0 messages should not create a meta file."""
        pp = self._parquet_path("data/15m/eth/trades/1765360800up.parquet")
        Path(pp).parent.mkdir(parents=True, exist_ok=True)

        write_metadata(pp)
        mp = _metadata_path(pp)
        assert not Path(mp).exists()

    def test_metadata_path_helper(self):
        pp = "data/5m/btc/orderbooks/1765359900up.parquet"
        mp = _metadata_path(pp)
        assert mp == "data/5m/btc/orderbooks/1765359900up.meta.json"


class TestDataQualityScan:
    """Data quality scan tool."""

    def _clear_meta(self):
        from polymarket_l2_collector import window_metadata
        window_metadata._metadata.clear()

    def setup_method(self):
        self._clear_meta()
        self.tmpdir = Path(tempfile.mkdtemp())

    def test_scan_empty_directory(self):
        report = scan_data_quality(str(self.tmpdir))
        assert report["empty_files"] == []
        assert report["missing_meta"] == []

    def test_scan_finds_empty_files(self):
        """An empty parquet file should be reported."""
        pp = self.tmpdir / "5m" / "btc" / "orderbooks"
        pp.mkdir(parents=True)
        import pandas as pd
        df = pd.DataFrame()
        parq_path = str(pp / "1765359900up.parquet")
        df.to_parquet(parq_path, engine="pyarrow")

        report = scan_data_quality(str(self.tmpdir))
        matching = [f for f in report["empty_files"] if "1765359900up.parquet" in f]
        assert len(matching) >= 1
