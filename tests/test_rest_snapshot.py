"""
Tests for REST snapshot backfill module.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pandas as pd
import pytest

from polymarket_l2_collector.rest_snapshot import (
    _GAP_PATTERN,
    _missing_timestamps,
    _parse_gap_message,
    backfill_all_gaps,
    backfill_gaps,
    fetch_snapshot,
)


class TestParseGapMessage:
    """Parsing gap-window strings from scan_data_quality."""

    def test_parse_valid(self):
        msg = (
            "interval=5m coin=btc type=orderbooks direction=up: "
            "gap between 1000 and 1600 "
            "(delta=600s, expected=300s, tolerance=60s, ~1 window(s) missing)"
        )
        result = _parse_gap_message(msg)
        assert result is not None
        assert result["interval"] == "5m"
        assert result["coin"] == "btc"
        assert result["data_type"] == "orderbooks"
        assert result["direction"] == "up"
        assert result["start_ts"] == 1000
        assert result["end_ts"] == 1600
        assert result["expected_seconds"] == 300

    def test_parse_unicode_equals(self):
        """The report may use '≈' in the expected field."""
        msg = (
            "interval=15m coin=eth type=orderbooks direction=down: "
            "gap between 2000 and 3000 "
            "(delta=1000s, expected≈900s, tolerance=60s, ~1 window(s) missing)"
        )
        result = _parse_gap_message(msg)
        assert result is not None
        assert result["interval"] == "15m"
        assert result["coin"] == "eth"
        assert result["expected_seconds"] == 900

    def test_parse_no_match(self):
        assert _parse_gap_message("not a gap message") is None
        assert _parse_gap_message("") is None

    def test_pattern_direct(self):
        """Direct regex test for robustness."""
        msg = (
            "interval=5m coin=btc type=orderbooks direction=up: "
            "gap between 1000 and 1600 "
            "(delta=600s, expected=300s, tolerance=60s, ~1 window(s) missing)"
        )
        m = _GAP_PATTERN.search(msg)
        assert m is not None
        assert m.group("interval") == "5m"
        assert m.group("coin") == "btc"
        assert m.group("start_ts") == "1000"
        assert m.group("end_ts") == "1600"
        assert m.group("expected") == "300"


class TestMissingTimestamps:
    """Computing the list of missing window start timestamps."""

    def test_one_missing(self):
        # gap between 1000 and 1600 with 300s interval -> one at 1300
        assert _missing_timestamps(1000, 1600, 300) == [1300]

    def test_two_missing(self):
        # gap between 1000 and 1900 with 300s interval -> 1300, 1600
        assert _missing_timestamps(1000, 1900, 300) == [1300, 1600]

    def test_adjacent_no_missing(self):
        # 1000 and 1300 are adjacent (1000 + 300 = 1300)
        assert _missing_timestamps(1000, 1300, 300) == []

    def test_15m_interval(self):
        # gap between 2000 and 3800 with 900s interval -> 2900
        assert _missing_timestamps(2000, 3800, 900) == [2900]


@pytest.mark.asyncio
async def test_fetch_snapshot_mocked():
    """Mock aiohttp response and verify parsing."""
    raw_json = {
        "bids": [["0.48", "30"], ["0.47", "50"]],
        "asks": [["0.52", "25"], ["0.53", "40"]],
    }

    # Mock response — returned by __aenter__
    mock_resp = AsyncMock()
    mock_resp.status = 200
    mock_resp.json = AsyncMock(return_value=raw_json)

    # session.get() returns an async context manager
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=mock_resp)
    cm.__aexit__ = AsyncMock(return_value=None)

    mock_session = AsyncMock(spec_set=["get"])
    mock_session.get = MagicMock(return_value=cm)

    result = await fetch_snapshot("test_asset_123", session=mock_session)

    assert result is not None
    assert result["bids"] == [["0.48", "30"], ["0.47", "50"]]
    assert result["asks"] == [["0.52", "25"], ["0.53", "40"]]
    assert isinstance(result["timestamp"], int)
    assert result["timestamp"] > 0

    mock_session.get.assert_called_once_with(
        "https://clob.polymarket.com/orderbook?asset_id=test_asset_123"
    )


@pytest.mark.asyncio
async def test_fetch_snapshot_non_200():
    """Non-200 status should return None."""
    mock_resp = AsyncMock()
    mock_resp.status = 404

    cm = MagicMock()
    cm.__aenter__ = AsyncMock(return_value=mock_resp)
    cm.__aexit__ = AsyncMock(return_value=None)

    mock_session = AsyncMock(spec_set=["get"])
    mock_session.get = MagicMock(return_value=cm)

    result = await fetch_snapshot("missing_asset", session=mock_session)
    assert result is None


@pytest.mark.asyncio
async def test_fetch_snapshot_exception():
    """Network exception should return None."""
    cm = MagicMock()
    cm.__aenter__ = AsyncMock(side_effect=Exception("Connection refused"))
    cm.__aexit__ = AsyncMock(return_value=None)

    mock_session = AsyncMock(spec_set=["get"])
    mock_session.get = MagicMock(return_value=cm)

    result = await fetch_snapshot("bad", session=mock_session)
    assert result is None


class TestBackfillGapsDryRun:
    """Dry-run backfill with known gaps."""

    @pytest.fixture(autouse=True)
    def _setup_environment(self):
        """Patch load_settings to return a deterministic config."""
        patcher_settings = patch(
            "polymarket_l2_collector.rest_snapshot.load_settings"
        )
        mock_load = patcher_settings.start()
        mock_settings = MagicMock()
        mock_settings.interval_seconds.return_value = 300
        mock_settings.intervals = ["5m"]
        mock_settings.coins = ["btc"]
        mock_settings.directions = ["up"]
        mock_load.return_value = mock_settings
        self._patcher_settings = patcher_settings

        # Patch time to be deterministic (far in the future so windows are "complete")
        patcher_time = patch("polymarket_l2_collector.rest_snapshot.time")
        mock_time = patcher_time.start()
        mock_time.time.return_value = 100000000
        self._patcher_time = patcher_time

        yield

        self._patcher_settings.stop()
        self._patcher_time.stop()

    @pytest.mark.asyncio
    async def test_dry_run_reports_correct_windows(self):
        """With a gap between 1000 and 1600, dry-run should report the missing window."""
        with tempfile.TemporaryDirectory() as td:
            pdir = Path(td) / "5m" / "btc" / "orderbooks"
            pdir.mkdir(parents=True)

            for ts in [1000, 1600]:
                pp = pdir / f"{ts}up.parquet"
                pd.DataFrame({"col": [1]}).to_parquet(str(pp))
                meta = {
                    "interval": "5m",
                    "coin": "btc",
                    "data_type": "orderbooks",
                    "direction": "up",
                    "window_ts": ts,
                    "message_count": 1,
                    "status": "complete",
                }
                mp = pdir / f"{ts}up.meta.json"
                mp.write_text(json.dumps(meta))

            results = await backfill_gaps(
                data_dir=td,
                interval="5m",
                coin="btc",
                direction="up",
                dry_run=True,
            )

            dry_run_entries = [r for r in results if r["status"] == "dry_run_would_fetch"]
            assert len(dry_run_entries) == 1
            assert dry_run_entries[0]["window_ts"] == 1300
            assert dry_run_entries[0]["interval"] == "5m"
            assert dry_run_entries[0]["coin"] == "btc"
            assert dry_run_entries[0]["direction"] == "up"

    @pytest.mark.asyncio
    async def test_no_gaps_no_op(self):
        """With adjacent windows, no gaps should be reported."""
        with tempfile.TemporaryDirectory() as td:
            pdir = Path(td) / "5m" / "btc" / "orderbooks"
            pdir.mkdir(parents=True)

            for ts in [1000, 1300]:
                pp = pdir / f"{ts}up.parquet"
                pd.DataFrame({"col": [1]}).to_parquet(str(pp))
                meta = {
                    "interval": "5m",
                    "coin": "btc",
                    "data_type": "orderbooks",
                    "direction": "up",
                    "window_ts": ts,
                    "message_count": 1,
                    "status": "complete",
                }
                mp = pdir / f"{ts}up.meta.json"
                mp.write_text(json.dumps(meta))

            results = await backfill_gaps(
                data_dir=td,
                interval="5m",
                coin="btc",
                direction="up",
                dry_run=True,
            )

            assert results == []

    @pytest.mark.asyncio
    async def test_skips_window_if_already_present(self):
        """If a gap window already has a parquet file, skip it.

        We use a gap between 1000 and 2200 (so missing windows at 1300, 1600, 1900),
        and mock os.path.exists to return True for one of them.
        """
        with tempfile.TemporaryDirectory() as td:
            pdir = Path(td) / "5m" / "btc" / "orderbooks"
            pdir.mkdir(parents=True)

            # Create windows at 1000 and 2200 (gap with 3 missing windows)
            for ts in [1000, 2200]:
                pp = pdir / f"{ts}up.parquet"
                pd.DataFrame({"col": [1]}).to_parquet(str(pp))
                meta = {
                    "interval": "5m",
                    "coin": "btc",
                    "data_type": "orderbooks",
                    "direction": "up",
                    "window_ts": ts,
                    "message_count": 1,
                    "status": "complete",
                }
                mp = pdir / f"{ts}up.meta.json"
                mp.write_text(json.dumps(meta))

            # Actually create one of the gap windows (1300) so it exists on disk.
            # This makes scan_data_quality see [1000, 1300, 2200], the gap is
            # now between 1300 and 2200, and 1600/1900 are the missing windows.
            # BUT we want the gap to include 1300 as a missing window.
            # So instead of creating 1300 before calling backfill_gaps,
            # let's mock os.path.exists to pretend one of the gap windows exists.
            target_path = str(pdir / "1300up.parquet")

            import os as _real_os
            _real_exists = _real_os.path.exists

            with patch("polymarket_l2_collector.rest_snapshot.os.path.exists") as mock_exists:
                def _exists(path_):
                    if path_ == target_path:
                        return True
                    return _real_exists(path_)

                mock_exists.side_effect = _exists

                results = await backfill_gaps(
                    data_dir=td,
                    interval="5m",
                    coin="btc",
                    direction="up",
                    dry_run=True,
                )

            # Three missing windows: 1300 (skipped), 1600 (dry_run), 1900 (dry_run)
            skipped = [r for r in results if r["status"] == "skipped_already_exists"]
            assert len(skipped) == 1
            assert skipped[0]["window_ts"] == 1300

            fetched = [r for r in results if r["status"] == "dry_run_would_fetch"]
            assert len(fetched) == 2
            assert {r["window_ts"] for r in fetched} == {1600, 1900}

    @pytest.mark.asyncio
    async def test_skips_too_recent_window(self):
        """Windows whose end + 2*interval buffer is in the future should be skipped."""
        with tempfile.TemporaryDirectory() as td:
            pdir = Path(td) / "5m" / "btc" / "orderbooks"
            pdir.mkdir(parents=True)

            for ts in [1000, 1600]:
                pp = pdir / f"{ts}up.parquet"
                pd.DataFrame({"col": [1]}).to_parquet(str(pp))
                meta = {
                    "interval": "5m",
                    "coin": "btc",
                    "data_type": "orderbooks",
                    "direction": "up",
                    "window_ts": ts,
                    "message_count": 1,
                    "status": "complete",
                }
                mp = pdir / f"{ts}up.meta.json"
                mp.write_text(json.dumps(meta))

            # Patch time to be BEFORE the 1300 window's completion buffer
            # Window 1300 ends at 1300 + 300 = 1600
            # With buffer 2*300 = 600, the window is complete at 1600 + 600 = 2200
            # Set current time to 2000 — the window is NOT old enough yet.
            with patch("polymarket_l2_collector.rest_snapshot.time.time") as mock_time:
                mock_time.return_value = 2000

                results = await backfill_gaps(
                    data_dir=td,
                    interval="5m",
                    coin="btc",
                    direction="up",
                    dry_run=True,
                )

            too_recent = [r for r in results if r["status"] == "skipped_too_recent"]
            assert len(too_recent) == 1
            assert too_recent[0]["window_ts"] == 1300


class TestBackfillGapsNoGaps:
    """Backfill on a directory with no gaps should do nothing."""

    @pytest.fixture(autouse=True)
    def _setup_environment(self):
        patcher = patch("polymarket_l2_collector.rest_snapshot.load_settings")
        mock_load = patcher.start()
        mock_settings = MagicMock()
        mock_settings.interval_seconds.return_value = 300
        mock_load.return_value = mock_settings
        self._patcher = patcher
        yield
        self._patcher.stop()

    @pytest.mark.asyncio
    async def test_backfill_gaps_no_gaps(self):
        """Run on a directory with no gaps, verify it does nothing."""
        with tempfile.TemporaryDirectory() as td:
            pdir = Path(td) / "5m" / "btc" / "orderbooks"
            pdir.mkdir(parents=True)

            for ts in [1000, 1300]:
                pp = pdir / f"{ts}up.parquet"
                pd.DataFrame({"col": [1]}).to_parquet(str(pp))
                meta = {
                    "interval": "5m",
                    "coin": "btc",
                    "data_type": "orderbooks",
                    "direction": "up",
                    "window_ts": ts,
                    "message_count": 1,
                    "status": "complete",
                }
                mp = pdir / f"{ts}up.meta.json"
                mp.write_text(json.dumps(meta))

            results = await backfill_gaps(
                data_dir=td,
                interval="5m",
                coin="btc",
                direction="up",
                dry_run=True,
            )

            assert results == []


@pytest.mark.asyncio
async def test_backfill_all_gaps_no_data():
    """backfill_all_gaps on empty directory should return empty dict."""
    with tempfile.TemporaryDirectory() as td:
        results = await backfill_all_gaps(data_dir=td, dry_run=True)
        assert isinstance(results, dict)
        assert len(results) == 0
