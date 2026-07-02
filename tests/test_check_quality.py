"""
Tests for data quality check module.
"""

import tempfile
from pathlib import Path

from polymarket_l2_collector.check_quality import scan_data_quality


def test_check_quality_no_data():
    """Scanning an empty directory should return empty reports."""
    with tempfile.TemporaryDirectory() as td:
        report = scan_data_quality(td)
        assert report["empty_files"] == []
        assert report["missing_meta"] == []
        assert report["failed_windows"] == []


def test_check_quality_healthy_file():
    """A healthy parquet file with companion meta should pass."""
    import json

    import pandas as pd

    with tempfile.TemporaryDirectory() as td:
        # Create valid parquet
        pdir = Path(td) / "5m" / "btc" / "orderbooks"
        pdir.mkdir(parents=True)
        pp = pdir / "1765359900up.parquet"

        df = pd.DataFrame({"col": [1, 2, 3]})
        df.to_parquet(str(pp))

        # Create companion meta
        meta = {
            "interval": "5m", "coin": "btc",
            "data_type": "orderbooks", "direction": "up",
            "window_ts": 1765359900,
            "message_count": 3, "status": "complete",
        }
        mp = pdir / "1765359900up.meta.json"
        mp.write_text(json.dumps(meta))

        report = scan_data_quality(td)
        assert report["empty_files"] == []
        assert report["missing_meta"] == []
        assert report["zero_message_meta"] == []


def test_check_quality_empty_parquet():
    """Empty parquet files should be flagged."""
    import pandas as pd

    with tempfile.TemporaryDirectory() as td:
        pdir = Path(td) / "5m" / "btc" / "orderbooks"
        pdir.mkdir(parents=True)
        pp = pdir / "1765359900up.parquet"
        pd.DataFrame().to_parquet(str(pp))

        report = scan_data_quality(td)
        assert any("1765359900up.parquet" in f for f in report["empty_files"])


def test_check_quality_missing_meta():
    """Parquet without companion meta should be flagged."""
    import pandas as pd

    with tempfile.TemporaryDirectory() as td:
        pdir = Path(td) / "5m" / "btc" / "orderbooks"
        pdir.mkdir(parents=True)
        pp = pdir / "1765359900up.parquet"
        pd.DataFrame({"a": [1]}).to_parquet(str(pp))

        report = scan_data_quality(td)
        assert any("1765359900up.parquet" in f for f in report["missing_meta"])


def test_check_quality_failed_window():
    """A .meta.json with status 'failed' should be flagged."""
    import json

    import pandas as pd

    with tempfile.TemporaryDirectory() as td:
        pdir = Path(td) / "5m" / "btc" / "orderbooks"
        pdir.mkdir(parents=True)

        # Parquet
        pp = pdir / "1765359900up.parquet"
        pd.DataFrame({"a": [1]}).to_parquet(str(pp))

        # Meta with failed status
        meta = {"interval": "5m", "status": "failed", "error": "WS disconnect",
                "message_count": 5, "window_ts": 1765359900,
                "coin": "btc", "data_type": "orderbooks", "direction": "up"}
        mp = pdir / "1765359900up.meta.json"
        mp.write_text(json.dumps(meta))

        report = scan_data_quality(td)
        assert any("1765359900up.parquet" in f for f in report["failed_windows"])
