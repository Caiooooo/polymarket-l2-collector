"""
Window metadata tracking — every Parquet window gets a companion `.json`
file recording collection quality metrics (message count, timestamps,
disconnection periods, etc.).

This makes it possible to detect data gaps, empty windows, or corrupted
windows without scanning the Parquet files themselves.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from .logger_config import get_logger

logger = get_logger("window_meta")

# In-memory store: key = window cache key → WindowMeta
_metadata: dict[str, WindowMeta] = {}


@dataclass
class DisconnectPeriod:
    """A period where the WebSocket was disconnected."""

    start_ts: float  # Unix timestamp when disconnect was detected
    end_ts: float = 0.0  # Unix timestamp when reconnected; 0 = ongoing
    duration_seconds: float = 0.0


@dataclass
class WindowMeta:
    """Quality metadata for one data window."""

    # Identity
    interval: str  # "5m" | "15m"
    coin: str  # "btc" | "eth"
    data_type: str  # "orderbooks" | "trades"
    direction: str  # "up" | "down"
    window_ts: int  # window start Unix timestamp
    market_slug: str  # e.g. "btc-updown-5m-1765359900"

    # Asset IDs
    asset_id: str  # the Poloymarket token ID for this window
    coin_asset_ids: dict[str, str] = field(default_factory=dict)  # coin → asset_id map

    # Timing
    window_start_utc: str = ""  # ISO-8601
    window_end_utc: str = ""
    first_message_time: float = 0.0  # Unix ms or relative
    last_message_time: float = 0.0

    # Counts
    message_count: int = 0
    flush_count: int = 0  # how many times data was written to disk
    disconnect_count: int = 0

    # Quality
    status: str = "active"  # "active" | "complete" | "failed" | "partial"
    error: str = ""


def _metadata_path(parquet_path: str) -> str:
    """Return the companion metadata path for a Parquet file."""
    return parquet_path.replace(".parquet", ".meta.json")


def _init_meta(
    interval: str,
    coin: str,
    data_type: str,
    direction: str,
    window_ts: int,
    market_slug: str,
    asset_id: str,
) -> WindowMeta:
    """Create a new WindowMeta, store it, and return it."""
    key = f"{interval}/{coin}/{data_type}/{direction}/{window_ts}"
    meta = WindowMeta(
        interval=interval,
        coin=coin,
        data_type=data_type,
        direction=direction,
        window_ts=window_ts,
        market_slug=market_slug,
        asset_id=asset_id,
    )
    _metadata[key] = meta
    return meta


def get_or_create_meta(
    interval: str,
    coin: str,
    data_type: str,
    direction: str,
    window_ts: int,
    market_slug: str = "",
    asset_id: str = "",
) -> WindowMeta:
    """Get existing metadata for a window, or create a new one."""
    key = f"{interval}/{coin}/{data_type}/{direction}/{window_ts}"
    if key in _metadata:
        return _metadata[key]
    return _init_meta(interval, coin, data_type, direction, window_ts, market_slug, asset_id)


def touch_message(
    interval: str,
    coin: str,
    data_type: str,
    direction: str,
    window_ts: int,
    timestamp_ms: float,
) -> None:
    """Record that a message was received for this window."""
    meta = get_or_create_meta(interval, coin, data_type, direction, window_ts)
    meta.message_count += 1
    if meta.first_message_time == 0.0:
        meta.first_message_time = timestamp_ms
    meta.last_message_time = max(meta.last_message_time, timestamp_ms)


def record_flush(interval: str, coin: str, data_type: str, direction: str, window_ts: int) -> None:
    """Increment flush count for a window."""
    meta = get_or_create_meta(interval, coin, data_type, direction, window_ts)
    meta.flush_count += 1


def record_disconnect(interval: str, coin: str, data_type: str, direction: str, window_ts: int) -> None:
    """Increment disconnect count for a window."""
    meta = get_or_create_meta(interval, coin, data_type, direction, window_ts)
    meta.disconnect_count += 1


def mark_failed(interval: str, coin: str, data_type: str, direction: str, window_ts: int, error: str = "") -> None:
    """Mark a window as failed."""
    meta = get_or_create_meta(interval, coin, data_type, direction, window_ts)
    meta.status = "failed"
    if error:
        meta.error = error


def mark_complete(interval: str, coin: str, data_type: str, direction: str, window_ts: int) -> None:
    """Mark a window as complete (window boundary passed, WS switched)."""
    meta = get_or_create_meta(interval, coin, data_type, direction, window_ts)
    meta.status = "complete"
    # Infer window end from start + interval
    gap = {"5m": 300, "15m": 900}.get(interval, 300)
    meta.window_end_utc = _ts_to_iso(meta.window_ts + gap)


def write_metadata(parquet_path: str) -> None:
    """Write the companion ``.meta.json`` for a Parquet file.

    Call this after each flush.  If the meta dict is still empty
    (nothing was recorded), the file is not created.
    """
    parts = parquet_path.replace(".parquet", "").split("/")
    # parts = [data_dir, interval, coin, data_type, timestamp_direction]
    # See _build_file_path in collector.py
    interval = parts[-4]
    coin = parts[-3]
    data_type = parts[-2]
    fname = parts[-1]
    direction = "up" if "up" in fname else "down"
    ts_str = fname.replace("up", "").replace("down", "")
    if not ts_str.isdigit():
        return
    ts = int(ts_str)

    meta = get_or_create_meta(interval, coin, data_type, direction, ts)
    if meta.message_count == 0:
        return  # nothing worth writing

    mpath = _metadata_path(parquet_path)
    try:
        Path(mpath).parent.mkdir(parents=True, exist_ok=True)
        with open(mpath, "w") as f:
            json.dump(asdict(meta), f, indent=2, default=str)
    except Exception as exc:
        logger.debug("Failed to write metadata %s: %s", mpath, exc)


def _ts_to_iso(ts: float) -> str:
    """Convert Unix timestamp to ISO-8601 string."""
    from datetime import datetime, timezone

    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


# ── Data quality scan tool ─────────────────────────────────────────


def scan_data_quality(data_dir: str = "data") -> dict[str, Any]:
    """Scan a data directory for quality issues.

    Returns a report dict with counts of:
    - ``empty_files``: Parquet files with 0 rows
    - ``missing_meta``: Parquet files without a companion .meta.json
    - ``zero_message_meta``: .meta.json files with message_count == 0
    - ``failed_windows``: windows with status "failed"
    - ``duplicate_ts``: files with same window_ts and direction (overwritten)
    - ``gap_windows``: consecutive windows where Δ > interval + tolerance
    """
    import glob

    report: dict[str, Any] = {
        "empty_files": [],
        "missing_meta": [],
        "zero_message_meta": [],
        "failed_windows": [],
        "duplicate_ts": [],
        "gap_windows": [],
    }

    parquet_files = glob.glob(f"{data_dir}/**/*.parquet", recursive=True)
    seen_ts: dict[str, list[str]] = {}

    for pf in parquet_files:
        # Check empty parquet
        try:
            import pandas as pd

            df = pd.read_parquet(pf)
            if len(df) == 0:
                report["empty_files"].append(pf)
        except Exception:
            report["empty_files"].append(pf)

        # Check companion meta
        mp = _metadata_path(pf)
        if not os.path.exists(mp):
            report["missing_meta"].append(pf)
        else:
            try:
                with open(mp) as f:
                    m = json.load(f)
                if m.get("message_count", 0) == 0:
                    report["zero_message_meta"].append(pf)
                if m.get("status") == "failed":
                    report["failed_windows"].append(pf)
            except Exception:
                report["missing_meta"].append(pf)

        # Check for duplicate timestamps
        parts = pf.replace(".parquet", "").split("/")
        fname = parts[-1]
        direction = "up" if "up" in fname else "down"
        ts_str = fname.replace("up", "").replace("down", "")
        if ts_str.isdigit():
            key = f"{direction}/{ts_str}"
            if key in seen_ts:
                report["duplicate_ts"].append(pf)
            seen_ts[key] = seen_ts.get(key, []) + [pf]

    # ── Gap detection ──────────────────────────────────────────────
    interval_seconds_map = {"5m": 300, "15m": 900, "1h": 3600}
    tolerance = 60  # seconds
    gap_windows: list[str] = []

    # Collect file info: (interval, coin, data_type, direction, window_ts)
    file_info: list[tuple[str, str, str, str, int]] = []
    for pf in parquet_files:
        parts = pf.replace(".parquet", "").split("/")
        if len(parts) < 4:
            continue
        interval_g = parts[-4]
        coin_g = parts[-3]
        data_type_g = parts[-2]
        fname = parts[-1]
        direction_g = "up" if "up" in fname else "down"
        ts_str_g = fname.replace("up", "").replace("down", "")
        if not ts_str_g.isdigit():
            continue
        file_info.append((interval_g, coin_g, data_type_g, direction_g, int(ts_str_g)))

    # Group by (interval, coin, data_type, direction)
    from collections import defaultdict

    groups: dict[tuple[str, str, str, str], list[int]] = defaultdict(list)
    for interval_g, coin_g, data_type_g, direction_g, ts_g in file_info:
        groups[(interval_g, coin_g, data_type_g, direction_g)].append(ts_g)

    # Detect gaps in each group
    for (interval_g, coin_g, data_type_g, direction_g), timestamps in groups.items():
        expected_interval = interval_seconds_map.get(interval_g)
        if expected_interval is None:
            continue
        timestamps.sort()
        for i in range(len(timestamps) - 1):
            delta = timestamps[i + 1] - timestamps[i]
            if delta > expected_interval + tolerance:
                missed_windows = (delta // expected_interval) - 1
                gap_desc = (
                    f"interval={interval_g} coin={coin_g} type={data_type_g} "
                    f"direction={direction_g}: gap between {timestamps[i]} "
                    f"and {timestamps[i + 1]} "
                    f"(delta={delta}s, expected≈{expected_interval}s, "
                    f"tolerance={tolerance}s, ~{missed_windows} window(s) missing)"
                )
                gap_windows.append(gap_desc)

    report["gap_windows"] = gap_windows

    return report
