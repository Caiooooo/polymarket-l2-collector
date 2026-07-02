"""
Parquet write cache with atomic file replacement.

Messages accumulate in memory per window; when the threshold is reached
(or on shutdown) they are flushed to a Parquet file via atomic rename
to prevent data corruption on crash.
"""

from __future__ import annotations

import gc
import os
import tempfile
from typing import Any

import pandas as pd

from .config import load_settings
from .logger_config import get_logger
from .window_metadata import record_flush, write_metadata

logger = get_logger("file_cache")

# In-memory caches: key = "interval/coin/type/direction/timestamp"
trades_cache_dict: dict[str, dict[str, Any]] = {}
orderbook_cache_dict: dict[str, dict[str, Any]] = {}


# ── Optimisation helpers ───────────────────────────────────────────


def _conv_order_item(item: dict[str, Any]) -> dict[str, Any]:
    """Convert order-level 'price'/'size' to compact 'p'/'s' (×100 int)."""
    if not isinstance(item, dict):
        return item
    result: dict[str, Any] = {}
    for k, v in item.items():
        if k == "price" and v is not None and not pd.isna(v):
            result["p"] = int(float(v) * 100)
        elif k == "size" and v is not None and not pd.isna(v):
            result["s"] = int(float(v) * 100)
        else:
            result[k] = v
    return result


def _restore_order_item(item: dict[str, Any]) -> dict[str, Any]:
    """Reverse ``_conv_order_item``: restore 'price'/'size' from 'p'/'s'."""
    if not isinstance(item, dict):
        return item
    result: dict[str, Any] = {}
    for k, v in item.items():
        if k == "p" and v is not None and not pd.isna(v):
            result["price"] = float(v) / 100
        elif k == "s" and v is not None and not pd.isna(v):
            result["size"] = float(v) / 100
        elif k == "price" and v is not None and not pd.isna(v):
            result["price"] = float(v) / 100
        elif k == "size" and v is not None and not pd.isna(v):
            result["size"] = float(v) / 100
        else:
            result[k] = v
    return result


def _iterable_items(value: Any) -> list:
    """Coerce a value to a list of items for iteration.

    Handles plain lists, numpy arrays (from parquet read-back), and
    other iterables.  Strings and scalars are returned as empty list.
    """
    if isinstance(value, list):
        return value
    if hasattr(value, "__iter__") and not isinstance(value, (str, bytes, dict)):
        return list(value)
    if hasattr(value, "tolist"):
        return value.tolist()
    return []


def optimize_for_parquet(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Compress price/size fields to int×100 for better compression."""
    out: list[dict[str, Any]] = []
    for record in data:
        rec: dict[str, Any] = {}
        for key, value in record.items():
            if key in ("bids", "asks"):
                items = _iterable_items(value)
                rec[key] = [_conv_order_item(x) for x in items]
            elif key == "price" and value is not None and not pd.isna(value):
                rec["p"] = int(float(value) * 100)
            elif key == "size" and value is not None and not pd.isna(value):
                rec["s"] = int(float(value) * 100)
            elif key == "timestamp" and value is not None and not pd.isna(value):
                rec[key] = int(value)
            else:
                rec[key] = value
        out.append(rec)
    return out


def restore_from_parquet(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Reverse ``optimize_for_parquet``, restoring floats."""
    out: list[dict[str, Any]] = []
    for record in data:
        rec: dict[str, Any] = {}
        for key, value in record.items():
            if key in ("bids", "asks"):
                items = _iterable_items(value)
                rec[key] = [_restore_order_item(x) for x in items]
            elif key == "p" and value is not None and not pd.isna(value):
                rec["price"] = float(value) / 100
            elif key == "s" and value is not None and not pd.isna(value):
                rec["size"] = float(value) / 100
            elif (key == "price" or key == "size") and value is not None and not pd.isna(value):
                # Backward compat — old files stored float directly
                rec[key] = float(value)
            else:
                rec[key] = value
        out.append(rec)
    return out


# ── Key helpers ────────────────────────────────────────────────────


def _parse_file_path(file_path: str) -> tuple:
    """Split a data-file path into (interval, coin, data_type, direction, timestamp).

    Parses from the end of the path, so it works with both relative
    (``data/5m/btc/orderbooks/1765359900up.parquet``) and absolute
    (``/tmp/xxx/5m/btc/orderbooks/1765359900up.parquet``) paths.
    """
    parts = file_path.replace("\\", "/").rstrip("/").split("/")
    if len(parts) < 5:
        raise ValueError(f"Path does not have enough parts: {file_path}")
    fname = parts[-1].split(".")[0]
    data_type = parts[-2]
    coin = parts[-3]
    interval = parts[-4]
    direction = "up" if "up" in fname else "down"
    ts = int(fname.replace("up", "").replace("down", ""))
    return interval, coin, data_type, direction, ts


def _window_cache_key(file_path: str) -> str:
    """Build the in-memory cache key from a file path."""
    interval, coin, data_type, direction, ts = _parse_file_path(file_path)
    return f"{interval}/{coin}/{data_type}/{direction}/{ts}"


def _build_file_path(
    data_dir: str,
    interval: str,
    coin: str,
    data_type: str,
    window_ts: int,
    direction: str = "up",
) -> str:
    """Build the canonical output file path."""
    return f"{data_dir}/{interval}/{coin}/{data_type}/{window_ts}{direction}.parquet"


# ── Atomic Parquet write ───────────────────────────────────────────


def _write_parquet_atomic(data: list[dict[str, Any]], path: str) -> None:
    """Write a DataFrame to Parquet using a temp file + atomic rename.

    If the process crashes during write, the target file (if it existed)
    is unaffected.
    """
    os.makedirs(os.path.dirname(path), exist_ok=True)

    fd, tmp_path = tempfile.mkstemp(suffix=".parquet", dir=os.path.dirname(path))
    os.close(fd)

    try:
        df = pd.DataFrame(data)
        df.to_parquet(tmp_path, index=False, engine="pyarrow", compression="zstd")
        os.replace(tmp_path, path)
    except Exception:
        # Clean up temp file on failure
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


# ── Flush ──────────────────────────────────────────────────────────


def _flush_cache_entry(cache_dict: dict, cache_key: str, file_path: str) -> int:
    """Write one cached window to disk; return count of rows flushed."""
    info = cache_dict.get(cache_key)
    if not info or not info.get("data"):
        return 0

    pending = info["data"]
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Read existing data for append
    existing: list[dict] = []
    if os.path.exists(file_path):
        try:
            df = pd.read_parquet(file_path)
            existing = restore_from_parquet(df.to_dict("records"))
            del df
        except Exception:
            existing = []

    rows = len(pending)
    combined = existing + pending
    optimized = optimize_for_parquet(combined)

    _write_parquet_atomic(optimized, file_path)

    info["data"] = []
    if "df" in dir():
        del df
    del optimized, combined, existing
    gc.collect()

    # Record flush in metadata
    try:
        interval, coin, data_type, direction, ts = _parse_file_path(file_path)
        record_flush(interval, coin, data_type, direction, ts)
        write_metadata(file_path)
    except Exception as exc:
        logger.debug("Metadata write failed: %s", exc)

    return rows


def flush_all_caches() -> int:
    """Force-flush every cached window to disk."""
    total = 0
    for cache_dict in (trades_cache_dict, orderbook_cache_dict):
        for key in list(cache_dict.keys()):
            fp = cache_dict[key].get("file_path")
            if not fp:
                continue
            total += _flush_cache_entry(cache_dict, key, fp)
    if total:
        logger.info("Flushed %d cached rows to disk", total)
    return total


def drop_empty_cache_windows(max_windows: int = 30) -> int:
    """Remove empty cache entries exceeding *max_windows* per dict."""
    removed = 0
    for cache_dict in (trades_cache_dict, orderbook_cache_dict):
        keys = list(cache_dict.keys())
        if len(keys) <= max_windows:
            continue
        sorted_keys = sorted(keys, key=lambda k: int(k.rsplit("/", 1)[-1]) if k.rsplit("/", 1)[-1].isdigit() else 0)
        for k in sorted_keys[: len(sorted_keys) - max_windows]:
            if not cache_dict[k].get("data"):
                del cache_dict[k]
                removed += 1
    if removed:
        logger.debug("Dropped %d empty cache windows", removed)
    return removed


# ── Cache-size limit ───────────────────────────────────────────────


def _cleanup_old_cache(cache_dict: dict, max_windows: int = 30) -> None:
    """Flush & evict the oldest windows when *max_windows* is exceeded."""
    if len(cache_dict) <= max_windows:
        return
    sorted_keys = sorted(cache_dict, key=lambda k: int(k.rsplit("/", 1)[-1]) if k.rsplit("/", 1)[-1].isdigit() else 0)
    to_remove = sorted_keys[: len(sorted_keys) - max_windows]
    for key in to_remove:
        fp = cache_dict[key].get("file_path")
        if fp:
            _flush_cache_entry(cache_dict, key, fp)
        del cache_dict[key]
    if to_remove:
        logger.debug("Cleaned %d old cache windows", len(to_remove))


# ── Public save API ────────────────────────────────────────────────


def save_trades(data: list[dict], file_path: str) -> None:
    """Buffer trade data; flush when threshold is reached."""
    global trades_cache_dict
    key = _window_cache_key(file_path)

    if key not in trades_cache_dict:
        trades_cache_dict[key] = {"data": [], "file_path": file_path}
        _cleanup_old_cache(trades_cache_dict, load_settings().max_cached_windows)
    else:
        trades_cache_dict[key]["file_path"] = file_path

    trades_cache_dict[key]["data"].extend(data)

    if len(trades_cache_dict[key]["data"]) >= load_settings().flush_threshold_trades:
        _flush_cache_entry(trades_cache_dict, key, file_path)


def save_book(data: list[dict], file_path: str) -> None:
    """Buffer orderbook data; flush when threshold is reached."""
    global orderbook_cache_dict
    key = _window_cache_key(file_path)

    if key not in orderbook_cache_dict:
        orderbook_cache_dict[key] = {"data": [], "file_path": file_path}
        _cleanup_old_cache(orderbook_cache_dict, load_settings().max_cached_windows)
    else:
        orderbook_cache_dict[key]["file_path"] = file_path

    orderbook_cache_dict[key]["data"].extend(data)

    if len(orderbook_cache_dict[key]["data"]) >= load_settings().flush_threshold_book:
        _flush_cache_entry(orderbook_cache_dict, key, file_path)
