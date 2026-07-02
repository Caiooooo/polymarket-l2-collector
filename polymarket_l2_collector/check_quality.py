#!/usr/bin/env python3
"""
Data quality check CLI — scan collected Parquet data for issues.

Usage:
    python -m polymarket_l2_collector.check_quality [--data-dir data]

Reports:
  - Empty Parquet files
  - Missing .meta.json companions
  - Meta files with zero messages
  - Windows with status "failed"
  - Duplicate timestamps (same window overwritten)
  - Gaps in window sequence (missing intervals)
"""

from __future__ import annotations

import argparse
import sys

from .logger_config import get_logger
from .window_metadata import scan_data_quality

logger = get_logger("check_quality")


def main() -> None:
    parser = argparse.ArgumentParser(description="Check Polymarket L2 data quality")
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Root data directory (default: data)",
    )
    args = parser.parse_args()

    report = scan_data_quality(args.data_dir)

    total_issues = sum(len(v) for v in report.values())
    if total_issues == 0:
        print(f"\n✅ No quality issues found in {args.data_dir}")
        sys.exit(0)

    print(f"\n{'=' * 60}")
    print(f"Data Quality Report — {args.data_dir}")
    print(f"{'=' * 60}")

    if report["empty_files"]:
        print(f"\n❌ Empty Parquet files ({len(report['empty_files'])}):")
        for f in report["empty_files"][:20]:
            print(f"   - {f}")
        if len(report["empty_files"]) > 20:
            print(f"   ... and {len(report['empty_files']) - 20} more")

    if report["missing_meta"]:
        print(f"\n⚠️  Missing companion .meta.json ({len(report['missing_meta'])}):")
        for f in report["missing_meta"][:20]:
            print(f"   - {f}")
        if len(report["missing_meta"]) > 20:
            print(f"   ... and {len(report['missing_meta']) - 20} more")

    if report["zero_message_meta"]:
        print(f"\n⚠️  Meta files with 0 messages ({len(report['zero_message_meta'])}):")
        for f in report["zero_message_meta"][:10]:
            print(f"   - {f}")

    if report["failed_windows"]:
        print(f"\n❌ Failed windows ({len(report['failed_windows'])}):")
        for f in report["failed_windows"][:10]:
            print(f"   - {f}")

    if report["duplicate_ts"]:
        print(f"\n⚠️  Duplicate timestamps (overwritten) ({len(report['duplicate_ts'])}):")
        for f in report["duplicate_ts"][:10]:
            print(f"   - {f}")

    if report["gap_windows"]:
        print(f"\n⚠️  Window sequence gaps ({len(report['gap_windows'])}):")
        for g in report["gap_windows"]:
            print(f"   - {g}")

    print(f"\n{'=' * 60}")
    print(f"Total issues: {total_issues}")
    sys.exit(1 if total_issues > 0 else 0)


if __name__ == "__main__":
    main()
