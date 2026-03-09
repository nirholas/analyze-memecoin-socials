#!/usr/bin/env python3
"""
True Validation System for Price Data

Validates that exported price data has complete coverage from launch_date to today.
Unlike basic validation that just checks "does data exist", this validates against
the mathematical truth of what SHOULD exist.

Usage:
    python validate_candle_coverage.py --asset hype
    python validate_candle_coverage.py --asset hype --verbose
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Paths
SCRIPT_DIR = Path(__file__).parent
ASSETS_JSON = SCRIPT_DIR / "assets.json"
STATIC_DIR = SCRIPT_DIR.parent / "web" / "public" / "static"

# Timeframe intervals in seconds
INTERVALS = {
    "1d": 86400,      # 24 * 60 * 60
    "1h": 3600,       # 60 * 60
    "15m": 900,       # 15 * 60
}

# Coverage threshold (95% required)
COVERAGE_THRESHOLD = 0.95

# Import age-based skip thresholds from config
from config import SKIP_1M_AFTER_DAYS, SKIP_15M_AFTER_DAYS


def load_asset_config(asset_id: str) -> Optional[dict]:
    """Load asset config from assets.json"""
    with open(ASSETS_JSON) as f:
        data = json.load(f)

    for asset in data["assets"]:
        if asset["id"] == asset_id:
            return asset
    return None


def parse_launch_date(launch_date_str: str) -> datetime:
    """Parse ISO 8601 launch date string to datetime"""
    # Handle "2024-11-29T00:00:00Z" format
    return datetime.fromisoformat(launch_date_str.replace("Z", "+00:00"))


def load_price_data(asset_id: str, timeframe: str) -> Optional[dict]:
    """Load exported price JSON file"""
    price_file = STATIC_DIR / asset_id / f"prices_{timeframe}.json"
    if not price_file.exists():
        return None

    with open(price_file) as f:
        return json.load(f)


def calculate_expected_candles(launch_date: datetime, now: datetime, interval_seconds: int) -> int:
    """Calculate expected number of candles from launch to now"""
    duration_seconds = (now - launch_date).total_seconds()
    return int(duration_seconds / interval_seconds)


def find_gaps(candles: list, interval_seconds: int) -> list:
    """
    Find gaps in candle data where gap > 2x expected interval.

    Returns list of dicts with:
        - start: timestamp of candle before gap
        - end: timestamp of candle after gap
        - missing_candles: estimated number of missing candles
        - start_date: human readable start date
        - end_date: human readable end date
    """
    if len(candles) < 2:
        return []

    gaps = []
    threshold = interval_seconds * 2  # Gap must be > 2x interval

    for i in range(1, len(candles)):
        prev_t = candles[i - 1]["t"]
        curr_t = candles[i]["t"]
        actual_gap = curr_t - prev_t

        if actual_gap > threshold:
            missing = int(actual_gap / interval_seconds) - 1
            gaps.append({
                "start": prev_t,
                "end": curr_t,
                "missing_candles": missing,
                "start_date": datetime.fromtimestamp(prev_t, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
                "end_date": datetime.fromtimestamp(curr_t, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
            })

    return gaps


def validate_timeframe(
    asset_id: str,
    timeframe: str,
    launch_date: datetime,
    now: datetime,
    verbose: bool = False
) -> dict:
    """
    Validate a single timeframe for an asset.

    Returns dict with:
        - expected: expected candle count
        - actual: actual candle count
        - coverage: percentage (0.0 - 1.0+)
        - status: "OK", "WARN", or "FAIL"
        - gaps: list of gap details
        - first_candle_date: datetime of first candle
        - pre_launch_data: bool if data exists before launch
        - issues: list of issue strings
    """
    interval_seconds = INTERVALS[timeframe]
    expected = calculate_expected_candles(launch_date, now, interval_seconds)

    price_data = load_price_data(asset_id, timeframe)

    if price_data is None:
        return {
            "expected": expected,
            "actual": 0,
            "coverage": 0.0,
            "status": "FAIL",
            "gaps": [],
            "first_candle_date": None,
            "pre_launch_data": False,
            "issues": [f"No {timeframe} data file found"],
        }

    candles = price_data.get("candles", [])
    actual = len(candles)
    coverage = actual / expected if expected > 0 else 0.0

    # Check for pre-launch data
    first_candle_date = None
    pre_launch_data = False
    if candles:
        first_t = candles[0]["t"]
        first_candle_date = datetime.fromtimestamp(first_t, tz=timezone.utc)
        launch_ts = launch_date.timestamp()
        if first_t < launch_ts:
            pre_launch_data = True

    # Find gaps
    gaps = find_gaps(candles, interval_seconds)

    # Determine status
    issues = []
    if coverage < COVERAGE_THRESHOLD:
        status = "FAIL"
        issues.append(f"Coverage {coverage:.1%} below {COVERAGE_THRESHOLD:.0%} threshold")
    elif pre_launch_data:
        status = "WARN"
        issues.append(f"Data exists before launch date ({first_candle_date.strftime('%Y-%m-%d %H:%M')})")
    elif coverage > 1.05:  # More than 105% - suspicious
        status = "WARN"
        issues.append(f"More candles than expected ({coverage:.1%}) - possible duplicate data")
    else:
        status = "OK"

    if gaps and verbose:
        total_missing = sum(g["missing_candles"] for g in gaps)
        issues.append(f"{len(gaps)} gaps found ({total_missing} candles missing)")

    return {
        "expected": expected,
        "actual": actual,
        "coverage": coverage,
        "status": status,
        "gaps": gaps,
        "first_candle_date": first_candle_date,
        "pre_launch_data": pre_launch_data,
        "issues": issues,
    }


def validate_asset(asset_id: str, verbose: bool = False) -> bool:
    """
    Validate all timeframes for an asset.

    Returns True if all validations pass, False otherwise.
    """
    # Load asset config
    asset = load_asset_config(asset_id)
    if asset is None:
        print(f"ERROR: Asset '{asset_id}' not found in assets.json")
        return False

    launch_date = parse_launch_date(asset["launch_date"])
    now = datetime.now(timezone.utc)
    days_since_launch = (now - launch_date).days

    # Print header
    print(f"\n{'=' * 60}")
    print(f"  {asset['name']} ({asset_id}) Validation Report")
    print(f"{'=' * 60}")
    print(f"\nLaunch Date: {launch_date.strftime('%Y-%m-%d')}")
    print(f"Today:       {now.strftime('%Y-%m-%d')}")
    print(f"Days Since:  {days_since_launch}")
    print()

    # Get skip_timeframes from asset config + auto-skip based on age
    skip_timeframes = set(asset.get("skip_timeframes", []))

    # Auto-skip based on asset age (same thresholds as fetch_prices.py and export_static.py)
    if days_since_launch > SKIP_1M_AFTER_DAYS:
        skip_timeframes.add("1m")
    if days_since_launch > SKIP_15M_AFTER_DAYS:
        skip_timeframes.add("15m")

    # Validate each timeframe
    results = {}
    for tf in ["1d", "1h", "15m"]:
        if tf in skip_timeframes:
            results[tf] = {
                "expected": 0,
                "actual": 0,
                "coverage": 1.0,
                "status": "SKIP",
                "gaps": [],
                "first_candle_date": None,
                "pre_launch_data": False,
                "issues": [],
            }
        else:
            results[tf] = validate_timeframe(asset_id, tf, launch_date, now, verbose)

    # Print summary table
    print(f"{'Timeframe':<10} {'Expected':>10} {'Actual':>10} {'Coverage':>10} {'Status':>8}")
    print("-" * 50)

    for tf in ["1d", "1h", "15m"]:
        r = results[tf]
        coverage_str = f"{r['coverage']:.1%}"
        print(f"{tf:<10} {r['expected']:>10,} {r['actual']:>10,} {coverage_str:>10} {r['status']:>8}")

    print()

    # Print issues
    any_issues = False
    for tf in ["1d", "1h", "15m"]:
        r = results[tf]
        if r["issues"]:
            any_issues = True
            print(f"Issues ({tf}):")
            for issue in r["issues"]:
                print(f"  - {issue}")

    # Print gaps if verbose
    if verbose:
        for tf in ["1d", "1h", "15m"]:
            r = results[tf]
            if r["gaps"]:
                print(f"\nGaps Found ({tf}):")
                for gap in r["gaps"][:10]:  # Show first 10 gaps
                    print(f"  - {gap['start_date']} to {gap['end_date']}: ~{gap['missing_candles']:,} candles missing")
                if len(r["gaps"]) > 10:
                    print(f"  ... and {len(r['gaps']) - 10} more gaps")

    # Print first candle info
    print(f"\nFirst Candle Dates:")
    for tf in ["1d", "1h", "15m"]:
        r = results[tf]
        if r["status"] == "SKIP":
            print(f"  {tf}: Skipped (in skip_timeframes)")
        elif r["first_candle_date"]:
            status_marker = " (BEFORE LAUNCH!)" if r["pre_launch_data"] else ""
            print(f"  {tf}: {r['first_candle_date'].strftime('%Y-%m-%d %H:%M')}{status_marker}")
        else:
            print(f"  {tf}: No data")

    print()

    # Determine overall pass/fail
    all_passed = all(r["status"] != "FAIL" for r in results.values())

    if all_passed:
        print("RESULT: PASS")
    else:
        print("RESULT: FAIL")

    return all_passed


def main():
    parser = argparse.ArgumentParser(
        description="Validate price data coverage against launch date"
    )
    parser.add_argument(
        "--asset",
        required=True,
        help="Asset ID to validate (e.g., hype, pump)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Show detailed gap information"
    )

    args = parser.parse_args()

    success = validate_asset(args.asset, args.verbose)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
