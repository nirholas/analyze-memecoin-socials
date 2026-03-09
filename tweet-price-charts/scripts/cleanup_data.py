#!/usr/bin/env python3
"""
Data cleanup utilities for tweet-price.

DESTRUCTIVE OPERATIONS - Use with caution.
Always verify with validate_export.py after cleanup.

Usage:
    python cleanup_data.py --asset aster --list-sources          # List data sources
    python cleanup_data.py --asset aster --list-sources --timeframe 1d
    python cleanup_data.py --asset aster --remove-source chart_reconstruction --confirm
    python cleanup_data.py --asset believe --remove-dots --timeframe 15m --confirm
"""
import argparse
import sys
from datetime import datetime
from typing import Optional

from db import (
    get_connection, init_schema, get_asset,
    delete_by_source, delete_dot_candles
)


# =============================================================================
# List Operations (Read-only)
# =============================================================================

def list_sources(conn, asset_id: str, timeframe: Optional[str] = None) -> dict:
    """
    List all data sources for an asset with counts.

    Returns dict of {source: {count, first_date, last_date}}
    """
    query = """
        SELECT
            data_source,
            timeframe,
            COUNT(*) as count,
            MIN(timestamp) as first_ts,
            MAX(timestamp) as last_ts
        FROM prices
        WHERE asset_id = ?
    """
    params = [asset_id]

    if timeframe:
        query += " AND timeframe = ?"
        params.append(timeframe)

    query += " GROUP BY data_source, timeframe ORDER BY timeframe, data_source"

    results = conn.execute(query, params).fetchall()

    sources = {}
    for row in results:
        source = row[0] or "(null)"
        tf = row[1]
        count = row[2]
        first_ts = row[3]
        last_ts = row[4]

        # Convert timestamps to dates
        if hasattr(first_ts, 'timestamp'):
            import calendar
            first_ts = calendar.timegm(first_ts.timetuple())
            last_ts = calendar.timegm(last_ts.timetuple())

        first_date = datetime.utcfromtimestamp(first_ts).strftime("%Y-%m-%d") if first_ts else "N/A"
        last_date = datetime.utcfromtimestamp(last_ts).strftime("%Y-%m-%d") if last_ts else "N/A"

        key = f"{tf}:{source}"
        sources[key] = {
            "timeframe": tf,
            "source": source,
            "count": count,
            "first_date": first_date,
            "last_date": last_date
        }

    return sources


def count_dots(conn, asset_id: str, timeframe: str) -> dict:
    """
    Count candles where O=H=L=C (dots).

    Returns {total, dots, pct}
    """
    result = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN open = high AND high = low AND low = close THEN 1 ELSE 0 END) as dots
        FROM prices
        WHERE asset_id = ? AND timeframe = ?
    """, [asset_id, timeframe]).fetchone()

    total = result[0] or 0
    dots = result[1] or 0
    pct = (dots / total * 100) if total > 0 else 0

    return {"total": total, "dots": dots, "pct": pct}


# =============================================================================
# Destructive Operations (Require --confirm)
# These are wrappers around db.py functions with consistent interface
# =============================================================================

def remove_source(conn, asset_id: str, source: str, dry_run: bool = True) -> dict:
    """
    Remove all candles from a specific data source.
    Wrapper around db.delete_by_source() for CLI use.
    """
    return delete_by_source(conn, asset_id, source, timeframe=None, dry_run=dry_run)


def remove_dots(conn, asset_id: str, timeframe: str, dry_run: bool = True) -> dict:
    """
    Remove candles where O=H=L=C (dots).
    Wrapper around db.delete_dot_candles() for CLI use.
    """
    return delete_dot_candles(conn, asset_id, timeframe, dry_run=dry_run)


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Data cleanup utilities (DESTRUCTIVE - use with caution)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python cleanup_data.py --asset aster --list-sources
    python cleanup_data.py --asset aster --remove-source chart_reconstruction --confirm
    python cleanup_data.py --asset believe --remove-dots --timeframe 15m --confirm

SAFETY: All destructive operations require --confirm flag.
        Dry-run (default) shows what would be deleted without making changes.
        """
    )
    parser.add_argument(
        "--asset", "-a",
        required=True,
        help="Asset ID to operate on"
    )
    parser.add_argument(
        "--timeframe", "-t",
        choices=["1d", "1h", "15m", "1m"],
        help="Filter by timeframe"
    )

    # Operations (mutually exclusive)
    ops = parser.add_mutually_exclusive_group(required=True)
    ops.add_argument(
        "--list-sources",
        action="store_true",
        help="List all data sources for asset"
    )
    ops.add_argument(
        "--remove-source",
        metavar="SOURCE",
        help="Remove all candles from specified data source"
    )
    ops.add_argument(
        "--remove-dots",
        action="store_true",
        help="Remove candles where O=H=L=C (dots)"
    )
    ops.add_argument(
        "--count-dots",
        action="store_true",
        help="Count candles where O=H=L=C (read-only)"
    )

    # Safety flag
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Actually perform destructive operation (default: dry-run)"
    )

    args = parser.parse_args()

    conn = get_connection()
    init_schema(conn)

    # Verify asset exists
    asset = get_asset(conn, args.asset)
    if not asset:
        print(f"Error: Asset '{args.asset}' not found")
        sys.exit(1)

    print("=" * 60)
    print(f"DATA CLEANUP: {args.asset.upper()}")
    print("=" * 60)

    # Execute operation
    if args.list_sources:
        sources = list_sources(conn, args.asset, args.timeframe)
        if not sources:
            print("\nNo data found.")
        else:
            print(f"\n{'Timeframe':<10} {'Source':<30} {'Count':>8} {'First':>12} {'Last':>12}")
            print("-" * 75)
            for key, info in sources.items():
                print(f"{info['timeframe']:<10} {info['source']:<30} {info['count']:>8} {info['first_date']:>12} {info['last_date']:>12}")

    elif args.count_dots:
        if not args.timeframe:
            print("\nError: --count-dots requires --timeframe")
            sys.exit(1)

        result = count_dots(conn, args.asset, args.timeframe)
        print(f"\n{args.timeframe}: {result['dots']}/{result['total']} candles are dots ({result['pct']:.1f}%)")

    elif args.remove_source:
        result = remove_source(conn, args.asset, args.remove_source, dry_run=not args.confirm)

        if args.confirm:
            print(f"\nDELETED {result['deleted']} candles from source '{args.remove_source}'")
            for tf, count in result["by_timeframe"].items():
                print(f"  {tf}: {count} candles")
            print("\nRun 'python export_static.py' to regenerate JSON files.")
        else:
            if result["would_delete"] == 0:
                print(f"\nNo candles found with source '{args.remove_source}'")
            else:
                print(f"\nDRY RUN: Would delete {result['would_delete']} candles from source '{args.remove_source}'")
                for tf, count in result["by_timeframe"].items():
                    print(f"  {tf}: {count} candles")
                print("\nRun with --confirm to actually delete.")

    elif args.remove_dots:
        if not args.timeframe:
            print("\nError: --remove-dots requires --timeframe")
            sys.exit(1)

        result = remove_dots(conn, args.asset, args.timeframe, dry_run=not args.confirm)

        if args.confirm:
            print(f"\nDELETED {result['deleted']} dot candles from {args.timeframe}")
            print(f"  Was: {result['total_before']} candles ({result['pct']:.1f}% dots)")
            print(f"  Now: {result['total_before'] - result['deleted']} candles")
            print("\nRun 'python export_static.py' to regenerate JSON files.")
        else:
            if result["would_delete"] == 0:
                print(f"\n{args.timeframe}: No dot candles found")
            else:
                print(f"\nDRY RUN: Would delete {result['would_delete']} dot candles from {args.timeframe}")
                print(f"  Total: {result['total']} candles ({result['pct']:.1f}% are dots)")
                print("\nRun with --confirm to actually delete.")

    conn.close()


if __name__ == "__main__":
    main()
