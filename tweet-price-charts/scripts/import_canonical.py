#!/usr/bin/env python3
"""
Import Canonical Prices from Static JSONs

This script imports price data from the static JSON files (web/public/static/)
into the `prices` table (canonical). This ensures DuckDB matches the website exactly.

The `prices` table is the canonical source of truth for queries.
The `prices_RAW_INGESTION` table contains raw API data (for debugging only).

Usage:
    python import_canonical.py              # Sync all assets
    python import_canonical.py --asset pump # Sync single asset
    python import_canonical.py --verify     # Verify counts match
"""

import argparse
import json
import sys
from pathlib import Path

import duckdb

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
STATIC_DIR = PROJECT_ROOT / "web" / "public" / "static"
DB_PATH = PROJECT_ROOT / "data" / "analytics.duckdb"

# Timeframes to import (in order)
TIMEFRAMES = ["1d", "1h", "15m"]


def get_enabled_assets() -> list[dict]:
    """Load enabled assets from assets.json"""
    assets_path = SCRIPT_DIR / "assets.json"
    with open(assets_path) as f:
        data = json.load(f)
    return [a for a in data["assets"] if a.get("enabled", True)]


def import_asset_timeframe(conn, asset_id: str, timeframe: str) -> int:
    """
    Import a single JSON file into the prices table (canonical).

    Returns number of candles imported.
    """
    json_path = STATIC_DIR / asset_id / f"prices_{timeframe}.json"
    if not json_path.exists():
        return 0

    # Delete existing canonical data for this asset/timeframe
    conn.execute("""
        DELETE FROM prices
        WHERE asset_id = ? AND timeframe = ?
    """, [asset_id, timeframe])

    # DuckDB can read JSON directly - much faster than Python iteration
    try:
        conn.execute(f"""
            INSERT INTO prices (asset_id, timeframe, timestamp, open, high, low, close, volume)
            SELECT
                '{asset_id}' as asset_id,
                '{timeframe}' as timeframe,
                to_timestamp(candle.t) as timestamp,
                candle.o as open,
                candle.h as high,
                candle.l as low,
                candle.c as close,
                candle.v as volume
            FROM (
                SELECT unnest(candles) as candle
                FROM read_json_auto('{json_path}')
            )
        """)
    except Exception as e:
        print(f"  ERROR importing {asset_id}/{timeframe}: {e}")
        return 0

    # Get count of imported candles
    result = conn.execute("""
        SELECT COUNT(*) FROM prices
        WHERE asset_id = ? AND timeframe = ?
    """, [asset_id, timeframe]).fetchone()

    return result[0] if result else 0


def import_1m_chunks(conn, asset_id: str) -> int:
    """
    Import monthly 1m chunks for an asset.

    1m data is stored as monthly files (prices_1m_2025-07.json, etc.)
    with an index file (prices_1m_index.json).

    Returns total number of candles imported.
    """
    index_path = STATIC_DIR / asset_id / "prices_1m_index.json"
    if not index_path.exists():
        return 0

    with open(index_path) as f:
        index = json.load(f)

    # Delete existing 1m canonical data
    conn.execute("""
        DELETE FROM prices
        WHERE asset_id = ? AND timeframe = '1m'
    """, [asset_id])

    total = 0
    chunks = index.get("chunks", [])

    for chunk in chunks:
        chunk_path = STATIC_DIR / asset_id / chunk["file"]
        if not chunk_path.exists():
            print(f"  WARNING: Chunk file not found: {chunk_path}")
            continue

        try:
            conn.execute(f"""
                INSERT INTO prices (asset_id, timeframe, timestamp, open, high, low, close, volume)
                SELECT
                    '{asset_id}' as asset_id,
                    '1m' as timeframe,
                    to_timestamp(c.t) as timestamp,
                    c.o as open,
                    c.h as high,
                    c.l as low,
                    c.c as close,
                    c.v as volume
                FROM (
                    SELECT unnest(candles) as c
                    FROM read_json_auto('{chunk_path}')
                )
            """)
            total += chunk.get("count", 0)
        except Exception as e:
            print(f"  ERROR importing {asset_id}/1m/{chunk['file']}: {e}")

    return total


def sync_asset(conn, asset_id: str, verbose: bool = True) -> dict:
    """
    Sync all price data for a single asset from JSONs to canonical table.

    Returns dict with counts per timeframe.
    """
    if verbose:
        print(f"\n  {asset_id}:")

    counts = {}

    # Import standard timeframes
    for tf in TIMEFRAMES:
        count = import_asset_timeframe(conn, asset_id, tf)
        counts[tf] = count
        if verbose and count > 0:
            print(f"    {tf}: {count:,} candles")

    # Import 1m chunks
    count_1m = import_1m_chunks(conn, asset_id)
    counts["1m"] = count_1m
    if verbose and count_1m > 0:
        print(f"    1m: {count_1m:,} candles (chunked)")

    return counts


def sync_all_from_json(conn=None, verbose: bool = True):
    """
    Sync all enabled assets from static JSONs to canonical prices table.

    This is the main entry point called by:
    - migrate_to_canonical.py (initial population)
    - export_static.py (post-export sync)
    """
    should_close = False
    if conn is None:
        conn = duckdb.connect(str(DB_PATH))
        should_close = True

    if verbose:
        print("=" * 60)
        print("SYNCING: Static JSONs → prices (canonical table)")
        print("=" * 60)

    assets = get_enabled_assets()
    total_counts = {}

    for asset in assets:
        asset_id = asset["id"]
        counts = sync_asset(conn, asset_id, verbose)
        total_counts[asset_id] = counts

    if verbose:
        # Print summary
        print("\n" + "-" * 60)
        total_candles = sum(
            sum(counts.values())
            for counts in total_counts.values()
        )
        print(f"Total: {total_candles:,} candles synced to canonical table")
        print("=" * 60)

    if should_close:
        conn.close()

    return total_counts


def verify_counts(verbose: bool = True) -> bool:
    """
    Verify that canonical table counts match JSON counts.

    Returns True if all counts match, False otherwise.
    """
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    assets = get_enabled_assets()

    all_match = True

    if verbose:
        print("=" * 60)
        print("VERIFYING: prices table vs JSON files")
        print("=" * 60)

    for asset in assets:
        asset_id = asset["id"]
        if verbose:
            print(f"\n  {asset_id}:")

        for tf in TIMEFRAMES + ["1m"]:
            # Get JSON count
            if tf == "1m":
                index_path = STATIC_DIR / asset_id / "prices_1m_index.json"
                if index_path.exists():
                    with open(index_path) as f:
                        index = json.load(f)
                    json_count = sum(c.get("count", 0) for c in index.get("chunks", []))
                else:
                    json_count = 0
            else:
                json_path = STATIC_DIR / asset_id / f"prices_{tf}.json"
                if json_path.exists():
                    with open(json_path) as f:
                        data = json.load(f)
                    json_count = data.get("count", len(data.get("candles", [])))
                else:
                    json_count = 0

            # Get DB count
            result = conn.execute("""
                SELECT COUNT(*) FROM prices
                WHERE asset_id = ? AND timeframe = ?
            """, [asset_id, tf]).fetchone()
            db_count = result[0] if result else 0

            # Compare
            match = json_count == db_count
            if not match:
                all_match = False

            if verbose and (json_count > 0 or db_count > 0):
                status = "✓" if match else "✗"
                print(f"    {tf}: JSON={json_count:,} DB={db_count:,} {status}")

    conn.close()

    if verbose:
        print("\n" + "-" * 60)
        if all_match:
            print("RESULT: All counts match ✓")
        else:
            print("RESULT: Some counts don't match ✗")
        print("=" * 60)

    return all_match


def main():
    parser = argparse.ArgumentParser(
        description="Import canonical prices from static JSONs"
    )
    parser.add_argument(
        "--asset",
        help="Sync single asset (default: all enabled assets)"
    )
    parser.add_argument(
        "--verify",
        action="store_true",
        help="Verify counts match instead of syncing"
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress output"
    )

    args = parser.parse_args()
    verbose = not args.quiet

    if args.verify:
        success = verify_counts(verbose)
        sys.exit(0 if success else 1)

    if args.asset:
        conn = duckdb.connect(str(DB_PATH))
        sync_asset(conn, args.asset, verbose)
        conn.close()
    else:
        sync_all_from_json(verbose=verbose)


if __name__ == "__main__":
    main()
