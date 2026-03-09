#!/usr/bin/env python3
"""
Unified Migration Script - Consolidate all data into DuckDB.

This script imports data from multiple sources into the unified DuckDB database:
- Tweets from data/{asset}/tweets.json
- Prices from data/{asset}/prices.db (SQLite)
- Prices from web/public/static/{asset}/prices_*.json (exported JSON)

Import order matters for deduplication (ON CONFLICT DO UPDATE):
- JSON prices imported FIRST
- SQLite prices imported SECOND (richer source wins on conflicts)

Usage:
    python migrate_unified.py --dry-run              # Preview without changes
    python migrate_unified.py --asset pump           # Migrate single asset
    python migrate_unified.py --asset pump --verify  # Verify only (no migration)
    python migrate_unified.py                        # Migrate all assets

Author: Migration script for Phase 1 data consolidation
"""
import argparse
import json
import sqlite3
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple

# Add scripts directory to path for imports
SCRIPTS_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPTS_DIR))

from db import (
    get_connection, init_schema, load_assets_from_json,
    get_asset, insert_tweets, insert_prices, update_ingestion_state,
    ANALYTICS_DB
)

# Paths
PROJECT_ROOT = SCRIPTS_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"
STATIC_DIR = PROJECT_ROOT / "web" / "public" / "static"
ASSETS_FILE = SCRIPTS_DIR / "assets.json"


def log(msg: str, level: str = "INFO"):
    """Print timestamped log message."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    prefix = {"INFO": "   ", "OK": " ✓ ", "WARN": " ⚠ ", "ERROR": " ✗ ", "SKIP": " · "}
    print(f"[{timestamp}]{prefix.get(level, '   ')}{msg}")


def log_section(title: str):
    """Print section header."""
    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")


# =============================================================================
# FORMAT TRANSFORMATIONS
# =============================================================================

def transform_compact_candle(c: Dict) -> Dict:
    """
    Transform exported JSON compact format to insert_prices() format.

    Compact: {"t": 1752523200, "o": 0.005, "h": 0.006, "l": 0.004, "c": 0.005, "v": 1000}
    Full:    {"timestamp_epoch": 1752523200, "open": 0.005, "high": 0.006, ...}
    """
    return {
        "timestamp_epoch": c["t"],
        "open": c["o"],
        "high": c["h"],
        "low": c["l"],
        "close": c["c"],
        "volume": c["v"],
    }


def parse_iso_timestamp(iso_str: str) -> datetime:
    """Parse ISO timestamp string to datetime."""
    iso_str = iso_str.replace("Z", "+00:00")
    if "+" not in iso_str and "-" not in iso_str[-6:]:
        iso_str += "+00:00"
    try:
        return datetime.fromisoformat(iso_str)
    except ValueError:
        # Handle some edge cases
        return datetime.strptime(iso_str[:19], "%Y-%m-%dT%H:%M:%S")


# =============================================================================
# TWEET MIGRATION
# =============================================================================

def migrate_tweets_from_json(
    conn,
    asset_id: str,
    tweets_file: Path,
    launch_date: datetime,
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Migrate tweets from legacy JSON file to DuckDB.

    Returns dict with migration stats or error info.
    """
    if not tweets_file.exists():
        return {"status": "skipped", "reason": "file not found"}

    try:
        with open(tweets_file) as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        return {"status": "error", "reason": f"JSON parse error: {e}"}

    tweets_raw = data.get("tweets", [])
    if not tweets_raw:
        return {"status": "skipped", "reason": "no tweets in file"}

    # Filter and transform tweets
    tweets_to_insert = []
    skipped_pre_launch = 0

    for t in tweets_raw:
        try:
            tweet_time = parse_iso_timestamp(t["created_at"])
        except (KeyError, ValueError) as e:
            continue  # Skip malformed tweets

        # Skip tweets before launch
        if tweet_time < launch_date:
            skipped_pre_launch += 1
            continue

        tweets_to_insert.append({
            "id": t["id"],
            "created_at": tweet_time,
            "text": t.get("text"),
            "likes": t.get("likes", 0),
            "retweets": t.get("retweets", 0),
            "replies": t.get("replies", 0),
            "impressions": t.get("impressions", 0),
        })

    if dry_run:
        return {
            "status": "dry_run",
            "would_insert": len(tweets_to_insert),
            "skipped_pre_launch": skipped_pre_launch,
        }

    # Insert tweets
    inserted = insert_tweets(conn, asset_id, tweets_to_insert)

    # Update ingestion state
    if tweets_to_insert:
        sorted_tweets = sorted(tweets_to_insert, key=lambda x: x["created_at"])
        last_tweet = sorted_tweets[-1]
        update_ingestion_state(conn, asset_id, "tweets", last_id=last_tweet["id"])

    return {
        "status": "success",
        "inserted": inserted,
        "skipped_pre_launch": skipped_pre_launch,
    }


# =============================================================================
# PRICE MIGRATION - FROM SQLITE
# =============================================================================

def migrate_prices_from_sqlite(
    conn,
    asset_id: str,
    sqlite_db: Path,
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Migrate prices from legacy SQLite database to DuckDB.

    Returns dict with migration stats per timeframe.
    """
    if not sqlite_db.exists():
        return {"status": "skipped", "reason": "file not found"}

    try:
        sqlite_conn = sqlite3.connect(sqlite_db)
    except Exception as e:
        return {"status": "error", "reason": f"SQLite connection error: {e}"}

    stats = {"status": "success", "timeframes": {}}

    # Get all timeframes in this database
    try:
        cursor = sqlite_conn.execute("SELECT DISTINCT timeframe FROM ohlcv ORDER BY timeframe")
        timeframes = [row[0] for row in cursor.fetchall()]
    except Exception as e:
        sqlite_conn.close()
        return {"status": "error", "reason": f"Query error: {e}"}

    for tf in timeframes:
        cursor = sqlite_conn.execute("""
            SELECT timestamp_epoch, open, high, low, close, volume
            FROM ohlcv
            WHERE timeframe = ?
            ORDER BY timestamp_epoch
        """, (tf,))

        candles = []
        for row in cursor:
            ts, o, h, l, c, v = row
            candles.append({
                "timestamp_epoch": ts,
                "open": o,
                "high": h,
                "low": l,
                "close": c,
                "volume": v,
            })

        if not candles:
            continue

        if dry_run:
            stats["timeframes"][tf] = {"would_insert": len(candles)}
        else:
            inserted = insert_prices(conn, asset_id, tf, candles, data_source="sqlite_migration")
            stats["timeframes"][tf] = {"inserted": inserted}

    sqlite_conn.close()
    return stats


# =============================================================================
# PRICE MIGRATION - FROM EXPORTED JSON
# =============================================================================

def migrate_prices_from_exported_json(
    conn,
    asset_id: str,
    static_dir: Path,
    dry_run: bool = False
) -> Dict[str, Any]:
    """
    Migrate prices from exported JSON files (web/public/static/{asset}/).

    Handles compact format transformation and discovers timeframes dynamically.
    Returns dict with migration stats per timeframe.
    """
    if not static_dir.exists():
        return {"status": "skipped", "reason": "directory not found"}

    stats = {"status": "success", "timeframes": {}}

    # Find all price JSON files (excluding index and chunk files)
    price_files = []
    for f in static_dir.glob("prices_*.json"):
        # Skip 1m index and monthly chunks (we'll handle 1m specially)
        if "index" in f.name or f.name.startswith("prices_1m_2"):
            continue
        price_files.append(f)

    # Also check for 1m data via index
    index_file = static_dir / "prices_1m_index.json"
    has_1m_chunks = index_file.exists()

    for price_file in price_files:
        # Extract timeframe from filename: prices_1d.json -> 1d
        tf = price_file.stem.replace("prices_", "")

        try:
            with open(price_file) as f:
                data = json.load(f)
        except json.JSONDecodeError as e:
            stats["timeframes"][tf] = {"status": "error", "reason": f"JSON parse error: {e}"}
            continue

        candles_raw = data.get("candles", [])
        if not candles_raw:
            stats["timeframes"][tf] = {"status": "skipped", "reason": "no candles"}
            continue

        # Transform compact format to full format
        candles = [transform_compact_candle(c) for c in candles_raw]

        if dry_run:
            stats["timeframes"][tf] = {"would_insert": len(candles)}
        else:
            inserted = insert_prices(conn, asset_id, tf, candles, data_source="json_import")
            stats["timeframes"][tf] = {"inserted": inserted}

    # Handle 1m chunked data if present
    if has_1m_chunks:
        try:
            with open(index_file) as f:
                index = json.load(f)

            all_1m_candles = []
            for chunk_info in index.get("chunks", []):
                chunk_file = static_dir / chunk_info["file"]
                if chunk_file.exists():
                    with open(chunk_file) as f:
                        chunk_data = json.load(f)
                    chunk_candles = chunk_data.get("candles", [])
                    all_1m_candles.extend([transform_compact_candle(c) for c in chunk_candles])

            if all_1m_candles:
                if dry_run:
                    stats["timeframes"]["1m"] = {"would_insert": len(all_1m_candles)}
                else:
                    inserted = insert_prices(conn, asset_id, "1m", all_1m_candles, data_source="json_import")
                    stats["timeframes"]["1m"] = {"inserted": inserted}
        except Exception as e:
            stats["timeframes"]["1m"] = {"status": "error", "reason": str(e)}

    return stats


# =============================================================================
# VERIFICATION
# =============================================================================

def verify_asset(conn, asset_id: str) -> Tuple[bool, Dict[str, Any]]:
    """
    Verify migration for a single asset.

    Returns (success: bool, details: dict)
    """
    details = {"tweets": {}, "prices": {}, "tweet_events": {}}
    issues = []

    # Check tweets
    result = conn.execute("""
        SELECT COUNT(*), MIN(timestamp), MAX(timestamp)
        FROM tweets WHERE asset_id = ?
    """, [asset_id]).fetchone()
    details["tweets"] = {
        "count": result[0],
        "earliest": str(result[1]) if result[1] else None,
        "latest": str(result[2]) if result[2] else None,
    }

    # Check prices by timeframe
    result = conn.execute("""
        SELECT timeframe, COUNT(*), MIN(timestamp), MAX(timestamp)
        FROM prices WHERE asset_id = ?
        GROUP BY timeframe ORDER BY timeframe
    """, [asset_id]).fetchall()

    for tf, count, min_ts, max_ts in result:
        details["prices"][tf] = {
            "count": count,
            "earliest": str(min_ts) if min_ts else None,
            "latest": str(max_ts) if max_ts else None,
        }

    # Check tweet_events (joined view)
    result = conn.execute("""
        SELECT COUNT(*),
               SUM(CASE WHEN price_at_tweet IS NOT NULL THEN 1 ELSE 0 END)
        FROM tweet_events WHERE asset_id = ?
    """, [asset_id]).fetchone()
    details["tweet_events"] = {
        "total": result[0],
        "with_price": result[1],
        "without_price": result[0] - result[1] if result[0] and result[1] else 0,
    }

    # Determine if successful
    if details["tweets"]["count"] == 0:
        issues.append("No tweets in DuckDB")

    if not details["prices"]:
        issues.append("No prices in DuckDB")

    if details["tweet_events"]["total"] > 0 and details["tweet_events"]["with_price"] == 0:
        issues.append("All tweet_events missing price data")

    success = len(issues) == 0
    details["issues"] = issues

    return success, details


# =============================================================================
# MAIN MIGRATION LOGIC
# =============================================================================

def migrate_asset(
    conn,
    asset_id: str,
    asset_config: Dict,
    dry_run: bool = False
) -> Tuple[bool, Dict[str, Any]]:
    """
    Migrate a single asset from all available sources.

    Order: JSON prices first, SQLite prices second (richer source wins).

    Returns (success: bool, results: dict)
    """
    results = {
        "tweets": None,
        "prices_json": None,
        "prices_sqlite": None,
    }

    launch_date = parse_iso_timestamp(asset_config["launch_date"])

    log_section(f"Migrating {asset_config['name']} ({asset_id})")
    log(f"Founder: @{asset_config['founder']}")
    log(f"Launch: {launch_date.strftime('%Y-%m-%d')}")

    # --- TWEETS ---
    tweets_file = DATA_DIR / asset_id / "tweets.json"
    log(f"Tweets from: {tweets_file.name}")

    if tweets_file.exists():
        result = migrate_tweets_from_json(conn, asset_id, tweets_file, launch_date, dry_run)
        results["tweets"] = result

        if result["status"] == "success":
            log(f"Inserted {result['inserted']} tweets (skipped {result['skipped_pre_launch']} pre-launch)", "OK")
        elif result["status"] == "dry_run":
            log(f"Would insert {result['would_insert']} tweets", "INFO")
        else:
            log(f"Tweets: {result.get('reason', 'unknown error')}", "WARN")
    else:
        log("No tweets.json file found", "SKIP")

    # --- PRICES FROM JSON (FIRST - will be overwritten by SQLite if overlap) ---
    static_dir = STATIC_DIR / asset_id
    log(f"Prices from JSON: {static_dir.relative_to(PROJECT_ROOT)}")

    if static_dir.exists():
        result = migrate_prices_from_exported_json(conn, asset_id, static_dir, dry_run)
        results["prices_json"] = result

        if result["status"] == "success" and result.get("timeframes"):
            for tf, tf_result in result["timeframes"].items():
                if "inserted" in tf_result:
                    log(f"  {tf}: {tf_result['inserted']:,} candles", "OK")
                elif "would_insert" in tf_result:
                    log(f"  {tf}: would insert {tf_result['would_insert']:,} candles", "INFO")
        elif result["status"] == "skipped":
            log(f"JSON prices: {result.get('reason')}", "SKIP")
    else:
        log("No static directory found", "SKIP")

    # --- PRICES FROM SQLITE (SECOND - richer source wins) ---
    sqlite_db = DATA_DIR / asset_id / "prices.db"
    log(f"Prices from SQLite: {sqlite_db.name}")

    if sqlite_db.exists():
        result = migrate_prices_from_sqlite(conn, asset_id, sqlite_db, dry_run)
        results["prices_sqlite"] = result

        if result["status"] == "success" and result.get("timeframes"):
            for tf, tf_result in result["timeframes"].items():
                if "inserted" in tf_result:
                    log(f"  {tf}: {tf_result['inserted']:,} candles (overwrites JSON)", "OK")
                elif "would_insert" in tf_result:
                    log(f"  {tf}: would insert {tf_result['would_insert']:,} candles", "INFO")
        elif result["status"] == "skipped":
            log(f"SQLite prices: {result.get('reason')}", "SKIP")
    else:
        log("No prices.db file found", "SKIP")

    # --- VERIFICATION (only if not dry run) ---
    if not dry_run:
        log("Verifying migration...")
        success, details = verify_asset(conn, asset_id)
        results["verification"] = details

        if success:
            log(f"Verification passed: {details['tweets']['count']} tweets, "
                f"{sum(p['count'] for p in details['prices'].values())} prices, "
                f"{details['tweet_events']['with_price']}/{details['tweet_events']['total']} events with prices", "OK")
        else:
            for issue in details["issues"]:
                log(f"VERIFICATION FAILED: {issue}", "ERROR")
            return False, results

    return True, results


def main():
    parser = argparse.ArgumentParser(
        description="Unified migration: consolidate all data into DuckDB"
    )
    parser.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Preview changes without writing to database"
    )
    parser.add_argument(
        "--asset", "-a",
        type=str,
        help="Migrate specific asset only (default: all enabled)"
    )
    parser.add_argument(
        "--verify", "-v",
        action="store_true",
        help="Verify only, no migration"
    )
    parser.add_argument(
        "--skip-useless",
        action="store_true",
        help="Skip USELESS (already migrated correctly)"
    )

    args = parser.parse_args()

    print("\n" + "="*60)
    print("  UNIFIED MIGRATION TO DUCKDB")
    print("="*60)
    print(f"  Database: {ANALYTICS_DB}")
    print(f"  Mode: {'DRY RUN' if args.dry_run else 'VERIFY ONLY' if args.verify else 'LIVE MIGRATION'}")
    print("="*60)

    # Load assets config
    with open(ASSETS_FILE) as f:
        config = json.load(f)

    assets = [a for a in config.get("assets", []) if a.get("enabled", True)]

    if args.asset:
        assets = [a for a in assets if a["id"] == args.asset]
        if not assets:
            print(f"\nERROR: Asset '{args.asset}' not found or not enabled")
            sys.exit(1)

    if args.skip_useless:
        assets = [a for a in assets if a["id"] != "useless"]

    print(f"\n  Assets to process: {', '.join(a['id'] for a in assets)}")

    # Initialize database connection
    conn = get_connection()
    init_schema(conn)
    load_assets_from_json(conn)

    # Verify only mode
    if args.verify:
        print("\n" + "="*60)
        print("  VERIFICATION RESULTS")
        print("="*60)

        all_passed = True
        for asset in assets:
            success, details = verify_asset(conn, asset["id"])
            status = "✓ PASS" if success else "✗ FAIL"
            print(f"\n  {asset['id']}: {status}")
            print(f"    Tweets: {details['tweets']['count']}")
            price_summary = ', '.join(f"{tf}:{p['count']}" for tf, p in details['prices'].items())
            print(f"    Prices: {price_summary or 'none'}")
            print(f"    Events: {details['tweet_events']['with_price']}/{details['tweet_events']['total']} with prices")
            if details["issues"]:
                for issue in details["issues"]:
                    print(f"    ⚠ {issue}")
                all_passed = False

        conn.close()
        sys.exit(0 if all_passed else 1)

    # Migration mode
    all_results = {}
    failed_assets = []

    for asset in assets:
        success, results = migrate_asset(conn, asset["id"], asset, dry_run=args.dry_run)
        all_results[asset["id"]] = results

        if not success:
            failed_assets.append(asset["id"])
            print(f"\n{'!'*60}")
            print(f"  STOPPING: {asset['id']} migration/verification failed")
            print(f"  Fix issues before continuing to next asset")
            print(f"{'!'*60}")
            break

    conn.close()

    # Summary
    log_section("MIGRATION SUMMARY")

    for asset_id, results in all_results.items():
        status = "✗ FAILED" if asset_id in failed_assets else "✓ OK" if not args.dry_run else "· DRY RUN"
        print(f"  {asset_id}: {status}")

    if args.dry_run:
        print(f"\n  This was a DRY RUN. No changes were made.")
        print(f"  To execute, run without --dry-run flag.")
    elif failed_assets:
        print(f"\n  Migration stopped due to failures: {', '.join(failed_assets)}")
        sys.exit(1)
    else:
        print(f"\n  Migration completed successfully!")
        print(f"  Next step: python scripts/export_static.py")

    sys.exit(0)


if __name__ == "__main__":
    main()
