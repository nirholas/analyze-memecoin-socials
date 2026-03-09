"""
Migration script to import existing data into the unified DuckDB database.
Handles:
- Per-asset tweets.json files
- Per-asset prices.db SQLite files
- Filters pre-launch tweets based on assets.json launch_date
- Populates ingestion_state for incremental fetching
"""
import json
import sqlite3
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

from db import (
    get_connection, init_schema, load_assets_from_json,
    get_asset, insert_tweets, insert_prices, update_ingestion_state,
    get_db_stats, ANALYTICS_DB, DATA_DIR, ASSETS_FILE
)


def parse_iso_timestamp(iso_str: str) -> datetime:
    """Parse ISO timestamp string to datetime."""
    # Handle various formats
    iso_str = iso_str.replace("Z", "+00:00")
    if "+" not in iso_str and "-" not in iso_str[-6:]:
        iso_str += "+00:00"
    return datetime.fromisoformat(iso_str)


def migrate_tweets_for_asset(
    conn,
    asset_id: str,
    tweets_file: Path,
    launch_date: datetime
) -> Dict[str, Any]:
    """
    Migrate tweets from JSON file for a single asset.
    Filters tweets before launch_date.
    Returns migration stats.
    """
    if not tweets_file.exists():
        return {"status": "skipped", "reason": "file not found", "file": str(tweets_file)}
    
    with open(tweets_file) as f:
        data = json.load(f)
    
    tweets_raw = data.get("tweets", [])
    if not tweets_raw:
        return {"status": "skipped", "reason": "no tweets in file"}
    
    # Filter and transform tweets
    tweets_to_insert = []
    skipped_pre_launch = 0
    
    for t in tweets_raw:
        tweet_time = parse_iso_timestamp(t["created_at"])
        
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
    
    # Insert tweets
    inserted = insert_tweets(conn, asset_id, tweets_to_insert)
    
    # Update ingestion state with the latest tweet ID
    if tweets_to_insert:
        # Sort by timestamp to get the latest
        sorted_tweets = sorted(tweets_to_insert, key=lambda x: x["created_at"])
        last_tweet = sorted_tweets[-1]
        update_ingestion_state(conn, asset_id, "tweets", last_id=last_tweet["id"])
    
    return {
        "status": "success",
        "total_in_file": len(tweets_raw),
        "skipped_pre_launch": skipped_pre_launch,
        "inserted": inserted,
    }


def migrate_prices_for_asset(
    conn,
    asset_id: str,
    prices_db: Path
) -> Dict[str, Any]:
    """
    Migrate prices from SQLite database for a single asset.
    Returns migration stats.
    """
    if not prices_db.exists():
        return {"status": "skipped", "reason": "file not found", "file": str(prices_db)}
    
    try:
        sqlite_conn = sqlite3.connect(prices_db)
    except Exception as e:
        return {"status": "error", "reason": str(e)}
    
    stats = {"status": "success", "timeframes": {}}
    
    # Get all timeframes
    cursor = sqlite_conn.execute("""
        SELECT DISTINCT timeframe FROM ohlcv ORDER BY timeframe
    """)
    timeframes = [row[0] for row in cursor.fetchall()]
    
    for tf in timeframes:
        # Fetch all candles for this timeframe
        cursor = sqlite_conn.execute("""
            SELECT timestamp_epoch, open, high, low, close, volume
            FROM ohlcv
            WHERE timeframe = ?
            ORDER BY timestamp_epoch
        """, (tf,))
        
        candles = []
        latest_ts = None
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
            if latest_ts is None or ts > latest_ts:
                latest_ts = ts
        
        if candles:
            inserted = insert_prices(conn, asset_id, tf, candles)
            stats["timeframes"][tf] = {
                "count": inserted,
                "latest": datetime.utcfromtimestamp(latest_ts).isoformat() if latest_ts else None,
            }
            
            # Update ingestion state with latest timestamp
            if latest_ts:
                update_ingestion_state(
                    conn, asset_id, f"prices_{tf}",
                    last_timestamp=datetime.utcfromtimestamp(latest_ts)
                )
    
    sqlite_conn.close()
    return stats


def run_migration(dry_run: bool = False, assets_filter: Optional[List[str]] = None):
    """
    Run the full migration.
    
    Args:
        dry_run: If True, don't actually write to database
        assets_filter: Optional list of asset IDs to migrate (None = all)
    """
    print("=" * 70)
    print("DuckDB Migration")
    print("=" * 70)
    print(f"Database: {ANALYTICS_DB}")
    print(f"Assets config: {ASSETS_FILE}")
    print(f"Dry run: {dry_run}")
    print()
    
    # Initialize database
    if not dry_run:
        conn = get_connection()
        init_schema(conn)
        load_assets_from_json(conn)
    else:
        conn = None
    
    # Load assets config
    with open(ASSETS_FILE) as f:
        config = json.load(f)
    
    assets = config.get("assets", [])
    if assets_filter:
        assets = [a for a in assets if a["id"] in assets_filter]
    
    print(f"Migrating {len(assets)} assets...\n")
    
    migration_results = {}
    
    for asset in assets:
        asset_id = asset["id"]
        launch_date = parse_iso_timestamp(asset["launch_date"])
        
        print(f"--- {asset['name']} ({asset_id}) ---")
        print(f"    Founder: @{asset['founder']}")
        print(f"    Launch: {launch_date.strftime('%Y-%m-%d')}")
        
        result = {"tweets": None, "prices": None}
        
        # Migrate tweets
        # Check both per-asset folder and root-level files
        tweets_file = DATA_DIR / asset_id / "tweets.json"
        if not tweets_file.exists() and asset_id == "pump":
            # Fallback to root-level for pump (legacy)
            tweets_file = DATA_DIR / "tweets.json"
        
        print(f"    Tweets: {tweets_file.name}...", end=" ")
        if not dry_run:
            tweet_result = migrate_tweets_for_asset(conn, asset_id, tweets_file, launch_date)
            result["tweets"] = tweet_result
            if tweet_result["status"] == "success":
                print(f"✓ {tweet_result['inserted']} inserted ({tweet_result['skipped_pre_launch']} pre-launch skipped)")
            else:
                print(f"⊘ {tweet_result.get('reason', 'unknown')}")
        else:
            print("(dry run)")
        
        # Migrate prices
        prices_db = DATA_DIR / asset_id / "prices.db"
        if not prices_db.exists() and asset_id == "pump":
            # Fallback to root-level for pump (legacy)
            prices_db = DATA_DIR / "prices.db"
        
        print(f"    Prices: {prices_db.name}...", end=" ")
        if not dry_run:
            price_result = migrate_prices_for_asset(conn, asset_id, prices_db)
            result["prices"] = price_result
            if price_result["status"] == "success":
                tf_summary = ", ".join(f"{tf}:{info['count']}" for tf, info in price_result.get("timeframes", {}).items())
                print(f"✓ {tf_summary or 'no data'}")
            else:
                print(f"⊘ {price_result.get('reason', 'unknown')}")
        else:
            print("(dry run)")
        
        migration_results[asset_id] = result
        print()
    
    # Print summary
    if not dry_run:
        print("=" * 70)
        print("MIGRATION SUMMARY")
        print("=" * 70)
        
        stats = get_db_stats(conn)
        print(f"\nTotal assets: {stats['assets']} ({stats['enabled_assets']} enabled)")
        
        print("\nTweets by asset:")
        for asset_id, info in stats.get("tweets_by_asset", {}).items():
            print(f"  {info['name']}: {info['count']:,} tweets")
        
        print("\nPrice data by asset:")
        for asset_id, info in stats.get("prices_by_asset", {}).items():
            print(f"  {info['name']}:")
            for tf, tf_info in info.get("timeframes", {}).items():
                start = tf_info['start'][:10] if tf_info.get('start') else 'N/A'
                end = tf_info['end'][:10] if tf_info.get('end') else 'N/A'
                print(f"    {tf}: {tf_info['count']:,} candles ({start} to {end})")
        
        conn.close()
        print(f"\nMigration complete! Database: {ANALYTICS_DB}")
    
    return migration_results


def verify_migration():
    """Verify migration by comparing counts with source files."""
    print("=" * 70)
    print("Migration Verification")
    print("=" * 70)
    
    conn = get_connection()
    
    with open(ASSETS_FILE) as f:
        config = json.load(f)
    
    all_good = True
    
    for asset in config.get("assets", []):
        asset_id = asset["id"]
        launch_date = parse_iso_timestamp(asset["launch_date"])
        
        print(f"\n{asset['name']} ({asset_id}):")
        
        # Check tweets
        tweets_file = DATA_DIR / asset_id / "tweets.json"
        if not tweets_file.exists() and asset_id == "pump":
            tweets_file = DATA_DIR / "tweets.json"
        
        if tweets_file.exists():
            with open(tweets_file) as f:
                source_tweets = json.load(f).get("tweets", [])
            
            # Count post-launch tweets in source
            source_post_launch = sum(
                1 for t in source_tweets 
                if parse_iso_timestamp(t["created_at"]) >= launch_date
            )
            
            # Count in database
            db_count = conn.execute("""
                SELECT COUNT(*) FROM tweets WHERE asset_id = ?
            """, [asset_id]).fetchone()[0]
            
            match = "✓" if source_post_launch == db_count else "✗"
            print(f"  Tweets: source={source_post_launch} (post-launch), db={db_count} {match}")
            if source_post_launch != db_count:
                all_good = False
        else:
            print(f"  Tweets: source file not found")
        
        # Check prices
        prices_db = DATA_DIR / asset_id / "prices.db"
        if not prices_db.exists() and asset_id == "pump":
            prices_db = DATA_DIR / "prices.db"
        
        if prices_db.exists():
            sqlite_conn = sqlite3.connect(prices_db)
            cursor = sqlite_conn.execute("""
                SELECT timeframe, COUNT(*) FROM ohlcv GROUP BY timeframe
            """)
            source_counts = {row[0]: row[1] for row in cursor}
            sqlite_conn.close()
            
            # Check each timeframe
            for tf, source_count in source_counts.items():
                db_count = conn.execute("""
                    SELECT COUNT(*) FROM prices WHERE asset_id = ? AND timeframe = ?
                """, [asset_id, tf]).fetchone()[0]
                
                match = "✓" if source_count == db_count else "✗"
                print(f"  Prices ({tf}): source={source_count}, db={db_count} {match}")
                if source_count != db_count:
                    all_good = False
        else:
            print(f"  Prices: source file not found")
    
    conn.close()
    
    print("\n" + "=" * 70)
    if all_good:
        print("Verification PASSED - all counts match!")
    else:
        print("Verification FAILED - some counts don't match")
    
    return all_good


def main():
    """CLI interface for migration."""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python migrate_to_duckdb.py <command> [options]")
        print("Commands:")
        print("  migrate [--dry-run] [--asset=ID]  - Run migration")
        print("  verify                            - Verify migration")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "migrate":
        dry_run = "--dry-run" in sys.argv
        
        # Parse asset filter
        assets_filter = None
        for arg in sys.argv[2:]:
            if arg.startswith("--asset="):
                assets_filter = [arg.split("=")[1]]
        
        run_migration(dry_run=dry_run, assets_filter=assets_filter)
    
    elif command == "verify":
        verify_migration()
    
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()


