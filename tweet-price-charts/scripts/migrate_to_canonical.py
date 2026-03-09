#!/usr/bin/env python3
"""
Migrate Database to Canonical Table Architecture

This is an IDEMPOTENT migration script that:
1. Renames `prices` → `prices_RAW_INGESTION` (raw API data)
2. Creates new `prices` table (canonical, matches website JSONs)
3. Populates canonical table from static JSONs

SAFE TO RUN MULTIPLE TIMES - will skip if already migrated.

This script is called:
- By GitHub Actions workflow (after cache restore, before fetch)
- Manually for local development

Usage:
    python migrate_to_canonical.py          # Run migration
    python migrate_to_canonical.py --check  # Check status without migrating
"""

import argparse
import sys
from pathlib import Path

import duckdb

# Paths
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
STATIC_DIR = PROJECT_ROOT / "web" / "public" / "static"
DB_PATH = PROJECT_ROOT / "data" / "analytics.duckdb"


def check_migration_status(conn) -> dict:
    """
    Check current migration status.

    Returns dict with:
        - migrated: bool (True if already migrated)
        - has_prices: bool (True if prices table exists)
        - has_raw: bool (True if prices_RAW_INGESTION exists)
    """
    tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]

    return {
        "migrated": "prices_RAW_INGESTION" in tables,
        "has_prices": "prices" in tables,
        "has_raw": "prices_RAW_INGESTION" in tables,
        "tables": tables,
    }


def migrate(conn, verbose: bool = True) -> bool:
    """
    Run the migration.

    Returns True if migration was performed, False if skipped.
    """
    status = check_migration_status(conn)

    if status["migrated"]:
        if verbose:
            print("Migration already complete, skipping")
            print(f"  prices_RAW_INGESTION exists: {status['has_raw']}")
            print(f"  prices exists: {status['has_prices']}")
        return False

    if not status["has_prices"]:
        if verbose:
            print("ERROR: No prices table found - nothing to migrate")
        return False

    if verbose:
        print("=" * 60)
        print("RUNNING ONE-TIME MIGRATION")
        print("=" * 60)

    # Step 1: Drop views that depend on prices table
    # These will be recreated after the new prices table is created
    if verbose:
        print("\n1. Dropping dependent views...")
    views_to_drop = ['tweet_events', 'tweet_events_daily', 'price_gaps', 'data_source_summary']
    for view in views_to_drop:
        try:
            conn.execute(f"DROP VIEW IF EXISTS {view}")
        except Exception:
            pass  # View may not exist
    if verbose:
        print("   Done ✓")

    # Step 1b: Drop indexes on prices table (they block rename)
    if verbose:
        print("\n1b. Dropping indexes on prices table...")
    indexes_to_drop = ['idx_prices_asset_tf_ts', 'idx_prices_source', 'idx_prices_asset_tf']
    for idx in indexes_to_drop:
        try:
            conn.execute(f"DROP INDEX IF EXISTS {idx}")
        except Exception:
            pass  # Index may not exist
    if verbose:
        print("   Done ✓")

    # Step 2: Rename current prices → prices_RAW_INGESTION
    if verbose:
        print("\n2. Renaming prices → prices_RAW_INGESTION...")
    conn.execute("ALTER TABLE prices RENAME TO prices_RAW_INGESTION")
    if verbose:
        print("   Done ✓")

    # Step 2b: Recreate indexes on raw table
    if verbose:
        print("\n2b. Creating indexes on prices_RAW_INGESTION...")
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_raw_prices_asset_tf_ts
        ON prices_RAW_INGESTION(asset_id, timeframe, timestamp)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_raw_prices_source
        ON prices_RAW_INGESTION(data_source)
    """)
    if verbose:
        print("   Done ✓")

    # Step 3: Create new canonical prices table
    if verbose:
        print("\n3. Creating new prices table (canonical)...")
    conn.execute("""
        CREATE TABLE prices (
            asset_id VARCHAR NOT NULL,
            timeframe VARCHAR NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            PRIMARY KEY (asset_id, timeframe, timestamp)
        )
    """)

    # Create index for efficient queries
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_prices_asset_tf
        ON prices(asset_id, timeframe)
    """)
    if verbose:
        print("   Done ✓")

    # Step 4: Recreate views (they now point to canonical prices table)
    if verbose:
        print("\n4. Recreating views...")
    from db import init_schema
    init_schema(conn)  # This recreates all views
    if verbose:
        print("   Done ✓")

    # Step 5: Check if JSONs exist before attempting sync
    if verbose:
        print("\n5. Checking for static JSON files...")

    if not STATIC_DIR.exists():
        if verbose:
            print(f"   WARNING: Static directory not found: {STATIC_DIR}")
            print("   Skipping initial sync - run export_static.py to populate")
        return True

    json_files = list(STATIC_DIR.glob("*/prices_1h.json"))
    if not json_files:
        if verbose:
            print("   WARNING: No JSON files found")
            print("   Skipping initial sync - run export_static.py to populate")
        return True

    if verbose:
        print(f"   Found {len(json_files)} assets with price data")

    # Step 6: Populate from JSONs
    if verbose:
        print("\n6. Populating canonical table from JSONs...")

    try:
        from import_canonical import sync_all_from_json
        sync_all_from_json(conn, verbose=verbose)
    except Exception as e:
        if verbose:
            print(f"   ERROR during sync: {e}")
            print("   Canonical table created but empty - run export_static.py to populate")
        return True

    if verbose:
        print("\n" + "=" * 60)
        print("MIGRATION COMPLETE ✓")
        print("=" * 60)
        print("\nNew architecture:")
        print("  prices              → Canonical (matches website)")
        print("  prices_RAW_INGESTION → Raw API data (debugging only)")

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Migrate database to canonical table architecture"
    )
    parser.add_argument(
        "--check",
        action="store_true",
        help="Check migration status without migrating"
    )
    parser.add_argument(
        "--quiet", "-q",
        action="store_true",
        help="Suppress output"
    )

    args = parser.parse_args()
    verbose = not args.quiet

    # Check if database exists
    if not DB_PATH.exists():
        if verbose:
            print(f"ERROR: Database not found: {DB_PATH}")
        sys.exit(1)

    conn = duckdb.connect(str(DB_PATH))

    if args.check:
        status = check_migration_status(conn)
        if verbose:
            print("Migration Status:")
            print(f"  Migrated: {status['migrated']}")
            print(f"  Tables: {', '.join(status['tables'])}")
        conn.close()
        sys.exit(0 if status["migrated"] else 1)

    try:
        performed = migrate(conn, verbose)
        conn.close()
        sys.exit(0)
    except Exception as e:
        if verbose:
            print(f"ERROR: Migration failed: {e}")
        conn.close()
        sys.exit(1)


if __name__ == "__main__":
    main()
