"""
Export data from DuckDB as static JSON files for the frontend.
Generates per-asset directories with prices and tweet events.

This is the final step in the data pipeline:
    fetch_tweets.py / fetch_prices.py  ->  analytics.duckdb  ->  export_static.py  ->  JSON files

AUTOMATIC PROCESSING:
    This script automatically handles steps that were previously manual:
    1. Keyword filtering - For assets with keyword_filter, auto-applies filter
       before export (ensures adopter tweets are properly filtered)
    2. Stats computation - Auto-computes stats.json after each asset export
       (ensures correlation data is always up to date)

    You no longer need to manually run apply_keyword_filter.py or compute_stats.py.

Output structure:
    web/public/static/
        assets.json                 # Copy of asset config for frontend
        {asset_id}/
            prices_1d.json          # Daily OHLCV candles
            prices_1h.json          # Hourly OHLCV candles
            prices_15m.json         # 15-minute OHLCV candles
            prices_1m_index.json    # Index for chunked 1m data
            prices_1m_2025-07.json  # Monthly chunks for 1m data
            tweet_events.json       # Tweets with aligned price data
            stats.json              # Correlation and statistical analysis

Usage:
    python export_static.py                 # Export all enabled assets
    python export_static.py --asset pump    # Export specific asset
"""
import argparse
import json
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional

from config import PUBLIC_DATA_DIR, ASSETS_FILE
from db import (
    get_connection, init_schema, get_asset, get_enabled_assets,
    get_tweet_events, get_raw_price_table
)
from apply_keyword_filter import apply_filter_to_asset, get_filter_stats
from compute_stats import compute_stats_for_asset, save_stats


# =============================================================================
# DATA OVERRIDES - Manual fixes that persist across re-fetches
# =============================================================================
#
# The data_overrides.json file contains manual fixes for data quality issues.
# These are applied at EXPORT time, not stored in the database.
#
# This ensures:
# 1. Raw data in DB is never corrupted
# 2. Fixes persist even if data is re-fetched
# 3. All fixes are documented and auditable
# 4. Future developers/agents can understand what was fixed and why
#
# DO NOT delete entries from data_overrides.json without understanding why
# they were added. See GOTCHAS.md.
# =============================================================================

OVERRIDES_FILE = Path(__file__).parent / "data_overrides.json"
_overrides_cache = None


def load_overrides() -> Dict[str, Any]:
    """Load data overrides from JSON file. Cached after first load."""
    global _overrides_cache
    if _overrides_cache is not None:
        return _overrides_cache

    if not OVERRIDES_FILE.exists():
        print("  [WARN] data_overrides.json not found - no manual fixes will be applied")
        _overrides_cache = {}
        return _overrides_cache

    with open(OVERRIDES_FILE) as f:
        _overrides_cache = json.load(f)

    # Count overrides for logging
    price_count = len(_overrides_cache.get("price_overrides", {}).get("entries", []))
    tweet_count = len(_overrides_cache.get("tweet_exclusions", {}).get("entries", []))
    range_count = len(_overrides_cache.get("asset_data_ranges", {}).get("entries", []))

    if price_count + tweet_count + range_count > 0:
        print(f"  Loaded {price_count} price overrides, {tweet_count} tweet exclusions, {range_count} range filters")

    return _overrides_cache


def get_price_overrides(asset_id: str, timeframe: str) -> Dict[int, Dict]:
    """
    Get price overrides for an asset/timeframe as a dict of timestamp -> override.

    Returns: {unix_timestamp: {"action": "cap_high", "value": 0.009, ...}}
    """
    overrides = load_overrides()
    entries = overrides.get("price_overrides", {}).get("entries", [])

    result = {}
    for entry in entries:
        if entry.get("asset_id") == asset_id and entry.get("timeframe") == timeframe:
            # Parse timestamp to Unix epoch
            ts_str = entry.get("timestamp")
            if ts_str:
                ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
                ts_epoch = int(ts.timestamp())
                result[ts_epoch] = entry

    return result


def get_asset_date_range(asset_id: str, data_type: str = "prices") -> Optional[datetime]:
    """
    Get minimum date for an asset's data (beyond launch_date filter).

    Returns: datetime or None if no override exists
    """
    overrides = load_overrides()
    entries = overrides.get("asset_data_ranges", {}).get("entries", [])

    for entry in entries:
        if entry.get("asset_id") == asset_id and entry.get("type") == data_type:
            min_date_str = entry.get("min_date")
            if min_date_str:
                return datetime.fromisoformat(min_date_str.replace("Z", "+00:00"))

    return None


def get_excluded_tweet_ids(asset_id: str) -> set:
    """Get set of tweet IDs to exclude from export."""
    overrides = load_overrides()
    entries = overrides.get("tweet_exclusions", {}).get("entries", [])

    return {
        entry.get("tweet_id")
        for entry in entries
        if entry.get("asset_id") == asset_id and entry.get("tweet_id")
    }


def apply_price_override(candle: Dict, override: Dict) -> Dict:
    """Apply a price override to a candle. Returns modified candle."""
    action = override.get("action")
    value = override.get("value")

    if action == "cap_high" and value is not None:
        if candle.get("h", 0) > value:
            candle["h"] = round(value, 8)
    elif action == "cap_low" and value is not None:
        if candle.get("l", 0) < value:
            candle["l"] = round(value, 8)
    elif action == "exclude":
        return None  # Signal to skip this candle

    return candle


# =============================================================================
# FAKE WICK DETECTION - Automatic outlier capping at export time
# =============================================================================
#
# Fake wicks occur when HIGH spikes way above both OPEN and CLOSE (or LOW dips
# way below both). These are typically MEV bots, fat fingers, or data errors.
#
# Detection: If HIGH > max(OPEN, CLOSE) * WICK_CAP_MULTIPLIER, cap it.
# This preserves legitimate crashes (where HIGH ≈ OPEN) while removing spikes.
#
# See GOTCHAS.md for context.
# =============================================================================

WICK_CAP_MULTIPLIER = 2.0  # Allow wicks up to 2x the candle body


def cap_fake_wicks(o: float, h: float, l: float, c: float) -> tuple:
    """
    Cap fake wicks that spike beyond reasonable bounds.

    A fake wick has HIGH way above both OPEN and CLOSE (or LOW way below both).
    We cap HIGH to max(O,C) * 2 and LOW to min(O,C) / 2.

    Returns (h, l) - potentially capped values.
    """
    if o is None or h is None or l is None or c is None:
        return h, l

    if o <= 0 or c <= 0:
        return h, l

    max_body = max(o, c)
    min_body = min(o, c)

    # Cap upper wick
    max_allowed_high = max_body * WICK_CAP_MULTIPLIER
    capped_h = min(h, max_allowed_high) if h > max_allowed_high else h

    # Cap lower wick
    min_allowed_low = min_body / WICK_CAP_MULTIPLIER
    capped_l = max(l, min_allowed_low) if l < min_allowed_low else l

    return capped_h, capped_l


def export_prices_for_asset(
    conn,
    asset_id: str,
    output_dir: Path,
    force: bool = False,
) -> Dict[str, int]:
    """
    Export price data for a single asset.

    Uses age-based thresholds from config.py to auto-skip granular timeframes.
    Also respects skip_timeframes from assets.json.
    Deletes old files for skipped timeframes.

    Args:
        force: If True, bypass age-based skipping for granular timeframes.

    Returns dict of timeframe -> candle count.
    """
    from config import SKIP_1M_AFTER_DAYS, SKIP_15M_AFTER_DAYS

    output_dir.mkdir(parents=True, exist_ok=True)
    stats = {}

    # Get asset config to check for skip_timeframes
    with open(ASSETS_FILE) as f:
        assets_data = json.load(f)
    asset_config = next((a for a in assets_data.get("assets", []) if a["id"] == asset_id), {})
    skip_timeframes = set(asset_config.get("skip_timeframes", []))

    # Auto-skip based on asset age (same thresholds as fetch_prices.py)
    asset_info = conn.execute("""
        SELECT launch_date FROM assets WHERE id = ?
    """, [asset_id]).fetchone()

    if asset_info and asset_info[0] and not force:
        days_old = (datetime.utcnow() - asset_info[0]).days
        if days_old > SKIP_1M_AFTER_DAYS and "1m" not in skip_timeframes:
            skip_timeframes.add("1m")
            print(f"    (Skipping 1m data - asset is {days_old} days old, threshold: {SKIP_1M_AFTER_DAYS}d)")
        if days_old > SKIP_15M_AFTER_DAYS and "15m" not in skip_timeframes:
            skip_timeframes.add("15m")
            print(f"    (Skipping 15m data - asset is {days_old} days old, threshold: {SKIP_15M_AFTER_DAYS}d)")
    elif force:
        print(f"    (--force: Bypassing age-based timeframe skipping)")

    # Delete existing files for skipped timeframes
    for tf in skip_timeframes:
        old_file = output_dir / f"prices_{tf}.json"
        if old_file.exists():
            old_file.unlink()
            print(f"    (Deleted old {tf} file)")
    
    # Get available timeframes for this asset
    # Read from RAW table (prices_RAW_INGESTION) after migration
    raw_table = get_raw_price_table(conn)
    timeframes = conn.execute(f"""
        SELECT DISTINCT timeframe FROM {raw_table}
        WHERE asset_id = ?
        ORDER BY timeframe
    """, [asset_id]).fetchall()
    timeframes = [t[0] for t in timeframes]
    
    for tf in timeframes:
        if tf in skip_timeframes:
            # Already logged above when added to skip_timeframes
            continue

        if tf == "1m":
            # Export 1m data chunked by month
            count = export_1m_chunked(conn, asset_id, output_dir)
        else:
            count = export_timeframe(conn, asset_id, tf, output_dir)

        if count > 0:
            stats[tf] = count
    
    return stats


def export_timeframe(
    conn,
    asset_id: str,
    timeframe: str,
    output_dir: Path
) -> int:
    """Export all data for a timeframe to JSON."""
    # Read from RAW table (prices_RAW_INGESTION) after migration
    raw_table = get_raw_price_table(conn)

    # Check for date range override (e.g., JUP only showing Apr 2025+)
    min_date = get_asset_date_range(asset_id, "prices")

    if min_date:
        cursor = conn.execute(f"""
            SELECT timestamp, open, high, low, close, volume
            FROM {raw_table}
            WHERE asset_id = ? AND timeframe = ? AND timestamp >= ?
            ORDER BY timestamp
        """, [asset_id, timeframe, min_date])
    else:
        cursor = conn.execute(f"""
            SELECT timestamp, open, high, low, close, volume
            FROM {raw_table}
            WHERE asset_id = ? AND timeframe = ?
            ORDER BY timestamp
        """, [asset_id, timeframe])

    # Load manual price overrides for this asset/timeframe
    price_overrides = get_price_overrides(asset_id, timeframe)

    candles = []
    seen_timestamps = set()
    duplicates_skipped = 0
    wicks_capped = 0
    overrides_applied = 0

    normalized_count = 0

    for row in cursor.fetchall():
        ts, o, h, l, c, v = row
        # Convert to Unix timestamp
        ts_epoch = int(ts.timestamp()) if hasattr(ts, 'timestamp') else ts

        # =================================================================
        # 1D TIMESTAMP NORMALIZATION - DO NOT REMOVE
        # =================================================================
        # Daily candles can come from multiple sources with different timestamp
        # conventions (midnight UTC, 4am UTC, 5am UTC, etc.). This causes the
        # "DOUBLE CANDLE" issue where charts show 2+ candles per day.
        #
        # Solution: Normalize all 1D timestamps to midnight UTC (00:00:00).
        # This snaps any timestamp to the start of its day.
        #
        # See GOTCHAS.md and docs/DATA_QUALITY_ISSUES.md for full context.
        # =================================================================
        if timeframe == "1d":
            # Snap to midnight UTC: floor divide by 86400 seconds, multiply back
            normalized_ts = (ts_epoch // 86400) * 86400
            if normalized_ts != ts_epoch:
                normalized_count += 1
            ts_epoch = normalized_ts

        # =================================================================
        # DST DEDUPLICATION - DO NOT REMOVE
        # =================================================================
        # During DST "fall back" transitions, two different wall-clock times
        # can map to the SAME Unix timestamp. lightweight-charts CRASHES on
        # duplicate timestamps with "Value is null" error.
        # See GOTCHAS.md for full context.
        # =================================================================
        if ts_epoch in seen_timestamps:
            duplicates_skipped += 1
            continue
        seen_timestamps.add(ts_epoch)

        # Cap fake wicks (MEV/fat finger spikes)
        capped_h, capped_l = cap_fake_wicks(o, h, l, c)
        if capped_h != h or capped_l != l:
            wicks_capped += 1

        # Build candle object
        candle = {
            "t": ts_epoch,
            "o": round(o, 8) if o else 0,
            "h": round(capped_h, 8) if capped_h else 0,
            "l": round(capped_l, 8) if capped_l else 0,
            "c": round(c, 8) if c else 0,
            "v": round(v, 2) if v else 0,
        }

        # =================================================================
        # APPLY MANUAL OVERRIDES from data_overrides.json
        # =================================================================
        # These are specific fixes that must persist across re-fetches.
        # See data_overrides.json for documentation of each fix.
        # =================================================================
        if ts_epoch in price_overrides:
            override = price_overrides[ts_epoch]
            candle = apply_price_override(candle, override)
            if candle is None:
                continue  # Skip this candle (excluded)
            overrides_applied += 1

        candles.append(candle)

    if normalized_count > 0:
        print(f"    (Normalized {normalized_count} timestamps to midnight UTC)")
    if duplicates_skipped > 0:
        print(f"    (Deduplicated {duplicates_skipped} candles - same day from different sources)")
    if wicks_capped > 0:
        print(f"    (Capped {wicks_capped} fake wicks)")
    if overrides_applied > 0:
        print(f"    (Applied {overrides_applied} manual overrides from data_overrides.json)")

    if not candles:
        return 0
    
    output = {
        "asset_id": asset_id,
        "timeframe": timeframe,
        "count": len(candles),
        "start": candles[0]["t"],
        "end": candles[-1]["t"],
        "candles": candles
    }
    
    filename = f"prices_{timeframe}.json"
    filepath = output_dir / filename

    with open(filepath, "w") as f:
        json.dump(output, f, separators=(",", ":"))

    # =================================================================
    # SAFETY CHECK - DO NOT REMOVE
    # =================================================================
    # Verify no duplicate timestamps in output.
    # lightweight-charts crashes with "Value is null" on duplicates.
    # This check catches any bugs in the deduplication logic above.
    # =================================================================
    timestamps = [c["t"] for c in candles]
    if len(timestamps) != len(set(timestamps)):
        raise ValueError(f"CRITICAL: Duplicate timestamps in {filepath} - this will crash the chart!")

    size_kb = filepath.stat().st_size / 1024
    print(f"    {timeframe}: {len(candles):,} candles ({size_kb:.1f} KB)")

    return len(candles)


def export_1m_chunked(
    conn,
    asset_id: str,
    output_dir: Path
) -> int:
    """
    Export 1m data chunked by month for lazy loading.

    WHY CHUNKED BY MONTH:
    Large assets can have millions of 1-minute candles. We chunk by month for:
    1. Lazy loading in frontend (only load visible range)
    2. Smaller individual file sizes
    3. Efficient cache invalidation (only update current month)

    Output structure:
        prices_1m_index.json     # Lists all chunks with date ranges
        prices_1m_2025-07.json   # July 2025 data
        prices_1m_2025-08.json   # August 2025 data

    DO NOT switch to a single prices_1m.json - it will break the frontend.
    See GOTCHAS.md.
    """
    # Read from RAW table (prices_RAW_INGESTION) after migration
    raw_table = get_raw_price_table(conn)
    cursor = conn.execute(f"""
        SELECT timestamp, open, high, low, close, volume
        FROM {raw_table}
        WHERE asset_id = ? AND timeframe = '1m'
        ORDER BY timestamp
    """, [asset_id])
    
    # Group by month, deduplicating DST artifacts
    months = {}
    seen_timestamps = set()
    duplicates_skipped = 0
    wicks_capped = 0

    for row in cursor.fetchall():
        ts, o, h, l, c, v = row

        if hasattr(ts, 'strftime'):
            month_key = ts.strftime("%Y-%m")
            ts_epoch = int(ts.timestamp())
        else:
            dt = datetime.utcfromtimestamp(ts)
            month_key = dt.strftime("%Y-%m")
            ts_epoch = ts

        # DST DEDUPLICATION - DO NOT REMOVE (see comment in export_timeframe)
        if ts_epoch in seen_timestamps:
            duplicates_skipped += 1
            continue
        seen_timestamps.add(ts_epoch)

        # Cap fake wicks (MEV/fat finger spikes)
        capped_h, capped_l = cap_fake_wicks(o, h, l, c)
        if capped_h != h or capped_l != l:
            wicks_capped += 1

        if month_key not in months:
            months[month_key] = []

        months[month_key].append({
            "t": ts_epoch,
            "o": round(o, 8) if o else 0,
            "h": round(capped_h, 8) if capped_h else 0,
            "l": round(capped_l, 8) if capped_l else 0,
            "c": round(c, 8) if c else 0,
            "v": round(v, 2) if v else 0,
        })

    if duplicates_skipped > 0:
        print(f"    (Skipped {duplicates_skipped} duplicate 1m timestamps - DST artifacts)")
    if wicks_capped > 0:
        print(f"    (Capped {wicks_capped} fake wicks in 1m data)")

    if not months:
        return 0
    
    total = 0
    
    for month_key, candles in sorted(months.items()):
        output = {
            "asset_id": asset_id,
            "timeframe": "1m",
            "month": month_key,
            "count": len(candles),
            "start": candles[0]["t"],
            "end": candles[-1]["t"],
            "candles": candles
        }
        
        filename = f"prices_1m_{month_key}.json"
        filepath = output_dir / filename
        
        with open(filepath, "w") as f:
            json.dump(output, f, separators=(",", ":"))

        # SAFETY CHECK: Verify no duplicate timestamps
        timestamps = [c["t"] for c in candles]
        if len(timestamps) != len(set(timestamps)):
            raise ValueError(f"CRITICAL: Duplicate timestamps in {filepath} - this will crash the chart!")

        size_kb = filepath.stat().st_size / 1024
        print(f"    1m/{month_key}: {len(candles):,} candles ({size_kb:.1f} KB)")
        total += len(candles)
    
    # Create index file for 1m chunks
    index = {
        "asset_id": asset_id,
        "timeframe": "1m",
        "chunks": [
            {
                "month": month_key,
                "file": f"prices_1m_{month_key}.json",
                "count": len(candles),
                "start": candles[0]["t"],
                "end": candles[-1]["t"],
            }
            for month_key, candles in sorted(months.items())
        ]
    }
    
    with open(output_dir / "prices_1m_index.json", "w") as f:
        json.dump(index, f, indent=2)
    
    return total


def export_tweet_events_for_asset(
    conn,
    asset_id: str,
    output_dir: Path,
    use_daily_fallback: bool = False,
    filter_no_price: bool = True,
    strict: bool = False
) -> int:
    """
    Export tweet events for a single asset.
    
    Args:
        filter_no_price: If True, exclude tweets without price data
        strict: If True, raise error if any tweets lack price data
    
    Returns count of events exported.
    Raises ValueError in strict mode if data gaps exist.
    """
    # Check if asset has 1m price data, otherwise try 1h
    # Note: We check the canonical 'prices' table here because tweet_events view
    # reads from canonical data (tweet alignment uses clean, deduplicated prices)
    has_1m = conn.execute("""
        SELECT COUNT(*) FROM prices
        WHERE asset_id = ? AND timeframe = '1m'
    """, [asset_id]).fetchone()[0] > 0

    has_1h = conn.execute("""
        SELECT COUNT(*) FROM prices
        WHERE asset_id = ? AND timeframe = '1h'
    """, [asset_id]).fetchone()[0] > 0
    
    # Use daily fallback if no 1m or 1h data
    if not has_1m and not has_1h:
        use_daily_fallback = True
    
    events = get_tweet_events(conn, asset_id, use_daily_fallback=use_daily_fallback)
    
    if not events:
        return 0
    
    # Filter out tweets without price data
    original_count = len(events)
    if filter_no_price:
        events_with_price = [e for e in events if e.get("price_at_tweet") is not None]
        filtered_count = original_count - len(events_with_price)
        
        if filtered_count > 0:
            # LOUD WARNING
            print(f"\n    {'!'*50}")
            print(f"    WARNING: {filtered_count}/{original_count} tweets filtered (NO PRICE DATA)")
            print(f"    Asset: {asset_id}")
            print(f"    {'!'*50}")
            
            # Show which tweets are affected
            missing_tweets = [e for e in events if e.get("price_at_tweet") is None]
            for t in missing_tweets[:5]:
                print(f"      - {t['timestamp_iso'][:19]}: {t['text'][:50]}...")
            if len(missing_tweets) > 5:
                print(f"      ... and {len(missing_tweets) - 5} more")
            
            print(f"    -> These tweets need price backfill or manual review")
            print(f"    -> Run: python fetch_prices.py --asset {asset_id} --backfill")
            print()
            
            # STRICT MODE: Fail if any data gaps
            if strict:
                raise ValueError(f"STRICT MODE: {filtered_count} tweets without price data for {asset_id}")
        
        events = events_with_price
    
    if not events:
        print(f"    Tweet events: 0 events (all filtered - no price data)")
        return 0
    
    # Get asset info for metadata
    asset = get_asset(conn, asset_id)
    
    output = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "asset": asset_id,
        "asset_name": asset["name"] if asset else asset_id.upper(),
        "founder": asset["founder"] if asset else "",
        "founder_type": asset.get("founder_type", "founder") if asset else "founder",
        "price_definition": "candle close at minute boundary" if not use_daily_fallback else "daily close",
        "count": len(events),
        "events": events
    }
    
    # Add keyword filter note if asset uses keyword filtering
    has_keyword_filter = asset and asset.get("keyword_filter")
    if has_keyword_filter:
        output["keyword_filter"] = asset["keyword_filter"]
        output["tweet_filter_note"] = asset.get("tweet_filter_note", f"Only tweets mentioning \"{asset['keyword_filter']}\"")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Determine export strategy based on founder_type
    is_adopter = asset and asset.get("founder_type") == "adopter"

    if is_adopter:
        # ADOPTER: Only export filtered tweets (we don't have all their tweets)
        # tweet_events.json = filtered only
        filepath = output_dir / "tweet_events.json"
        with open(filepath, "w") as f:
            json.dump(output, f, indent=2)

        size_kb = filepath.stat().st_size / 1024
        print(f"    Tweet events: {len(events)} events ({size_kb:.1f} KB) [adopter - filtered only]")

        # Delete tweet_events_all.json if it exists (legacy cleanup)
        legacy_all = output_dir / "tweet_events_all.json"
        if legacy_all.exists():
            legacy_all.unlink()
            print(f"    Deleted legacy tweet_events_all.json")

    elif has_keyword_filter:
        # FOUNDER with keyword_filter:
        # tweet_events.json = ALL tweets (default view)
        # tweet_events_filtered.json = filtered tweets (toggle option)

        # Get ALL tweets (unfiltered) for the main file
        all_events = get_tweet_events(conn, asset_id, use_daily_fallback=use_daily_fallback, include_filtered=True)
        if filter_no_price:
            all_events = [e for e in all_events if e.get("price_at_tweet") is not None]

        # Main file: all tweets
        output_all = {
            "generated_at": datetime.utcnow().isoformat() + "Z",
            "asset": asset_id,
            "asset_name": asset["name"] if asset else asset_id.upper(),
            "founder": asset["founder"] if asset else "",
            "founder_type": "founder",
            "price_definition": "candle close at minute boundary" if not use_daily_fallback else "daily close",
            "count": len(all_events),
            "events": all_events
        }

        filepath = output_dir / "tweet_events.json"
        with open(filepath, "w") as f:
            json.dump(output_all, f, indent=2)

        size_kb = filepath.stat().st_size / 1024
        print(f"    Tweet events: {len(all_events)} events ({size_kb:.1f} KB) [all tweets]")

        # Filtered file: only mentions
        output["keyword_filter"] = asset["keyword_filter"]
        output["tweet_filter_note"] = asset.get("tweet_filter_note", f"Only tweets mentioning \"{asset['keyword_filter']}\"")

        filepath_filtered = output_dir / "tweet_events_filtered.json"
        with open(filepath_filtered, "w") as f:
            json.dump(output, f, indent=2)

        size_kb_filtered = filepath_filtered.stat().st_size / 1024
        print(f"    Tweet events (filtered): {len(events)} events ({size_kb_filtered:.1f} KB) [mentions only]")

        # Delete legacy tweet_events_all.json if it exists
        legacy_all = output_dir / "tweet_events_all.json"
        if legacy_all.exists():
            legacy_all.unlink()
            print(f"    Deleted legacy tweet_events_all.json")

        events = all_events  # Return all events count

    else:
        # FOUNDER without keyword_filter: just export all tweets
        filepath = output_dir / "tweet_events.json"
        with open(filepath, "w") as f:
            json.dump(output, f, indent=2)

        size_kb = filepath.stat().st_size / 1024
        print(f"    Tweet events: {len(events)} events ({size_kb:.1f} KB)")

    return len(events)


def export_asset(asset_id: str, strict: bool = False, force: bool = False) -> Dict[str, Any]:
    """Export all data for a single asset.

    This function automatically handles:
    1. Applying keyword filter for adopter assets (ensures clean tweet data)
    2. Computing stats after export (ensures stats.json is up to date)

    Args:
        force: If True, bypass age-based timeframe skipping (e.g., export 1m data for old assets).
    """
    conn = get_connection()
    init_schema(conn)

    asset = get_asset(conn, asset_id)
    if not asset:
        conn.close()
        return {"status": "error", "reason": f"Asset '{asset_id}' not found"}

    print(f"\nExporting {asset['name']} ({asset_id})...")

    output_dir = PUBLIC_DATA_DIR / asset_id

    # =========================================================================
    # PRE-EXPORT: Auto-apply keyword filter for assets with keyword_filter
    # This ensures adopter tweets are properly filtered before export
    # =========================================================================
    keyword_filter = asset.get("keyword_filter")

    # Load extra config from assets.json (not stored in DB)
    use_nitter_keyword_search = False
    reply_to_accounts = None
    try:
        import json
        assets_json_path = Path(__file__).parent / "assets.json"
        with open(assets_json_path) as f:
            assets_json_data = {a["id"]: a for a in json.load(f).get("assets", [])}
        asset_json = assets_json_data.get(asset_id, {})
        reply_to_accounts = asset_json.get("reply_to_accounts")
        use_nitter_keyword_search = asset_json.get("use_nitter_keyword_search", False)
    except Exception:
        pass

    if keyword_filter and not use_nitter_keyword_search:
        # Standard keyword filter: apply text-based filtering
        filter_stats = get_filter_stats(conn, asset_id)
        total_tweets = filter_stats.get("total", 0)
        filtered_out = filter_stats.get("filtered", 0)

        # Check if filter needs to be (re)applied
        # Apply if: has tweets AND (no filtered tweets OR filter not applied)
        if total_tweets > 0 and (filtered_out == 0 or not filter_stats.get("applied")):
            print(f"    Auto-applying keyword filter...")
            result = apply_filter_to_asset(
                conn, asset_id, keyword_filter,
                reply_to_accounts=reply_to_accounts,
                verbose=False
            )
            if result:
                print(f"    → {result['matched']}/{result['total']} tweets match '{keyword_filter}'")
    elif keyword_filter and use_nitter_keyword_search:
        # Nitter keyword search: tweets already filtered by the search itself
        # Include all scraped tweets (the search WAS the filter)
        print(f"    Nitter keyword search - all scraped tweets included (search was the filter)")

    # Export prices
    price_stats = export_prices_for_asset(conn, asset_id, output_dir, force=force)

    # =========================================================================
    # SYNC PRICES TO CANONICAL TABLE (before tweet alignment)
    # The tweet_events view joins against 'prices' (canonical), not RAW.
    # For new assets, canonical is empty until we sync from the just-exported JSONs.
    # This ensures tweets can be aligned with prices during export.
    # =========================================================================
    try:
        from import_canonical import sync_asset
        sync_asset(conn, asset_id, verbose=False)
    except Exception as e:
        print(f"    [WARN] Price sync failed: {e}")
        # Continue anyway - tweet alignment might still work if some canonical data exists

    # Export tweet events (may raise in strict mode)
    try:
        events_count = export_tweet_events_for_asset(conn, asset_id, output_dir, strict=strict)
    except ValueError as e:
        conn.close()
        return {"status": "error", "reason": str(e)}

    conn.close()

    # =========================================================================
    # POST-EXPORT: Auto-compute stats
    # This ensures stats.json is always up to date after export
    # =========================================================================
    if events_count > 0:
        try:
            stats = compute_stats_for_asset(asset_id)
            if stats:
                save_stats(stats, asset_id)
                # Don't print verbose stats - just confirm it was done
        except Exception as e:
            print(f"    [WARN] Stats computation failed: {e}")

    return {
        "status": "success",
        "prices": price_stats,
        "tweet_events": events_count,
    }


def export_all_assets(strict: bool = False, force: bool = False) -> Dict[str, Any]:
    """Export data for all enabled assets."""
    conn = get_connection()
    init_schema(conn)

    assets = get_enabled_assets(conn)
    conn.close()

    print(f"Exporting {len(assets)} enabled assets...")
    if strict:
        print("STRICT MODE: Will fail if any tweets lack price data")

    results = {}
    for asset in assets:
        result = export_asset(asset["id"], strict=strict, force=force)
        results[asset["id"]] = result

        # In strict mode, stop on first error
        if strict and result.get("status") == "error":
            print(f"\nSTRICT MODE FAILURE: {result.get('reason')}")
            break

    return results


def validate_exported_data() -> bool:
    """
    Post-export validation to catch data integrity issues.

    Checks:
    1. All 1D timestamps are at midnight UTC
    2. No duplicate timestamps
    3. Timestamps are sorted ascending
    4. Logs coverage percentage for each asset

    Returns True if all checks pass, False if any issues found.
    """
    from datetime import datetime

    print("\n" + "=" * 60)
    print("POST-EXPORT VALIDATION")
    print("=" * 60)

    all_passed = True

    for asset_dir in PUBLIC_DATA_DIR.iterdir():
        if not asset_dir.is_dir():
            continue

        asset_id = asset_dir.name
        prices_file = asset_dir / "prices_1d.json"

        if not prices_file.exists():
            continue

        with open(prices_file) as f:
            data = json.load(f)

        candles = data.get("candles", [])
        if not candles:
            continue

        timestamps = [c["t"] for c in candles]
        issues = []

        # Check 1: All at midnight UTC
        hours = set(datetime.utcfromtimestamp(t).hour for t in timestamps)
        if hours != {0}:
            issues.append(f"timestamps not at midnight: hours={hours}")

        # Check 2: No duplicates
        if len(timestamps) != len(set(timestamps)):
            dup_count = len(timestamps) - len(set(timestamps))
            issues.append(f"{dup_count} duplicate timestamps")

        # Check 3: Sorted ascending
        if timestamps != sorted(timestamps):
            issues.append("timestamps not sorted")

        # Check 4: Coverage (for logging)
        if len(timestamps) >= 2:
            ts_sorted = sorted(timestamps)
            expected_days = (ts_sorted[-1] - ts_sorted[0]) // 86400 + 1
            coverage = len(timestamps) / expected_days * 100
        else:
            coverage = 100.0

        if issues:
            all_passed = False
            print(f"  ❌ {asset_id}: {', '.join(issues)}")
        else:
            coverage_icon = "✓" if coverage >= 95 else "⚠️"
            print(f"  {coverage_icon} {asset_id}: {len(candles)} candles, {coverage:.1f}% coverage")

    print("=" * 60)
    if all_passed:
        print("✓ All validation checks passed")
    else:
        print("❌ VALIDATION FAILED - see errors above")

    return all_passed


def export_assets_json():
    """Copy assets.json to public directory for frontend."""
    if not ASSETS_FILE.exists():
        print("Warning: assets.json not found")
        return
    
    PUBLIC_DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    # Load and filter to only enabled assets, and remove internal fields
    with open(ASSETS_FILE) as f:
        config = json.load(f)
    
    # Create frontend-friendly version
    frontend_assets = []
    for asset in config.get("assets", []):
        if asset.get("enabled", True):
            frontend_asset = {
                "id": asset["id"],
                "name": asset["name"],
                "founder": asset["founder"],
                "network": asset.get("network"),
                "launch_date": asset["launch_date"],
                "color": asset.get("color"),
                "logo": asset.get("logo"),  # Token logo path
                "enabled": True,  # All exported assets are enabled
            }
            # Include data_note if present (data quality disclaimer)
            if asset.get("data_note"):
                frontend_asset["data_note"] = asset["data_note"]
            frontend_assets.append(frontend_asset)
    
    output = {
        "version": config.get("version", "1.0.0"),
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "assets": frontend_assets
    }
    
    output_path = PUBLIC_DATA_DIR / "assets.json"
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)
    
    print(f"\nExported assets.json with {len(frontend_assets)} assets")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Export data as static JSON for frontend"
    )
    parser.add_argument(
        "--asset", "-a",
        type=str,
        help="Specific asset ID to export (default: all enabled assets)"
    )
    parser.add_argument(
        "--strict", "-s",
        action="store_true",
        help="Strict mode: fail if any tweets lack price data (no silent filtering)"
    )
    parser.add_argument(
        "--validate", "-v",
        action="store_true",
        help="Run validation after export and fail if issues found"
    )
    parser.add_argument(
        "--no-validate",
        action="store_true",
        help="Skip post-export validation"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Bypass age-based timeframe skipping (e.g., export 1m data for old assets)"
    )

    args = parser.parse_args()
    
    print("=" * 60)
    print("Exporting Static Data")
    print("=" * 60)
    print(f"Output: {PUBLIC_DATA_DIR}")
    if args.strict:
        print("Mode: STRICT (will fail on data gaps)")
    
    # Always export assets.json
    export_assets_json()
    
    if args.asset:
        result = export_asset(args.asset, strict=args.strict, force=args.force)
        print(f"\nResult: {result}")
    else:
        results = export_all_assets(strict=args.strict, force=args.force)

        # Run post-export validation
        validate_exported_data()

    # =========================================================================
    # SYNC CANONICAL TABLE from freshly exported JSONs
    # =========================================================================
    # After export, sync the canonical `prices` table from JSONs.
    # This ensures DuckDB queries match the website exactly.
    # =========================================================================
    print("\n" + "=" * 60)
    print("SYNCING: JSONs → prices (canonical table)")
    print("=" * 60)
    try:
        from import_canonical import sync_all_from_json
        sync_all_from_json(verbose=True)
        print("Canonical sync complete ✓")
    except Exception as e:
        print(f"⚠️ Canonical sync failed: {e}")
        print("  Run 'python import_canonical.py' manually to sync")

        # Print summary
        print("\n" + "=" * 60)
        print("EXPORT SUMMARY")
        print("=" * 60)
        
        for asset_id, result in results.items():
            status = result.get("status", "unknown")
            if status == "success":
                prices = result.get("prices", {})
                events = result.get("tweet_events", 0)
                price_summary = ", ".join(f"{tf}:{count}" for tf, count in prices.items())
                print(f"  {asset_id}: {price_summary}, {events} events")
            else:
                print(f"  {asset_id}: {status} - {result.get('reason', '')}")
    
    # List generated files
    print("\n" + "=" * 60)
    print("Generated files:")
    print("=" * 60)

    for f in sorted(PUBLIC_DATA_DIR.rglob("*.json")):
        rel_path = f.relative_to(PUBLIC_DATA_DIR)
        size_kb = f.stat().st_size / 1024
        print(f"  {rel_path} ({size_kb:.1f} KB)")

    # Run validation unless skipped
    if not args.no_validate:
        print("\n" + "=" * 60)
        print("POST-EXPORT VALIDATION")
        print("=" * 60)

        try:
            from validate_export import validate_asset, validate_all_assets

            conn = get_connection()
            init_schema(conn)

            if args.asset:
                passed, results = validate_asset(conn, args.asset)
                failures = [r for r in results if not r.passed]
                if passed:
                    print(f"✓ {args.asset}: All validation checks passed")
                else:
                    print(f"❌ {args.asset}: {len(failures)} issue(s)")
                    for r in failures:
                        print(f"  {r}")
            else:
                all_results = validate_all_assets(conn)
                failed_count = sum(1 for _, (p, _) in all_results.items() if not p)
                total = len(all_results)

                if failed_count == 0:
                    print(f"✓ All {total} assets passed validation")
                else:
                    print(f"⚠️ {failed_count}/{total} assets have validation issues")
                    for asset_id, (passed, results) in all_results.items():
                        if not passed:
                            failures = [r for r in results if not r.passed]
                            print(f"\n  {asset_id}:")
                            for r in failures:
                                print(f"    {r}")

                    if args.validate:
                        print("\n❌ Validation failed (--validate mode)")
                        conn.close()
                        import sys
                        sys.exit(1)

            conn.close()
        except ImportError:
            print("  [WARN] validate_export.py not found, skipping validation")


if __name__ == "__main__":
    main()
