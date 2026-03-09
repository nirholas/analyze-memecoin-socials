"""
Apply Keyword Filter to Tweet Data
==================================

This script filters tweets for assets that track non-founders (adopters, traders, influencers).
Instead of showing ALL tweets from an account, we only show tweets that MENTION the coin.

WHY THIS EXISTS:
- Some assets are tracked via "adopters" not founders (e.g., theunipcs adopted USELESS)
- These accounts tweet about many topics; showing all tweets creates noise
- Filtering to keyword mentions provides much stronger signal for price correlation

USE CASES:
1. Traders who adopted a coin (theunipcs -> USELESS)
2. Influencers who frequently mention a coin
3. Any account where only coin-related tweets matter for analysis

CONFIGURATION:
- Add `keyword_filter` to asset in assets.json (e.g., "useless")
- Add `tweet_filter_note` for UI display (e.g., "Only tweets mentioning $USELESS")
- Add `founder_type` as "adopter" (vs "founder") for clarity

WHAT THIS SCRIPT DOES:
1. Loads asset configuration from assets.json
2. For assets with keyword_filter set:
   - Marks ALL tweets as inactive (filtered out)
   - Re-activates only tweets containing the keyword
3. The export process will only export active tweets

Usage:
    # Apply filter to specific asset
    python apply_keyword_filter.py --asset useless
    
    # Apply filter to all assets with keyword_filter set
    python apply_keyword_filter.py --all
    
    # Preview what would be filtered (dry run)
    python apply_keyword_filter.py --asset useless --dry-run
    
    # Show filter statistics
    python apply_keyword_filter.py --asset useless --stats
"""

import argparse
import re
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any

# Add scripts dir to path
SCRIPTS_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPTS_DIR))

from db import get_connection, init_schema, load_assets_from_json, get_asset, get_enabled_assets


def keyword_matches(
    text: str,
    keyword_filter: str,
    reply_to: str = None,
    reply_to_accounts: List[str] = None
) -> bool:
    """
    Check if tweet text matches keyword filter OR is replying to tracked accounts.

    Supports comma-separated keywords (matches ANY):
    - "hype,hyperliquid" matches tweets containing "hype" OR "hyperliquid"

    Matching rules per keyword (case-insensitive):
    - Exact word: "useless" matches "USELESS coin" but not "uselessness"
    - With $ prefix: matches "$USELESS" or "$useless"
    - With # prefix: matches "#USELESS"
    - With @ prefix: matches "@useless" (catches replies/mentions)
    - As cashtag pattern: matches variations like $USELESS, USELESS, #useless

    Special syntax:
    - "@username" in keyword_filter explicitly matches @-mentions/replies
    - Plain keywords auto-expand to also match @keyword (catches replies)

    reply_to_accounts matching:
    - If tweet is replying to an account in reply_to_accounts, it matches
    - This catches tweets like "@gork nice" that don't contain the keyword
    - Useful for adopters who reply to token-related accounts

    Examples:
        keyword_filter="gork" matches: "gork", "$gork", "#gork", "@gork"
        keyword_filter="gork,@gork" same as above (explicit)
        keyword_filter="worldcoin,wld" matches either keyword
        reply_to="gork", reply_to_accounts=["gork"] -> matches (reply to tracked account)

    Args:
        text: Tweet text to check
        keyword_filter: Keyword(s) to match, comma-separated (e.g., "hype,hyperliquid")
        reply_to: Username being replied to (from tweet metadata)
        reply_to_accounts: List of tracked usernames to match replies

    Returns:
        True if text contains ANY keyword in valid context OR is replying to tracked account
    """
    # First check: Is this a reply to a tracked account?
    if reply_to_accounts and reply_to:
        reply_to_lower = reply_to.lower()
        tracked_lower = [a.lower() for a in reply_to_accounts]
        if reply_to_lower in tracked_lower:
            return True

    # Second check: Does text match keyword filter?
    if not text or not keyword_filter:
        return False

    text_lower = text.lower()

    # Split on comma and check each keyword
    keywords = [k.strip().lower() for k in keyword_filter.split(',') if k.strip()]

    for keyword_lower in keywords:
        # Handle explicit @username syntax
        if keyword_lower.startswith('@'):
            # Explicit @mention - match exactly
            username = keyword_lower[1:]  # Remove @ prefix
            pattern = rf'@{username}\b'
            if re.search(pattern, text_lower):
                return True
        else:
            # Regular keyword - match $KEYWORD, #KEYWORD, @KEYWORD, or plain KEYWORD
            # Auto-expanding to @keyword catches replies naturally
            patterns = [
                rf'\${keyword_lower}\b',      # $gork (cashtag)
                rf'#{keyword_lower}\b',       # #gork (hashtag)
                rf'@{keyword_lower}\b',       # @gork (mention/reply) - NEW
                rf'\b{keyword_lower}\b',      # gork (word boundary)
            ]

            for pattern in patterns:
                if re.search(pattern, text_lower):
                    return True

    return False


def apply_filter_to_asset(
    conn,
    asset_id: str,
    keyword: str,
    reply_to_accounts: List[str] = None,
    dry_run: bool = False,
    verbose: bool = True
) -> Dict[str, int]:
    """
    Apply keyword filter to all tweets for an asset.

    This uses a soft-delete approach via an 'is_filtered' column:
    - All tweets initially marked is_filtered = TRUE
    - Tweets matching keyword OR replying to tracked accounts are marked is_filtered = FALSE
    - Export process only exports non-filtered tweets

    Args:
        conn: Database connection
        asset_id: Asset ID to filter
        keyword: Keyword to match
        reply_to_accounts: List of usernames - replies to these also match
        dry_run: If True, don't actually update, just report
        verbose: Print progress

    Returns:
        Dict with stats: {total, matched, filtered_out}
    """
    # Get all tweets for this asset (including reply_to metadata)
    tweets = conn.execute("""
        SELECT id, text, reply_to FROM tweets WHERE asset_id = ?
    """, [asset_id]).fetchall()

    total = len(tweets)
    matched_ids = []
    filtered_ids = []

    for tweet_id, text, reply_to in tweets:
        if keyword_matches(text, keyword, reply_to=reply_to, reply_to_accounts=reply_to_accounts):
            matched_ids.append(tweet_id)
        else:
            filtered_ids.append(tweet_id)
    
    if verbose:
        print(f"\n[FILTER] Asset: {asset_id}")
        print(f"[FILTER] Keyword: \"{keyword}\"")
        if reply_to_accounts:
            print(f"[FILTER] Reply-to accounts: {reply_to_accounts}")
        print(f"[FILTER] Total tweets: {total}")
        print(f"[FILTER] Matching keyword: {len(matched_ids)} ({100*len(matched_ids)/total:.1f}%)")
        print(f"[FILTER] Filtered out: {len(filtered_ids)} ({100*len(filtered_ids)/total:.1f}%)")
    
    if not dry_run and matched_ids:
        # First, ensure is_filtered column exists
        try:
            conn.execute("ALTER TABLE tweets ADD COLUMN is_filtered BOOLEAN DEFAULT FALSE")
        except:
            pass  # Column already exists
        
        # Mark all tweets as filtered
        conn.execute("""
            UPDATE tweets SET is_filtered = TRUE WHERE asset_id = ?
        """, [asset_id])
        
        # Un-filter tweets that match keyword
        # DuckDB doesn't support IN with large lists well, so batch it
        batch_size = 100
        for i in range(0, len(matched_ids), batch_size):
            batch = matched_ids[i:i+batch_size]
            placeholders = ','.join(['?'] * len(batch))
            conn.execute(f"""
                UPDATE tweets SET is_filtered = FALSE 
                WHERE id IN ({placeholders})
            """, batch)
        
        if verbose:
            print(f"[FILTER] ✓ Applied filter - {len(matched_ids)} tweets active")
    elif dry_run:
        if verbose:
            print(f"[FILTER] (dry run - no changes made)")
            
            # Show sample of matched and filtered tweets
            print(f"\n[FILTER] Sample MATCHING tweets:")
            sample_matched = [t for t in tweets if t[0] in matched_ids[:5]]
            for tid, text in sample_matched:
                print(f"  ✓ {text[:80]}...")
            
            print(f"\n[FILTER] Sample FILTERED OUT tweets:")
            sample_filtered = [t for t in tweets if t[0] in filtered_ids[:5]]
            for tid, text in sample_filtered:
                print(f"  ✗ {text[:80]}...")
    
    return {
        'total': total,
        'matched': len(matched_ids),
        'filtered_out': len(filtered_ids),
    }


def get_filter_stats(conn, asset_id: str) -> Dict[str, Any]:
    """Get current filter status for an asset."""
    
    # Check if is_filtered column exists
    try:
        result = conn.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN is_filtered = FALSE THEN 1 ELSE 0 END) as active,
                SUM(CASE WHEN is_filtered = TRUE THEN 1 ELSE 0 END) as filtered
            FROM tweets WHERE asset_id = ?
        """, [asset_id]).fetchone()
        
        return {
            'total': result[0],
            'active': result[1] or 0,
            'filtered': result[2] or 0,
            'filter_applied': True,
        }
    except:
        # Column doesn't exist - no filter applied
        total = conn.execute(
            "SELECT COUNT(*) FROM tweets WHERE asset_id = ?", [asset_id]
        ).fetchone()[0]
        
        return {
            'total': total,
            'active': total,
            'filtered': 0,
            'filter_applied': False,
        }


def main():
    parser = argparse.ArgumentParser(
        description='Apply keyword filter to tweets for non-founder assets',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Apply filter to specific asset (uses keyword_filter from assets.json)
  python apply_keyword_filter.py --asset useless
  
  # Preview what would be filtered
  python apply_keyword_filter.py --asset useless --dry-run
  
  # Apply custom keyword (override config)
  python apply_keyword_filter.py --asset useless --keyword "useless"
  
  # Show current filter statistics
  python apply_keyword_filter.py --asset useless --stats
  
  # Apply to all assets with keyword_filter configured
  python apply_keyword_filter.py --all
"""
    )
    
    parser.add_argument('--asset', '-a', help='Asset ID to filter')
    parser.add_argument('--keyword', '-k', help='Override keyword filter')
    parser.add_argument('--all', action='store_true', help='Apply to all assets with keyword_filter')
    parser.add_argument('--dry-run', action='store_true', help='Preview without applying')
    parser.add_argument('--stats', action='store_true', help='Show filter statistics')
    parser.add_argument('--quiet', '-q', action='store_true', help='Suppress output')
    
    args = parser.parse_args()
    
    if not args.asset and not args.all:
        parser.print_help()
        print("\nERROR: Must specify --asset or --all")
        sys.exit(1)
    
    conn = get_connection()
    init_schema(conn)
    load_assets_from_json(conn)
    
    verbose = not args.quiet
    
    if args.stats:
        # Show statistics only
        if args.asset:
            asset = get_asset(conn, args.asset)
            if not asset:
                print(f"Asset '{args.asset}' not found")
                sys.exit(1)
            
            stats = get_filter_stats(conn, args.asset)
            print(f"\nFilter Statistics for {asset['name']} ({args.asset})")
            print(f"  Keyword filter: {asset.get('keyword_filter') or 'None'}")
            print(f"  Filter applied: {stats['filter_applied']}")
            print(f"  Total tweets: {stats['total']}")
            print(f"  Active tweets: {stats['active']}")
            print(f"  Filtered out: {stats['filtered']}")
        conn.close()
        return
    
    # Apply filter
    assets_to_process = []
    
    if args.asset:
        asset = get_asset(conn, args.asset)
        if not asset:
            print(f"Asset '{args.asset}' not found")
            sys.exit(1)
        
        keyword = args.keyword or asset.get('keyword_filter')
        if not keyword:
            print(f"No keyword specified and asset has no keyword_filter configured")
            sys.exit(1)
        
        assets_to_process.append((asset, keyword))
    
    elif args.all:
        # Get all assets with keyword_filter
        all_assets = get_enabled_assets(conn)
        for asset in all_assets:
            keyword = asset.get('keyword_filter')
            if keyword:
                assets_to_process.append((asset, keyword))
        
        if not assets_to_process:
            print("No assets found with keyword_filter configured")
            sys.exit(0)
        
        if verbose:
            print(f"Found {len(assets_to_process)} assets with keyword_filter")
    
    # Load assets.json to get reply_to_accounts (not stored in DB)
    import json
    assets_json_path = Path(__file__).parent / "assets.json"
    assets_json_data = {}
    try:
        with open(assets_json_path) as f:
            assets_json_data = {a["id"]: a for a in json.load(f).get("assets", [])}
    except Exception:
        pass  # If assets.json not available, reply_to_accounts will be None

    # Process each asset
    total_stats = {'total': 0, 'matched': 0, 'filtered_out': 0}

    for asset, keyword in assets_to_process:
        # Get reply_to_accounts from assets.json config
        asset_json = assets_json_data.get(asset['id'], {})
        reply_to_accounts = asset_json.get('reply_to_accounts')

        stats = apply_filter_to_asset(
            conn,
            asset['id'],
            keyword,
            reply_to_accounts=reply_to_accounts,
            dry_run=args.dry_run,
            verbose=verbose
        )

        for k in total_stats:
            total_stats[k] += stats[k]
    
    conn.close()
    
    if verbose and len(assets_to_process) > 1:
        print(f"\n{'='*60}")
        print(f"[FILTER] TOTAL: {total_stats['total']} tweets")
        print(f"[FILTER] Matched: {total_stats['matched']}")
        print(f"[FILTER] Filtered: {total_stats['filtered_out']}")


if __name__ == '__main__':
    main()

