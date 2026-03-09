"""
Fetch tweets from founders using X API v2.
RESILIENT VERSION: Saves each page immediately, can resume from interruption.

Features:
- Incremental page-by-page saving (no data loss on timeout)
- Dual watermarks: newest_id (for updates) + oldest_id (for backfill)
- Automatic retry with exponential backoff
- Resume capability after interruption
- Keyword filtering at fetch time (prevents DB pollution)

KEYWORD FILTERING:
    For assets with keyword_filter set in assets.json, tweets are filtered
    BEFORE being stored in the database. This ensures:
    1. DB stays in sync with what will be exported to JSON
    2. No accumulation of irrelevant tweets over time
    3. Consistent counts between fetch and export
    
    The filtering uses word-boundary matching (not substring matching):
    - "wif" matches "bought some $WIF" but NOT "wifey"
    - Supports cashtags ($WIF) and hashtags (#WIF)
    
    See apply_keyword_filter.py for the matching logic.

Usage:
    python fetch_tweets.py                    # Update all assets (fetch new tweets)
    python fetch_tweets.py --asset pump       # Update specific asset
    python fetch_tweets.py --asset pump --backfill  # Backfill older tweets
    python fetch_tweets.py --full             # Full refetch (ignore watermarks)
"""
import argparse
import httpx
import json
import time
import random
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any, Tuple

from config import (
    X_BEARER_TOKEN,
    X_API_BASE,
    RATE_LIMIT_DELAY,
    DATA_DIR
)
from db import (
    get_connection, init_schema, get_asset, get_enabled_assets,
    get_ingestion_state, update_ingestion_state, insert_tweets,
    load_assets_from_json
)
# Import keyword matching function for consistent filtering across codebase
from apply_keyword_filter import keyword_matches

# Path to fetch state file (persists between runs)
FETCH_STATE_FILE = DATA_DIR / "fetch_state.json"


def read_fetch_state() -> dict:
    """
    Read the fetch state from disk.
    
    Returns state dict with structure:
    {
        "tweets": {
            "last_run": "2025-12-28T15:05:00Z",
            "skipped_assets": ["asset1", "asset2"],
            "skip_reason": "rate_limit"
        }
    }
    """
    try:
        if FETCH_STATE_FILE.exists():
            with open(FETCH_STATE_FILE) as f:
                return json.load(f)
    except Exception as e:
        print(f"    ⚠️ Could not read fetch state: {e}")
    return {}


def write_fetch_state(state: dict) -> None:
    """Write the fetch state to disk."""
    try:
        # Ensure data directory exists
        FETCH_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(FETCH_STATE_FILE, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as e:
        print(f"    ⚠️ Could not write fetch state: {e}")


def prioritize_assets(assets: list, skipped_ids: list) -> list:
    """
    Reorder assets so previously-skipped ones come first.
    
    This ensures assets that were rate-limited in the previous run
    get priority in the next run.
    """
    if not skipped_ids:
        return assets
    
    skipped_set = set(skipped_ids)
    priority = []
    normal = []
    
    for asset in assets:
        if asset["id"] in skipped_set:
            priority.append(asset)
        else:
            normal.append(asset)
    
    if priority:
        print(f"    📋 Prioritizing {len(priority)} previously-skipped assets: {[a['id'] for a in priority]}")
    
    return priority + normal


def get_user_id(client: httpx.Client, username: str) -> tuple:
    """
    Get the user ID from username with retry.
    Returns (user_id, error_reason) tuple.
    """
    url = f"{X_API_BASE}/users/by/username/{username}"
    
    for attempt in range(3):
        try:
            response = client.get(url, timeout=30.0)
            
            # Granular rate limit logging
            if response.status_code == 429:
                reset_time = response.headers.get("x-rate-limit-reset", "unknown")
                remaining = response.headers.get("x-rate-limit-remaining", "unknown")
                print(f"      ⚠️ RATE LIMIT 429 on user lookup")
                print(f"         Remaining: {remaining}, Reset: {reset_time}")
                wait = (2 ** attempt) * 15 + random.uniform(0, 5)
                print(f"         Waiting {wait:.0f}s (attempt {attempt + 1}/3)...")
                time.sleep(wait)
                continue
            
            if response.status_code == 403:
                print(f"      ❌ FORBIDDEN 403 on user lookup")
                print(f"         Response: {response.text[:200]}")
                return None, "403 Forbidden - likely rate limit exhausted or suspended"
            
            if response.status_code == 401:
                print(f"      ❌ UNAUTHORIZED 401 on user lookup")
                return None, "401 Unauthorized - check X_BEARER_TOKEN"
            
            if response.status_code != 200:
                print(f"      ❌ HTTP {response.status_code} on user lookup")
                print(f"         Response: {response.text[:200]}")
                return None, f"HTTP {response.status_code}"
            
            data = response.json()
            if "data" not in data:
                print(f"      ❌ User not found: @{username}")
                return None, f"User @{username} not found"
            
            return data["data"]["id"], "ok"
            
        except httpx.TimeoutException:
            print(f"      ⏱️ Timeout on user lookup (attempt {attempt + 1}/3)")
            time.sleep(5)
        except Exception as e:
            print(f"      ❌ Exception on user lookup: {type(e).__name__}: {e}")
            if attempt < 2:
                time.sleep(5)
    
    return None, "Failed after 3 attempts"


def fetch_tweet_page(
    client: httpx.Client,
    user_id: str,
    since_id: Optional[str] = None,
    until_id: Optional[str] = None,
    pagination_token: Optional[str] = None,
    max_results: int = 100
) -> Tuple[List[Dict], Optional[str], bool]:
    """
    Fetch a single page of tweets with retry logic.
    
    Args:
        client: HTTP client
        user_id: Twitter user ID
        since_id: Only return tweets newer than this ID
        until_id: Only return tweets older than this ID (for backfill)
        pagination_token: Token for pagination
        max_results: Max tweets per page
    
    Returns (tweets, next_pagination_token, success)
    """
    url = f"{X_API_BASE}/users/{user_id}/tweets"
    params = {
        "max_results": max_results,
        "tweet.fields": "created_at,public_metrics,conversation_id",
        "exclude": "retweets,replies",
    }
    
    if since_id:
        params["since_id"] = since_id
    if until_id:
        params["until_id"] = until_id
    if pagination_token:
        params["pagination_token"] = pagination_token
    
    for attempt in range(3):
        try:
            response = client.get(url, params=params, timeout=30.0)
            
            # Granular rate limit logging
            if response.status_code == 429:
                reset_ts = response.headers.get("x-rate-limit-reset", "0")
                remaining = response.headers.get("x-rate-limit-remaining", "0")
                
                # Calculate actual wait time
                try:
                    reset_epoch = int(reset_ts)
                    now_epoch = int(time.time())
                    wait_seconds = max(0, reset_epoch - now_epoch + 5)  # +5 buffer
                except:
                    wait_seconds = 60
                
                print(f"      ⚠️ RATE LIMIT 429 on tweet fetch")
                print(f"         Remaining: {remaining}, Reset in: {wait_seconds}s")
                
                # If reset is > 2 minutes away, don't wait - just fail fast
                if wait_seconds > 120:
                    print(f"         Rate limit reset too far away ({wait_seconds}s) - skipping")
                    return [], None, False  # Signal to skip this asset
                
                # Otherwise wait for the actual reset time
                print(f"         Waiting {wait_seconds}s until reset...")
                time.sleep(wait_seconds)
                continue
            
            if response.status_code == 403:
                print(f"      ❌ FORBIDDEN 403 on tweet fetch")
                print(f"         Response: {response.text[:200]}")
                return [], None, False
            
            if response.status_code != 200:
                print(f"      ❌ HTTP {response.status_code} on tweet fetch")
                print(f"         Response: {response.text[:100]}")
                return [], None, False
            
            data = response.json()
            tweets_data = data.get("data", [])
            
            # Process tweets
            tweets = []
            for tweet in tweets_data:
                metrics = tweet.get("public_metrics", {})
                tweets.append({
                    "id": tweet["id"],
                    "text": tweet["text"],
                    "created_at": tweet["created_at"],
                    "timestamp": parse_iso_timestamp(tweet["created_at"]),
                    "likes": metrics.get("like_count", 0),
                    "retweets": metrics.get("retweet_count", 0),
                    "replies": metrics.get("reply_count", 0),
                    "impressions": metrics.get("impression_count", 0),
                })
            
            next_token = data.get("meta", {}).get("next_token")
            return tweets, next_token, True
            
        except httpx.TimeoutException:
            wait = (2 ** attempt) * 5 + random.uniform(0, 3)
            print(f"      Timeout (attempt {attempt + 1}/3), waiting {wait:.0f}s...")
            time.sleep(wait)
        except Exception as e:
            print(f"      Error: {e}")
            if attempt < 2:
                time.sleep(5)
    
    return [], None, False


def parse_iso_timestamp(iso_str: str) -> datetime:
    """Parse ISO timestamp string to timezone-aware datetime."""
    iso_str = iso_str.replace("Z", "+00:00")
    dt = datetime.fromisoformat(iso_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def fetch_for_asset(
    asset_id: str,
    full_fetch: bool = False,
    backfill: bool = False,
    max_pages: int = 50
) -> Dict[str, Any]:
    """
    Fetch tweets for a specific asset with incremental saving.
    
    Args:
        asset_id: Asset ID from assets.json
        full_fetch: If True, ignore watermarks and fetch everything
        backfill: If True, fetch older tweets (use oldest_id as until_id)
        max_pages: Maximum pages to fetch per run
    
    Returns fetch result stats.
    """
    if not X_BEARER_TOKEN:
        return {"status": "error", "reason": "X_BEARER_TOKEN not configured"}
    
    conn = get_connection()
    init_schema(conn)
    load_assets_from_json(conn)
    
    asset = get_asset(conn, asset_id)
    if not asset:
        conn.close()
        return {"status": "error", "reason": f"Asset '{asset_id}' not found"}
    
    if not asset["enabled"]:
        conn.close()
        return {"status": "skipped", "reason": "Asset is disabled"}
    
    # Check for skip_tweet_fetch flag (for high-volume accounts that rarely mention the asset)
    if asset.get("skip_tweet_fetch"):
        reason = asset.get("skip_tweet_fetch_reason", "skip_tweet_fetch flag is set")
        conn.close()
        return {"status": "skipped", "reason": reason}
    
    print(f"\n{'='*60}")
    print(f"Fetching tweets for {asset['name']} (@{asset['founder']})")
    print(f"{'='*60}")
    
    # Get launch date for filtering
    launch_date = asset["launch_date"]
    if isinstance(launch_date, str):
        launch_date = parse_iso_timestamp(launch_date)
    if launch_date.tzinfo is None:
        launch_date = launch_date.replace(tzinfo=timezone.utc)
    
    # Get watermarks from ingestion state
    state = get_ingestion_state(conn, asset_id, "tweets")
    newest_id = None
    oldest_id = None
    
    if state and not full_fetch:
        newest_id = state.get("last_id")  # Most recent tweet we have
        # Get oldest_id from a separate state entry
        oldest_state = get_ingestion_state(conn, asset_id, "tweets_oldest")
        if oldest_state:
            oldest_id = oldest_state.get("last_id")
    
    # AUTO-DETECT: If backfill requested but no oldest_id in state, query DB
    if backfill and not oldest_id:
        oldest_in_db = conn.execute("""
            SELECT MIN(id) FROM tweets WHERE asset_id = ?
        """, [asset_id]).fetchone()[0]
        
        if oldest_in_db:
            oldest_id = oldest_in_db
            print(f"    Auto-detected oldest tweet ID from DB: {oldest_id}")
            # Save it to state for future runs
            update_ingestion_state(conn, asset_id, "tweets_oldest", last_id=oldest_id)
        else:
            # LOUD ERROR: Can't backfill if we have no tweets yet
            conn.close()
            print(f"\n{'!'*60}")
            print(f"ERROR: Cannot backfill - no tweets in DB for '{asset_id}'")
            print(f"Run without --backfill first to fetch initial tweets.")
            print(f"{'!'*60}\n")
            return {"status": "error", "reason": f"No tweets in DB for {asset_id}. Run without --backfill first."}
    
    # Determine fetch mode
    if backfill and oldest_id:
        print(f"    Mode: BACKFILL (fetching tweets older than {oldest_id})")
        since_id = None
        until_id = oldest_id
    elif newest_id and not full_fetch:
        print(f"    Mode: UPDATE (fetching tweets newer than {newest_id})")
        since_id = newest_id
        until_id = None
    else:
        print(f"    Mode: FULL FETCH")
        since_id = None
        until_id = None
    
    # Set up HTTP client with browser-like headers
    # This helps bypass Cloudflare protection on some datacenter IPs (e.g., GitHub Actions)
    headers = {
        "Authorization": f"Bearer {X_BEARER_TOKEN}",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
    }
    
    total_fetched = 0
    total_inserted = 0
    total_filtered = 0
    page = 0
    pagination_token = None
    
    # Track watermarks for this run
    run_newest_id = None
    run_oldest_id = None
    fetch_failed = False  # Track if we hit rate limits or other errors
    
    with httpx.Client(headers=headers) as client:
        # Get user ID first
        print(f"    Looking up @{asset['founder']}...")
        user_id, error_reason = get_user_id(client, asset["founder"])
        
        if not user_id:
            conn.close()
            return {"status": "error", "reason": error_reason}
        
        print(f"    User ID: {user_id}")
        
        # Fetch pages
        
        while page < max_pages:
            page += 1
            
            tweets, next_token, success = fetch_tweet_page(
                client, user_id,
                since_id=since_id,
                until_id=until_id,
                pagination_token=pagination_token
            )
            
            if not success:
                print(f"    Page {page}: Failed to fetch, stopping")
                fetch_failed = True  # Mark that we failed (likely rate limit)
                break
            
            if not tweets:
                if page == 1:
                    print(f"    No {'new ' if since_id else ''}tweets found")
                break
            
            # Filter pre-launch tweets
            filtered_tweets = []
            for t in tweets:
                tweet_time = t["timestamp"]
                if tweet_time < launch_date:
                    total_filtered += 1
                    continue
                filtered_tweets.append(t)
                
                # Track watermarks using INT comparison (tweet IDs are numeric)
                tid = t["id"]
                if run_newest_id is None or int(tid) > int(run_newest_id):
                    run_newest_id = tid
                if run_oldest_id is None or int(tid) < int(run_oldest_id):
                    run_oldest_id = tid
            
            total_fetched += len(tweets)
            
            # KEYWORD FILTER - ONLY for ADOPTER assets (founder_type == "adopter")
            # Adopters are high-volume accounts (e.g. @blknoiz06 for WIF) where only
            # coin-specific tweets matter. Founders created the token, so ALL their tweets are relevant.
            # Uses word-boundary matching (same as apply_keyword_filter.py) for consistency.
            # Example: "wif" matches "$WIF" but not "wifey"
            founder_type = asset.get("founder_type")
            keyword_filter = asset.get("keyword_filter")
            if founder_type == "adopter" and keyword_filter and filtered_tweets:
                keyword_matched = []
                for t in filtered_tweets:
                    tweet_text = t.get("text", "")
                    if keyword_matches(tweet_text, keyword_filter):
                        keyword_matched.append(t)
                keyword_filtered_count = len(filtered_tweets) - len(keyword_matched)
                filtered_tweets = keyword_matched
                if keyword_filtered_count > 0:
                    print(f"      (adopter asset - filtered {keyword_filtered_count} tweets not matching '{keyword_filter}')")
            
            # INSERT IMMEDIATELY - this is the key for resilience
            if filtered_tweets:
                inserted = insert_tweets(conn, asset_id, filtered_tweets)
                total_inserted += inserted
            
            print(f"    Page {page}: {len(tweets)} fetched, {len(filtered_tweets)} kept, {total_inserted} total saved")
            
            # Check for more pages
            if not next_token:
                print(f"    No more pages")
                break
            
            pagination_token = next_token
            
            # Rate limiting between pages
            time.sleep(RATE_LIMIT_DELAY)
    
    # UPDATE WATERMARKS ONCE AT END (not per-page)
    if run_newest_id and (newest_id is None or int(run_newest_id) > int(newest_id)):
        update_ingestion_state(conn, asset_id, "tweets", last_id=run_newest_id)
        print(f"    ✓ Saved watermark: newest_id = {run_newest_id}")
    
    if run_oldest_id:
        update_ingestion_state(conn, asset_id, "tweets_oldest", last_id=run_oldest_id)
    
    # DB STATS - Show total tweets stored for this asset
    # For adopters: filtered to keyword matches at fetch time
    # For founders: ALL tweets stored (no keyword filtering)
    founder_type = asset.get("founder_type")
    keyword_filter = asset.get("keyword_filter")
    total_in_db = conn.execute("""
        SELECT COUNT(*) FROM tweets WHERE asset_id = ?
    """, [asset_id]).fetchone()[0]
    
    if founder_type == "adopter" and keyword_filter:
        print(f"    📊 Total tweets in DB for {asset_id}: {total_in_db} (adopter - filtered to '{keyword_filter}')")
    else:
        print(f"    📊 Total tweets in DB for {asset_id}: {total_in_db} (founder - all tweets stored)")
    
    # Summary
    print(f"\n    Summary: {total_fetched} fetched, {total_filtered} pre-launch filtered, {total_inserted} saved")
    
    if run_newest_id:
        print(f"    Newest tweet ID: {run_newest_id}")
    if run_oldest_id:
        print(f"    Oldest tweet ID: {run_oldest_id}")
    
    conn.close()
    
    # If we failed to fetch (e.g., rate limit) AND got no tweets, report as error
    if fetch_failed and total_fetched == 0:
        return {
            "status": "error",
            "reason": "rate limit or fetch failure",
            "fetched": 0,
            "inserted": 0,
        }
    
    return {
        "status": "success",
        "fetched": total_fetched,
        "filtered": total_filtered,
        "inserted": total_inserted,
        "newest_id": run_newest_id,
        "oldest_id": run_oldest_id,
    }


def fetch_all_assets(
    full_fetch: bool = False,
    backfill: bool = False
) -> Dict[str, Any]:
    """
    Fetch tweets for all enabled assets.
    
    Features:
    - Reads previous run state to prioritize skipped assets
    - Writes state file after completion for next run
    - Stops early on rate limits to preserve quota
    """
    conn = get_connection()
    init_schema(conn)
    load_assets_from_json(conn)
    
    assets = get_enabled_assets(conn)
    conn.close()
    
    # READ PREVIOUS STATE - prioritize assets that were skipped last time
    prev_state = read_fetch_state()
    prev_tweet_state = prev_state.get("tweets", {})
    prev_skipped = prev_tweet_state.get("skipped_assets", [])
    
    if prev_skipped:
        print(f"\n📋 Previous run skipped {len(prev_skipped)} assets due to: {prev_tweet_state.get('skip_reason', 'unknown')}")
    
    # Reorder assets: previously-skipped come first
    assets = prioritize_assets(assets, prev_skipped)
    
    print(f"\nFetching tweets for {len(assets)} enabled assets...")
    if backfill:
        print("Mode: BACKFILL (fetching older tweets)")
    elif full_fetch:
        print("Mode: FULL FETCH (ignoring watermarks)")
    else:
        print("Mode: UPDATE (fetching new tweets only)")
    
    results = {}
    rate_limit_hit = False
    skipped_assets = []  # Track assets skipped this run
    
    for asset in assets:
        # If we hit rate limits, skip remaining assets
        if rate_limit_hit:
            skipped_assets.append(asset["id"])
            results[asset["id"]] = {"status": "skipped", "reason": "Rate limit hit on earlier asset"}
            continue
        
        result = fetch_for_asset(
            asset["id"], 
            full_fetch=full_fetch,
            backfill=backfill
        )
        results[asset["id"]] = result
        
        # Check if this asset hit rate limits (indicated by 0 fetched after error)
        if result.get("status") == "error" and "rate limit" in result.get("reason", "").lower():
            rate_limit_hit = True
            print("\n⚠️ Rate limit hit - skipping remaining assets to preserve quota")
        
        # Pause between assets to avoid rate limits
        time.sleep(2)
    
    # WRITE STATE - save skipped assets for next run to prioritize
    new_state = read_fetch_state()  # Preserve other keys (e.g., prices state)
    new_state["tweets"] = {
        "last_run": datetime.now(timezone.utc).isoformat(),
        "skipped_assets": skipped_assets,
        "skip_reason": "rate_limit" if rate_limit_hit else "none",
        "total_assets": len(assets),
        "completed_assets": len(assets) - len(skipped_assets),
    }
    write_fetch_state(new_state)
    
    if skipped_assets:
        print(f"\n💾 Saved state: {len(skipped_assets)} skipped assets will be prioritized next run")
    
    # Print summary
    print("\n" + "=" * 60)
    print("FETCH SUMMARY")
    print("=" * 60)
    
    total_fetched = 0
    total_inserted = 0
    for asset_id, result in results.items():
        status = result.get("status", "unknown")
        if status == "success":
            fetched = result.get("fetched", 0)
            inserted = result.get("inserted", 0)
            total_fetched += fetched
            total_inserted += inserted
            print(f"  {asset_id}: {fetched} fetched, {inserted} saved")
        else:
            print(f"  {asset_id}: {status} - {result.get('reason', '')}")
    
    print(f"\nTotal: {total_fetched} tweets fetched, {total_inserted} saved")
    
    return results


def main():
    """Main entry point with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description="Fetch tweets for tracked assets (resilient version)"
    )
    parser.add_argument(
        "--asset", "-a",
        type=str,
        help="Specific asset ID to fetch (default: all enabled assets)"
    )
    parser.add_argument(
        "--full", "-f",
        action="store_true",
        help="Full fetch (ignore watermarks, fetch all available tweets)"
    )
    parser.add_argument(
        "--backfill", "-b",
        action="store_true",
        help="Backfill mode (fetch older tweets we don't have yet)"
    )
    parser.add_argument(
        "--max-pages", "-m",
        type=int,
        default=50,
        help="Maximum pages to fetch per asset (default: 50)"
    )
    
    args = parser.parse_args()
    
    if args.asset:
        fetch_for_asset(
            args.asset, 
            full_fetch=args.full,
            backfill=args.backfill,
            max_pages=args.max_pages
        )
    else:
        fetch_all_assets(
            full_fetch=args.full,
            backfill=args.backfill
        )


if __name__ == "__main__":
    main()
