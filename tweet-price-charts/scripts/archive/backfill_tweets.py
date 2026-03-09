"""
Resilient tweet backfill script.
Fetches tweets page by page, saving each page immediately to the database.
Can be interrupted and resumed - progress is never lost.

Usage:
    python backfill_tweets.py --asset jup --max-pages 100
"""
import argparse
import httpx
import time
import random
from datetime import datetime, timezone

from config import X_BEARER_TOKEN, X_API_BASE, DATA_DIR
from db import (
    get_connection, init_schema, get_asset, insert_tweets,
    load_assets_from_json
)


def parse_iso_timestamp(iso_str: str) -> datetime:
    """Parse ISO timestamp string to timezone-aware datetime."""
    iso_str = iso_str.replace("Z", "+00:00")
    dt = datetime.fromisoformat(iso_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def get_user_id(client: httpx.Client, username: str) -> str:
    """Get Twitter user ID from username."""
    url = f"{X_API_BASE}/users/by/username/{username}"
    
    for attempt in range(3):
        try:
            response = client.get(url, timeout=30.0)
            if response.status_code == 429:
                wait = (2 ** attempt) * 30 + random.uniform(0, 10)
                print(f"  Rate limited, waiting {wait:.0f}s...")
                time.sleep(wait)
                continue
            response.raise_for_status()
            return response.json()["data"]["id"]
        except Exception as e:
            print(f"  Error: {e}")
            time.sleep(5)
    
    raise Exception(f"Could not get user ID for {username}")


def fetch_page(
    client: httpx.Client,
    user_id: str,
    pagination_token: str = None,
    until_id: str = None,
) -> tuple:
    """
    Fetch a single page of tweets.
    Returns (tweets, next_token, success)
    """
    url = f"{X_API_BASE}/users/{user_id}/tweets"
    params = {
        "max_results": 100,
        "tweet.fields": "created_at,public_metrics",
        "exclude": "retweets,replies",
    }
    
    if pagination_token:
        params["pagination_token"] = pagination_token
    if until_id:
        params["until_id"] = until_id
    
    for attempt in range(5):
        try:
            response = client.get(url, params=params, timeout=30.0)
            
            if response.status_code == 429:
                wait = (2 ** attempt) * 30 + random.uniform(0, 10)
                print(f"    Rate limited, waiting {wait:.0f}s (attempt {attempt+1}/5)...")
                time.sleep(wait)
                continue
            
            if response.status_code != 200:
                print(f"    API error {response.status_code}: {response.text[:200]}")
                return [], None, False
            
            data = response.json()
            tweets_data = data.get("data", [])
            
            tweets = []
            for t in tweets_data:
                metrics = t.get("public_metrics", {})
                tweets.append({
                    "id": t["id"],
                    "text": t["text"],
                    "created_at": t["created_at"],
                    "timestamp": parse_iso_timestamp(t["created_at"]),
                    "likes": metrics.get("like_count", 0),
                    "retweets": metrics.get("retweet_count", 0),
                    "replies": metrics.get("reply_count", 0),
                    "impressions": metrics.get("impression_count", 0),
                })
            
            next_token = data.get("meta", {}).get("next_token")
            return tweets, next_token, True
            
        except httpx.TimeoutException:
            wait = (2 ** attempt) * 5
            print(f"    Timeout, waiting {wait}s...")
            time.sleep(wait)
        except Exception as e:
            print(f"    Error: {e}")
            time.sleep(5)
    
    return [], None, False


def backfill_asset(asset_id: str, max_pages: int = 100):
    """
    Backfill tweets for an asset, saving each page immediately.
    """
    if not X_BEARER_TOKEN:
        print("ERROR: X_BEARER_TOKEN not configured")
        return
    
    conn = get_connection()
    init_schema(conn)
    load_assets_from_json(conn)
    
    asset = get_asset(conn, asset_id)
    if not asset:
        print(f"ERROR: Asset '{asset_id}' not found")
        conn.close()
        return
    
    founder = asset["founder"]
    launch_date = asset["launch_date"]
    if isinstance(launch_date, str):
        launch_date = parse_iso_timestamp(launch_date)
    if launch_date.tzinfo is None:
        launch_date = launch_date.replace(tzinfo=timezone.utc)
    
    print(f"\n{'='*60}")
    print(f"BACKFILL: {asset['name']} (@{founder})")
    print(f"Launch date: {launch_date.strftime('%Y-%m-%d')}")
    print(f"{'='*60}")
    
    # Get current oldest tweet in DB
    oldest_in_db = conn.execute("""
        SELECT id, timestamp FROM tweets 
        WHERE asset_id = ? 
        ORDER BY timestamp ASC LIMIT 1
    """, [asset_id]).fetchone()
    
    if oldest_in_db:
        print(f"Current oldest in DB: {oldest_in_db[1]} (ID: {oldest_in_db[0]})")
        until_id = oldest_in_db[0]
    else:
        print("No tweets in DB yet, starting fresh")
        until_id = None
    
    # Set up HTTP client
    headers = {"Authorization": f"Bearer {X_BEARER_TOKEN}"}
    
    total_fetched = 0
    total_inserted = 0
    total_filtered = 0
    page = 0
    pagination_token = None
    
    with httpx.Client(headers=headers) as client:
        # Get user ID
        print(f"\nLooking up @{founder}...")
        user_id = get_user_id(client, founder)
        print(f"User ID: {user_id}")
        
        print(f"\nFetching tweets (max {max_pages} pages)...")
        print("-" * 40)
        
        while page < max_pages:
            page += 1
            
            tweets, next_token, success = fetch_page(
                client, user_id,
                pagination_token=pagination_token,
                until_id=until_id if page == 1 else None,  # Only use until_id on first page
            )
            
            if not success:
                print(f"  Page {page}: FAILED, stopping")
                break
            
            if not tweets:
                print(f"  Page {page}: No more tweets available")
                break
            
            # Filter pre-launch tweets
            valid_tweets = []
            for t in tweets:
                if t["timestamp"] < launch_date:
                    total_filtered += 1
                    # If we hit pre-launch tweets, we've gone far enough
                    print(f"  Page {page}: Hit pre-launch tweet ({t['timestamp'].strftime('%Y-%m-%d')}), stopping")
                    break
                valid_tweets.append(t)
            
            total_fetched += len(tweets)
            
            # SAVE IMMEDIATELY - this is key for resilience
            if valid_tweets:
                inserted = insert_tweets(conn, asset_id, valid_tweets)
                total_inserted += inserted
                
                oldest = min(valid_tweets, key=lambda t: t["timestamp"])
                newest = max(valid_tweets, key=lambda t: t["timestamp"])
                
                print(f"  Page {page}: {len(tweets)} fetched, {inserted} new, oldest: {oldest['timestamp'].strftime('%Y-%m-%d %H:%M')}")
            else:
                print(f"  Page {page}: {len(tweets)} fetched, 0 valid (pre-launch)")
            
            # Check if we hit pre-launch
            if total_filtered > 0:
                break
            
            # Check for more pages
            if not next_token:
                print(f"  No more pages (pagination ended)")
                break
            
            pagination_token = next_token
            
            # Rate limiting
            time.sleep(1.5)
    
    # Final summary
    print("-" * 40)
    print(f"\nSUMMARY:")
    print(f"  Pages fetched: {page}")
    print(f"  Tweets fetched: {total_fetched}")
    print(f"  Tweets inserted: {total_inserted}")
    print(f"  Pre-launch filtered: {total_filtered}")
    
    # Show new date range
    result = conn.execute("""
        SELECT COUNT(*), MIN(timestamp), MAX(timestamp)
        FROM tweets WHERE asset_id = ?
    """, [asset_id]).fetchone()
    
    print(f"\nDB now has {result[0]} tweets")
    print(f"Date range: {result[1]} to {result[2]}")
    
    conn.close()
    return total_inserted


def main():
    parser = argparse.ArgumentParser(description="Backfill tweets for an asset")
    parser.add_argument("--asset", "-a", required=True, help="Asset ID (e.g., jup)")
    parser.add_argument("--max-pages", "-m", type=int, default=100, help="Max pages to fetch")
    
    args = parser.parse_args()
    backfill_asset(args.asset, args.max_pages)


if __name__ == "__main__":
    main()

