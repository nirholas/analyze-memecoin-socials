"""
Fetch historical tweets using date-based filtering.
Handles rate limits gracefully and saves progress after each successful fetch.

Usage:
    python fetch_historical_tweets.py --asset jup
    python fetch_historical_tweets.py --asset useless
"""
import argparse
import httpx
import time
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path

from config import X_BEARER_TOKEN, X_API_BASE, DATA_DIR
from db import (
    get_connection, init_schema, get_asset, insert_tweets,
    load_assets_from_json
)


def parse_iso(iso_str: str) -> datetime:
    iso_str = iso_str.replace("Z", "+00:00")
    dt = datetime.fromisoformat(iso_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def get_user_id(client: httpx.Client, username: str) -> str:
    for attempt in range(3):
        resp = client.get(f"{X_API_BASE}/users/by/username/{username}", timeout=30.0)
        if resp.status_code == 429:
            wait = 60 * (attempt + 1)
            print(f"  Rate limited on user lookup, waiting {wait}s...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()["data"]["id"]
    raise Exception(f"Could not get user ID for {username}")


def fetch_month(client: httpx.Client, user_id: str, year: int, month: int) -> list:
    """Fetch all tweets from a specific month."""
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    if month == 12:
        end = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
    else:
        end = datetime(year, month + 1, 1, tzinfo=timezone.utc)
    
    start_str = start.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_str = end.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    all_tweets = []
    next_token = None
    page = 0
    
    while page < 20:  # Max 20 pages per month
        page += 1
        params = {
            "max_results": 100,
            "tweet.fields": "created_at,public_metrics",
            "exclude": "retweets,replies",
            "start_time": start_str,
            "end_time": end_str,
        }
        if next_token:
            params["pagination_token"] = next_token
        
        for attempt in range(5):
            resp = client.get(
                f"{X_API_BASE}/users/{user_id}/tweets",
                params=params,
                timeout=30.0
            )
            
            if resp.status_code == 429:
                wait = 60 * (attempt + 1)
                print(f"    Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            break
        
        if resp.status_code != 200:
            print(f"    API error {resp.status_code}: {resp.text[:100]}")
            break
        
        data = resp.json()
        
        if "data" not in data:
            break
        
        tweets = data["data"]
        for t in tweets:
            metrics = t.get("public_metrics", {})
            all_tweets.append({
                "id": t["id"],
                "text": t["text"],
                "created_at": t["created_at"],
                "timestamp": parse_iso(t["created_at"]),
                "likes": metrics.get("like_count", 0),
                "retweets": metrics.get("retweet_count", 0),
                "replies": metrics.get("reply_count", 0),
                "impressions": metrics.get("impression_count", 0),
            })
        
        next_token = data.get("meta", {}).get("next_token")
        if not next_token:
            break
        
        time.sleep(1)
    
    return all_tweets


def fetch_historical(asset_id: str):
    """Fetch historical tweets month by month, saving progress."""
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
        launch_date = parse_iso(launch_date)
    
    print(f"\n{'='*60}")
    print(f"HISTORICAL FETCH: {asset['name']} (@{founder})")
    print(f"Launch: {launch_date.strftime('%Y-%m-%d')}")
    print(f"{'='*60}")
    
    headers = {"Authorization": f"Bearer {X_BEARER_TOKEN}"}
    
    with httpx.Client(headers=headers) as client:
        # Get user ID
        print(f"\nLooking up @{founder}...")
        user_id = get_user_id(client, founder)
        print(f"User ID: {user_id}")
        
        # Get current oldest tweet in DB
        oldest = conn.execute("""
            SELECT MIN(timestamp) FROM tweets WHERE asset_id = ?
        """, [asset_id]).fetchone()[0]
        
        if oldest:
            print(f"Current oldest in DB: {oldest}")
        
        # Generate months to fetch (from launch to now)
        now = datetime.now(timezone.utc)
        current = datetime(launch_date.year, launch_date.month, 1, tzinfo=timezone.utc)
        
        total_inserted = 0
        
        print(f"\nFetching month by month...")
        print("-" * 40)
        
        while current < now:
            year, month = current.year, current.month
            label = current.strftime("%Y-%m")
            
            print(f"\n{label}:", end=" ", flush=True)
            
            tweets = fetch_month(client, user_id, year, month)
            
            if tweets:
                # Filter pre-launch
                valid = [t for t in tweets if t["timestamp"] >= launch_date]
                
                if valid:
                    inserted = insert_tweets(conn, asset_id, valid)
                    total_inserted += inserted
                    print(f"{len(tweets)} fetched, {inserted} new")
                else:
                    print(f"{len(tweets)} fetched, 0 valid (pre-launch)")
            else:
                print("0 tweets")
            
            # Move to next month
            if month == 12:
                current = datetime(year + 1, 1, 1, tzinfo=timezone.utc)
            else:
                current = datetime(year, month + 1, 1, tzinfo=timezone.utc)
            
            # Rate limit between months
            time.sleep(2)
        
        print("-" * 40)
        print(f"\nTotal inserted: {total_inserted}")
        
        # Show final state
        result = conn.execute("""
            SELECT COUNT(*), MIN(timestamp), MAX(timestamp)
            FROM tweets WHERE asset_id = ?
        """, [asset_id]).fetchone()
        
        print(f"DB now has {result[0]} tweets")
        print(f"Range: {result[1]} to {result[2]}")
    
    conn.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--asset", "-a", required=True)
    args = parser.parse_args()
    
    fetch_historical(args.asset)


if __name__ == "__main__":
    main()

