"""
Fetch specific tweets by ID using X API v2.

Use this to fill gaps when Nitter scraping misses individual tweets.
X API's /tweets endpoint guarantees retrieval if the tweet exists.

Usage:
    python fetch_tweet_by_id.py --asset gork --ids 1919087089276142005
    python fetch_tweet_by_id.py --asset gork --ids 1919087089276142005,1919087780681990520
    python fetch_tweet_by_id.py --asset gork --ids "1919087089276142005 1919087780681990520"
"""
import argparse
import httpx
import json
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from config import X_BEARER_TOKEN, X_API_BASE
from db import get_connection, init_schema, get_asset, insert_tweets, load_assets_from_json


def parse_iso_timestamp(iso_str: str) -> datetime:
    """Parse ISO timestamp string to timezone-aware datetime."""
    iso_str = iso_str.replace("Z", "+00:00")
    dt = datetime.fromisoformat(iso_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def fetch_tweets_by_ids(tweet_ids: List[str]) -> Dict[str, Any]:
    """
    Fetch multiple tweets by ID using X API v2 /tweets endpoint.

    Args:
        tweet_ids: List of tweet ID strings

    Returns:
        Dict with 'tweets' (list of tweet data) and 'errors' (list of error messages)
    """
    if not X_BEARER_TOKEN:
        return {"tweets": [], "errors": ["X_BEARER_TOKEN not configured"]}

    if not tweet_ids:
        return {"tweets": [], "errors": ["No tweet IDs provided"]}

    # X API v2 /tweets endpoint - can fetch up to 100 tweets at once
    url = f"{X_API_BASE}/tweets"
    params = {
        "ids": ",".join(tweet_ids[:100]),  # Max 100 per request
        "tweet.fields": "created_at,public_metrics,conversation_id,author_id,in_reply_to_user_id",
        "expansions": "author_id,in_reply_to_user_id",
        "user.fields": "username",
    }

    headers = {
        "Authorization": f"Bearer {X_BEARER_TOKEN}",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    }

    result = {"tweets": [], "errors": []}

    try:
        with httpx.Client(headers=headers) as client:
            response = client.get(url, params=params, timeout=30.0)

            if response.status_code == 429:
                result["errors"].append("Rate limit exceeded (429)")
                return result

            if response.status_code == 401:
                result["errors"].append("Unauthorized - check X_BEARER_TOKEN")
                return result

            if response.status_code != 200:
                result["errors"].append(f"HTTP {response.status_code}: {response.text[:200]}")
                return result

            data = response.json()

            # Build user lookup from includes
            user_lookup = {}
            if "includes" in data and "users" in data["includes"]:
                for user in data["includes"]["users"]:
                    user_lookup[user["id"]] = user["username"]

            # Process tweets
            tweets_data = data.get("data", [])
            for tweet in tweets_data:
                metrics = tweet.get("public_metrics", {})

                # Get reply_to username if this is a reply
                reply_to = None
                in_reply_to_user_id = tweet.get("in_reply_to_user_id")
                if in_reply_to_user_id and in_reply_to_user_id in user_lookup:
                    reply_to = user_lookup[in_reply_to_user_id]

                result["tweets"].append({
                    "id": tweet["id"],
                    "text": tweet["text"],
                    "created_at": tweet["created_at"],
                    "timestamp": parse_iso_timestamp(tweet["created_at"]),
                    "likes": metrics.get("like_count", 0),
                    "retweets": metrics.get("retweet_count", 0),
                    "replies": metrics.get("reply_count", 0),
                    "impressions": metrics.get("impression_count", 0),
                    "reply_to": reply_to,
                    "author_id": tweet.get("author_id"),
                    "author_username": user_lookup.get(tweet.get("author_id")),
                })

            # Check for errors (e.g., deleted tweets, not found)
            if "errors" in data:
                for err in data["errors"]:
                    tweet_id = err.get("resource_id", "unknown")
                    detail = err.get("detail", err.get("title", "Unknown error"))
                    result["errors"].append(f"Tweet {tweet_id}: {detail}")

    except httpx.TimeoutException:
        result["errors"].append("Request timed out")
    except Exception as e:
        result["errors"].append(f"Exception: {type(e).__name__}: {e}")

    return result


def add_tweets_to_asset(asset_id: str, tweet_ids: List[str], dry_run: bool = False) -> Dict[str, Any]:
    """
    Fetch tweets by ID and add them to an asset in the database.

    Args:
        asset_id: The asset ID (e.g., "gork")
        tweet_ids: List of tweet IDs to fetch
        dry_run: If True, fetch but don't insert

    Returns:
        Result dict with status, fetched count, inserted count
    """
    conn = get_connection()
    init_schema(conn)
    load_assets_from_json(conn)

    asset = get_asset(conn, asset_id)
    if not asset:
        conn.close()
        return {"status": "error", "reason": f"Asset '{asset_id}' not found"}

    print(f"\nFetching {len(tweet_ids)} tweets by ID for {asset['name']} (@{asset['founder']})")
    print(f"Tweet IDs: {', '.join(tweet_ids)}")

    # Fetch from X API
    result = fetch_tweets_by_ids(tweet_ids)

    if result["errors"]:
        print(f"\nErrors:")
        for err in result["errors"]:
            print(f"  ❌ {err}")

    if not result["tweets"]:
        conn.close()
        return {
            "status": "error" if result["errors"] else "no_tweets",
            "reason": result["errors"][0] if result["errors"] else "No tweets returned",
            "fetched": 0,
            "inserted": 0,
        }

    print(f"\nFetched {len(result['tweets'])} tweets:")
    for tweet in result["tweets"]:
        author = tweet.get("author_username", "unknown")
        created = tweet["created_at"]
        text_preview = tweet["text"][:60].replace("\n", " ") + ("..." if len(tweet["text"]) > 60 else "")
        reply_info = f" (reply to @{tweet['reply_to']})" if tweet.get("reply_to") else ""
        print(f"  • @{author} [{created}]{reply_info}")
        print(f"    \"{text_preview}\"")
        print(f"    Likes: {tweet['likes']}, RTs: {tweet['retweets']}, Impressions: {tweet['impressions']}")

    # Verify author matches expected founder
    expected_author = asset["founder"].lower()
    for tweet in result["tweets"]:
        author = (tweet.get("author_username") or "").lower()
        if author and author != expected_author:
            print(f"\n⚠️  WARNING: Tweet {tweet['id']} is by @{author}, not @{asset['founder']}")
            print(f"    This tweet may not belong to this asset.")

    if dry_run:
        print(f"\n🔍 DRY RUN - not inserting into database")
        conn.close()
        return {
            "status": "dry_run",
            "fetched": len(result["tweets"]),
            "inserted": 0,
        }

    # Insert into database
    inserted = insert_tweets(conn, asset_id, result["tweets"])

    # Show updated count
    total_in_db = conn.execute(
        "SELECT COUNT(*) FROM tweets WHERE asset_id = ?", [asset_id]
    ).fetchone()[0]

    print(f"\n✓ Inserted {inserted} tweets")
    print(f"📊 Total tweets in DB for {asset_id}: {total_in_db}")

    conn.close()

    return {
        "status": "success",
        "fetched": len(result["tweets"]),
        "inserted": inserted,
        "errors": result["errors"],
    }


def main():
    parser = argparse.ArgumentParser(
        description="Fetch specific tweets by ID and add to asset",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s --asset gork --ids 1919087089276142005
  %(prog)s --asset gork --ids 1919087089276142005,1919087780681990520
  %(prog)s --asset gork --ids "1919087089276142005 1919087780681990520"
  %(prog)s --asset gork --ids 1919087089276142005 --dry-run
        """
    )
    parser.add_argument(
        "--asset", "-a",
        required=True,
        help="Asset ID to add tweets to (e.g., 'gork')"
    )
    parser.add_argument(
        "--ids", "-i",
        required=True,
        help="Tweet IDs to fetch (comma or space separated)"
    )
    parser.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Fetch and display tweets without inserting into database"
    )

    args = parser.parse_args()

    # Parse tweet IDs - support comma, space, or newline separated
    ids_input = args.ids.replace(",", " ").replace("\n", " ")
    tweet_ids = [tid.strip() for tid in ids_input.split() if tid.strip()]

    if not tweet_ids:
        print("Error: No valid tweet IDs provided")
        return

    result = add_tweets_to_asset(args.asset, tweet_ids, dry_run=args.dry_run)

    if result["status"] == "success":
        print(f"\n✅ Done: {result['fetched']} fetched, {result['inserted']} inserted")
    elif result["status"] == "dry_run":
        print(f"\n🔍 Dry run complete: {result['fetched']} tweets would be inserted")
    else:
        print(f"\n❌ Failed: {result.get('reason', 'Unknown error')}")


if __name__ == "__main__":
    main()
