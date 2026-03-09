"""
Data loader for exploration - reads all tweet events from static JSON files.
Read-only, no modifications to source data.
"""

import json
from pathlib import Path
from dataclasses import dataclass
from typing import Optional
import statistics

STATIC_DIR = Path(__file__).parent.parent / "web" / "public" / "static"

@dataclass
class TweetEvent:
    tweet_id: str
    asset: str
    asset_name: str
    founder: str
    timestamp: int
    timestamp_iso: str
    text: str
    likes: int
    retweets: int
    replies: int
    impressions: int
    price_at_tweet: Optional[float]
    price_1h: Optional[float]
    price_24h: Optional[float]
    change_1h_pct: Optional[float]
    change_24h_pct: Optional[float]
    market_cap_at_tweet: Optional[float]

def load_all_tweets() -> list[TweetEvent]:
    """Load all tweet events from all assets."""
    all_events = []

    for asset_dir in STATIC_DIR.iterdir():
        if not asset_dir.is_dir():
            continue

        tweet_file = asset_dir / "tweet_events.json"
        if not tweet_file.exists():
            continue

        with open(tweet_file) as f:
            data = json.load(f)

        for event in data.get("events", []):
            all_events.append(TweetEvent(
                tweet_id=event.get("tweet_id", ""),
                asset=asset_dir.name,
                asset_name=event.get("asset_name", asset_dir.name),
                founder=event.get("founder", ""),
                timestamp=event.get("timestamp", 0),
                timestamp_iso=event.get("timestamp_iso", ""),
                text=event.get("text", ""),
                likes=event.get("likes", 0),
                retweets=event.get("retweets", 0),
                replies=event.get("replies", 0),
                impressions=event.get("impressions", 0),
                price_at_tweet=event.get("price_at_tweet"),
                price_1h=event.get("price_1h"),
                price_24h=event.get("price_24h"),
                change_1h_pct=event.get("change_1h_pct"),
                change_24h_pct=event.get("change_24h_pct"),
                market_cap_at_tweet=event.get("market_cap_at_tweet"),
            ))

    # Sort by timestamp
    all_events.sort(key=lambda e: e.timestamp)
    return all_events

def summary(tweets: list[TweetEvent]) -> dict:
    """Quick summary stats."""
    changes_24h = [t.change_24h_pct for t in tweets if t.change_24h_pct is not None]

    return {
        "total_tweets": len(tweets),
        "with_price_data": len(changes_24h),
        "assets": len(set(t.asset for t in tweets)),
        "founders": len(set(t.founder for t in tweets)),
        "date_range": (
            min(t.timestamp_iso for t in tweets),
            max(t.timestamp_iso for t in tweets),
        ),
        "change_24h": {
            "mean": statistics.mean(changes_24h),
            "median": statistics.median(changes_24h),
            "stdev": statistics.stdev(changes_24h),
            "min": min(changes_24h),
            "max": max(changes_24h),
        }
    }

if __name__ == "__main__":
    tweets = load_all_tweets()
    stats = summary(tweets)

    print(f"Loaded {stats['total_tweets']} tweets")
    print(f"With price data: {stats['with_price_data']}")
    print(f"Assets: {stats['assets']}")
    print(f"Founders: {stats['founders']}")
    print(f"Date range: {stats['date_range'][0]} to {stats['date_range'][1]}")
    print()
    print("24h Change Distribution:")
    print(f"  Mean:   {stats['change_24h']['mean']:+.2f}%")
    print(f"  Median: {stats['change_24h']['median']:+.2f}%")
    print(f"  StdDev: {stats['change_24h']['stdev']:.2f}%")
    print(f"  Min:    {stats['change_24h']['min']:+.2f}%")
    print(f"  Max:    {stats['change_24h']['max']:+.2f}%")
