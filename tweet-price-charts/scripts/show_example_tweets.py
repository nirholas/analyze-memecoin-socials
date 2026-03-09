#!/usr/bin/env python3
"""
Show example tweets that exemplify each founder's style.
"""

import json
from pathlib import Path
from datetime import datetime

def load_tweets(asset_id):
    """Load tweet data for an asset."""
    path = Path(f"/Users/satoshi/tweet-price/web/public/static/{asset_id}/tweet_events.json")
    if not path.exists():
        return []

    with open(path) as f:
        data = json.load(f)
    return data.get("events", [])

def format_tweet(tweet, founder):
    """Format a tweet for display."""
    text = tweet["text"]
    if len(text) > 200:
        text = text[:197] + "..."

    timestamp = datetime.utcfromtimestamp(tweet["timestamp"]).strftime("%Y-%m-%d")
    likes = tweet.get("likes", 0)
    length = len(tweet["text"])

    return f"""
  [{timestamp}] {likes:,} likes, {length} chars
  "{text}"
"""

def main():
    print("=" * 100)
    print("EXAMPLE TWEETS - FOUNDER STYLE PROFILES")
    print("=" * 100)

    # Load all tweets
    assets = {
        "pump": "a1lon9",
        "hype": "chameleon_jeff",
        "aster": "cz_binance",
        "believe": "pasternak",
        "jup": "weremeow",
        "monad": "keoneHD",
        "useless": "theunipcs",
    }

    for asset_id, founder in assets.items():
        tweets = load_tweets(asset_id)
        if not tweets:
            continue

        print(f"\n{'=' * 100}")
        print(f"FOUNDER: {founder} ({asset_id.upper()})")
        print("=" * 100)

        # Sort by engagement to get best examples
        sorted_tweets = sorted(tweets, key=lambda t: t.get("likes", 0), reverse=True)

        # Show top 3 most engaged tweets
        print("\nMOST ENGAGED TWEETS:")
        for i, tweet in enumerate(sorted_tweets[:3], 1):
            print(f"\n{i}.")
            print(format_tweet(tweet, founder))

        # Show a few recent tweets for current style
        recent = sorted(tweets, key=lambda t: t["timestamp"], reverse=True)[:2]
        print("\nRECENT TWEETS (current style):")
        for i, tweet in enumerate(recent, 1):
            print(f"\n{i}.")
            print(format_tweet(tweet, founder))

    # Specific style examples
    print("\n" + "=" * 100)
    print("STYLE SIGNATURE EXAMPLES")
    print("=" * 100)

    print("\n1. SHORTEST vs LONGEST TWEETS:")
    print("-" * 100)

    # Get one short and one long from each
    for asset_id, founder in assets.items():
        tweets = load_tweets(asset_id)
        if not tweets:
            continue

        shortest = min(tweets, key=lambda t: len(t["text"]))
        longest = max(tweets, key=lambda t: len(t["text"]))

        print(f"\n{founder}:")
        print(f"  Shortest ({len(shortest['text'])} chars): \"{shortest['text'][:100]}\"")
        print(f"  Longest ({len(longest['text'])} chars): \"{longest['text'][:100]}...\"")

    print("\n" + "=" * 100)
    print("CONTENT TYPE EXAMPLES")
    print("=" * 100)

    # Show examples of different content types
    print("\nDEFENSIVE/FUD-FIGHTING TWEETS:")
    print("-" * 100)

    # chameleon_jeff defensive examples
    tweets = load_tweets("hype")
    defensive = [t for t in tweets if "fud" in t["text"].lower() or "debunk" in t["text"].lower()]
    if defensive:
        print(f"\nchameleon_jeff (signature defensive style):")
        for tweet in defensive[:2]:
            print(format_tweet(tweet, "chameleon_jeff"))

    print("\nSHIPPING/BUILDING TWEETS:")
    print("-" * 100)

    # pasternak shipping examples
    tweets = load_tweets("believe")
    shipping = [t for t in tweets if any(word in t["text"].lower()
                for word in ["launch", "ship", "release", "build", "live"])]
    if shipping:
        print(f"\npasternak (builder's journal style):")
        for tweet in shipping[:2]:
            print(format_tweet(tweet, "pasternak"))

    print("\nMEDIA-HEAVY TWEETS:")
    print("-" * 100)

    # cz_binance media examples
    tweets = load_tweets("aster")
    media = [t for t in tweets if "t.co" in t["text"]]
    if media:
        print(f"\ncz_binance (media machine style):")
        for tweet in media[:2]:
            print(format_tweet(tweet, "cz_binance"))

    print("\nLONG-FORM EXPLANATION TWEETS:")
    print("-" * 100)

    # weremeow and theunipcs long tweets
    for asset_id, founder in [("jup", "weremeow"), ("useless", "theunipcs")]:
        tweets = load_tweets(asset_id)
        long_tweets = sorted([t for t in tweets if len(t["text"]) > 400],
                           key=lambda t: len(t["text"]), reverse=True)
        if long_tweets:
            print(f"\n{founder}:")
            print(format_tweet(long_tweets[0], founder))

    print("\n" + "=" * 100)

if __name__ == "__main__":
    main()
