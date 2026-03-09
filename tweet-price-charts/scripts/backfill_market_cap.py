#!/usr/bin/env python3
"""
Backfill historical market cap for assets with unstable supply (vesting unlocks).

For JUP and WLD, we can't use current_supply × historical_price because their
circulating supply changes significantly over time. Instead, we fetch historical
market cap directly from CoinGecko.

Usage:
    python backfill_market_cap.py
"""

import json
import time
from pathlib import Path
from typing import List, Tuple, Optional
import httpx

# CoinGecko API
CG_API = "https://api.coingecko.com/api/v3"

# Assets with supply_unstable that need CoinGecko historical market cap
UNSTABLE_ASSETS = {
    "jup": "jupiter-exchange-solana",
    "wld": "worldcoin-wld",
}

STATIC_DIR = Path(__file__).parent.parent / "web" / "public" / "static"


def fetch_coingecko_market_caps(
    coingecko_id: str,
    from_ts: int,
    to_ts: int
) -> List[Tuple[int, float]]:
    """
    Fetch historical market cap from CoinGecko.

    Returns list of (timestamp_seconds, market_cap) tuples.
    """
    url = f"{CG_API}/coins/{coingecko_id}/market_chart/range"
    params = {
        "vs_currency": "usd",
        "from": from_ts,
        "to": to_ts,
    }

    print(f"  Fetching from CoinGecko: {coingecko_id}")

    with httpx.Client(timeout=60.0) as client:
        response = client.get(url, params=params)

        if response.status_code == 429:
            print("  Rate limited, waiting 60s...")
            time.sleep(60)
            return fetch_coingecko_market_caps(coingecko_id, from_ts, to_ts)

        if response.status_code != 200:
            print(f"  Error: {response.status_code} - {response.text}")
            return []

        data = response.json()

    # Convert from [timestamp_ms, market_cap] to (timestamp_seconds, market_cap)
    market_caps = [(int(ts / 1000), mc) for ts, mc in data.get("market_caps", [])]
    print(f"  Got {len(market_caps)} market cap data points")

    return market_caps


def find_closest_market_cap(
    tweet_ts: int,
    market_caps: List[Tuple[int, float]]
) -> Optional[float]:
    """
    Find the market cap closest to the tweet timestamp.

    Uses the most recent market cap at or before the tweet time.
    """
    if not market_caps:
        return None

    # Find the last market cap at or before tweet time
    closest = None
    for ts, mc in market_caps:
        if ts <= tweet_ts:
            closest = mc
        else:
            break

    return closest


def backfill_asset(asset_id: str, coingecko_id: str) -> int:
    """
    Backfill market cap for a single asset.

    Returns number of events updated.
    """
    events_file = STATIC_DIR / asset_id / "tweet_events.json"

    if not events_file.exists():
        print(f"  No tweet_events.json found for {asset_id}")
        return 0

    # Load existing events
    with open(events_file) as f:
        data = json.load(f)

    events = data.get("events", [])
    if not events:
        print(f"  No events found for {asset_id}")
        return 0

    # Get timestamp range from events
    timestamps = [e["timestamp"] for e in events]
    min_ts = min(timestamps)
    max_ts = max(timestamps)

    # CoinGecko free tier: max 365 days of historical data
    now = int(time.time())
    max_lookback = 365 * 86400  # 365 days in seconds
    earliest_allowed = now - max_lookback

    # Clamp from_ts to 365 days ago
    from_ts = max(min_ts - 86400, earliest_allowed)
    to_ts = now

    if min_ts < earliest_allowed:
        old_count = sum(1 for ts in timestamps if ts < earliest_allowed)
        print(f"  Note: {old_count} tweets older than 365 days (CoinGecko free tier limit)")

    print(f"  Date range: {time.strftime('%Y-%m-%d', time.gmtime(min_ts))} to {time.strftime('%Y-%m-%d', time.gmtime(max_ts))}")

    # Fetch historical market caps
    market_caps = fetch_coingecko_market_caps(coingecko_id, from_ts, to_ts)

    if not market_caps:
        print(f"  No market cap data available")
        return 0

    # Update each event
    updated = 0
    for event in events:
        tweet_ts = event["timestamp"]
        mc = find_closest_market_cap(tweet_ts, market_caps)

        if mc is not None:
            event["market_cap_at_tweet"] = round(mc, 2)
            updated += 1
        else:
            event["market_cap_at_tweet"] = None

    # Save updated events
    with open(events_file, "w") as f:
        json.dump(data, f, separators=(',', ':'))

    return updated


def main():
    print("Backfilling market cap for assets with unstable supply...\n")

    for asset_id, coingecko_id in UNSTABLE_ASSETS.items():
        print(f"{asset_id.upper()} ({coingecko_id}):")
        updated = backfill_asset(asset_id, coingecko_id)
        print(f"  Updated {updated} events\n")

        # Rate limit courtesy
        time.sleep(1)

    print("Done!")


if __name__ == "__main__":
    main()
