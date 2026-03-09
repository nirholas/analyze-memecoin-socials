"""
01: What does the distribution of outcomes look like?
Where are the tails? What creates the extreme events?
"""

import json
from pathlib import Path
from collections import Counter
import statistics

# Load all tweets
STATIC_DIR = Path(__file__).parent.parent / "web" / "public" / "static"

all_events = []
for asset_dir in STATIC_DIR.iterdir():
    if not asset_dir.is_dir():
        continue
    tweet_file = asset_dir / "tweet_events.json"
    if tweet_file.exists():
        with open(tweet_file) as f:
            data = json.load(f)
            for event in data.get("events", []):
                event["asset"] = asset_dir.name
                all_events.append(event)

# Get all 24h changes
changes = [e["change_24h_pct"] for e in all_events if e.get("change_24h_pct") is not None]

print("=" * 70)
print("DISTRIBUTION OF 24H CHANGES AFTER FOUNDER TWEETS")
print("=" * 70)
print(f"Total tweets with price data: {len(changes)}")
print()

# Basic stats
print("Basic Statistics:")
print(f"  Mean:   {statistics.mean(changes):+.2f}%")
print(f"  Median: {statistics.median(changes):+.2f}%")
print(f"  StdDev: {statistics.stdev(changes):.2f}%")
print()

# Percentiles
sorted_changes = sorted(changes)
n = len(sorted_changes)
percentiles = [1, 5, 10, 25, 50, 75, 90, 95, 99]
print("Percentiles:")
for p in percentiles:
    idx = int(n * p / 100)
    val = sorted_changes[min(idx, n-1)]
    print(f"  {p:>2}th percentile: {val:+.2f}%")
print()

# Bucket distribution
print("Distribution by bucket:")
buckets = {
    "< -20%": 0,
    "-20% to -10%": 0,
    "-10% to -5%": 0,
    "-5% to 0%": 0,
    "0% to 5%": 0,
    "5% to 10%": 0,
    "10% to 20%": 0,
    "> 20%": 0,
}

for c in changes:
    if c < -20:
        buckets["< -20%"] += 1
    elif c < -10:
        buckets["-20% to -10%"] += 1
    elif c < -5:
        buckets["-10% to -5%"] += 1
    elif c < 0:
        buckets["-5% to 0%"] += 1
    elif c < 5:
        buckets["0% to 5%"] += 1
    elif c < 10:
        buckets["5% to 10%"] += 1
    elif c < 20:
        buckets["10% to 20%"] += 1
    else:
        buckets["> 20%"] += 1

total = len(changes)
for bucket, count in buckets.items():
    pct = count / total * 100
    bar = "█" * int(pct / 2)
    print(f"  {bucket:>15}: {count:>4} ({pct:>5.1f}%) {bar}")

print()
print("=" * 70)
print("THE TAILS: EXTREME EVENTS")
print("=" * 70)

# Top 15 biggest pumps
sorted_by_change = sorted(all_events, key=lambda e: e.get("change_24h_pct") or 0, reverse=True)

print("\nTOP 15 BIGGEST PUMPS (24h after tweet):")
print("-" * 70)
for i, e in enumerate(sorted_by_change[:15]):
    change = e.get("change_24h_pct", 0)
    mcap = e.get("market_cap_at_tweet", 0)
    mcap_str = f"${mcap/1e6:.0f}M" if mcap else "N/A"
    text = e.get("text", "")[:50].replace("\n", " ")
    print(f"{i+1:>2}. {e['asset']:>10} | {change:>+7.1f}% | {mcap_str:>8} | {e['founder'][:15]}")
    print(f"    \"{text}...\"")

# Top 15 biggest dumps
print("\nTOP 15 BIGGEST DUMPS (24h after tweet):")
print("-" * 70)
for i, e in enumerate(sorted_by_change[-15:][::-1]):
    change = e.get("change_24h_pct", 0)
    mcap = e.get("market_cap_at_tweet", 0)
    mcap_str = f"${mcap/1e6:.0f}M" if mcap else "N/A"
    text = e.get("text", "")[:50].replace("\n", " ")
    print(f"{i+1:>2}. {e['asset']:>10} | {change:>+7.1f}% | {mcap_str:>8} | {e['founder'][:15]}")
    print(f"    \"{text}...\"")

# Analyze what the extremes have in common
print()
print("=" * 70)
print("WHAT DO THE EXTREMES HAVE IN COMMON?")
print("=" * 70)

top_20_pumps = sorted_by_change[:20]
top_20_dumps = sorted_by_change[-20:]

# Asset concentration
print("\nAsset concentration in top 20 pumps:")
pump_assets = Counter(e["asset"] for e in top_20_pumps)
for asset, count in pump_assets.most_common():
    print(f"  {asset}: {count}")

print("\nAsset concentration in top 20 dumps:")
dump_assets = Counter(e["asset"] for e in top_20_dumps)
for asset, count in dump_assets.most_common():
    print(f"  {asset}: {count}")

# Market cap comparison
pump_mcaps = [e.get("market_cap_at_tweet", 0) for e in top_20_pumps if e.get("market_cap_at_tweet")]
dump_mcaps = [e.get("market_cap_at_tweet", 0) for e in top_20_dumps if e.get("market_cap_at_tweet")]
all_mcaps = [e.get("market_cap_at_tweet", 0) for e in all_events if e.get("market_cap_at_tweet")]

print(f"\nAverage market cap:")
print(f"  Top 20 pumps: ${statistics.mean(pump_mcaps)/1e6:.0f}M")
print(f"  Top 20 dumps: ${statistics.mean(dump_mcaps)/1e6:.0f}M")
print(f"  All tweets:   ${statistics.mean(all_mcaps)/1e6:.0f}M")
