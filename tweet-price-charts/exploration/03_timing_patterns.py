"""
03: When do tweets land best?
Day of week, time of day, sequence effects
"""

import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import statistics

STATIC_DIR = Path(__file__).parent.parent / "web" / "public" / "static"

# Load all tweets
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

# Filter to those with price data
events = [e for e in all_events if e.get("change_24h_pct") is not None]

print("=" * 70)
print("TIMING PATTERN ANALYSIS")
print("=" * 70)

# Parse timestamps
for e in events:
    dt = datetime.fromisoformat(e["timestamp_iso"].replace("Z", "+00:00"))
    e["hour"] = dt.hour
    e["day_of_week"] = dt.weekday()  # 0=Monday, 6=Sunday
    e["day_name"] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"][dt.weekday()]

# Day of week analysis
print("\nDay of Week:")
print("-" * 70)
day_buckets = defaultdict(list)
for e in events:
    day_buckets[e["day_name"]].append(e["change_24h_pct"])

for day in ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]:
    changes = day_buckets[day]
    if changes:
        avg = statistics.mean(changes)
        med = statistics.median(changes)
        wins = sum(1 for c in changes if c > 0) / len(changes) * 100
        print(f"  {day}: avg {avg:+6.2f}%, med {med:+6.2f}%, win {wins:5.1f}%, n={len(changes)}")

# Weekday vs Weekend
weekday = [e["change_24h_pct"] for e in events if e["day_of_week"] < 5]
weekend = [e["change_24h_pct"] for e in events if e["day_of_week"] >= 5]

print(f"\nWeekday vs Weekend:")
print(f"  Weekday: avg {statistics.mean(weekday):+.2f}%, med {statistics.median(weekday):+.2f}%, win {sum(1 for c in weekday if c > 0)/len(weekday)*100:.1f}%, n={len(weekday)}")
print(f"  Weekend: avg {statistics.mean(weekend):+.2f}%, med {statistics.median(weekend):+.2f}%, win {sum(1 for c in weekend if c > 0)/len(weekend)*100:.1f}%, n={len(weekend)}")

# Hour of day analysis (UTC)
print("\n" + "=" * 70)
print("Hour of Day (UTC):")
print("-" * 70)
hour_buckets = defaultdict(list)
for e in events:
    hour_buckets[e["hour"]].append(e["change_24h_pct"])

for hour in range(24):
    changes = hour_buckets[hour]
    if len(changes) >= 20:  # Only show hours with enough data
        avg = statistics.mean(changes)
        med = statistics.median(changes)
        wins = sum(1 for c in changes if c > 0) / len(changes) * 100
        bar = "█" * int(wins / 5)
        print(f"  {hour:02d}:00: avg {avg:+6.2f}%, med {med:+6.2f}%, win {wins:5.1f}% {bar} n={len(changes)}")

# Best and worst hours
print("\nBest 3 hours:")
sorted_hours = sorted(hour_buckets.items(), key=lambda x: statistics.median(x[1]) if len(x[1]) >= 20 else -100, reverse=True)
for hour, changes in sorted_hours[:3]:
    if len(changes) >= 20:
        print(f"  {hour:02d}:00 UTC: median {statistics.median(changes):+.2f}%")

print("\nWorst 3 hours:")
for hour, changes in sorted_hours[-3:]:
    if len(changes) >= 20:
        print(f"  {hour:02d}:00 UTC: median {statistics.median(changes):+.2f}%")

# Sequence analysis - nth tweet from founder
print("\n" + "=" * 70)
print("SEQUENCE ANALYSIS")
print("=" * 70)

# Group by asset and sort by time
asset_tweets = defaultdict(list)
for e in events:
    asset_tweets[e["asset"]].append(e)

for asset in asset_tweets:
    asset_tweets[asset].sort(key=lambda x: x["timestamp"])
    for i, e in enumerate(asset_tweets[asset]):
        e["tweet_order"] = i + 1

# Analyze by tweet order
print("\nTweet order (nth tweet from founder for this asset):")
print("-" * 70)
order_buckets = {
    "1-5 (early)": [],
    "6-20": [],
    "21-50": [],
    "51-100": [],
    "100+": [],
}

for e in events:
    order = e.get("tweet_order", 0)
    if order <= 5:
        order_buckets["1-5 (early)"].append(e["change_24h_pct"])
    elif order <= 20:
        order_buckets["6-20"].append(e["change_24h_pct"])
    elif order <= 50:
        order_buckets["21-50"].append(e["change_24h_pct"])
    elif order <= 100:
        order_buckets["51-100"].append(e["change_24h_pct"])
    else:
        order_buckets["100+"].append(e["change_24h_pct"])

for bucket in ["1-5 (early)", "6-20", "21-50", "51-100", "100+"]:
    changes = order_buckets[bucket]
    if changes:
        avg = statistics.mean(changes)
        med = statistics.median(changes)
        wins = sum(1 for c in changes if c > 0) / len(changes) * 100
        print(f"  {bucket:>12}: avg {avg:+6.2f}%, med {med:+6.2f}%, win {wins:5.1f}%, n={len(changes)}")

# Gap since last tweet
print("\n" + "=" * 70)
print("GAP SINCE LAST TWEET")
print("=" * 70)

for asset in asset_tweets:
    tweets = asset_tweets[asset]
    for i, e in enumerate(tweets):
        if i == 0:
            e["gap_hours"] = None
        else:
            gap_seconds = e["timestamp"] - tweets[i-1]["timestamp"]
            e["gap_hours"] = gap_seconds / 3600

gap_buckets = {
    "< 1 hour": [],
    "1-6 hours": [],
    "6-24 hours": [],
    "1-3 days": [],
    "3-7 days": [],
    "7+ days": [],
}

for e in events:
    gap = e.get("gap_hours")
    if gap is None:
        continue
    if gap < 1:
        gap_buckets["< 1 hour"].append(e["change_24h_pct"])
    elif gap < 6:
        gap_buckets["1-6 hours"].append(e["change_24h_pct"])
    elif gap < 24:
        gap_buckets["6-24 hours"].append(e["change_24h_pct"])
    elif gap < 72:
        gap_buckets["1-3 days"].append(e["change_24h_pct"])
    elif gap < 168:
        gap_buckets["3-7 days"].append(e["change_24h_pct"])
    else:
        gap_buckets["7+ days"].append(e["change_24h_pct"])

print("\nTime since last tweet:")
print("-" * 70)
for bucket in ["< 1 hour", "1-6 hours", "6-24 hours", "1-3 days", "3-7 days", "7+ days"]:
    changes = gap_buckets[bucket]
    if changes:
        avg = statistics.mean(changes)
        med = statistics.median(changes)
        wins = sum(1 for c in changes if c > 0) / len(changes) * 100
        print(f"  {bucket:>12}: avg {avg:+6.2f}%, med {med:+6.2f}%, win {wins:5.1f}%, n={len(changes)}")

# First tweet after long silence (7+ days)
print("\n" + "=" * 70)
print("FIRST TWEET AFTER SILENCE")
print("=" * 70)

silence_tweets = [e for e in events if e.get("gap_hours") and e["gap_hours"] >= 168]  # 7+ days
normal_tweets = [e for e in events if e.get("gap_hours") and e["gap_hours"] < 168]

print(f"\nFirst tweet after 7+ day silence: {len(silence_tweets)} tweets")
if silence_tweets:
    changes = [e["change_24h_pct"] for e in silence_tweets]
    print(f"  Avg: {statistics.mean(changes):+.2f}%")
    print(f"  Med: {statistics.median(changes):+.2f}%")
    print(f"  Win rate: {sum(1 for c in changes if c > 0) / len(changes) * 100:.1f}%")

print(f"\nAll other tweets: {len(normal_tweets)} tweets")
if normal_tweets:
    changes = [e["change_24h_pct"] for e in normal_tweets]
    print(f"  Avg: {statistics.mean(changes):+.2f}%")
    print(f"  Med: {statistics.median(changes):+.2f}%")
    print(f"  Win rate: {sum(1 for c in changes if c > 0) / len(changes) * 100:.1f}%")
