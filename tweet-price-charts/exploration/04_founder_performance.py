"""
04: Which founders actually move markets?
Per-founder batting average, consistency, engagement correlation
"""

import json
from pathlib import Path
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

events = [e for e in all_events if e.get("change_24h_pct") is not None]

print("=" * 70)
print("FOUNDER PERFORMANCE LEADERBOARD")
print("=" * 70)

# Group by founder
founder_stats = defaultdict(list)
for e in events:
    founder_stats[e["founder"]].append(e)

print("\nRanked by WIN RATE (min 50 tweets):")
print("-" * 70)
print(f"{'Founder':<20} {'Win%':>7} {'Avg':>8} {'Med':>8} {'StdDev':>8} {'N':>6}")
print("-" * 70)

founder_rankings = []
for founder, tweets in founder_stats.items():
    if len(tweets) >= 50:
        changes = [t["change_24h_pct"] for t in tweets]
        wins = sum(1 for c in changes if c > 0) / len(changes) * 100
        avg = statistics.mean(changes)
        med = statistics.median(changes)
        std = statistics.stdev(changes)
        founder_rankings.append({
            "founder": founder,
            "win_rate": wins,
            "avg": avg,
            "med": med,
            "std": std,
            "n": len(tweets),
            "asset": tweets[0]["asset"],
        })

# Sort by win rate
founder_rankings.sort(key=lambda x: x["win_rate"], reverse=True)
for f in founder_rankings:
    print(f"{f['founder']:<20} {f['win_rate']:>6.1f}% {f['avg']:>+7.2f}% {f['med']:>+7.2f}% {f['std']:>7.2f}% {f['n']:>6}")

print("\n" + "=" * 70)
print("CONSISTENCY RANKING (lowest stdev = most predictable)")
print("-" * 70)
founder_rankings.sort(key=lambda x: x["std"])
for f in founder_rankings[:5]:
    print(f"{f['founder']:<20} StdDev: {f['std']:>6.2f}% | Win: {f['win_rate']:.1f}% | N={f['n']}")

print("\nMost volatile founders:")
for f in founder_rankings[-5:]:
    print(f"{f['founder']:<20} StdDev: {f['std']:>6.2f}% | Win: {f['win_rate']:.1f}% | N={f['n']}")

# Engagement analysis
print("\n" + "=" * 70)
print("ENGAGEMENT VS OUTCOME")
print("=" * 70)

# Get engagement percentiles for each founder
for founder, tweets in founder_stats.items():
    if len(tweets) < 50:
        continue

    likes = [t.get("likes", 0) for t in tweets]
    median_likes = statistics.median(likes)

    for t in tweets:
        t["high_engagement"] = t.get("likes", 0) > median_likes

# Compare high vs low engagement
high_eng = [e for e in events if e.get("high_engagement") == True]
low_eng = [e for e in events if e.get("high_engagement") == False]

if high_eng and low_eng:
    print("\nHigh engagement (above founder's median likes):")
    changes = [e["change_24h_pct"] for e in high_eng]
    print(f"  Avg: {statistics.mean(changes):+.2f}%")
    print(f"  Med: {statistics.median(changes):+.2f}%")
    print(f"  Win rate: {sum(1 for c in changes if c > 0) / len(changes) * 100:.1f}%")
    print(f"  N: {len(high_eng)}")

    print("\nLow engagement (below founder's median likes):")
    changes = [e["change_24h_pct"] for e in low_eng]
    print(f"  Avg: {statistics.mean(changes):+.2f}%")
    print(f"  Med: {statistics.median(changes):+.2f}%")
    print(f"  Win rate: {sum(1 for c in changes if c > 0) / len(changes) * 100:.1f}%")
    print(f"  N: {len(low_eng)}")

# Impressions analysis
print("\n" + "=" * 70)
print("IMPRESSIONS VS OUTCOME")
print("=" * 70)

# Bucket by impressions
imp_buckets = {
    "< 10K": [],
    "10K-100K": [],
    "100K-1M": [],
    "1M+": [],
}

for e in events:
    imp = e.get("impressions", 0)
    if imp < 10000:
        imp_buckets["< 10K"].append(e["change_24h_pct"])
    elif imp < 100000:
        imp_buckets["10K-100K"].append(e["change_24h_pct"])
    elif imp < 1000000:
        imp_buckets["100K-1M"].append(e["change_24h_pct"])
    else:
        imp_buckets["1M+"].append(e["change_24h_pct"])

print("\nBy impression count:")
for bucket in ["< 10K", "10K-100K", "100K-1M", "1M+"]:
    changes = imp_buckets[bucket]
    if len(changes) >= 20:
        avg = statistics.mean(changes)
        med = statistics.median(changes)
        wins = sum(1 for c in changes if c > 0) / len(changes) * 100
        print(f"  {bucket:>10}: avg {avg:+6.2f}%, med {med:+6.2f}%, win {wins:5.1f}%, n={len(changes)}")

# Best individual founders deep dive
print("\n" + "=" * 70)
print("TOP FOUNDER DEEP DIVE")
print("=" * 70)

# Take top 3 by win rate
top_founders = [f["founder"] for f in sorted(founder_rankings, key=lambda x: x["win_rate"], reverse=True)[:3]]

for founder in top_founders:
    tweets = founder_stats[founder]
    print(f"\n{founder} ({len(tweets)} tweets):")
    print("-" * 50)

    # Best tweets
    sorted_tweets = sorted(tweets, key=lambda x: x["change_24h_pct"], reverse=True)
    print("Top 3 tweets:")
    for t in sorted_tweets[:3]:
        text = t.get("text", "")[:60].replace("\n", " ")
        print(f"  {t['change_24h_pct']:+6.1f}% | \"{text}...\"")

    print("Worst 3 tweets:")
    for t in sorted_tweets[-3:]:
        text = t.get("text", "")[:60].replace("\n", " ")
        print(f"  {t['change_24h_pct']:+6.1f}% | \"{text}...\"")
