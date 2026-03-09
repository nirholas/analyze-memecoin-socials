#!/usr/bin/env python3
"""
Validate: Is time-of-day tweeting pattern an interesting view to build?

Hypothesis: Founders have distinct behavioral patterns in WHEN they tweet.
If patterns are uniform across founders, this view isn't worth building.
"""

import json
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import math

STATIC_DIR = Path("/Users/satoshi/dev/tweet-price/web/public/static")


def load_all_tweets():
    """Load all tweet events from all assets."""
    all_tweets = []

    for tweet_file in STATIC_DIR.glob("*/tweet_events.json"):
        with open(tweet_file) as f:
            data = json.load(f)
            for event in data.get("events", []):
                all_tweets.append({
                    "founder": event.get("founder"),
                    "timestamp_iso": event.get("timestamp_iso"),
                    "asset": event.get("asset_id"),
                })

    return all_tweets


def extract_hour(timestamp_iso: str) -> int:
    """Extract UTC hour (0-23) from ISO timestamp."""
    # Format: "2025-07-14T13:00:51Z"
    dt = datetime.fromisoformat(timestamp_iso.replace("Z", "+00:00"))
    return dt.hour


def calculate_entropy(hour_counts: dict) -> float:
    """
    Calculate normalized entropy (0-1) of hour distribution.
    0 = all tweets in one hour (concentrated)
    1 = perfectly uniform across all 24 hours
    """
    total = sum(hour_counts.values())
    if total == 0:
        return 0

    entropy = 0
    for count in hour_counts.values():
        if count > 0:
            p = count / total
            entropy -= p * math.log2(p)

    # Normalize by max entropy (log2(24) for uniform distribution)
    max_entropy = math.log2(24)
    return entropy / max_entropy


def analyze_founder(founder: str, tweets: list) -> dict:
    """Analyze time-of-day pattern for a single founder."""
    hour_counts = defaultdict(int)

    for tweet in tweets:
        hour = extract_hour(tweet["timestamp_iso"])
        hour_counts[hour] += 1

    total = len(tweets)

    # Find peak hours (top 3)
    sorted_hours = sorted(hour_counts.items(), key=lambda x: -x[1])
    peak_hours = sorted_hours[:3]

    # Calculate concentration: what % of tweets in top 3 hours?
    top3_count = sum(c for _, c in peak_hours)
    concentration = top3_count / total if total > 0 else 0

    # Calculate entropy
    entropy = calculate_entropy(hour_counts)

    # Determine pattern strength
    # If top 3 hours have >50% of tweets AND entropy < 0.85, pattern is distinct
    # If entropy > 0.9, pattern is uniform
    if concentration > 0.50 and entropy < 0.80:
        pattern = "DISTINCT"
    elif concentration > 0.35 or entropy < 0.90:
        pattern = "MODERATE"
    else:
        pattern = "UNIFORM"

    return {
        "founder": founder,
        "total_tweets": total,
        "peak_hours": [(h, c, f"{c/total*100:.1f}%") for h, c in peak_hours],
        "top3_concentration": concentration,
        "entropy": entropy,
        "pattern": pattern,
        "hour_counts": dict(hour_counts),
    }


def main():
    print("=" * 70)
    print("TIME-OF-DAY TWEETING PATTERNS - VALIDATION ANALYSIS")
    print("=" * 70)
    print()

    # Load all tweets
    all_tweets = load_all_tweets()
    print(f"Loaded {len(all_tweets)} total tweets")

    # Group by founder
    by_founder = defaultdict(list)
    for tweet in all_tweets:
        founder = tweet["founder"]
        if founder:
            by_founder[founder].append(tweet)

    print(f"Found {len(by_founder)} founders")
    print()

    # Analyze each founder
    results = []
    for founder, tweets in sorted(by_founder.items(), key=lambda x: -len(x[1])):
        if len(tweets) >= 10:  # Only analyze founders with enough data
            result = analyze_founder(founder, tweets)
            results.append(result)

    # Print results
    print("-" * 70)
    print(f"{'Founder':<20} {'Tweets':>7} {'Pattern':<10} {'Top 3 Hours (UTC)':<30} {'Conc.':<7} {'Entropy'}")
    print("-" * 70)

    pattern_counts = {"DISTINCT": 0, "MODERATE": 0, "UNIFORM": 0}

    for r in results:
        peak_str = ", ".join([f"{h}:00 ({pct})" for h, c, pct in r["peak_hours"]])
        print(f"{r['founder']:<20} {r['total_tweets']:>7} {r['pattern']:<10} {peak_str:<30} {r['top3_concentration']:.2f}   {r['entropy']:.2f}")
        pattern_counts[r["pattern"]] += 1

    print("-" * 70)
    print()

    # Summary
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print()
    print(f"Pattern distribution:")
    print(f"  DISTINCT: {pattern_counts['DISTINCT']} founders")
    print(f"  MODERATE: {pattern_counts['MODERATE']} founders")
    print(f"  UNIFORM:  {pattern_counts['UNIFORM']} founders")
    print()

    # Visual: Show hour distribution for top founders
    print("=" * 70)
    print("HOUR DISTRIBUTION HEATMAP (Top 5 founders by tweet count)")
    print("=" * 70)
    print()

    print(f"{'Founder':<15} | Hour: ", end="")
    for h in range(24):
        print(f"{h:>2}", end=" ")
    print()
    print("-" * 15 + "-+-" + "-" * 72)

    for r in results[:5]:
        print(f"{r['founder']:<15} |      ", end="")
        max_count = max(r["hour_counts"].values()) if r["hour_counts"] else 1
        for h in range(24):
            count = r["hour_counts"].get(h, 0)
            # Normalize to 0-3 scale for visual
            level = int((count / max_count) * 3) if max_count > 0 else 0
            chars = [" .", "..", "##", "@@"]
            print(f"{chars[level]}", end=" ")
        print()

    print()

    # Final verdict
    print("=" * 70)
    print("VERDICT")
    print("=" * 70)
    print()

    distinct_pct = pattern_counts["DISTINCT"] / len(results) if results else 0
    moderate_pct = pattern_counts["MODERATE"] / len(results) if results else 0

    if distinct_pct >= 0.4:
        verdict = "STRONG YES"
        reason = f"{pattern_counts['DISTINCT']}/{len(results)} founders have distinct patterns"
    elif distinct_pct + moderate_pct >= 0.6:
        verdict = "YES"
        reason = f"Most founders show some pattern ({pattern_counts['DISTINCT']} distinct + {pattern_counts['MODERATE']} moderate)"
    elif distinct_pct + moderate_pct >= 0.3:
        verdict = "MAYBE"
        reason = "Some patterns exist but not universally interesting"
    else:
        verdict = "NO"
        reason = "Most founders tweet uniformly throughout the day"

    print(f"Is this view worth building? {verdict}")
    print(f"Reason: {reason}")
    print()

    # Interesting observations
    print("INTERESTING OBSERVATIONS:")
    for r in results:
        if r["pattern"] == "DISTINCT":
            peak_hour = r["peak_hours"][0][0]
            peak_pct = r["peak_hours"][0][2]
            print(f"  - {r['founder']}: Peak at {peak_hour}:00 UTC ({peak_pct} of tweets)")

    print()
    print("=" * 70)


if __name__ == "__main__":
    main()
