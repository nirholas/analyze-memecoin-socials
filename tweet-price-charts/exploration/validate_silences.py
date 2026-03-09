#!/usr/bin/env python3
"""
Validate "The Silences" View - Analyzing gaps in founder tweeting activity.

Goal: Determine if there are enough notable tweet gaps (silences) to make
a visualization interesting.

Hypothesis: Founders have periods of silence that could be visually interesting,
especially when juxtaposed with price movements during those gaps.
"""

import json
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

# Constants
STATIC_DIR = Path("/Users/satoshi/dev/tweet-price/web/public/static")
NOTABLE_GAP_DAYS = 7  # Minimum days to be "notable"
DRAMATIC_GAP_DAYS = 30  # What we consider a "dramatic" silence


def load_all_tweet_events():
    """Load tweet events from all asset directories."""
    all_events = {}

    # Load assets.json to get founder info
    with open(STATIC_DIR / "assets.json") as f:
        assets_data = json.load(f)

    asset_founders = {a["id"]: a["founder"] for a in assets_data["assets"]}

    for asset_dir in STATIC_DIR.iterdir():
        if not asset_dir.is_dir():
            continue

        tweet_file = asset_dir / "tweet_events.json"
        if not tweet_file.exists():
            continue

        with open(tweet_file) as f:
            data = json.load(f)

        asset_id = data.get("asset", asset_dir.name)
        founder = data.get("founder", asset_founders.get(asset_id, "unknown"))
        events = data.get("events", [])

        if events:
            all_events[asset_id] = {
                "founder": founder,
                "events": events
            }

    return all_events


def calculate_gaps(events):
    """
    Calculate gaps between consecutive tweets.
    Returns list of (gap_days, start_ts, end_ts, start_iso, end_iso).
    """
    if len(events) < 2:
        return []

    # Sort by timestamp (should already be sorted, but be safe)
    sorted_events = sorted(events, key=lambda e: e["timestamp"])

    gaps = []
    for i in range(1, len(sorted_events)):
        prev = sorted_events[i - 1]
        curr = sorted_events[i]

        gap_seconds = curr["timestamp"] - prev["timestamp"]
        gap_days = gap_seconds / (24 * 3600)

        gaps.append({
            "gap_days": gap_days,
            "start_ts": prev["timestamp"],
            "end_ts": curr["timestamp"],
            "start_iso": prev.get("timestamp_iso", ""),
            "end_iso": curr.get("timestamp_iso", ""),
            "tweet_before": prev.get("text", "")[:80],
            "tweet_after": curr.get("text", "")[:80],
        })

    return gaps


def analyze_founder(asset_id, founder, events):
    """Analyze a single founder's tweet patterns."""
    gaps = calculate_gaps(events)

    if not gaps:
        return None

    gap_days_list = [g["gap_days"] for g in gaps]

    # Calculate stats
    avg_gap = sum(gap_days_list) / len(gap_days_list)
    max_gap = max(gap_days_list)
    min_gap = min(gap_days_list)
    notable_gaps = [g for g in gaps if g["gap_days"] >= NOTABLE_GAP_DAYS]
    dramatic_gaps = [g for g in gaps if g["gap_days"] >= DRAMATIC_GAP_DAYS]

    # Tweet frequency (tweets per day)
    first_ts = min(e["timestamp"] for e in events)
    last_ts = max(e["timestamp"] for e in events)
    span_days = (last_ts - first_ts) / (24 * 3600)
    tweets_per_day = len(events) / span_days if span_days > 0 else 0

    return {
        "asset_id": asset_id,
        "founder": founder,
        "tweet_count": len(events),
        "span_days": round(span_days, 1),
        "tweets_per_day": round(tweets_per_day, 2),
        "avg_gap_days": round(avg_gap, 2),
        "max_gap_days": round(max_gap, 1),
        "min_gap_days": round(min_gap, 2),
        "notable_gap_count": len(notable_gaps),
        "dramatic_gap_count": len(dramatic_gaps),
        "notable_gaps": sorted(notable_gaps, key=lambda g: -g["gap_days"]),  # Longest first
    }


def main():
    print("=" * 80)
    print("SILENCES VIEW VALIDATION - Gap Analysis for Founder Tweets")
    print("=" * 80)
    print()

    # Load all data
    all_events = load_all_tweet_events()

    print(f"Loaded tweet events for {len(all_events)} assets\n")

    # Analyze each founder
    analyses = []
    for asset_id, data in all_events.items():
        analysis = analyze_founder(asset_id, data["founder"], data["events"])
        if analysis:
            analyses.append(analysis)

    # Sort by tweet count (largest datasets first)
    analyses.sort(key=lambda a: -a["tweet_count"])

    # Print per-founder analysis
    print("-" * 80)
    print("PER-FOUNDER ANALYSIS")
    print("-" * 80)
    print()
    print(f"{'Asset':<12} {'Founder':<16} {'Tweets':<8} {'Days':<8} {'Avg Gap':<10} {'Max Gap':<10} {'7+ Days':<10} {'30+ Days':<10}")
    print(f"{'-----':<12} {'-------':<16} {'------':<8} {'----':<8} {'-------':<10} {'-------':<10} {'-------':<10} {'--------':<10}")

    total_tweets = 0
    total_notable = 0
    total_dramatic = 0

    for a in analyses:
        print(f"{a['asset_id']:<12} {a['founder']:<16} {a['tweet_count']:<8} {a['span_days']:<8} {a['avg_gap_days']:<10} {a['max_gap_days']:<10} {a['notable_gap_count']:<10} {a['dramatic_gap_count']:<10}")
        total_tweets += a["tweet_count"]
        total_notable += a["notable_gap_count"]
        total_dramatic += a["dramatic_gap_count"]

    print()
    print(f"TOTALS: {total_tweets} tweets, {total_notable} notable gaps (7+ days), {total_dramatic} dramatic gaps (30+ days)")
    print()

    # Collect all notable gaps across all founders
    all_notable_gaps = []
    for a in analyses:
        for gap in a["notable_gaps"]:
            all_notable_gaps.append({
                "asset_id": a["asset_id"],
                "founder": a["founder"],
                **gap
            })

    # Sort by gap length
    all_notable_gaps.sort(key=lambda g: -g["gap_days"])

    # Print top 10 most dramatic silences
    print("-" * 80)
    print("TOP 10 MOST DRAMATIC SILENCES (across all founders)")
    print("-" * 80)
    print()

    for i, gap in enumerate(all_notable_gaps[:10], 1):
        print(f"{i}. {gap['founder']} ({gap['asset_id']}): {gap['gap_days']:.1f} days silent")
        print(f"   From: {gap['start_iso']}")
        print(f"   To:   {gap['end_iso']}")
        print(f"   Last tweet: \"{gap['tweet_before']}...\"")
        print(f"   Next tweet: \"{gap['tweet_after']}...\"")
        print()

    # Final verdict
    print("=" * 80)
    print("VERDICT: IS THE SILENCES VIEW WORTH BUILDING?")
    print("=" * 80)
    print()

    # Calculate key metrics
    total_gap_opportunities = sum(a["tweet_count"] - 1 for a in analyses)  # Number of gaps possible
    notable_ratio = total_notable / total_gap_opportunities if total_gap_opportunities > 0 else 0

    founders_with_silences = sum(1 for a in analyses if a["notable_gap_count"] > 0)
    founders_with_dramatic = sum(1 for a in analyses if a["dramatic_gap_count"] > 0)

    print(f"Total founders analyzed:          {len(analyses)}")
    print(f"Founders with 7+ day gaps:        {founders_with_silences}/{len(analyses)} ({100*founders_with_silences/len(analyses):.0f}%)")
    print(f"Founders with 30+ day gaps:       {founders_with_dramatic}/{len(analyses)} ({100*founders_with_dramatic/len(analyses):.0f}%)")
    print(f"Total notable gaps (7+ days):     {total_notable}")
    print(f"Total dramatic gaps (30+ days):   {total_dramatic}")
    print(f"% of gaps that are notable:       {100*notable_ratio:.1f}%")
    print()

    # Qualitative assessment
    if total_notable >= 30 and founders_with_silences >= len(analyses) * 0.5:
        verdict = "YES - BUILD IT"
        reason = f"Rich dataset with {total_notable} notable silences across {founders_with_silences} founders."
    elif total_notable >= 15 and total_dramatic >= 3:
        verdict = "MAYBE - CONSIDER CAREFULLY"
        reason = f"Moderate dataset with {total_notable} notable gaps and {total_dramatic} dramatic ones."
    elif total_notable < 10:
        verdict = "NO - NOT ENOUGH DATA"
        reason = f"Only {total_notable} notable gaps. Most founders tweet too frequently."
    else:
        verdict = "MARGINAL - NEEDS MORE DATA"
        reason = f"Only {total_notable} notable gaps across {founders_with_silences} founders."

    print(f"VERDICT: {verdict}")
    print(f"REASON:  {reason}")
    print()

    # Specific recommendations
    print("DETAILED OBSERVATIONS:")
    print()

    # Find the best candidates for silences visualization
    best_candidates = [a for a in analyses if a["notable_gap_count"] >= 2]
    if best_candidates:
        print("Best founders for silences visualization:")
        for a in sorted(best_candidates, key=lambda x: -x["notable_gap_count"])[:5]:
            print(f"  - {a['founder']} ({a['asset_id']}): {a['notable_gap_count']} notable gaps, longest {a['max_gap_days']:.0f} days")
        print()

    # Find the most active (boring for silences)
    hyperactive = [a for a in analyses if a["avg_gap_days"] < 1]
    if hyperactive:
        print("Most active founders (few/no silences to show):")
        for a in hyperactive:
            print(f"  - {a['founder']} ({a['asset_id']}): tweets every {a['avg_gap_days']:.1f} days on average")
        print()


if __name__ == "__main__":
    main()
