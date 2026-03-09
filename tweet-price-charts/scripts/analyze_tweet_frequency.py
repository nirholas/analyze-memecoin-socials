#!/usr/bin/env python3
"""
Analyze tweet frequency baselines for all assets.
"""

import json
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict

# Load all tweet data
base_path = Path('/Users/satoshi/tweet-price/web/public/static')
assets = ['pump', 'hype', 'aster', 'believe', 'jup', 'monad', 'useless']

print("=" * 80)
print("TWEET FREQUENCY BASELINE ANALYSIS")
print("=" * 80)
print()

all_stats = []

for asset in assets:
    json_path = base_path / asset / 'tweet_events.json'
    if not json_path.exists():
        print(f"❌ Missing: {json_path}")
        continue

    with open(json_path, 'r') as f:
        data = json.load(f)
        events = data['events']

    if len(events) == 0:
        print(f"⚠️  No tweets for {asset}")
        continue

    # Extract timestamps and sort
    timestamps = sorted([e['timestamp'] for e in events])
    founder = events[0]['founder'] if events else 'unknown'

    # Date range
    start_date = datetime.fromtimestamp(timestamps[0])
    end_date = datetime.fromtimestamp(timestamps[-1])
    duration_days = (end_date - start_date).total_seconds() / 86400

    # Basic frequency stats
    total_tweets = len(timestamps)
    tweets_per_day = total_tweets / duration_days if duration_days > 0 else 0
    tweets_per_week = tweets_per_day * 7

    # Calculate gaps between consecutive tweets (in hours)
    gaps_hours = []
    for i in range(1, len(timestamps)):
        gap = (timestamps[i] - timestamps[i-1]) / 3600  # Convert to hours
        gaps_hours.append(gap)

    # Gap statistics
    if gaps_hours:
        mean_gap = np.mean(gaps_hours)
        median_gap = np.median(gaps_hours)
        std_gap = np.std(gaps_hours)
        max_gap = max(gaps_hours)

        # Calculate sigma thresholds
        gap_1sigma = mean_gap + std_gap
        gap_2sigma = mean_gap + 2 * std_gap
        gap_3sigma = mean_gap + 3 * std_gap
    else:
        mean_gap = median_gap = std_gap = max_gap = 0
        gap_1sigma = gap_2sigma = gap_3sigma = 0

    # Time-of-day analysis (UTC)
    hours_utc = [datetime.fromtimestamp(ts).hour for ts in timestamps]
    hour_dist = pd.Series(hours_utc).value_counts().sort_index()
    most_active_hours = hour_dist.nlargest(3).index.tolist()

    # Weekly frequency trend analysis
    df = pd.DataFrame({'timestamp': timestamps})
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s')
    df['week'] = df['datetime'].dt.to_period('W')
    weekly_counts = df.groupby('week').size()

    # Detect quiet/burst periods (>2σ from mean)
    weekly_mean = weekly_counts.mean()
    weekly_std = weekly_counts.std()
    quiet_weeks = weekly_counts[weekly_counts < weekly_mean - 2 * weekly_std]
    burst_weeks = weekly_counts[weekly_counts > weekly_mean + 2 * weekly_std]

    # Store stats
    stats = {
        'asset': asset,
        'founder': founder,
        'total_tweets': total_tweets,
        'date_range': f"{start_date.date()} to {end_date.date()}",
        'duration_days': round(duration_days, 1),
        'tweets_per_day': round(tweets_per_day, 2),
        'tweets_per_week': round(tweets_per_week, 2),
        'mean_gap_hours': round(mean_gap, 2),
        'median_gap_hours': round(median_gap, 2),
        'std_gap_hours': round(std_gap, 2),
        'max_gap_hours': round(max_gap, 2),
        'gap_1sigma': round(gap_1sigma, 2),
        'gap_2sigma': round(gap_2sigma, 2),
        'gap_3sigma': round(gap_3sigma, 2),
        'most_active_hours_utc': most_active_hours,
        'weekly_mean': round(weekly_mean, 2),
        'weekly_std': round(weekly_std, 2),
        'quiet_weeks_count': len(quiet_weeks),
        'burst_weeks_count': len(burst_weeks)
    }
    all_stats.append(stats)

    # Print detailed report for this asset
    print(f"\n{'─' * 80}")
    print(f"ASSET: {asset.upper()} (Founder: @{founder})")
    print(f"{'─' * 80}")
    print()
    print("📊 BASIC FREQUENCY STATS")
    print(f"  Total tweets:       {total_tweets:,}")
    print(f"  Date range:         {start_date.date()} to {end_date.date()} ({duration_days:.1f} days)")
    print(f"  Tweets per day:     {tweets_per_day:.2f}")
    print(f"  Tweets per week:    {tweets_per_week:.2f}")
    print()
    print("⏱️  GAP DISTRIBUTION (hours between consecutive tweets)")
    print(f"  Mean gap:           {mean_gap:.2f} hours ({mean_gap/24:.2f} days)")
    print(f"  Median gap:         {median_gap:.2f} hours ({median_gap/24:.2f} days)")
    print(f"  Std deviation:      {std_gap:.2f} hours")
    print(f"  Longest gap:        {max_gap:.2f} hours ({max_gap/24:.2f} days)")
    print()
    print("  UNUSUAL GAP THRESHOLDS:")
    print(f"    1σ (84th %ile):   >{gap_1sigma:.2f} hours ({gap_1sigma/24:.2f} days)")
    print(f"    2σ (97.7th %ile): >{gap_2sigma:.2f} hours ({gap_2sigma/24:.2f} days)")
    print(f"    3σ (99.9th %ile): >{gap_3sigma:.2f} hours ({gap_3sigma/24:.2f} days)")
    print()
    print("🕐 TIME-OF-DAY PATTERNS (UTC)")
    print(f"  Most active hours:  {', '.join(f'{h:02d}:00' for h in most_active_hours)}")
    print(f"  Hour distribution:")
    for hour in range(24):
        count = hour_dist.get(hour, 0)
        if count > 0:
            bar = '█' * int(count / hour_dist.max() * 40)
            print(f"    {hour:02d}:00  {bar} {count}")
    print()
    print("📈 FREQUENCY OVER TIME")
    print(f"  Weekly mean:        {weekly_mean:.2f} tweets/week")
    print(f"  Weekly std:         {weekly_std:.2f}")
    print(f"  Quiet weeks (<-2σ): {len(quiet_weeks)}")
    print(f"  Burst weeks (>+2σ): {len(burst_weeks)}")

    if len(burst_weeks) > 0:
        print(f"\n  BURST PERIODS:")
        for week, count in burst_weeks.items():
            print(f"    {week}: {count} tweets ({count/weekly_mean:.1f}x average)")

    if len(quiet_weeks) > 0:
        print(f"\n  QUIET PERIODS:")
        for week, count in quiet_weeks.items():
            print(f"    {week}: {count} tweets ({count/weekly_mean:.1f}x average)")

# Print summary table
print("\n" + "=" * 80)
print("SUMMARY TABLE: BASELINE FREQUENCY STATS")
print("=" * 80)
print()

# Create summary DataFrame
df_summary = pd.DataFrame(all_stats)
df_summary = df_summary.sort_values('tweets_per_week', ascending=False)

print(df_summary[['asset', 'founder', 'total_tweets', 'tweets_per_week',
                   'mean_gap_hours', 'gap_2sigma', 'max_gap_hours']].to_string(index=False))

print("\n" + "=" * 80)
print("KEY INSIGHTS: WHAT CONSTITUTES 'UNUSUAL' ACTIVITY")
print("=" * 80)
print()

for stats in all_stats:
    print(f"\n{stats['asset'].upper()} (@{stats['founder']}):")
    print(f"  Baseline: {stats['tweets_per_week']:.1f} tweets/week")
    print(f"  Unusual silence: Gap >{stats['gap_2sigma']:.1f} hours ({stats['gap_2sigma']/24:.1f} days)")
    print(f"  Longest observed silence: {stats['max_gap_hours']:.1f} hours ({stats['max_gap_hours']/24:.1f} days)")
    print(f"  Unusual activity: Week with >{stats['weekly_mean'] + 2*stats['weekly_std']:.1f} tweets")

print("\n" + "=" * 80)
print("ANALYSIS COMPLETE")
print("=" * 80)
