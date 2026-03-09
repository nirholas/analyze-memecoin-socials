"""
05: What happens when we combine the winning filters?
Stack the insights to find the highest-signal subset.
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

events = [e for e in all_events if e.get("change_24h_pct") is not None]

# Enrich events with derived fields
for e in events:
    dt = datetime.fromisoformat(e["timestamp_iso"].replace("Z", "+00:00"))
    e["hour"] = dt.hour
    e["day_of_week"] = dt.weekday()
    e["is_weekend"] = dt.weekday() >= 5
    e["is_short"] = len(e.get("text", "")) < 50
    e["mcap"] = e.get("market_cap_at_tweet", 0)
    e["is_small_cap"] = e["mcap"] < 500_000_000 if e["mcap"] else False

# Group by asset for gap calculation
asset_tweets = defaultdict(list)
for e in events:
    asset_tweets[e["asset"]].append(e)

for asset in asset_tweets:
    asset_tweets[asset].sort(key=lambda x: x["timestamp"])
    for i, e in enumerate(asset_tweets[asset]):
        if i == 0:
            e["gap_hours"] = 0
        else:
            e["gap_hours"] = (e["timestamp"] - asset_tweets[asset][i-1]["timestamp"]) / 3600
        e["is_sweet_spot_gap"] = 72 <= e["gap_hours"] <= 168  # 3-7 days

# Known good founders
good_founders = ["a1lon9", "theunipcs", "blknoiz06"]

for e in events:
    e["is_good_founder"] = e["founder"] in good_founders

print("=" * 70)
print("COMBINED FILTER ANALYSIS")
print("=" * 70)

# Define filters
filters = {
    "all": lambda e: True,
    "weekend": lambda e: e["is_weekend"],
    "small_cap (<$500M)": lambda e: e["is_small_cap"],
    "short tweet (<50)": lambda e: e["is_short"],
    "good founder": lambda e: e["is_good_founder"],
    "sweet spot gap (3-7d)": lambda e: e.get("is_sweet_spot_gap", False),
    "04:00 UTC hour": lambda e: e["hour"] == 4,
}

print("\nIndividual Filters:")
print("-" * 70)
print(f"{'Filter':<25} {'Win%':>7} {'Avg':>8} {'Med':>8} {'N':>7}")
print("-" * 70)

baseline_changes = [e["change_24h_pct"] for e in events]
baseline_win = sum(1 for c in baseline_changes if c > 0) / len(baseline_changes) * 100

for name, f in filters.items():
    subset = [e for e in events if f(e)]
    if len(subset) >= 20:
        changes = [e["change_24h_pct"] for e in subset]
        wins = sum(1 for c in changes if c > 0) / len(changes) * 100
        avg = statistics.mean(changes)
        med = statistics.median(changes)
        delta = wins - baseline_win
        delta_str = f"({delta:+.1f})" if name != "all" else ""
        print(f"{name:<25} {wins:>6.1f}% {avg:>+7.2f}% {med:>+7.2f}% {len(subset):>7} {delta_str}")

# Combined filters
print("\n" + "=" * 70)
print("STACKED FILTERS")
print("=" * 70)

combos = [
    ("weekend + small_cap", lambda e: e["is_weekend"] and e["is_small_cap"]),
    ("weekend + good_founder", lambda e: e["is_weekend"] and e["is_good_founder"]),
    ("small_cap + good_founder", lambda e: e["is_small_cap"] and e["is_good_founder"]),
    ("small_cap + short_tweet", lambda e: e["is_small_cap"] and e["is_short"]),
    ("weekend + small_cap + good_founder", lambda e: e["is_weekend"] and e["is_small_cap"] and e["is_good_founder"]),
    ("weekend + small_cap + short", lambda e: e["is_weekend"] and e["is_small_cap"] and e["is_short"]),
    ("ALL POSITIVE SIGNALS", lambda e: (
        e["is_weekend"] and
        e["is_small_cap"] and
        e["is_good_founder"]
    )),
]

print(f"\n{'Combo':<40} {'Win%':>7} {'Avg':>9} {'Med':>9} {'N':>5}")
print("-" * 70)

for name, f in combos:
    subset = [e for e in events if f(e)]
    if len(subset) >= 5:
        changes = [e["change_24h_pct"] for e in subset]
        wins = sum(1 for c in changes if c > 0) / len(changes) * 100
        avg = statistics.mean(changes)
        med = statistics.median(changes)
        print(f"{name:<40} {wins:>6.1f}% {avg:>+8.2f}% {med:>+8.2f}% {len(subset):>5}")
    else:
        print(f"{name:<40} (n={len(subset)}, too few)")

# The "anti-signal" - what predicts dumps?
print("\n" + "=" * 70)
print("NEGATIVE SIGNALS (what predicts dumps?)")
print("=" * 70)

bad_filters = [
    ("Monday", lambda e: e["day_of_week"] == 0),
    ("large_cap (>$1B)", lambda e: e["mcap"] > 1_000_000_000 if e["mcap"] else False),
    ("bad founders (mert, keoneHD)", lambda e: e["founder"] in ["mert", "keoneHD"]),
    ("20:00-22:00 UTC", lambda e: 20 <= e["hour"] <= 22),
    ("Monday + large_cap", lambda e: e["day_of_week"] == 0 and e["mcap"] > 1_000_000_000),
]

print(f"\n{'Filter':<35} {'Win%':>7} {'Avg':>9} {'Med':>9} {'N':>6}")
print("-" * 70)

for name, f in bad_filters:
    subset = [e for e in events if f(e)]
    if len(subset) >= 10:
        changes = [e["change_24h_pct"] for e in subset]
        wins = sum(1 for c in changes if c > 0) / len(changes) * 100
        avg = statistics.mean(changes)
        med = statistics.median(changes)
        print(f"{name:<35} {wins:>6.1f}% {avg:>+8.2f}% {med:>+8.2f}% {len(subset):>6}")

# Summary
print("\n" + "=" * 70)
print("SUMMARY: THE TRADING EDGE")
print("=" * 70)

print("""
BASELINE: All tweets
  - 51.5% win rate, +2.48% avg

BEST FILTER (single): Weekend
  - 58.9% win rate (+7.4% vs baseline)

BEST COMBO: Weekend + Small Cap + Good Founder
  - Check output above for combined stats

WORST FILTER: Monday + Large Cap
  - Check output above

KEY INSIGHT:
The edge isn't in one magic signal. It's in AVOIDING the bad setups
(Monday, large cap, bad founders, evening UTC) and FAVORING good setups
(weekend, small cap, proven founders, short tweets).
""")
