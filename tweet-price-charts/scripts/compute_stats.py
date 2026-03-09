"""
Compute pre-computed statistics for the frontend.
All heavy computation happens here, not in the browser.

Supports multi-asset statistics computation.

IMPORTANT: This script reads from JSON files (source of truth), NOT DuckDB.
The canonical DuckDB table is synced AFTER export, so reading from DuckDB
during the pipeline would give stale data.

Usage:
    python compute_stats.py                 # Compute stats for all assets
    python compute_stats.py --asset pump    # Compute stats for specific asset
"""
import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

import numpy as np
from scipy import stats as scipy_stats

from config import DATA_DIR, PUBLIC_DATA_DIR, ASSETS_FILE


class NumpyEncoder(json.JSONEncoder):
    """Custom JSON encoder for numpy types."""
    def default(self, obj):
        if isinstance(obj, (np.integer, np.int64)):
            return int(obj)
        if isinstance(obj, (np.floating, np.float64)):
            return float(obj)
        if isinstance(obj, np.bool_):
            return bool(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super().default(obj)


def load_assets() -> Dict[str, Dict]:
    """Load asset definitions from assets.json."""
    with open(ASSETS_FILE) as f:
        data = json.load(f)
    assets = data.get("assets", [])
    return {a["id"]: a for a in assets}


def get_asset(asset_id: str) -> Optional[Dict]:
    """Get a single asset definition."""
    assets = load_assets()
    return assets.get(asset_id)


def get_enabled_assets() -> List[Dict]:
    """Get all enabled assets."""
    assets = load_assets()
    return [a for a in assets.values() if a.get("enabled", True)]


def load_daily_prices(asset_id: str) -> Dict[int, float]:
    """
    Load daily close prices as timestamp -> price map from JSON file.

    Reads from prices_1d.json (source of truth) instead of DuckDB.
    """
    json_path = PUBLIC_DATA_DIR / asset_id / "prices_1d.json"
    if not json_path.exists():
        return {}

    with open(json_path) as f:
        data = json.load(f)

    result = {}
    for candle in data.get("candles", []):
        ts = candle["t"]  # Already Unix timestamp
        result[ts] = candle["c"]  # Close price

    return result


def load_tweet_events(asset_id: str) -> List[Dict]:
    """
    Load tweet events from JSON file.

    Reads from tweet_events.json (source of truth) instead of DuckDB.
    """
    json_path = PUBLIC_DATA_DIR / asset_id / "tweet_events.json"
    if not json_path.exists():
        return []

    with open(json_path) as f:
        data = json.load(f)

    return data.get("events", [])


def compute_distribution(values: List[float]) -> Dict[str, Any]:
    """Compute distribution statistics for a list of values."""
    if not values:
        return {}
    
    arr = np.array(values)
    return {
        "count": len(values),
        "mean": round(float(np.mean(arr)), 2),
        "median": round(float(np.median(arr)), 2),
        "std_dev": round(float(np.std(arr)), 2),
        "min": round(float(np.min(arr)), 2),
        "max": round(float(np.max(arr)), 2),
        "q1": round(float(np.percentile(arr, 25)), 2),
        "q3": round(float(np.percentile(arr, 75)), 2),
        "positive_count": int(np.sum(arr > 0)),
        "negative_count": int(np.sum(arr < 0)),
    }


def compute_daily_stats(
    events: List[Dict],
    daily_prices: Dict[int, float]
) -> Dict[str, Any]:
    """
    Compute tweet day vs no-tweet day statistics.
    """
    DAY = 86400
    
    # Get unique tweet days (epoch at midnight)
    tweet_days = set()
    for event in events:
        ts = event["timestamp"]
        day_start = (ts // DAY) * DAY
        tweet_days.add(day_start)
    
    # Calculate daily returns
    sorted_days = sorted(daily_prices.keys())
    tweet_day_returns = []
    no_tweet_day_returns = []
    
    for i in range(1, len(sorted_days)):
        day = sorted_days[i]
        prev_day = sorted_days[i - 1]
        
        price = daily_prices[day]
        prev_price = daily_prices[prev_day]
        
        if prev_price and prev_price > 0:
            ret = (price - prev_price) / prev_price * 100
            
            if day in tweet_days:
                tweet_day_returns.append(ret)
            else:
                no_tweet_day_returns.append(ret)
    
    # Statistical test
    t_stat, p_value = None, None
    if len(tweet_day_returns) >= 5 and len(no_tweet_day_returns) >= 5:
        t_stat, p_value = scipy_stats.ttest_ind(tweet_day_returns, no_tweet_day_returns)
    
    # Determine significance label
    significant = p_value < 0.05 if p_value else False
    if p_value:
        if p_value < 0.01:
            confidence_label = "strong"
        elif p_value < 0.05:
            confidence_label = "weak"
        else:
            confidence_label = "none"
    else:
        confidence_label = "insufficient_data"
    
    return {
        "tweet_day_count": len(tweet_day_returns),
        "tweet_day_avg_return": round(sum(tweet_day_returns) / len(tweet_day_returns), 2) if tweet_day_returns else 0,
        "tweet_day_win_rate": round(sum(1 for r in tweet_day_returns if r > 0) / len(tweet_day_returns) * 100, 1) if tweet_day_returns else 0,
        "tweet_day_distribution": compute_distribution(tweet_day_returns),
        "no_tweet_day_count": len(no_tweet_day_returns),
        "no_tweet_day_avg_return": round(sum(no_tweet_day_returns) / len(no_tweet_day_returns), 2) if no_tweet_day_returns else 0,
        "no_tweet_day_win_rate": round(sum(1 for r in no_tweet_day_returns if r > 0) / len(no_tweet_day_returns) * 100, 1) if no_tweet_day_returns else 0,
        "no_tweet_day_distribution": compute_distribution(no_tweet_day_returns),
        "t_statistic": round(t_stat, 3) if t_stat else None,
        "p_value": round(p_value, 4) if p_value else None,
        "significant": significant,
        "confidence_label": confidence_label,
    }


def compute_quiet_periods(
    events: List[Dict], 
    min_gap_days: int = 3
) -> List[Dict]:
    """
    Identify periods where the founder stopped tweeting.
    """
    if not events:
        return []
    
    DAY = 86400
    sorted_events = sorted(events, key=lambda x: x["timestamp"])
    
    quiet_periods = []
    prev_ts = None
    
    for event in sorted_events:
        ts = event["timestamp"]
        
        if prev_ts is not None:
            gap_days = (ts - prev_ts) / DAY
            if gap_days >= min_gap_days:
                quiet_periods.append({
                    "start_ts": prev_ts,
                    "end_ts": ts,
                    "gap_days": round(gap_days, 1),
                    "start_date": datetime.utcfromtimestamp(prev_ts).strftime("%Y-%m-%d"),
                    "end_date": datetime.utcfromtimestamp(ts).strftime("%Y-%m-%d"),
                })
        
        prev_ts = ts
    
    # Check if currently in quiet period
    if sorted_events:
        last_ts = sorted_events[-1]["timestamp"]
        now_ts = int(datetime.utcnow().timestamp())
        gap_days = (now_ts - last_ts) / DAY

        if gap_days >= min_gap_days:
            quiet_periods.append({
                "start_ts": last_ts,
                "end_ts": now_ts,
                "gap_days": round(gap_days, 1),
                "start_date": datetime.utcfromtimestamp(last_ts).strftime("%Y-%m-%d"),
                "end_date": "ongoing",
                "is_current": True,
            })

    return quiet_periods


def compute_quiet_period_impact(
    quiet_periods: List[Dict],
    daily_prices: Dict[int, float]
) -> List[Dict]:
    """
    Calculate price impact during each quiet period.
    """
    sorted_days = sorted(daily_prices.keys())
    
    results = []
    for qp in quiet_periods:
        start_ts = qp["start_ts"]
        end_ts = qp["end_ts"]
        
        # Find price at start (closest day)
        start_price = None
        for day in sorted_days:
            if day >= start_ts:
                start_price = daily_prices.get(day)
                break
        
        # Find price at end (closest day)
        end_price = None
        for day in reversed(sorted_days):
            if day <= end_ts:
                end_price = daily_prices.get(day)
                break
        
        # Calculate change
        change_pct = None
        if start_price and end_price:
            change_pct = round((end_price - start_price) / start_price * 100, 1)
        
        results.append({
            **qp,
            "price_start": start_price,
            "price_end": end_price,
            "change_pct": change_pct,
        })
    
    return results


def compute_correlation(
    events: List[Dict],
    daily_prices: Dict[int, float]
) -> Dict[str, Any]:
    """
    Compute correlation between 7-day tweet count and price.
    """
    DAY = 86400
    
    # Build 7-day rolling tweet count for each day
    tweet_timestamps = [e["timestamp"] for e in events]
    sorted_days = sorted(daily_prices.keys())
    
    rolling_counts = []
    prices = []
    
    for day in sorted_days:
        # Count tweets in prior 7 days
        week_start = day - (7 * DAY)
        count = sum(1 for t in tweet_timestamps if week_start <= t < day)
        rolling_counts.append(count)
        prices.append(daily_prices[day])
    
    # Pearson correlation
    if len(rolling_counts) >= 10:
        corr, p_val = scipy_stats.pearsonr(rolling_counts, prices)
        
        # Determine strength label
        abs_corr = abs(corr)
        if abs_corr >= 0.7:
            strength_label = "strong"
        elif abs_corr >= 0.4:
            strength_label = "moderate"
        elif abs_corr >= 0.2:
            strength_label = "weak"
        else:
            strength_label = "negligible"
        
        return {
            "correlation_7d": round(corr, 3),
            "p_value": round(p_val, 4),
            "significant": p_val < 0.05,
            "sample_size": len(rolling_counts),
            "strength_label": strength_label,
        }
    
    return {}


def compute_limitations(
    daily_stats: Dict,
    events: List[Dict],
    daily_prices: Dict[int, float]
) -> Dict[str, Any]:
    """Compute data limitations and warnings."""
    notes = []
    
    tweet_day_count = daily_stats.get("tweet_day_count", 0)
    p_value = daily_stats.get("p_value")
    
    sample_warning = tweet_day_count < 100
    borderline = p_value and 0.04 < p_value < 0.06
    short_history = len(daily_prices) < 365
    
    if sample_warning:
        notes.append(f"Small sample: only {tweet_day_count} tweet-days analyzed")
    
    if borderline:
        notes.append(f"Borderline significance: p={p_value} is very close to 0.05 threshold")
    
    if short_history:
        notes.append(f"Limited history: {len(daily_prices)} days may not capture full market cycles")
    
    win_rate = daily_stats.get("tweet_day_win_rate", 50)
    if 45 <= win_rate <= 55:
        notes.append(f"Win rate of {win_rate}% is near random (50%)")
    
    notes.append("Correlation does not imply causation")
    
    return {
        "sample_size_warning": sample_warning,
        "borderline_significance": borderline,
        "short_history": short_history,
        "notes": notes,
    }


def compute_stats_for_asset(asset_id: str) -> Dict[str, Any]:
    """
    Compute all statistics for a single asset.

    Reads from JSON files (source of truth), NOT DuckDB.
    """
    asset = get_asset(asset_id)
    if not asset:
        return {"error": f"Asset '{asset_id}' not found"}

    print(f"\nComputing stats for {asset['name']} ({asset_id})...")

    # Load data from JSON files (source of truth)
    events = load_tweet_events(asset_id)
    daily_prices = load_daily_prices(asset_id)

    print(f"    Tweet events: {len(events)}")
    print(f"    Daily prices: {len(daily_prices)}")

    if not events or not daily_prices:
        return {"error": "Missing data", "events": len(events), "prices": len(daily_prices)}
    
    # Compute all stats
    print("    Computing tweet day vs no-tweet day stats...")
    daily_stats = compute_daily_stats(events, daily_prices)
    
    print("    Computing quiet periods...")
    quiet_periods = compute_quiet_periods(events)
    quiet_with_impact = compute_quiet_period_impact(quiet_periods, daily_prices)
    
    print("    Computing correlations...")
    correlation = compute_correlation(events, daily_prices)
    
    # Find current status
    current_quiet = next((q for q in quiet_with_impact if q.get("is_current")), None)
    
    # Compute limitations
    limitations = compute_limitations(daily_stats, events, daily_prices)
    
    # Get date range
    sorted_events = sorted(events, key=lambda x: x["timestamp"])
    
    # Assemble final stats
    stats_output = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "asset": asset_id,
        "asset_name": asset["name"],
        "founder": asset["founder"],
        "summary": {
            "total_tweets": len(events),
            "tweets_with_price": sum(1 for e in events if e.get("price_at_tweet")),
            "date_range": {
                "start": sorted_events[0]["timestamp_iso"][:10] if sorted_events else None,
                "end": sorted_events[-1]["timestamp_iso"][:10] if sorted_events else None,
            },
            "total_days_analyzed": len(daily_prices),
        },
        "daily_comparison": daily_stats,
        "correlation": correlation,
        "current_status": {
            "days_since_last_tweet": int(current_quiet["gap_days"]) if current_quiet else 0,
            "price_change_during_silence": current_quiet["change_pct"] if current_quiet else None,
            "last_tweet_date": current_quiet["start_date"] if current_quiet else (
                sorted_events[-1]["timestamp_iso"][:10] if sorted_events else None
            ),
        },
        "limitations": limitations,
        "quiet_periods": quiet_with_impact,
    }

    return stats_output


def save_stats(stats: Dict[str, Any], asset_id: str):
    """Save stats to JSON files."""
    # Save to data directory
    output_path = DATA_DIR / asset_id / "stats.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, "w") as f:
        json.dump(stats, f, indent=2, cls=NumpyEncoder)
    print(f"    Saved to {output_path}")
    
    # Save to public directory
    public_path = PUBLIC_DATA_DIR / asset_id / "stats.json"
    public_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(public_path, "w") as f:
        json.dump(stats, f, indent=2, cls=NumpyEncoder)
    print(f"    Saved to {public_path}")


def print_stats_summary(stats: Dict[str, Any]):
    """Print a summary of computed stats."""
    if "error" in stats:
        print(f"    Error: {stats['error']}")
        return
    
    daily = stats.get("daily_comparison", {})
    corr = stats.get("correlation", {})
    current = stats.get("current_status", {})
    
    print(f"\n    Tweet Days: {daily.get('tweet_day_count', 0)} days")
    print(f"      Avg Return: {daily.get('tweet_day_avg_return', 0):+.2f}%")
    print(f"      Win Rate: {daily.get('tweet_day_win_rate', 0):.1f}%")
    
    print(f"\n    No-Tweet Days: {daily.get('no_tweet_day_count', 0)} days")
    print(f"      Avg Return: {daily.get('no_tweet_day_avg_return', 0):+.2f}%")
    print(f"      Win Rate: {daily.get('no_tweet_day_win_rate', 0):.1f}%")
    
    sig = "YES" if daily.get("significant") else "NO"
    p_val = daily.get("p_value", "N/A")
    print(f"\n    Statistical Significance: {sig} (p={p_val})")
    
    if corr:
        print(f"\n    Correlation (7d tweets vs price): {corr.get('correlation_7d', 'N/A')}")
    
    if current.get("days_since_last_tweet", 0) > 0:
        print(f"\n    Current Silence: {current['days_since_last_tweet']} days")
        if current.get("price_change_during_silence"):
            print(f"    Price Impact: {current['price_change_during_silence']:+.1f}%")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Compute statistics for assets"
    )
    parser.add_argument(
        "--asset", "-a",
        type=str,
        help="Specific asset ID (default: all enabled assets)"
    )
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("Computing Statistics")
    print("=" * 60)
    
    if args.asset:
        stats = compute_stats_for_asset(args.asset)
        if "error" not in stats:
            save_stats(stats, args.asset)
        print_stats_summary(stats)
    else:
        # Load assets from JSON (no DuckDB needed)
        assets = get_enabled_assets()

        for asset in assets:
            stats = compute_stats_for_asset(asset["id"])
            if "error" not in stats:
                save_stats(stats, asset["id"])
            print_stats_summary(stats)


if __name__ == "__main__":
    main()
