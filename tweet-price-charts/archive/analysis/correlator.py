"""
Correlation analysis between Alon's tweets and $PUMP price.
Identifies patterns, quiet periods, and potential signals.
"""
from typing import List, Dict, Tuple, Optional
import json
import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime, timedelta
from pathlib import Path

# Project paths
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "data"
TWEETS_FILE = DATA_DIR / "tweets.json"
PRICES_FILE = DATA_DIR / "prices.json"


def load_tweets() -> pd.DataFrame:
    """Load tweets into a DataFrame."""
    with open(TWEETS_FILE) as f:
        data = json.load(f)
    
    df = pd.DataFrame(data["tweets"])
    df["created_at"] = pd.to_datetime(df["created_at"]).dt.tz_localize(None)
    df["date"] = df["created_at"].dt.date
    df = df.sort_values("created_at")
    return df


def load_prices() -> pd.DataFrame:
    """Load price data into a DataFrame."""
    with open(PRICES_FILE) as f:
        data = json.load(f)
    
    df = pd.DataFrame(data["prices"])
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["date"] = df["timestamp"].dt.date
    df = df.sort_values("timestamp")
    return df


def calculate_tweet_frequency(tweets_df: pd.DataFrame, window_days: int = 7) -> pd.DataFrame:
    """
    Calculate rolling tweet frequency.
    Returns daily tweet counts and rolling averages.
    """
    # Count tweets per day
    daily_tweets = tweets_df.groupby("date").size().reset_index(name="tweet_count")
    daily_tweets["date"] = pd.to_datetime(daily_tweets["date"])
    
    # Create a complete date range
    date_range = pd.date_range(
        start=daily_tweets["date"].min(),
        end=daily_tweets["date"].max(),
        freq="D"
    )
    
    # Reindex to include all dates (fill missing with 0)
    daily_tweets = daily_tweets.set_index("date").reindex(date_range, fill_value=0)
    daily_tweets = daily_tweets.reset_index().rename(columns={"index": "date"})
    
    # Calculate rolling averages
    daily_tweets[f"tweets_{window_days}d_avg"] = (
        daily_tweets["tweet_count"].rolling(window=window_days, min_periods=1).mean()
    )
    daily_tweets[f"tweets_{window_days}d_sum"] = (
        daily_tweets["tweet_count"].rolling(window=window_days, min_periods=1).sum()
    )
    
    return daily_tweets


def merge_tweet_price_data(tweets_df: pd.DataFrame, prices_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge tweet frequency data with price data by date.
    """
    # Get daily tweet counts
    tweet_freq = calculate_tweet_frequency(tweets_df)
    tweet_freq["date"] = pd.to_datetime(tweet_freq["date"]).dt.date
    
    # Prepare price data
    prices = prices_df.copy()
    prices["date"] = pd.to_datetime(prices["date"])
    prices["date"] = prices["date"].dt.date
    
    # Merge on date
    merged = prices.merge(tweet_freq, on="date", how="left")
    
    # Fill NaN tweet counts with 0 (days before tweeting started)
    merged["tweet_count"] = merged["tweet_count"].fillna(0)
    merged["tweets_7d_avg"] = merged["tweets_7d_avg"].fillna(0)
    merged["tweets_7d_sum"] = merged["tweets_7d_sum"].fillna(0)
    
    # Calculate price changes
    merged["price_change_1d"] = merged["close"].pct_change() * 100
    merged["price_change_7d"] = merged["close"].pct_change(periods=7) * 100
    
    return merged


def identify_quiet_periods(tweets_df: pd.DataFrame, min_gap_days: int = 3) -> List[Dict]:
    """
    Identify periods where Alon stopped tweeting.
    Returns list of quiet periods with start, end, and duration.
    """
    tweets_df = tweets_df.sort_values("created_at")
    
    quiet_periods = []
    prev_date = None
    
    for _, row in tweets_df.iterrows():
        curr_date = row["created_at"]
        
        if prev_date is not None:
            gap = (curr_date - prev_date).days
            if gap >= min_gap_days:
                quiet_periods.append({
                    "start": prev_date.isoformat(),
                    "end": curr_date.isoformat(),
                    "gap_days": gap,
                    "last_tweet_before": prev_date.strftime("%Y-%m-%d"),
                    "first_tweet_after": curr_date.strftime("%Y-%m-%d"),
                })
        
        prev_date = curr_date
    
    # Check if currently in a quiet period (no tweets recently)
    last_tweet = tweets_df["created_at"].max()
    days_since_last = (datetime.now() - last_tweet.to_pydatetime().replace(tzinfo=None)).days
    
    if days_since_last >= min_gap_days:
        quiet_periods.append({
            "start": last_tweet.isoformat(),
            "end": "ongoing",
            "gap_days": days_since_last,
            "last_tweet_before": last_tweet.strftime("%Y-%m-%d"),
            "first_tweet_after": None,
            "is_current": True,
        })
    
    return quiet_periods


def analyze_quiet_period_impact(
    merged_df: pd.DataFrame,
    quiet_periods: List[Dict],
    prices_df: pd.DataFrame
) -> List[Dict]:
    """
    Analyze what happened to price during and after quiet periods.
    """
    prices_df = prices_df.copy()
    prices_df["timestamp"] = pd.to_datetime(prices_df["timestamp"]).dt.tz_localize(None)
    
    results = []
    
    for qp in quiet_periods:
        start = pd.to_datetime(qp["start"]).tz_localize(None)
        
        if qp.get("is_current"):
            end = datetime.now()
        else:
            end = pd.to_datetime(qp["end"]).tz_localize(None)
        
        # Get price at start of quiet period
        price_before = prices_df[prices_df["timestamp"] <= start]["close"].iloc[-1] if len(prices_df[prices_df["timestamp"] <= start]) > 0 else None
        
        # Get price at end of quiet period
        price_after = prices_df[prices_df["timestamp"] >= end]["close"].iloc[0] if len(prices_df[prices_df["timestamp"] >= end]) > 0 else None
        
        # Get price during quiet period (min, max, end)
        during_period = prices_df[
            (prices_df["timestamp"] >= start) & 
            (prices_df["timestamp"] <= end)
        ]
        
        if len(during_period) > 0:
            price_min = during_period["close"].min()
            price_max = during_period["close"].max()
            price_at_end = during_period["close"].iloc[-1]
        else:
            price_min = price_max = price_at_end = None
        
        result = {
            **qp,
            "price_before": price_before,
            "price_after": price_after,
            "price_min_during": price_min,
            "price_max_during": price_max,
            "price_at_end": price_at_end,
        }
        
        # Calculate percentage changes
        if price_before and price_at_end:
            result["price_change_during"] = ((price_at_end - price_before) / price_before) * 100
        
        if price_before and price_after:
            result["price_change_total"] = ((price_after - price_before) / price_before) * 100
        
        results.append(result)
    
    return results


def calculate_correlation(merged_df: pd.DataFrame) -> Dict:
    """
    Calculate various correlation metrics between tweet activity and price.
    """
    df = merged_df.dropna()
    
    if len(df) < 10:
        return {"error": "Not enough overlapping data points"}
    
    results = {}
    
    # 1. Pearson correlation: tweet count vs price change
    if "tweet_count" in df.columns and "price_change_1d" in df.columns:
        corr, p_value = stats.pearsonr(df["tweet_count"], df["price_change_1d"])
        results["tweet_count_vs_price_1d"] = {
            "correlation": corr,
            "p_value": p_value,
            "significant": p_value < 0.05,
        }
    
    # 2. Rolling tweet average vs price
    if "tweets_7d_avg" in df.columns and "close" in df.columns:
        corr, p_value = stats.pearsonr(df["tweets_7d_avg"], df["close"])
        results["tweets_7d_avg_vs_price"] = {
            "correlation": corr,
            "p_value": p_value,
            "significant": p_value < 0.05,
        }
    
    # 3. Tweet activity vs next day's price change (predictive)
    df_shifted = df.copy()
    df_shifted["next_day_change"] = df_shifted["price_change_1d"].shift(-1)
    df_shifted = df_shifted.dropna()
    
    if len(df_shifted) > 10:
        corr, p_value = stats.pearsonr(df_shifted["tweet_count"], df_shifted["next_day_change"])
        results["tweet_count_vs_next_day_price"] = {
            "correlation": corr,
            "p_value": p_value,
            "significant": p_value < 0.05,
        }
    
    # 4. High activity vs low activity comparison
    high_activity = df[df["tweet_count"] >= 2]
    low_activity = df[df["tweet_count"] == 0]
    
    if len(high_activity) > 5 and len(low_activity) > 5:
        results["high_vs_low_activity"] = {
            "high_activity_avg_return": high_activity["price_change_1d"].mean(),
            "low_activity_avg_return": low_activity["price_change_1d"].mean(),
            "high_activity_days": len(high_activity),
            "low_activity_days": len(low_activity),
        }
    
    return results


def analyze_tweet_impact(merged_df: pd.DataFrame, tweets_df: pd.DataFrame) -> Dict:
    """
    Analyze the immediate price impact of individual tweets.
    """
    # Find days with tweets and their price performance
    tweet_days = merged_df[merged_df["tweet_count"] > 0].copy()
    no_tweet_days = merged_df[merged_df["tweet_count"] == 0].copy()
    
    results = {
        "tweet_day_stats": {
            "count": len(tweet_days),
            "avg_return": tweet_days["price_change_1d"].mean() if len(tweet_days) > 0 else 0,
            "median_return": tweet_days["price_change_1d"].median() if len(tweet_days) > 0 else 0,
            "positive_days": (tweet_days["price_change_1d"] > 0).sum() if len(tweet_days) > 0 else 0,
            "negative_days": (tweet_days["price_change_1d"] < 0).sum() if len(tweet_days) > 0 else 0,
        },
        "no_tweet_day_stats": {
            "count": len(no_tweet_days),
            "avg_return": no_tweet_days["price_change_1d"].mean() if len(no_tweet_days) > 0 else 0,
            "median_return": no_tweet_days["price_change_1d"].median() if len(no_tweet_days) > 0 else 0,
            "positive_days": (no_tweet_days["price_change_1d"] > 0).sum() if len(no_tweet_days) > 0 else 0,
            "negative_days": (no_tweet_days["price_change_1d"] < 0).sum() if len(no_tweet_days) > 0 else 0,
        },
    }
    
    # Statistical test: are returns different on tweet days vs non-tweet days?
    if len(tweet_days) > 5 and len(no_tweet_days) > 5:
        t_stat, p_value = stats.ttest_ind(
            tweet_days["price_change_1d"].dropna(),
            no_tweet_days["price_change_1d"].dropna()
        )
        results["statistical_test"] = {
            "t_statistic": t_stat,
            "p_value": p_value,
            "significant_difference": p_value < 0.05,
        }
    
    return results


def generate_report() -> Dict:
    """
    Generate a comprehensive correlation report.
    """
    print("Loading data...")
    tweets_df = load_tweets()
    prices_df = load_prices()
    
    print(f"Tweets: {len(tweets_df)} from {tweets_df['created_at'].min()} to {tweets_df['created_at'].max()}")
    print(f"Prices: {len(prices_df)} from {prices_df['timestamp'].min()} to {prices_df['timestamp'].max()}")
    
    print("\nMerging data...")
    merged = merge_tweet_price_data(tweets_df, prices_df)
    print(f"Merged dataset: {len(merged)} days")
    
    print("\nIdentifying quiet periods...")
    quiet_periods = identify_quiet_periods(tweets_df)
    print(f"Found {len(quiet_periods)} quiet periods (3+ days)")
    
    print("\nAnalyzing quiet period impact...")
    quiet_impact = analyze_quiet_period_impact(merged, quiet_periods, prices_df)
    
    print("\nCalculating correlations...")
    correlations = calculate_correlation(merged)
    
    print("\nAnalyzing tweet impact...")
    tweet_impact = analyze_tweet_impact(merged, tweets_df)
    
    report = {
        "generated_at": datetime.now().isoformat(),
        "data_summary": {
            "total_tweets": len(tweets_df),
            "tweet_date_range": {
                "start": tweets_df["created_at"].min().isoformat(),
                "end": tweets_df["created_at"].max().isoformat(),
            },
            "total_price_points": len(prices_df),
            "price_date_range": {
                "start": prices_df["timestamp"].min().isoformat(),
                "end": prices_df["timestamp"].max().isoformat(),
            },
            "overlapping_days": len(merged),
        },
        "correlations": correlations,
        "tweet_impact": tweet_impact,
        "quiet_periods": quiet_impact,
        "merged_data": merged.to_dict(orient="records"),
    }
    
    return report


def print_summary(report: Dict):
    """Print a human-readable summary of the analysis."""
    print("\n" + "=" * 60)
    print("TWEET-PRICE CORRELATION ANALYSIS")
    print("=" * 60)
    
    # Data summary
    ds = report["data_summary"]
    print(f"\n📊 DATA SUMMARY")
    print(f"   Tweets analyzed: {ds['total_tweets']}")
    print(f"   Price points: {ds['total_price_points']}")
    print(f"   Overlapping days: {ds['overlapping_days']}")
    
    # Correlations
    print(f"\n📈 CORRELATIONS")
    corr = report["correlations"]
    
    if "tweet_count_vs_price_1d" in corr:
        c = corr["tweet_count_vs_price_1d"]
        sig = "✓ SIGNIFICANT" if c["significant"] else "✗ not significant"
        print(f"   Tweet count vs same-day price change: {c['correlation']:.3f} ({sig})")
    
    if "tweets_7d_avg_vs_price" in corr:
        c = corr["tweets_7d_avg_vs_price"]
        sig = "✓ SIGNIFICANT" if c["significant"] else "✗ not significant"
        print(f"   7-day tweet avg vs price level: {c['correlation']:.3f} ({sig})")
    
    if "tweet_count_vs_next_day_price" in corr:
        c = corr["tweet_count_vs_next_day_price"]
        sig = "✓ SIGNIFICANT" if c["significant"] else "✗ not significant"
        print(f"   Tweet count vs NEXT day price change: {c['correlation']:.3f} ({sig})")
    
    # Tweet impact
    print(f"\n🐦 TWEET DAY VS NO-TWEET DAY")
    ti = report["tweet_impact"]
    td = ti["tweet_day_stats"]
    ntd = ti["no_tweet_day_stats"]
    
    print(f"   Days with tweets: {td['count']}")
    print(f"      Avg return: {td['avg_return']:.2f}%")
    print(f"      Win rate: {td['positive_days']}/{td['count']} ({100*td['positive_days']/max(1,td['count']):.0f}%)")
    
    print(f"   Days without tweets: {ntd['count']}")
    print(f"      Avg return: {ntd['avg_return']:.2f}%")
    print(f"      Win rate: {ntd['positive_days']}/{ntd['count']} ({100*ntd['positive_days']/max(1,ntd['count']):.0f}%)")
    
    if "statistical_test" in ti:
        st = ti["statistical_test"]
        sig = "YES" if st["significant_difference"] else "NO"
        print(f"   Statistically significant difference? {sig} (p={st['p_value']:.4f})")
    
    # Quiet periods
    print(f"\n🔇 QUIET PERIODS (3+ days without tweets)")
    for i, qp in enumerate(report["quiet_periods"], 1):
        gap = qp["gap_days"]
        is_current = qp.get("is_current", False)
        
        if is_current:
            print(f"   {i}. CURRENT SILENCE: {gap} days (since {qp['last_tweet_before']})")
        else:
            print(f"   {i}. {gap} days: {qp['last_tweet_before']} → {qp['first_tweet_after']}")
        
        if "price_change_during" in qp and qp["price_change_during"] is not None:
            change = qp["price_change_during"]
            direction = "📉" if change < 0 else "📈"
            print(f"      {direction} Price change during silence: {change:.1f}%")


def main():
    """Run the analysis and print results."""
    report = generate_report()
    print_summary(report)
    
    # Save full report
    output_file = DATA_DIR / "analysis_report.json"
    
    # Convert non-serializable objects
    def convert_for_json(obj):
        if isinstance(obj, (pd.Timestamp, datetime)):
            return obj.isoformat()
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if pd.isna(obj):
            return None
        return obj
    
    # Clean the report for JSON serialization
    def clean_for_json(data):
        if isinstance(data, dict):
            return {k: clean_for_json(v) for k, v in data.items()}
        if isinstance(data, list):
            return [clean_for_json(v) for v in data]
        return convert_for_json(data)
    
    clean_report = clean_for_json(report)
    
    with open(output_file, "w") as f:
        json.dump(clean_report, f, indent=2, default=str)
    
    print(f"\n💾 Full report saved to: {output_file}")
    
    return report


if __name__ == "__main__":
    main()

