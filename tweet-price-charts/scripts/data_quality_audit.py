#!/usr/bin/env python3
"""
Data Quality Audit for Tweet-Price Correlation Project
Comprehensive assessment of data coverage, gaps, and limitations
"""

import duckdb
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from collections import defaultdict

def format_timestamp(ts):
    """Format timestamp for display"""
    if ts is None:
        return "N/A"
    if isinstance(ts, str):
        return ts
    return ts.strftime("%Y-%m-%d %H:%M:%S UTC")

def days_ago(ts):
    """Calculate days ago from now"""
    if ts is None:
        return None
    now = datetime.now(timezone.utc)
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts.replace('Z', '+00:00'))
    elif ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    delta = now - ts
    return delta.days

def main():
    db_path = Path(__file__).parent.parent / 'data' / 'analytics.duckdb'
    assets_path = Path(__file__).parent / 'assets.json'
    web_public = Path(__file__).parent.parent / 'web' / 'public' / 'static'

    conn = duckdb.connect(str(db_path))

    # Load assets config
    with open(assets_path) as f:
        assets_config = json.load(f)

    assets = [a for a in assets_config['assets'] if a['enabled']]
    asset_ids = [a['id'] for a in assets]

    print("=" * 80)
    print("DATA QUALITY AUDIT - Tweet-Price Correlation Project")
    print(f"Generated: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print("=" * 80)

    # ========================================================================
    # 1. PRICE DATA COVERAGE
    # ========================================================================
    print("\n" + "=" * 80)
    print("1. PRICE DATA COVERAGE")
    print("=" * 80)

    for asset in assets:
        asset_id = asset['id']
        launch_date = asset.get('launch_date', 'N/A')
        data_note = asset.get('data_note', '')

        print(f"\n--- {asset['name']} ({asset_id}) ---")
        print(f"Launch Date: {launch_date}")
        print(f"Price Source: {asset['price_source']}")
        if data_note:
            print(f"NOTE: {data_note}")

        # Get timeframes and coverage
        timeframes_query = """
            SELECT
                timeframe,
                COUNT(*) as candle_count,
                MIN(timestamp) as first_candle,
                MAX(timestamp) as last_candle,
                data_source
            FROM prices
            WHERE asset_id = ?
            GROUP BY timeframe, data_source
            ORDER BY
                CASE timeframe
                    WHEN '1m' THEN 1
                    WHEN '1h' THEN 2
                    WHEN '1d' THEN 3
                END,
                data_source
        """

        timeframes = conn.execute(timeframes_query, [asset_id]).fetchall()

        if not timeframes:
            print("  ⚠️  NO PRICE DATA FOUND")
            continue

        for tf, count, first, last, source in timeframes:
            days_range = (last - first).days if first and last else 0
            freshness = days_ago(last)

            print(f"\n  Timeframe: {tf} (source: {source})")
            print(f"    Candles: {count:,}")
            print(f"    Range: {format_timestamp(first)} → {format_timestamp(last)}")
            print(f"    Duration: {days_range} days")
            print(f"    Freshness: {freshness} days ago")

            # Check for gaps (for 1d data)
            if tf == '1d':
                expected_days = days_range + 1
                missing_days = expected_days - count
                if missing_days > 0:
                    print(f"    ⚠️  GAPS: {missing_days} missing days ({missing_days/expected_days*100:.1f}%)")

                    # Find specific gaps
                    gaps_query = """
                        WITH date_series AS (
                            SELECT range::DATE as expected_date
                            FROM range(
                                (SELECT MIN(timestamp)::DATE FROM prices WHERE asset_id = ? AND timeframe = '1d'),
                                (SELECT MAX(timestamp)::DATE FROM prices WHERE asset_id = ? AND timeframe = '1d') + INTERVAL 1 DAY,
                                INTERVAL 1 DAY
                            )
                        ),
                        actual_dates AS (
                            SELECT timestamp::DATE as actual_date
                            FROM prices
                            WHERE asset_id = ? AND timeframe = '1d'
                        )
                        SELECT expected_date
                        FROM date_series
                        WHERE expected_date NOT IN (SELECT actual_date FROM actual_dates)
                        ORDER BY expected_date
                        LIMIT 10
                    """
                    gaps = conn.execute(gaps_query, [asset_id, asset_id, asset_id]).fetchall()
                    if gaps:
                        gap_dates = [str(g[0]) for g in gaps]
                        print(f"    Missing dates (first 10): {', '.join(gap_dates)}")

    # ========================================================================
    # 2. TWEET DATA COVERAGE
    # ========================================================================
    print("\n" + "=" * 80)
    print("2. TWEET DATA COVERAGE")
    print("=" * 80)

    for asset in assets:
        asset_id = asset['id']
        founder = asset['founder']
        launch_date = asset.get('launch_date', 'N/A')
        tweet_filter_note = asset.get('tweet_filter_note', '')

        print(f"\n--- {asset['name']} ({asset_id}) ---")
        print(f"Founder: @{founder}")
        print(f"Launch Date: {launch_date}")
        if tweet_filter_note:
            print(f"Filter: {tweet_filter_note}")

        tweet_stats = conn.execute("""
            SELECT
                COUNT(*) as total_tweets,
                COUNT(CASE WHEN is_filtered THEN 1 END) as filtered_tweets,
                MIN(timestamp) as first_tweet,
                MAX(timestamp) as last_tweet,
                AVG(likes) as avg_likes,
                AVG(retweets) as avg_retweets
            FROM tweets
            WHERE asset_id = ?
        """, [asset_id]).fetchone()

        total, filtered, first, last, avg_likes, avg_rts = tweet_stats

        if total == 0:
            print("  ⚠️  NO TWEETS FOUND")
            continue

        active_tweets = total - (filtered or 0)
        freshness = days_ago(last)

        print(f"  Total Tweets: {total:,} ({active_tweets:,} active, {filtered or 0} filtered)")
        print(f"  Date Range: {format_timestamp(first)} → {format_timestamp(last)}")
        print(f"  Duration: {(last - first).days} days" if first and last else "  Duration: N/A")
        print(f"  Freshness: {freshness} days ago")
        print(f"  Avg Engagement: {avg_likes:.0f} likes, {avg_rts:.0f} retweets")

        # Compare to launch date
        if launch_date != 'N/A' and first:
            launch_dt = datetime.fromisoformat(launch_date.replace('Z', '+00:00'))
            # Ensure first is timezone aware
            if first.tzinfo is None:
                first = first.replace(tzinfo=timezone.utc)
            days_before_launch = (launch_dt - first).days
            if days_before_launch > 0:
                print(f"  ℹ️  First tweet {days_before_launch} days BEFORE launch")
            elif days_before_launch < 0:
                print(f"  ⚠️  First tweet {abs(days_before_launch)} days AFTER launch")

        # Check for obvious gaps (periods > 30 days with no tweets)
        gaps_query = """
            WITH tweet_dates AS (
                SELECT
                    timestamp::DATE as tweet_date,
                    LEAD(timestamp::DATE) OVER (ORDER BY timestamp) as next_tweet_date
                FROM tweets
                WHERE asset_id = ? AND (is_filtered IS NULL OR is_filtered = FALSE)
            )
            SELECT
                tweet_date,
                next_tweet_date,
                next_tweet_date - tweet_date as gap_days
            FROM tweet_dates
            WHERE next_tweet_date - tweet_date > 30
            ORDER BY gap_days DESC
            LIMIT 5
        """
        gaps = conn.execute(gaps_query, [asset_id]).fetchall()
        if gaps:
            print(f"  ⚠️  GAPS DETECTED (>30 days between tweets):")
            for gap_start, gap_end, gap_days in gaps:
                print(f"    {gap_start} → {gap_end}: {gap_days} day gap")

    # ========================================================================
    # 3. ALIGNMENT CHECK
    # ========================================================================
    print("\n" + "=" * 80)
    print("3. TWEET-PRICE ALIGNMENT CHECK")
    print("=" * 80)

    for asset_id in asset_ids:
        print(f"\n--- {asset_id.upper()} ---")

        # Check tweet_events view for alignment
        alignment_query = """
            SELECT
                COUNT(*) as total_tweets,
                COUNT(CASE WHEN price_at_tweet IS NOT NULL THEN 1 END) as tweets_with_price,
                COUNT(CASE WHEN price_at_tweet IS NULL THEN 1 END) as tweets_without_price
            FROM tweet_events
            WHERE asset_id = ?
        """

        result = conn.execute(alignment_query, [asset_id]).fetchone()
        total, with_price, without_price = result

        if total == 0:
            print("  No tweets to analyze")
            continue

        coverage_pct = (with_price / total * 100) if total > 0 else 0

        print(f"  Total Active Tweets: {total:,}")
        print(f"  Tweets with Price Data: {with_price:,} ({coverage_pct:.1f}%)")
        print(f"  Tweets without Price Data: {without_price:,} ({100-coverage_pct:.1f}%)")

        if without_price > 0:
            # Find date ranges where tweets have no price
            no_price_query = """
                SELECT
                    MIN(timestamp) as earliest_missing,
                    MAX(timestamp) as latest_missing
                FROM tweet_events
                WHERE asset_id = ?
                  AND price_at_tweet IS NULL
            """
            missing_range = conn.execute(no_price_query, [asset_id]).fetchone()
            if missing_range[0]:
                print(f"  Missing Price Range: {format_timestamp(missing_range[0])} → {format_timestamp(missing_range[1])}")

        # Check for tweets outside price range
        outside_range_query = """
            SELECT COUNT(*) as outside_count
            FROM tweet_events
            WHERE asset_id = ?
              AND timestamp < (SELECT MIN(timestamp) FROM prices WHERE asset_id = ? AND timeframe = '1h')
        """
        outside = conn.execute(outside_range_query, [asset_id, asset_id]).fetchone()[0]
        if outside > 0:
            print(f"  ⚠️  {outside} tweets BEFORE first price data")

        outside_range_query_after = """
            SELECT COUNT(*) as outside_count
            FROM tweet_events
            WHERE asset_id = ?
              AND timestamp > (SELECT MAX(timestamp) FROM prices WHERE asset_id = ? AND timeframe = '1h')
        """
        outside_after = conn.execute(outside_range_query_after, [asset_id, asset_id]).fetchone()[0]
        if outside_after > 0:
            print(f"  ⚠️  {outside_after} tweets AFTER last price data")

    # ========================================================================
    # 4. DATA FRESHNESS
    # ========================================================================
    print("\n" + "=" * 80)
    print("4. DATA FRESHNESS")
    print("=" * 80)

    for asset_id in asset_ids:
        print(f"\n--- {asset_id.upper()} ---")

        # Last price update
        last_price = conn.execute("""
            SELECT
                MAX(timestamp) as last_price_ts,
                MAX(fetched_at) as last_fetch
            FROM prices
            WHERE asset_id = ?
        """, [asset_id]).fetchone()

        if last_price[0]:
            print(f"  Last Price: {format_timestamp(last_price[0])} (fetched: {format_timestamp(last_price[1])})")
            print(f"  Price Age: {days_ago(last_price[0])} days ago")
        else:
            print("  Last Price: N/A")

        # Last tweet
        last_tweet = conn.execute("""
            SELECT
                MAX(timestamp) as last_tweet_ts,
                MAX(fetched_at) as last_fetch
            FROM tweets
            WHERE asset_id = ?
        """, [asset_id]).fetchone()

        if last_tweet[0]:
            print(f"  Last Tweet: {format_timestamp(last_tweet[0])} (fetched: {format_timestamp(last_tweet[1])})")
            print(f"  Tweet Age: {days_ago(last_tweet[0])} days ago")
        else:
            print("  Last Tweet: N/A")

    # ========================================================================
    # 5. KNOWN ISSUES & ANOMALIES
    # ========================================================================
    print("\n" + "=" * 80)
    print("5. KNOWN ISSUES & ANOMALIES")
    print("=" * 80)

    print("\n--- From assets.json data_note fields ---")
    for asset in assets:
        if asset.get('data_note'):
            print(f"\n{asset['name']} ({asset['id']}):")
            print(f"  {asset['data_note']}")

    print("\n--- Price Data Quality Checks ---")

    # Check for zero/null prices
    for asset_id in asset_ids:
        zero_prices = conn.execute("""
            SELECT COUNT(*)
            FROM prices
            WHERE asset_id = ?
              AND (close = 0 OR close IS NULL)
        """, [asset_id]).fetchone()[0]

        if zero_prices > 0:
            print(f"\n{asset_id.upper()}:")
            print(f"  ⚠️  {zero_prices} candles with zero/null close price")

    # Check for extreme price spikes (>5x in single candle)
    print("\n--- Extreme Price Movements (>5x in single 1m candle) ---")

    spikes_query = """
        SELECT
            asset_id,
            timestamp,
            open,
            close,
            close / NULLIF(open, 0) as price_ratio
        FROM prices
        WHERE timeframe = '1m'
          AND close / NULLIF(open, 0) > 5
        ORDER BY price_ratio DESC
        LIMIT 20
    """

    spikes = conn.execute(spikes_query).fetchall()
    if spikes:
        for asset_id, ts, open_p, close_p, ratio in spikes:
            print(f"  {asset_id}: {format_timestamp(ts)} - {ratio:.1f}x spike (${open_p:.8f} → ${close_p:.8f})")
    else:
        print("  ✓ No extreme spikes detected")

    # Check JSON exports existence
    print("\n--- JSON Export Files ---")
    for asset_id in asset_ids:
        json_file = web_public / asset_id / 'tweet_events.json'
        if json_file.exists():
            size_kb = json_file.stat().st_size / 1024
            mod_time = datetime.fromtimestamp(json_file.stat().st_mtime, tz=timezone.utc)
            age_days = days_ago(mod_time)
            print(f"  {asset_id}: ✓ ({size_kb:.1f} KB, {age_days} days old)")

            # Quick validation
            try:
                with open(json_file) as f:
                    data = json.load(f)
                    print(f"    Events: {len(data.get('events', []))}, Hourly: {len(data.get('hourly_data', []))}")
            except Exception as e:
                print(f"    ⚠️  JSON parse error: {e}")
        else:
            print(f"  {asset_id}: ⚠️  MISSING")

    # ========================================================================
    # SUMMARY & RECOMMENDATIONS
    # ========================================================================
    print("\n" + "=" * 80)
    print("SUMMARY & TRUST ASSESSMENT")
    print("=" * 80)

    print("""
WHAT CAN WE TRUST:
1. Price data quality appears solid for recent data (last 30-90 days)
2. Tweet data is comprehensive for active periods
3. Alignment between tweets and prices is generally good where both exist

WHAT TO BE CAREFUL ABOUT:
1. Historical data gaps - some assets have incomplete early history
2. Launch period coverage varies - not all assets have day-1 data
3. Some assets have reconstructed data (see data_note in assets.json)
4. Tweet gaps may exist during low-activity periods
5. Price data freshness depends on hourly cron job execution

RECOMMENDATIONS:
1. Always check date ranges when analyzing specific time periods
2. Be aware of data_note warnings in assets.json
3. Consider tweet-price alignment % when drawing correlations
4. Monitor data freshness (should update hourly via GitHub Actions)
5. Validate critical findings against multiple timeframes (1m, 1h, 1d)
""")

    conn.close()
    print("\n" + "=" * 80)
    print("Audit Complete")
    print("=" * 80)

if __name__ == '__main__':
    main()
