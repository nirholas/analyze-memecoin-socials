#!/usr/bin/env python3
"""
Analyze tweet content to establish baseline profiles for each founder.
"""

import json
import re
from collections import defaultdict, Counter
from datetime import datetime
from pathlib import Path

# Content categories with keywords
CATEGORIES = {
    "shipping": r'\b(ship|launch|release|live|build|deploy|v\d+|update|feature|now\s+available|rolling\s+out)\b',
    "hype": r'\b(excited|amazing|incredible|wild|lfg|gm|bullish|moon|huge|congrats|honored)\b',
    "metrics": r'\b(\d+\s*million|\d+\s*billion|\d+\s*%|users|holders|volume|tvl|mcap|liquidity|\$\d+[MB])\b',
    "defensive": r'\b(sorry|issue|fix|fud|address|debunk|mistake|chaos|misinformation|clarif)\b',
    "engagement": r'(\?|\bwho\b|\bwhat do you think\b|\bthoughts\b|\breply\b|\blet me know\b)',
    "media": r'(t\.co|http|https)',
}

def load_tweet_data(asset_id):
    """Load tweet data for an asset."""
    path = Path(f"/Users/satoshi/tweet-price/web/public/static/{asset_id}/tweet_events.json")
    if not path.exists():
        return None

    with open(path) as f:
        data = json.load(f)
    return data

def categorize_tweet(text):
    """Categorize a tweet based on its content."""
    text_lower = text.lower()
    categories = []

    for category, pattern in CATEGORIES.items():
        if re.search(pattern, text_lower, re.IGNORECASE):
            categories.append(category)

    if not categories:
        categories.append("other")

    return categories

def analyze_tweet_style(text):
    """Analyze tweet style characteristics."""
    return {
        "length": len(text),
        "has_emoji": bool(re.search(r'[\U0001F300-\U0001F9FF]', text)),
        "has_caps": bool(re.search(r'[A-Z]{3,}', text)),
        "has_link": bool(re.search(r'(t\.co|http)', text)),
        "has_question": '?' in text,
        "word_count": len(text.split()),
    }

def get_month(timestamp):
    """Convert timestamp to YYYY-MM format."""
    return datetime.utcfromtimestamp(timestamp).strftime("%Y-%m")

def analyze_founder(asset_id):
    """Analyze all tweets for a founder."""
    data = load_tweet_data(asset_id)
    if not data or not data.get("events"):
        return None

    founder = data["founder"]
    tweets = data["events"]

    # Overall stats
    total_tweets = len(tweets)

    # Category analysis
    category_counts = Counter()
    category_engagement = defaultdict(lambda: {"likes": [], "retweets": []})

    # Monthly content evolution
    monthly_categories = defaultdict(Counter)

    # Style analysis
    styles = []

    for tweet in tweets:
        text = tweet["text"]
        timestamp = tweet["timestamp"]
        likes = tweet.get("likes", 0)
        retweets = tweet.get("retweets", 0)
        month = get_month(timestamp)

        # Categorize
        categories = categorize_tweet(text)
        for cat in categories:
            category_counts[cat] += 1
            category_engagement[cat]["likes"].append(likes)
            category_engagement[cat]["retweets"].append(retweets)
            monthly_categories[month][cat] += 1

        # Style
        style = analyze_tweet_style(text)
        styles.append(style)

    # Calculate averages
    avg_style = {
        "length": sum(s["length"] for s in styles) / len(styles),
        "emoji_pct": sum(s["has_emoji"] for s in styles) / len(styles) * 100,
        "caps_pct": sum(s["has_caps"] for s in styles) / len(styles) * 100,
        "link_pct": sum(s["has_link"] for s in styles) / len(styles) * 100,
        "question_pct": sum(s["has_question"] for s in styles) / len(styles) * 100,
        "avg_words": sum(s["word_count"] for s in styles) / len(styles),
    }

    # Engagement by category
    engagement_by_cat = {}
    for cat, data_cat in category_engagement.items():
        if data_cat["likes"]:
            engagement_by_cat[cat] = {
                "avg_likes": sum(data_cat["likes"]) / len(data_cat["likes"]),
                "avg_retweets": sum(data_cat["retweets"]) / len(data_cat["retweets"]),
                "total_tweets": len(data_cat["likes"]),
            }

    # Monthly evolution
    months = sorted(monthly_categories.keys())
    early_months = months[:len(months)//2] if len(months) > 2 else months[:1]
    late_months = months[len(months)//2:] if len(months) > 2 else months[-1:]

    early_cats = Counter()
    late_cats = Counter()
    for month in early_months:
        early_cats.update(monthly_categories[month])
    for month in late_months:
        late_cats.update(monthly_categories[month])

    # Normalize to percentages
    early_total = sum(early_cats.values()) or 1
    late_total = sum(late_cats.values()) or 1
    early_pct = {k: v/early_total*100 for k, v in early_cats.items()}
    late_pct = {k: v/late_total*100 for k, v in late_cats.items()}

    return {
        "asset_id": asset_id,
        "founder": founder,
        "total_tweets": total_tweets,
        "category_distribution": {k: v/total_tweets*100 for k, v in category_counts.items()},
        "engagement_by_category": engagement_by_cat,
        "style_profile": avg_style,
        "content_evolution": {
            "early_period": dict(early_pct),
            "late_period": dict(late_pct),
            "early_months": early_months,
            "late_months": late_months,
        },
        "most_common_categories": category_counts.most_common(5),
    }

def print_report(results):
    """Print formatted analysis report."""
    print("=" * 80)
    print("TWEET CONTENT BASELINE ANALYSIS")
    print("=" * 80)

    for result in results:
        if not result:
            continue

        print(f"\n{'=' * 80}")
        print(f"FOUNDER: {result['founder']} (Asset: {result['asset_id']})")
        print(f"Total Tweets: {result['total_tweets']}")
        print("=" * 80)

        # Category distribution
        print("\n1. CONTENT CATEGORY DISTRIBUTION:")
        print("-" * 40)
        for cat, pct in sorted(result['category_distribution'].items(), key=lambda x: -x[1]):
            print(f"  {cat:15s}: {pct:5.1f}%")

        # Engagement by category
        print("\n2. ENGAGEMENT BY CATEGORY:")
        print("-" * 40)
        print(f"  {'Category':<15} {'Avg Likes':>10} {'Avg RTs':>10} {'Count':>8}")
        for cat, eng in sorted(result['engagement_by_category'].items(),
                               key=lambda x: -x[1]['avg_likes']):
            print(f"  {cat:<15} {eng['avg_likes']:>10.0f} {eng['avg_retweets']:>10.0f} {eng['total_tweets']:>8}")

        # Style profile
        print("\n3. TWEET STYLE PROFILE:")
        print("-" * 40)
        style = result['style_profile']
        print(f"  Average length:    {style['length']:.0f} characters")
        print(f"  Average words:     {style['avg_words']:.1f}")
        print(f"  Uses emojis:       {style['emoji_pct']:.1f}% of tweets")
        print(f"  Uses CAPS:         {style['caps_pct']:.1f}% of tweets")
        print(f"  Includes links:    {style['link_pct']:.1f}% of tweets")
        print(f"  Asks questions:    {style['question_pct']:.1f}% of tweets")

        # Content evolution
        print("\n4. CONTENT EVOLUTION:")
        print("-" * 40)
        evo = result['content_evolution']
        print(f"  Early period ({', '.join(evo['early_months'][:3])}...):")
        for cat, pct in sorted(evo['early_period'].items(), key=lambda x: -x[1])[:5]:
            print(f"    {cat:15s}: {pct:5.1f}%")
        print(f"\n  Late period ({', '.join(evo['late_months'][-3:])}):")
        for cat, pct in sorted(evo['late_period'].items(), key=lambda x: -x[1])[:5]:
            print(f"    {cat:15s}: {pct:5.1f}%")

        # Key changes
        early = evo['early_period']
        late = evo['late_period']
        changes = {}
        for cat in set(list(early.keys()) + list(late.keys())):
            change = late.get(cat, 0) - early.get(cat, 0)
            if abs(change) > 5:  # Only show significant changes
                changes[cat] = change

        if changes:
            print("\n  Significant changes:")
            for cat, change in sorted(changes.items(), key=lambda x: -abs(x[1])):
                direction = "↑" if change > 0 else "↓"
                print(f"    {cat:15s}: {direction} {abs(change):5.1f}pp")

def main():
    assets = ["pump", "hype", "aster", "believe", "jup", "monad", "useless"]

    results = []
    for asset in assets:
        print(f"Analyzing {asset}...", end=" ")
        result = analyze_founder(asset)
        if result:
            results.append(result)
            print(f"✓ ({result['total_tweets']} tweets)")
        else:
            print("✗ (no data)")

    print_report(results)

    # Summary comparison
    print("\n" + "=" * 80)
    print("CROSS-FOUNDER COMPARISON")
    print("=" * 80)

    print("\nMost 'shipping' focused:")
    shipping = [(r['founder'], r['category_distribution'].get('shipping', 0))
                for r in results if r]
    for founder, pct in sorted(shipping, key=lambda x: -x[1])[:3]:
        print(f"  {founder:20s}: {pct:5.1f}%")

    print("\nMost 'defensive' content:")
    defensive = [(r['founder'], r['category_distribution'].get('defensive', 0))
                 for r in results if r]
    for founder, pct in sorted(defensive, key=lambda x: -x[1])[:3]:
        print(f"  {founder:20s}: {pct:5.1f}%")

    print("\nMost 'hype' oriented:")
    hype = [(r['founder'], r['category_distribution'].get('hype', 0))
            for r in results if r]
    for founder, pct in sorted(hype, key=lambda x: -x[1])[:3]:
        print(f"  {founder:20s}: {pct:5.1f}%")

    print("\nMost engagement-seeking:")
    engagement = [(r['founder'], r['category_distribution'].get('engagement', 0))
                  for r in results if r]
    for founder, pct in sorted(engagement, key=lambda x: -x[1])[:3]:
        print(f"  {founder:20s}: {pct:5.1f}%")

    print("\nAverage tweet length:")
    lengths = [(r['founder'], r['style_profile']['length']) for r in results if r]
    for founder, length in sorted(lengths, key=lambda x: -x[1]):
        print(f"  {founder:20s}: {length:5.0f} characters")

    print("\n" + "=" * 80)
    print("CONTENT BASELINE PROFILES COMPLETE")
    print("=" * 80)

if __name__ == "__main__":
    main()
