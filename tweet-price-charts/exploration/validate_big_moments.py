#!/usr/bin/env python3
"""
Validate "The Big Moments" view hypothesis.

Are tweets at price extremes actually interesting to read?
"""

import json
import os
from pathlib import Path
from collections import defaultdict
import re

STATIC_DIR = Path("/Users/satoshi/dev/tweet-price/web/public/static")


def load_all_tweet_events():
    """Load all tweet events from all asset folders."""
    all_events = []

    for asset_dir in STATIC_DIR.iterdir():
        if not asset_dir.is_dir():
            continue

        tweet_file = asset_dir / "tweet_events.json"
        if not tweet_file.exists():
            continue

        with open(tweet_file) as f:
            data = json.load(f)

        for event in data.get("events", []):
            # Only include events with valid price change data
            if event.get("change_24h_pct") is not None:
                event["_asset_id"] = data.get("asset", asset_dir.name)
                event["_founder"] = data.get("founder", "unknown")
                all_events.append(event)

    return all_events


def categorize_tweet(text: str) -> str:
    """
    Categorize a tweet into one of:
    - announcement: substantive news, launches, updates
    - meme_casual: jokes, vibes, casual chat, motivational
    - emoji_only: just emojis or very short
    - thread: appears to be part of a thread
    - question: asking the community something
    - retweet_reply: RT or reply context
    - other
    """
    text_clean = text.strip()

    # Check for empty or very short
    if len(text_clean) < 5:
        return "emoji_only"

    # Remove URLs for analysis
    text_no_urls = re.sub(r'https?://\S+', '', text_clean)

    # Check for emoji-only (mostly emojis after removing URLs)
    text_no_emoji = re.sub(r'[^\w\s]', '', text_no_urls)
    if len(text_no_emoji.strip()) < 10:
        return "emoji_only"

    # Check if it's a retweet or reply
    if text_clean.startswith("RT @") or text_clean.startswith("@"):
        return "retweet_reply"

    # Check for thread indicators
    if re.search(r'^\d+[/\.)]|thread|🧵', text_clean.lower()):
        return "thread"

    # Check for questions
    if '?' in text_clean and len(text_clean) < 200:
        return "question"

    # Look for announcement keywords
    announcement_keywords = [
        'launch', 'announce', 'introducing', 'releasing', 'shipped',
        'live', 'now available', 'going live', 'mainnet', 'testnet',
        'upgrade', 'update', 'v2', 'v3', 'new feature', 'partnership',
        'integration', 'listing', 'exchange', 'milestone', 'airdrop',
        'tokenomics', 'roadmap', 'whitepaper', 'audit', 'security',
        'governance', 'proposal', 'vote', 'dao', 'treasury',
        'raised', 'funding', 'backed', 'investors', 'team',
        'breaking', 'important', 'excited to', 'proud to'
    ]

    text_lower = text_clean.lower()
    if any(kw in text_lower for kw in announcement_keywords):
        return "announcement"

    # Meme/casual indicators
    casual_keywords = [
        'gm', 'gn', 'wagmi', 'ngmi', 'lfg', 'based', 'fren',
        'lol', 'lmao', 'haha', 'vibes', 'mood', 'feels',
        'bullish', 'bearish', 'pump', 'dump', 'moon', 'wen',
        'ser', 'anon', 'degen', 'chad', 'gigachad', 'alpha',
        'cope', 'seethe', 'touch grass', 'ngmi'
    ]

    # Very short tweets are likely casual
    if len(text_clean) < 50:
        return "meme_casual"

    if any(kw in text_lower for kw in casual_keywords):
        return "meme_casual"

    # If it's long and has substance, might be announcement even without keywords
    if len(text_clean) > 200:
        return "announcement"

    return "other"


def is_substantive(category: str) -> bool:
    """Is this category considered substantive content?"""
    return category in ["announcement", "thread"]


def format_tweet_for_display(event: dict, rank: int, direction: str) -> str:
    """Format a tweet for display."""
    change = event.get("change_24h_pct", 0)
    text = event.get("text", "")
    category = categorize_tweet(text)
    substantive = "SUBSTANTIVE" if is_substantive(category) else "noise"

    asset = event.get("_asset_id", "unknown")
    founder = event.get("_founder", "unknown")

    # Truncate for display but show full text
    display_text = text.replace('\n', ' \\n ')

    arrow = "+" if change >= 0 else ""

    return f"""
{'='*80}
#{rank} {direction.upper()} | {asset.upper()} | @{founder}
24h Change: {arrow}{change:.1f}%
Category: {category.upper()} ({substantive})
{'='*80}
{text}
"""


def main():
    print("Loading all tweet events...")
    events = load_all_tweet_events()
    print(f"Loaded {len(events)} tweets with price data")

    # Filter out events without change_24h_pct
    events_with_change = [e for e in events if e.get("change_24h_pct") is not None]
    print(f"Events with 24h change data: {len(events_with_change)}")

    # Sort for pumps (highest change) and dumps (lowest change)
    sorted_by_change = sorted(events_with_change, key=lambda x: x.get("change_24h_pct", 0))

    top_dumps = sorted_by_change[:30]  # Most negative (biggest dumps)
    top_pumps = sorted_by_change[-30:][::-1]  # Most positive (biggest pumps), reversed

    print("\n" + "="*80)
    print("TOP 30 BIGGEST PUMPS (24h after tweet)")
    print("="*80)

    pump_categories = defaultdict(int)
    pump_substantive = 0

    for i, event in enumerate(top_pumps, 1):
        category = categorize_tweet(event.get("text", ""))
        pump_categories[category] += 1
        if is_substantive(category):
            pump_substantive += 1
        print(format_tweet_for_display(event, i, "PUMP"))

    print("\n" + "="*80)
    print("TOP 30 BIGGEST DUMPS (24h after tweet)")
    print("="*80)

    dump_categories = defaultdict(int)
    dump_substantive = 0

    for i, event in enumerate(top_dumps, 1):
        category = categorize_tweet(event.get("text", ""))
        dump_categories[category] += 1
        if is_substantive(category):
            dump_substantive += 1
        print(format_tweet_for_display(event, i, "DUMP"))

    # Summary stats
    print("\n" + "="*80)
    print("SUMMARY ANALYSIS")
    print("="*80)

    print("\n--- PUMP TWEETS (Top 30) ---")
    for cat, count in sorted(pump_categories.items(), key=lambda x: -x[1]):
        pct = count / 30 * 100
        print(f"  {cat}: {count} ({pct:.0f}%)")
    print(f"  SUBSTANTIVE: {pump_substantive}/30 ({pump_substantive/30*100:.0f}%)")

    print("\n--- DUMP TWEETS (Top 30) ---")
    for cat, count in sorted(dump_categories.items(), key=lambda x: -x[1]):
        pct = count / 30 * 100
        print(f"  {cat}: {count} ({pct:.0f}%)")
    print(f"  SUBSTANTIVE: {dump_substantive}/30 ({dump_substantive/30*100:.0f}%)")

    total_substantive = pump_substantive + dump_substantive
    total = 60

    print("\n--- OVERALL ---")
    print(f"Total substantive content: {total_substantive}/{total} ({total_substantive/total*100:.0f}%)")
    print(f"Total noise: {total - total_substantive}/{total} ({(total-total_substantive)/total*100:.0f}%)")

    print("\n" + "="*80)
    print("VERDICT")
    print("="*80)

    if total_substantive >= 40:
        verdict = "STRONG YES - Most tweets at price extremes are substantive and interesting!"
    elif total_substantive >= 25:
        verdict = "MODERATE YES - About half are interesting, could work with curation"
    elif total_substantive >= 15:
        verdict = "WEAK - Only ~25% substantive, would need heavy filtering"
    else:
        verdict = "NO - Most tweets are noise (emojis, gm, casual). View would be boring."

    print(f"\n{verdict}\n")

    # Additional analysis: What makes the announcements interesting?
    print("\n" + "="*80)
    print("DETAILED BREAKDOWN: What types of tweets are at extremes?")
    print("="*80)

    # Count by founder
    pump_founders = defaultdict(int)
    dump_founders = defaultdict(int)
    for e in top_pumps:
        pump_founders[e.get("_founder", "unknown")] += 1
    for e in top_dumps:
        dump_founders[e.get("_founder", "unknown")] += 1

    print("\n--- Pumps by Founder ---")
    for f, c in sorted(pump_founders.items(), key=lambda x: -x[1]):
        print(f"  @{f}: {c} tweets")

    print("\n--- Dumps by Founder ---")
    for f, c in sorted(dump_founders.items(), key=lambda x: -x[1]):
        print(f"  @{f}: {c} tweets")

    # Manual quality assessment
    print("\n" + "="*80)
    print("MANUAL QUALITY ASSESSMENT")
    print("="*80)
    print("""
Looking at the actual tweets, here's what we see:

PUMP TWEETS BREAKDOWN:
- Elon's emojis (3): Not interesting to read, but iconic for meme coins
- CZ tweets (4): Mix of milestone announcements and general commentary
- @pasternak (3): Actually substantive - product updates, vision
- @blknoiz06 (5): Mix of meme/hype and some analysis
- @js_horne (7): Mostly links/retweets, some product updates
- @DipWheeler (5): All "fartcoin to $10" type content
- @theunipcs (2): Mix of shilling and analysis
- @keoneHD (1): Actual announcement (Coinbase listing)

DUMP TWEETS BREAKDOWN:
- Elon emojis dominate (11): 🦾😂💞 etc - not readable content
- @DipWheeler (12): "Fartcoin" jokes and shilling
- @pasternak (3): Substantive updates about Believe
- @blknoiz06 (2): General commentary
- @theunipcs (1): Market commentary
- @cz_binance (1): Defense tweet

KEY INSIGHT:
The problem is that many of these extreme moves are from MEMECOINS where
founders post emojis/memes, NOT from protocol founders who post announcements.

ASSETS CONTRIBUTING TO NOISE:
- GORK (Elon): Pure emoji content
- FARTCOIN: Shitpost content by design
- WIF: Dog meme, casual vibes

ASSETS WITH POTENTIALLY GOOD CONTENT:
- MONAD: Protocol with technical updates
- BELIEVE: Product updates
- ASTER: CZ commentary (mixed quality)
- ZORA: Some product updates
""")

    # What if we filter to only "serious" projects?
    print("\n" + "="*80)
    print("ALTERNATIVE: Filter to serious projects only")
    print("="*80)

    serious_assets = {"monad", "believe", "aster", "zora", "jup", "hype", "zec"}

    serious_pumps = [e for e in top_pumps if e.get("_asset_id") in serious_assets]
    serious_dumps = [e for e in top_dumps if e.get("_asset_id") in serious_assets]

    serious_pump_substantive = sum(1 for e in serious_pumps if is_substantive(categorize_tweet(e.get("text", ""))))
    serious_dump_substantive = sum(1 for e in serious_dumps if is_substantive(categorize_tweet(e.get("text", ""))))

    print(f"Serious project tweets in top pumps: {len(serious_pumps)}/30")
    print(f"  Of those, substantive: {serious_pump_substantive}")
    print(f"Serious project tweets in top dumps: {len(serious_dumps)}/30")
    print(f"  Of those, substantive: {serious_dump_substantive}")

    if len(serious_pumps) + len(serious_dumps) > 0:
        serious_pct = (serious_pump_substantive + serious_dump_substantive) / (len(serious_pumps) + len(serious_dumps)) * 100
        print(f"\nSubstantive rate for serious projects: {serious_pct:.0f}%")

    # Price range info
    print("\n--- PRICE MOVE RANGES ---")
    pump_changes = [e.get("change_24h_pct", 0) for e in top_pumps]
    dump_changes = [e.get("change_24h_pct", 0) for e in top_dumps]

    print(f"Pump range: +{min(pump_changes):.1f}% to +{max(pump_changes):.1f}%")
    print(f"Dump range: {max(dump_changes):.1f}% to {min(dump_changes):.1f}%")


if __name__ == "__main__":
    main()
