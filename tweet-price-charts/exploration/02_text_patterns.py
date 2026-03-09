"""
02: What do the tweet texts reveal?
Are there keyword patterns that predict outcomes?
"""

import json
import re
from pathlib import Path
from collections import Counter
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

# Filter to those with price data
events = [e for e in all_events if e.get("change_24h_pct") is not None]

print("=" * 70)
print("TEXT PATTERN ANALYSIS")
print("=" * 70)

# Tweet length analysis
def get_length_bucket(text):
    length = len(text)
    if length < 50:
        return "short (<50)"
    elif length < 150:
        return "medium (50-150)"
    elif length < 280:
        return "long (150-280)"
    else:
        return "thread (280+)"

length_buckets = {}
for e in events:
    bucket = get_length_bucket(e.get("text", ""))
    if bucket not in length_buckets:
        length_buckets[bucket] = []
    length_buckets[bucket].append(e["change_24h_pct"])

print("\nTweet Length vs Outcome:")
print("-" * 70)
for bucket in ["short (<50)", "medium (50-150)", "long (150-280)", "thread (280+)"]:
    if bucket in length_buckets:
        changes = length_buckets[bucket]
        avg = statistics.mean(changes)
        med = statistics.median(changes)
        wins = sum(1 for c in changes if c > 0) / len(changes) * 100
        print(f"  {bucket:>20}: avg {avg:+6.2f}%, med {med:+6.2f}%, win {wins:5.1f}%, n={len(changes)}")

# Emoji-only tweets
def is_emoji_only(text):
    # Remove whitespace and check if mostly emoji/special chars
    cleaned = re.sub(r'[\s\u200d]', '', text)
    if len(cleaned) < 3:
        return True
    # Check if text is mostly non-ASCII (emojis)
    non_ascii = sum(1 for c in cleaned if ord(c) > 127)
    return non_ascii / len(cleaned) > 0.7

emoji_tweets = [e for e in events if is_emoji_only(e.get("text", ""))]
text_tweets = [e for e in events if not is_emoji_only(e.get("text", ""))]

print(f"\nEmoji-only tweets: {len(emoji_tweets)}")
if emoji_tweets:
    changes = [e["change_24h_pct"] for e in emoji_tweets]
    print(f"  Avg: {statistics.mean(changes):+.2f}%")
    print(f"  Med: {statistics.median(changes):+.2f}%")
    print(f"  Win rate: {sum(1 for c in changes if c > 0) / len(changes) * 100:.1f}%")

print(f"\nText tweets: {len(text_tweets)}")
if text_tweets:
    changes = [e["change_24h_pct"] for e in text_tweets]
    print(f"  Avg: {statistics.mean(changes):+.2f}%")
    print(f"  Med: {statistics.median(changes):+.2f}%")
    print(f"  Win rate: {sum(1 for c in changes if c > 0) / len(changes) * 100:.1f}%")

# Keyword analysis
print("\n" + "=" * 70)
print("KEYWORD ANALYSIS")
print("=" * 70)

keywords = {
    "announcement": ["announce", "announcing", "launch", "launching", "release", "releasing", "live", "shipped"],
    "bullish": ["bullish", "moon", "pump", "ath", "all time high", "buy", "long"],
    "partnership": ["partner", "collab", "integrat", "working with"],
    "milestone": ["milestone", "million", "billion", "reached", "hit", "crossed"],
    "gm/casual": ["gm", "good morning", "gn", "good night", "lol", "lmao"],
    "question": ["?"],
    "has_link": ["http", "t.co"],
    "thread": ["🧵", "thread", "1/"],
}

print("\nKeyword presence vs outcome:")
print("-" * 70)

for category, words in keywords.items():
    matching = []
    for e in events:
        text = e.get("text", "").lower()
        if any(w.lower() in text for w in words):
            matching.append(e)

    if len(matching) >= 10:
        changes = [e["change_24h_pct"] for e in matching]
        avg = statistics.mean(changes)
        med = statistics.median(changes)
        wins = sum(1 for c in changes if c > 0) / len(changes) * 100
        print(f"  {category:>15}: avg {avg:+6.2f}%, med {med:+6.2f}%, win {wins:5.1f}%, n={len(matching)}")

# Top words in pumps vs dumps
print("\n" + "=" * 70)
print("WORD FREQUENCY IN EXTREMES")
print("=" * 70)

def get_words(text):
    # Simple tokenization
    text = text.lower()
    text = re.sub(r'http\S+', '', text)  # Remove URLs
    text = re.sub(r'@\S+', '', text)  # Remove mentions
    text = re.sub(r'[^\w\s]', ' ', text)  # Remove punctuation
    words = text.split()
    # Filter short words and common words
    stopwords = {'the', 'a', 'an', 'is', 'are', 'was', 'were', 'be', 'been', 'being',
                 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
                 'should', 'may', 'might', 'must', 'shall', 'can', 'need', 'dare',
                 'to', 'of', 'in', 'for', 'on', 'with', 'at', 'by', 'from', 'as',
                 'into', 'through', 'during', 'before', 'after', 'above', 'below',
                 'and', 'but', 'or', 'nor', 'so', 'yet', 'both', 'either', 'neither',
                 'not', 'only', 'own', 'same', 'than', 'too', 'very', 'just', 'also',
                 'it', 'its', 'this', 'that', 'these', 'those', 'i', 'me', 'my', 'we',
                 'our', 'you', 'your', 'he', 'him', 'his', 'she', 'her', 'they', 'them',
                 'their', 'what', 'which', 'who', 'whom', 'when', 'where', 'why', 'how',
                 'all', 'each', 'every', 'any', 'some', 'no', 'if', 'up', 'out', 'about'}
    return [w for w in words if len(w) > 2 and w not in stopwords]

sorted_events = sorted(events, key=lambda e: e.get("change_24h_pct", 0), reverse=True)
top_50_pumps = sorted_events[:50]
top_50_dumps = sorted_events[-50:]
middle_events = sorted_events[len(sorted_events)//2 - 50 : len(sorted_events)//2 + 50]

pump_words = Counter()
for e in top_50_pumps:
    pump_words.update(get_words(e.get("text", "")))

dump_words = Counter()
for e in top_50_dumps:
    dump_words.update(get_words(e.get("text", "")))

middle_words = Counter()
for e in middle_events:
    middle_words.update(get_words(e.get("text", "")))

print("\nTop 15 words in PUMP tweets (top 50):")
for word, count in pump_words.most_common(15):
    print(f"  {word}: {count}")

print("\nTop 15 words in DUMP tweets (bottom 50):")
for word, count in dump_words.most_common(15):
    print(f"  {word}: {count}")

print("\nTop 15 words in NEUTRAL tweets (middle 100):")
for word, count in middle_words.most_common(15):
    print(f"  {word}: {count}")

# Words that appear way more in pumps than dumps
print("\n" + "=" * 70)
print("DISTINCTIVE WORDS")
print("=" * 70)

print("\nWords much more common in PUMPS vs DUMPS:")
for word, count in pump_words.most_common(50):
    if count >= 3:
        dump_count = dump_words.get(word, 0)
        if dump_count == 0 or count / (dump_count + 1) > 2:
            print(f"  {word}: {count} pumps, {dump_count} dumps")

print("\nWords much more common in DUMPS vs PUMPS:")
for word, count in dump_words.most_common(50):
    if count >= 3:
        pump_count = pump_words.get(word, 0)
        if pump_count == 0 or count / (pump_count + 1) > 2:
            print(f"  {word}: {count} dumps, {pump_count} pumps")
