"""
Nitter Scraper - Robust tweet backfill via Nitter instances.

===============================================================================
WHEN TO USE THIS VS X API (fetch_tweets.py)
===============================================================================

DECISION TREE:
                    ┌─────────────────────────┐
                    │ Is asset > 150 days old?│
                    └───────────┬─────────────┘
                          ┌─────┴─────┐
                         YES          NO
                          │            │
              ┌───────────▼───────┐   │
              │ Use NITTER first  │   │
              │ for historical    │   │
              │ backfill          │   ▼
              └───────────────────┘  Use X API only
                          │          (fetch_tweets.py)
                          ▼
              ┌───────────────────┐
              │ Then use X API    │
              │ for ongoing       │
              │ updates           │
              └───────────────────┘

USE NITTER (this script) WHEN:
1. Asset is > 150 days old (X API limit is ~150 days)
2. Need historical backfill beyond X API reach
3. X API rate limits are exhausted
4. Adding an "adopter" asset with keyword filtering

USE X API (fetch_tweets.py) WHEN:
1. Asset is < 150 days old
2. Running ongoing/scheduled tweet updates
3. Need real-time or near-real-time tweets
4. Have available API quota

TYPICAL WORKFLOW FOR NEW ASSETS:
1. Add asset to assets.json (manually for adopters, CLI for founders)
2. Run fetch_tweets.py --asset X (gets recent 150 days + sets up watermarks)
3. If asset > 150 days: Run nitter_scraper.py --asset X --full --no-headless --parallel 3
4. Run cache_logos.py --asset X (downloads token logo from CoinGecko)
5. Run cache_avatars.py --asset X (downloads founder's Twitter PFP)
6. Run export_static.py --asset X
7. Schedule fetch_tweets.py for hourly updates

WHY X API FIRST:
- More reliable and faster than Nitter
- Sets up watermarks for ongoing updates immediately
- Gets you "live" recent data right away
- Nitter is ONLY needed for historical backfill beyond 150 days

===============================================================================
FOUNDER VS ADOPTER ASSETS
===============================================================================

FOUNDER: Person who created the token (all their tweets are relevant)
  - Example: @a1lon9 created PUMP
  - No keyword filter needed
  - founder_type: "founder" (default)

ADOPTER: Person who adopted/promoted an existing token
  - Example: @blknoiz06 adopted WIF (didn't create it)
  - MUST have keyword_filter in assets.json (e.g., "wif")
  - Most of their tweets are NOT about the token
  - founder_type: "adopter" in assets.json
  - Results in tweet_events.json (filtered) AND tweet_events_all.json

===============================================================================
KEYWORD SEARCH MODE (--keyword-search)
===============================================================================

USE --keyword-search WHEN:
  The founder/adopter RARELY tweets about the token (sparse tweeter).

  Example: Sam Altman (@sama) has tweeted about Worldcoin only 13 times
  in 4 years. Fetching his entire timeline (thousands of tweets) just to
  find 13 is wasteful. Instead, use:

    python nitter_scraper.py --asset wld --keyword-search

  This searches Nitter directly: nitter.net/sama/search?q=worldcoin
  and returns ONLY tweets matching the keyword filter.

WHEN TO USE:
  - Founder has < 50 tweets about the token over their entire history
  - You want a quick count/sample before doing a full scrape
  - X API is rate-limited and you need tweets NOW

WHEN NOT TO USE:
  - Founder tweets frequently about the token (use --full instead)
  - You need date-range filtering (use --since/--until instead)
  - The keyword is very common (will return too many results)

HOW IT WORKS:
  1. Uses Nitter's user search: /{username}/search?q={keyword}
  2. Paginates through ALL matching tweets (no date filtering)
  3. Much faster than --full for sparse tweeters
  4. Returns only keyword-matching tweets (no post-filtering needed)

===============================================================================
ARCHITECTURE OVERVIEW
===============================================================================

This scraper uses Playwright (headless Chrome) to scrape tweets from Nitter,
a Twitter frontend that doesn't require API access. It's designed as a fallback
when the official Twitter API is rate-limited or unavailable.

WHY NITTER?
- No API key required
- No rate limits (with careful scraping)
- Access to historical tweets
- Date-range filtering via URL params

KEY FEATURES:
1. Single instance (nitter.net) - Only reliable instance as of Dec 2024
2. Conservative delays - 30-60s between chunks to avoid rate limiting
3. Exponential backoff - Retries with increasing delays on failure
4. Progress tracking - Resumable via JSON file (data/nitter_progress.json)
5. Username filtering - Only captures tweets from the target founder
6. Keyword filtering - For adopter assets, filters tweets by keyword
7. Parallel scraping - Multiple browser instances for faster backfill

===============================================================================
LESSONS LEARNED FROM V1 (Why this exists)
===============================================================================

The original scraper (nitter_scraper_v1_deprecated.py) failed because:
- 5 second delay was too fast → net::ERR_HTTP_RESPONSE_CODE_FAILURE
- Single instance = single point of failure  
- No retry logic → lost progress on any error
- No progress tracking → couldn't resume

V2 IMPROVEMENTS:
1. Much longer delays (30-60s between chunks vs 5s)
2. Exponential backoff retry (up to 5 attempts per chunk)
3. Instance rotation (nitter.net → nitter.poast.org)
4. Randomized delays (appear human-like)
5. Browser session reuse (reduces detection fingerprint)
6. Progress tracking (resume from failures)
7. Early pagination termination (stops after 3 empty pages)

===============================================================================
USAGE EXAMPLES
===============================================================================

# Full backfill from launch date to present (RECOMMENDED for new assets)
python nitter_scraper.py --asset useless --full --no-headless

# Specific date range
python nitter_scraper.py --asset useless --since 2025-06-01 --until 2025-07-01

# Resume after interruption (uses progress file)
python nitter_scraper.py --asset useless --full

# Clear progress and start fresh
python nitter_scraper.py --asset useless --clear-progress
python nitter_scraper.py --asset useless --full

# Test with a short chunk (for debugging)
python nitter_scraper.py --asset useless --since 2025-06-01 --until 2025-06-08

===============================================================================
PERFORMANCE NOTES
===============================================================================

SEQUENTIAL SCRAPING (--parallel 1, default):
- ~1 minute per 7-day chunk (including delays)
- ~2 hours for a 2-year backfill (108 chunks)
- Safe, proven reliable

PARALLEL SCRAPING (--parallel 2 or --parallel 3):
- Same ~1 minute per chunk, but N chunks at once
- 2 workers: ~1 hour for 2-year backfill (2x speedup)
- 3 workers: ~40 min for 2-year backfill (3x speedup)
- Tested with 3 workers on nitter.net - NO RATE LIMITING

WHY PARALLEL WORKS (discovered Dec 2024):
- Nitter rate limiting is SESSION-based, not IP-based
- Each browser instance has its own session
- Multiple browsers = multiple sessions = no conflicts
- All workers can use nitter.net simultaneously

RECOMMENDED SETTINGS:
- New users: Start with --parallel 2 to be safe
- Experienced: --parallel 3 works fine
- Maximum tested: 3 workers (beyond that untested)

===============================================================================
PARALLEL SCRAPING ARCHITECTURE
===============================================================================

┌─────────────────────────────────────────────────────────────┐
│                     Main Thread                              │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │  Worker 0    │  │  Worker 1    │  │  Worker 2    │       │
│  │ Browser 0    │  │ Browser 1    │  │ Browser 2    │       │
│  │ Chunks 0,3,6 │  │ Chunks 1,4,7 │  │ Chunks 2,5,8 │       │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘       │
│         │                 │                 │                │
│         └─────────────────┼─────────────────┘                │
│                           ▼                                  │
│                  ┌──────────────┐                           │
│                  │ Write Queue  │ (thread-safe)             │
│                  └──────┬───────┘                           │
│                         ▼                                    │
│                  ┌──────────────┐                           │
│                  │ DB Writer    │ (single thread)           │
│                  │ Thread       │                           │
│                  └──────────────┘                           │
└─────────────────────────────────────────────────────────────┘

WHY SINGLE DB WRITER:
- DuckDB has exclusive write locks
- Multiple writers = lock conflicts
- Queue pattern: scrapers push, one writer consumes
- Result: Zero lock conflicts, maximum parallelism

===============================================================================
DATA FORMAT
===============================================================================

Tweets are saved in the exact format expected by db.insert_tweets():
{
    'id': '123456789',           # Tweet ID (string)
    'text': 'Tweet content...',  # Full text
    'timestamp': datetime,        # Python datetime (UTC)
    'created_at': 'ISO8601',     # ISO string for display
    'likes': 100,                # Like count
    'retweets': 50,              # Retweet count
    'replies': 10,               # Reply count
    'impressions': 1000          # View count (if available)
}

===============================================================================
"""
import argparse
import json
import re
import sys
import time
import random
import threading
from queue import Queue
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

# Add scripts dir to path for imports
SCRIPTS_DIR = Path(__file__).parent
sys.path.insert(0, str(SCRIPTS_DIR))

from db import (
    get_connection, init_schema, get_asset, load_assets_from_json,
    insert_tweets, get_ingestion_state, update_ingestion_state
)

# Check for playwright
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout, Browser, BrowserContext, Page
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    # Define placeholder types for type hints when playwright not installed
    Page = Any
    Browser = Any
    BrowserContext = Any


# =============================================================================
# CONFIGURATION - TUNED FOR RELIABILITY
# =============================================================================

# Instance list - ONLY nitter.net works reliably as of Dec 2024
#
# TESTED INSTANCES (Dec 2024):
# - nitter.net: ✅ WORKS - Primary instance, reliable
# - nitter.poast.org: ❌ FAILS - Cloudflare challenges, often down
# - nitter.privacydev.net: ❌ FAILS - Dead/unresponsive
# - Others: Not tested, likely dead
#
# IMPORTANT: All parallel workers use the same instance (nitter.net).
# This works because rate limiting is SESSION-based, not IP-based.
# Each browser has its own session, so multiple browsers = no conflicts.
NITTER_INSTANCES = [
    "https://nitter.net",
]

# Timing - MUCH more conservative than v1
# These delays are CRITICAL for reliability. Do not reduce.
#
# WHY 30-60 SECONDS:
# - Nitter servers are community-run with limited resources
# - Too fast = ERR_HTTP_RESPONSE_CODE_FAILURE (429-like)
# - 5 seconds (v1) was too aggressive, caused failures
# - 30-60 seconds has proven reliable over months of use
MIN_CHUNK_DELAY = 30  # Minimum 30 seconds between chunks
MAX_CHUNK_DELAY = 60  # Maximum 60 seconds between chunks
MIN_PAGE_DELAY = 3    # Minimum 3 seconds between pagination clicks
MAX_PAGE_DELAY = 6    # Maximum 6 seconds between pagination clicks

# Retry configuration
MAX_RETRIES = 5              # Retry each chunk up to 5 times
INITIAL_RETRY_DELAY = 60     # Start with 60 second delay
RETRY_BACKOFF_FACTOR = 1.5   # Multiply delay by 1.5 each retry

# Timeouts
PAGE_LOAD_TIMEOUT = 45000    # 45 seconds for page load
TWEET_WAIT_TIMEOUT = 20000   # 20 seconds to wait for tweets
CLOUDFLARE_WAIT = 5          # 5 seconds for Cloudflare challenges

# Chunk settings
DEFAULT_CHUNK_DAYS = 7       # 7 days per chunk (smaller = safer)
MAX_TWEETS_PER_CHUNK = 500   # Safety limit
MAX_PAGES_PER_CHUNK = 20     # Pagination safety limit

# Progress file for resumability
PROGRESS_FILE = Path(__file__).parent.parent / "data" / "nitter_progress.json"

# Thread safety for parallel scraping
progress_lock = threading.Lock()


# =============================================================================
# LOGGING
# =============================================================================

def log(msg: str, level: str = "INFO"):
    """Log with timestamp and level."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prefix = {
        "INFO": "ℹ️ ",
        "OK": "✅ ",
        "WARN": "⚠️ ",
        "ERROR": "❌ ",
        "DEBUG": "🔍 ",
        "WAIT": "⏳ ",
    }.get(level, "")
    print(f"[{timestamp}] {prefix}{msg}", flush=True)


# =============================================================================
# PROGRESS TRACKING
# =============================================================================

def load_progress() -> Dict:
    """Load progress from file (thread-safe)."""
    with progress_lock:
        if PROGRESS_FILE.exists():
            try:
                return json.loads(PROGRESS_FILE.read_text())
            except:
                pass
        return {}


def save_progress(progress: Dict):
    """Save progress to file (thread-safe)."""
    with progress_lock:
        PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
        PROGRESS_FILE.write_text(json.dumps(progress, indent=2, default=str))


def get_completed_chunks(asset_id: str) -> set:
    """Get set of completed chunk date ranges (thread-safe)."""
    progress = load_progress()
    return set(progress.get(asset_id, {}).get("completed_chunks", []))


def mark_chunk_complete(asset_id: str, chunk_key: str):
    """Mark a chunk as completed (thread-safe)."""
    with progress_lock:
        # Re-load inside lock to avoid race conditions
        if PROGRESS_FILE.exists():
            try:
                progress = json.loads(PROGRESS_FILE.read_text())
            except:
                progress = {}
        else:
            progress = {}

        if asset_id not in progress:
            progress[asset_id] = {"completed_chunks": []}
        if chunk_key not in progress[asset_id]["completed_chunks"]:
            progress[asset_id]["completed_chunks"].append(chunk_key)

        PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
        PROGRESS_FILE.write_text(json.dumps(progress, indent=2, default=str))


def clear_progress(asset_id: str):
    """Clear progress for an asset (thread-safe)."""
    with progress_lock:
        if PROGRESS_FILE.exists():
            try:
                progress = json.loads(PROGRESS_FILE.read_text())
            except:
                progress = {}
        else:
            progress = {}

        if asset_id in progress:
            del progress[asset_id]
            PROGRESS_FILE.write_text(json.dumps(progress, indent=2, default=str))


# =============================================================================
# DATE PARSING
# =============================================================================

def parse_nitter_date(title_attr: str) -> Optional[datetime]:
    """
    Parse Nitter's date title attribute to datetime.
    Format: "Dec 17, 2024 · 3:45 PM UTC" or similar variants.
    """
    if not title_attr:
        return None
    
    # Clean up the string
    clean = title_attr.replace('·', '').strip()
    clean = re.sub(r'\s+', ' ', clean)  # Normalize whitespace
    
    # Try various formats
    formats = [
        "%b %d, %Y %I:%M %p UTC",
        "%b %d, %Y %I:%M %p",
        "%d %b %Y %I:%M %p UTC",
        "%d %b %Y %I:%M %p",
        "%b %d, %Y %H:%M UTC",
        "%b %d, %Y %H:%M",
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(clean, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    
    return None


def extract_tweet_id(link: str) -> Optional[str]:
    """Extract tweet ID from Nitter link like '/user/status/123456789'"""
    match = re.search(r'/status/(\d+)', link)
    return match.group(1) if match else None


def parse_stat_number(text: str) -> int:
    """Parse stat number, handling K/M suffixes and commas."""
    if not text:
        return 0
    
    text = text.strip().replace(',', '')
    
    multiplier = 1
    if text.endswith('K'):
        multiplier = 1000
        text = text[:-1]
    elif text.endswith('M'):
        multiplier = 1000000
        text = text[:-1]
    
    try:
        return int(float(text) * multiplier)
    except (ValueError, TypeError):
        return 0


# =============================================================================
# SCRAPER CORE
# =============================================================================

def wait_random(min_sec: float, max_sec: float, reason: str = ""):
    """Wait a random amount of time."""
    delay = random.uniform(min_sec, max_sec)
    if reason:
        log(f"Waiting {delay:.1f}s - {reason}", "WAIT")
    time.sleep(delay)


def handle_cloudflare(page: Page) -> bool:
    """
    Handle Cloudflare challenge if present.
    Returns True if challenge was resolved or not present.
    """
    try:
        # Check for Cloudflare challenge indicators
        content = page.content().lower()
        if 'verifying' in content or 'cloudflare' in content or 'checking your browser' in content:
            log("Cloudflare challenge detected, waiting...", "WAIT")
            time.sleep(CLOUDFLARE_WAIT)
            
            # Wait for challenge to resolve
            for _ in range(10):  # Max 10 attempts
                content = page.content().lower()
                if 'verifying' not in content and 'cloudflare' not in content:
                    log("Cloudflare challenge passed", "OK")
                    return True
                time.sleep(2)
            
            log("Cloudflare challenge did not resolve", "WARN")
            return False
        
        return True
    except Exception as e:
        log(f"Error checking Cloudflare: {e}", "WARN")
        return True  # Continue anyway


def extract_tweets_from_page(page: Page, target_username: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Extract all tweets from the current page state.
    Returns list of tweet dicts in db.insert_tweets() format.
    
    Args:
        page: Playwright page object
        target_username: If provided, only include tweets from this user (without @)
    """
    tweets = []
    seen_ids = set()
    
    try:
        tweet_elements = page.query_selector_all('.timeline-item')
        
        for el in tweet_elements:
            try:
                # Skip non-tweet items
                classes = el.get_attribute('class') or ''
                if 'show-more' in classes:
                    continue
                
                # Get tweet link and extract ID
                date_link = el.query_selector('.tweet-date a')
                if not date_link:
                    continue
                
                href = date_link.get_attribute('href') or ''
                tweet_id = extract_tweet_id(href)
                
                if not tweet_id or tweet_id in seen_ids:
                    continue
                
                # Skip retweets
                if el.query_selector('.retweet-header'):
                    continue

                # Capture reply-to username (for adopter filtering)
                # Instead of skipping replies, we capture who they're replying to
                # This allows matching tweets replying to tracked accounts (e.g., @gork)
                reply_to = None
                replying_to_el = el.query_selector('.replying-to a')
                if replying_to_el:
                    href = replying_to_el.get_attribute('href') or ''
                    if href.startswith('/'):
                        # Extract username from href like "/gork"
                        reply_to = href.split('/')[1].lower() if len(href.split('/')) > 1 else None

                # IMPORTANT: Filter by author username
                if target_username:
                    # Get the username from the tweet header
                    # Try multiple selectors as Nitter HTML can vary
                    username_el = el.query_selector('.tweet-header .username')
                    if not username_el:
                        username_el = el.query_selector('.username')
                    
                    if username_el:
                        tweet_username = username_el.inner_text().strip().lstrip('@').lower()
                        if tweet_username != target_username.lower():
                            # This tweet is from a different user (e.g., a quote tweet)
                            continue
                    else:
                        # Can't determine username from selector - check URL as fallback
                        if f"/{target_username.lower()}/status/" not in href.lower():
                            continue
                
                seen_ids.add(tweet_id)
                
                # Get date
                date_title = date_link.get_attribute('title') or ''
                tweet_dt = parse_nitter_date(date_title)
                
                if not tweet_dt:
                    continue
                
                # Get text
                text_el = el.query_selector('.tweet-content')
                text = text_el.inner_text().strip() if text_el else ""
                
                # Get stats
                stats = {'replies': 0, 'retweets': 0, 'likes': 0, 'impressions': 0}
                
                stat_elements = el.query_selector_all('.tweet-stat')
                for stat_el in stat_elements:
                    stat_text = stat_el.inner_text().strip()
                    stat_val = parse_stat_number(stat_text)
                    
                    icon = stat_el.query_selector('.icon-comment, .icon-retweet, .icon-heart, .icon-chart')
                    if icon:
                        icon_class = icon.get_attribute('class') or ''
                        if 'comment' in icon_class:
                            stats['replies'] = stat_val
                        elif 'retweet' in icon_class:
                            stats['retweets'] = stat_val
                        elif 'heart' in icon_class:
                            stats['likes'] = stat_val
                        elif 'chart' in icon_class:
                            stats['impressions'] = stat_val
                
                # Build tweet in db format
                tweet = {
                    'id': tweet_id,
                    'text': text,
                    'timestamp': tweet_dt,
                    'created_at': tweet_dt.isoformat().replace('+00:00', 'Z'),
                    'likes': stats['likes'],
                    'retweets': stats['retweets'],
                    'replies': stats['replies'],
                    'impressions': stats['impressions'],
                    'reply_to': reply_to,  # Username being replied to (for adopter filtering)
                }

                tweets.append(tweet)
                
            except Exception as e:
                log(f"Error parsing tweet: {e}", "DEBUG")
                continue
    
    except Exception as e:
        log(f"Error extracting tweets: {e}", "ERROR")
    
    return tweets


def scrape_chunk_with_context(
    context: BrowserContext,
    instance: str,
    username: str,
    since: str,
    until: str,
    keyword: Optional[str] = None,
    max_tweets: int = MAX_TWEETS_PER_CHUNK,
) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Scrape a single date chunk using existing browser context.
    
    Returns:
        (list of tweets, success boolean)
    """
    from urllib.parse import quote_plus
    
    query = quote_plus(keyword) if keyword else ""
    url = f"{instance}/{username}/search?f=tweets&q={query}&since={since}&until={until}"
    
    log(f"Scraping: {since} to {until} via {instance.split('//')[1]}", "INFO")
    
    all_tweets = []
    seen_ids = set()
    page = None
    
    try:
        page = context.new_page()
        page.goto(url, timeout=PAGE_LOAD_TIMEOUT)
        
        # Handle Cloudflare
        if not handle_cloudflare(page):
            page.close()
            return [], False
        
        # Wait for content
        try:
            page.wait_for_selector('.timeline-item, .error-panel', timeout=TWEET_WAIT_TIMEOUT)
        except PlaywrightTimeout:
            log("No tweets found for date range (timeout)", "INFO")
            page.close()
            return [], True  # Empty but successful
        
        # Check for error panel
        error = page.query_selector('.error-panel')
        if error:
            error_text = error.inner_text()
            if "No items found" in error_text:
                log("No tweets in this range", "INFO")
                page.close()
                return [], True
            else:
                log(f"Error panel: {error_text}", "WARN")
                page.close()
                return [], False
        
        # Paginate and collect tweets
        pages_scraped = 0
        empty_pages = 0  # Track consecutive pages with no new tweets
        MAX_EMPTY_PAGES = 3  # Stop after 3 consecutive empty pages
        
        while len(all_tweets) < max_tweets and pages_scraped < MAX_PAGES_PER_CHUNK:
            pages_scraped += 1
            
            # Extract tweets from current page (filtered by username)
            new_tweets = extract_tweets_from_page(page, target_username=username)
            added = 0
            
            for t in new_tweets:
                if t['id'] not in seen_ids:
                    seen_ids.add(t['id'])
                    all_tweets.append(t)
                    added += 1
            
            log(f"  Page {pages_scraped}: +{added} tweets (total: {len(all_tweets)})", "DEBUG")
            
            # Track empty pages
            if added == 0:
                empty_pages += 1
                if empty_pages >= MAX_EMPTY_PAGES:
                    log(f"  Stopping after {MAX_EMPTY_PAGES} empty pages", "DEBUG")
                    break
            else:
                empty_pages = 0  # Reset on successful page
            
            if len(all_tweets) >= max_tweets:
                break
            
            # Check for "Load more" button
            load_more = page.query_selector('.show-more a')
            if not load_more:
                break
            
            try:
                load_more.click()
                wait_random(MIN_PAGE_DELAY, MAX_PAGE_DELAY, "pagination")
                page.wait_for_selector('.timeline-item', timeout=10000)
            except Exception as e:
                log(f"Pagination ended: {e}", "DEBUG")
                break
        
        page.close()
        return all_tweets, True
        
    except PlaywrightTimeout:
        log("Page load timeout", "WARN")
        if page:
            page.close()
        return [], False
    except Exception as e:
        log(f"Scrape error: {e}", "ERROR")
        if page:
            page.close()
        return [], False


def scrape_chunk_with_retry(
    context: BrowserContext,
    username: str,
    since: str,
    until: str,
    keyword: Optional[str] = None,
) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Scrape a chunk with retry logic and instance rotation.
    """
    retry_delay = INITIAL_RETRY_DELAY
    
    for attempt in range(MAX_RETRIES):
        # Rotate instances
        instance = NITTER_INSTANCES[attempt % len(NITTER_INSTANCES)]
        
        if attempt > 0:
            log(f"Retry {attempt}/{MAX_RETRIES} after {retry_delay:.0f}s delay", "WAIT")
            time.sleep(retry_delay)
            retry_delay *= RETRY_BACKOFF_FACTOR
        
        tweets, success = scrape_chunk_with_context(
            context=context,
            instance=instance,
            username=username,
            since=since,
            until=until,
            keyword=keyword,
        )
        
        if success:
            return tweets, True
    
    log(f"Chunk failed after {MAX_RETRIES} retries", "ERROR")
    return [], False


# =============================================================================
# MAIN ORCHESTRATION
# =============================================================================

def scrape_asset(
    asset_id: str,
    since: Optional[str] = None,
    until: Optional[str] = None,
    keyword: Optional[str] = None,
    full: bool = False,
    resume: bool = True,
    chunk_days: int = DEFAULT_CHUNK_DAYS,
    headless: bool = False,
) -> Dict[str, Any]:
    """
    Main entry point for scraping an asset's tweets.
    
    Args:
        asset_id: Asset ID from assets.json
        since: Start date YYYY-MM-DD (optional)
        until: End date YYYY-MM-DD (optional)
        keyword: Keyword filter (uses asset config if None)
        full: Scrape from launch to now
        resume: Skip already-completed chunks
        chunk_days: Days per chunk
        headless: Run browser headless
    
    Returns:
        Summary dict with status and stats
    """
    if not PLAYWRIGHT_AVAILABLE:
        return {'status': 'error', 'reason': 'Playwright not installed'}
    
    # Load asset config
    conn = get_connection()
    init_schema(conn)
    load_assets_from_json(conn)
    
    asset = get_asset(conn, asset_id)
    if not asset:
        conn.close()
        return {'status': 'error', 'reason': f"Asset '{asset_id}' not found"}
    
    username = asset['founder']
    launch_date = asset['launch_date']
    
    # Use keyword from args, or fall back to asset config
    effective_keyword = keyword if keyword is not None else asset.get('keyword_filter')
    
    if isinstance(launch_date, str):
        launch_date = datetime.fromisoformat(launch_date.replace('Z', '+00:00'))
    if launch_date.tzinfo is None:
        launch_date = launch_date.replace(tzinfo=timezone.utc)
    
    log(f"Asset: {asset['name']} (@{username})", "INFO")
    log(f"Launch: {launch_date.date()}", "INFO")
    if effective_keyword:
        log(f"Keyword filter: \"{effective_keyword}\"", "INFO")
    
    # Determine date range
    if since and until:
        since_dt = datetime.strptime(since, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        until_dt = datetime.strptime(until, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    elif full:
        since_dt = launch_date
        until_dt = datetime.now(timezone.utc)
    else:
        # Backfill mode: from launch to oldest tweet
        oldest = conn.execute("""
            SELECT MIN(timestamp) FROM tweets WHERE asset_id = ?
        """, [asset_id]).fetchone()[0]
        
        if oldest:
            since_dt = launch_date
            until_dt = oldest if isinstance(oldest, datetime) else datetime.fromisoformat(str(oldest))
            if until_dt.tzinfo is None:
                until_dt = until_dt.replace(tzinfo=timezone.utc)
        else:
            since_dt = launch_date
            until_dt = datetime.now(timezone.utc)
    
    # Clamp dates
    if since_dt < launch_date:
        since_dt = launch_date
    now = datetime.now(timezone.utc)
    if until_dt > now:
        until_dt = now
    
    if since_dt >= until_dt:
        log("No date range to scrape", "INFO")
        conn.close()
        return {'status': 'skipped', 'reason': 'No date range'}
    
    # Generate chunks
    chunks = []
    current = since_dt
    while current < until_dt:
        chunk_end = min(current + timedelta(days=chunk_days), until_dt)
        chunk_key = f"{current.strftime('%Y-%m-%d')}_{chunk_end.strftime('%Y-%m-%d')}"
        chunks.append({
            'since': current.strftime('%Y-%m-%d'),
            'until': chunk_end.strftime('%Y-%m-%d'),
            'key': chunk_key,
        })
        current = chunk_end
    
    # Filter completed chunks if resuming
    completed = get_completed_chunks(asset_id) if resume else set()
    pending_chunks = [c for c in chunks if c['key'] not in completed]
    
    log(f"Total chunks: {len(chunks)}, Pending: {len(pending_chunks)}", "INFO")
    log("=" * 60)
    
    # Scrape with persistent browser
    total_tweets = 0
    total_inserted = 0
    failed_chunks = []
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=['--disable-blink-features=AutomationControlled', '--no-sandbox']
        )
        
        context = browser.new_context(
            viewport={'width': 1280, 'height': 900},
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        
        for i, chunk in enumerate(pending_chunks, 1):
            log(f"\nChunk {i}/{len(pending_chunks)}: {chunk['since']} to {chunk['until']}", "INFO")
            
            tweets, success = scrape_chunk_with_retry(
                context=context,
                username=username,
                since=chunk['since'],
                until=chunk['until'],
                keyword=effective_keyword,
            )
            
            if success:
                # Filter pre-launch tweets
                tweets = [t for t in tweets if t['timestamp'] >= launch_date]
                
                if tweets:
                    inserted = insert_tweets(conn, asset_id, tweets)
                    total_tweets += len(tweets)
                    total_inserted += inserted
                    log(f"Found {len(tweets)} tweets, inserted {inserted} new", "OK")
                else:
                    log("No tweets in this chunk", "INFO")
                
                mark_chunk_complete(asset_id, chunk['key'])
            else:
                failed_chunks.append(chunk['key'])
                log(f"Chunk failed: {chunk['key']}", "ERROR")
            
            # Delay before next chunk
            if i < len(pending_chunks):
                wait_random(MIN_CHUNK_DELAY, MAX_CHUNK_DELAY, "before next chunk")
        
        browser.close()
    
    # Summary
    log("\n" + "=" * 60)
    log("SUMMARY", "INFO")
    log(f"  Chunks processed: {len(pending_chunks)}", "INFO")
    log(f"  Chunks failed: {len(failed_chunks)}", "INFO")
    log(f"  Tweets found: {total_tweets}", "INFO")
    log(f"  Tweets inserted: {total_inserted}", "INFO")
    
    if failed_chunks:
        log(f"  Failed chunks: {failed_chunks}", "WARN")
    
    conn.close()
    
    return {
        'status': 'success' if not failed_chunks else 'partial',
        'asset': asset_id,
        'username': username,
        'date_range': f"{since_dt.date()} to {until_dt.date()}",
        'chunks_total': len(chunks),
        'chunks_pending': len(pending_chunks),
        'chunks_failed': len(failed_chunks),
        'tweets_found': total_tweets,
        'tweets_inserted': total_inserted,
        'failed_chunks': failed_chunks,
    }


# =============================================================================
# PARALLEL SCRAPING
# =============================================================================

def db_writer_thread(write_queue: Queue, conn, asset_id: str, launch_date: datetime, stats: Dict):
    """
    Consumer thread that writes tweets to DB.
    Runs until it receives None as shutdown signal.
    """
    while True:
        task = write_queue.get()
        if task is None:  # Shutdown signal
            write_queue.task_done()
            break

        tweets, chunk_key = task

        try:
            # Filter pre-launch tweets
            tweets = [t for t in tweets if t['timestamp'] >= launch_date]

            if tweets:
                inserted = insert_tweets(conn, asset_id, tweets)
                stats['tweets_found'] += len(tweets)
                stats['tweets_inserted'] += inserted
                log(f"[Writer] Saved {inserted} tweets from chunk {chunk_key}", "OK")

            mark_chunk_complete(asset_id, chunk_key)
            stats['chunks_done'] += 1

        except Exception as e:
            log(f"[Writer] Error saving chunk {chunk_key}: {e}", "ERROR")
            stats['chunks_failed'] += 1

        write_queue.task_done()


def scraper_worker(
    worker_id: int,
    chunks: List[Dict],
    username: str,
    keyword: Optional[str],
    write_queue: Queue,
    headless: bool = False,
):
    """
    Worker thread that scrapes assigned chunks and pushes results to write queue.
    """
    instance = NITTER_INSTANCES[worker_id % len(NITTER_INSTANCES)]
    log(f"[Worker {worker_id}] Starting with {len(chunks)} chunks via {instance.split('//')[1]}", "INFO")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=headless,
                args=['--disable-blink-features=AutomationControlled', '--no-sandbox']
            )

            context = browser.new_context(
                viewport={'width': 1280, 'height': 900},
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )

            for i, chunk in enumerate(chunks):
                log(f"[Worker {worker_id}] Chunk {i+1}/{len(chunks)}: {chunk['since']} to {chunk['until']}", "INFO")

                tweets, success = scrape_chunk_with_retry(
                    context=context,
                    username=username,
                    since=chunk['since'],
                    until=chunk['until'],
                    keyword=keyword,
                )

                if success:
                    write_queue.put((tweets, chunk['key']))
                else:
                    log(f"[Worker {worker_id}] Chunk failed: {chunk['key']}", "ERROR")
                    # Still mark as needing retry - don't put in queue
                    pass

                # Delay before next chunk
                if i < len(chunks) - 1:
                    wait_random(MIN_CHUNK_DELAY, MAX_CHUNK_DELAY, f"Worker {worker_id} waiting")

            browser.close()

    except Exception as e:
        log(f"[Worker {worker_id}] Fatal error: {e}", "ERROR")

    log(f"[Worker {worker_id}] Finished", "OK")


def scrape_asset_parallel(
    asset_id: str,
    since: Optional[str] = None,
    until: Optional[str] = None,
    keyword: Optional[str] = None,
    full: bool = False,
    resume: bool = True,
    chunk_days: int = DEFAULT_CHUNK_DAYS,
    headless: bool = False,
    num_workers: int = 2,
) -> Dict[str, Any]:
    """
    Parallel version of scrape_asset.
    Uses multiple browser instances to scrape different chunks simultaneously.

    Args:
        num_workers: Number of parallel browser instances (default: 2)
        (other args same as scrape_asset)
    """
    if not PLAYWRIGHT_AVAILABLE:
        return {'status': 'error', 'reason': 'Playwright not installed'}

    # Load asset config
    conn = get_connection()
    init_schema(conn)
    load_assets_from_json(conn)

    asset = get_asset(conn, asset_id)
    if not asset:
        conn.close()
        return {'status': 'error', 'reason': f"Asset '{asset_id}' not found"}

    username = asset['founder']
    launch_date = asset['launch_date']
    effective_keyword = keyword if keyword is not None else asset.get('keyword_filter')

    if isinstance(launch_date, str):
        launch_date = datetime.fromisoformat(launch_date.replace('Z', '+00:00'))
    if launch_date.tzinfo is None:
        launch_date = launch_date.replace(tzinfo=timezone.utc)

    log(f"Asset: {asset['name']} (@{username})", "INFO")
    log(f"Launch: {launch_date.date()}", "INFO")
    log(f"Parallel workers: {num_workers}", "INFO")
    if effective_keyword:
        log(f"Keyword filter: \"{effective_keyword}\"", "INFO")

    # Determine date range (same logic as scrape_asset)
    if since and until:
        since_dt = datetime.strptime(since, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        until_dt = datetime.strptime(until, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    elif full:
        since_dt = launch_date
        until_dt = datetime.now(timezone.utc)
    else:
        oldest = conn.execute("""
            SELECT MIN(timestamp) FROM tweets WHERE asset_id = ?
        """, [asset_id]).fetchone()[0]

        if oldest:
            since_dt = launch_date
            until_dt = oldest if isinstance(oldest, datetime) else datetime.fromisoformat(str(oldest))
            if until_dt.tzinfo is None:
                until_dt = until_dt.replace(tzinfo=timezone.utc)
        else:
            since_dt = launch_date
            until_dt = datetime.now(timezone.utc)

    # Clamp dates
    if since_dt < launch_date:
        since_dt = launch_date
    now = datetime.now(timezone.utc)
    if until_dt > now:
        until_dt = now

    if since_dt >= until_dt:
        log("No date range to scrape", "INFO")
        conn.close()
        return {'status': 'skipped', 'reason': 'No date range'}

    # Generate chunks
    chunks = []
    current = since_dt
    while current < until_dt:
        chunk_end = min(current + timedelta(days=chunk_days), until_dt)
        chunk_key = f"{current.strftime('%Y-%m-%d')}_{chunk_end.strftime('%Y-%m-%d')}"
        chunks.append({
            'since': current.strftime('%Y-%m-%d'),
            'until': chunk_end.strftime('%Y-%m-%d'),
            'key': chunk_key,
        })
        current = chunk_end

    # Filter completed chunks
    completed = get_completed_chunks(asset_id) if resume else set()
    pending_chunks = [c for c in chunks if c['key'] not in completed]

    log(f"Total chunks: {len(chunks)}, Pending: {len(pending_chunks)}", "INFO")

    if not pending_chunks:
        log("All chunks already completed!", "OK")
        conn.close()
        return {'status': 'success', 'reason': 'All chunks completed'}

    log("=" * 60)

    # Divide chunks among workers (interleaved for even distribution)
    worker_chunks = [[] for _ in range(num_workers)]
    for i, chunk in enumerate(pending_chunks):
        worker_chunks[i % num_workers].append(chunk)

    # Shared stats
    stats = {
        'tweets_found': 0,
        'tweets_inserted': 0,
        'chunks_done': 0,
        'chunks_failed': 0,
    }

    # Create write queue and start DB writer
    write_queue = Queue()
    writer = threading.Thread(
        target=db_writer_thread,
        args=(write_queue, conn, asset_id, launch_date, stats)
    )
    writer.start()

    # Start scraper workers
    workers = []
    for i in range(num_workers):
        if worker_chunks[i]:  # Only start if there are chunks
            t = threading.Thread(
                target=scraper_worker,
                args=(i, worker_chunks[i], username, effective_keyword, write_queue, headless)
            )
            workers.append(t)
            t.start()

    # Wait for all scrapers to finish
    for t in workers:
        t.join()

    # Shutdown writer
    write_queue.put(None)
    writer.join()

    # Summary
    log("\n" + "=" * 60)
    log("PARALLEL SCRAPE SUMMARY", "INFO")
    log(f"  Workers: {num_workers}", "INFO")
    log(f"  Chunks processed: {stats['chunks_done']}", "INFO")
    log(f"  Chunks failed: {stats['chunks_failed']}", "INFO")
    log(f"  Tweets found: {stats['tweets_found']}", "INFO")
    log(f"  Tweets inserted: {stats['tweets_inserted']}", "INFO")

    conn.close()

    return {
        'status': 'success' if stats['chunks_failed'] == 0 else 'partial',
        'asset': asset_id,
        'username': username,
        'date_range': f"{since_dt.date()} to {until_dt.date()}",
        'chunks_total': len(chunks),
        'chunks_pending': len(pending_chunks),
        'chunks_done': stats['chunks_done'],
        'chunks_failed': stats['chunks_failed'],
        'tweets_found': stats['tweets_found'],
        'tweets_inserted': stats['tweets_inserted'],
        'workers': num_workers,
    }


# =============================================================================
# KEYWORD SEARCH MODE
# =============================================================================
#
# This mode is for SPARSE TWEETERS - people who rarely tweet about a topic.
# Instead of fetching their entire timeline and filtering, we search Nitter
# directly for tweets matching the keyword.
#
# Example: Sam Altman tweeted about Worldcoin only 13 times in 4 years.
#          Using --keyword-search gets those 13 tweets in seconds.
#          Using --full would fetch thousands of tweets to find 13.
#
# =============================================================================

def scrape_keyword_search(
    asset_id: str,
    keyword: Optional[str] = None,
    since: Optional[str] = None,
    until: Optional[str] = None,
    headless: bool = False,
    max_pages: int = 20,
) -> Dict[str, Any]:
    """
    Keyword search mode for sparse tweeters.

    Searches Nitter directly: /{username}/search?q={keyword}
    Optionally filters by date range with &since= and &until= parameters.

    USE THIS WHEN:
      - Founder/adopter rarely tweets about the token (< 50 tweets ever)
      - You want to quickly count matching tweets before a full scrape
      - X API is rate-limited
      - You need to backfill a specific date range (use --since/--until)

    Args:
        asset_id: Asset ID from assets.json
        keyword: Override keyword (uses asset's keyword_filter if None)
        headless: Run browser headless (not recommended - Cloudflare blocks)
        max_pages: Maximum pages to paginate (safety limit)

    Returns:
        Summary dict with status, tweet count, and tweets
    """
    if not PLAYWRIGHT_AVAILABLE:
        return {'status': 'error', 'reason': 'Playwright not installed'}

    # Load asset config
    conn = get_connection()
    init_schema(conn)
    load_assets_from_json(conn)

    asset = get_asset(conn, asset_id)
    if not asset:
        conn.close()
        return {'status': 'error', 'reason': f"Asset '{asset_id}' not found"}

    username = asset['founder']
    launch_date = asset['launch_date']
    effective_keyword = keyword if keyword else asset.get('keyword_filter')

    if not effective_keyword:
        conn.close()
        return {'status': 'error', 'reason': 'No keyword specified and asset has no keyword_filter'}

    # Handle multiple keywords - use first one for search, others for validation
    keywords = [k.strip() for k in effective_keyword.split(',')]
    search_keyword = keywords[0]  # Primary keyword for Nitter search

    if isinstance(launch_date, str):
        launch_date = datetime.fromisoformat(launch_date.replace('Z', '+00:00'))
    if launch_date.tzinfo is None:
        launch_date = launch_date.replace(tzinfo=timezone.utc)

    log(f"Asset: {asset['name']} (@{username})", "INFO")
    log(f"Keyword search: \"{search_keyword}\"", "INFO")
    log(f"Mode: KEYWORD SEARCH (for sparse tweeters)", "INFO")
    log("=" * 60)

    tweets = []
    seen_ids = set()

    from urllib.parse import quote_plus

    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=['--disable-blink-features=AutomationControlled', '--no-sandbox']
        )

        context = browser.new_context(
            viewport={'width': 1280, 'height': 900},
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )

        url = f"{NITTER_INSTANCES[0]}/{username}/search?f=tweets&q={quote_plus(search_keyword)}"
        # Add date range if specified
        if since:
            url += f"&since={since}"
        if until:
            url += f"&until={until}"
        log(f"Fetching: {url}", "INFO")
        if since or until:
            log(f"Date range: {since or 'start'} to {until or 'now'}", "INFO")

        page = context.new_page()

        try:
            page.goto(url, timeout=PAGE_LOAD_TIMEOUT)
            time.sleep(5)  # Wait for Cloudflare

            if not handle_cloudflare(page):
                browser.close()
                conn.close()
                return {'status': 'error', 'reason': 'Cloudflare challenge failed'}

            page.wait_for_selector('.timeline-item, .error-panel', timeout=TWEET_WAIT_TIMEOUT)

            # Check for error
            error = page.query_selector('.error-panel')
            if error:
                error_text = error.inner_text()
                if "No items found" in error_text:
                    log("No tweets found matching keyword", "INFO")
                    browser.close()
                    conn.close()
                    return {'status': 'success', 'tweets_found': 0, 'tweets_inserted': 0}

            # Paginate through all results
            page_num = 0
            empty_pages = 0

            while page_num < max_pages:
                page_num += 1

                # Extract tweets from current page
                new_tweets = extract_tweets_from_page(page, target_username=username)
                added = 0

                for t in new_tweets:
                    if t['id'] not in seen_ids:
                        # Skip pre-launch tweets
                        if t['timestamp'] and t['timestamp'] < launch_date:
                            continue
                        seen_ids.add(t['id'])
                        tweets.append(t)
                        added += 1

                log(f"  Page {page_num}: +{added} tweets (total: {len(tweets)})", "DEBUG")

                if added == 0:
                    empty_pages += 1
                    if empty_pages >= 3:
                        log("  Stopping after 3 empty pages", "DEBUG")
                        break
                else:
                    empty_pages = 0

                # Check for more
                load_more = page.query_selector('.show-more a')
                if not load_more:
                    break

                try:
                    load_more.click()
                    wait_random(MIN_PAGE_DELAY, MAX_PAGE_DELAY, "pagination")
                    page.wait_for_selector('.timeline-item', timeout=10000)
                except Exception as e:
                    log(f"Pagination ended: {e}", "DEBUG")
                    break

        except PlaywrightTimeout:
            log("Page load timeout", "WARN")
            browser.close()
            conn.close()
            return {'status': 'error', 'reason': 'Timeout loading search results'}
        except Exception as e:
            log(f"Error: {e}", "ERROR")
            browser.close()
            conn.close()
            return {'status': 'error', 'reason': str(e)}

        browser.close()

    # Insert tweets
    inserted = 0
    if tweets:
        inserted = insert_tweets(conn, asset_id, tweets)
        log(f"Inserted {inserted} tweets", "OK")

    conn.close()

    # Summary
    log("\n" + "=" * 60)
    log("KEYWORD SEARCH SUMMARY", "INFO")
    log(f"  Keyword: \"{search_keyword}\"", "INFO")
    log(f"  Tweets found: {len(tweets)}", "INFO")
    log(f"  Tweets inserted: {inserted}", "INFO")

    return {
        'status': 'success',
        'mode': 'keyword_search',
        'asset': asset_id,
        'username': username,
        'keyword': search_keyword,
        'tweets_found': len(tweets),
        'tweets_inserted': inserted,
    }


# =============================================================================
# PARALLEL KEYWORD SEARCH
# =============================================================================

def keyword_search_worker(
    worker_id: int,
    chunks: List[Dict],
    username: str,
    keyword: str,
    launch_date: datetime,
    write_queue: Queue,
    headless: bool = False,
):
    """
    Worker thread for parallel keyword search.
    Each chunk is a date-bounded keyword search: /{username}/search?q={keyword}&since={}&until={}
    """
    from urllib.parse import quote_plus

    instance = NITTER_INSTANCES[worker_id % len(NITTER_INSTANCES)]
    log(f"[Worker {worker_id}] Starting with {len(chunks)} chunks via {instance.split('//')[1]}", "INFO")

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(
                headless=headless,
                args=['--disable-blink-features=AutomationControlled', '--no-sandbox']
            )

            context = browser.new_context(
                viewport={'width': 1280, 'height': 900},
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
            )

            for i, chunk in enumerate(chunks):
                since = chunk['since']
                until = chunk['until']

                # Build date-bounded keyword search URL
                url = f"{instance}/{username}/search?f=tweets&q={quote_plus(keyword)}&since={since}&until={until}"
                log(f"[Worker {worker_id}] Chunk {i+1}/{len(chunks)}: {since} to {until}", "INFO")

                page = context.new_page()
                tweets = []
                seen_ids = set()

                try:
                    page.goto(url, timeout=PAGE_LOAD_TIMEOUT)
                    time.sleep(5)  # Wait for Cloudflare

                    if not handle_cloudflare(page):
                        log(f"[Worker {worker_id}] Cloudflare failed for chunk {i+1}", "WARN")
                        page.close()
                        continue

                    page.wait_for_selector('.timeline-item, .error-panel', timeout=TWEET_WAIT_TIMEOUT)

                    # Check for error/no results
                    error = page.query_selector('.error-panel')
                    if error:
                        error_text = error.inner_text()
                        if "No items found" in error_text:
                            log(f"[Worker {worker_id}] No tweets in chunk {i+1}", "DEBUG")
                            page.close()
                            write_queue.put(([], chunk['key']))
                            continue

                    # Paginate through results for this chunk
                    page_num = 0
                    max_pages = 20

                    while page_num < max_pages:
                        page_num += 1

                        new_tweets = extract_tweets_from_page(page, target_username=username)
                        added = 0

                        for t in new_tweets:
                            if t['id'] not in seen_ids:
                                if t['timestamp'] and t['timestamp'] < launch_date:
                                    continue
                                seen_ids.add(t['id'])
                                tweets.append(t)
                                added += 1

                        if added == 0:
                            break

                        load_more = page.query_selector('.show-more a')
                        if not load_more:
                            break

                        try:
                            load_more.click()
                            wait_random(MIN_PAGE_DELAY, MAX_PAGE_DELAY, "pagination")
                            page.wait_for_selector('.timeline-item', timeout=10000)
                        except Exception:
                            break

                    log(f"[Worker {worker_id}] Found {len(tweets)} tweets in chunk {i+1}", "OK")
                    write_queue.put((tweets, chunk['key']))

                except PlaywrightTimeout:
                    log(f"[Worker {worker_id}] Timeout for chunk {i+1}", "WARN")
                except Exception as e:
                    log(f"[Worker {worker_id}] Error in chunk {i+1}: {e}", "ERROR")
                finally:
                    page.close()

                # Delay before next chunk
                if i < len(chunks) - 1:
                    wait_random(MIN_CHUNK_DELAY, MAX_CHUNK_DELAY, f"Worker {worker_id} waiting")

            browser.close()

    except Exception as e:
        log(f"[Worker {worker_id}] Fatal error: {e}", "ERROR")

    log(f"[Worker {worker_id}] Finished", "OK")


def scrape_keyword_search_parallel(
    asset_id: str,
    keyword: Optional[str] = None,
    first_tweet_date: str = None,
    last_tweet_date: Optional[str] = None,
    num_workers: int = 3,
    headless: bool = False,
) -> Dict[str, Any]:
    """
    Parallel keyword search with smart date chunking.

    Combines keyword search efficiency with date-bounded parallelization.
    3 workers scrape different date ranges simultaneously.

    Chunking strategy:
      - If last_tweet_date provided: active period split into small chunks,
        sparse tail (last_tweet to now) is single chunk
      - If only first_tweet_date: split entire period evenly among workers

    Args:
        asset_id: Asset ID from assets.json
        keyword: Override keyword (uses asset's keyword_filter if None)
        first_tweet_date: First known tweet date YYYY-MM-DD
        last_tweet_date: Date activity concentration ends YYYY-MM-DD (optional)
        num_workers: Number of parallel browser instances (default: 3)
        headless: Run browser headless

    Returns:
        Summary dict with status, tweet count, etc.
    """
    if not PLAYWRIGHT_AVAILABLE:
        return {'status': 'error', 'reason': 'Playwright not installed'}

    if not first_tweet_date:
        return {'status': 'error', 'reason': 'first_tweet_date required for parallel keyword search'}

    # Load asset config
    conn = get_connection()
    init_schema(conn)
    load_assets_from_json(conn)

    asset = get_asset(conn, asset_id)
    if not asset:
        conn.close()
        return {'status': 'error', 'reason': f"Asset '{asset_id}' not found"}

    username = asset['founder']
    launch_date = asset['launch_date']
    effective_keyword = keyword if keyword else asset.get('keyword_filter')

    if not effective_keyword:
        conn.close()
        return {'status': 'error', 'reason': 'No keyword specified and asset has no keyword_filter'}

    # Handle multiple keywords - use first one for search
    keywords = [k.strip() for k in effective_keyword.split(',')]
    search_keyword = keywords[0]

    if isinstance(launch_date, str):
        launch_date = datetime.fromisoformat(launch_date.replace('Z', '+00:00'))
    if launch_date.tzinfo is None:
        launch_date = launch_date.replace(tzinfo=timezone.utc)

    # Parse dates
    first_dt = datetime.strptime(first_tweet_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    now = datetime.now(timezone.utc)

    log(f"Asset: {asset['name']} (@{username})", "INFO")
    log(f"Keyword: \"{search_keyword}\"", "INFO")
    log(f"Mode: PARALLEL KEYWORD SEARCH ({num_workers} workers)", "INFO")
    log(f"First tweet: {first_tweet_date}", "INFO")

    # Calculate smart chunks
    chunks = []

    if last_tweet_date:
        last_dt = datetime.strptime(last_tweet_date, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        active_days = (last_dt - first_dt).days
        chunk_days = max(3, active_days // num_workers)  # At least 3 days per chunk

        log(f"Active period: {first_tweet_date} to {last_tweet_date} ({active_days} days)", "INFO")
        log(f"Chunk size: {chunk_days} days", "INFO")

        # Create chunks for active period
        current = first_dt
        while current < last_dt:
            end = min(current + timedelta(days=chunk_days), last_dt)
            chunks.append({
                'since': current.strftime('%Y-%m-%d'),
                'until': end.strftime('%Y-%m-%d'),
                'key': f"{current.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}",
            })
            current = end

        # Add sparse tail as single chunk (from last_tweet_date to now)
        chunks.append({
            'since': last_dt.strftime('%Y-%m-%d'),
            'until': now.strftime('%Y-%m-%d'),
            'key': f"{last_dt.strftime('%Y-%m-%d')}_{now.strftime('%Y-%m-%d')}_sparse",
        })
        log(f"Sparse tail: {last_tweet_date} to now (single chunk)", "INFO")

    else:
        # No last_tweet_date - split entire period evenly
        total_days = (now - first_dt).days
        chunk_days = max(7, total_days // num_workers)

        log(f"Total period: {first_tweet_date} to now ({total_days} days)", "INFO")
        log(f"Chunk size: {chunk_days} days", "INFO")

        current = first_dt
        while current < now:
            end = min(current + timedelta(days=chunk_days), now)
            chunks.append({
                'since': current.strftime('%Y-%m-%d'),
                'until': end.strftime('%Y-%m-%d'),
                'key': f"{current.strftime('%Y-%m-%d')}_{end.strftime('%Y-%m-%d')}",
            })
            current = end

    log(f"Total chunks: {len(chunks)}", "INFO")
    log("=" * 60)

    # Distribute chunks to workers (round-robin)
    worker_chunks = [[] for _ in range(num_workers)]
    for i, chunk in enumerate(chunks):
        worker_chunks[i % num_workers].append(chunk)

    # Setup write queue and stats
    write_queue = Queue()
    stats = {
        'tweets_found': 0,
        'tweets_inserted': 0,
        'chunks_done': 0,
        'chunks_failed': 0,
    }

    # Start DB writer thread
    writer_thread = threading.Thread(
        target=db_writer_thread,
        args=(write_queue, conn, asset_id, launch_date, stats),
        daemon=True,
    )
    writer_thread.start()

    # Start worker threads
    workers = []
    for worker_id in range(num_workers):
        if not worker_chunks[worker_id]:
            continue
        t = threading.Thread(
            target=keyword_search_worker,
            args=(worker_id, worker_chunks[worker_id], username, search_keyword, launch_date, write_queue, headless),
        )
        t.start()
        workers.append(t)

    # Wait for all workers to finish
    for t in workers:
        t.join()

    # Signal writer to stop and wait
    write_queue.put(None)
    writer_thread.join()

    conn.close()

    # Summary
    log("\n" + "=" * 60)
    log("PARALLEL KEYWORD SEARCH SUMMARY", "INFO")
    log(f"  Keyword: \"{search_keyword}\"", "INFO")
    log(f"  Workers: {len(workers)}", "INFO")
    log(f"  Chunks processed: {stats['chunks_done']}", "INFO")
    log(f"  Tweets found: {stats['tweets_found']}", "INFO")
    log(f"  Tweets inserted: {stats['tweets_inserted']}", "INFO")

    return {
        'status': 'success',
        'mode': 'parallel_keyword_search',
        'asset': asset_id,
        'username': username,
        'keyword': search_keyword,
        'workers': len(workers),
        'chunks': len(chunks),
        'tweets_found': stats['tweets_found'],
        'tweets_inserted': stats['tweets_inserted'],
    }


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Nitter Scraper v2 - Robust tweet backfill',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Full backfill from launch to now
  python nitter_scraper.py --asset useless --full

  # Specific date range
  python nitter_scraper.py --asset useless --since 2025-06-01 --until 2025-07-01

  # Resume after interruption (skips completed chunks)
  python nitter_scraper.py --asset useless --full

  # Start fresh (clear progress)
  python nitter_scraper.py --asset useless --full --no-resume

  # Non-headless mode (shows browser)
  python nitter_scraper.py --asset useless --full --no-headless

  # KEYWORD SEARCH - for sparse tweeters (< 50 tweets about topic)
  # Use this when the founder rarely tweets about the token.
  # Example: Sam Altman tweeted about Worldcoin only 13 times in 4 years.
  python nitter_scraper.py --asset wld --keyword-search
"""
    )
    
    parser.add_argument('--asset', '-a', required=True, help='Asset ID from assets.json')
    parser.add_argument('--since', '-s', help='Start date YYYY-MM-DD')
    parser.add_argument('--until', '-u', help='End date YYYY-MM-DD')
    parser.add_argument('--keyword', '-k', help='Keyword filter')
    parser.add_argument('--full', '-f', action='store_true', help='Full scrape from launch to now')
    parser.add_argument('--no-resume', action='store_true', help='Start fresh, ignore progress')
    parser.add_argument('--chunk-days', type=int, default=DEFAULT_CHUNK_DAYS, help=f'Days per chunk (default: {DEFAULT_CHUNK_DAYS})')
    # HEADLESS MODE NOTE:
    # Headless browsers are often detected and blocked by Nitter/Cloudflare.
    # --no-headless (showing the browser) is STRONGLY RECOMMENDED.
    # Only use --headless for automated CI/CD where display is unavailable.
    parser.add_argument('--headless', action='store_true', help='Run headless (more likely to fail)')
    parser.add_argument('--no-headless', action='store_true', help='Show browser (recommended)')

    # PARALLEL MODE NOTE:
    # Tested with up to 3 workers on nitter.net with no rate limiting.
    # Each worker runs its own browser instance with its own session.
    # Chunks are distributed round-robin for even load.
    # Example: --parallel 3 gives ~3x speedup for large backfills.
    parser.add_argument('--parallel', '-p', type=int, default=1, help='Number of parallel workers (default: 1, max tested: 3)')
    parser.add_argument('--clear-progress', action='store_true', help='Clear progress for asset and exit')

    # KEYWORD SEARCH MODE:
    # For SPARSE TWEETERS who rarely tweet about the token.
    # Instead of fetching their entire timeline, search Nitter directly.
    # Example: Sam Altman tweeted about Worldcoin only 13 times in 4 years.
    #          --keyword-search finds those 13 instantly vs fetching thousands.
    # DO NOT USE for frequent tweeters - use --full instead.
    parser.add_argument('--keyword-search', action='store_true',
        help='Keyword search mode for sparse tweeters (< 50 tweets about topic)')

    # PARALLEL KEYWORD SEARCH - combines keyword efficiency with date-bounded parallelization
    # When --keyword-search is combined with --first-tweet-date (and optionally --last-tweet-date),
    # the scraper runs parallel workers with date-bounded keyword searches.
    # Example: With 3 workers and dates May 3 - May 21, each worker handles ~6 days
    parser.add_argument('--first-tweet-date',
        help='First known tweet date (YYYY-MM-DD). Enables parallel keyword search.')
    parser.add_argument('--last-tweet-date',
        help='Date activity concentration ends (YYYY-MM-DD). Sparse tail after this is single chunk.')

    args = parser.parse_args()

    if args.clear_progress:
        clear_progress(args.asset)
        print(f"Cleared progress for {args.asset}")
        return

    # Validate mode selection
    if not args.keyword_search and not (args.since and args.until) and not args.full:
        print("ERROR: Must specify --since/--until, --full, or --keyword-search")
        parser.print_help()
        sys.exit(1)
    
    if not PLAYWRIGHT_AVAILABLE:
        print("ERROR: Playwright not installed. Run:")
        print("  pip install playwright")
        print("  playwright install chromium")
        sys.exit(1)
    
    # Determine headless mode
    headless = args.headless and not args.no_headless

    # Choose scraping mode
    if args.keyword_search and args.first_tweet_date:
        # PARALLEL KEYWORD SEARCH - best of both worlds
        # Combines keyword efficiency with date-bounded parallelization
        result = scrape_keyword_search_parallel(
            asset_id=args.asset,
            keyword=args.keyword,
            first_tweet_date=args.first_tweet_date,
            last_tweet_date=args.last_tweet_date,
            num_workers=args.parallel if args.parallel > 1 else 3,
            headless=headless,
        )
    elif args.keyword_search:
        # SIMPLE KEYWORD SEARCH - single thread
        # Supports optional --since/--until for date range filtering
        result = scrape_keyword_search(
            asset_id=args.asset,
            keyword=args.keyword,
            since=args.since,
            until=args.until,
            headless=headless,
        )
    elif args.parallel > 1:
        # Parallel date-range scraping
        result = scrape_asset_parallel(
            asset_id=args.asset,
            since=args.since,
            until=args.until,
            keyword=args.keyword,
            full=args.full,
            resume=not args.no_resume,
            chunk_days=args.chunk_days,
            headless=headless,
            num_workers=args.parallel,
        )
    else:
        # Sequential date-range scraping
        result = scrape_asset(
            asset_id=args.asset,
            since=args.since,
            until=args.until,
            keyword=args.keyword,
            full=args.full,
            resume=not args.no_resume,
            chunk_days=args.chunk_days,
            headless=headless,
        )
    
    print(f"\n[RESULT] {json.dumps(result, indent=2)}")
    
    if result['status'] == 'error':
        sys.exit(1)


if __name__ == '__main__':
    main()

