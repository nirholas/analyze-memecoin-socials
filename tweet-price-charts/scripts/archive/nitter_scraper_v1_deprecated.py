"""
======================================================================
DEPRECATED - This is the v1 scraper that had reliability issues.
Use nitter_scraper.py (v2) instead.

DEPRECATION REASON:
- 5s inter-chunk delay was too fast → triggered rate limiting
- No retry logic → entire runs would fail on transient errors
- Single Nitter instance → single point of failure
- No progress tracking → couldn't resume from failures

See nitter_scraper.py for the improved v2 implementation.
======================================================================

Nitter Scraper - Supplement Twitter API with Nitter for historical data.

This scraper bypasses API rate limits by using a real browser via Playwright.
It's designed to work seamlessly with the existing tweet pipeline.

ARCHITECTURE:
- Uses date-range URL params for efficient targeted scraping
- Outputs data in EXACT format expected by db.insert_tweets()
- Integrates with ingestion_state for resumability
- Designed as API rate-limit fallback + ad-hoc backfill tool

Usage:
    # Backfill a specific date range
    python nitter_scraper.py --asset useless --since 2025-05-23 --until 2025-05-30
    
    # Auto-detect and fill gaps (uses ingestion_state)
    python nitter_scraper.py --asset useless --backfill
    
    # Full scrape for new asset
    python nitter_scraper.py --asset pump --full
"""
import argparse
import json
import re
import sys
import time
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
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False


# =============================================================================
# CONFIGURATION
# =============================================================================

NITTER_INSTANCE = "https://nitter.net"
DEFAULT_CHUNK_DAYS = 7  # Scrape in 7-day chunks for efficiency
MAX_TWEETS_PER_CHUNK = 500  # Safety limit per chunk
PAGE_LOAD_TIMEOUT = 30000  # 30 seconds
TWEET_WAIT_TIMEOUT = 15000  # 15 seconds
INTER_PAGE_DELAY = 2.0  # Seconds between pagination
INTER_CHUNK_DELAY = 5.0  # Seconds between date chunks


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
    
    # Handle K/M suffixes
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

def scrape_date_range(
    username: str,
    since: str,
    until: str,
    keyword: Optional[str] = None,
    max_tweets: int = MAX_TWEETS_PER_CHUNK,
    headless: bool = False,
    verbose: bool = True
) -> Tuple[List[Dict[str, Any]], bool]:
    """
    Scrape tweets for a specific date range.
    
    Args:
        username: Twitter username (without @)
        since: Start date YYYY-MM-DD
        until: End date YYYY-MM-DD (exclusive)
        keyword: Optional keyword to filter tweets (e.g., "useless" for coin mentions)
        max_tweets: Safety limit
        headless: Run headless (may trigger bot detection)
        verbose: Print progress
    
    Returns:
        (list of tweets, success boolean)
    """
    if not PLAYWRIGHT_AVAILABLE:
        print("[NITTER] ERROR: Playwright not installed. Run:")
        print("  pip install playwright && playwright install chromium")
        return [], False
    
    tweets = []
    seen_ids = set()
    
    # Build URL with date params and optional keyword filter
    # URL encode the keyword for the search query
    from urllib.parse import quote_plus
    query = quote_plus(keyword) if keyword else ""
    url = f"{NITTER_INSTANCE}/{username}/search?f=tweets&q={query}&since={since}&until={until}"
    
    if verbose:
        print(f"[NITTER] Scraping {username}: {since} to {until}")
        print(f"[NITTER] URL: {url}")
    
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=headless,
            args=['--disable-blink-features=AutomationControlled', '--no-sandbox']
        )
        
        context = browser.new_context(
            viewport={'width': 1280, 'height': 900},
            user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        
        page = context.new_page()
        success = False
        
        try:
            page.goto(url, timeout=PAGE_LOAD_TIMEOUT)
            
            # Wait for timeline or error
            try:
                page.wait_for_selector('.timeline-item, .error-panel', timeout=TWEET_WAIT_TIMEOUT)
            except PlaywrightTimeout:
                if verbose:
                    print(f"[NITTER] No tweets found for date range")
                browser.close()
                return [], True  # Empty is still success
            
            # Check for error
            error = page.query_selector('.error-panel')
            if error:
                error_text = error.inner_text()
                if verbose:
                    print(f"[NITTER] Error: {error_text}")
                browser.close()
                return [], "No items found" in error_text  # Empty search is OK
            
            pages_scraped = 0
            max_pages = 20  # Safety limit
            
            while len(tweets) < max_tweets and pages_scraped < max_pages:
                pages_scraped += 1
                
                tweet_elements = page.query_selector_all('.timeline-item')
                new_count = 0
                
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
                        
                        # Skip retweets (to match X API behavior)
                        if el.query_selector('.retweet-header'):
                            continue
                        
                        # Skip replies (to match X API behavior with exclude=replies)
                        # Nitter shows "Replying to @user" with class "replying-to"
                        if el.query_selector('.replying-to'):
                            continue
                        
                        seen_ids.add(tweet_id)
                        
                        # Get date
                        date_title = date_link.get_attribute('title') or ''
                        tweet_dt = parse_nitter_date(date_title)
                        
                        if not tweet_dt:
                            # Try to parse from relative date (skip these)
                            continue
                        
                        # Get text
                        text_el = el.query_selector('.tweet-content')
                        text = text_el.inner_text().strip() if text_el else ""
                        
                        # Get stats: comments, retweets, quotes, likes
                        # Nitter shows: 💬 replies, 🔁 retweets, ❤️ likes, 📊 impressions
                        stats = {'replies': 0, 'retweets': 0, 'likes': 0, 'impressions': 0}
                        
                        stat_elements = el.query_selector_all('.tweet-stat')
                        for stat_el in stat_elements:
                            stat_text = stat_el.inner_text().strip()
                            stat_val = parse_stat_number(stat_text)
                            
                            # Determine stat type by icon/class
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
                        
                        # Build tweet in EXACT format for db.insert_tweets()
                        tweet = {
                            'id': tweet_id,
                            'text': text,
                            'timestamp': tweet_dt,  # datetime object
                            'created_at': tweet_dt.isoformat().replace('+00:00', 'Z'),  # ISO string
                            'likes': stats['likes'],
                            'retweets': stats['retweets'],
                            'replies': stats['replies'],
                            'impressions': stats['impressions'],
                        }
                        
                        tweets.append(tweet)
                        new_count += 1
                        
                        if len(tweets) >= max_tweets:
                            break
                            
                    except Exception as e:
                        if verbose:
                            print(f"[NITTER] Error parsing tweet: {e}")
                        continue
                
                if verbose:
                    print(f"[NITTER]   Page {pages_scraped}: +{new_count} tweets (total: {len(tweets)})")
                
                # Check for pagination
                if len(tweets) >= max_tweets:
                    break
                
                show_more = page.query_selector_all('.show-more a')
                if show_more:
                    try:
                        show_more[-1].click()
                        time.sleep(INTER_PAGE_DELAY)
                        page.wait_for_selector('.timeline-item', timeout=10000)
                    except PlaywrightTimeout:
                        break
                    except Exception as e:
                        if verbose:
                            print(f"[NITTER]   Pagination error: {e}")
                        break
                else:
                    break
            
            success = True
            
        except PlaywrightTimeout:
            if verbose:
                print(f"[NITTER] Page load timeout - bot detection may have triggered")
        except Exception as e:
            if verbose:
                print(f"[NITTER] Error: {e}")
        finally:
            browser.close()
    
    return tweets, success


def scrape_tweets_chunked(
    username: str,
    since: datetime,
    until: datetime,
    keyword: Optional[str] = None,
    chunk_days: int = DEFAULT_CHUNK_DAYS,
    headless: bool = False,
    verbose: bool = True
) -> List[Dict[str, Any]]:
    """
    Scrape tweets in date chunks for efficiency and reliability.
    
    Args:
        username: Twitter username
        since: Start datetime
        until: End datetime
        keyword: Optional keyword to filter tweets (e.g., "useless" for coin mentions)
        chunk_days: Days per chunk
        headless: Run headless
        verbose: Print progress
    
    Returns:
        List of all tweets found
    """
    all_tweets = []
    current = since
    chunk_num = 0
    total_chunks = ((until - since).days // chunk_days) + 1
    
    if verbose:
        print(f"[NITTER] Scraping @{username} from {since.date()} to {until.date()}")
        if keyword:
            print(f"[NITTER] Keyword filter: \"{keyword}\"")
        print(f"[NITTER] Splitting into ~{total_chunks} chunks of {chunk_days} days")
        print("=" * 60)
    
    while current < until:
        chunk_num += 1
        chunk_end = min(current + timedelta(days=chunk_days), until)
        
        since_str = current.strftime('%Y-%m-%d')
        until_str = chunk_end.strftime('%Y-%m-%d')
        
        if verbose:
            print(f"\n[NITTER] Chunk {chunk_num}/{total_chunks}: {since_str} to {until_str}")
        
        tweets, success = scrape_date_range(
            username=username,
            since=since_str,
            until=until_str,
            keyword=keyword,
            headless=headless,
            verbose=verbose
        )
        
        if tweets:
            all_tweets.extend(tweets)
            if verbose:
                print(f"[NITTER]   ✓ Found {len(tweets)} tweets")
        elif success:
            if verbose:
                print(f"[NITTER]   ○ No tweets in this range")
        else:
            if verbose:
                print(f"[NITTER]   ✗ Failed to scrape this chunk")
        
        current = chunk_end
        
        # Delay between chunks to avoid detection
        if current < until:
            time.sleep(INTER_CHUNK_DELAY)
    
    if verbose:
        print("\n" + "=" * 60)
        print(f"[NITTER] Complete: {len(all_tweets)} total tweets scraped")
    
    return all_tweets


# =============================================================================
# DATABASE INTEGRATION
# =============================================================================

def scrape_and_save(
    asset_id: str,
    since: Optional[str] = None,
    until: Optional[str] = None,
    keyword: Optional[str] = None,
    backfill: bool = False,
    full: bool = False,
    chunk_days: int = DEFAULT_CHUNK_DAYS,
    headless: bool = False,
    verbose: bool = True
) -> Dict[str, Any]:
    """
    Scrape tweets and save directly to database.
    
    Modes:
    - Specific range: --since and --until provided
    - Backfill: Auto-detect gaps and fill from launch to oldest tweet
    - Full: Scrape from launch to now
    
    Args:
        keyword: Optional keyword filter (e.g., "useless"). 
                 If None, uses asset's keyword_filter from config if set.
    
    Returns summary dict.
    """
    conn = get_connection()
    init_schema(conn)
    load_assets_from_json(conn)
    
    asset = get_asset(conn, asset_id)
    if not asset:
        conn.close()
        return {'status': 'error', 'reason': f"Asset '{asset_id}' not found"}
    
    username = asset['founder']
    launch_date = asset['launch_date']
    
    # Use keyword from args, or fall back to asset's configured keyword_filter
    effective_keyword = keyword
    if effective_keyword is None:
        effective_keyword = asset.get('keyword_filter')
    if isinstance(launch_date, str):
        launch_date = datetime.fromisoformat(launch_date.replace('Z', '+00:00'))
    if launch_date.tzinfo is None:
        launch_date = launch_date.replace(tzinfo=timezone.utc)
    
    if verbose:
        print(f"\n[NITTER] Asset: {asset['name']} (@{username})")
        print(f"[NITTER] Launch date: {launch_date.date()}")
        if effective_keyword:
            print(f"[NITTER] Keyword filter: \"{effective_keyword}\"")
    
    # Determine date range
    if since and until:
        # Explicit range
        since_dt = datetime.strptime(since, '%Y-%m-%d').replace(tzinfo=timezone.utc)
        until_dt = datetime.strptime(until, '%Y-%m-%d').replace(tzinfo=timezone.utc)
    elif backfill:
        # Auto-detect: from launch to oldest tweet we have
        oldest = conn.execute("""
            SELECT MIN(timestamp) FROM tweets WHERE asset_id = ?
        """, [asset_id]).fetchone()[0]
        
        if oldest:
            since_dt = launch_date
            until_dt = oldest if isinstance(oldest, datetime) else datetime.fromisoformat(str(oldest))
            if until_dt.tzinfo is None:
                until_dt = until_dt.replace(tzinfo=timezone.utc)
            if verbose:
                print(f"[NITTER] Backfill mode: {since_dt.date()} to {until_dt.date()}")
        else:
            # No tweets yet, do full scrape from launch
            since_dt = launch_date
            until_dt = datetime.now(timezone.utc)
            if verbose:
                print(f"[NITTER] No existing tweets, doing full scrape")
    elif full:
        # Full: launch to now
        since_dt = launch_date
        until_dt = datetime.now(timezone.utc)
    else:
        conn.close()
        return {'status': 'error', 'reason': 'Must specify --since/--until, --backfill, or --full'}
    
    # Don't scrape before launch
    if since_dt < launch_date:
        since_dt = launch_date
    
    # Don't scrape into the future
    now = datetime.now(timezone.utc)
    if until_dt > now:
        until_dt = now
    
    if since_dt >= until_dt:
        if verbose:
            print(f"[NITTER] No date range to scrape (since >= until)")
        conn.close()
        return {'status': 'skipped', 'reason': 'No date range to scrape'}
    
    # Scrape
    tweets = scrape_tweets_chunked(
        username=username,
        since=since_dt,
        until=until_dt,
        keyword=effective_keyword,
        chunk_days=chunk_days,
        headless=headless,
        verbose=verbose
    )
    
    # Filter out any tweets before launch (safety check)
    tweets = [t for t in tweets if t['timestamp'] >= launch_date]
    
    # Insert into database
    if tweets:
        inserted = insert_tweets(conn, asset_id, tweets)
        if verbose:
            print(f"\n[NITTER] Inserted {inserted} tweets into database")
        
        # Update ingestion state
        sorted_tweets = sorted(tweets, key=lambda t: t['id'])
        newest_id = sorted_tweets[-1]['id']
        oldest_id = sorted_tweets[0]['id']
        
        # Track nitter-specific state
        update_ingestion_state(conn, asset_id, 'nitter_newest', last_id=newest_id)
        update_ingestion_state(conn, asset_id, 'nitter_oldest', last_id=oldest_id)
    else:
        inserted = 0
    
    conn.close()
    
    return {
        'status': 'success',
        'asset': asset_id,
        'username': username,
        'date_range': f"{since_dt.date()} to {until_dt.date()}",
        'tweets_found': len(tweets),
        'tweets_inserted': inserted,
    }


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description='Scrape tweets via Nitter (rate-limit fallback)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Backfill missing tweets for an asset (launch to oldest tweet)
  python nitter_scraper.py --asset useless --backfill
  
  # Scrape specific date range
  python nitter_scraper.py --asset useless --since 2025-05-23 --until 2025-05-30
  
  # Full scrape for new asset
  python nitter_scraper.py --asset pump --full
  
  # Keyword filter for non-founders (only tweets mentioning the coin)
  python nitter_scraper.py --asset useless --keyword useless --full
  
  # Non-headless mode (more reliable but shows browser)
  python nitter_scraper.py --asset useless --backfill --no-headless
"""
    )
    
    parser.add_argument('--asset', '-a', required=True, help='Asset ID from assets.json')
    parser.add_argument('--since', '-s', help='Start date YYYY-MM-DD')
    parser.add_argument('--until', '-u', help='End date YYYY-MM-DD')
    parser.add_argument('--keyword', '-k', help='Keyword filter (e.g., "useless" for coin mentions)')
    parser.add_argument('--backfill', '-b', action='store_true', help='Auto-fill gap from launch to oldest tweet')
    parser.add_argument('--full', '-f', action='store_true', help='Full scrape from launch to now')
    parser.add_argument('--chunk-days', type=int, default=DEFAULT_CHUNK_DAYS, help=f'Days per scraping chunk (default: {DEFAULT_CHUNK_DAYS})')
    parser.add_argument('--headless', action='store_true', help='Run browser in headless mode (may fail)')
    parser.add_argument('--no-headless', action='store_true', help='Show browser window (more reliable)')
    parser.add_argument('--quiet', '-q', action='store_true', help='Suppress output')
    
    args = parser.parse_args()
    
    # Validate args
    if not (args.since and args.until) and not args.backfill and not args.full:
        print("ERROR: Must specify --since/--until, --backfill, or --full")
        parser.print_help()
        sys.exit(1)
    
    if not PLAYWRIGHT_AVAILABLE:
        print("ERROR: Playwright not installed. Run:")
        print("  pip install playwright")
        print("  playwright install chromium")
        sys.exit(1)
    
    # Determine headless mode (default: headless, unless --no-headless)
    headless = not args.no_headless if args.no_headless else args.headless
    
    result = scrape_and_save(
        asset_id=args.asset,
        since=args.since,
        until=args.until,
        keyword=args.keyword,
        backfill=args.backfill,
        full=args.full,
        chunk_days=args.chunk_days,
        headless=headless,
        verbose=not args.quiet
    )
    
    if not args.quiet:
        print(f"\n[NITTER] Result: {json.dumps(result, indent=2)}")
    
    if result['status'] != 'success':
        sys.exit(1)


if __name__ == '__main__':
    main()
