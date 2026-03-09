# Adopter Asset Workflow

This document describes how to add "adopter" assets - tokens where the tracked account didn't create the coin but became a prominent promoter/adopter (e.g., @blknoiz06 with WIF).

## Adopter vs Founder Assets

| Type | Description | Example |
|------|-------------|---------|
| **Founder** | Account created the token | @a1lon9 → PUMP |
| **Adopter** | Account promoted/adopted existing token | @blknoiz06 → WIF |

Key difference: Adopter accounts have MANY tweets unrelated to the token. We must filter tweets by keyword.

---

## Current Workflow

### Step 1: Add Asset via CLI

The `add_asset.py` CLI now fully supports adopter assets:

```bash
cd scripts
python add_asset.py wif \
  --name "WIF" \
  --founder blknoiz06 \
  --founder-type adopter \
  --keyword-filter wif \
  --coingecko dogwifcoin \
  --network solana \
  --pool EP2ib6dYdEeqD8MfE2ezHCxX3kP3K2eLKkirfPm5eyMx \
  --launch-date 2023-12-01 \
  --color "#D4A76A" \
  --skip-tweets \
  --skip-prices
```

**Adopter-specific flags:**
- `--founder-type adopter`: Indicates this account promoted (not created) the token
- `--keyword-filter wif`: Only tweets containing this keyword are shown
- `--tweet-filter-note "Custom note"`: Optional custom description (auto-generated if omitted)

**Note:** Use `--skip-tweets --skip-prices` if you plan to use Nitter for historical backfill.

Alternatively, you can manually edit `scripts/assets.json`:

```json
{
  "id": "wif",
  "name": "WIF",
  "founder": "blknoiz06",
  "founder_type": "adopter",
  "network": "solana",
  "pool_address": "EP2ib6dYdEeqD8MfE2ezHCxX3kP3K2eLKkirfPm5eyMx",
  "token_mint": "EKpQGSJtjMFqKZ9KQanSqYXRcF8fBopzLHYxdM65zcjm",
  "coingecko_id": "dogwifcoin",
  "price_source": "geckoterminal",
  "backfill_source": "birdeye",
  "launch_date": "2023-12-01T00:00:00Z",
  "color": "#D4A76A",
  "enabled": true,
  "logo": "/logos/wif.png",
  "keyword_filter": "wif",
  "tweet_filter_note": "Only tweets mentioning $WIF"
}
```

### Step 2: Fetch Price History

For older assets (>90 days), use Birdeye backfill:

```bash
cd scripts
python fetch_prices.py --asset wif --backfill
```

**What happens:**
- Skips 1m data for assets >90 days old (would take hours)
- Skips 15m data for assets >365 days old
- Fetches 1d first (most useful), then 1h
- Progressive saving - can interrupt and resume
- Real-time progress output

### Step 3: Fetch Recent Tweets (X API First)

**Always run X API first** - it's more reliable and sets up watermarks for ongoing updates:

```bash
python fetch_tweets.py --asset wif
```

**What happens:**
- Fetches most recent ~150 days of tweets
- Sets up watermarks for future incremental updates
- Keyword filtering happens at export time, not fetch time

### Step 4: Backfill Historical Tweets (Nitter)

For tweets older than X API's 150-day limit, use Nitter:

```bash
# Sequential (safe, slower)
python nitter_scraper.py --asset wif --full --no-headless

# Parallel (recommended for large backfills)
python nitter_scraper.py --asset wif --full --no-headless --parallel 3
```

**Important:**
- Use `--no-headless` - headless mode often fails (Nitter blocks it)
- The scraper is resumable - if interrupted, run same command to continue
- Use `--parallel 2` or `--parallel 3` for faster scraping (see below)

**Timing (tested on WIF, Dec 2024):**

| Mode | Time per chunk | 2-year backfill (108 chunks) |
|------|---------------|------------------------------|
| Sequential (`--parallel 1`) | ~1 min | ~2 hours |
| 2 workers (`--parallel 2`) | ~1 min | ~1 hour |
| 3 workers (`--parallel 3`) | ~1 min | ~40 min |

**Why parallel works:**
- Nitter rate limiting is SESSION-based, not IP-based
- Each browser has its own session
- All workers use nitter.net (only reliable instance)
- Tested with 3 workers - NO rate limiting observed

### Step 5: Cache Logo and Avatar

```bash
# Download token logo from CoinGecko (uses coingecko_id from assets.json)
python cache_logos.py --asset wif

# Download founder's Twitter profile picture
python cache_avatars.py --asset wif
```

**What happens:**
- Logo: Fetched from CoinGecko, resized to 64x64, saved to `web/public/logos/wif.png`
- Avatar: Fetched from X API, resized to 48x48, saved to `web/public/avatars/blknoiz06.png`

### Step 6: Export Static Data

```bash
python export_static.py --asset wif
```

This generates:
- `tweet_events.json` - Filtered tweets (only those mentioning keyword)
- `tweet_events_all.json` - All tweets (for "show all" toggle)

---

## CLI Status

### ✅ Fixed: `add_asset.py` Adopter Support (Dec 2024)

Now supports:
```bash
python add_asset.py wif \
  --name "WIF" \
  --founder blknoiz06 \
  --founder-type adopter \
  --keyword-filter wif \
  --coingecko dogwifcoin \
  --network solana \
  --pool EP2ib6dYdEeqD8MfE2ezHCxX3kP3K2eLKkirfPm5eyMx
```

Also provides guidance:
- Shows Nitter backfill command for assets >150 days old
- Explains keyword filtering for adopters

### ✅ Fixed: `cache_logos.py` Single-Asset Mode (Dec 2024)

Now supports `--asset` flag like `cache_avatars.py`:
```bash
python cache_logos.py --asset wif
```

### Remaining: Unified Workflow Command

Would be nice to have:
```bash
python add_adopter.py wif \
  --founder blknoiz06 \
  --keyword wif \
  --coingecko dogwifcoin
# Does everything: adds to assets.json, fetches prices, scrapes tweets, exports
```

### Remaining: Nitter Defaults to Headless

**Current:** `--headless` is default, but it often fails

**Suggestion:** Make `--no-headless` the default, or detect failures and suggest non-headless

---

## Historical Context: Why These Gaps Exist

The original pipeline was designed for "founder" assets where:
1. The founder created the token
2. All their tweets are relevant
3. Tweet history is <150 days (within X API limits)

Adopter assets break these assumptions:
1. The adopter didn't create the token
2. Most tweets are NOT about the token (need filtering)
3. Interesting history is often >150 days old (need Nitter)

---

## Testing Checklist

After adding an adopter asset:

- [ ] Asset appears in dropdown on /chart and /data pages
- [ ] Price chart loads with full history
- [ ] Tweet markers appear on chart
- [ ] "Only mentions" toggle works (shows filtered vs all tweets)
- [ ] Data table shows correct tweet count
- [ ] Tweet links open correct Twitter URLs

---

## Example: Adding WIF (Dec 2023 - Present)

**Actual results from WIF (tested Dec 2024):**
- Price history: 751 daily candles, 18,034 hourly candles
- Tweet history: 297 tweets (filtered by "wif" keyword)
- Total time: ~1 hour with parallel scraping

```bash
# 1. Manually edit scripts/assets.json (see above)
cd scripts

# 2. Fetch prices (~10 seconds for 1d+1h)
python fetch_prices.py --asset wif --backfill

# 3. Fetch recent tweets via X API (sets up watermarks)
python fetch_tweets.py --asset wif

# 4. Backfill historical tweets via Nitter (~40 min with --parallel 3)
python nitter_scraper.py --asset wif --full --no-headless --parallel 3

# 5. Cache logo and avatar
python cache_logos.py --asset wif
python cache_avatars.py --asset wif

# 6. Export
python export_static.py --asset wif

# 7. Verify
cd ../web && npm run dev
# Open http://localhost:3000/chart?asset=wif
```

---

## Learnings from WIF Implementation (Dec 2024)

### What Worked Well
1. **Parallel Nitter scraping** - 3x speedup with no rate limiting
2. **Keyword filtering** - "wif" keyword reduced 297 relevant tweets from thousands
3. **Birdeye backfill** - 10 seconds for full price history (skipped 1m/15m)
4. **Resumable scraping** - Interrupted and resumed multiple times

### What We Learned
1. **Only nitter.net works** - Other instances (poast.org, privacydev.net) are dead/unreliable
2. **--no-headless is essential** - Headless browsers get blocked by Cloudflare
3. **Rate limiting is session-based** - Multiple browsers = multiple sessions = no conflicts
4. **Future dates return 0 tweets** - Nitter correctly handles date ranges beyond current date

### Infrastructure Improvements Made
1. Added `--parallel` flag to nitter_scraper.py
2. Thread-safe progress tracking with locks
3. Queue-based DB writes (single writer, multiple scrapers)
4. Comprehensive documentation in script headers
