# GOTCHAS.md - Read Before Changing ANYTHING

This file documents non-obvious design decisions and past debugging sessions.
**AI agents and developers: Read this BEFORE modifying any code.**

---

## Future Work / TODO

### Chart Gap Line Statistics (Chart.tsx)

**Current state:** Gap % calculations use `avgPrice` (cluster averages) so line color matches visual direction.

**Ideal state:** Tie % to actual tweet moments (boundary prices) while maintaining visual consistency.

**Problem:** When using boundary prices (lastTweet → firstTweet), line color often contradicts visual direction during volatile trending markets. A cluster's lastTweet might be at a local high, while the next cluster's firstTweet is at a local low, showing negative % even though overall trend is up.

**Tradeoff:** avgPrice makes % values zoom-dependent (change as clusters merge/split), but always matches visual intuition.

**To revisit:** Find a hybrid approach that uses actual tweet moments but doesn't create red lines over green visual trends.

---

## Critical: Things That Will Crash the Frontend

### DST Duplicate Timestamps (export_static.py)

**Symptom:** Chart crashes with "Value is null" error from lightweight-charts.

**Root Cause:** Database stores datetime values. During DST "fall back" transitions,
two different wall-clock times (e.g., 2:00 AM before and after the change) can map
to the **same Unix timestamp**. lightweight-charts requires strictly unique timestamps.

**Solution:** export_static.py deduplicates timestamps during export, keeping the first
occurrence. There's a safety check that raises ValueError if duplicates slip through.

```python
# DO NOT REMOVE this deduplication logic in export_timeframe() and export_1m_chunked()
if ts_epoch in seen_timestamps:
    duplicates_skipped += 1
    continue
seen_timestamps.add(ts_epoch)
```

**Fixed in:** December 2024 after JUP chart started crashing.

---

## DOUBLE CANDLE Issue (1D Charts)

### What It Looks Like

The daily (1D) chart shows **two or more candles for the same day**, making the chart look "doubled" or have irregular spacing.

### Root Cause

Daily price data comes from multiple sources with **different timestamp conventions**:

| Source | Timestamp Convention |
|--------|---------------------|
| CoinGecko API | 00:00 UTC (midnight) |
| GeckoTerminal | 04:00 or 05:00 UTC |
| Birdeye backfill | 00:00 UTC |
| Hyperliquid API | 05:00 UTC |

When data from multiple sources is merged, you get **multiple candles per day**.

### How It's Fixed (Automatic)

`export_static.py` automatically normalizes all 1D timestamps to midnight UTC:

```python
# 1D TIMESTAMP NORMALIZATION - DO NOT REMOVE
if timeframe == "1d":
    normalized_ts = (ts_epoch // 86400) * 86400  # Snap to midnight
```

After normalization, deduplication keeps only the first candle for each timestamp.

### Verification

Run `python export_static.py` and look for:
```
(Normalized 379 timestamps to midnight UTC)
(Deduplicated 379 candles - same day from different sources)
```

### If Still Broken

See `docs/DATA_QUALITY_ISSUES.md` for detailed diagnosis and historical incidents.

---

## DuckDB Table Architecture (READ THIS FIRST)

### Two Price Tables - Using the Wrong One Causes Bugs

| Table | What it is | Use for queries? |
|-------|------------|------------------|
| `prices` | **Canonical** (matches website JSONs) | ✅ YES - always |
| `prices_RAW_INGESTION` | Raw API data (messy, duplicates) | ❌ NO - debugging only |

### Why Two Tables?

Raw API ingestion can have:
- Duplicate timestamps from different sources
- Timezone inconsistencies (00:00 vs 04:00 UTC)
- Overlapping data from Birdeye, GeckoTerminal, CoinGecko

The export process (`export_static.py`) cleans this up:
1. Reads from `prices_RAW_INGESTION`
2. Normalizes timestamps, deduplicates, applies overrides
3. Writes to static JSONs (the source of truth)
4. Syncs JSONs back to `prices` (canonical table)

### Data Flow

```
fetch_prices.py → prices_RAW_INGESTION → export_static.py → JSONs → prices (canonical)
                        ↓                                                    ↓
                   (messy raw data)                              (clean, matches website)
```

### If Counts Don't Match Website

Run `python export_static.py` - it syncs the canonical table after export.

### For Developers/Agents

- **Analysis queries:** Use `prices` (canonical)
- **Incremental fetch logic:** Uses `prices_RAW_INGESTION` (via `get_raw_price_table()`)
- **Export reads from:** `prices_RAW_INGESTION`
- **Views (tweet_events):** Read from `prices` (canonical)

See `db.py` header comment for full architecture documentation.

---

## Tweet Fetching: Watermarks & Rate Limits

### Tweet Watermarks (ingestion_state table)

**What it is:** The `ingestion_state` table tracks `last_id` (newest tweet ID fetched) for each asset.
This enables incremental fetching - only tweets newer than `last_id` are requested.

**Critical Rule:** Watermarks should ONLY advance to match the LAST SAVED tweet, not the last SEEN tweet.

**Why this matters:** If watermarks advance past what's saved, those tweets are lost forever.
This happened in Dec 2024 when keyword filtering was incorrectly applied to founder assets -
tweets were seen but filtered out, yet the watermark advanced, creating a gap.

**How to verify watermarks are correct:**
```bash
# Compare DB watermarks to JSON last tweet IDs
python3 << 'EOF'
import duckdb, json
conn = duckdb.connect('data/analytics.duckdb')
for asset in ['aster', 'jup', 'monad', 'meta']:
    wm = conn.execute("SELECT last_id FROM ingestion_state WHERE asset_id=? AND data_type='tweets'", [asset]).fetchone()
    with open(f'web/public/static/{asset}/tweet_events.json') as f:
        tweets = json.load(f).get('events', [])
        last = max(tweets, key=lambda t: int(t['tweet_id']))['tweet_id'] if tweets else None
    print(f"{asset}: DB={wm[0] if wm else 'N/A'}, JSON={last}, Match={'✅' if str(wm[0])==str(last) else '❌'}")
EOF
```

**If watermarks are ahead of JSONs:** Download the release DB, fix watermarks, re-upload.

---

### Founder vs Adopter Tweet Filtering

**Two types of assets:**

| Type | Definition | Tweet Behavior |
|------|------------|----------------|
| **Founder** | Person who created the token | Store ALL tweets (they define the project) |
| **Adopter** | Celebrity/influencer who adopted token | Filter to keyword matches only (reduce noise) |

**Configuration in assets.json:**
```json
{
  "id": "meta",
  "founder": "metaproph3t",
  "founder_type": "founder",     // ← ALL tweets stored
  "keyword_filter": "meta"        // ← Only used for optional UI filtering
}

{
  "id": "wif", 
  "founder": "blknoiz06",
  "founder_type": "adopter",     // ← Only keyword matches stored
  "keyword_filter": "wif"         // ← Applied at FETCH time
}
```

**Why this distinction matters:**
- Founders define the project - every tweet is potentially relevant
- Adopters tweet hundreds of times per day about many topics - storing all would waste API credits and add noise

**DO NOT apply keyword filtering to founder assets at fetch time.**

---

### Rate Limit Handling (fetch_tweets.py)

**X API Rate Limits:** Basic plan allows ~100 requests per 15 minutes.

**Current behavior:**
1. On 429 response, read `x-rate-limit-reset` header
2. If reset is <2 minutes away, wait and retry
3. If reset is >2 minutes away, skip this asset and set `rate_limit_hit` flag
4. Once `rate_limit_hit` is set, skip all remaining assets in current run
5. Save skipped assets to `data/fetch_state.json` for priority on next run

**Why skip remaining assets:** Prevents wasting time on requests that will definitely fail.
Better to exit early and let the next hourly run pick up where we left off.

**State persistence:** `data/fetch_state.json` tracks:
- `skipped_assets`: List of assets that were skipped due to rate limit
- `last_run`: Timestamp of last run

Next run reads this and processes skipped assets FIRST before others.

---

### Skip Tweet Fetch Flag

**For problematic accounts** (e.g., Sam Altman rarely tweets about WLD):

```json
{
  "id": "wld",
  "founder": "sama",
  "skip_tweet_fetch": true,
  "skip_tweet_fetch_reason": "Sam Altman rarely tweets about WLD - manual updates only"
}
```

This prevents wasting API credits on accounts that:
- Tweet too frequently (exhausts rate limit before other assets)
- Rarely mention the relevant token
- Require manual curation

---

## GitHub Actions: Cache vs Release DB Logic

### The Problem

GitHub Actions caches the DuckDB database between runs for speed.
But if the cache contains bugs (wrong watermarks, missing data), those bugs persist.

**Fix propagation path:**
1. Fix the release DB (upload corrected version to GitHub Releases)
2. Clear the cache OR ensure workflow prefers release over cache

### Current Logic (hourly-update.yml)

```bash
if [ "$RELEASE_SIZE" -ge "$CACHE_SIZE" ]; then
    # Download release (ensures fixes propagate)
else
    # Use cache (faster, but may have stale data)
fi
```

**Why `-ge` instead of `-gt`:** If sizes are equal, we STILL prefer release because:
- Release may have watermark fixes that don't change file size
- Cache may have advanced watermarks that skip tweets

### When to Delete Cache

Delete cache when you need fixes to propagate immediately:
```bash
gh cache list --limit 5  # Find cache ID
gh cache delete <ID>     # Delete it
```

Next run will have a cache miss → downloads release DB → uses fixed data.

**Use sparingly:** Cache deletion adds ~30-60s to workflow (must download 475MB).

---

## Database: Non-Obvious Design Decisions

### Staleness Limits in tweet_events View (db.py:139-142)

**What it does:** Each timeframe has a staleness limit:
- 1m: max 1 hour old
- 1h: max 24 hours old
- 1d: max 7 days old

**Why this exists:** Without staleness limits, a tweet from TODAY could match with
1-minute price data from 3 MONTHS AGO (when 1m collection started) instead of using
fresh 1h or 1d data. This caused wildly incorrect price_at_tweet values.

**DO NOT "optimize" by removing staleness checks.**

### CASE Statement Ordering (db.py:178-179)

```sql
CASE p.timeframe WHEN '1m' THEN 1 WHEN '1h' THEN 2 WHEN '1d' THEN 3 END
```

**DO NOT reorder this.** The fallback priority (1m > 1h > 1d) is intentional.
Changing the order breaks price lookups for tweets.

### Prices Table PRIMARY KEY (db.py:95)

```sql
PRIMARY KEY (asset_id, timeframe, timestamp)
```

The upsert logic in `insert_prices()` depends on this composite key.
**DO NOT change this** without also updating all INSERT...ON CONFLICT statements.

### insert_tweets Uses ON CONFLICT DO UPDATE (db.py:533-541)

This is intentional - we WANT to update engagement metrics (likes, retweets)
on re-fetch while preserving the original timestamp. Using INSERT OR IGNORE
would discard updated metrics.

### launch_date Filter in tweet_events (db.py:161)

```sql
WHERE t.timestamp >= a.launch_date
```

This prevents showing tweets from BEFORE the token existed.
**DO NOT remove this filter.**

---

## Fake Wick Detection (export_static.py)

### Automatic Wick Capping at Export Time

Fake wicks (MEV bots, fat fingers, data errors) are automatically capped during export.

**How it works:**
- If HIGH > max(OPEN, CLOSE) * 2, cap HIGH to that limit
- If LOW < min(OPEN, CLOSE) / 2, cap LOW to that limit

**Why this approach:**
- Catches fake spikes (HIGH way above both OPEN and CLOSE)
- Preserves legitimate crashes (where HIGH ≈ OPEN, price falls to lower CLOSE)
- Automatic - no manual intervention needed
- Non-destructive - candles are preserved, just wicks are capped

**Example:**
- PUMP spike: O=0.008, H=0.020, C=0.008 → HIGH capped to 0.016 (2x max body)
- BELIEVE crash: O=0.038, H=0.038, C=0.0003 → No change (HIGH ≈ OPEN, legitimate)

**Configuration:** `WICK_CAP_MULTIPLIER = 2.0` in export_static.py

---

## API Quirks: Know Before You Fetch

### CoinGecko: Use /ohlc/range, NOT /market_chart/range (fetch_prices.py)

**Symptom:** Chart renders as dots instead of candles, irregular timestamps.

**Root Cause:** The `/market_chart/range` endpoint returns **price points at irregular intervals**
(e.g., 13:12:40, 14:11:16), not proper OHLC candles. lightweight-charts expects regular intervals.

**Wrong endpoint:**
```
GET /coins/{id}/market_chart/range?from=X&to=Y
Returns: [[timestamp, price], ...] at ~hourly but IRREGULAR times
```

**Correct endpoint:**
```
GET /coins/{id}/ohlc/range?from=X&to=Y&interval=hourly
Returns: [[timestamp, O, H, L, C], ...] at EXACT hour boundaries
```

**Plan requirements:**
- `/market_chart/range`: Basic plan ($35/mo) - but gives irregular data!
- `/ohlc/range`: Analyst plan ($129/mo) or higher - proper OHLC candles

**Request limits:**
- Hourly: max 31 days per request (chunk into 30-day batches)
- Daily: max 180 days per request

**CoinGecko only supports 1h and 1d.** No 15m or 1m data available at any tier.

**Fixed in:** December 2024 after HYPE chart showed dots instead of candles.

---

### CoinGecko Usage Guidelines

**When to use CoinGecko:**
- Non-Solana chains (HYPE on Hyperliquid, EVM chains without DEX pools)
- CEX-only tokens not available on DEXes
- Backup/validation data source

**When NOT to use CoinGecko:**
- Solana DEX tokens (use GeckoTerminal + Birdeye instead)
- When you need 15m or 1m data (not supported)
- Demo/free tier (irregular timestamps break charts)

### CoinGecko /market_chart Returns Price Points, NOT OHLC (CRITICAL)

**Symptom:** Chart shows scattered dots instead of candles on 1D view.

**Root Cause:** The `/market_chart` endpoint returns **single price values**, not OHLC data.
If you insert this data with `O=H=L=C=price`, lightweight-charts renders them as **dots**
because there's no candle body (high=low, open=close).

**Example of BAD data (all values identical = dot):**
```
O=0.027144 H=0.027144 L=0.027144 C=0.027144 ⚫ DOT
```

**Example of GOOD data (different OHLC = candle):**
```
O=0.043135 H=0.074817 L=0.017399 C=0.024064 📊 CANDLE
```

**How to diagnose:**
```python
# Check exported JSON for O=H=L=C dots
dots = [c for c in candles if c['o'] == c['h'] == c['l'] == c['c']]
print(f'Dots: {len(dots)} / {len(candles)}')
```

**Solution:**
1. Delete the bad `/market_chart` data from database
2. Use Birdeye backfill instead (provides real OHLC data):
   ```bash
   python fetch_prices.py --asset ASSET --backfill
   ```
3. Re-export: `python export_static.py --asset ASSET`

**Prevention:** Never use CoinGecko `/market_chart` for price backfills.
Always use Birdeye for Solana/BSC tokens, or CoinGecko `/ohlc/range` (requires Analyst plan).

**Fixed in:** December 2024 after BELIEVE chart showed dots instead of candles.

---

**CLI usage:**
```bash
# Backfill with CoinGecko (requires backfill_source: "coingecko" in assets.json)
python fetch_prices.py --asset hype --backfill --fresh

# Regular fetch uses price_source, backfill uses backfill_source
```

**GitHub Actions:** CoinGecko backfill is NOT run in hourly workflow.
Use `--backfill` manually for historical data. Regular hourly updates use primary price_source.

---

### GeckoTerminal Only Supports Backward Pagination (fetch_prices.py)

GeckoTerminal API only accepts `before_timestamp`, not `after_timestamp`.
You MUST paginate backwards from present to past.

**Why this matters for incremental fetch:**
You can't say "give me everything after timestamp X". Instead, you must:
1. Fetch the most recent page
2. Check if any returned candles are older than your last known timestamp
3. Stop when you hit existing data

**DO NOT try to add `after_timestamp` - the API doesn't support it.**

### GeckoTerminal 180-Day Paywall (fetch_prices.py:396-398)

Free tier returns HTTP 401 for data older than ~180 days.
Code handles this gracefully by returning empty list.

### X API 150-Day Historical Limit

X API v2 cannot retrieve tweets older than ~150 days.
For historical backfill, use Nitter scraper (`nitter_scraper.py`).

---

## Asset Backfill Process

This section documents the complete process for backfilling asset price data,
including edge cases encountered in December 2024.

### Standard Backfill (Birdeye)

For Solana/BSC tokens with `backfill_source: "birdeye"` in assets.json:

```bash
python fetch_prices.py --asset ASSET --backfill
```

This fetches all available historical OHLC data from Birdeye API.
Use `--fresh` to ignore resume points and fetch from scratch.

### Migration Token Backfill

**When to use:** Asset has a pre-merge/pre-migration token with historical data.

**Examples:**
- ASTER: Pre-merge token was APX (`0x78F5d389F5CDCcFc41594aBaB4B0Ed02F31398b3`)
- BELIEVE: Pre-launch token was ben-pasternak on CoinGecko

**Process:**

1. **Identify the gap** - Run validation to find missing period:
   ```bash
   python validate_candle_coverage.py --asset ASSET --verbose
   ```

2. **Find pre-merge token** - Check CoinGecko, Birdeye, or project documentation

3. **Verify price alignment** - Compare prices at crossover point:
   ```python
   # Pre-merge token end price should ≈ new token start price
   # ASTER example: APX at Sept 19 = $0.66, ASTER at Sept 19 = $0.63 ✓
   ```

4. **Fetch and insert** - Use Birdeye API with pre-merge token address:
   ```python
   # Fetch from pre-merge token
   url = 'https://public-api.birdeye.so/defi/ohlcv'
   params = {'address': PRE_MERGE_TOKEN, 'type': '15m', ...}

   # Insert as target asset
   conn.execute('''
       INSERT INTO prices (asset_id, timeframe, timestamp, ...)
       VALUES ('target_asset', '15m', ..., 'migration_backfill')
   ''')
   ```

5. **Export and validate**:
   ```bash
   python export_static.py --asset ASSET
   python validate_candle_coverage.py --asset ASSET
   ```

**Evidence from ASTER migration (Dec 2024):**
- Gap: 15m data missing Sept 17-19, 2025
- APX price at crossover: O=$0.66
- ASTER price at crossover: O=$0.63
- Difference: ~4% (acceptable for different data sources)

### Data Quality Issues & Detection

#### 1. Dots Instead of Candles (O=H=L=C)

**Symptom:** Chart shows scattered dots instead of candlesticks.

**Root Cause:** Data source returned price points, not OHLC candles.
When O=H=L=C, there's no candle body or wicks to render.

**Detection:**
```python
dots = [c for c in candles if c['o'] == c['h'] == c['l'] == c['c']]
if len(dots) / len(candles) > 0.1:  # >10% is suspicious
    print(f"WARNING: {len(dots)}/{len(candles)} candles are dots")
```

**Fix:**
1. Identify and delete the bad data source
2. Re-backfill from Birdeye (provides real OHLC)
3. Re-export

**Evidence from BELIEVE (Dec 2024):**
- CoinGecko `/market_chart` returned 333 price points
- All had O=H=L=C (100% dots)
- Fixed by deleting and using Birdeye backfill

#### 2. Corrupt Candle Data (Open/Close Discontinuity)

**Symptom:** Abnormally shaped candle with impossible wick.

**Root Cause:** Bad data entry where Open doesn't match previous Close.

**Detection:**
```python
for i in range(1, len(candles)):
    prev_close = candles[i-1]['c']
    curr_open = candles[i]['o']
    pct_diff = abs(curr_open - prev_close) / prev_close
    if pct_diff > 0.5:  # >50% jump
        print(f"CORRUPT: {candles[i]['t']} Open={curr_open} vs PrevClose={prev_close}")
```

**Fix:**
1. Identify the specific bad candle
2. Check if alternative data exists for same timestamp
3. Delete bad entry, keep good one
4. Re-export

**Evidence from ASTER Sept 21 (Dec 2024):**
- Bad: O=$0.16, H=$1.74, L=$0.10, C=$1.67 (Open 10x too low)
- Good: O=$1.66, H=$1.99, L=$1.32, C=$1.40 (Open matches prev close)
- Fix: Deleted entry at 04:00, kept entry at 08:00

#### 3. Data Source Conflicts

**Symptom:** Multiple candles for same day from different sources.

**Root Cause:** Different data sources have different timestamp conventions:
- Birdeye: 00:00 UTC
- GeckoTerminal: 04:00 or 05:00 UTC
- chart_reconstruction: varies

**Current Behavior:** Export normalizes 1D to midnight, deduplicates (keeps first).

**Best Practice:**
1. After backfill, check data sources: `SELECT data_source, COUNT(*) FROM prices WHERE asset_id='X' GROUP BY data_source`
2. Remove superseded sources (e.g., chart_reconstruction after real data backfill)
3. Keep only authoritative source for each period

### Data Source Priority

When multiple sources exist, priority order:

1. **birdeye** - Authoritative for Solana/BSC DEX tokens
2. **geckoterminal** - Real-time, but limited history
3. **coingecko** - Good for CEX tokens, but only daily/hourly
4. **migration_backfill** - For pre-merge token data
5. **json_import** - Legacy imports
6. **chart_reconstruction** - Manually created (remove after real data available)

### Cleanup Checklist

After any backfill operation:

- [ ] Run `python export_static.py --asset ASSET`
- [ ] Run `python validate_candle_coverage.py --asset ASSET`
- [ ] Verify coverage ≥95% for all timeframes
- [ ] Check for dots: `validate_export.py --asset ASSET` (if --quality flag exists)
- [ ] Visual check: Load chart in browser, verify no anomalies
- [ ] Clean up stale sources if needed

---

## Timezone Handling: Here Be Dragons

### Use calendar.timegm(), NOT datetime.timestamp() (fetch_prices.py:836)

**Wrong:**
```python
stop_at = last_ts.timestamp()  # Treats naive datetime as LOCAL time!
```

**Correct:**
```python
stop_at = calendar.timegm(last_ts.timetuple())  # Treats as UTC
```

DuckDB returns naive datetime objects. Using `.timestamp()` interprets them
as local time, which breaks incremental fetch logic.

**Fixed in:** December 2024 after workflow was re-fetching all historical data.

---

## Outlier Detection: Why 5-sigma?

### Threshold is 5σ, Not 3σ (fetch_prices.py:52, db.py:813)

```python
OUTLIER_THRESHOLD_STD = 5
```

Crypto prices have fat tails. Using 3-sigma (standard for normal distributions)
flagged legitimate 50-100% pumps as outliers. 5-sigma catches only true anomalies
like sniper bot spikes (1000x+ of median).

**DO NOT lower this threshold without testing on real pump data.**

---

## Export Pipeline: Chunking Strategy

### 1m Data is Chunked by Month (export_static.py)

Large assets can have millions of 1-minute candles. We chunk by month for:
1. Lazy loading in frontend (only load visible range)
2. Smaller individual file sizes
3. Efficient cache invalidation (only update current month)

Structure:
```
prices_1m_index.json     # Lists all chunks with date ranges
prices_1m_2025-07.json   # July 2025 data
prices_1m_2025-08.json   # August 2025 data
```

**DO NOT switch to a single prices_1m.json - it will break the frontend.**

---

## Frontend Integration

### Asset Data is Loaded Per-Asset

Frontend fetches:
1. `assets.json` (list of all assets)
2. `{asset}/tweet_events.json` (selected asset's tweets)
3. `{asset}/prices_1h.json` (for chart)
4. `{asset}/prices_1m_index.json` (for zoom, lazy loaded)

**DO NOT combine all assets into a single JSON - defeats lazy loading.**

---

## GitHub Actions Workflow

### YAML Heredoc Syntax for Inline Python (.github/workflows/hourly-update.yml)

Inline Python in YAML must use heredoc syntax:
```yaml
run: |
  python3 << 'PYTHON_SCRIPT'
  import sys
  # ... code here
  PYTHON_SCRIPT
```

**DO NOT use `python -c "..."` for multiline code** - YAML parsing breaks.

**Fixed in:** December 2024 after workflow failed with "unexpected token" errors.

---

## Data Overrides: Persistent Manual Fixes

### Why Data Overrides Exist (scripts/data_overrides.json)

**Problem:** One-time data fixes (removing outliers, excluding bad candles, filtering date ranges)
get RESET when:
- Data is re-fetched
- Export is re-run
- A new developer/agent doesn't know about the fix

**Solution:** `data_overrides.json` contains all manual fixes in a declarative format.
These are applied at EXPORT time, not stored in the database.

**File location:** `scripts/data_overrides.json`

### How It Works

```
Raw Data (DB) → Export → Apply Overrides → JSON Files (Frontend)
```

The database contains RAW data. Overrides are applied during export.
This means:
1. Raw data is never corrupted
2. Fixes persist across re-fetches
3. All fixes are documented and auditable
4. You can always see what was fixed and WHY

### Override Types

1. **price_overrides** - Fix specific candles (cap HIGH, exclude candle)
2. **tweet_exclusions** - Exclude specific tweet IDs from export
3. **asset_data_ranges** - Override the date range for an asset's data
4. **price_exclusions** - Exclude entire candles from export

### Example: PUMP Fake Wick Fix

```json
{
  "id": "pump-fake-wick-sept18",
  "asset_id": "pump",
  "timeframe": "1h",
  "timestamp": "2025-09-18T18:00:00Z",
  "action": "cap_high",
  "value": 0.009,
  "reason": "Fake wick from MEV bot/sniper. Original HIGH was ~0.02."
}
```

### CRITICAL RULES

1. **NEVER delete entries** without understanding WHY they were added
2. **ALWAYS add a reason** when creating new overrides
3. **Run export after changes** to verify they work
4. **Check this file FIRST** when investigating data issues

### When to Add an Override

- Found a fake wick that the automatic detector missed
- Need to exclude a specific bad candle
- Need to restrict date range for an asset (data quality issues)
- Need to exclude specific tweets (spam, off-topic, etc.)

**DO NOT manually edit the database for one-time fixes.** Use data_overrides.json.

---

## NEVER DELETE DATA FROM DATABASE

### The Rule

**NEVER run DELETE statements on the prices or tweets tables without verifying overlap first.**

### Why This Exists (December 2024 Incident)

A cleanup script deleted 92 HYPE candles assuming they were "duplicate migration data."
They weren't duplicates—they had **zero overlap** with existing data. Result: 24% of HYPE
daily data was lost and had to be restored from backup.

### Before ANY Bulk Delete

Run this verification query:

```sql
-- Check overlap BEFORE deleting
WITH to_delete AS (
    SELECT DISTINCT DATE_TRUNC('day', timestamp) as dt
    FROM prices
    WHERE asset_id = 'ASSET' AND timeframe = '1d'
    AND data_source = 'SOURCE_TO_DELETE'
),
to_keep AS (
    SELECT DISTINCT DATE_TRUNC('day', timestamp) as dt
    FROM prices
    WHERE asset_id = 'ASSET' AND timeframe = '1d'
    AND data_source != 'SOURCE_TO_DELETE'
)
SELECT
    (SELECT COUNT(*) FROM to_delete) as dates_to_delete,
    (SELECT COUNT(*) FROM to_keep) as dates_to_keep,
    (SELECT COUNT(*) FROM to_delete WHERE dt IN (SELECT dt FROM to_keep)) as overlap;

-- ONLY proceed if overlap = dates_to_delete (all deletions are true duplicates)
```

### Safe Alternatives

1. **Use data_overrides.json** to exclude data at export time (non-destructive)
2. **Add a `deprecated` flag** instead of deleting
3. **Archive to a backup table** before deleting

---

## Data Truncation Protection (hourly-update.yml)

### Problem: Silent Data Loss

Historical data can be silently lost when:
1. Database cache is corrupted/missing historical data
2. Git merge conflict overwrites full export with partial data
3. Incremental fetch runs but historical data wasn't in DB

### Solution: Pre/Post Comparison

The hourly workflow now:
1. **Snapshots** current data counts BEFORE export
2. **Compares** new counts to old counts AFTER export
3. **Blocks commit** if any asset's data drops >10%

```
🚨 DATA TRUNCATION DETECTED - BLOCKING COMMIT
The following data would be lost:
   - zora/1h: 5794 → 737 (lost 5057)
```

### Manual Fix

If truncation is detected:
```bash
# Check what's wrong
python validate_export.py --asset ASSET

# Re-export from DB (if DB has data)
python validate_export.py --asset ASSET --fix

# If DB is missing data, restore from backup or re-fetch with --backfill
python fetch_prices.py --asset ASSET --backfill
```

**DO NOT bypass the truncation check** - it exists to prevent production data loss.

---

## True Validation: Expected vs Actual Candles

### The Problem with "File Exists" Validation

Checking "does the JSON file exist and have data" is **NOT real validation**.
This approach missed the HYPE bug where 14,294 candles existed but at irregular
timestamps, causing the chart to render as dots instead of candles.

### True Validation Method

Use `validate_candle_coverage.py` to validate against mathematical truth:

```bash
python validate_candle_coverage.py --asset hype --verbose
```

**How it works:**
1. Load `launch_date` from assets.json (source of truth)
2. Calculate expected candles: `(today - launch_date) / interval`
3. Compare against actual candles in exported JSON
4. Report coverage % and detect gaps
5. Fail if coverage < 95%

**Example output:**
```
Timeframe    Expected     Actual   Coverage   Status
--------------------------------------------------
1d                387        384      99.2%       OK
1h              9,310      9,229      99.1%       OK
15m            37,241      5,936      15.9%     FAIL
```

### When to Run Validation

- After any `fetch_prices.py --backfill` operation
- After changing CoinGecko/Birdeye fetch logic
- Before committing data changes
- When investigating chart rendering issues

### What Validation Catches

1. **Coverage gaps** - Missing date ranges (e.g., 15m data only covers recent months)
2. **Pre-launch data** - Data before token existed (suspicious)
3. **Irregular timestamps** - More candles than expected suggests non-bucketed data
4. **Data source issues** - Wrong endpoint, API plan limitations

**DO NOT rely on "file exists" checks.** Always use the validation script.

---

## Checklist: Before You Commit

- [ ] Did you read this file?
- [ ] If modifying db.py views: Did you preserve staleness limits?
- [ ] If modifying export: Did you test for duplicate timestamps?
- [ ] If modifying fetch: Did you use calendar.timegm() for UTC?
- [ ] If adding timeframes: Did you add to CASE statements?
- [ ] If fixing data issues: Did you add to data_overrides.json (not manual DB edit)?
- [ ] If deleting DB data: Did you verify overlap first? (See "NEVER DELETE DATA" section)
- [ ] Run `python export_static.py` - does it complete without errors?
- [ ] Run `python validate_export.py` - do all assets pass?
- [ ] Run `python validate_candle_coverage.py --asset X` - is coverage ≥95%?
- [ ] Open the frontend - do charts load without crashing?
- [ ] Do charts render as CANDLES (not dots)?

---

*Last updated: December 29, 2025*
*Maintainer: Learned these the hard way so you don't have to.*
