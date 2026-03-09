# CLAUDE.md - Project Instructions for AI Agents

## BEFORE YOU CHANGE ANYTHING

**Read `GOTCHAS.md` first.** It documents non-obvious design decisions and past debugging sessions that will save you hours of re-fixing things.

## IMPORTANT: Database Architecture

Before querying DuckDB, understand the two-table architecture:

| Table | Purpose | Query this? |
|-------|---------|-------------|
| `prices` | Canonical (matches website) | ✅ YES |
| `prices_RAW_INGESTION` | Raw messy data | ❌ NO |

**TL;DR:** Query `prices`, never `prices_RAW_INGESTION`. See GOTCHAS.md "DuckDB Table Architecture" section.

## Project Overview

Tweet-Price Correlation Analyzer - visualizes the relationship between founder tweets and token prices.

## Data Pipeline

```
fetch_tweets.py / nitter_scraper.py  →  analytics.duckdb  →  export_static.py  →  JSON files  →  Frontend
fetch_prices.py                      ↗
```

## Key Files

- `scripts/db.py` - DuckDB schema and queries (tweet_events view is critical)
- `scripts/fetch_prices.py` - Multi-source price fetching with incremental sync
- `scripts/export_static.py` - Static JSON generation for frontend
- `.github/workflows/hourly-update.yml` - Automated data pipeline

## Critical Gotchas (read GOTCHAS.md for details)

1. **DST Duplicate Timestamps** - export_static.py deduplicates or charts crash
2. **Staleness Limits in Views** - DO NOT remove from tweet_events view
3. **calendar.timegm() for UTC** - NOT datetime.timestamp()
4. **GeckoTerminal Backward Pagination** - No after_timestamp support
5. **5-sigma Outlier Threshold** - 3-sigma flags legitimate pumps
6. **Founder vs Adopter Tweets** - Founders store ALL tweets, adopters filter by keyword
7. **Tweet Watermarks** - Must match last SAVED tweet, not last SEEN tweet
8. **GitHub Actions Cache** - May need deletion when watermark fixes must propagate

## Running Locally

```bash
cd scripts
python fetch_prices.py --asset pump  # Fetch prices for one asset
python export_static.py              # Generate static JSON
cd ../web && npm run dev             # Start frontend
```

## Adding a New Asset

1. Add entry to `scripts/assets.json`
2. Run `python fetch_prices.py --asset {id}` (prices first - validates data source works)
3. Run `python nitter_scraper.py --asset {id} --full` (scrape tweets)
4. Run `python export_static.py`

## Before Committing

- [ ] Read GOTCHAS.md
- [ ] Run `python export_static.py` - completes without errors?
- [ ] Open frontend - charts load without crashing?
