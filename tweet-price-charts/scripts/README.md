# Tweet-Price Data Pipeline

This directory contains the backend data pipeline for the Tweet-Price Correlation Analyzer.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA PIPELINE                                      │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   1. FETCH (Data Ingestion)                                                  │
│   ┌──────────────────────────────────────────────────────────────────────┐  │
│   │  fetch_tweets.py       X API v2 tweets (UPDATE/BACKFILL/FULL modes)  │  │
│   │  fetch_prices.py       Multi-source prices (GT, Birdeye, CoinGecko)  │  │
│   │  nitter_scraper.py     Fallback tweet scraper when API unavailable   │  │
│   └──────────────────────────────────────────────────────────────────────┘  │
│                                    │                                         │
│                                    ▼                                         │
│   2. STORE (Database Layer)                                                  │
│   ┌──────────────────────────────────────────────────────────────────────┐  │
│   │  db.py                 DuckDB abstraction layer                      │  │
│   │    └─ analytics.duckdb (SINGLE SOURCE OF TRUTH)                      │  │
│   │         ├─ assets      Asset metadata (8 tokens)                     │  │
│   │         ├─ tweets      All tweets with engagement metrics            │  │
│   │         ├─ prices      OHLCV candles (1m, 15m, 1h, 1d)               │  │
│   │         └─ tweet_events View: tweets aligned with prices             │  │
│   └──────────────────────────────────────────────────────────────────────┘  │
│                                    │                                         │
│                                    ▼                                         │
│   3. EXPORT (Frontend Data)                                                  │
│   ┌──────────────────────────────────────────────────────────────────────┐  │
│   │  export_static.py      Generates JSON for web frontend               │  │
│   │    └─ web/public/static/{asset}/                                     │  │
│   │         ├─ prices_1d.json     Daily candles                          │  │
│   │         ├─ prices_1h.json     Hourly candles                         │  │
│   │         ├─ prices_15m.json    15-minute candles                      │  │
│   │         ├─ prices_1m_*.json   1-minute candles (chunked by month)    │  │
│   │         └─ tweet_events.json  Tweets with price alignment            │  │
│   └──────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Core Scripts

### `db.py` - Database Layer
The central database module. All data operations go through here.

**Key functions:**
- `get_connection()` - Get DuckDB connection
- `init_schema()` - Initialize tables and views
- `insert_tweets()` / `insert_prices()` - Data ingestion
- `get_tweet_events()` - Query aligned tweet-price data
- `get_db_stats()` - Database statistics

### `fetch_tweets.py` - Tweet Ingestion
Fetches tweets from X API v2 with intelligent mode detection.

```bash
# Update mode (new tweets only)
python fetch_tweets.py --asset pump

# Backfill mode (older tweets)
python fetch_tweets.py --asset pump --backfill

# Full fetch (all available)
python fetch_tweets.py --asset pump --full
```

### `fetch_prices.py` - Price Ingestion
Multi-source price fetcher with automatic source selection.

```bash
# Fetch all timeframes for an asset
python fetch_prices.py --asset pump

# Specific timeframe
python fetch_prices.py --asset pump --timeframe 1h

# Specific date range
python fetch_prices.py --asset pump --start 2024-01-01 --end 2024-12-31
```

**Data sources:** GeckoTerminal, Birdeye, CoinGecko, Hyperliquid

### `export_static.py` - Frontend Export
Generates static JSON files for the web frontend.

```bash
# Export all assets
python export_static.py

# Export specific asset
python export_static.py --asset pump
```

---

## Utility Scripts

| Script | Purpose |
|--------|---------|
| `add_asset.py` | **CLI for adding new assets** (validates, fetches, exports) |
| `validate_export.py` | **Data validation** with auto-fix capability |
| `compute_stats.py` | Generate pre-computed statistics for dashboard |
| `cache_avatars.py` | Download and cache founder profile pictures |
| `cache_logos.py` | Download and cache token logos |
| `apply_keyword_filter.py` | Filter tweets by keyword for adopter accounts |
| `align_tweets.py` | Diagnostic tool for tweet-price alignment |
| `tweet_poller.py` | Real-time polling daemon (runs continuously) |
| `nitter_scraper.py` | Fallback scraper when X API is unavailable |

---

## Configuration

### `config.py`
Central configuration file with:
- File paths (`DATA_DIR`, `STATIC_DIR`)
- API endpoints
- Timeframe definitions
- Asset configuration

### `assets.json`
Asset definitions including:
- Token identifiers (GeckoTerminal pool, CoinGecko ID, etc.)
- Founder Twitter handles
- Display colors and names

---

## Common Operations

### Add a new asset
```bash
# One-shot CLI (handles everything)
python add_asset.py mytoken --name "My Token" --founder twitterhandle --coingecko my-token-id --auto-best

# Or step by step:
# 1. Add entry to scripts/assets.json
# 2. python fetch_tweets.py --asset {new_asset} --full
# 3. python fetch_prices.py --asset {new_asset}
# 4. python export_static.py --asset {new_asset}
```

### Validate data integrity
```bash
# Check all assets
python validate_export.py

# Auto-fix issues by re-exporting
python validate_export.py --fix

# Check specific asset
python validate_export.py --asset pump
```

### Refresh all data
```bash
python fetch_tweets.py --all
python fetch_prices.py --all
python export_static.py
```

### Check data quality
```bash
python -c "from db import get_db_stats; print(get_db_stats())"
```

---

## Archived Scripts

See `archive/README.md` for deprecated scripts kept for reference.
