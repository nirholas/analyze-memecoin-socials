# Archived Scripts

These scripts are no longer actively used but kept for reference.

---

## Tweet Fetching (Superseded)

### `backfill_tweets.py`
**Status:** Superseded by `fetch_tweets.py --backfill`

Original backfill script that fetched older tweets. All functionality
is now consolidated into `fetch_tweets.py` with the `--backfill` flag.

### `fetch_historical_tweets.py`
**Status:** Superseded by `fetch_tweets.py --full`

Date-range based tweet fetcher. Functionality consolidated into
`fetch_tweets.py` with full/update/backfill modes.

### `nitter_scraper_v1_deprecated.py`
**Status:** Superseded by `nitter_scraper.py`

Original Nitter scraper with reliability issues:
- 5 second delay between chunks triggered rate limiting
- No retry logic meant lost progress on errors
- Single Nitter instance = single point of failure

---

## Price Fetching (Superseded)

### `fetch_sol_prices.py`
**Status:** Superseded by `fetch_prices.py`

Single-asset SOL price fetcher. All price fetching is now consolidated
into `fetch_prices.py` which supports multiple sources (GeckoTerminal,
Birdeye, CoinGecko, Hyperliquid) and all assets.

---

## Migration Scripts (One-Time Use)

### `migrate_unified.py`
**Status:** One-time migration, completed Dec 2024

Final migration script that consolidated all data sources (JSON exports,
per-asset SQLite files) into the unified `analytics.duckdb` database.
Keep for reference on data source formats and migration patterns.

### `migrate_to_duckdb.py`
**Status:** One-time migration, completed

Earlier DuckDB migration attempt. Superseded by `migrate_unified.py`.

### `migrate_pump.py`
**Status:** One-time migration, completed

Migrated PUMP data from flat structure to subdirectory structure.
Only needed during the initial multi-asset refactor.

---

## Utility Scripts (Obsolete)

### `clean_db.py`
**Status:** Obsolete

Manual one-off cleanup script with hardcoded paths. Database cleanup
is now handled through `db.py` functions.

### `audit_data.py`
**Status:** Obsolete

Basic data quality check that read JSON files. Data integrity checks
should now query `analytics.duckdb` directly via `db.py`.

---

## Usage Note

These scripts should **not** be run in production. They are kept only
for historical reference and as templates for similar future work.

For current data operations, use:
- `fetch_tweets.py` - All tweet fetching
- `fetch_prices.py` - All price fetching
- `export_static.py` - Frontend data generation
- `db.py` - Database operations
