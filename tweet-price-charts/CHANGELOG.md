# Changelog - Tweet-Price Correlation Analyzer

## Dec 29, 2025 - About Page Redesign (Tool Contract v0)
- **New landing page** implementing "Tool Contract" framework
- Hero with clear claim: "Most founder tweets do nothing. Some coincide with big moves."
- **ImpactExplorer** - Scatter plot of all tweets vs price changes
  - Token logos with colored glow rings (green=up, red=down)
  - "Biggest moves" filter (15% threshold) to highlight outliers
  - Hover tooltip with tweet content and engagement stats
- **TweetTimeHeatmap** - When founders tweet (hour-of-day patterns)
- **SilencesExplorer** - Notable quiet periods with price context
- **AssetGrid** - Token picker with credibility-based ordering
- Orientation strip with disclaimers (correlation ≠ causation)

## Dec 29, 2025 - Market Cap Integration & GORK Asset

### Market Cap in UI
- **Tweet preview tooltip** now shows market cap in ticker style: `@$6.0B MC ▼0.5% 1h ▼2.6% 24h`
- **Data Table** already had market cap column - now fully populated for all assets
- **JUP backfilled** with 467 historical market cap values from CoinGecko

### Pipeline Integration
- **`fetch_supply.py`** integrated into `add_asset.py` pipeline (auto-fetches circulating supply)
- **`--skip-supply`** flag added for selective runs
- **Python 3.9 compatibility** fix for `fetch_supply.py` (Optional[dict] syntax)

### New Asset: GORK
- **Elon Musk as adopter** - 106 tweets mentioning "gork"
- **1m price data** for May 3-21 concentrated tweet period (212k candles)
- First asset using parallel Nitter keyword search with date bounds

### 1m Timeframe Support
- **`--force`** flag bypasses age-based timeframe skipping
- **`--since`/`--until`** flags for fetching specific date ranges
- **UI toggle** shows 1m option only for assets with 1m data

### Assets: 15 total
ASTER, BELIEVE, FARTCOIN, **GORK**, HYPE, JUP, META, MONAD, PUMP, USELESS, WIF, WLD, XPL, ZEC, ZORA

---

## Dec 29, 2025 - Tweet Fetching Reliability & Watermark Fixes

### Tweet Fetching Fixes
- **Fixed founder vs adopter tweet filtering** - Founders now store ALL tweets, adopters filter by keyword at fetch time
- **Fixed watermark synchronization** - Watermarks now correctly track last saved tweet, not last seen tweet
- **Added `skip_tweet_fetch` flag** - Allows disabling tweet fetch for problematic accounts (e.g., WLD/@sama)
- **Improved rate limit handling** - Reads actual reset time from API headers, skips remaining assets on rate limit
- **Added fetch state persistence** - `data/fetch_state.json` tracks skipped assets for priority on next run

### Workflow Improvements
- **Cache vs Release DB logic** - Changed to `-ge` (prefer release when equal) to ensure fixes propagate
- **Cache deletion strategy** - Delete cache when watermark fixes need to propagate
- **Better logging** - Clear distinction between founder (all tweets) and adopter (keyword filtered) in logs

### Data Integrity
- **33 new tweets recovered** - ASTER (12), JUP (14), MONAD (5), META (2) - previously missed due to watermark bug

## Dec 28, 2025 - Canonical Table Architecture
- **New architecture:** Two-table system for DuckDB to fix JSON/DB sync issues
  - `prices` → Canonical table (matches website JSONs exactly)
  - `prices_RAW_INGESTION` → Raw API data (for debugging only)
- New scripts: `migrate_to_canonical.py`, `import_canonical.py`
- Automatic canonical sync after every export
- Idempotent migration runs in GitHub Actions workflow
- Updated GOTCHAS.md and CLAUDE.md with prominent architecture docs
- Fix git push conflicts in hourly workflow with retry logic

## Dec 28, 2025 - ZEC Asset & Workflow Fixes
- Add ZEC asset (adopter: mert)
- Fix workflow failures - validate cache vs release, use CoinGecko Pro API
- Add missing `get_latest_price_timestamp` function to db.py

## Dec 23, 2025 - Market Cap Tracking & Data Enhancements
- Backfill WIF tweets from Dec 2023 (30 new early tweets)
- Add market cap column to Data Table with historical backfill
- Add `supply_unstable` flag for assets like JUP/WLD
- Redesign Data Table with price context and full tweet text
- Add "First Tweet" navigation button

## Dec 21-22, 2025 - Asset Expansion Phase
- **New assets:** FARTCOIN (DipWheeler), WLD (Sam Altman), XPL (Plasma), WIF
- Add `--keyword-search` mode to nitter_scraper.py
- Hybrid arrow/bubble markers based on zoom level
- Bi-directional tweet navigation system
- Replace ClusterDrawer with TweetImpactCard
- Unified tweet filtering for founders and adopters
- Comprehensive data quality validation tools

## Dec 20, 2025 - ZORA Launch & Mobile Overhaul
- **New asset:** ZORA (@js_horne)
- Complete price history recovery from Birdeye
- Post-export validation with auto-fix capability
- Mobile UI Phase 1 & 2: bottom tab navigation, viewport fixes
- Data overrides system for persistent fixes
- Chart resize loop prevention

## Dec 19-20, 2025 - Premium Visual Redesign
- Premium CSS reskin with Inter font and design tokens
- Impact-encoded tweet markers (green=pump, red=dump)
- Direction-aware silence line labels
- Data Table redesign with primitive table philosophy
- TopMovers component and days-since-tweet indicator
- Admin UI for adding new assets
- Automatic fake wick capping at export time

## Dec 19, 2025 - Data Pipeline & Chart Engine
- Proper incremental price fetching
- Hourly data update workflow (GitHub Actions)
- Complete chart engine overhaul for accurate markers
- DST timestamp deduplication
- GOTCHAS.md documentation to prevent regressions
- Comprehensive Chart.tsx comments

## Dec 18-19, 2025 - Historical Data Recovery
- ASTER/CZ tweets backfill (Sept 17-20)
- JUP backfill: 321 tweets (Jan 2024 - Mar 2025)
- Fix duplicate timestamps in JUP and BELIEVE
- All data consolidated into DuckDB

## Dec 17-18, 2025 - Multi-Asset Foundation
- Multi-asset support added to frontend
- Cluster zoom with micro-delights
- Adaptive tweet heat visualization
- TradingView-style interface with separate pages
- Token logos and proper ticker names
- Timeframe-adaptive tweet markers

## Dec 17, 2025 - Project Genesis
- Initial commit with TradingView-style chart
- Core chart implementation with lightweight-charts v4
- Basic tweet-price correlation visualization

---

## Assets (15 total)

ASTER, BELIEVE, FARTCOIN, GORK, HYPE, JUP, META, MONAD, PUMP, USELESS, WIF, WLD, XPL, ZEC, ZORA

## Key Infrastructure

- **Data sources:** X API, CoinGecko, GeckoTerminal, Birdeye, Nitter
- **Pipeline:** fetch → DuckDB → export_static → JSON → Frontend
- **Deployment:** Vercel with hourly automated updates
- **Quality:** 5-sigma outlier detection, validation safeguards, data overrides system
