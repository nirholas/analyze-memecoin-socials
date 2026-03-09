# Tweet-Price Correlation Analyzer

**Do founder tweets move token prices?**

A multi-asset analytics platform that visualizes and analyzes the relationship between crypto project founders' tweets and their token's price action.

## Live Site

**https://tweet-price-charts.vercel.app**

📜 [View Changelog](CHANGELOG.md)

## Current State

### What's Built
- **Interactive chart** with TradingView-style candlesticks + tweet markers as avatar bubbles
- **Data table** sorted by price impact - instantly see which tweets moved price most
- **15 assets tracked** across 7 networks (Solana, Hyperliquid, BSC, Monad, Base, Ethereum, Zcash)
- **4,700+ tweet events** with aligned price data
- **Multi-source price fetching** (GeckoTerminal, Birdeye, CoinGecko, Hyperliquid)
- **Nitter scraper** for historical tweet backfill
- **CLI tools** for adding assets and validating data integrity
- **Automated hourly updates** via GitHub Actions with truncation protection
- **Static export** for fast CDN delivery via Vercel

### Assets Tracked

| Token | Founder | Type | Network | Tweets |
|-------|---------|------|---------|--------|
| FARTCOIN | [@DipWheeler](https://x.com/DipWheeler) | Adopter | Solana | 1,875 |
| JUP | [@weremeow](https://x.com/weremeow) | Founder | Solana | 680 |
| ZORA | [@js_horne](https://x.com/js_horne) | Founder | Base | 449 |
| USELESS | [@theunipcs](https://x.com/theunipcs) | Adopter | Solana | 435 |
| ASTER | [@cz_binance](https://x.com/cz_binance) | Founder | BSC | 318 |
| WIF | [@blknoiz06](https://x.com/blknoiz06) | Adopter | Solana | 300 |
| META | [@metaproph3t](https://x.com/metaproph3t) | Founder | Solana | 142 |
| GORK | [@elonmusk](https://x.com/elonmusk) | Adopter | Solana | 106 |
| PUMP | [@a1lon9](https://x.com/a1lon9) | Founder | Solana | 102 |
| ZEC | [@mert_](https://x.com/mert_) | Adopter | Zcash | 89 |
| LAUNCHCOIN | [@pasternak](https://x.com/pasternak) | Founder | Solana | 81 |
| MON | [@keoneHD](https://x.com/keoneHD) | Founder | Monad | 66 |
| HYPE | [@chameleon_jeff](https://x.com/chameleon_jeff) | Founder | Hyperliquid | 35 |
| XPL | [@pauliepunt](https://x.com/pauliepunt) | Founder | BSC | 26 |
| WLD | [@sama](https://x.com/sama) | Adopter | Ethereum | 2 |

**Founder vs Adopter:** Founders created the token (all tweets stored). Adopters are influencers who adopted it (only keyword-matching tweets stored).

## Features

### About (`/about`)
- **Tool Contract landing page** - Clear claim: "Most founder tweets do nothing. Some coincide with big moves."
- **ImpactExplorer** - Scatter plot of all tweets vs price changes with token logos
- **"Biggest moves" filter** - Toggle to show only tweets with 15%+ price changes
- **TweetTimeHeatmap** - Hour-of-day patterns showing when founders tweet
- **SilencesExplorer** - Notable quiet periods with price context
- **AssetGrid** - Pick a token to explore on the chart

### Chart (`/chart`)
- **TradingView-style candlesticks** with multiple timeframes (1m, 15m, 1h, 1D)
- **Tweet markers** as founder avatar bubbles overlaid on price
- **Silence gaps** - dashed lines showing quiet periods with % price change
- **Smart clustering** - nearby tweets grouped with count badges
- **Click-to-tweet** - click any marker to open the original tweet

### Data Table (`/data`)
- **Sortable columns** - default sorted by 24h impact (biggest moves first)
- **Market cap at tweet** - see token scale when each tweet was posted
- **Tweet Days stats** - avg return and win rate when founder tweets
- **Price impact** - 1h and 24h % change after each tweet (green/red)
- **Clickable tweets** - click any row to open original on X
- **Search & export** - filter tweets, export full dataset to CSV

### Multi-Asset Support
- **Asset selector** dropdown to switch between tracked projects
- **Per-asset statistics** computed independently
- **Multi-network** - Solana, Hyperliquid, BSC, Monad, Base, Ethereum

## Quick Start

### Prerequisites
- Python 3.9+
- Node.js 18+
- X API Bearer Token
- Birdeye API Key (optional, for historical backfill)

### Setup

```bash
# Clone
git clone https://github.com/rohunvora/tweet-price.git
cd tweet-price

# Python setup
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Create .env with your API tokens
cat > .env << EOF
X_BEARER_TOKEN=your_x_api_token
BIRDEYE_API_KEY=your_birdeye_key
EOF
```

### Fetch Data

```bash
# Fetch tweets for all configured assets
python scripts/fetch_tweets.py

# Fetch price data (GeckoTerminal, Birdeye, CoinGecko, Hyperliquid)
python scripts/fetch_prices.py

# Align tweets with price candles
python scripts/align_tweets.py

# Compute statistics
python scripts/compute_stats.py

# Export static JSON for frontend
python scripts/export_static.py
```

### Run Frontend

```bash
cd web
npm install
npm run dev
```

Open http://localhost:3000

## Project Structure

```
tweet-price/
├── scripts/
│   ├── assets.json         # Asset configuration (tokens, founders, networks)
│   ├── config.py           # Global settings
│   ├── db.py               # DuckDB database management
│   ├── fetch_tweets.py     # X API tweet fetcher (incremental)
│   ├── nitter_scraper.py   # Nitter-based historical tweet scraper
│   ├── fetch_prices.py     # Multi-source price fetcher
│   ├── align_tweets.py     # Tweet-price alignment
│   ├── compute_stats.py    # Statistical analysis
│   ├── export_static.py    # JSON export for frontend
│   ├── add_asset.py        # CLI for adding new assets
│   ├── fetch_supply.py     # Circulating supply fetcher (for market cap)
│   └── validate_export.py  # Data integrity validation
│
├── data/
│   ├── analytics.duckdb    # Main DuckDB database
│   └── {asset_id}/         # Per-asset raw data
│
├── web/                    # Next.js frontend
│   ├── src/
│   │   ├── app/
│   │   │   ├── about/      # About page with ImpactExplorer
│   │   │   ├── chart/      # Chart page with candlesticks + tweet markers
│   │   │   └── data/       # Data table page
│   │   ├── components/
│   │   │   ├── Chart.tsx            # TradingView-style chart
│   │   │   ├── DataTable.tsx        # Sortable tweet table
│   │   │   ├── StatsPanel.tsx       # Statistics display
│   │   │   ├── AssetSelector.tsx
│   │   │   ├── ImpactExplorer.tsx   # Scatter plot (tweets vs price)
│   │   │   ├── TweetTimeHeatmap.tsx # Hour-of-day patterns
│   │   │   ├── SilencesExplorer.tsx # Quiet periods
│   │   │   └── AssetGrid.tsx        # Token picker
│   │   └── lib/
│   │       ├── dataLoader.ts   # Data fetching
│   │       ├── formatters.ts   # Display utilities
│   │       └── types.ts        # TypeScript interfaces
│   │
│   └── public/static/      # Pre-exported JSON data
│       ├── assets.json
│       └── {asset_id}/
│           ├── prices_*.json
│           └── tweet_events.json
│
└── vercel.json             # Deployment config
```

## How It Works

1. **Fetch tweets** from founder accounts via X API v2 (incremental) or Nitter scraper (historical backfill)
2. **Fetch prices** from multiple sources:
   - GeckoTerminal (DEX pools)
   - Birdeye (Solana historical backfill)
   - CoinGecko (listed tokens)
   - Hyperliquid (perp/spot)
3. **Outlier detection** - 5-sigma filtering removes sniper bot trades
4. **Align** each tweet with the price candle at that exact minute
5. **Calculate** 1h and 24h price changes after each tweet
6. **Export** to static JSON for fast CDN delivery
7. **Visualize** on interactive chart with tweet markers

## Adding a New Asset

Use the CLI tool for a guided setup:

```bash
cd scripts

# CoinGecko-listed token (simplest)
python add_asset.py mytoken --name "My Token" --founder twitterhandle --coingecko my-token-id

# DEX token with auto-discovery of best price source
python add_asset.py mytoken --name "My Token" --founder twitterhandle --coingecko my-token-id --auto-best

# Validate only (dry run)
python add_asset.py mytoken --name "My Token" --founder twitterhandle --coingecko my-token-id --dry-run
```

The CLI will:
1. Validate the Twitter handle and CoinGecko ID
2. Discover the best price source (GeckoTerminal pools vs CoinGecko)
3. Add the asset to `assets.json`
4. Fetch tweets and prices
5. Download the logo from CoinGecko
6. Export static files for the frontend
7. Run validation to ensure data integrity

## Tech Stack

**Backend:** Python, DuckDB, pandas, httpx  
**Frontend:** Next.js 15, React 19, TypeScript, Tailwind CSS v4  
**Charts:** lightweight-charts (TradingView)  
**Tables:** TanStack React Table  
**Deployment:** Vercel (static export)

## What's Next

- [x] ~~Market cap display instead of/alongside price~~ ✓ Done (Dec 29)
- [ ] More assets based on community requests
- [ ] Real-time tweet notifications
- [ ] Enhanced about page with founder profiles

## Disclaimer

This is for **research and educational purposes only**. Not financial advice. Correlation ≠ causation. DYOR.

---

Built to explore whether founder activity correlates with token price movements.
