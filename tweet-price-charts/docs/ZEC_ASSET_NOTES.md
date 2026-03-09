# ZEC Asset Addition - Data Notes & Known Limitations

## Overview

ZEC (Zcash) is an **ADOPTER ASSET** - tracking tweets from [@mert](https://twitter.com/mert_) who is a prominent figure advocating for ZEC rather than a founder. This differs from most other assets in the system which track founder tweets.

## Asset Configuration

| Field | Value |
|-------|-------|
| ID | `zec` |
| Name | ZEC |
| Founder (Adopter) | mert |
| Network | zcash |
| CoinGecko ID | zcash |
| Price Source | coingecko |
| Launch Date | 2016-10-28 |
| Color | #F4B728 |
| Keyword Filter | `zec,zcash` |

## Data Limitations

### 1. Small Tweet Sample Size

- **Only 38 tweet-days analyzed** (as of Dec 2025)
- Total tweets collected: 89
- Date range: Aug 2025 - Dec 2025
- The small sample significantly reduces statistical confidence

### 2. Statistical Significance Issues

- **Win rate: 47.4%** - essentially random (50%)
- Tweet-day avg return: 1.31% vs no-tweet day: 0.19%
- T-statistic: 0.878, P-value: 0.3801 (NOT significant)
- Correlation: 0.224 (weak) but P-value: 0.0 (significant due to large sample of price days)

### 3. Historical Price Granularity

- **Skip 15m and 1m timeframes** - CoinGecko doesn't provide granular historical data for legacy coins like ZEC
- 1h data limited before 2021 - early history only has daily granularity
- 1d data available from launch (Oct 2016)

### 4. Tweet Filtering

- Filtered by keywords: `zec,zcash`
- Only includes tweets explicitly mentioning $ZEC or Zcash
- May miss broader market commentary that affects ZEC price

## Quiet Periods Observed

Notable gaps in tweet activity with significant price movements:

| Period | Gap (days) | Price Change |
|--------|------------|--------------|
| Aug 31 - Oct 06, 2025 | 35.6 | +309.1% |
| Oct 06 - Oct 15, 2025 | 9.2 | +58.5% |
| Oct 30 - Nov 08, 2025 | 8.4 | +88.0% |

These large price moves during tweet silence suggest external factors (market conditions, ZEC-specific news) may dominate over adopter influence.

## Technical Notes

### Data Sources
- **Price data**: CoinGecko API
- **Tweet data**: Scraped via existing pipeline (uses keyword filter)
- **Historical backfill**: CoinGecko historical API

### Files Generated
```
web/public/static/zec/
├── prices_1d.json      # Daily candles (full history from 2016)
├── prices_1h.json      # Hourly candles (limited pre-2021)
├── stats.json          # Computed statistics
└── tweet_events.json   # Filtered tweet events
```

## Recommendations for Review

1. **Consider longer observation period** - 38 tweet-days may not be representative
2. **Monitor for pattern changes** - Current data shows near-random correlation
3. **Compare with founder-type assets** - Adopter influence may be fundamentally different
4. **Watch for major ZEC announcements** - External events likely dominate

## Known Issues to Monitor

- [ ] CoinGecko rate limits may affect future data fetches
- [ ] Circulating supply (16.5M) may need periodic updates
- [ ] Keyword filter may need expansion if mert uses other ZEC-related terms

---

*Last updated: Dec 2025*
*Generated during feature/add-zec-asset branch creation*

