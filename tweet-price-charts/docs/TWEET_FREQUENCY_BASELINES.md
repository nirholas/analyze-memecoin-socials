# Tweet Frequency Baselines

**Analysis Date**: 2025-12-20

## Executive Summary

Baseline frequency statistics for 7 founders, establishing normal activity patterns and thresholds for detecting unusual silence or burst activity.

---

## Frequency Spectrum

| Rank | Founder | Asset | Tweets/Week | Tweets/Day | Active Since |
|------|---------|-------|-------------|------------|--------------|
| 1 | @cz_binance | aster | 23.27 | 3.32 | 92 days |
| 2 | @keoneHD | monad | 17.89 | 2.56 | 24 days |
| 3 | @theunipcs | useless | 14.62 | 2.09 | 201 days |
| 4 | @weremeow | jup | 6.80 | 0.97 | 681 days |
| 5 | @a1lon9 | pump | 6.27 | 0.90 | 114 days |
| 6 | @pasternak | believe | 3.34 | 0.48 | 170 days |
| 7 | @chameleon_jeff | hype | 0.71 | 0.10 | 336 days |

**Classification:**
- **High Frequency** (>10/week): aster, monad, useless
- **Medium Frequency** (3-10/week): jup, pump, believe
- **Low Frequency** (<3/week): hype

---

## Unusual Silence Thresholds (2σ)

What constitutes an unusual gap between tweets for each founder:

| Founder | Normal Gap | Unusual Gap (2σ) | Longest Observed |
|---------|------------|------------------|------------------|
| @cz_binance | 7.3h (0.3d) | **>28.7h (1.2d)** | 76.1h (3.2d) |
| @keoneHD | 9.6h (0.4d) | **>32.0h (1.3d)** | 60.6h (2.5d) |
| @theunipcs | 11.5h (0.5d) | **>44.8h (1.9d)** | 118.5h (4.9d) |
| @a1lon9 | 27.1h (1.1d) | **>104.1h (4.3d)** | 289.4h (12.1d) |
| @pasternak | 51.0h (2.1d) | **>176.5h (7.4d)** | 316.5h (13.2d) |
| @weremeow | 24.7h (1.0d) | **>244.4h (10.2d)** | 2625.4h (109.4d)* |
| @chameleon_jeff | 244.3h (10.2d) | **>737.9h (30.7d)** | 1083.2h (45.1d) |

*Note: @weremeow had a 109-day gap during which tweet frequency is effectively zero.*

---

## Unusual Activity Thresholds (2σ)

What constitutes an unusually high number of tweets per week:

| Founder | Baseline | Burst Threshold | Observed Bursts |
|---------|----------|-----------------|-----------------|
| @cz_binance | 23.3/wk | **>42.8/wk** | None detected |
| @keoneHD | 17.9/wk | **>29.1/wk** | None detected |
| @theunipcs | 14.6/wk | **>27.7/wk** | 1 burst (30 tweets, Jul 2025) |
| @a1lon9 | 6.3/wk | **>14.7/wk** | None detected |
| @pasternak | 3.3/wk | **>8.1/wk** | 1 burst (11 tweets, May 2025) |
| @weremeow | 6.8/wk | **>32.4/wk** | 3 bursts (Jul-Aug 2025: 48, 89, 37 tweets) |
| @chameleon_jeff | 0.7/wk | **>2.5/wk** | 2 bursts (Jan, Oct 2025) |

---

## Time-of-Day Patterns (UTC)

Peak activity hours for each founder:

| Founder | Peak Hours | Pattern |
|---------|------------|---------|
| @cz_binance | 13:00, 00:00, 02:00 | Bimodal: midday + late night/early morning |
| @keoneHD | 11:00, 10:00, 13:00 | Morning-focused (10-13 UTC) |
| @theunipcs | 08:00, 05:00, 15:00 | Spread: early morning + afternoon |
| @a1lon9 | 15:00, 17:00, 18:00 | Afternoon/evening concentration |
| @pasternak | 17:00, 12:00, 13:00 | Midday + evening |
| @weremeow | 13:00, 09:00, 21:00 | Trimodal: morning, midday, evening |
| @chameleon_jeff | 12:00, 00:00, 01:00 | Scattered: midday + midnight |

---

## Key Insights

### 1. Frequency Variability
- **Consistent posters**: @cz_binance (std: 10.5h), @keoneHD (std: 11.2h)
- **Bursty posters**: @weremeow (std: 109.8h), @chameleon_jeff (std: 246.8h)

### 2. Detection Guidelines

**For Real-Time Monitoring:**
- **Silence Alert**: Gap exceeds 2σ threshold for that founder
- **Activity Alert**: Weekly tweet count exceeds 2σ above baseline

**Example:**
- If @cz_binance goes >28.7 hours without tweeting → unusual silence
- If @weremeow posts >32 tweets in a week → unusual burst

### 3. Historical Burst Periods

Most significant burst detected:
- **@weremeow (jup)**: 89 tweets in one week (Jul 28 - Aug 3, 2025)
  - 10.5x baseline frequency
  - Likely corresponds to major announcement/event

### 4. Gap Distribution
- High-frequency posters have **lognormal** gap distributions (many short gaps, few long ones)
- Low-frequency posters have more **uniform** distributions

---

## Statistical Methodology

- **Mean Gap**: Average time between consecutive tweets
- **2σ Threshold**: Mean + 2 standard deviations (captures ~97.7% of normal behavior)
- **Weekly Variance**: Calculated across all complete weeks in dataset
- **Burst Detection**: Weeks with tweet count >mean + 2σ
- **Quiet Detection**: Weeks with tweet count <mean - 2σ

---

## Limitations

1. **Sample size**: @monad only has 24 days of data (low confidence)
2. **Seasonality**: Not accounted for (e.g., holiday patterns)
3. **Event correlation**: Thresholds don't account for known events
4. **Evolution**: Baseline may drift over time (not tracked)

---

## Recommendations

1. **Use asset-specific thresholds** - do not apply generic rules
2. **Monitor 2σ gaps** as potential signals for price movement
3. **Track burst periods** - often precede major announcements
4. **Consider time zones** - some founders consistently tweet at specific hours
5. **Update baselines quarterly** - founder behavior may evolve
