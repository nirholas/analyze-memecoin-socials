# Data Quality Issues: Diagnosis & Fixes

This document is the **definitive reference** for diagnosing and fixing data quality issues in the tweet-price pipeline. When you encounter a chart bug, **START HERE**.

---

## Quick Diagnosis Checklist

| Symptom | Likely Cause | Fix Location |
|---------|--------------|--------------|
| Chart shows 2+ candles per day | **DOUBLE CANDLE** issue | `export_static.py` (auto-fixed) |
| Chart crashes with "Value is null" | Duplicate timestamps | `export_static.py` DST dedup |
| Fake price spike on chart | MEV bot / fat finger | `export_static.py` cap_fake_wicks |
| Wrong price for old tweets | Staleness limit issue | `db.py` tweet_events view |
| Missing recent data | Fetch not running | Check GitHub workflow |
| Gaps in historical data | Accidental deletion | Restore from backup (see below) |

---

## DOUBLE CANDLE Issue

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

When data from multiple sources is merged, you get **multiple candles per day** with different timestamps:

```
2024-12-05 00:00:00 UTC: O=13.21, C=12.72  ← from source A
2024-12-05 05:00:00 UTC: O=13.21, C=12.72  ← from source B (duplicate!)
```

### How It's Fixed (Automatic)

`export_static.py` automatically normalizes all 1D timestamps to midnight UTC:

```python
if timeframe == "1d":
    # Snap to midnight UTC
    normalized_ts = (ts_epoch // 86400) * 86400
```

After normalization, both candles have the same timestamp:
```
2024-12-05 00:00:00 UTC: O=13.21, C=12.72  ← kept
2024-12-05 00:00:00 UTC: O=13.21, C=12.72  ← deduplicated (skipped)
```

The deduplication logic then keeps only the first candle for each timestamp.

### Verification

After running `python export_static.py`, check for the log messages:
```
(Normalized 379 timestamps to midnight UTC)
(Deduplicated 379 candles - same day from different sources)
```

### If It Happens Again

1. **First:** Just re-run `python export_static.py` - it should auto-fix
2. **If still broken:** Check if the asset's price_source changed
3. **If data looks wrong:** Check the database directly:
   ```bash
   python3 -c "
   import duckdb
   conn = duckdb.connect('data/analytics.duckdb', read_only=True)
   result = conn.execute('''
       SELECT EXTRACT(HOUR FROM timestamp) as hour, COUNT(*)
       FROM prices WHERE asset_id = 'hype' AND timeframe = '1d'
       GROUP BY hour ORDER BY COUNT(*) DESC
   ''').fetchall()
   print(result)
   "
   ```
   If you see multiple hours with counts, the data has mixed sources.

### Historical Incidents

| Date | Asset | Issue | Resolution |
|------|-------|-------|------------|
| Dec 2024 | JUP, BELIEVE, ASTER, MONAD | Mixed timestamps (00:00 + 04:00/05:00) | Manual DB cleanup + re-export |
| Dec 2024 | HYPE | Same issue recurred after data re-fetch | Added auto-fix to export_static.py |
| Dec 2024 | HYPE | 92 candles deleted as "duplicates" but had 0 overlap | Restored from backup, added NEVER DELETE rule to GOTCHAS.md |

---

## Duplicate Timestamps (DST)

### What It Looks Like

Chart crashes with error: `Value is null` from lightweight-charts library.

### Root Cause

During Daylight Saving Time "fall back" transitions, two different wall-clock times can map to the **same Unix timestamp**. The charting library requires strictly unique timestamps.

### How It's Fixed (Automatic)

`export_static.py` deduplicates timestamps:

```python
if ts_epoch in seen_timestamps:
    duplicates_skipped += 1
    continue
seen_timestamps.add(ts_epoch)
```

### Verification

The export has a safety check that will **fail loudly** if duplicates slip through:

```python
if len(timestamps) != len(set(timestamps)):
    raise ValueError(f"CRITICAL: Duplicate timestamps in {filepath}!")
```

---

## Fake Wicks (MEV/Fat Finger)

### What It Looks Like

A single candle has an extreme HIGH or LOW spike that's obviously wrong (e.g., price momentarily shows $0.02 when normal range is $0.008).

### Root Cause

- MEV bots executing at extreme prices
- Fat finger trades
- Data errors from source APIs
- Sniper bots on new tokens

### How It's Fixed (Automatic)

`export_static.py` caps fake wicks:

```python
# If HIGH > 2x the candle body, cap it
max_allowed_high = max(open, close) * WICK_CAP_MULTIPLIER
capped_high = min(high, max_allowed_high)
```

This preserves legitimate price action while removing obvious spikes.

### If Auto-Fix Doesn't Catch It

Add a manual override to `scripts/data_overrides.json`:

```json
{
  "id": "pump-fake-wick-sept18",
  "asset_id": "pump",
  "timeframe": "1h",
  "timestamp": "2025-09-18T18:00:00Z",
  "action": "cap_high",
  "value": 0.009,
  "reason": "Fake wick from MEV bot. Original HIGH was ~0.02."
}
```

---

## Accidental Data Deletion

### What It Looks Like

Chart suddenly shows gaps/missing candles that weren't there before. Data coverage drops significantly.

### Root Cause

Someone ran a DELETE query on the database assuming data was "duplicate" without verifying overlap.

### December 2024 Incident

A cleanup deleted 92 HYPE candles marked as `sqlite_migration` source, assuming they duplicated
data from other sources. They didn't—**zero overlap**. 24% of HYPE daily data was lost.

### Prevention

**NEVER delete data without running the overlap verification query in GOTCHAS.md.**

### Recovery

If you have a backup:
```bash
# Extract backup DB
unzip data_backup_YYYYMMDD.zip -d /tmp/

# Restore missing data
python3 << 'PYEOF'
import sqlite3, duckdb
from datetime import datetime

# Read from backup
backup = sqlite3.connect('/tmp/backup/asset/prices.db')
data = backup.execute('SELECT * FROM ohlcv WHERE timeframe = "1d"').fetchall()

# Insert into current DB
db = duckdb.connect('data/analytics.duckdb')
for row in data:
    db.execute('INSERT INTO prices ... ON CONFLICT DO NOTHING', row)
db.commit()
PYEOF
```

---

## Adding a New Asset: Data Quality Checklist

When adding a new asset, verify data quality BEFORE going live:

### 1. Check for Double Candles
```bash
python3 -c "
import duckdb
conn = duckdb.connect('data/analytics.duckdb', read_only=True)
result = conn.execute('''
    SELECT EXTRACT(HOUR FROM timestamp) as hour, COUNT(*)
    FROM prices WHERE asset_id = 'NEW_ASSET' AND timeframe = '1d'
    GROUP BY hour ORDER BY COUNT(*) DESC
''').fetchall()
for r in result:
    print(f'Hour {int(r[0]):02d}:00 - {r[1]} candles')
"
```
**Expected:** Only one hour with candles (usually 00:00)
**Problem:** Multiple hours = double candle issue incoming

### 2. Check for Fake Wicks
```bash
python3 -c "
import duckdb
conn = duckdb.connect('data/analytics.duckdb', read_only=True)
result = conn.execute('''
    SELECT timestamp, high, close, high/close as ratio
    FROM prices WHERE asset_id = 'NEW_ASSET' AND timeframe = '1h'
    ORDER BY ratio DESC LIMIT 10
''').fetchall()
for r in result:
    print(f'{r[0]}: ratio={r[3]:.2f}x')
"
```
**Expected:** Ratios < 2x
**Problem:** Ratio > 2x = potential fake wick

### 3. Check for Missing Data
```bash
python3 -c "
import duckdb
conn = duckdb.connect('data/analytics.duckdb', read_only=True)
result = conn.execute('''
    SELECT timeframe, MIN(timestamp), MAX(timestamp), COUNT(*)
    FROM prices WHERE asset_id = 'NEW_ASSET'
    GROUP BY timeframe
''').fetchall()
for r in result:
    print(f'{r[0]}: {r[1]} to {r[2]} ({r[3]} candles)')
"
```

### 4. Run Export and Check Logs
```bash
python export_static.py --asset NEW_ASSET
```
Watch for:
- `(Normalized X timestamps to midnight UTC)` - expected for 1D
- `(Deduplicated X candles)` - expected if double candle was present
- `(Capped X fake wicks)` - check if X is reasonable

---

## Standard Operating Procedure: Fixing Data Issues

### If You See Bad Data on the Chart

1. **Identify the issue type** using the table at the top
2. **Check if it's already auto-fixed** by running `python export_static.py`
3. **If not auto-fixed**, add to `scripts/data_overrides.json`
4. **Document** what you found and how you fixed it

### If You're Adding a Manual Fix

1. **Add to `data_overrides.json`** with a clear `reason` field
2. **Run export** to verify the fix works
3. **Check the chart** to confirm it looks right
4. **Commit** with a descriptive message

### If You're Tempted to Edit the Database Directly

**DON'T.** Manual DB edits:
- Get lost when data is re-fetched
- Aren't documented
- Can't be undone easily
- Don't help future developers

Instead:
- For one-off fixes: Use `data_overrides.json`
- For systematic fixes: Add logic to `export_static.py`

---

## Files That Handle Data Quality

| File | What It Does |
|------|--------------|
| `scripts/export_static.py` | 1D normalization, DST dedup, fake wick capping |
| `scripts/data_overrides.json` | Manual per-candle fixes |
| `scripts/db.py` | Staleness limits in tweet_events view |
| `scripts/fetch_prices.py` | Outlier detection (5-sigma) at fetch time |
| `GOTCHAS.md` | Quick reference for common issues |

---

*Last updated: December 2024*
*If this doc didn't help you fix your issue, UPDATE IT with what you learned.*
