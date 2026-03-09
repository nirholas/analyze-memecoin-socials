#!/usr/bin/env python3
"""
Fetch 1-minute OHLCV candle data for $pump-sdk from GeckoTerminal.
Covers both the bonding curve period and the PumpSwap pool period.

Token: F4ZHCzizwpop2XS8auuAGQ7LZcUTEwQ22m5sRmB2pump
Created: 2026-02-26 ~20:55 UTC (first trades)
Graduated: ~2026-02-27 03:15 UTC → PumpSwap pool

We paginate backwards from end_ts using before_timestamp to get all 1m bars
for the first 2 days (Feb 26 - Feb 28 2026).
"""

import json
import csv
import time
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

MINT = "F4ZHCzizwpop2XS8auuAGQ7LZcUTEwQ22m5sRmB2pump"
PUMPSWAP_POOL = "AS5KLCFs4HVYPebV8MNt2YzD2Fs2jgb5F2p3xXjxm9Aj"
BONDING_CURVE = "6jF4GfToHT5xz8g39f6hKMQ9S9CCv1fjWFzV37V2BtZ4"

# Time window: first 2 days from token creation
CREATED_TS = 1772138147  # Feb 27 2026 03:15:47 UTC (pump.fun created_timestamp)
# Extend back to catch bonding curve trades before graduation
START_TS = 1772060000    # Feb 26 2026 ~02:00 UTC (well before first trade)
END_TS   = CREATED_TS + (2 * 86400)  # +2 days from creation

BASE_URL = "https://api.geckoterminal.com/api/v2/networks/solana/pools"


def fetch_ohlcv_page(pool: str, before_ts: int, aggregate: int = 1, limit: int = 1000, max_retries: int = 5) -> list:
    """Fetch one page of 1-minute OHLCV bars from GeckoTerminal with retry."""
    url = (
        f"{BASE_URL}/{pool}/ohlcv/minute"
        f"?aggregate={aggregate}&limit={limit}"
        f"&before_timestamp={before_ts}&currency=usd"
    )
    for attempt in range(max_retries):
        try:
            req = urllib.request.Request(url, headers={
                "Accept": "application/json",
                "User-Agent": "pump-sdk-case-study/1.0",
            })
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read())
            break
        except urllib.error.HTTPError as e:
            if e.code == 429:
                wait = 5 * (attempt + 1)
                print(f" [429 rate limit, waiting {wait}s]", end="", flush=True)
                time.sleep(wait)
            else:
                raise
    else:
        print(" [max retries exceeded]", end="")
        return []

    bars_raw = data.get("data", {}).get("attributes", {}).get("ohlcv_list", [])
    # Each bar: [timestamp, open, high, low, close, volume]
    return [
        {
            "timestamp": int(b[0]),
            "datetime": datetime.fromtimestamp(b[0], tz=timezone.utc).isoformat(),
            "open": float(b[1]),
            "high": float(b[2]),
            "low": float(b[3]),
            "close": float(b[4]),
            "volume_usd": float(b[5]),
        }
        for b in bars_raw
    ]


def fetch_all_ohlcv(pool: str, pool_label: str, start_ts: int, end_ts: int) -> list:
    """Paginate backwards through all 1m candles in the time window."""
    all_bars = []
    cursor = end_ts
    page = 0

    while cursor > start_ts:
        page += 1
        print(f"  [{pool_label}] Page {page}: fetching before {datetime.fromtimestamp(cursor, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')} ...", end="", flush=True)

        bars = fetch_ohlcv_page(pool, before_ts=cursor)

        if not bars:
            print(" no more data.")
            break

        # Bars come newest-first; filter to our window
        bars_in_window = [b for b in bars if start_ts <= b["timestamp"] <= end_ts]
        all_bars.extend(bars_in_window)

        oldest_ts = min(b["timestamp"] for b in bars)
        print(f" got {len(bars)} bars (oldest: {datetime.fromtimestamp(oldest_ts, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')})")

        if oldest_ts <= start_ts:
            break

        # Move cursor to just before the oldest bar we got
        cursor = oldest_ts
        time.sleep(3)  # Rate limit: ~30 req/min for GeckoTerminal free tier

    # Deduplicate by timestamp and sort ascending
    seen = set()
    unique = []
    for b in all_bars:
        if b["timestamp"] not in seen:
            seen.add(b["timestamp"])
            unique.append(b)
    unique.sort(key=lambda x: x["timestamp"])
    return unique


def write_csv(bars: list, path: str):
    """Write bars to CSV."""
    if not bars:
        print(f"  No bars to write for {path}")
        return
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["timestamp", "datetime", "open", "high", "low", "close", "volume_usd"])
        w.writeheader()
        w.writerows(bars)
    print(f"  Wrote {len(bars)} bars to {path}")


def write_json(data: dict, path: str):
    """Write data to JSON."""
    with open(path, "w") as f:
        json.dump(data, f, indent=2)
    print(f"  Wrote {path}")


def main():
    out_dir = Path(__file__).parent
    print(f"\n{'='*60}")
    print(f"  Fetching 1-minute OHLCV for $pump-sdk")
    print(f"  Mint: {MINT}")
    print(f"  Window: {datetime.fromtimestamp(START_TS, tz=timezone.utc)} → {datetime.fromtimestamp(END_TS, tz=timezone.utc)}")
    print(f"{'='*60}\n")

    # 1. Bonding curve (pre-graduation)
    print("Phase 1: Bonding Curve (pre-graduation)")
    bonding_bars = fetch_all_ohlcv(BONDING_CURVE, "bonding", START_TS, END_TS)
    print(f"  Total bonding curve bars: {len(bonding_bars)}")
    if bonding_bars:
        print(f"  Range: {bonding_bars[0]['datetime']} → {bonding_bars[-1]['datetime']}")
    print()

    time.sleep(2)

    # 2. PumpSwap pool (post-graduation)
    print("Phase 2: PumpSwap Pool (post-graduation)")
    pool_bars = fetch_all_ohlcv(PUMPSWAP_POOL, "pumpswap", START_TS, END_TS)
    print(f"  Total PumpSwap bars: {len(pool_bars)}")
    if pool_bars:
        print(f"  Range: {pool_bars[0]['datetime']} → {pool_bars[-1]['datetime']}")
    print()

    # 3. Merge both phases (bonding curve first, then pool)
    # They shouldn't overlap much, but deduplicate just in case
    all_bars = bonding_bars + pool_bars
    seen = set()
    merged = []
    for b in sorted(all_bars, key=lambda x: x["timestamp"]):
        if b["timestamp"] not in seen:
            seen.add(b["timestamp"])
            merged.append(b)

    print(f"\nMerged total: {len(merged)} 1-minute candles")
    if merged:
        print(f"Full range: {merged[0]['datetime']} → {merged[-1]['datetime']}")

    # 4. Export
    print("\nExporting...")
    write_csv(bonding_bars, str(out_dir / "pump-sdk-1m-bonding-curve.csv"))
    write_csv(pool_bars, str(out_dir / "pump-sdk-1m-pumpswap.csv"))
    write_csv(merged, str(out_dir / "pump-sdk-1m-merged-first-2-days.csv"))

    full_export = {
        "mint": MINT,
        "token": "pump-sdk",
        "created_timestamp": CREATED_TS,
        "created_datetime": datetime.fromtimestamp(CREATED_TS, tz=timezone.utc).isoformat(),
        "ath_market_cap_usd": 423490.28,
        "ath_timestamp": 1772164921,
        "ath_datetime": datetime.fromtimestamp(1772164921, tz=timezone.utc).isoformat(),
        "data_window": {
            "start": datetime.fromtimestamp(START_TS, tz=timezone.utc).isoformat(),
            "end": datetime.fromtimestamp(END_TS, tz=timezone.utc).isoformat(),
        },
        "bonding_curve": {
            "pool": BONDING_CURVE,
            "candle_count": len(bonding_bars),
            "bars": bonding_bars,
        },
        "pumpswap_pool": {
            "pool": PUMPSWAP_POOL,
            "candle_count": len(pool_bars),
            "bars": pool_bars,
        },
        "merged": {
            "candle_count": len(merged),
            "bars": merged,
        },
    }
    write_json(full_export, str(out_dir / "pump-sdk-1m-first-2-days.json"))

    print(f"\nDone! {len(merged)} total 1-minute candles exported.")


if __name__ == "__main__":
    main()
