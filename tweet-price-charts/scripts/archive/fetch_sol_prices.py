"""
Fetch SOL/USD price data from GeckoTerminal for market comparison.
Uses the Raydium SOL/USDC pool as the reference.
"""
from typing import Optional, List, Dict, Tuple
import httpx
import sqlite3
import time
import json
from datetime import datetime
from pathlib import Path
from config import (
    DATA_DIR, TIMEFRAMES, TIMEFRAME_TO_GT, PUBLIC_DATA_DIR
)

# GeckoTerminal API
GT_API = "https://api.geckoterminal.com/api/v2"
MAX_CANDLES_PER_REQUEST = 1000
RATE_LIMIT_DELAY = 0.5

# Major SOL/USDC pool on Raydium (high liquidity, good price reference)
SOL_USDC_POOL = "58oQChx4yWmvKdwLLZzBi4ChoCc2fqCUWBkwMihLYQo2"

# Database for SOL prices
SOL_PRICES_DB = DATA_DIR / "sol_prices.db"


def init_db(db_path=SOL_PRICES_DB) -> sqlite3.Connection:
    """Initialize SQLite database."""
    DATA_DIR.mkdir(exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ohlcv (
            timeframe TEXT NOT NULL,
            timestamp_epoch INTEGER NOT NULL,
            open REAL NOT NULL,
            high REAL NOT NULL,
            low REAL NOT NULL,
            close REAL NOT NULL,
            volume REAL NOT NULL,
            PRIMARY KEY (timeframe, timestamp_epoch)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_tf_ts 
        ON ohlcv(timeframe, timestamp_epoch)
    """)
    conn.commit()
    return conn


def fetch_ohlcv_page(
    pool_address: str,
    timeframe: str,
    before_timestamp: Optional[int] = None
) -> Tuple[List[Dict], Optional[int]]:
    """Fetch a single page of OHLCV data from GeckoTerminal."""
    tf_type, aggregate = TIMEFRAME_TO_GT[timeframe]
    url = f"{GT_API}/networks/solana/pools/{pool_address}/ohlcv/{tf_type}"
    
    params = {
        "aggregate": aggregate,
        "limit": MAX_CANDLES_PER_REQUEST,
        "currency": "usd",
    }
    
    if before_timestamp:
        params["before_timestamp"] = before_timestamp
    
    with httpx.Client(timeout=30.0) as client:
        response = client.get(url, params=params)
        
        if response.status_code == 429:
            print("  Rate limited, waiting 60s...")
            time.sleep(60)
            return fetch_ohlcv_page(pool_address, timeframe, before_timestamp)
        
        if response.status_code != 200:
            print(f"  Error {response.status_code}: {response.text[:200]}")
            return [], None
        
        data = response.json()
        ohlcv_list = data.get("data", {}).get("attributes", {}).get("ohlcv_list", [])
        
        if not ohlcv_list:
            return [], None
        
        candles = []
        oldest_ts = None
        
        for candle in ohlcv_list:
            ts, o, h, l, c, v = candle
            candles.append({
                "timestamp_epoch": int(ts),
                "open": float(o),
                "high": float(h),
                "low": float(l),
                "close": float(c),
                "volume": float(v),
            })
            if oldest_ts is None or ts < oldest_ts:
                oldest_ts = ts
        
        return candles, oldest_ts


def fetch_all_timeframe(
    pool_address: str,
    timeframe: str,
    conn: sqlite3.Connection,
    max_pages: int = 50
) -> int:
    """Fetch all available data for a timeframe via pagination."""
    print(f"\nFetching {timeframe} SOL data...")
    
    total_candles = 0
    before_ts = None
    
    for page in range(max_pages):
        candles, oldest_ts = fetch_ohlcv_page(pool_address, timeframe, before_ts)
        
        if not candles:
            print(f"  Page {page + 1}: No more data")
            break
        
        conn.executemany("""
            INSERT OR REPLACE INTO ohlcv 
            (timeframe, timestamp_epoch, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [
            (timeframe, c["timestamp_epoch"], c["open"], c["high"], c["low"], c["close"], c["volume"])
            for c in candles
        ])
        conn.commit()
        
        total_candles += len(candles)
        oldest_date = datetime.utcfromtimestamp(oldest_ts).strftime("%Y-%m-%d %H:%M")
        print(f"  Page {page + 1}: {len(candles)} candles (oldest: {oldest_date})")
        
        if len(candles) < MAX_CANDLES_PER_REQUEST:
            break
        
        before_ts = oldest_ts
        time.sleep(RATE_LIMIT_DELAY)
    
    return total_candles


def export_to_json(conn: sqlite3.Connection):
    """Export SOL prices to JSON files for the frontend."""
    PUBLIC_DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    for tf in TIMEFRAMES:
        cursor = conn.execute("""
            SELECT timestamp_epoch, open, high, low, close, volume
            FROM ohlcv
            WHERE timeframe = ?
            ORDER BY timestamp_epoch ASC
        """, (tf,))
        
        rows = cursor.fetchall()
        if not rows:
            continue
        
        candles = [
            {"t": r[0], "o": r[1], "h": r[2], "l": r[3], "c": r[4], "v": r[5]}
            for r in rows
        ]
        
        data = {
            "asset": "SOL",
            "timeframe": tf,
            "count": len(candles),
            "start": candles[0]["t"] if candles else 0,
            "end": candles[-1]["t"] if candles else 0,
            "candles": candles,
        }
        
        output_file = PUBLIC_DATA_DIR / f"sol_prices_{tf}.json"
        with open(output_file, "w") as f:
            json.dump(data, f)
        
        print(f"Exported {tf}: {len(candles):,} candles to {output_file.name}")


def main():
    """Main entry point."""
    print("=" * 60)
    print("SOL Price Fetcher (for market comparison)")
    print("=" * 60)
    print(f"Pool: {SOL_USDC_POOL} (Raydium SOL/USDC)")
    print(f"Database: {SOL_PRICES_DB}")
    
    conn = init_db()
    
    # Fetch each timeframe
    for tf in TIMEFRAMES:
        max_pages = {
            "1m": 100,
            "15m": 50,
            "1h": 30,
            "1d": 10,
        }.get(tf, 20)
        
        total = fetch_all_timeframe(SOL_USDC_POOL, tf, conn, max_pages)
        print(f"  Total {tf}: {total:,} candles")
    
    # Export to JSON
    print("\n" + "=" * 60)
    print("EXPORTING TO JSON")
    print("=" * 60)
    export_to_json(conn)
    
    conn.close()
    print(f"\nDone! SOL price data saved.")


if __name__ == "__main__":
    main()

