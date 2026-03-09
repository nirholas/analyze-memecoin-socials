"""
DuckDB database management for tweet-price analytics.

=============================================================================
DATABASE ARCHITECTURE - READ THIS FIRST
=============================================================================

⚠️  TWO PRICE TABLES - Using the wrong one causes bugs!

    prices              → CANONICAL data (matches website JSONs)
                          ✅ USE THIS FOR ALL ANALYSIS QUERIES

    prices_RAW_INGESTION → Raw API data (duplicates, timezone issues, messy)
                          ❌ NEVER QUERY FOR ANALYSIS (debugging only)

DATA FLOW:
    1. fetch_prices.py writes to → prices_RAW_INGESTION
    2. export_static.py reads from prices_RAW_INGESTION → cleans → JSONs
    3. import_canonical.py syncs JSONs → prices (canonical)

WHY TWO TABLES?
    Raw ingestion can have duplicates, timezone bugs, overlapping data from
    multiple sources. The export process cleans this up and writes JSONs,
    which are the source of truth. JSONs are imported back to `prices` so
    DuckDB queries match the website exactly.

IF COUNTS DON'T MATCH WEBSITE:
    Run `python export_static.py` - it syncs the canonical table after export.

See GOTCHAS.md for full documentation.
=============================================================================

v2.0 Changes:
- Added data_source tracking for prices (provenance)
- Added token_mint column to assets
- Optimized tweet_events with pre-computed price lookups
- Added gap detection and data quality views

v3.0 Changes:
- Split prices into canonical (prices) and raw (prices_RAW_INGESTION)
- Added backwards-compatible table detection functions
"""
import json
import duckdb
from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any

# Paths
SCRIPTS_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPTS_DIR.parent
DATA_DIR = PROJECT_ROOT / "data"
ASSETS_FILE = SCRIPTS_DIR / "assets.json"
ANALYTICS_DB = DATA_DIR / "analytics.duckdb"


def get_connection(db_path: Path = ANALYTICS_DB) -> duckdb.DuckDBPyConnection:
    """Get a DuckDB connection with WAL mode enabled."""
    DATA_DIR.mkdir(exist_ok=True)
    conn = duckdb.connect(str(db_path))
    return conn


def get_raw_price_table(conn: duckdb.DuckDBPyConnection) -> str:
    """
    Return the table name for RAW price data (write target for ingestion).

    BACKWARDS COMPATIBLE:
    - Pre-migration: returns 'prices' (old schema, single table)
    - Post-migration: returns 'prices_RAW_INGESTION' (new schema)

    Use this for:
    - insert_prices() write target
    - get_latest_price_timestamp() for incremental fetch resumption
    - export_static.py reading raw data for export

    DO NOT use this for analysis queries - use 'prices' directly (canonical).
    """
    tables = [r[0] for r in conn.execute("SHOW TABLES").fetchall()]
    return 'prices_RAW_INGESTION' if 'prices_RAW_INGESTION' in tables else 'prices'


def init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    """Initialize the database schema."""
    
    # Assets table (v2: added token_mint, backfill_source)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS assets (
            id VARCHAR PRIMARY KEY,
            name VARCHAR NOT NULL,
            founder VARCHAR NOT NULL,
            founder_type VARCHAR DEFAULT 'founder',
            network VARCHAR,
            pool_address VARCHAR,
            token_mint VARCHAR,
            coingecko_id VARCHAR,
            price_source VARCHAR NOT NULL,
            backfill_source VARCHAR,
            launch_date TIMESTAMP NOT NULL,
            color VARCHAR,
            enabled BOOLEAN DEFAULT true,
            keyword_filter VARCHAR,
            tweet_filter_note VARCHAR,
            skip_tweet_fetch BOOLEAN DEFAULT FALSE,
            skip_tweet_fetch_reason VARCHAR,
            created_at TIMESTAMP DEFAULT now(),
            updated_at TIMESTAMP DEFAULT now()
        )
    """)
    
    # Add new columns if they don't exist (migration)
    try:
        conn.execute("ALTER TABLE assets ADD COLUMN IF NOT EXISTS token_mint VARCHAR")
        conn.execute("ALTER TABLE assets ADD COLUMN IF NOT EXISTS backfill_source VARCHAR")
        conn.execute("ALTER TABLE assets ADD COLUMN IF NOT EXISTS skip_tweet_fetch BOOLEAN DEFAULT FALSE")
        conn.execute("ALTER TABLE assets ADD COLUMN IF NOT EXISTS skip_tweet_fetch_reason VARCHAR")
    except:
        pass
    
    # Tweets table
    conn.execute("""
        CREATE TABLE IF NOT EXISTS tweets (
            id VARCHAR PRIMARY KEY,
            asset_id VARCHAR NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            text VARCHAR,
            likes INTEGER DEFAULT 0,
            retweets INTEGER DEFAULT 0,
            replies INTEGER DEFAULT 0,
            impressions INTEGER DEFAULT 0,
            is_filtered BOOLEAN DEFAULT FALSE,
            reply_to VARCHAR,
            fetched_at TIMESTAMP DEFAULT now(),
            FOREIGN KEY (asset_id) REFERENCES assets(id)
        )
    """)

    # Add reply_to column if it doesn't exist (migration for adopter reply tracking)
    try:
        conn.execute("ALTER TABLE tweets ADD COLUMN IF NOT EXISTS reply_to VARCHAR")
    except:
        pass
    
    # Prices table (v2: added data_source for provenance tracking)
    #
    # PRIMARY KEY (asset_id, timeframe, timestamp):
    # DO NOT CHANGE - The upsert logic in insert_prices() depends on this
    # composite key for ON CONFLICT behavior. See GOTCHAS.md.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS prices (
            asset_id VARCHAR NOT NULL,
            timeframe VARCHAR NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            open DOUBLE,
            high DOUBLE,
            low DOUBLE,
            close DOUBLE,
            volume DOUBLE,
            data_source VARCHAR DEFAULT 'unknown',
            fetched_at TIMESTAMP DEFAULT now(),
            PRIMARY KEY (asset_id, timeframe, timestamp),
            FOREIGN KEY (asset_id) REFERENCES assets(id)
        )
    """)
    
    # Add data_source column if it doesn't exist (migration)
    try:
        conn.execute("ALTER TABLE prices ADD COLUMN IF NOT EXISTS data_source VARCHAR DEFAULT 'unknown'")
    except:
        pass
    
    # Ingestion state for incremental fetching
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ingestion_state (
            asset_id VARCHAR NOT NULL,
            data_type VARCHAR NOT NULL,
            last_id VARCHAR,
            last_timestamp TIMESTAMP,
            updated_at TIMESTAMP DEFAULT now(),
            PRIMARY KEY (asset_id, data_type),
            FOREIGN KEY (asset_id) REFERENCES assets(id)
        )
    """)
    
    # Create indexes for performance
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_tweets_asset_ts 
        ON tweets(asset_id, timestamp)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_tweets_asset 
        ON tweets(asset_id)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_prices_asset_tf_ts 
        ON prices(asset_id, timeframe, timestamp)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_prices_source 
        ON prices(data_source)
    """)
    
    # ==========================================================================
    # tweet_events VIEW - Core alignment between tweets and prices
    # ==========================================================================
    #
    # WHY THIS EXISTS:
    # This view aligns tweets with the best available price data at tweet time.
    # It uses timeframe fallback (1m > 1h > 1d) with staleness limits.
    #
    # STALENESS LIMITS - DO NOT REMOVE:
    #   - 1m: max 1 hour old
    #   - 1h: max 24 hours old
    #   - 1d: max 7 days old
    #
    # WHY STALENESS LIMITS:
    # Without them, a tweet from TODAY could match 1m data from MONTHS AGO
    # (when 1m collection started) instead of fresh 1h/1d data. This caused
    # wildly incorrect price_at_tweet values. See GOTCHAS.md.
    #
    # CASE STATEMENT ORDERING (1m=1, 1h=2, 1d=3):
    # DO NOT CHANGE - this defines the fallback priority.
    # ==========================================================================
    conn.execute("""
        CREATE OR REPLACE VIEW tweet_events AS
        WITH tweet_base AS (
            SELECT 
                t.id AS tweet_id,
                t.asset_id,
                a.name AS asset_name,
                a.founder,
                a.color AS asset_color,
                t.timestamp,
                t.text,
                t.likes,
                t.retweets,
                t.replies,
                t.impressions
            FROM tweets t
            JOIN assets a ON t.asset_id = a.id
            WHERE a.enabled = true
              AND t.timestamp >= a.launch_date
              AND (t.is_filtered = FALSE OR t.is_filtered IS NULL)
        ),
        -- Get best available price at tweet time (prefer 1m > 1h > 1d)
        -- Only use data if fresh enough for its timeframe
        price_at_tweet AS (
            SELECT DISTINCT ON (tb.tweet_id)
                tb.tweet_id,
                p.close AS price_at_tweet
            FROM tweet_base tb
            LEFT JOIN prices p ON p.asset_id = tb.asset_id 
                AND p.timestamp <= tb.timestamp
                AND (
                    (p.timeframe = '1m' AND tb.timestamp - p.timestamp <= INTERVAL '1 hour')
                    OR (p.timeframe = '1h' AND tb.timestamp - p.timestamp <= INTERVAL '24 hours')
                    OR (p.timeframe = '1d' AND tb.timestamp - p.timestamp <= INTERVAL '7 days')
                )
            ORDER BY tb.tweet_id, 
                CASE p.timeframe WHEN '1m' THEN 1 WHEN '1h' THEN 2 WHEN '1d' THEN 3 END,
                p.timestamp DESC
        ),
        -- Get price 1 hour later (with same staleness checks)
        price_1h AS (
            SELECT DISTINCT ON (tb.tweet_id)
                tb.tweet_id,
                p.close AS price_1h
            FROM tweet_base tb
            LEFT JOIN prices p ON p.asset_id = tb.asset_id 
                AND p.timestamp <= tb.timestamp + INTERVAL '1 hour'
                AND (
                    (p.timeframe = '1m' AND (tb.timestamp + INTERVAL '1 hour') - p.timestamp <= INTERVAL '1 hour')
                    OR (p.timeframe = '1h' AND (tb.timestamp + INTERVAL '1 hour') - p.timestamp <= INTERVAL '24 hours')
                    OR (p.timeframe = '1d' AND (tb.timestamp + INTERVAL '1 hour') - p.timestamp <= INTERVAL '7 days')
                )
            ORDER BY tb.tweet_id,
                CASE p.timeframe WHEN '1m' THEN 1 WHEN '1h' THEN 2 WHEN '1d' THEN 3 END,
                p.timestamp DESC
        ),
        -- Get price 24 hours later (with same staleness checks)
        price_24h AS (
            SELECT DISTINCT ON (tb.tweet_id)
                tb.tweet_id,
                p.close AS price_24h
            FROM tweet_base tb
            LEFT JOIN prices p ON p.asset_id = tb.asset_id 
                AND p.timestamp <= tb.timestamp + INTERVAL '24 hours'
                AND (
                    (p.timeframe = '1m' AND (tb.timestamp + INTERVAL '24 hours') - p.timestamp <= INTERVAL '1 hour')
                    OR (p.timeframe = '1h' AND (tb.timestamp + INTERVAL '24 hours') - p.timestamp <= INTERVAL '24 hours')
                    OR (p.timeframe = '1d' AND (tb.timestamp + INTERVAL '24 hours') - p.timestamp <= INTERVAL '7 days')
                )
            ORDER BY tb.tweet_id,
                CASE p.timeframe WHEN '1m' THEN 1 WHEN '1h' THEN 2 WHEN '1d' THEN 3 END,
                p.timestamp DESC
        )
        SELECT 
            tb.*,
            pat.price_at_tweet,
            p1h.price_1h,
            p24h.price_24h
        FROM tweet_base tb
        LEFT JOIN price_at_tweet pat ON tb.tweet_id = pat.tweet_id
        LEFT JOIN price_1h p1h ON tb.tweet_id = p1h.tweet_id
        LEFT JOIN price_24h p24h ON tb.tweet_id = p24h.tweet_id
        ORDER BY tb.timestamp
    """)
    
    # Fallback view using daily prices for assets without 1m data
    conn.execute("""
        CREATE OR REPLACE VIEW tweet_events_daily AS
        SELECT 
            t.id AS tweet_id,
            t.asset_id,
            a.name AS asset_name,
            a.founder,
            a.color AS asset_color,
            t.timestamp,
            t.text,
            t.likes,
            t.retweets,
            t.replies,
            t.impressions,
            -- Price at tweet time (find closest 1d candle)
            (SELECT p.close 
             FROM prices p 
             WHERE p.asset_id = t.asset_id 
               AND p.timeframe = '1d'
               AND p.timestamp <= t.timestamp
             ORDER BY p.timestamp DESC 
             LIMIT 1) AS price_at_tweet,
            -- Price 1 day later (approximate 1h as same-day)
            (SELECT p.close 
             FROM prices p 
             WHERE p.asset_id = t.asset_id 
               AND p.timeframe = '1d'
               AND p.timestamp <= t.timestamp + INTERVAL '1 day'
             ORDER BY p.timestamp DESC 
             LIMIT 1) AS price_1h,
            -- Price 1 day later
            (SELECT p.close 
             FROM prices p 
             WHERE p.asset_id = t.asset_id 
               AND p.timeframe = '1d'
               AND p.timestamp <= t.timestamp + INTERVAL '1 day'
             ORDER BY p.timestamp DESC 
             LIMIT 1) AS price_24h
        FROM tweets t
        JOIN assets a ON t.asset_id = a.id
        WHERE a.enabled = true
          AND t.timestamp >= a.launch_date
          AND (t.is_filtered = FALSE OR t.is_filtered IS NULL)
        ORDER BY t.timestamp
    """)
    
    # Data quality view: detect gaps in price data
    conn.execute("""
        CREATE OR REPLACE VIEW price_gaps AS
        WITH price_with_prev AS (
            SELECT 
                asset_id,
                timeframe,
                timestamp,
                LAG(timestamp) OVER (
                    PARTITION BY asset_id, timeframe 
                    ORDER BY timestamp
                ) AS prev_timestamp
            FROM prices
        ),
        expected_intervals AS (
            SELECT 
                asset_id,
                timeframe,
                timestamp,
                prev_timestamp,
                CASE timeframe
                    WHEN '1m' THEN 60
                    WHEN '15m' THEN 900
                    WHEN '1h' THEN 3600
                    WHEN '1d' THEN 86400
                END AS expected_seconds
            FROM price_with_prev
            WHERE prev_timestamp IS NOT NULL
        )
        SELECT 
            asset_id,
            timeframe,
            prev_timestamp AS gap_start,
            timestamp AS gap_end,
            EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) AS actual_seconds,
            expected_seconds,
            EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) / expected_seconds AS missing_candles
        FROM expected_intervals
        WHERE EXTRACT(EPOCH FROM (timestamp - prev_timestamp)) > expected_seconds * 2
        ORDER BY asset_id, timeframe, gap_start
    """)
    
    # Data source summary view
    conn.execute("""
        CREATE OR REPLACE VIEW data_source_summary AS
        SELECT 
            asset_id,
            timeframe,
            data_source,
            COUNT(*) AS candle_count,
            MIN(timestamp) AS earliest,
            MAX(timestamp) AS latest
        FROM prices
        GROUP BY asset_id, timeframe, data_source
        ORDER BY asset_id, timeframe, data_source
    """)


def load_assets_from_json(conn: duckdb.DuckDBPyConnection, assets_file: Path = ASSETS_FILE) -> int:
    """
    Load/sync assets from JSON config into database.
    Returns number of assets upserted.
    """
    with open(assets_file) as f:
        config = json.load(f)
    
    count = 0
    for asset in config.get("assets", []):
        conn.execute("""
            INSERT INTO assets (id, name, founder, founder_type, network, pool_address, token_mint,
                               coingecko_id, price_source, backfill_source, launch_date, 
                               color, enabled, keyword_filter, tweet_filter_note,
                               skip_tweet_fetch, skip_tweet_fetch_reason, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now())
            ON CONFLICT (id) DO UPDATE SET
                name = EXCLUDED.name,
                founder = EXCLUDED.founder,
                founder_type = EXCLUDED.founder_type,
                network = EXCLUDED.network,
                pool_address = EXCLUDED.pool_address,
                token_mint = EXCLUDED.token_mint,
                coingecko_id = EXCLUDED.coingecko_id,
                price_source = EXCLUDED.price_source,
                backfill_source = EXCLUDED.backfill_source,
                launch_date = EXCLUDED.launch_date,
                color = EXCLUDED.color,
                enabled = EXCLUDED.enabled,
                keyword_filter = EXCLUDED.keyword_filter,
                tweet_filter_note = EXCLUDED.tweet_filter_note,
                skip_tweet_fetch = EXCLUDED.skip_tweet_fetch,
                skip_tweet_fetch_reason = EXCLUDED.skip_tweet_fetch_reason,
                updated_at = now()
        """, [
            asset["id"],
            asset["name"],
            asset["founder"],
            asset.get("founder_type", "founder"),
            asset.get("network"),
            asset.get("pool_address"),
            asset.get("token_mint"),
            asset.get("coingecko_id"),
            asset["price_source"],
            asset.get("backfill_source"),
            asset["launch_date"],
            asset.get("color"),
            asset.get("enabled", True),
            asset.get("keyword_filter"),
            asset.get("tweet_filter_note"),
            asset.get("skip_tweet_fetch", False),
            asset.get("skip_tweet_fetch_reason"),
        ])
        count += 1
    
    return count


def get_asset(conn: duckdb.DuckDBPyConnection, asset_id: str) -> Optional[Dict[str, Any]]:
    """Get a single asset by ID."""
    result = conn.execute("""
        SELECT id, name, founder, founder_type, network, pool_address, token_mint, coingecko_id,
               price_source, backfill_source, launch_date, color, enabled, keyword_filter, tweet_filter_note,
               skip_tweet_fetch, skip_tweet_fetch_reason
        FROM assets WHERE id = ?
    """, [asset_id]).fetchone()
    
    if not result:
        return None
    
    return {
        "id": result[0],
        "name": result[1],
        "founder": result[2],
        "founder_type": result[3],
        "network": result[4],
        "pool_address": result[5],
        "token_mint": result[6],
        "coingecko_id": result[7],
        "price_source": result[8],
        "backfill_source": result[9],
        "launch_date": result[10],
        "color": result[11],
        "enabled": result[12],
        "keyword_filter": result[13],
        "tweet_filter_note": result[14],
        "skip_tweet_fetch": result[15],
        "skip_tweet_fetch_reason": result[16],
    }


def get_enabled_assets(conn: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    """Get all enabled assets."""
    results = conn.execute("""
        SELECT id, name, founder, founder_type, network, pool_address, token_mint, coingecko_id,
               price_source, backfill_source, launch_date, color, enabled, keyword_filter, tweet_filter_note,
               skip_tweet_fetch, skip_tweet_fetch_reason
        FROM assets WHERE enabled = true
        ORDER BY name
    """).fetchall()
    
    return [
        {
            "id": r[0],
            "name": r[1],
            "founder": r[2],
            "founder_type": r[3],
            "network": r[4],
            "pool_address": r[5],
            "token_mint": r[6],
            "coingecko_id": r[7],
            "price_source": r[8],
            "backfill_source": r[9],
            "launch_date": r[10],
            "color": r[11],
            "enabled": r[12],
            "keyword_filter": r[13],
            "tweet_filter_note": r[14],
            "skip_tweet_fetch": r[15],
            "skip_tweet_fetch_reason": r[16],
        }
        for r in results
    ]


def get_all_assets(conn: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    """Get all assets (including disabled)."""
    results = conn.execute("""
        SELECT id, name, founder, founder_type, network, pool_address, token_mint, coingecko_id,
               price_source, backfill_source, launch_date, color, enabled, keyword_filter, tweet_filter_note,
               skip_tweet_fetch, skip_tweet_fetch_reason
        FROM assets
        ORDER BY name
    """).fetchall()
    
    return [
        {
            "id": r[0],
            "name": r[1],
            "founder": r[2],
            "founder_type": r[3],
            "network": r[4],
            "pool_address": r[5],
            "token_mint": r[6],
            "coingecko_id": r[7],
            "price_source": r[8],
            "backfill_source": r[9],
            "launch_date": r[10],
            "color": r[11],
            "enabled": r[12],
            "keyword_filter": r[13],
            "tweet_filter_note": r[14],
            "skip_tweet_fetch": r[15],
            "skip_tweet_fetch_reason": r[16],
        }
        for r in results
    ]


def get_ingestion_state(
    conn: duckdb.DuckDBPyConnection, 
    asset_id: str, 
    data_type: str
) -> Optional[Dict[str, Any]]:
    """Get ingestion state for an asset/data_type combo."""
    result = conn.execute("""
        SELECT last_id, last_timestamp, updated_at
        FROM ingestion_state
        WHERE asset_id = ? AND data_type = ?
    """, [asset_id, data_type]).fetchone()
    
    if not result:
        return None
    
    return {
        "last_id": result[0],
        "last_timestamp": result[1],
        "updated_at": result[2],
    }


def get_latest_price_timestamp(
    conn: duckdb.DuckDBPyConnection,
    asset_id: str,
    timeframe: str
) -> Optional[datetime]:
    """
    Get the latest price timestamp for an asset/timeframe combo.

    IMPORTANT: Reads from RAW table (prices_RAW_INGESTION) after migration.
    This is correct because we need the most recent ingested data to know
    where to resume fetching, not the canonical data (which may lag).
    """
    # Use raw table for incremental fetch resumption
    table = get_raw_price_table(conn)
    result = conn.execute(f"""
        SELECT MAX(timestamp) FROM {table}
        WHERE asset_id = ? AND timeframe = ?
    """, [asset_id, timeframe]).fetchone()

    return result[0] if result and result[0] else None


def update_ingestion_state(
    conn: duckdb.DuckDBPyConnection,
    asset_id: str,
    data_type: str,
    last_id: Optional[str] = None,
    last_timestamp: Optional[datetime] = None
) -> None:
    """Update ingestion state after a successful fetch."""
    conn.execute("""
        INSERT INTO ingestion_state (asset_id, data_type, last_id, last_timestamp, updated_at)
        VALUES (?, ?, ?, ?, now())
        ON CONFLICT (asset_id, data_type) DO UPDATE SET
            last_id = COALESCE(EXCLUDED.last_id, ingestion_state.last_id),
            last_timestamp = COALESCE(EXCLUDED.last_timestamp, ingestion_state.last_timestamp),
            updated_at = now()
    """, [asset_id, data_type, last_id, last_timestamp])


def insert_tweets(
    conn: duckdb.DuckDBPyConnection,
    asset_id: str,
    tweets: List[Dict[str, Any]]
) -> int:
    """
    Insert tweets into database.
    Returns number of NEW tweets inserted (not updates).

    IMPORTANT - Uses ON CONFLICT DO UPDATE, NOT INSERT OR IGNORE:
    We WANT to update engagement metrics (likes, retweets, impressions)
    on re-fetch while preserving the original timestamp and text.
    Using INSERT OR IGNORE would discard updated metrics.
    See GOTCHAS.md.

    Fields:
        reply_to: Username being replied to (for adopter filtering).
                  Captured from Nitter's .replying-to element.
    """
    if not tweets:
        return 0

    new_inserts = 0
    updated = 0

    for tweet in tweets:
        try:
            # Check if tweet already exists
            existing = conn.execute(
                "SELECT 1 FROM tweets WHERE id = ?", [tweet["id"]]
            ).fetchone()

            conn.execute("""
                INSERT INTO tweets (id, asset_id, timestamp, text, likes, retweets, replies, impressions, reply_to, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, now())
                ON CONFLICT (id) DO UPDATE SET
                    likes = EXCLUDED.likes,
                    retweets = EXCLUDED.retweets,
                    replies = EXCLUDED.replies,
                    impressions = EXCLUDED.impressions,
                    reply_to = COALESCE(EXCLUDED.reply_to, tweets.reply_to),
                    fetched_at = now()
            """, [
                tweet["id"],
                asset_id,
                tweet["timestamp"] if isinstance(tweet.get("timestamp"), datetime) else tweet.get("created_at"),
                tweet.get("text"),
                tweet.get("likes", 0),
                tweet.get("retweets", 0),
                tweet.get("replies", 0),
                tweet.get("impressions", 0),
                tweet.get("reply_to"),
            ])

            if existing:
                updated += 1
            else:
                new_inserts += 1

        except Exception as e:
            print(f"Error inserting tweet {tweet.get('id')}: {e}")

    if updated > 0:
        print(f"      ({new_inserts} new, {updated} updated)")

    return new_inserts


def insert_prices(
    conn: duckdb.DuckDBPyConnection,
    asset_id: str,
    timeframe: str,
    candles: List[Dict[str, Any]],
    data_source: str = "unknown"
) -> int:
    """
    Insert price candles into database.
    Returns number of NEW candles inserted (not updates to existing ones).

    IMPORTANT: Writes to RAW table (prices_RAW_INGESTION) after migration.
    The canonical `prices` table is synced from JSONs after export.

    Args:
        conn: Database connection
        asset_id: Asset identifier
        timeframe: Timeframe (1m, 15m, 1h, 1d)
        candles: List of candle dicts with timestamp_epoch, open, high, low, close, volume
        data_source: Source of data (geckoterminal, birdeye, hyperliquid, coingecko)

    Note:
        Uses ON CONFLICT to upsert - existing candles are updated, new ones inserted.
        We count before/after to accurately report only NEW inserts (not updates).
    """
    if not candles:
        return 0

    # Use raw table for ingestion
    table = get_raw_price_table(conn)

    # Count existing candles BEFORE insert to calculate actual new inserts
    count_before = conn.execute(f"""
        SELECT COUNT(*) FROM {table}
        WHERE asset_id = ? AND timeframe = ?
    """, [asset_id, timeframe]).fetchone()[0]

    # Prepare data for bulk insert
    data = [
        (
            asset_id,
            timeframe,
            datetime.utcfromtimestamp(c["timestamp_epoch"]) if "timestamp_epoch" in c else c["timestamp"],
            c.get("open"),
            c.get("high"),
            c.get("low"),
            c.get("close"),
            c.get("volume"),
            data_source,
        )
        for c in candles
    ]

    conn.executemany(f"""
        INSERT INTO {table} (asset_id, timeframe, timestamp, open, high, low, close, volume, data_source, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, now())
        ON CONFLICT (asset_id, timeframe, timestamp) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            data_source = EXCLUDED.data_source,
            fetched_at = now()
    """, data)

    # Count AFTER to get actual new inserts
    count_after = conn.execute(f"""
        SELECT COUNT(*) FROM {table}
        WHERE asset_id = ? AND timeframe = ?
    """, [asset_id, timeframe]).fetchone()[0]

    # Return only the NEW candles (updates don't increase count)
    new_inserts = count_after - count_before
    return new_inserts


def _get_tweet_events_all(
    conn: duckdb.DuckDBPyConnection,
    asset_id: Optional[str],
    use_daily_fallback: bool
) -> List[tuple]:
    """
    Internal helper to get ALL tweet events including filtered ones.
    Returns raw tuples matching the same format as the view queries.
    """
    # Build a query that mirrors the view but without is_filtered filter
    if use_daily_fallback:
        # Simple daily fallback query (no is_filtered filter)
        query = """
            SELECT
                t.id AS tweet_id,
                t.asset_id,
                a.name AS asset_name,
                a.founder,
                a.color AS asset_color,
                t.timestamp,
                t.text,
                t.likes,
                t.retweets,
                t.replies,
                t.impressions,
                (SELECT p.close
                 FROM prices p
                 WHERE p.asset_id = t.asset_id
                   AND p.timeframe = '1d'
                   AND p.timestamp <= t.timestamp
                 ORDER BY p.timestamp DESC
                 LIMIT 1) AS price_at_tweet,
                (SELECT p.close
                 FROM prices p
                 WHERE p.asset_id = t.asset_id
                   AND p.timeframe = '1d'
                   AND p.timestamp <= t.timestamp + INTERVAL '1 day'
                 ORDER BY p.timestamp DESC
                 LIMIT 1) AS price_1h,
                (SELECT p.close
                 FROM prices p
                 WHERE p.asset_id = t.asset_id
                   AND p.timeframe = '1d'
                   AND p.timestamp <= t.timestamp + INTERVAL '1 day'
                 ORDER BY p.timestamp DESC
                 LIMIT 1) AS price_24h
            FROM tweets t
            JOIN assets a ON t.asset_id = a.id
            WHERE a.enabled = true
              AND t.timestamp >= a.launch_date
        """
    else:
        # Full query mirroring tweet_events view but without is_filtered filter
        query = """
            WITH tweet_base AS (
                SELECT
                    t.id AS tweet_id,
                    t.asset_id,
                    a.name AS asset_name,
                    a.founder,
                    a.color AS asset_color,
                    t.timestamp,
                    t.text,
                    t.likes,
                    t.retweets,
                    t.replies,
                    t.impressions
                FROM tweets t
                JOIN assets a ON t.asset_id = a.id
                WHERE a.enabled = true
                  AND t.timestamp >= a.launch_date
            ),
            price_at_tweet AS (
                SELECT DISTINCT ON (tb.tweet_id)
                    tb.tweet_id,
                    p.close AS price_at_tweet
                FROM tweet_base tb
                LEFT JOIN prices p ON p.asset_id = tb.asset_id
                    AND p.timestamp <= tb.timestamp
                    AND (
                        (p.timeframe = '1m' AND tb.timestamp - p.timestamp <= INTERVAL '1 hour')
                        OR (p.timeframe = '1h' AND tb.timestamp - p.timestamp <= INTERVAL '24 hours')
                        OR (p.timeframe = '1d' AND tb.timestamp - p.timestamp <= INTERVAL '7 days')
                    )
                ORDER BY tb.tweet_id,
                    CASE p.timeframe WHEN '1m' THEN 1 WHEN '1h' THEN 2 WHEN '1d' THEN 3 END,
                    p.timestamp DESC
            ),
            price_1h AS (
                SELECT DISTINCT ON (tb.tweet_id)
                    tb.tweet_id,
                    p.close AS price_1h
                FROM tweet_base tb
                LEFT JOIN prices p ON p.asset_id = tb.asset_id
                    AND p.timestamp <= tb.timestamp + INTERVAL '1 hour'
                    AND (
                        (p.timeframe = '1m' AND (tb.timestamp + INTERVAL '1 hour') - p.timestamp <= INTERVAL '1 hour')
                        OR (p.timeframe = '1h' AND (tb.timestamp + INTERVAL '1 hour') - p.timestamp <= INTERVAL '24 hours')
                        OR (p.timeframe = '1d' AND (tb.timestamp + INTERVAL '1 hour') - p.timestamp <= INTERVAL '7 days')
                    )
                ORDER BY tb.tweet_id,
                    CASE p.timeframe WHEN '1m' THEN 1 WHEN '1h' THEN 2 WHEN '1d' THEN 3 END,
                    p.timestamp DESC
            ),
            price_24h AS (
                SELECT DISTINCT ON (tb.tweet_id)
                    tb.tweet_id,
                    p.close AS price_24h
                FROM tweet_base tb
                LEFT JOIN prices p ON p.asset_id = tb.asset_id
                    AND p.timestamp <= tb.timestamp + INTERVAL '24 hours'
                    AND (
                        (p.timeframe = '1m' AND (tb.timestamp + INTERVAL '24 hours') - p.timestamp <= INTERVAL '1 hour')
                        OR (p.timeframe = '1h' AND (tb.timestamp + INTERVAL '24 hours') - p.timestamp <= INTERVAL '24 hours')
                        OR (p.timeframe = '1d' AND (tb.timestamp + INTERVAL '24 hours') - p.timestamp <= INTERVAL '7 days')
                    )
                ORDER BY tb.tweet_id,
                    CASE p.timeframe WHEN '1m' THEN 1 WHEN '1h' THEN 2 WHEN '1d' THEN 3 END,
                    p.timestamp DESC
            )
            SELECT
                tb.tweet_id, tb.asset_id, tb.asset_name, tb.founder, tb.asset_color,
                tb.timestamp, tb.text, tb.likes, tb.retweets, tb.replies, tb.impressions,
                pat.price_at_tweet,
                p1h.price_1h,
                p24h.price_24h
            FROM tweet_base tb
            LEFT JOIN price_at_tweet pat ON tb.tweet_id = pat.tweet_id
            LEFT JOIN price_1h p1h ON tb.tweet_id = p1h.tweet_id
            LEFT JOIN price_24h p24h ON tb.tweet_id = p24h.tweet_id
        """

    if asset_id:
        query += " WHERE tb.asset_id = ?" if not use_daily_fallback else " AND t.asset_id = ?"
        query += " ORDER BY " + ("t.timestamp" if use_daily_fallback else "tb.timestamp")
        return conn.execute(query, [asset_id]).fetchall()
    else:
        query += " ORDER BY " + ("t.timestamp" if use_daily_fallback else "tb.timestamp")
        return conn.execute(query).fetchall()


def get_tweet_events(
    conn: duckdb.DuckDBPyConnection,
    asset_id: Optional[str] = None,
    use_daily_fallback: bool = False,
    include_filtered: bool = False
) -> List[Dict[str, Any]]:
    """
    Get aligned tweet events from the view.

    Args:
        asset_id: Filter by asset (optional)
        use_daily_fallback: Use 1d prices for assets without 1m data
        include_filtered: If True, include tweets marked as filtered (is_filtered=TRUE)
                          Used to export all tweets for "Only mentions" toggle
    """
    # Load circulating supply from assets.json for market cap calculation
    # Also load supply_unstable flag to skip market cap for tokens with changing supply
    supply_by_asset = {}
    supply_unstable = set()
    try:
        with open(ASSETS_FILE) as f:
            assets_data = json.load(f)
            for asset in assets_data.get("assets", []):
                if "circulating_supply" in asset:
                    supply_by_asset[asset["id"]] = asset["circulating_supply"]
                if asset.get("supply_unstable"):
                    supply_unstable.add(asset["id"])
    except Exception:
        pass  # If assets.json not available, market_cap will be None

    # If include_filtered, we need a custom query since the views exclude filtered tweets
    if include_filtered:
        results = _get_tweet_events_all(conn, asset_id, use_daily_fallback)
    else:
        view_name = "tweet_events_daily" if use_daily_fallback else "tweet_events"

        if asset_id:
            results = conn.execute(f"""
                SELECT tweet_id, asset_id, asset_name, founder, asset_color,
                       timestamp, text, likes, retweets, replies, impressions,
                       price_at_tweet, price_1h, price_24h
                FROM {view_name}
                WHERE asset_id = ?
                ORDER BY timestamp
            """, [asset_id]).fetchall()
        else:
            results = conn.execute(f"""
                SELECT tweet_id, asset_id, asset_name, founder, asset_color,
                       timestamp, text, likes, retweets, replies, impressions,
                       price_at_tweet, price_1h, price_24h
                FROM {view_name}
                ORDER BY timestamp
            """).fetchall()
    
    events = []
    for r in results:
        price_at = r[11]
        price_1h = r[12]
        price_24h = r[13]
        
        change_1h = None
        change_24h = None
        if price_at and price_1h:
            change_1h = round((price_1h - price_at) / price_at * 100, 2)
        if price_at and price_24h:
            change_24h = round((price_24h - price_at) / price_at * 100, 2)

        # Calculate market cap at tweet time
        # market_cap = price × circulating_supply
        # Skip for assets with supply_unstable flag (e.g., JUP, WLD have vesting unlocks)
        event_asset_id = r[1]
        supply = supply_by_asset.get(event_asset_id)
        market_cap_at_tweet = None
        if price_at and supply and event_asset_id not in supply_unstable:
            market_cap_at_tweet = round(price_at * supply, 2)

        events.append({
            "tweet_id": r[0],
            "asset_id": event_asset_id,
            "asset_name": r[2],
            "founder": r[3],
            "asset_color": r[4],
            "timestamp": int(r[5].timestamp()) if hasattr(r[5], 'timestamp') else r[5],
            "timestamp_iso": r[5].isoformat() + "Z" if hasattr(r[5], 'isoformat') else str(r[5]),
            "text": r[6],
            "likes": r[7],
            "retweets": r[8],
            "replies": r[9],
            "impressions": r[10],
            "price_at_tweet": price_at,
            "price_1h": price_1h,
            "price_24h": price_24h,
            "change_1h_pct": change_1h,
            "change_24h_pct": change_24h,
            "market_cap_at_tweet": market_cap_at_tweet,
        })
    
    return events


def get_price_gaps(
    conn: duckdb.DuckDBPyConnection,
    asset_id: Optional[str] = None,
    timeframe: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Get gaps in price data for quality monitoring.
    
    Args:
        asset_id: Filter by asset (optional)
        timeframe: Filter by timeframe (optional)
    
    Returns list of gaps with start, end, and missing candle count.
    """
    query = "SELECT * FROM price_gaps WHERE 1=1"
    params = []
    
    if asset_id:
        query += " AND asset_id = ?"
        params.append(asset_id)
    if timeframe:
        query += " AND timeframe = ?"
        params.append(timeframe)
    
    query += " ORDER BY asset_id, timeframe, gap_start"
    
    results = conn.execute(query, params).fetchall()
    
    return [
        {
            "asset_id": r[0],
            "timeframe": r[1],
            "gap_start": r[2].isoformat() if hasattr(r[2], 'isoformat') else r[2],
            "gap_end": r[3].isoformat() if hasattr(r[3], 'isoformat') else r[3],
            "actual_seconds": r[4],
            "expected_seconds": r[5],
            "missing_candles": int(r[6]) if r[6] else 0,
        }
        for r in results
    ]


def get_data_source_summary(conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """Get summary of data sources used for each asset/timeframe."""
    results = conn.execute("SELECT * FROM data_source_summary").fetchall()
    
    summary = {}
    for r in results:
        asset_id, timeframe, data_source, count, earliest, latest = r
        
        if asset_id not in summary:
            summary[asset_id] = {}
        if timeframe not in summary[asset_id]:
            summary[asset_id][timeframe] = {}
        
        summary[asset_id][timeframe][data_source] = {
            "count": count,
            "earliest": earliest.isoformat() if earliest else None,
            "latest": latest.isoformat() if latest else None,
        }
    
    return summary


def get_db_stats(conn: duckdb.DuckDBPyConnection) -> Dict[str, Any]:
    """Get overall database statistics."""
    stats = {}
    
    # Asset counts
    stats["assets"] = conn.execute("SELECT COUNT(*) FROM assets").fetchone()[0]
    stats["enabled_assets"] = conn.execute("SELECT COUNT(*) FROM assets WHERE enabled = true").fetchone()[0]
    
    # Tweet counts per asset
    tweet_counts = conn.execute("""
        SELECT a.id, a.name, COUNT(t.id) as tweet_count
        FROM assets a
        LEFT JOIN tweets t ON a.id = t.asset_id
        GROUP BY a.id, a.name
        ORDER BY a.name
    """).fetchall()
    stats["tweets_by_asset"] = {r[0]: {"name": r[1], "count": r[2]} for r in tweet_counts}
    
    # Price data ranges per asset
    price_ranges = conn.execute("""
        SELECT a.id, a.name, p.timeframe, 
               MIN(p.timestamp) as start_date,
               MAX(p.timestamp) as end_date,
               COUNT(*) as candle_count
        FROM assets a
        LEFT JOIN prices p ON a.id = p.asset_id
        WHERE p.timestamp IS NOT NULL
        GROUP BY a.id, a.name, p.timeframe
        ORDER BY a.name, p.timeframe
    """).fetchall()
    
    stats["prices_by_asset"] = {}
    for r in price_ranges:
        asset_id = r[0]
        if asset_id not in stats["prices_by_asset"]:
            stats["prices_by_asset"][asset_id] = {"name": r[1], "timeframes": {}}
        stats["prices_by_asset"][asset_id]["timeframes"][r[2]] = {
            "start": r[3].isoformat() if r[3] else None,
            "end": r[4].isoformat() if r[4] else None,
            "count": r[5],
        }
    
    # Data source summary
    stats["data_sources"] = get_data_source_summary(conn)
    
    # Gap summary
    gap_summary = conn.execute("""
        SELECT asset_id, timeframe, COUNT(*) as gap_count, SUM(missing_candles) as total_missing
        FROM price_gaps
        GROUP BY asset_id, timeframe
    """).fetchall()
    stats["gaps"] = {
        f"{r[0]}_{r[1]}": {"gap_count": r[2], "total_missing": int(r[3]) if r[3] else 0}
        for r in gap_summary
    }
    
    return stats


def init_db(db_path: Path = ANALYTICS_DB) -> duckdb.DuckDBPyConnection:
    """Initialize database with schema and load assets from JSON."""
    conn = get_connection(db_path)
    init_schema(conn)
    load_assets_from_json(conn)
    return conn


# =============================================================================
# OUTLIER DETECTION FUNCTIONS (for CLI)
# =============================================================================

# WHY 5-SIGMA, NOT 3-SIGMA:
# Crypto prices have fat tails. Using 3-sigma (standard for normal distributions)
# flagged legitimate 50-100% pumps as outliers. 5-sigma catches only true
# anomalies like sniper bot spikes (1000x+ of median). See GOTCHAS.md.
OUTLIER_THRESHOLD_STD = 5  # DO NOT lower without testing on real pump data
OUTLIER_MIN_CANDLES = 20    # Minimum candles for detection


def detect_price_outliers(
    conn: duckdb.DuckDBPyConnection,
    asset_id: str,
    timeframe: str = "1m",
    threshold_std: float = OUTLIER_THRESHOLD_STD
) -> List[Dict[str, Any]]:
    """
    Detect outlier candles in the database for a given asset/timeframe.
    
    Returns list of outlier dicts with timestamp, prices, and stats.
    """
    # Get all candles for this asset/timeframe
    results = conn.execute("""
        SELECT timestamp, open, high, low, close, volume
        FROM prices
        WHERE asset_id = ? AND timeframe = ?
        ORDER BY timestamp
    """, [asset_id, timeframe]).fetchall()
    
    if len(results) < OUTLIER_MIN_CANDLES:
        return []
    
    candles = [
        {
            "timestamp": r[0],
            "open": r[1],
            "high": r[2],
            "low": r[3],
            "close": r[4],
            "volume": r[5],
        }
        for r in results
    ]
    
    # Calculate stats on HIGH values
    highs = [c["high"] for c in candles if c["high"] is not None]
    
    if len(highs) < OUTLIER_MIN_CANDLES:
        return []
    
    sorted_highs = sorted(highs)
    n = len(sorted_highs)
    median = sorted_highs[n // 2] if n % 2 else (sorted_highs[n//2 - 1] + sorted_highs[n//2]) / 2
    
    mean = sum(highs) / len(highs)
    variance = sum((h - mean) ** 2 for h in highs) / len(highs)
    std = variance ** 0.5
    
    if std == 0:
        return []
    
    upper_threshold = median + (threshold_std * std)
    
    outliers = []
    for c in candles:
        if c["high"] is not None and c["high"] > upper_threshold:
            outliers.append({
                "timestamp": c["timestamp"],
                "timestamp_str": c["timestamp"].strftime("%Y-%m-%d %H:%M") if hasattr(c["timestamp"], "strftime") else str(c["timestamp"]),
                "open": c["open"],
                "high": c["high"],
                "low": c["low"],
                "close": c["close"],
                "ratio": c["high"] / median if median > 0 else float("inf"),
                "median": median,
                "threshold": upper_threshold,
            })
    
    return outliers


def cleanup_outliers(
    conn: duckdb.DuckDBPyConnection,
    asset_id: str,
    timeframe: str = "1m",
    threshold_std: float = OUTLIER_THRESHOLD_STD,
    dry_run: bool = True
) -> int:
    """
    Remove outlier candles from the database.
    
    Args:
        dry_run: If True, just report what would be deleted
    
    Returns number of candles deleted (or would be deleted).
    """
    outliers = detect_price_outliers(conn, asset_id, timeframe, threshold_std)
    
    if not outliers:
        return 0
    
    if dry_run:
        return len(outliers)
    
    # Delete the outliers
    for o in outliers:
        conn.execute("""
            DELETE FROM prices
            WHERE asset_id = ? AND timeframe = ? AND timestamp = ?
        """, [asset_id, timeframe, o["timestamp"]])

    return len(outliers)


def delete_by_source(
    conn: duckdb.DuckDBPyConnection,
    asset_id: str,
    data_source: str,
    timeframe: str = None,
    dry_run: bool = True
) -> dict:
    """
    Delete all candles from a specific data source.

    Use this to clean up stale or superseded data (e.g., chart_reconstruction
    data that has been replaced by Birdeye backfill).

    Args:
        conn: Database connection
        asset_id: Asset ID
        data_source: Data source to delete (e.g., 'chart_reconstruction', 'coingecko')
        timeframe: Optional - limit to specific timeframe
        dry_run: If True, only report what would be deleted

    Returns dict with {deleted, by_timeframe}
    """
    # Build query with optional timeframe filter
    where_clause = "WHERE asset_id = ? AND data_source = ?"
    params = [asset_id, data_source]

    if timeframe:
        where_clause += " AND timeframe = ?"
        params.append(timeframe)

    # Count by timeframe
    counts = conn.execute(f"""
        SELECT timeframe, COUNT(*) as count
        FROM prices
        {where_clause}
        GROUP BY timeframe
    """, params).fetchall()

    by_timeframe = {row[0]: row[1] for row in counts}
    total = sum(by_timeframe.values())

    if dry_run:
        return {"deleted": 0, "would_delete": total, "by_timeframe": by_timeframe}

    # Actually delete
    conn.execute(f"DELETE FROM prices {where_clause}", params)
    conn.commit()

    return {"deleted": total, "by_timeframe": by_timeframe}


def delete_dot_candles(
    conn: duckdb.DuckDBPyConnection,
    asset_id: str,
    timeframe: str,
    dry_run: bool = True
) -> dict:
    """
    Delete candles where O=H=L=C (dots).

    These typically come from CoinGecko /market_chart endpoint which returns
    price points instead of OHLC data. They render as dots on charts.

    Args:
        conn: Database connection
        asset_id: Asset ID
        timeframe: Timeframe to clean
        dry_run: If True, only report what would be deleted

    Returns dict with {deleted, total_before, pct}
    """
    # Count totals
    result = conn.execute("""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN open = high AND high = low AND low = close THEN 1 ELSE 0 END) as dots
        FROM prices
        WHERE asset_id = ? AND timeframe = ?
    """, [asset_id, timeframe]).fetchone()

    total = result[0] or 0
    dots = result[1] or 0
    pct = (dots / total * 100) if total > 0 else 0

    if dry_run:
        return {"deleted": 0, "would_delete": dots, "total": total, "pct": pct}

    # Actually delete
    conn.execute("""
        DELETE FROM prices
        WHERE asset_id = ? AND timeframe = ?
        AND open = high AND high = low AND low = close
    """, [asset_id, timeframe])
    conn.commit()

    return {"deleted": dots, "total_before": total, "pct": pct}


# CLI interface
def main():
    """CLI for database management."""
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python db.py <command>")
        print("Commands:")
        print("  init              - Initialize database schema")
        print("  sync-assets       - Sync assets from JSON to database")
        print("  stats             - Show database statistics")
        print("  list-assets       - List all assets")
        print("  gaps              - Show price data gaps")
        print("  sources           - Show data source summary")
        print("  show-outliers     - Show potential price outliers (sniper bot detection)")
        print("  cleanup-outliers  - Remove price outliers from database")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == "init":
        print("Initializing database...")
        conn = get_connection()
        init_schema(conn)
        count = load_assets_from_json(conn)
        print(f"Database initialized at {ANALYTICS_DB}")
        print(f"Loaded {count} assets from {ASSETS_FILE}")
        conn.close()
    
    elif command == "sync-assets":
        print("Syncing assets from JSON...")
        conn = get_connection()
        init_schema(conn)
        count = load_assets_from_json(conn)
        print(f"Synced {count} assets")
        conn.close()
    
    elif command == "stats":
        conn = get_connection()
        init_schema(conn)
        stats = get_db_stats(conn)
        print(f"\nDatabase: {ANALYTICS_DB}")
        print(f"Total assets: {stats['assets']} ({stats['enabled_assets']} enabled)")
        print("\nTweets by asset:")
        for asset_id, info in stats.get("tweets_by_asset", {}).items():
            print(f"  {info['name']}: {info['count']:,} tweets")
        print("\nPrice data by asset:")
        for asset_id, info in stats.get("prices_by_asset", {}).items():
            print(f"  {info['name']}:")
            for tf, tf_info in info.get("timeframes", {}).items():
                print(f"    {tf}: {tf_info['count']:,} candles ({tf_info['start'][:10] if tf_info['start'] else 'N/A'} to {tf_info['end'][:10] if tf_info['end'] else 'N/A'})")
        conn.close()
    
    elif command == "list-assets":
        conn = get_connection()
        init_schema(conn)
        assets = get_all_assets(conn)
        print(f"\n{'ID':<12} {'Name':<12} {'Founder':<18} {'Network':<12} {'Source':<14} {'Backfill':<10} {'Enabled'}")
        print("-" * 100)
        for a in assets:
            enabled = "✓" if a["enabled"] else "✗"
            backfill = a.get("backfill_source") or "-"
            print(f"{a['id']:<12} {a['name']:<12} {a['founder']:<18} {a['network'] or 'N/A':<12} {a['price_source']:<14} {backfill:<10} {enabled}")
        conn.close()
    
    elif command == "gaps":
        conn = get_connection()
        init_schema(conn)
        gaps = get_price_gaps(conn)
        if not gaps:
            print("No gaps detected in price data!")
        else:
            print(f"\nDetected {len(gaps)} gaps in price data:")
            for g in gaps[:20]:  # Show first 20
                print(f"  {g['asset_id']}/{g['timeframe']}: {g['gap_start'][:19]} → {g['gap_end'][:19]} ({g['missing_candles']} missing)")
            if len(gaps) > 20:
                print(f"  ... and {len(gaps) - 20} more")
        conn.close()
    
    elif command == "sources":
        conn = get_connection()
        init_schema(conn)
        summary = get_data_source_summary(conn)
        print("\nData source summary:")
        for asset_id, timeframes in summary.items():
            print(f"\n  {asset_id}:")
            for tf, sources in timeframes.items():
                for source, info in sources.items():
                    print(f"    {tf}/{source}: {info['count']:,} candles")
        conn.close()
    
    elif command == "show-outliers":
        # Parse args: --asset ASSET [--timeframe TF] [--threshold N]
        asset_id = None
        timeframe = "1m"
        threshold = OUTLIER_THRESHOLD_STD
        
        i = 2
        while i < len(sys.argv):
            if sys.argv[i] == "--asset" and i + 1 < len(sys.argv):
                asset_id = sys.argv[i + 1]
                i += 2
            elif sys.argv[i] == "--timeframe" and i + 1 < len(sys.argv):
                timeframe = sys.argv[i + 1]
                i += 2
            elif sys.argv[i] == "--threshold" and i + 1 < len(sys.argv):
                threshold = float(sys.argv[i + 1])
                i += 2
            else:
                i += 1
        
        if not asset_id:
            print("ERROR: --asset required")
            print("Usage: python db.py show-outliers --asset ASSET [--timeframe 1m] [--threshold 10]")
            sys.exit(1)
        
        conn = get_connection()
        init_schema(conn)
        
        print(f"\nDetecting outliers for {asset_id}/{timeframe} (threshold: {threshold}σ)")
        print("=" * 70)
        
        outliers = detect_price_outliers(conn, asset_id, timeframe, threshold)
        
        if not outliers:
            print("No outliers detected.")
        else:
            print(f"\nFound {len(outliers)} potential outliers:\n")
            print(f"{'Timestamp':<20} {'HIGH':>12} {'Ratio':>8} {'Median':>12} {'Threshold':>12}")
            print("-" * 70)
            for o in outliers[:20]:
                print(f"{o['timestamp_str']:<20} ${o['high']:>10.4f} {o['ratio']:>7.1f}x ${o['median']:>10.4f} ${o['threshold']:>10.4f}")
            if len(outliers) > 20:
                print(f"... and {len(outliers) - 20} more")
            
            print(f"\nTo remove these outliers, run:")
            print(f"  python db.py cleanup-outliers --asset {asset_id} --timeframe {timeframe} --confirm")
        
        conn.close()
    
    elif command == "cleanup-outliers":
        # Parse args: --asset ASSET [--timeframe TF] [--threshold N] --confirm
        asset_id = None
        timeframe = "1m"
        threshold = OUTLIER_THRESHOLD_STD
        confirm = False
        
        i = 2
        while i < len(sys.argv):
            if sys.argv[i] == "--asset" and i + 1 < len(sys.argv):
                asset_id = sys.argv[i + 1]
                i += 2
            elif sys.argv[i] == "--timeframe" and i + 1 < len(sys.argv):
                timeframe = sys.argv[i + 1]
                i += 2
            elif sys.argv[i] == "--threshold" and i + 1 < len(sys.argv):
                threshold = float(sys.argv[i + 1])
                i += 2
            elif sys.argv[i] == "--confirm":
                confirm = True
                i += 1
            else:
                i += 1
        
        if not asset_id:
            print("ERROR: --asset required")
            print("Usage: python db.py cleanup-outliers --asset ASSET [--timeframe 1m] [--threshold 10] --confirm")
            sys.exit(1)
        
        conn = get_connection()
        init_schema(conn)
        
        print(f"\nCleaning outliers for {asset_id}/{timeframe} (threshold: {threshold}σ)")
        print("=" * 70)
        
        # First show what would be deleted
        outliers = detect_price_outliers(conn, asset_id, timeframe, threshold)
        
        if not outliers:
            print("No outliers to clean.")
            conn.close()
            sys.exit(0)
        
        print(f"\nFound {len(outliers)} outliers to remove:")
        for o in outliers[:10]:
            print(f"  {o['timestamp_str']}: HIGH=${o['high']:.4f} ({o['ratio']:.1f}x median)")
        if len(outliers) > 10:
            print(f"  ... and {len(outliers) - 10} more")
        
        if not confirm:
            print(f"\n{'!'*60}")
            print("DRY RUN - no changes made. Add --confirm to actually delete.")
            print(f"{'!'*60}")
            conn.close()
            sys.exit(0)
        
        # Actually delete
        deleted = cleanup_outliers(conn, asset_id, timeframe, threshold, dry_run=False)
        print(f"\n✓ Deleted {deleted} outlier candles")
        print("Note: You may need to re-run export_static.py to regenerate JSON files")
        
        conn.close()
    
    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
