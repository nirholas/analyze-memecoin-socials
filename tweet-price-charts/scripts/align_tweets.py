"""
Tweet-Price alignment utility.
Now uses the tweet_events view in DuckDB which handles alignment automatically.

The tweet_events view joins tweets with prices and filters pre-launch tweets.
This script provides a CLI to view and export aligned events.

Usage:
    python align_tweets.py                     # Show alignment stats for all assets
    python align_tweets.py --asset pump        # Show alignment for specific asset
    python align_tweets.py --asset pump --export  # Export to JSON
"""
import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any

from config import DATA_DIR, PUBLIC_DATA_DIR
from db import (
    get_connection, init_schema, get_asset, get_enabled_assets,
    get_tweet_events
)


def get_alignment_stats(
    conn,
    asset_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Get alignment statistics for assets.
    
    Returns stats about how many tweets have price data aligned.
    """
    if asset_id:
        assets = [get_asset(conn, asset_id)]
        if not assets[0]:
            return {"error": f"Asset '{asset_id}' not found"}
    else:
        assets = get_enabled_assets(conn)
    
    stats = {}
    
    for asset in assets:
        aid = asset["id"]
        
        # Total tweets (post-launch)
        total = conn.execute("""
            SELECT COUNT(*) FROM tweets t
            JOIN assets a ON t.asset_id = a.id
            WHERE t.asset_id = ? AND t.timestamp >= a.launch_date
        """, [aid]).fetchone()[0]
        
        # Check if we have 1m data
        has_1m = conn.execute("""
            SELECT COUNT(*) FROM prices 
            WHERE asset_id = ? AND timeframe = '1m'
        """, [aid]).fetchone()[0] > 0
        
        # Get aligned events
        events = get_tweet_events(conn, aid, use_daily_fallback=not has_1m)
        
        # Count alignments
        with_price = sum(1 for e in events if e.get("price_at_tweet") is not None)
        with_1h = sum(1 for e in events if e.get("price_1h") is not None)
        with_24h = sum(1 for e in events if e.get("price_24h") is not None)
        
        # Average price changes
        changes_1h = [e["change_1h_pct"] for e in events if e.get("change_1h_pct") is not None]
        changes_24h = [e["change_24h_pct"] for e in events if e.get("change_24h_pct") is not None]
        
        avg_1h = sum(changes_1h) / len(changes_1h) if changes_1h else None
        avg_24h = sum(changes_24h) / len(changes_24h) if changes_24h else None
        
        stats[aid] = {
            "name": asset["name"],
            "founder": asset["founder"],
            "total_tweets": total,
            "aligned_count": len(events),
            "with_price_at_tweet": with_price,
            "with_price_1h": with_1h,
            "with_price_24h": with_24h,
            "alignment_rate": round(with_price / total * 100, 1) if total > 0 else 0,
            "price_resolution": "1m" if has_1m else "1d",
            "avg_change_1h": round(avg_1h, 2) if avg_1h else None,
            "avg_change_24h": round(avg_24h, 2) if avg_24h else None,
        }
    
    return stats


def export_aligned_events(
    asset_id: str,
    output_path: Optional[Path] = None,
    use_daily_fallback: bool = False
) -> int:
    """
    Export aligned tweet events to JSON.
    
    Returns count of events exported.
    """
    conn = get_connection()
    init_schema(conn)
    
    asset = get_asset(conn, asset_id)
    if not asset:
        conn.close()
        print(f"Asset '{asset_id}' not found")
        return 0
    
    # Check if we have 1m data
    has_1m = conn.execute("""
        SELECT COUNT(*) FROM prices 
        WHERE asset_id = ? AND timeframe = '1m'
    """, [asset_id]).fetchone()[0] > 0
    
    if not has_1m:
        use_daily_fallback = True
    
    events = get_tweet_events(conn, asset_id, use_daily_fallback=use_daily_fallback)
    conn.close()
    
    if not events:
        print(f"No aligned events for {asset_id}")
        return 0
    
    output = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "asset": asset_id,
        "asset_name": asset["name"],
        "founder": asset["founder"],
        "price_definition": "candle close at minute boundary" if not use_daily_fallback else "daily close",
        "count": len(events),
        "events": events
    }
    
    # Determine output path
    if output_path is None:
        # Save to both data/ and web/public/data/
        output_path = DATA_DIR / asset_id / "tweet_events.json"
    
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, "w") as f:
        json.dump(output, f, indent=2)
    
    print(f"Exported {len(events)} events to {output_path}")

    # Also save to public directory
    public_path = PUBLIC_DATA_DIR / asset_id / "tweet_events.json"
    public_path.parent.mkdir(parents=True, exist_ok=True)
    
    with open(public_path, "w") as f:
        json.dump(output, f, indent=2)
    
    print(f"Exported {len(events)} events to {public_path}")
    
    return len(events)


def print_alignment_stats(stats: Dict[str, Any]):
    """Pretty print alignment statistics."""
    print("\n" + "=" * 80)
    print("TWEET-PRICE ALIGNMENT STATISTICS")
    print("=" * 80)
    
    print(f"\n{'Asset':<12} {'Founder':<18} {'Tweets':<8} {'Aligned':<8} {'Rate':<8} {'Res':<6} {'Avg 1h':<10} {'Avg 24h':<10}")
    print("-" * 80)
    
    for asset_id, s in stats.items():
        avg_1h = f"{s['avg_change_1h']:+.1f}%" if s.get('avg_change_1h') is not None else "N/A"
        avg_24h = f"{s['avg_change_24h']:+.1f}%" if s.get('avg_change_24h') is not None else "N/A"
        
        print(f"{s['name']:<12} @{s['founder']:<17} {s['total_tweets']:<8} {s['with_price_at_tweet']:<8} {s['alignment_rate']:.1f}%   {s['price_resolution']:<6} {avg_1h:<10} {avg_24h:<10}")
    
    # Summary
    total_tweets = sum(s["total_tweets"] for s in stats.values())
    total_aligned = sum(s["with_price_at_tweet"] for s in stats.values())
    
    print("-" * 80)
    print(f"{'TOTAL':<12} {'':<18} {total_tweets:<8} {total_aligned:<8} {total_aligned/total_tweets*100:.1f}%")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Tweet-Price alignment statistics and export"
    )
    parser.add_argument(
        "--asset", "-a",
        type=str,
        help="Specific asset ID (default: all enabled assets)"
    )
    parser.add_argument(
        "--export", "-e",
        action="store_true",
        help="Export aligned events to JSON"
    )
    parser.add_argument(
        "--daily",
        action="store_true",
        help="Use daily price fallback (for assets without 1m data)"
    )
    
    args = parser.parse_args()
    
    conn = get_connection()
    init_schema(conn)
    
    # Get and display stats
    stats = get_alignment_stats(conn, args.asset)
    conn.close()
    
    if "error" in stats:
        print(f"Error: {stats['error']}")
        return
    
    print_alignment_stats(stats)
    
    # Export if requested
    if args.export:
        print("\n" + "=" * 80)
        print("EXPORTING ALIGNED EVENTS")
        print("=" * 80)
        
        if args.asset:
            export_aligned_events(args.asset, use_daily_fallback=args.daily)
        else:
            for asset_id in stats.keys():
                export_aligned_events(asset_id, use_daily_fallback=args.daily)


if __name__ == "__main__":
    main()
