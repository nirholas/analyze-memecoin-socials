"""
Tweet polling daemon for real-time ingestion.
Polls Twitter API at regular intervals to fetch new tweets for all enabled assets.

Features:
- Configurable polling interval (default: 5 minutes)
- Incremental fetching using since_id
- Pre-launch filtering
- Optional auto-export after new tweets
- Graceful shutdown on SIGINT/SIGTERM

Usage:
    python tweet_poller.py                      # Run with default 5 min interval
    python tweet_poller.py --interval 60        # Poll every 60 seconds
    python tweet_poller.py --once               # Run once and exit
    python tweet_poller.py --export             # Auto-export after fetching
"""
import argparse
import signal
import sys
import time
from datetime import datetime
from typing import Optional

from fetch_tweets import fetch_for_asset
from export_static import export_asset, export_assets_json
from db import get_connection, init_schema, get_enabled_assets


# Global flag for graceful shutdown
shutdown_requested = False


def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global shutdown_requested
    print(f"\n\nShutdown requested (signal {signum}). Finishing current cycle...")
    shutdown_requested = True


def poll_once(auto_export: bool = False) -> dict:
    """
    Run a single polling cycle for all enabled assets.
    
    Args:
        auto_export: If True, export updated assets after fetching
    
    Returns dict of asset_id -> result
    """
    conn = get_connection()
    init_schema(conn)
    assets = get_enabled_assets(conn)
    conn.close()
    
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"\n{'='*60}")
    print(f"Poll cycle started at {timestamp}")
    print(f"{'='*60}")
    
    results = {}
    assets_with_new_tweets = []
    
    for asset in assets:
        asset_id = asset["id"]
        print(f"\n--- {asset['name']} (@{asset['founder']}) ---")
        
        try:
            result = fetch_for_asset(asset_id, full_fetch=False, save_json=True)
            results[asset_id] = result
            
            if result.get("status") == "success" and result.get("inserted", 0) > 0:
                assets_with_new_tweets.append(asset_id)
                print(f"    ✓ {result['inserted']} new tweets")
            elif result.get("status") == "success":
                print(f"    - No new tweets")
            else:
                print(f"    ✗ {result.get('reason', 'unknown error')}")
                
        except Exception as e:
            print(f"    ✗ Error: {e}")
            results[asset_id] = {"status": "error", "reason": str(e)}
    
    # Auto-export if requested and we have new tweets
    if auto_export and assets_with_new_tweets:
        print(f"\n{'='*60}")
        print(f"Auto-exporting {len(assets_with_new_tweets)} assets with new tweets...")
        print(f"{'='*60}")
        
        export_assets_json()  # Always update assets.json
        
        for asset_id in assets_with_new_tweets:
            try:
                export_asset(asset_id)
            except Exception as e:
                print(f"    Export error for {asset_id}: {e}")
    
    # Summary
    total_new = sum(
        r.get("inserted", 0) 
        for r in results.values() 
        if r.get("status") == "success"
    )
    
    print(f"\n{'='*60}")
    print(f"Poll cycle complete: {total_new} new tweets across {len(assets)} assets")
    print(f"{'='*60}")
    
    return results


def run_daemon(
    interval_seconds: int = 300,
    auto_export: bool = False
):
    """
    Run the polling daemon continuously.
    
    Args:
        interval_seconds: Time between poll cycles (default: 300 = 5 minutes)
        auto_export: If True, export after each cycle with new tweets
    """
    global shutdown_requested
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print("=" * 60)
    print("Tweet Polling Daemon")
    print("=" * 60)
    print(f"Poll interval: {interval_seconds} seconds")
    print(f"Auto-export: {auto_export}")
    print(f"Press Ctrl+C to stop")
    print()
    
    cycle = 0
    
    while not shutdown_requested:
        cycle += 1
        print(f"\n[Cycle {cycle}]")
        
        try:
            poll_once(auto_export=auto_export)
        except Exception as e:
            print(f"Error in poll cycle: {e}")
        
        if shutdown_requested:
            break
        
        # Wait for next cycle
        print(f"\nNext poll in {interval_seconds} seconds...")
        
        # Sleep in small increments to allow quick shutdown
        wait_time = 0
        while wait_time < interval_seconds and not shutdown_requested:
            time.sleep(min(10, interval_seconds - wait_time))
            wait_time += 10
    
    print("\nDaemon stopped.")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Tweet polling daemon for real-time ingestion"
    )
    parser.add_argument(
        "--interval", "-i",
        type=int,
        default=300,
        help="Polling interval in seconds (default: 300 = 5 minutes)"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run once and exit (don't loop)"
    )
    parser.add_argument(
        "--export", "-e",
        action="store_true",
        help="Auto-export assets after fetching new tweets"
    )
    
    args = parser.parse_args()
    
    if args.once:
        poll_once(auto_export=args.export)
    else:
        run_daemon(
            interval_seconds=args.interval,
            auto_export=args.export
        )


if __name__ == "__main__":
    main()


