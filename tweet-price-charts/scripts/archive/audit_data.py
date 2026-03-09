import json
from pathlib import Path
from datetime import datetime, timedelta

DATA_DIR = Path("web/public/data")
ASSETS_FILE = DATA_DIR / "assets.json"

def audit_assets():
    print(f"{'ASSET':<10} | {'TWEETS':<6} | {'PRICE START':<12} | {'TWEET START':<12} | {'GAP (Days)':<10} | {'ALIGNED %':<10} | {'ISSUES FOUND'}")
    print("-" * 110)

    with open(ASSETS_FILE) as f:
        assets = json.load(f)["assets"]

    for asset in assets:
        asset_id = asset["id"]
        path = DATA_DIR / asset_id / "tweet_events.json"
        
        if not path.exists():
            print(f"{asset_id:<10} | {'MISSING':<30} | ❌ No event file")
            continue

        with open(path) as f:
            data = json.load(f)
            events = data["events"]
            
        if not events:
            print(f"{asset_id:<10} | {'0':<6} | {'N/A':<12} | {'N/A':<12} | {'-':<10} | {'0%':<10} | ⚠️ No tweets found")
            continue

        # Sort just in case
        events.sort(key=lambda x: x["timestamp"])

        # Price Range
        price_start_str = asset.get("price_range", {}).get("start", "N/A")
        price_start = datetime.fromisoformat(price_start_str.replace("Z", "+00:00")) if price_start_str != "N/A" else None

        # Tweet Range
        tweet_start = datetime.fromisoformat(events[0]["timestamp_iso"].replace("Z", "+00:00"))
        tweet_end = datetime.fromisoformat(events[-1]["timestamp_iso"].replace("Z", "+00:00"))
        
        # Gap Analysis
        gap_days = (tweet_start - price_start).days if price_start else 0
        
        # Alignment Quality
        aligned_count = sum(1 for e in events if e["price_at_tweet"] is not None)
        aligned_pct = (aligned_count / len(events)) * 100 if events else 0

        issues = []
        if aligned_pct < 90:
            issues.append(f"Low Alignment ({aligned_pct:.1f}%)")
        
        if price_start and (tweet_start - price_start).days > 30:
             issues.append(f"Late Tweet Start (+{gap_days}d)")
             
        if price_start and (price_start - tweet_start).days > 30:
             issues.append(f"Missing Early Prices (-{(price_start - tweet_start).days}d)")

        if len(events) < 50:
             issues.append("Low Tweet Volume")

        issue_str = ", ".join(issues) if issues else "✅ Clean"
        
        p_start_fmt = price_start.strftime('%Y-%m-%d') if price_start else "N/A"
        t_start_fmt = tweet_start.strftime('%Y-%m-%d')
        
        print(f"{asset_id:<10} | {len(events):<6} | {p_start_fmt:<12} | {t_start_fmt:<12} | {gap_days:<10} | {aligned_pct:>5.1f}%    | {issue_str}")

if __name__ == "__main__":
    audit_assets()

