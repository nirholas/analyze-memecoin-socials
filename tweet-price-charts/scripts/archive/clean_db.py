import shutil
from pathlib import Path

DATA_DIR = Path("data")
WEB_DIR = Path("web/public/data")

# Assets to keep and refresh
ACTIVE_ASSETS = ["pump", "hype", "useless", "monad", "jup", "aster", "believe"]

# Assets to remove completely
REMOVE_ASSETS = ["pengu", "ada", "icp"]

def clean_all():
    print("=" * 60)
    print("CLEANING DATABASE")
    print("=" * 60)

    # 1. Remove deleted assets
    for asset in REMOVE_ASSETS:
        for base_dir in [DATA_DIR, WEB_DIR]:
            path = base_dir / asset
            if path.exists():
                shutil.rmtree(path)
                print(f"  Deleted folder: {path}")

    # 2. Delete tweet data for active assets (force refresh)
    for asset in ACTIVE_ASSETS:
        tweet_file = DATA_DIR / asset / "tweets.json"
        event_file = DATA_DIR / asset / "tweet_events.json"
        
        if tweet_file.exists():
            tweet_file.unlink()
            print(f"  Deleted: {tweet_file}")
            
        if event_file.exists():
            event_file.unlink()
            print(f"  Deleted: {event_file}")

    print("\nCleanup complete. Ready for fresh fetch.")

if __name__ == "__main__":
    clean_all()

