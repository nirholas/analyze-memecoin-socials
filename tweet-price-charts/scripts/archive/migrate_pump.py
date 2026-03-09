"""
One-time migration script to move existing PUMP data to the new subdirectory structure.
Run this once before using the new multi-asset scripts.
"""
import shutil
from pathlib import Path

# Source paths (old structure)
DATA_DIR = Path(__file__).parent.parent / "data"
WEB_PUBLIC_DATA = Path(__file__).parent.parent / "web" / "public" / "data"

# Destination paths (new structure)
PUMP_DATA_DIR = DATA_DIR / "pump"
PUMP_PUBLIC_DIR = WEB_PUBLIC_DATA / "pump"


def migrate_data_files():
    """Migrate data files to pump/ subdirectory."""
    print("=" * 60)
    print("Migrating PUMP data to new structure")
    print("=" * 60)
    
    # Create destination directories
    PUMP_DATA_DIR.mkdir(exist_ok=True)
    PUMP_PUBLIC_DIR.mkdir(parents=True, exist_ok=True)
    
    # Files to migrate from data/ to data/pump/
    data_files = [
        ("tweets.json", "tweets.json"),
        ("prices.db", "prices.db"),
        ("prices.db-shm", "prices.db-shm"),
        ("prices.db-wal", "prices.db-wal"),
        ("tweet_events.json", "tweet_events.json"),
        ("stats.json", "stats.json"),
    ]
    
    print("\nMigrating data/ files:")
    for src_name, dst_name in data_files:
        src = DATA_DIR / src_name
        dst = PUMP_DATA_DIR / dst_name
        
        if src.exists():
            if dst.exists():
                print(f"  ⊘ {src_name} → already exists at destination")
            else:
                shutil.copy2(src, dst)
                print(f"  ✓ {src_name} → pump/{dst_name}")
        else:
            print(f"  - {src_name} → not found, skipping")
    
    # Files to migrate in web/public/data/ to web/public/data/pump/
    # These are the exported price JSON files
    web_files = [
        "prices_1d.json",
        "prices_1h.json",
        "prices_15m.json",
        "prices_1m_index.json",
        "tweet_events.json",
        "stats.json",
    ]
    
    # Also migrate monthly 1m chunks
    for f in WEB_PUBLIC_DATA.glob("prices_1m_*.json"):
        if f.name not in web_files:
            web_files.append(f.name)
    
    print("\nMigrating web/public/data/ files:")
    for filename in web_files:
        src = WEB_PUBLIC_DATA / filename
        dst = PUMP_PUBLIC_DIR / filename
        
        if src.exists():
            if dst.exists():
                print(f"  ⊘ {filename} → already exists at destination")
            else:
                shutil.copy2(src, dst)
                print(f"  ✓ {filename} → pump/{filename}")
        else:
            print(f"  - {filename} → not found, skipping")
    
    print("\n" + "=" * 60)
    print("Migration complete!")
    print("=" * 60)
    print("\nNote: Original files were COPIED, not moved.")
    print("You can delete the old files manually after verifying.")
    print("\nOld file locations:")
    print(f"  {DATA_DIR}/tweets.json")
    print(f"  {DATA_DIR}/prices.db")
    print(f"  {WEB_PUBLIC_DATA}/prices_*.json")


if __name__ == "__main__":
    migrate_data_files()

