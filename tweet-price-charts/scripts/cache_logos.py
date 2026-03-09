"""
Cache token logos locally.
Downloads logos from CoinGecko (for tokens with coingecko_id) or GeckoTerminal
(for Solana tokens), resizes to 64x64, and stores in public/logos.

Also verifies and reports official token names/symbols from APIs.

Usage:
    python cache_logos.py                  # Cache all enabled assets
    python cache_logos.py --asset pump     # Cache specific asset's logo
    python cache_logos.py --force          # Re-download even if cached
"""
import argparse
import json
import time
from pathlib import Path
from typing import Optional, Dict, Any, Tuple
from io import BytesIO

import httpx
from PIL import Image

from config import LOGOS_DIR, ASSETS_FILE
from db import get_connection, init_schema, get_enabled_assets, get_asset


# Logo size for consistent display
LOGO_SIZE = 64

# API endpoints
COINGECKO_API = "https://api.coingecko.com/api/v3"
GECKOTERMINAL_API = "https://api.geckoterminal.com/api/v2"


def fetch_coingecko_logo(coingecko_id: str) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
    """
    Fetch logo URL and token info from CoinGecko.
    
    Returns:
        (logo_url, token_info) where token_info has 'name' and 'symbol'
    """
    url = f"{COINGECKO_API}/coins/{coingecko_id}"
    params = {
        "localization": "false",
        "tickers": "false",
        "market_data": "false",
        "community_data": "false",
        "developer_data": "false",
    }
    
    try:
        with httpx.Client(timeout=30.0) as client:
            print(f"    Fetching from CoinGecko: {coingecko_id}")
            response = client.get(url, params=params)
            
            if response.status_code == 429:
                print(f"    Rate limited, waiting 60s...")
                time.sleep(60)
                response = client.get(url, params=params)
            
            if response.status_code != 200:
                print(f"    CoinGecko error: {response.status_code}")
                return None, None
            
            data = response.json()
            
            # Get logo URL (prefer 'small' for reasonable quality)
            image_url = data.get("image", {}).get("small")
            
            # Get official name/symbol
            token_info = {
                "name": data.get("name"),
                "symbol": data.get("symbol", "").upper(),
            }
            
            return image_url, token_info
            
    except Exception as e:
        print(f"    Error fetching from CoinGecko: {e}")
        return None, None


def fetch_geckoterminal_logo(network: str, pool_address: str) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
    """
    Fetch logo URL and token info from GeckoTerminal.
    
    Args:
        network: Network name (e.g., 'solana', 'bsc')
        pool_address: The pool/pair address
    
    Returns:
        (logo_url, token_info) where token_info has 'name' and 'symbol'
    """
    # Map network names to GeckoTerminal network IDs
    network_map = {
        "solana": "solana",
        "bsc": "bsc",
        "ethereum": "eth",
    }
    gt_network = network_map.get(network, network)
    
    url = f"{GECKOTERMINAL_API}/networks/{gt_network}/pools/{pool_address}"
    
    try:
        with httpx.Client(timeout=30.0) as client:
            print(f"    Fetching from GeckoTerminal: {gt_network}/{pool_address[:16]}...")
            response = client.get(url)
            
            if response.status_code != 200:
                print(f"    GeckoTerminal error: {response.status_code}")
                return None, None
            
            data = response.json()
            pool_data = data.get("data", {}).get("attributes", {})
            
            # Get base token info (the token we care about, not the quote token)
            base_token = pool_data.get("base_token_price_quote_token")
            
            # Try to get token info from relationships
            relationships = data.get("data", {}).get("relationships", {})
            base_token_data = relationships.get("base_token", {}).get("data", {})
            
            # The token ID in GeckoTerminal is like "solana_<address>"
            token_id = base_token_data.get("id", "")
            
            # Fetch token details
            if token_id:
                token_url = f"{GECKOTERMINAL_API}/networks/{gt_network}/tokens/{token_id.split('_')[-1]}"
                token_response = client.get(token_url)
                
                if token_response.status_code == 200:
                    token_data = token_response.json().get("data", {}).get("attributes", {})
                    
                    image_url = token_data.get("image_url")
                    token_info = {
                        "name": token_data.get("name"),
                        "symbol": token_data.get("symbol", "").upper(),
                    }
                    
                    return image_url, token_info
            
            # Fallback: try to get from pool name
            pool_name = pool_data.get("name", "")
            if " / " in pool_name:
                symbol = pool_name.split(" / ")[0]
                return None, {"name": symbol, "symbol": symbol}
            
            return None, None
            
    except Exception as e:
        print(f"    Error fetching from GeckoTerminal: {e}")
        return None, None


def download_and_resize(url: str, output_path: Path, size: int = LOGO_SIZE) -> bool:
    """Download image, resize to square, and save as PNG."""
    try:
        with httpx.Client(timeout=30.0, follow_redirects=True) as client:
            response = client.get(url)
            response.raise_for_status()
            
            # Open and process image
            img = Image.open(BytesIO(response.content))
            
            # Convert to RGBA for transparency support
            if img.mode != "RGBA":
                img = img.convert("RGBA")
            
            # Resize with high-quality resampling
            img = img.resize((size, size), Image.Resampling.LANCZOS)
            
            # Save
            output_path.parent.mkdir(parents=True, exist_ok=True)
            img.save(output_path, "PNG", optimize=True)
            
            return True
            
    except Exception as e:
        print(f"    Error downloading/resizing: {e}")
        return False


def load_assets() -> list:
    """Load assets from assets.json."""
    if not ASSETS_FILE.exists():
        print(f"Error: {ASSETS_FILE} not found")
        return []
    
    with open(ASSETS_FILE) as f:
        data = json.load(f)
    
    return data.get("assets", [])


def save_assets(assets: list):
    """Save updated assets back to assets.json."""
    with open(ASSETS_FILE) as f:
        data = json.load(f)
    
    data["assets"] = assets
    
    with open(ASSETS_FILE, "w") as f:
        json.dump(data, f, indent=2)
    
    print(f"\nUpdated {ASSETS_FILE}")


def cache_single_asset(asset: dict, force: bool = False) -> bool:
    """Cache logo for a single asset. Returns True on success."""
    asset_id = asset["id"]
    output_path = LOGOS_DIR / f"{asset_id}.png"

    # Skip if already cached (unless force)
    if output_path.exists() and not force:
        size_kb = output_path.stat().st_size / 1024
        print(f"✓ {asset_id}: Already cached ({size_kb:.1f} KB)")
        return True

    print(f"→ {asset_id} ({asset['name']}): Fetching logo...")

    logo_url = None
    token_info = None

    # Strategy 1: Try CoinGecko if we have an ID
    coingecko_id = asset.get("coingecko_id")
    if coingecko_id:
        logo_url, token_info = fetch_coingecko_logo(coingecko_id)
        time.sleep(1.5)  # Rate limit

    # Strategy 2: Try GeckoTerminal if no CoinGecko or it failed
    if not logo_url:
        pool_address = asset.get("pool_address")
        network = asset.get("network")

        if pool_address and network:
            logo_url, token_info = fetch_geckoterminal_logo(network, pool_address)
            time.sleep(0.5)

    # Download and save logo
    if logo_url:
        print(f"  Logo URL: {logo_url[:60]}...")

        if download_and_resize(logo_url, output_path):
            size_kb = output_path.stat().st_size / 1024
            print(f"  ✓ Saved: {output_path.name} ({size_kb:.1f} KB)")
            return True
        else:
            print(f"  ✗ Failed to download logo")
            return False
    else:
        print(f"  ✗ Could not find logo URL")
        return False


def main():
    """Cache logos for all enabled assets."""
    parser = argparse.ArgumentParser(description="Cache token logos")
    parser.add_argument("--asset", type=str, help="Cache logo for specific asset")
    parser.add_argument("--force", action="store_true", help="Re-download even if cached")
    args = parser.parse_args()

    print("=" * 60)
    print("Token Logo Caching")
    print("=" * 60)

    # Ensure logos directory exists
    LOGOS_DIR.mkdir(parents=True, exist_ok=True)

    if args.asset:
        # Single asset mode
        conn = get_connection()
        init_schema(conn)
        asset = get_asset(conn, args.asset)
        conn.close()

        if not asset:
            print(f"✗ Asset '{args.asset}' not found")
            return

        print(f"\nCaching logo for {asset.get('name')} ({args.asset})")
        success = cache_single_asset(asset, force=args.force)
        print(f"\n{'✓ Success' if success else '✗ Failed'}")
        return

    # All assets mode
    print(f"Output: {LOGOS_DIR}")

    assets = load_assets()
    enabled_assets = [a for a in assets if a.get("enabled", True)]

    print(f"\nFound {len(enabled_assets)} enabled assets:")
    for a in enabled_assets:
        print(f"  - {a['id']}: {a['name']} ({a.get('network', 'unknown')})")
    
    success_count = 0
    fail_count = 0
    name_discrepancies = []
    
    for asset in enabled_assets:
        asset_id = asset["id"]
        output_path = LOGOS_DIR / f"{asset_id}.png"
        
        print(f"\n{'─' * 40}")
        print(f"Processing: {asset_id} ({asset['name']})")

        # Skip if already cached (unless --force)
        if output_path.exists() and not args.force:
            size_kb = output_path.stat().st_size / 1024
            print(f"  ✓ Already cached ({size_kb:.1f} KB)")
            success_count += 1

            # Still add logo field if missing
            if "logo" not in asset:
                asset["logo"] = f"/logos/{asset_id}.png"
            continue
        
        logo_url = None
        token_info = None
        
        # Strategy 1: Try CoinGecko if we have an ID
        coingecko_id = asset.get("coingecko_id")
        if coingecko_id:
            logo_url, token_info = fetch_coingecko_logo(coingecko_id)
            time.sleep(1.5)  # Rate limit: ~30 req/min for free tier
        
        # Strategy 2: Try GeckoTerminal if no CoinGecko or it failed
        if not logo_url:
            pool_address = asset.get("pool_address")
            network = asset.get("network")
            
            if pool_address and network:
                logo_url, token_info = fetch_geckoterminal_logo(network, pool_address)
                time.sleep(0.5)  # GeckoTerminal is more generous
        
        # Check for name discrepancies
        if token_info:
            api_name = token_info.get("name")
            api_symbol = token_info.get("symbol")
            local_name = asset.get("name")
            
            if api_symbol and api_symbol != local_name:
                discrepancy = {
                    "asset_id": asset_id,
                    "local_name": local_name,
                    "api_name": api_name,
                    "api_symbol": api_symbol,
                }
                name_discrepancies.append(discrepancy)
                print(f"  ⚠ Name mismatch: local='{local_name}' vs API='{api_symbol}' ({api_name})")
        
        # Download and save logo
        if logo_url:
            print(f"  Logo URL: {logo_url[:60]}...")
            
            if download_and_resize(logo_url, output_path):
                size_kb = output_path.stat().st_size / 1024
                print(f"  ✓ Saved: {output_path.name} ({size_kb:.1f} KB)")
                success_count += 1
                
                # Add logo path to asset
                asset["logo"] = f"/logos/{asset_id}.png"
            else:
                print(f"  ✗ Failed to download logo")
                fail_count += 1
        else:
            print(f"  ✗ Could not find logo URL")
            fail_count += 1
    
    # Save updated assets with logo paths
    save_assets(assets)
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"  Cached: {success_count}/{len(enabled_assets)}")
    if fail_count > 0:
        print(f"  Failed: {fail_count}")
    
    if name_discrepancies:
        print(f"\n  Name discrepancies found ({len(name_discrepancies)}):")
        for d in name_discrepancies:
            print(f"    - {d['asset_id']}: '{d['local_name']}' → API says '{d['api_symbol']}' ({d['api_name']})")
        print("\n  Review and manually update assets.json if needed.")
    
    print(f"\n  Output: {LOGOS_DIR}")
    
    # List generated files
    print("\n  Generated files:")
    for f in sorted(LOGOS_DIR.glob("*.png")):
        size_kb = f.stat().st_size / 1024
        print(f"    {f.name} ({size_kb:.1f} KB)")


if __name__ == "__main__":
    main()

