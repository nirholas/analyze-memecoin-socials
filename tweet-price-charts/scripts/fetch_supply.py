#!/usr/bin/env python3
"""
Fetch circulating supply for all assets from on-chain sources.

Usage:
    python fetch_supply.py           # Fetch all assets
    python fetch_supply.py pump      # Fetch specific asset
    python fetch_supply.py --update  # Fetch and update assets.json
"""

import json
import httpx
import argparse
from pathlib import Path
from typing import Optional

# RPC endpoints by network
RPC_ENDPOINTS = {
    "solana": "https://api.mainnet-beta.solana.com",
    "bsc": "https://bsc-dataseed1.binance.org",
    "base": "https://mainnet.base.org",
    "ethereum": "https://eth.llamarpc.com",
    "monad": "https://rpc.monad.xyz",
}

# ERC20 totalSupply() function signature
TOTAL_SUPPLY_SIG = "0x18160ddd"


def get_solana_supply(token_mint: str) -> dict:
    """
    Query Solana RPC for SPL token supply.

    Returns dict with:
        - amount: raw amount (string)
        - decimals: token decimals
        - ui_amount: human-readable amount
    """
    rpc_url = RPC_ENDPOINTS["solana"]
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTokenSupply",
        "params": [token_mint]
    }

    response = httpx.post(rpc_url, json=payload, timeout=30)
    response.raise_for_status()
    data = response.json()

    if "error" in data:
        raise Exception(f"Solana RPC error: {data['error']}")

    value = data["result"]["value"]
    return {
        "amount": value["amount"],
        "decimals": value["decimals"],
        "ui_amount": float(value["uiAmountString"]),
    }


def get_evm_supply(contract_address: str, network: str) -> dict:
    """
    Query EVM RPC for ERC20 totalSupply().

    Returns dict with:
        - amount: raw amount (int)
        - decimals: assumed 18 (need separate call for actual)
        - ui_amount: amount / 10^decimals
    """
    rpc_url = RPC_ENDPOINTS.get(network)
    if not rpc_url:
        raise Exception(f"No RPC endpoint for network: {network}")

    # eth_call for totalSupply()
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [
            {
                "to": contract_address,
                "data": TOTAL_SUPPLY_SIG
            },
            "latest"
        ]
    }

    response = httpx.post(rpc_url, json=payload, timeout=30)
    response.raise_for_status()
    data = response.json()

    if "error" in data:
        raise Exception(f"EVM RPC error: {data['error']}")

    # Result is hex-encoded uint256
    hex_result = data["result"]
    amount = int(hex_result, 16)

    # Get decimals
    decimals = get_evm_decimals(contract_address, network)
    ui_amount = amount / (10 ** decimals)

    return {
        "amount": str(amount),
        "decimals": decimals,
        "ui_amount": ui_amount,
    }


def get_evm_decimals(contract_address: str, network: str) -> int:
    """Query ERC20 decimals()."""
    rpc_url = RPC_ENDPOINTS.get(network)

    # decimals() function signature
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_call",
        "params": [
            {
                "to": contract_address,
                "data": "0x313ce567"  # decimals()
            },
            "latest"
        ]
    }

    response = httpx.post(rpc_url, json=payload, timeout=30)
    response.raise_for_status()
    data = response.json()

    if "error" in data or data["result"] == "0x":
        return 18  # Default to 18 decimals

    return int(data["result"], 16)


def get_hyperliquid_supply(coingecko_id: str) -> dict:
    """
    For Hyperliquid, use CoinGecko to get circulating supply.
    Hyperliquid is a perp exchange, not a standard blockchain.
    """
    url = f"https://api.coingecko.com/api/v3/coins/{coingecko_id}"
    response = httpx.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()

    circulating = data.get("market_data", {}).get("circulating_supply")
    total = data.get("market_data", {}).get("total_supply")

    return {
        "amount": str(int(circulating or total or 0)),
        "decimals": 0,  # Already in UI amount
        "ui_amount": circulating or total or 0,
        "source": "coingecko",
    }


def fetch_asset_supply(asset: dict) -> Optional[dict]:
    """
    Fetch supply for a single asset based on its network and token info.
    """
    asset_id = asset["id"]
    network = asset["network"]
    token_mint = asset.get("token_mint")
    coingecko_id = asset.get("coingecko_id")

    print(f"[{asset_id}] Fetching supply for {asset['name']} on {network}...")

    try:
        if network == "solana":
            if not token_mint:
                print(f"  ⚠ No token_mint for {asset_id}")
                return None
            result = get_solana_supply(token_mint)

        elif network == "hyperliquid":
            if not coingecko_id:
                print(f"  ⚠ No coingecko_id for {asset_id}")
                return None
            result = get_hyperliquid_supply(coingecko_id)

        elif network in ["bsc", "base", "ethereum", "monad"]:
            # Check if it's a native token (zero address)
            if not token_mint or token_mint == "0x0000000000000000000000000000000000000000":
                # Native token - use CoinGecko
                if coingecko_id:
                    print(f"  → Native token, using CoinGecko")
                    result = get_hyperliquid_supply(coingecko_id)
                else:
                    print(f"  ⚠ No token_mint or coingecko_id for {asset_id}")
                    return None
            else:
                result = get_evm_supply(token_mint, network)

        else:
            print(f"  ⚠ Unknown network: {network}")
            return None

        print(f"  ✓ Supply: {result['ui_amount']:,.0f} ({result['decimals']} decimals)")
        return result

    except Exception as e:
        print(f"  ✗ Error: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="Fetch token supply from on-chain sources")
    parser.add_argument("asset_id", nargs="?", help="Specific asset to fetch (optional)")
    parser.add_argument("--update", action="store_true", help="Update assets.json with fetched supply")
    args = parser.parse_args()

    # Load assets
    assets_path = Path(__file__).parent / "assets.json"
    with open(assets_path) as f:
        assets_data = json.load(f)

    assets = assets_data["assets"]

    # Filter to specific asset if provided
    if args.asset_id:
        assets = [a for a in assets if a["id"] == args.asset_id]
        if not assets:
            print(f"Asset not found: {args.asset_id}")
            return

    # Fetch supply for each asset
    results = {}
    for asset in assets:
        if not asset.get("enabled", True):
            continue

        result = fetch_asset_supply(asset)
        if result:
            results[asset["id"]] = result

    # Print summary
    print("\n" + "=" * 60)
    print("SUPPLY SUMMARY")
    print("=" * 60)

    for asset in assets:
        if not asset.get("enabled", True):
            continue
        asset_id = asset["id"]
        if asset_id in results:
            supply = results[asset_id]["ui_amount"]
            print(f"{asset['name']:12} {supply:>20,.0f}")
        else:
            print(f"{asset['name']:12} {'FAILED':>20}")

    # Update assets.json if requested
    if args.update:
        print("\n" + "=" * 60)
        print("UPDATING assets.json")
        print("=" * 60)

        for asset in assets_data["assets"]:
            asset_id = asset["id"]
            if asset_id in results:
                # Store as integer (ui_amount already accounts for decimals)
                supply = int(results[asset_id]["ui_amount"])
                asset["circulating_supply"] = supply
                print(f"  ✓ {asset['name']}: {supply:,}")

        with open(assets_path, "w") as f:
            json.dump(assets_data, f, indent=2)

        print(f"\n✓ Updated {assets_path}")


if __name__ == "__main__":
    main()
