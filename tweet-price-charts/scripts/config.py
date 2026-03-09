"""
Configuration for the tweet-price correlation analyzer.
"""
import os
import pathlib
from dotenv import load_dotenv

# Load environment variables from .env file (in project root)
PROJECT_ROOT = pathlib.Path(__file__).parent.parent
load_dotenv(PROJECT_ROOT / ".env")

# X API Configuration
X_BEARER_TOKEN = os.getenv("X_BEARER_TOKEN")
TARGET_USERNAME = os.getenv("TARGET_USERNAME", "a1lon9")

# API Endpoints
X_API_BASE = "https://api.twitter.com/2"

# Rate limiting
RATE_LIMIT_DELAY = 1.1  # seconds between requests

# Data paths
DATA_DIR = PROJECT_ROOT / "data"

# Primary database (single source of truth)
ANALYTICS_DB = DATA_DIR / "analytics.duckdb"
ASSETS_FILE = PROJECT_ROOT / "scripts" / "assets.json"

# Static output paths (for frontend)
WEB_DIR = PROJECT_ROOT / "web"
PUBLIC_DATA_DIR = WEB_DIR / "public" / "static"
AVATARS_DIR = WEB_DIR / "public" / "avatars"
LOGOS_DIR = WEB_DIR / "public" / "logos"

# Ensure directories exist
DATA_DIR.mkdir(exist_ok=True)

# PUMP pool configuration (found via DexScreener) - Legacy, use assets.json instead
PUMP_POOL_ADDRESS = "2uF4Xh61rDwxnG9woyxsVQP7zuA6kLFpb3NvnRQeoiSd"

# Supported timeframes
TIMEFRAMES = ["1m", "15m", "1h", "1d"]

# Age-based timeframe skipping thresholds
# Assets older than these thresholds don't get granular data (API limitations + storage)
SKIP_1M_AFTER_DAYS = 90      # Skip 1m for assets older than 90 days
SKIP_15M_AFTER_DAYS = 365    # Skip 15m for assets older than 365 days

# GeckoTerminal API mapping
TIMEFRAME_TO_GT = {
    "1m": ("minute", 1),
    "15m": ("minute", 15),
    "1h": ("hour", 1),
    "1d": ("day", 1),
}

# Birdeye API key (for Solana token historical data)
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY")

# Birdeye timeframe mapping
TIMEFRAME_TO_BIRDEYE = {
    "1m": "1m",
    "15m": "15m",
    "1h": "1H",
    "1d": "1D",
}

# CoinGecko API key (for multi-chain historical data)
# Basic plan ($35/mo) gives 1 year hourly data
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")
COINGECKO_API_BASE = "https://pro-api.coingecko.com/api/v3"

# CoinGecko interval mapping (for paid API interval parameter)
TIMEFRAME_TO_COINGECKO = {
    "1h": "hourly",
    "1d": "daily",
}
