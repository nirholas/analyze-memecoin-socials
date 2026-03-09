# Admin Panel Setup

## Overview

The admin panel at `/admin` lets you add new assets via a web UI instead of running scripts manually.

## Setup Required (One-Time)

### 1. Vercel Environment Variables

Add these in Vercel Dashboard → Settings → Environment Variables:

| Variable | Value | Notes |
|----------|-------|-------|
| `ADMIN_PASSWORD` | Your secret password | Used to authenticate the admin form |
| `GITHUB_TOKEN` | GitHub Personal Access Token | Needs `repo` + `workflow` scopes |
| `GITHUB_REPO` | `rohunvora/tweet-price` | Format: `owner/repo` |

### 2. GitHub Repository Secrets

Add these in GitHub → Settings → Secrets and variables → Actions:

| Secret | Value | Notes |
|--------|-------|-------|
| `X_BEARER_TOKEN` | Your Twitter API Bearer Token | For fetching tweets |
| `BIRDEYE_API_KEY` | Your Birdeye API key | For Solana token historical prices |

### 3. Create a GitHub Personal Access Token

1. Go to GitHub → Settings → Developer settings → Personal access tokens → Tokens (classic)
2. Generate new token with scopes:
   - `repo` (full control of private repositories)
   - `workflow` (update GitHub Action workflows)
3. Copy the token and add it as `GITHUB_TOKEN` in Vercel

## How to Use

### Via Web UI (Recommended)

1. Go to `yoursite.com/admin`
2. Fill out the form:
   - **Asset ID**: lowercase, no spaces (e.g., `pump`)
   - **Display Name**: how it appears in UI (e.g., `Pump Fun`)
   - **Founder Twitter**: their handle without @ (e.g., `aikiinc`)
   - **Price Source**: either CoinGecko ID OR Network + Pool Address
   - **Brand Color**: hex color for the asset theme
3. Enter your admin password
4. Click "Add Asset"
5. Wait 2-3 minutes for the workflow to complete
6. Site auto-deploys via Vercel

### Via CLI (Alternative)

```bash
# CoinGecko-listed token
python scripts/add_asset.py mytoken \
  --name "My Token" \
  --founder username \
  --coingecko my-token-id

# Solana DEX token
python scripts/add_asset.py mytoken \
  --name "My Token" \
  --founder username \
  --network solana \
  --pool 0x123...

# Refresh existing asset
python scripts/add_asset.py mytoken --refresh

# Dry run (validate only)
python scripts/add_asset.py mytoken --name "Test" --founder user --coingecko test-id --dry-run
```

## How It Works

```
┌──────────────────┐      ┌──────────────────┐      ┌──────────────────┐
│  /admin page     │─────▶│  /api/add-asset  │─────▶│  GitHub Actions  │
│  (form)          │      │  (triggers GH)   │      │  (runs scripts)  │
└──────────────────┘      └──────────────────┘      └──────────────────┘
                                                            │
                                                            ▼
                                                    ┌──────────────────┐
                                                    │ 1. Validate      │
                                                    │ 2. Add to config │
                                                    │ 3. Fetch tweets  │
                                                    │ 4. Fetch prices  │
                                                    │ 5. Compute stats │
                                                    │ 6. Export JSON   │
                                                    │ 7. Cache avatar  │
                                                    │ 8. Commit & push │
                                                    └──────────────────┘
                                                            │
                                                            ▼
                                                    ┌──────────────────┐
                                                    │ Vercel auto-     │
                                                    │ deploys          │
                                                    └──────────────────┘
```

## After Adding an Asset

1. **Add a logo**: Upload to `web/public/logos/{asset_id}.png` (recommended: 128x128 PNG)
2. **Verify on site**: Check the asset appears and data looks correct
3. **Monitor**: The hourly GitHub Action will keep data updated

## Troubleshooting

### "GitHub API error: 401"
- Check that `GITHUB_TOKEN` is set correctly in Vercel
- Ensure the token has `repo` and `workflow` scopes

### "Twitter handle not found"
- Verify the handle exists and is spelled correctly
- Check that `X_BEARER_TOKEN` is set in GitHub Secrets

### "CoinGecko ID not found"
- Go to coingecko.com and find the token
- The ID is in the URL: `coingecko.com/en/coins/[id]`

### Workflow fails
- Click "View on GitHub" to see the full logs
- Common issues: rate limits, API keys missing, network timeouts
- Re-run with `--refresh` flag to retry

## Files Created

| File | Purpose |
|------|---------|
| `/admin` | Web UI page |
| `/api/admin/add-asset` | API to trigger workflow |
| `/api/admin/workflow-status` | API to poll workflow status |
| `.github/workflows/add-asset.yml` | GitHub Actions workflow |
| `scripts/add_asset.py` | CLI orchestrator script |
