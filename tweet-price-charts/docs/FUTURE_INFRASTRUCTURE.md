# Future Infrastructure Plans

## Current Approach (Local)

For occasional backfills, we use:
```bash
caffeinate -i nohup ./venv/bin/python -u scripts/nitter_scraper.py ... > /tmp/scrape.log 2>&1 &
```

- `caffeinate -i` - Prevents Mac from sleeping
- `nohup` - Survives terminal close
- Progress saved to `data/nitter_progress.json` for resumability

**Limitations:** Requires laptop to stay on and awake.

---

## Future Options (When Needed)

### Option 1: DigitalOcean Droplet (Recommended for Reliability)

**Cost:** ~$5/month or ~$0.01/hour (destroy when done)

**Setup:**
```bash
# Create droplet (Ubuntu, $5/month tier)
# SSH in, then:
git clone https://github.com/rohunvora/tweet-price.git
cd tweet-price
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
playwright install chromium

# Run in tmux (survives SSH disconnect)
tmux new -s scraper
./venv/bin/python scripts/nitter_scraper.py --asset jup --full --no-headless
# Ctrl+B, D to detach
# tmux attach -t scraper to reattach
```

**Pros:** True fire-and-forget, 24/7 availability
**Cons:** Requires account setup, manual provisioning

---

### Option 2: GitHub Actions (For Scheduled Tasks)

Good for automated periodic refreshes (e.g., daily tweet polling).

```yaml
# .github/workflows/scrape-tweets.yml
name: Scrape Tweets
on:
  schedule:
    - cron: '0 6 * * *'  # Daily at 6 AM UTC
  workflow_dispatch:  # Manual trigger

jobs:
  scrape:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: |
          pip install -r requirements.txt
          playwright install chromium
          python scripts/nitter_scraper.py --asset jup --since $(date -d '7 days ago' +%Y-%m-%d)
      - uses: actions/upload-artifact@v4
        with:
          name: tweet-data
          path: data/
```

**Pros:** Free, automated, no server management
**Cons:** 6-hour job timeout, need to handle data persistence

---

### Option 3: Docker + Cloud Run (Production-Grade)

For truly scalable, production scraping infrastructure.

```dockerfile
FROM python:3.11-slim
RUN apt-get update && apt-get install -y chromium
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt && playwright install chromium
COPY . .
CMD ["python", "scripts/nitter_scraper.py", "--asset", "jup", "--full"]
```

Deploy to Google Cloud Run, AWS Fargate, or similar.

**Pros:** Scalable, containerized, production-ready
**Cons:** More setup, ongoing costs

---

## When to Upgrade

- **Stick with local** if backfills are rare (1-2x/month)
- **Use DigitalOcean** for multi-hour jobs or when laptop can't stay on
- **Use GitHub Actions** for automated daily/weekly refreshes
- **Use Docker** if this becomes a regular production workload

---

## Related Files

- `scripts/nitter_scraper.py` - Main scraper with progress tracking
- `data/nitter_progress.json` - Resumable progress state

