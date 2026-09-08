# analyze-memecoin-socials

Plot X (Twitter) posts on a crypto price chart and measure whether the posts actually moved
the price.

Scrape a handle's posts from the browser, line them up against the token's candles, and get
back an interactive chart with every post drawn on the candle it landed on, plus a report
saying whether the price action after those posts differs from the price action after any
other hour.

## Quick start

```bash
node analyze/generate.mjs --asset three --chart
```

Then open `out/three/chart.html`. No install, no dependencies, no API key. Node 18 or newer.

Any token works, not just the two studied here:

```bash
node analyze/generate.mjs --mint <contract address> --accounts <handle> --chart posts.json
```

## What's here

| Path | What it is |
|---|---|
| [`analyze/`](analyze/) | the engine: post loading, return measurement, significance, chart generation. [Its README](analyze/README.md) is the reference. |
| [`chart/chart.html`](chart/chart.html) | the live chart for any token: `?token=&pool=&handle=&avatar=&tweets=&network=`, candles fetched in the browser |
| [`chart/case-study.html`](chart/case-study.html) | the written case study, with eight Chart.js figures and the interactive chart embedded |
| [`chart/tweetcharts.html`](chart/tweetcharts.html) | a multi-asset front end over the upstream Python pipeline's static export |
| [`chart/social-sentiment.html`](chart/social-sentiment.html) | an aggregated crypto news feed with per-headline sentiment |
| [`scripts/`](scripts/) | the browser-console scrapers (cashtag search, profile with replies) and the chart verifier |
| [`data/`](data/) | scraped posts under `data/tweets/`, and OHLCV exports for the case-study token |
| [`out/`](out/) | generated per asset: `chart.json`, `chart.csv`, `tweets.json`, `chart.html` |
| [`tweet-price-charts/`](tweet-price-charts/) | the upstream Python project ([rohunvora/tweet-price-charts](https://github.com/rohunvora/tweet-price-charts)) |

## Getting the posts

There is no X API here and none is needed. `scripts/x_cashtag_scraper.js` and
`scripts/scrape-profile-with-replies.js` are pasted into the browser console on x.com and
scrape a cashtag search or a profile, replies included, with a floating control panel for
pausing and exporting. See their READMEs in [`scripts/`](scripts/).

The engine also reads posts from a running [XActions](https://github.com/nirholas/XActions)
instance with `--fetch-tweets`.

## What the analysis says

Both tables come from `out/<asset>/chart.json`, regenerated from live data.

**$THREE**, 405 original posts from @trythreews and @nichxbt, April 29 to June 15 2026,
against hourly candles through September 8:

| Window | Post median | Clean baseline | Edge | Win rate | Independent clusters | p |
|---|---|---|---|---|---|---|
| +1h | -1.25% | -0.30% | -0.95pp | 46% vs 46% | 153 | 1.00 |
| +4h | -1.30% | -1.08% | -0.22pp | 45% vs 43% | 102 | 0.64 |
| +24h | **+9.07%** | -3.34% | **+12.41pp** | **61% vs 40%** | 37 | **0.0005** |

No instant pump. Nothing at four hours. A real effect a day later, which survives a baseline
that excludes the posts' own hours and a bootstrap run on 37 independent clusters rather
than on 405 correlated posts.

**pump-sdk**, 66 posts in a single night in February 2026, the case-study launch:

| Window | Post median | Clean baseline | Edge | Win rate | Independent clusters | p |
|---|---|---|---|---|---|---|
| +1h | +43.94% | -0.71% | +44.65pp | 59% vs 38% | 7 | 0.0005 |
| +4h | +137.07% | -4.30% | +141.37pp | 88% vs 26% | 2 | withheld |
| +24h | -63.58% | -26.23% | -37.36pp | 0% vs 1% | 1 | withheld |

The opposite shape: a violent move inside the hour and a round trip past the starting point
within a day. The two windows that look most dramatic are withheld rather than reported,
because 66 posts in one night are two independent observations at 4h and one at 24h. A
naive sample size would have called those the strongest results in the repo.

How the baseline, the cluster count and the p-value are computed, and why the previous
version of this analysis overstated its significance, is in
[analyze/README.md](analyze/README.md).

## Reading a chart

- Candles with every post as an avatar bubble on the candle it landed on
- Bubbles cluster with a count badge and split apart as you zoom
- Hover for the post text, its views, and its actual +1h / +4h / +24h return
- Click to open the post, or to expand a cluster
- 15m / 1h / 1d timeframes, log or linear price scale
- A sidebar ranking every post by its 24h move

Verify a generated chart end to end in a real browser:

```bash
node scripts/verify-charts.mjs
```

## Credit

The idea and the original implementation are
[rohunvora/tweet-price-charts](https://github.com/rohunvora/tweet-price-charts), vendored
here under `tweet-price-charts/`.
