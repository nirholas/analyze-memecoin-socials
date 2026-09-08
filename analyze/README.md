# analyze/: the post-versus-price engine

`generate.mjs` takes a set of scraped X posts and a token's pool, measures what the price
did after each post, and writes both a statistics report and an interactive chart with
every post drawn on the candle it landed on.

It has no dependencies. Node 18 or newer, and nothing else.

```bash
node analyze/generate.mjs --asset three --chart
```

That writes four files into `out/three/`:

| File | What it is |
|---|---|
| `chart.json` | the statistics: per-window returns, baseline, significance, correlations |
| `chart.csv` | one row per post with its entry price and each forward return |
| `tweets.json` | the posts plus their returns, in the shape `chart/chart.html?tweets=` reads |
| `chart.html` | a self-contained chart: open the file, no server, no network |

## Assets

`assets.json` holds the tokens this repo studies. An entry names the pool, the handles
worth measuring, the avatars to draw, and where the post files live:

```json
{
  "three": {
    "symbol": "THREE",
    "network": "solana",
    "mint": "FeMbDoX7R1Psc4GEcvJdsbNbZA3bfztcyDCatJVJpump",
    "pool": "5ByL7MZoLABYnwMPZKPKjf4MGkZ7FeBzrAnos19Pre2z",
    "accounts": ["trythreews", "nichxbt"],
    "avatars": { "trythreews": "assets/trythreews.jpg" },
    "posts": ["data/tweets/three/*.json"]
  }
}
```

Nothing has to be registered first. Any token works from the command line, and the pool is
resolved from DexScreener when you only know the mint:

```bash
node analyze/generate.mjs \
  --mint <contract address> \
  --accounts somehandle \
  --chart \
  path/to/scraped-posts.json
```

## Flags

| Flag | Effect |
|---|---|
| `--asset <key>` | load a registry entry from `assets.json` |
| `--mint <CA>` | the token; the pool auto-resolves from DexScreener |
| `--pool <address>` | name the pool directly, skipping resolution |
| `--network <name>` | GeckoTerminal network slug, default `solana` |
| `--accounts a,b` | only measure these handles (timeline scrapes carry many more) |
| `--all-posts` | keep replies and retweets, which are dropped by default |
| `--windows 1,4,24` | forward windows in hours |
| `--prices <file>` | read hourly history from a local CSV instead of the API alone |
| `--prices-fine <file>` | same, for the fine-grained chart candles |
| `--out <base>` | output path prefix, default `out/<asset>/chart` |
| `--chart` | also write the self-contained `chart.html` |
| `--fetch-tweets` | pull fresh posts from a running XActions instance first |
| `--push-social` | POST the corpus to the three.ws Oracle social endpoint |
| `--no-cache` | ignore the on-disk response cache |

## How a return is measured

For a post at time *T*, the entry price is the hourly close at or just before *T*. Then
`+Nh = (close N hours after T - entry) / entry`. Medians are reported rather than means,
because a memecoin's mean is one or two outliers wearing a sample size.

## How significance is measured

This is the part that is easy to get wrong, and the earlier version of this analysis did.

**The baseline excludes hours the posts touched.** A baseline drawn from every candle puts
the treatment group inside its own control group. Here, an hour counts toward the baseline
for window *w* only if no post falls anywhere in `[hour, hour + w)`. `chart.json` reports
how many hours that dropped.

**Posts are not independent observations.** Two posts an hour apart share almost all of
their +24h path. The report counts `nEffective`: the number of clusters separated by at
least the window length. For $THREE, 405 posts collapse to 153 independent clusters at 1h
and to 37 at 24h.

**The p-value comes from a block bootstrap, not a standard error.** The null is built by
drawing `nEffective` blocks from the clean baseline, taking the same median, and repeating
2000 times; `p` is the share of draws that matched or beat the observed median. It is
one-sided and deterministic, so a rerun reproduces it.

**A p-value below five independent clusters is withheld.** It would be a number computed
from two observations, and it is reported as `null` with `pWithheld` saying why.

The version of this analysis that shipped in `tweet-price` used `baseline.std / sqrt(405)`
as its standard error. Because the baseline drifts as new candles arrive while the posts
stay frozen, that z-score climbed from 3.38 to 6.54 across three snapshots without a single
post changing. It was measuring how long the cron had been running.

## Price history when the API has none

GeckoTerminal drops history for pools that stop trading. The pump-sdk pool's candles now
begin in July 2026 while its posts are from February, so the API alone can measure nothing.
`--prices` reads one of the CSV exports committed under `data/` and merges it under
whatever the API still serves, so the analysis survives the pool going quiet. The loader
detects the column names, which differ between those exports.

## Rate limits

GeckoTerminal's free tier allows roughly 30 calls a minute and answers a sustained overage
with a 401 that reads like an auth failure. Every request goes through one serialized pacer
with a minimum gap, a throttle parks the whole queue rather than only the call that tripped
it, and responses are cached under `.cache/` for 15 minutes so iterating costs nothing.

Two traps worth knowing, both of which look like other bugs:

- A User-Agent containing a URL is rejected by the WAF with 429 and 401.
- On a throttled response GeckoTerminal omits its CORS header, so a browser reports
  rate limiting as a CORS error.

## Verifying the output

```bash
node scripts/verify-charts.mjs
```

Opens every generated chart plus the dynamic `chart/chart.html` in headless Chromium, fails
on a console error, a failed request, or a chart that drew nothing, and writes screenshots
to `.cache/shots/`. Upstream throttling is reported as its own outcome so it never hides a
real regression. It needs `playwright` available; the charts themselves do not.
