#!/usr/bin/env node
// Open every generated chart in a real browser and fail on a console error, a failed
// request, or a chart that drew no candles. A chart that only "looks generated" is the
// failure this catches: the JSON can be perfect while the page throws on load.
//
// Usage: node scripts/verify-charts.mjs [--headed] [--shots <dir>]

import { chromium } from 'playwright';
import { createServer } from 'node:http';
import { readFile, mkdir, readdir } from 'node:fs/promises';
import { existsSync } from 'node:fs';
import { join, extname, resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const REPO = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const args = process.argv.slice(2);
const shotDir = args.includes('--shots') ? args[args.indexOf('--shots') + 1] : join(REPO, '.cache', 'shots');

const MIME = { '.html': 'text/html', '.json': 'application/json', '.csv': 'text/csv', '.jpg': 'image/jpeg', '.png': 'image/png' };

// The dynamic chart fetches its feed over http, which file:// cannot serve.
function serve(root) {
  return new Promise((ok) => {
    const server = createServer(async (req, res) => {
      const path = join(root, decodeURIComponent(req.url.split('?')[0]));
      if (!path.startsWith(root) || !existsSync(path)) { res.writeHead(404).end('not found'); return; }
      res.writeHead(200, { 'content-type': MIME[extname(path)] || 'application/octet-stream' });
      res.end(await readFile(path));
    });
    server.listen(0, '127.0.0.1', () => ok({ server, port: server.address().port }));
  });
}

async function check(page, url, name, expect) {
  const errors = [], failed = [];
  page.on('console', (m) => { if (m.type() === 'error') errors.push(m.text()); });
  page.on('pageerror', (e) => errors.push(`pageerror: ${e.message}`));
  page.on('requestfailed', (r) => failed.push(`${r.url()} ${r.failure()?.errorText}`));

  await page.goto(url, { waitUntil: 'networkidle', timeout: 60000 });
  await page.waitForTimeout(2500);

  const seen = await page.evaluate(expect);
  await mkdir(shotDir, { recursive: true });
  await page.screenshot({ path: join(shotDir, `${name}.png`), fullPage: false });

  // GeckoTerminal drops its CORS header on a 429, so throttling reaches the page as a
  // CORS error. That is an upstream wait, not a defect in this repo, and it is reported
  // as its own outcome so a real regression never hides behind it.
  const all = [...errors, ...failed.map((f) => `request failed: ${f}`)];
  const named = (m) => /geckoterminal/i.test(m) && /(CORS|ERR_FAILED|429|Failed to fetch|rate limit)/i.test(m);
  // The browser also logs a bare "Failed to load resource" with no URL for the same
  // request. It only counts as throttling when a named one is present to explain it.
  const anyNamed = all.some(named);
  const isThrottle = (m) => named(m) || (anyNamed && /^Failed to load resource: net::ERR_FAILED$/.test(m));
  const throttled = all.filter(isThrottle);
  const problems = all.filter((m) => !isThrottle(m));
  return { name, seen, problems, throttled };
}

const { server, port } = await serve(REPO);
const browser = await chromium.launch({ headless: !args.includes('--headed') });
const results = [];

try {
  const assets = existsSync(join(REPO, 'out')) ? await readdir(join(REPO, 'out')) : [];

  for (const asset of assets) {
    if (!existsSync(join(REPO, 'out', asset, 'chart.html'))) continue;
    const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
    results.push(await check(
      page,
      `http://127.0.0.1:${port}/out/${asset}/chart.html`,
      `static-${asset}`,
      // The self-contained chart bakes its data in, so a drawn canvas plus bubbles is proof.
      () => ({ bubbles: document.querySelectorAll('.bub').length, canvases: document.querySelectorAll('canvas').length }),
    ));
    await page.close();
  }

  // The dynamic chart pulls the same posts over the network from the generated feed.
  for (const asset of assets) {
    if (!existsSync(join(REPO, 'out', asset, 'tweets.json'))) continue;
    const feed = JSON.parse(await readFile(join(REPO, 'out', asset, 'tweets.json'), 'utf8'));
    const q = new URLSearchParams({
      token: feed.token.mint, pool: feed.token.pool, network: feed.token.network,
      tweets: `/out/${asset}/tweets.json`,
    });
    const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
    results.push(await check(
      page,
      `http://127.0.0.1:${port}/chart/chart.html?${q}`,
      `dynamic-${asset}`,
      () => ({ tweetCount: Number(document.getElementById('tweet-count')?.textContent || 0), canvases: document.querySelectorAll('canvas').length }),
    ));
    await page.close();
  }
} finally {
  await browser.close();
  server.close();
}

let bad = 0, throttled = 0;
for (const r of results) {
  const drew = (r.seen.canvases || 0) > 0;
  const loaded = (r.seen.bubbles ?? r.seen.tweetCount ?? 0) > 0;
  const ok = drew && loaded && r.problems.length === 0;
  if (!ok) bad++;
  else if (r.throttled.length) throttled++;
  const label = !ok ? 'FAIL' : r.throttled.length ? 'PASS*' : 'PASS';
  console.log(`${label.padEnd(5)} ${r.name.padEnd(20)} ${JSON.stringify(r.seen)}`);
  for (const p of r.problems) console.log(`        ${p}`);
  if (ok && r.throttled.length) console.log(`        * ${r.throttled.length} GeckoTerminal throttle response(s); the page showed its retry banner`);
}
console.log(`\n${results.length - bad}/${results.length} charts verified`
  + (throttled ? `, ${throttled} while GeckoTerminal was throttling` : '')
  + `; screenshots in ${shotDir}`);
process.exit(bad ? 1 : 0);
