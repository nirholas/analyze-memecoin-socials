#!/usr/bin/env node
// Build the static JSON feed the multi-asset front end reads.
//
// Usage:
//   node analyze/export-static.mjs [--out _site/static]
//
// chart/tweetcharts.html was written against an upstream deployment that has since gone
// (its host answers 404 for every path), so the page rendered "Failed to load assets" for
// every visitor. The data it wants is data this repo already computes, one asset at a
// time, in out/<asset>/. This turns those files into the three documents it fetches:
//
//   assets.json                  the asset picker
//   <asset>/tweet_events.json    one entry per post, with its 1h and 24h return
//   <asset>/stats.json           the summary cards
//
// Everything is derived from out/<asset>/chart.{json,csv}; nothing is hand-maintained, so
// an asset added to assets.json and generated shows up here on the next run.

import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPO = resolve(__dirname, '..');
const args = process.argv.slice(2);
const flag = (name, fallback) => {
  const i = args.indexOf(`--${name}`);
  return i !== -1 && args[i + 1] && !args[i + 1].startsWith('--') ? args[i + 1] : fallback;
};
const OUT = resolve(REPO, flag('out', '_site/static'));

const REGISTRY = JSON.parse(readFileSync(join(__dirname, 'assets.json'), 'utf8'));

// The CSV is the per-post record; quoted post text can contain commas and doubled quotes.
function parseCsv(text) {
  const rows = [];
  let row = [], field = '', quoted = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (quoted) {
      if (c === '"') {
        if (text[i + 1] === '"') { field += '"'; i++; } else quoted = false;
      } else field += c;
    } else if (c === '"') quoted = true;
    else if (c === ',') { row.push(field); field = ''; }
    else if (c === '\n') { row.push(field); rows.push(row); row = []; field = ''; }
    else if (c !== '\r') field += c;
  }
  if (field || row.length) { row.push(field); rows.push(row); }
  const [head, ...body] = rows.filter((r) => r.length > 1);
  return body.map((r) => Object.fromEntries(head.map((h, i) => [h.trim(), r[i]])));
}

const num = (v) => (v === '' || v == null || Number.isNaN(Number(v)) ? null : Number(v));

function exportAsset(key) {
  const base = join(REPO, 'out', key);
  const chartPath = join(base, 'chart.json');
  const csvPath = join(base, 'chart.csv');
  if (!existsSync(chartPath) || !existsSync(csvPath)) return null;

  const chart = JSON.parse(readFileSync(chartPath, 'utf8'));
  const rows = parseCsv(readFileSync(csvPath, 'utf8'));
  const byUrl = new Map(
    (JSON.parse(readFileSync(join(base, 'tweets.json'), 'utf8')).tweets || [])
      .map((t) => [t.ts, t]),
  );

  const events = rows.map((r) => {
    const t = byUrl.get(r.timestamp);
    return {
      timestamp: Math.floor(Date.parse(r.timestamp) / 1000),
      date: r.timestamp,
      account: r.account,
      type: r.type,
      // chart.csv truncates post text for readability; tweets.json carries it whole.
      text: t?.text || r.text,
      url: t?.url || null,
      likes: num(r.likes) ?? 0,
      retweets: num(r.retweets) ?? 0,
      views: num(r.views) ?? 0,
      entry_price: num(r.entry_price),
      price_change_1h: num(r.r1h_pct),
      price_change_4h: num(r.r4h_pct),
      price_change_24h: num(r.r24h_pct),
    };
  }).filter((e) => Number.isFinite(e.timestamp));

  const spanDays = events.length
    ? (events.at(-1).timestamp - events[0].timestamp) / 86400
    : 0;
  const ranked = events.filter((e) => e.price_change_24h != null)
    .sort((a, b) => b.price_change_24h - a.price_change_24h);
  const day = (e) => e.date.slice(0, 10);

  const stats = {
    asset: key,
    generatedAt: chart.generatedAt,
    token: chart.token,
    posts: events.length,
    windows: chart.postWindowReturns,
    baseline: chart.baseline,
    tweet_frequency: { avg_per_week: spanDays > 0 ? (events.length / spanDays) * 7 : events.length },
    price_correlation: { coefficient: chart.dailyCorrelation?.postsPerDay_vs_dailyReturn ?? 0 },
    ...(ranked.length
      ? {
        biggest_pump: { change_pct: ranked[0].price_change_24h, date: day(ranked[0]) },
        biggest_dump: { change_pct: ranked.at(-1).price_change_24h, date: day(ranked.at(-1)) },
      }
      : {}),
  };

  mkdirSync(join(OUT, key), { recursive: true });
  writeFileSync(join(OUT, key, 'tweet_events.json'), JSON.stringify({ events }, null, 2));
  writeFileSync(join(OUT, key, 'stats.json'), JSON.stringify(stats, null, 2));
  return { key, events: events.length, chart };
}

mkdirSync(OUT, { recursive: true });
const exported = Object.keys(REGISTRY).map(exportAsset).filter(Boolean);
if (!exported.length) {
  console.error('Nothing to export: run `npm run generate:all` first so out/<asset>/ exists.');
  process.exit(1);
}

const assets = exported.map(({ key, chart }) => {
  const entry = REGISTRY[key];
  const [founder] = entry.accounts || [];
  const avatar = entry.avatars?.[founder];
  return {
    id: key,
    name: entry.symbol ? `$${entry.symbol}` : key,
    founder: founder || null,
    logo: avatar ? `/${avatar}` : null,
    mint: entry.mint,
    network: entry.network || 'solana',
    chart: `/out/${key}/chart.html`,
    generatedAt: chart.generatedAt,
    enabled: true,
  };
});
// The upstream export ships its own assets.json into the same directory. Merge rather
// than overwrite: its tokens keep working, ours are added, and a shared id takes the
// generated entry because that one is rebuilt from current data every run.
const assetsFile = join(OUT, 'assets.json');
let merged = assets;
if (existsSync(assetsFile)) {
  const existing = JSON.parse(readFileSync(assetsFile, 'utf8')).assets || [];
  const ours = new Set(assets.map((a) => a.id));
  merged = [...assets, ...existing.filter((a) => !ours.has(a.id))];
}
writeFileSync(assetsFile, JSON.stringify({ assets: merged, generated_at: new Date().toISOString() }, null, 2));

for (const { key, events } of exported) console.log(`  ${key}: ${events} events`);
console.log(`Wrote ${exported.length} asset(s) to ${OUT.replace(`${REPO}/`, '')}/`);
