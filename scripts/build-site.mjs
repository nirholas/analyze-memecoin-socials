#!/usr/bin/env node
/**
 * Assemble the static site that gets published to Cloudflare Pages.
 *
 * Everything the published pages read is copied into `_site/`, including the
 * upstream project's static export, so no page on the deployed site fetches
 * data from somebody else's deployment. Nothing here is generated: the build
 * is a pure copy of committed artifacts, so the same input always produces the
 * same site. Run `npm run site` first if you want fresh candles.
 */
import { cp, mkdir, rm, writeFile, access, readdir, stat } from 'node:fs/promises';
import { join, dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const site = join(root, '_site');
const upstreamPublic = join(root, 'tweet-price-charts', 'web', 'public');

/** Paths copied verbatim from the repo into the site root. */
const REPO_COPIES = [
  ['chart', 'chart'],
  ['assets', 'assets'],
  ['out', 'out'],
  ['data/ohlcv', 'data/ohlcv'],
];

/** The upstream static export, self-hosted so `/tweetcharts` has no third-party origin. */
const UPSTREAM_COPIES = [
  ['static', 'static'],
  ['logos', 'logos'],
  ['avatars', 'avatars'],
  ['favicon.svg', 'favicon.svg'],
  ['favicon.ico', 'favicon.ico'],
  ['og-image.png', 'og-image.png'],
];

const REDIRECTS = `# Short, stable URLs for the pages under /chart.
/chart            /chart/chart.html             302
/case-study       /chart/case-study.html        302
/sentiment        /chart/social-sentiment.html  302
/tweetcharts      /chart/tweetcharts.html       302
`;

const HEADERS = `/*
  X-Content-Type-Options: nosniff
  Referrer-Policy: strict-origin-when-cross-origin
  X-Frame-Options: SAMEORIGIN

# Regenerated daily, so the root chart must not be held at the edge.
/
  Cache-Control: public, max-age=0, must-revalidate

/out/*
  Cache-Control: public, max-age=0, must-revalidate

/data/*
  Cache-Control: public, max-age=0, must-revalidate

# Immutable historical exports.
/static/*
  Cache-Control: public, max-age=3600
  Access-Control-Allow-Origin: *

/logos/*
  Cache-Control: public, max-age=86400

/avatars/*
  Cache-Control: public, max-age=86400

/assets/*
  Cache-Control: public, max-age=86400
`;

const NOT_FOUND = `<!doctype html>
<html lang="en"><head><meta charset="utf-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1"/>
<title>Not found &middot; posts vs price</title>
<style>
  :root{color-scheme:dark;--bg:#0a0b0e;--panel:#12141a;--line:#1e2230;--txt:#e7ecf3;--mut:#8b93a7;--cyan:#22d3ee}
  *{box-sizing:border-box}
  body{margin:0;min-height:100vh;display:grid;place-items:center;padding:24px;
       font:15px/1.55 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif;
       background:var(--bg);color:var(--txt)}
  main{max-width:34rem;width:100%;background:var(--panel);border:1px solid var(--line);
       border-radius:14px;padding:32px}
  h1{margin:0 0 6px;font-size:1.3rem;letter-spacing:-.01em}
  p{margin:0 0 20px;color:var(--mut)}
  ul{list-style:none;margin:0;padding:0;display:grid;gap:8px}
  a{display:flex;justify-content:space-between;gap:12px;padding:11px 13px;border-radius:9px;
    border:1px solid var(--line);color:var(--txt);text-decoration:none;
    transition:border-color .15s ease,background .15s ease,transform .15s ease}
  a:hover{border-color:var(--cyan);background:#161923;transform:translateY(-1px)}
  a:focus-visible{outline:2px solid var(--cyan);outline-offset:2px}
  a span{color:var(--mut);font-size:.85rem}
  @media (prefers-reduced-motion:reduce){a{transition:none}a:hover{transform:none}}
</style></head>
<body><main>
  <h1>That page is not here</h1>
  <p>Nothing is served at this path. Everything the site publishes is below.</p>
  <ul>
    <li><a href="/">Posts vs price <span>live chart</span></a></li>
    <li><a href="/chart">Chart any token <span>interactive</span></a></li>
    <li><a href="/case-study">Case study <span>the write-up</span></a></li>
    <li><a href="/sentiment">News sentiment <span>feed</span></a></li>
    <li><a href="/tweetcharts">Multi-asset charts <span>archive</span></a></li>
    <li><a href="https://github.com/nirholas/analyze-memecoin-socials">Source and data <span>GitHub</span></a></li>
  </ul>
</main></body></html>
`;

const exists = (p) => access(p).then(() => true, () => false);

async function copyInto(from, to, { optional = false } = {}) {
  if (!(await exists(from))) {
    if (optional) return false;
    throw new Error(`build-site: required path is missing: ${from}`);
  }
  await mkdir(dirname(to), { recursive: true });
  await cp(from, to, { recursive: true });
  return true;
}

async function measure(dir) {
  let files = 0;
  let bytes = 0;
  let largest = { path: '', bytes: 0 };
  for (const entry of await readdir(dir, { recursive: true, withFileTypes: true })) {
    if (!entry.isFile()) continue;
    const full = join(entry.parentPath ?? entry.path, entry.name);
    const { size } = await stat(full);
    files += 1;
    bytes += size;
    if (size > largest.bytes) largest = { path: full.slice(dir.length + 1), bytes: size };
  }
  return { files, bytes, largest };
}

await rm(site, { recursive: true, force: true });
await mkdir(site, { recursive: true });

// The repo root index.html is a committed copy of the generated chart, which can
// lag it by a run. Take the generated file so the deployed root is never stale.
await copyInto(join(root, 'out', 'three', 'chart.html'), join(site, 'index.html'));

for (const [from, to] of REPO_COPIES) {
  await copyInto(join(root, from), join(site, to));
}

const skippedUpstream = [];
for (const [from, to] of UPSTREAM_COPIES) {
  const copied = await copyInto(join(upstreamPublic, from), join(site, to), { optional: true });
  if (!copied) skippedUpstream.push(from);
}

await writeFile(join(site, '_redirects'), REDIRECTS);
await writeFile(join(site, '_headers'), HEADERS);
await writeFile(join(site, '404.html'), NOT_FOUND);

const { files, bytes, largest } = await measure(site);
const mib = (n) => `${(n / 1024 / 1024).toFixed(1)} MiB`;

// Cloudflare Pages refuses a deployment over these limits, so fail here instead.
const FILE_LIMIT = 20000;
const SIZE_LIMIT = 25 * 1024 * 1024;
if (files > FILE_LIMIT) throw new Error(`build-site: ${files} files exceeds the ${FILE_LIMIT}-file deployment limit`);
if (largest.bytes > SIZE_LIMIT) throw new Error(`build-site: ${largest.path} is ${mib(largest.bytes)}, over the ${mib(SIZE_LIMIT)} per-file limit`);

console.log(`_site: ${files} files, ${mib(bytes)} (largest ${largest.path}, ${mib(largest.bytes)})`);
if (skippedUpstream.length) {
  console.log(`no upstream export for: ${skippedUpstream.join(', ')} - /tweetcharts will have no data`);
}
