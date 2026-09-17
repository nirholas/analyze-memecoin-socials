#!/usr/bin/env node
// Open every published route of the deployed site in a real browser and fail on a console
// error, a failed request, or a page that rendered none of its content. `verify-charts.mjs`
// proves the generated artifacts are sound; this proves the deployment serving them is.
//
// Usage: node scripts/verify-site.mjs [--site <origin>] [--headed] [--shots <dir>]

import { chromium } from 'playwright';
import { mkdir } from 'node:fs/promises';
import { join, resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const REPO = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const args = process.argv.slice(2);
const flag = (name, fallback) => (args.includes(name) ? args[args.indexOf(name) + 1] : fallback);
const site = flag('--site', process.env.SITE_ORIGIN || 'https://analyze-memecoin-socials.pages.dev').replace(/\/$/, '');
const shotDir = flag('--shots', join(REPO, '.cache', 'site-shots'));

// Every route the deployment publishes, with the evidence that it actually rendered.
const ROUTES = [
  {
    name: 'root-chart', path: '/',
    see: () => ({ canvases: document.querySelectorAll('canvas').length, bubbles: document.querySelectorAll('.bub').length }),
    want: (s) => s.canvases > 0 && s.bubbles > 0,
  },
  {
    name: 'chart-any-token', path: '/chart',
    see: () => ({ controls: document.querySelectorAll('input,select,button').length }),
    want: (s) => s.controls > 0,
  },
  {
    name: 'case-study', path: '/case-study',
    see: () => ({ canvases: document.querySelectorAll('canvas').length, sections: document.querySelectorAll('section').length }),
    want: (s) => s.canvases > 0,
  },
  {
    name: 'sentiment', path: '/sentiment',
    see: () => ({ controls: document.querySelectorAll('button,select').length }),
    want: (s) => s.controls > 0,
  },
  {
    // The one route that used to read another deployment's data. Cards on the home grid
    // are proof that /static is being served from this origin.
    name: 'tweetcharts', path: '/tweetcharts',
    see: () => ({ cards: document.querySelectorAll('#homeGrid > *').length, failed: !!document.querySelector('#homeGrid .state') }),
    want: (s) => s.cards > 0 && !s.failed,
  },
  {
    name: 'not-found', path: '/this-path-does-not-exist',
    see: () => ({ links: document.querySelectorAll('main a').length, status: document.title }),
    want: (s) => s.links > 0,
  },
];

// Public APIs the pages read directly. Their throttling is an upstream wait, not a defect
// in this deployment, so it is reported separately and never hides a real regression.
const UPSTREAM = /geckoterminal|coingecko|allorigins|unpkg|googleapis|gstatic|jsdelivr/i;
const THROTTLE = /(CORS|ERR_FAILED|ERR_TIMED_OUT|429|5\d\d|Failed to fetch|rate limit)/i;

async function check(browser, route) {
  const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
  const errors = [];
  const failed = [];
  page.on('console', (m) => { if (m.type() === 'error') errors.push(m.text()); });
  page.on('pageerror', (e) => errors.push(`pageerror: ${e.message}`));
  page.on('requestfailed', (r) => failed.push(`${r.url()} ${r.failure()?.errorText}`));
  page.on('response', (r) => { if (r.status() >= 400 && new URL(r.url()).origin === site) failed.push(`${r.url()} -> ${r.status()}`); });

  let seen = {};
  let fatal = null;
  try {
    const res = await page.goto(site + route.path, { waitUntil: 'networkidle', timeout: 60000 });
    if (route.name === 'not-found' && res?.status() !== 404) fatal = `expected 404, got ${res?.status()}`;
    await page.waitForTimeout(2500);
    seen = await page.evaluate(route.see);
    await mkdir(shotDir, { recursive: true });
    await page.screenshot({ path: join(shotDir, `${route.name}.png`) });
  } catch (e) {
    fatal = e.message;
  } finally {
    await page.close();
  }

  // The 404 route is expected to 404. Chromium reports that as both a failed response and
  // a console error, so the assertion for this route has to discount both of them.
  const expected404 = route.name === 'not-found'
    ? (m) => / -> 404$/.test(m) || /status of 404/.test(m)
    : () => false;
  const all = [
    ...errors.filter((m) => !expected404(m)),
    ...failed.filter((f) => !expected404(f)).map((f) => `request failed: ${f}`),
  ];
  const isUpstream = (m) => UPSTREAM.test(m) && THROTTLE.test(m);
  const anyUpstream = all.some(isUpstream);
  // The browser logs a bare, URL-less companion line for the same request; it only counts
  // as upstream noise when a named upstream failure is present to explain it.
  const soft = (m) => isUpstream(m) || (anyUpstream && /^Failed to load resource: net::ERR_FAILED$/.test(m));
  return { ...route, seen, fatal, upstream: all.filter(soft), problems: all.filter((m) => !soft(m)) };
}

const browser = await chromium.launch({ headless: !args.includes('--headed') });
const results = [];
try {
  for (const route of ROUTES) results.push(await check(browser, route));
} finally {
  await browser.close();
}

let bad = 0;
let soft = 0;
console.log(`site: ${site}\n`);
for (const r of results) {
  const ok = !r.fatal && r.want(r.seen) && r.problems.length === 0;
  if (!ok) bad++;
  else if (r.upstream.length) soft++;
  console.log(`${(ok ? (r.upstream.length ? 'PASS*' : 'PASS') : 'FAIL').padEnd(5)} ${r.path.padEnd(28)} ${JSON.stringify(r.seen)}`);
  if (r.fatal) console.log(`        ${r.fatal}`);
  for (const p of r.problems) console.log(`        ${p}`);
  if (ok && r.upstream.length) console.log(`        * ${r.upstream.length} upstream API response(s) throttled or unavailable`);
}
console.log(`\n${results.length - bad}/${results.length} routes verified`
  + (soft ? `, ${soft} with a throttled upstream API` : '')
  + `; screenshots in ${shotDir}`);
process.exit(bad ? 1 : 0);
