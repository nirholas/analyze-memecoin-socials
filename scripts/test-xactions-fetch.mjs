#!/usr/bin/env node
// Checks the XActions client against every response shape XActions actually serves.
//
// Usage: node scripts/test-xactions-fetch.mjs
//
// XActions answers /api/ai/scrape/tweets from three surfaces that wrap the tweet array
// differently, and a client that reads only one of them returns zero posts from a call
// that returned HTTP 200. Each case below is the shape that surface sends, with the field
// names its parser produces (src/scrapers/twitter/http/parse/tweet.js upstream), replayed
// from a local server so the check needs no credential and no live scrape.

import { createServer } from 'node:http';
import { fetchTweetsFromXActions } from '../analyze/xactions.mjs';

// One tweet as XActions' GraphQL parser emits it: createdAt, nested metrics, no url.
const PARSED = {
  id: '1932000000000000001',
  text: 'shipping all weekend',
  createdAt: '2026-06-14T18:20:00.000Z',
  author: { id: '1', username: 'nichxbt', name: 'nich' },
  metrics: { likes: 412, retweets: 33, replies: 21, quotes: 2, bookmarks: 9, views: 88000 },
  isReply: false,
  isRetweet: false,
};

// The same post in the older flat shape the docs advertise and the console scrapers write.
const FLAT = {
  id: '1932000000000000002',
  text: '@someone agreed',
  timestamp: '2026-06-14T19:05:00.000Z',
  likes: 12,
  retweets: 0,
  replies: 3,
  views: 900,
  url: 'https://x.com/nichxbt/status/1932000000000000002',
};

const CASES = [
  { name: 'node route    { success, data: [...] }', body: { success: true, data: [PARSED], count: 1 } },
  { name: 'edge function { user, tweets: [...] }', body: { user: { username: 'nichxbt' }, tweets: [PARSED], cursor: null } },
  { name: 'documented    { data: { tweets: [...] } }', body: { success: true, data: { username: 'nichxbt', scrapedCount: 1, tweets: [FLAT] } } },
];

let failures = 0;
const check = (label, cond, detail = '') => {
  if (cond) return;
  failures += 1;
  console.error(`  FAIL ${label}${detail ? `: ${detail}` : ''}`);
};

const server = createServer((req, res) => {
  let raw = '';
  req.on('data', (c) => { raw += c; });
  req.on('end', () => {
    const body = raw ? JSON.parse(raw) : {};
    res.setHeader('content-type', 'application/json');
    // The Node API rejects a request carrying no credential, which is the failure a user
    // hits first, so the client has to surface it as something better than "0 tweets".
    if (server.requireAuth && !body.authToken) {
      res.statusCode = 400;
      res.end(JSON.stringify({ error: 'username and authToken are required' }));
      return;
    }
    res.end(JSON.stringify(server.nextBody));
  });
});

await new Promise((r) => server.listen(0, '127.0.0.1', r));
const url = `http://127.0.0.1:${server.address().port}`;

for (const c of CASES) {
  server.nextBody = c.body;
  server.requireAuth = false;
  const posts = await fetchTweetsFromXActions('nichxbt', { url, token: 'test-token' });
  check(c.name, posts.length === 1, `got ${posts.length} posts`);
  const [p] = posts;
  if (!p) continue;
  check(c.name, Boolean(p.timestamp) && Number.isFinite(Date.parse(p.timestamp)), `timestamp ${p.timestamp}`);
  check(c.name, /^https:\/\/x\.com\/nichxbt\/status\/\d+$/.test(p.url || ''), `url ${p.url}`);
  check(c.name, p.metrics.likes > 0 && p.metrics.views > 0, `metrics ${JSON.stringify(p.metrics)}`);
  check(c.name, p.profile === 'nichxbt', `profile ${p.profile}`);
  console.log(`  ok   ${c.name} → ${p.id} ${p.timestamp} likes=${p.metrics.likes} reply=${p.type.isReply}`);
}

// The credential defaults to the environment, and a caller that passes only a url must
// still work. A self-referential default here threw "Cannot access 'token'" on every real
// run while every test that passed a token explicitly stayed green.
server.nextBody = { success: true, data: [PARSED] };
server.requireAuth = false;
const [defaulted] = await fetchTweetsFromXActions('nichxbt', { url });
check('env-default credential', defaulted?.id === PARSED.id, JSON.stringify(defaulted));
console.log('  ok   omitted credential falls back to the environment');

// A reply must be recognized from the parser's own flag, not only from a leading "@":
// the analyzer drops replies when measuring original posts.
server.nextBody = { success: true, data: [{ ...PARSED, isReply: true, text: 'no at-sign here' }] };
server.requireAuth = false;
const [reply] = await fetchTweetsFromXActions('nichxbt', { url, token: 't' });
check('reply detection', reply?.type.isReply === true, JSON.stringify(reply?.type));
console.log(`  ok   reply flag honored without a leading @`);

// No credential against the Node API: the error has to name the missing variable.
server.requireAuth = true;
let message = '';
try {
  await fetchTweetsFromXActions('nichxbt', { url, token: null });
} catch (err) {
  message = err.message;
}
check('missing-credential error', /XACTIONS_TOKEN/.test(message), message || 'no error thrown');
console.log(`  ok   missing credential explains itself: ${message.split(':')[0]}`);

// A 200 that carries no array at all must fail loudly rather than analyze zero posts.
server.requireAuth = false;
server.nextBody = { success: true, data: { note: 'nothing here' } };
let emptyErr = '';
try {
  await fetchTweetsFromXActions('nichxbt', { url, token: 't' });
} catch (err) {
  emptyErr = err.message;
}
check('shapeless response', /no tweet array/.test(emptyErr), emptyErr || 'no error thrown');
console.log('  ok   unrecognized shape raises instead of returning nothing');

server.close();
console.log(failures ? `\n${failures} check(s) failed` : '\nAll XActions response shapes parse.');
process.exit(failures ? 1 : 0);
