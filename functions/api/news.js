/**
 * GET /api/news - the crypto news feed behind /sentiment.
 *
 * A Cloudflare Pages Function that pulls every source feed server-side and returns one
 * normalized JSON array. The page used to read the feeds through a public CORS proxy,
 * which answered 408 often enough to leave the page empty; fetching from the edge removes
 * the third-party hop, collapses twenty browser requests into one, and lets the result be
 * cached for every later visitor.
 *
 * Response: { "articles": [{ source, title, description, pubDate, link, category }], ... }
 */

// Sources are RSS with <item> entries. A feed publishing Atom <entry> is not listed,
// because the reader below expects <item>. Keep this in sync with chart/social-sentiment.html.
const SOURCES = [
  { source: 'CoinDesk', url: 'https://www.coindesk.com/arc/outboundfeeds/rss/', category: 'General' },
  { source: 'Cointelegraph', url: 'https://cointelegraph.com/rss', category: 'General' },
  { source: 'Decrypt', url: 'https://decrypt.co/feed', category: 'General' },
  { source: 'The Block', url: 'https://www.theblock.co/rss.xml', category: 'Markets' },
  { source: 'The Defiant', url: 'https://thedefiant.io/feed', category: 'DeFi' },
  { source: 'Bitcoin Magazine', url: 'https://bitcoinmagazine.com/feed', category: 'Bitcoin' },
  { source: 'Blockworks', url: 'https://blockworks.co/feed', category: 'Markets' },
];

const PER_SOURCE = 25;
const MAX_ARTICLES = 120;
const EDGE_TTL = 300;
const UPSTREAM_TIMEOUT_MS = 8000;

const ENTITIES = { amp: '&', lt: '<', gt: '>', quot: '"', apos: "'", '#39': "'", nbsp: ' ' };

function decodeEntities(text) {
  return text.replace(/&(#x?[0-9a-fA-F]+|[a-zA-Z]+);/g, (whole, name) => {
    if (name[0] === '#') {
      const code = name[1] === 'x' || name[1] === 'X'
        ? parseInt(name.slice(2), 16)
        : parseInt(name.slice(1), 10);
      return Number.isFinite(code) ? String.fromCodePoint(code) : whole;
    }
    return ENTITIES[name.toLowerCase()] ?? whole;
  });
}

function textOf(itemXml, tag) {
  const match = itemXml.match(new RegExp(`<${tag}(?:\\s[^>]*)?>([\\s\\S]*?)</${tag}>`, 'i'));
  if (!match) return '';
  const raw = match[1].replace(/<!\[CDATA\[([\s\S]*?)\]\]>/g, '$1');
  return decodeEntities(raw.replace(/<[^>]*>/g, '')).replace(/\s+/g, ' ').trim();
}

function parseFeed(xml, { source, category }) {
  // RSS wraps a post in <item>, Atom in <entry>; Blockworks publishes Atom, and matching
  // only <item> read its feed as empty rather than as broken.
  const items = xml.match(/<(item|entry)(?:\s[^>]*)?>[\s\S]*?<\/\1>/gi) || [];
  return items.slice(0, PER_SOURCE).map((item) => {
    // Atom-style <link href="..."/> appears inside some RSS feeds alongside <link>text</link>.
    const href = item.match(/<link[^>]*\shref=["']([^"']+)["']/i);
    return {
      source,
      title: textOf(item, 'title'),
      description: textOf(item, 'description') || textOf(item, 'content:encoded') || textOf(item, 'summary'),
      pubDate: textOf(item, 'pubDate') || textOf(item, 'dc:date') || textOf(item, 'published') || textOf(item, 'updated'),
      link: textOf(item, 'link') || (href ? decodeEntities(href[1]) : ''),
      category: textOf(item, 'category') || category,
    };
  }).filter((article) => article.title && article.link);
}

async function readSource(config) {
  const abort = new AbortController();
  const timer = setTimeout(() => abort.abort(), UPSTREAM_TIMEOUT_MS);
  try {
    const response = await fetch(config.url, {
      signal: abort.signal,
      headers: { 'user-agent': 'analyze-memecoin-socials/1.0 (+https://github.com/nirholas/analyze-memecoin-socials)', accept: 'application/rss+xml, application/xml;q=0.9, */*;q=0.8' },
      cf: { cacheTtl: EDGE_TTL, cacheEverything: true },
    });
    if (!response.ok) return { source: config.source, articles: [], error: `HTTP ${response.status}` };
    return { source: config.source, articles: parseFeed(await response.text(), config) };
  } catch (error) {
    // One unreachable publisher must not empty the whole feed.
    return { source: config.source, articles: [], error: error.name === 'AbortError' ? 'timeout' : String(error.message || error) };
  } finally {
    clearTimeout(timer);
  }
}

// The Defiant stamps every item with the moment its feed was built, so sorting the merged
// list purely by time hands it the entire page and buries the publishers that date their
// posts honestly. Taking one article at a time from each source, newest head first, keeps
// the order roughly chronological without letting one feed crowd the others out.
function interleaveByRecency(queues, limit) {
  const at = (article) => (article ? new Date(article.pubDate).getTime() || 0 : 0);
  const pending = queues.map((list) => [...list].sort((a, b) => at(b) - at(a)));
  const out = [];
  while (out.length < limit) {
    const live = pending.filter((q) => q.length);
    if (!live.length) break;
    live.sort((a, b) => at(b[0]) - at(a[0]));
    for (const q of live) {
      if (out.length >= limit) break;
      out.push(q.shift());
    }
  }
  return out;
}

export async function onRequestGet({ request }) {
  const cache = caches.default;
  const key = new Request(new URL(request.url).origin + '/api/news', { method: 'GET' });
  const cached = await cache.match(key);
  if (cached) return cached;

  const results = await Promise.all(SOURCES.map(readSource));

  const seen = new Set();
  const queues = results.map((r) => r.articles.filter((article) => {
    const dedupeKey = `${article.source}|${article.title}`.toLowerCase();
    if (seen.has(dedupeKey)) return false;
    seen.add(dedupeKey);
    return true;
  })).filter((list) => list.length);

  const articles = interleaveByRecency(queues, MAX_ARTICLES);

  const body = {
    generatedAt: new Date().toISOString(),
    sources: results.map(({ source, articles: list, error }) => ({ source, count: list.length, ...(error ? { error } : {}) })),
    articles: articles.slice(0, MAX_ARTICLES),
  };

  // An empty feed means every publisher failed at once. Answering 503 keeps that out of the
  // edge cache, so the next request retries instead of serving nothing for five minutes.
  const response = new Response(JSON.stringify(body), {
    status: articles.length ? 200 : 503,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'access-control-allow-origin': '*',
      'cache-control': articles.length ? `public, max-age=60, s-maxage=${EDGE_TTL}` : 'no-store',
    },
  });
  if (articles.length) await cache.put(key, response.clone());
  return response;
}
