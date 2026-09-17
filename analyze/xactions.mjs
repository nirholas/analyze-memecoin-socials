// Reading posts out of a running XActions instance (https://github.com/nirholas/XActions).
//
// Any X account works: XActions scrapes the handle, this module normalizes whatever shape
// its surfaces return, and the analyzer measures those posts against any token's candles.
//
//   XACTIONS_URL=http://localhost:3001 XACTIONS_TOKEN=<x.com auth_token> \
//     node analyze/generate.mjs --mint <CA> --accounts <handle> --fetch-tweets --chart

const XACTIONS_URL = process.env.XACTIONS_URL || 'http://localhost:3001';
const XACTIONS_TOKEN = process.env.XACTIONS_TOKEN || process.env.XACTIONS_COOKIE || null;

// XActions wraps its tweet array three different ways depending on which surface answers:
// the Node route sends { success, data: [...] }, the edge function sends { user, tweets },
// and the documented shape nests { data: { tweets: [...] } }. Reading only one of them
// returns zero posts from a call that looked like it worked.
export function unwrapXActionsTweets(d) {
  const candidates = [d?.data?.results, d?.data?.tweets, d?.tweets, d?.results, d?.data, d];
  return candidates.find(Array.isArray) || null;
}

// A parsed XActions tweet carries createdAt/metrics/isReply and no url; the older console
// scrapes carry timestamp and flat counts. Both normalize to the loader's schema here.
export function normalizeXActionsTweet(t, account) {
  const handle = t.author?.username || t.username || account;
  const id = t.id || t.id_str || t.rest_id || null;
  return {
    id,
    text: t.text || t.full_text || '',
    timestamp: t.createdAt || t.created_at || t.timestamp || t.time || t.date || null,
    url: t.url || (id ? `https://x.com/${handle}/status/${id}` : null),
    metrics: {
      likes: t.metrics?.likes ?? t.likes ?? 0,
      retweets: t.metrics?.retweets ?? t.retweets ?? 0,
      replies: t.metrics?.replies ?? t.replies ?? 0,
      views: t.metrics?.views ?? t.views ?? 0,
    },
    type: {
      isRetweet: Boolean(t.isRetweet ?? t.retweetOf ?? t.type?.isRetweet),
      isReply: Boolean(t.isReply ?? t.inReplyTo ?? t.type?.isReply ?? (t.text || '').startsWith('@')),
    },
    profile: handle,
  };
}

export async function fetchTweetsFromXActions(account, { url = XACTIONS_URL, token = XACTIONS_TOKEN } = {}) {
  const body = {
    username: account,
    limit: 200,
    ...(token ? { authToken: token } : {}),
  };
  const r = await fetch(`${url}/api/ai/scrape/tweets`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { 'X-Session-Cookie': token } : {}),
    },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(60000),
  });
  if (!r.ok) {
    const detail = (await r.text()).slice(0, 200);
    const hint = r.status === 400 && !token
      ? ' (set XACTIONS_TOKEN to an x.com auth_token; the Node API requires one)'
      : '';
    throw new Error(`XActions ${r.status}${hint}: ${detail}`);
  }
  const d = await r.json();
  if (d?.success === false || d?.error) throw new Error(`XActions error: ${d.message || d.error}`);
  const list = unwrapXActionsTweets(d);
  if (!list) throw new Error(`XActions returned no tweet array (keys: ${Object.keys(d || {}).join(',') || 'none'})`);
  return list.map((t) => normalizeXActionsTweet(t, account));
}
