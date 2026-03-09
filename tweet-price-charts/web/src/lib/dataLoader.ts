import { PriceData, Timeframe, Candle, TweetEvent, TweetEventsData, Asset, Stats } from './types';

// =============================================================================
// Debug Logging
// =============================================================================

const DEBUG = false;

function log(msg: string, data?: unknown) {
  if (DEBUG) {
    if (data !== undefined) {
      console.log(`[dataLoader] ${msg}`, data);
    } else {
      console.log(`[dataLoader] ${msg}`);
    }
  }
}

// =============================================================================
// Cache
// =============================================================================

const priceCache = new Map<string, PriceData>();
const tweetCache = new Map<string, TweetEventsData>();
let assetsCache: Asset[] | null = null;

// =============================================================================
// Asset Loading
// =============================================================================

/**
 * Load all enabled assets from assets.json
 */
export async function loadAssets(): Promise<Asset[]> {
  if (assetsCache) {
    log('Using cached assets');
    return assetsCache;
  }

  log('Loading assets.json');

  const response = await fetch('/static/assets.json');

  if (!response.ok) {
    throw new Error(`Failed to load assets.json: ${response.status} ${response.statusText}`);
  }

  const data = await response.json();
  const enabledAssets = data.assets.filter((a: Asset) => a.enabled);

  log(`Loaded ${enabledAssets.length} enabled assets`, enabledAssets.map((a: Asset) => a.id));
  
  assetsCache = enabledAssets;
  return enabledAssets;
}

// =============================================================================
// Price Loading
// =============================================================================

/**
 * Load price data for a specific asset and timeframe
 */
export async function loadPrices(timeframe: Timeframe, assetId: string): Promise<PriceData> {
  const cacheKey = `${assetId}:${timeframe}`;
  
  if (priceCache.has(cacheKey)) {
    log(`Using cached prices for ${assetId} @ ${timeframe}`);
    return priceCache.get(cacheKey)!;
  }
  
  log(`Loading prices for ${assetId} @ ${timeframe}`);
  
  // For 1m, we need to load the index and potentially multiple chunks
  if (timeframe === '1m') {
    return load1mPrices(assetId);
  }

  // Add cache-busting timestamp to force fresh data after deployments
  const cacheBuster = `?v=${Date.now()}`;
  const path = `/static/${assetId}/prices_${timeframe}.json${cacheBuster}`;

  const response = await fetch(path, { cache: 'no-store' });
  
  if (!response.ok) {
    // Graceful fallback for missing timeframe data (e.g., CoinGecko assets only have 1d)
    console.warn(`[dataLoader] Missing price data: ${path} (${response.status})`);
    return {
      timeframe,
      count: 0,
      start: 0,
      end: 0,
      candles: [],
    };
  }
  
  const data: PriceData = await response.json();
  
  log(`Loaded ${data.count} candles for ${assetId} @ ${timeframe}`);
  
  priceCache.set(cacheKey, data);
  return data;
}

/**
 * Load 1m prices (chunked by month for performance)
 */
async function load1mPrices(assetId: string): Promise<PriceData> {
  const cacheKey = `${assetId}:1m`;
  
  if (priceCache.has(cacheKey)) {
    log(`Using cached 1m prices for ${assetId}`);
    return priceCache.get(cacheKey)!;
  }
  
  // Load index
  const indexPath = `/static/${assetId}/prices_1m_index.json`;
  log(`Loading 1m index from ${indexPath}`);
  
  const indexResponse = await fetch(indexPath);
  
  if (!indexResponse.ok) {
    // Graceful fallback for assets without 1m data
    console.warn(`[dataLoader] Missing 1m price index: ${indexPath} (${indexResponse.status})`);
    return {
      timeframe: '1m',
      count: 0,
      start: 0,
      end: 0,
      candles: [],
    };
  }
  
  const index = await indexResponse.json();
  
  log(`Found ${index.chunks.length} chunks for ${assetId} 1m data`);
  
  // Load all chunks in parallel
  const chunkPromises = index.chunks.map(async (chunk: { file: string }) => {
    const chunkPath = `/static/${assetId}/${chunk.file}`;
    log(`Loading chunk: ${chunkPath}`);
    
    const response = await fetch(chunkPath);
    
    if (!response.ok) {
      throw new Error(`Missing price chunk: ${chunkPath} (${response.status} ${response.statusText})`);
    }
    
    return response.json();
  });
  
  const chunks = await Promise.all(chunkPromises);
  
  // Merge all candles
  const allCandles: Candle[] = [];
  for (const chunk of chunks) {
    allCandles.push(...chunk.candles);
  }
  
  // Sort by timestamp
  allCandles.sort((a, b) => a.t - b.t);
  
  const merged: PriceData = {
    timeframe: '1m',
    count: allCandles.length,
    start: allCandles[0]?.t || 0,
    end: allCandles[allCandles.length - 1]?.t || 0,
    candles: allCandles,
  };
  
  log(`Loaded ${merged.count} total 1m candles for ${assetId}`);
  
  priceCache.set(cacheKey, merged);
  return merged;
}

// =============================================================================
// Tweet Loading
// =============================================================================

/**
 * Load tweet events data for a specific asset
 *
 * File structure:
 * - Founders: tweet_events.json (all tweets), tweet_events_filtered.json (mentions only)
 * - Adopters: tweet_events.json (filtered only, no toggle available)
 *
 * @param assetId - The asset ID to load tweets for
 * @param onlyMentions - If true, load filtered tweets (tweet_events_filtered.json for founders)
 *                       Ignored for adopters (they only have filtered tweets)
 */
export async function loadTweetEvents(assetId: string, onlyMentions: boolean = false): Promise<TweetEventsData> {
  const cacheKey = onlyMentions ? `${assetId}:filtered` : assetId;

  if (tweetCache.has(cacheKey)) {
    log(`Using cached tweet events for ${cacheKey}`);
    return tweetCache.get(cacheKey)!;
  }

  // Add cache-busting timestamp to force fresh data after deployments
  const cacheBuster = `?v=${Date.now()}`;

  // Determine which file to load:
  // - Default: tweet_events.json (all tweets for founders, filtered for adopters)
  // - onlyMentions=true: tweet_events_filtered.json (founders only)
  const filename = onlyMentions ? 'tweet_events_filtered.json' : 'tweet_events.json';
  const path = `/static/${assetId}/${filename}${cacheBuster}`;

  log(`Loading tweet events from ${path}`);

  const response = await fetch(path, { cache: 'no-store' });

  // If requesting filtered but file doesn't exist, fall back to main file
  // (This happens for adopters who don't have a separate filtered file)
  if (!response.ok && onlyMentions) {
    log(`No filtered tweets file for ${assetId}, falling back to main file`);
    return loadTweetEvents(assetId, false);
  }

  if (!response.ok) {
    throw new Error(`Missing tweet events: ${path} (${response.status} ${response.statusText})`);
  }

  const data: TweetEventsData = await response.json();

  log(`Loaded ${data.count} tweet events for ${assetId} (${onlyMentions ? 'filtered' : 'all'})`);

  tweetCache.set(cacheKey, data);
  return data;
}

/**
 * Check if an asset has a filter toggle available
 * (i.e., has a tweet_events_filtered.json file - only founders have this)
 *
 * Adopters don't have this file because we only have their filtered tweets.
 */
export async function hasFilterToggle(assetId: string): Promise<boolean> {
  const path = `/static/${assetId}/tweet_events_filtered.json`;
  try {
    const response = await fetch(path, { method: 'HEAD', cache: 'no-store' });
    return response.ok;
  } catch {
    return false;
  }
}

/**
 * @deprecated Use hasFilterToggle instead
 * Check if an asset has unfiltered tweets available
 */
export async function hasUnfilteredTweets(assetId: string): Promise<boolean> {
  // For backwards compatibility, check both old and new file names
  const oldPath = `/static/${assetId}/tweet_events_all.json`;
  const newPath = `/static/${assetId}/tweet_events_filtered.json`;
  try {
    const [oldResponse, newResponse] = await Promise.all([
      fetch(oldPath, { method: 'HEAD', cache: 'no-store' }),
      fetch(newPath, { method: 'HEAD', cache: 'no-store' }),
    ]);
    return oldResponse.ok || newResponse.ok;
  } catch {
    return false;
  }
}

// =============================================================================
// Stats Loading
// =============================================================================

/**
 * Load pre-computed statistics for a specific asset
 */
export async function loadStats(assetId: string): Promise<Stats> {
  log(`Loading stats for ${assetId}`);
  
  const path = `/static/${assetId}/stats.json`;
  const response = await fetch(path);
  
  if (!response.ok) {
    throw new Error(`Missing stats: ${path} (${response.status} ${response.statusText})`);
  }
  
  const data: Stats = await response.json();
  
  log(`Loaded stats for ${assetId}`);
  
  return data;
}

// =============================================================================
// Utility Functions
// =============================================================================

/**
 * Convert price data to Lightweight Charts candlestick format
 */
export function toCandlestickData(prices: PriceData) {
  return prices.candles.map(c => ({
    time: c.t as number,
    open: c.o,
    high: c.h,
    low: c.l,
    close: c.c,
  }));
}

/**
 * Get sorted array of tweet timestamps for binary search
 */
export function getSortedTweetTimestamps(tweets: TweetEvent[]): number[] {
  return tweets
    .filter(t => t.price_at_tweet !== null)
    .map(t => t.timestamp)
    .sort((a, b) => a - b);
}

/**
 * Clear all caches (useful for testing/debugging)
 */
export function clearCaches() {
  log('Clearing all caches');
  priceCache.clear();
  tweetCache.clear();
  assetsCache = null;
}

/**
 * Load last updated timestamp (set by hourly update workflow)
 * Returns null if not available (first deploy or local dev)
 */
export async function loadLastUpdated(): Promise<string | null> {
  try {
    const response = await fetch('/static/last_updated.json');
    if (!response.ok) return null;
    const data = await response.json();
    return data.timestamp || null;
  } catch {
    return null;
  }
}
