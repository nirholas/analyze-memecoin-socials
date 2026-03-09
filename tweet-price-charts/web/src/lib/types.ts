// =============================================================================
// Price Data Types
// =============================================================================

/** Compact OHLCV candle format from static JSON files */
export interface Candle {
  t: number;  // timestamp (unix epoch seconds)
  o: number;  // open price
  h: number;  // high price
  l: number;  // low price
  c: number;  // close price
  v: number;  // volume
}

/** Price data container with metadata */
export interface PriceData {
  timeframe: string;
  count: number;
  start: number;
  end: number;
  candles: Candle[];
}

// =============================================================================
// Tweet Event Types
// =============================================================================

/** Single tweet event with price impact data */
export interface TweetEvent {
  tweet_id: string;
  timestamp: number;
  timestamp_iso: string;
  text: string;
  price_at_tweet: number | null;
  price_1h: number | null;
  price_24h: number | null;
  change_1h_pct: number | null;
  change_24h_pct: number | null;
  /** Market cap at tweet time (price × circulating supply) */
  market_cap_at_tweet: number | null;
  likes: number;
  retweets: number;
  impressions: number;
}

/** Container for tweet events data */
export interface TweetEventsData {
  generated_at: string;
  price_definition: string;
  count: number;
  events: TweetEvent[];
  /** "founder" (default) or "adopter" for traders who adopted the coin */
  founder_type?: 'founder' | 'adopter';
  /** Keyword filter applied to tweets (e.g., "useless") */
  keyword_filter?: string;
  /** Human-readable note explaining the filter (e.g., "Only tweets mentioning $USELESS") */
  tweet_filter_note?: string;
}

// =============================================================================
// Statistics Types
// =============================================================================

/** Pre-computed statistics for the stats panel */
export interface Stats {
  generated_at: string;
  summary: {
    total_tweets: number;
    tweets_with_price: number;
    date_range: {
      start: string;
      end: string;
    };
  };
  daily_comparison: {
    tweet_day_count: number;
    tweet_day_avg_return: number;
    tweet_day_win_rate: number;
    no_tweet_day_count: number;
    no_tweet_day_avg_return: number;
    no_tweet_day_win_rate: number;
    t_statistic: number | null;
    p_value: number | null;
    significant: boolean;
  };
  correlation: {
    correlation_7d: number;
    p_value: number;
    significant: boolean;
    sample_size: number;
  };
  current_status: {
    days_since_last_tweet: number;
    price_change_during_silence: number | null;
    last_tweet_date: string | null;
  };
  quiet_periods: Array<{
    start_ts: number;
    end_ts: number;
    gap_days: number;
    start_date: string;
    end_date: string;
    price_start: number | null;
    price_end: number | null;
    change_pct: number | null;
    is_current?: boolean;
  }>;
}

// =============================================================================
// Chart Types
// =============================================================================

/** Available timeframe options for price data */
export type Timeframe = '1m' | '15m' | '1h' | '1d';

// =============================================================================
// Asset Types
// =============================================================================

/** Single asset configuration */
export interface Asset {
  id: string;
  name: string;
  founder: string;
  network: string | null;
  color: string;
  logo?: string;  // Token logo path (e.g., "/logos/pump.png")
  launch_date: string;
  enabled: boolean;
  /** Optional note about data quality/coverage limitations */
  data_note?: string;
}

/** Container for assets data */
export interface AssetsData {
  version: string;
  assets: Asset[];
}
