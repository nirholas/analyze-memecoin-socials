'use client';

import { Suspense, useEffect, useState, useCallback } from 'react';
import { useSearchParams, useRouter } from 'next/navigation';
import dynamic from 'next/dynamic';
import Link from 'next/link';
import { loadTweetEvents, loadAssets, loadLastUpdated, hasFilterToggle } from '@/lib/dataLoader';
import { TweetEvent, TweetEventsData, Asset } from '@/lib/types';
import AssetSelector from '@/components/AssetSelector';
import OnlyMentionsToggle from '@/components/OnlyMentionsToggle';

const Chart = dynamic(() => import('@/components/Chart'), { 
  ssr: false,
  loading: () => (
    <div className="flex items-center justify-center h-full bg-[var(--surface-0)]">
      <div className="text-[var(--text-muted)]">Loading chart...</div>
    </div>
  ),
});

/**
 * Format ISO timestamp to relative time (e.g., "2h ago")
 */
function formatRelativeTime(isoTimestamp: string): string {
  const date = new Date(isoTimestamp);
  const now = new Date();
  const diffMs = now.getTime() - date.getTime();
  const diffMins = Math.floor(diffMs / (1000 * 60));

  if (diffMins < 1) return 'just now';
  if (diffMins < 60) return `${diffMins}m ago`;

  const diffHours = Math.floor(diffMins / 60);
  if (diffHours < 24) return `${diffHours}h ago`;

  const diffDays = Math.floor(diffHours / 24);
  return `${diffDays}d ago`;
}

/**
 * Avatar component with fallback to colored circle
 */
function FounderAvatar({ founder, color }: { founder: string; color: string }) {
  const [imgError, setImgError] = useState(false);
  
  if (imgError) {
    console.warn(`[ChartPage] Missing avatar for ${founder}`);
    return (
      <div 
        className="w-5 h-5 rounded-full flex items-center justify-center text-white text-xs font-bold"
        style={{ backgroundColor: color }}
      >
        {founder.charAt(0).toUpperCase()}
      </div>
    );
  }
  
  return (
    <img 
      src={`/avatars/${founder}.png`} 
      alt={founder} 
      className="w-5 h-5 rounded-full"
      onError={() => setImgError(true)}
    />
  );
}

/**
 * Loading fallback for Suspense
 */
function ChartPageLoading() {
  return (
    <div className="h-dvh md:h-screen bg-[var(--surface-0)] flex items-center justify-center">
      <div className="text-center">
        <div className="animate-spin w-8 h-8 border-2 border-[var(--accent)] border-t-transparent rounded-full mx-auto mb-4"></div>
        <p className="text-[var(--text-muted)]">Loading...</p>
      </div>
    </div>
  );
}

/**
 * Main chart page content (uses useSearchParams)
 */
function ChartPageContent() {
  const searchParams = useSearchParams();
  const router = useRouter();
  
  const [assets, setAssets] = useState<Asset[]>([]);
  const [selectedAsset, setSelectedAsset] = useState<Asset | null>(null);
  const [tweetEvents, setTweetEvents] = useState<TweetEvent[]>([]);
  const [eventsMetadata, setEventsMetadata] = useState<Partial<TweetEventsData>>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<string | null>(null);
  const [onlyMentions, setOnlyMentions] = useState(false);
  const [hasFilteredTweets, setHasFilteredTweets] = useState(false);

  // Get asset ID from URL, default to 'pump'
  const assetId = searchParams.get('asset') || 'pump';

  // Load assets on mount
  useEffect(() => {
    async function init() {
      console.log(`[ChartPage] Initializing with asset: ${assetId}`);

      try {
        const loadedAssets = await loadAssets();
        setAssets(loadedAssets);

        // Validate asset exists
        const asset = loadedAssets.find(a => a.id === assetId);
        if (!asset) {
          throw new Error(
            `Invalid asset: "${assetId}". Valid assets: ${loadedAssets.map(a => a.id).join(', ')}`
          );
        }

        console.log(`[ChartPage] Selected asset: ${asset.name} (${asset.id})`);
        setSelectedAsset(asset);

        // Check if asset has filter toggle available (founders with keyword_filter)
        // Adopters don't have this - they only have filtered tweets
        const hasToggle = await hasFilterToggle(assetId);
        setHasFilteredTweets(hasToggle);

        // Load tweet events (default: all tweets for founders, filtered for adopters)
        const eventsData = await loadTweetEvents(assetId, false);
        setTweetEvents(eventsData.events);
        setEventsMetadata({
          founder_type: eventsData.founder_type,
          keyword_filter: eventsData.keyword_filter,
          tweet_filter_note: eventsData.tweet_filter_note,
        });

        console.log(`[ChartPage] Loaded ${eventsData.events.length} tweets for ${asset.name}`);
        if (eventsData.keyword_filter) {
          console.log(`[ChartPage] Keyword filter: "${eventsData.keyword_filter}" (toggle available: ${hasToggle})`);
        }

        // Load last updated timestamp
        const updated = await loadLastUpdated();
        setLastUpdated(updated);

        // Reset onlyMentions to false when switching assets (show all tweets by default)
        setOnlyMentions(false);

      } catch (err) {
        const message = err instanceof Error ? err.message : String(err);
        console.error(`[ChartPage] Error: ${message}`);
        setError(message);
      }

      setLoading(false);
    }

    init();
  }, [assetId]);

  // Handle asset selection
  const handleAssetSelect = useCallback((asset: Asset) => {
    console.log(`[ChartPage] Switching to asset: ${asset.id}`);
    router.push(`/chart?asset=${asset.id}`);
  }, [router]);

  // Handle "Only mentions" toggle
  const handleOnlyMentionsToggle = useCallback(async () => {
    if (!selectedAsset) return;

    const newValue = !onlyMentions;
    setOnlyMentions(newValue);

    console.log(`[ChartPage] Only mentions: ${newValue ? 'ON' : 'OFF'}`);

    // Reload tweets with the new filter setting
    // onlyMentions=true → load tweet_events_filtered.json (mentions only)
    // onlyMentions=false → load tweet_events.json (all tweets for founders)
    const eventsData = await loadTweetEvents(selectedAsset.id, newValue);
    setTweetEvents(eventsData.events);
    console.log(`[ChartPage] Reloaded ${eventsData.events.length} tweets`);
  }, [selectedAsset, onlyMentions]);

  // Error state
  if (error) {
    return (
      <div className="h-dvh md:h-screen bg-[var(--surface-0)] flex items-center justify-center p-8">
        <div className="max-w-lg w-full bg-[var(--negative-muted)]/30 border border-[var(--negative)]/50 rounded-xl p-6">
          <h2 className="text-[var(--negative)] font-bold text-lg mb-2">Data Error</h2>
          <pre className="text-[var(--negative)]/80 text-sm whitespace-pre-wrap font-mono">
            {error}
          </pre>
          <Link 
            href="/chart?asset=pump"
            className="inline-block mt-4 px-4 py-2 bg-[var(--negative)]/20 hover:bg-[var(--negative)]/30 text-[var(--negative)] rounded-lg transition-colors interactive"
          >
            Go to PUMP
          </Link>
        </div>
      </div>
    );
  }

  // Loading state
  if (loading || !selectedAsset) {
    return (
      <div className="h-dvh md:h-screen bg-[var(--surface-0)] flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin w-8 h-8 border-2 border-[var(--accent)] border-t-transparent rounded-full mx-auto mb-4"></div>
          <p className="text-[var(--text-muted)]">Loading {assetId}...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="h-dvh md:h-screen bg-[var(--surface-0)] flex flex-col">
      {/* Top toolbar */}
      <div className="h-14 md:h-11 bg-[var(--surface-1)] border-b border-[var(--border-subtle)] flex items-center px-3 md:px-3 gap-3">
        {/* Asset selector - always visible */}
        <AssetSelector 
          assets={assets}
          selectedAsset={selectedAsset}
          onSelect={handleAssetSelect}
        />
        
        {/* Network badge - desktop only */}
        <span className="text-[var(--text-muted)] text-xs hidden md:inline">/USD</span>
        
        {/* Navigation - desktop only */}
        <div className="hidden md:flex items-center gap-1 ml-auto">
          <Link
            href={`/chart?asset=${selectedAsset.id}`}
            className="px-3 py-2 min-h-[44px] flex items-center text-xs font-medium bg-[var(--accent)] text-white rounded-md interactive"
          >
            Chart
          </Link>
          <Link
            href={`/data?asset=${selectedAsset.id}`}
            className="px-3 py-2 min-h-[44px] flex items-center text-xs font-medium bg-[var(--surface-2)] text-[var(--text-secondary)] hover:text-[var(--text-primary)] rounded-md interactive"
          >
            Data Table
          </Link>
          <Link
            href="/about"
            className="px-3 py-2 min-h-[44px] flex items-center text-xs font-medium bg-[var(--surface-2)] text-[var(--text-secondary)] hover:text-[var(--text-primary)] rounded-md interactive"
          >
            About
          </Link>
        </div>

        {/* Mobile: Data + About links */}
        <div className="md:hidden ml-auto flex items-center gap-1">
          <Link
            href={`/data?asset=${selectedAsset.id}`}
            className="px-3 py-2.5 min-h-[44px] flex items-center text-sm text-[var(--text-secondary)] interactive rounded-md"
          >
            Data
          </Link>
          <Link
            href="/about"
            className="px-3 py-2.5 min-h-[44px] flex items-center text-sm text-[var(--text-secondary)] interactive rounded-md"
          >
            About
          </Link>
        </div>
      </div>

      {/* Chart area */}
      <div className="flex-1 relative">
        <Chart key={selectedAsset.id} tweetEvents={tweetEvents} asset={selectedAsset} />
      </div>

      {/* Bottom bar - visible on all devices */}
      <div className="flex flex-col md:flex-row h-auto md:h-9 bg-[var(--surface-1)] border-t border-[var(--border-subtle)] px-4 py-2 md:py-0 md:items-center justify-between gap-2 md:gap-0">
        <div className="flex items-center gap-4">
          <a
            href={`https://twitter.com/${selectedAsset.founder}`}
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-2 text-[var(--text-muted)] hover:text-[var(--text-primary)] text-xs transition-colors"
          >
            <FounderAvatar founder={selectedAsset.founder} color={selectedAsset.color} />
            <span>@{selectedAsset.founder}</span>
          </a>
          {/* "Only mentions" checkbox - only shown for assets with keyword filter */}
          {hasFilteredTweets && (
            <OnlyMentionsToggle
              checked={onlyMentions}
              onChange={handleOnlyMentionsToggle}
            />
          )}
          {/* Data quality note */}
          {selectedAsset.data_note && (
            <span
              className="badge badge-warning"
              title={selectedAsset.data_note}
            >
              ⚠️ Limited data
            </span>
          )}
        </div>
        <div className="text-[var(--text-muted)] text-xs tabular-nums">
          {tweetEvents.length} tweets • X + GeckoTerminal
          {lastUpdated && (
            <span title={`Last updated: ${new Date(lastUpdated).toLocaleString()}`}>
              {' '}• {formatRelativeTime(lastUpdated)}
            </span>
          )}
        </div>
      </div>
    </div>
  );
}

/**
 * Chart page wrapped in Suspense for useSearchParams
 */
export default function ChartPage() {
  return (
    <Suspense fallback={<ChartPageLoading />}>
      <ChartPageContent />
    </Suspense>
  );
}
