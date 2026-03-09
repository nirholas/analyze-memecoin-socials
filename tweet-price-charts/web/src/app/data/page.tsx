'use client';

/**
 * Data Page - Primitive Table View
 * =================================
 * Clean, screenshot-friendly tweet analysis table.
 *
 * Design philosophy: The insight emerges from LOOKING at the data,
 * not from reading annotations. One summary line + one clean table.
 */

import { Suspense, useEffect, useState, useCallback, useMemo } from 'react';
import { useSearchParams, useRouter } from 'next/navigation';
import Link from 'next/link';
import { loadTweetEvents, loadAssets, hasFilterToggle } from '@/lib/dataLoader';
import { TweetEvent, Asset } from '@/lib/types';
import DataTable from '@/components/DataTable';
import AssetSelector from '@/components/AssetSelector';

// ============================================================================
// LOADING STATE
// ============================================================================

function DataPageLoading() {
  return (
    <div className="min-h-screen bg-[var(--surface-0)] flex items-center justify-center">
      <div className="text-center">
        <div className="animate-spin w-8 h-8 border-2 border-[var(--accent)] border-t-transparent rounded-full mx-auto mb-4"></div>
        <p className="text-[var(--text-muted)]">Loading...</p>
      </div>
    </div>
  );
}

// ============================================================================
// SUMMARY LINE COMPONENT
// ============================================================================

/**
 * One-line summary that answers the key questions:
 * - How many tweets?
 * - How often green?
 * - What's the average outcome?
 * - How stale is the account?
 */
function SummaryLine({ events, founder }: { events: TweetEvent[]; founder: string }) {
  const stats = useMemo(() => {
    const withPrice = events.filter(e => e.change_24h_pct !== null);
    if (withPrice.length === 0) {
      return { count: events.length, winRate: 0, avgReturn: 0, daysSince: null };
    }

    const returns = withPrice.map(e => e.change_24h_pct!);
    const wins = returns.filter(r => r > 0).length;
    const winRate = Math.round((wins / returns.length) * 100);
    const avgReturn = Math.round(returns.reduce((a, b) => a + b, 0) / returns.length * 10) / 10;

    // Days since last tweet
    const lastTimestamp = Math.max(...events.map(e => e.timestamp));
    const daysSince = Math.floor((Date.now() / 1000 - lastTimestamp) / 86400);

    return { count: events.length, winRate, avgReturn, daysSince };
  }, [events]);

  const avgFormatted = stats.avgReturn >= 0 ? `+${stats.avgReturn}%` : `${stats.avgReturn}%`;

  return (
    <div className="text-sm text-[var(--text-secondary)] px-4 py-3 border-b border-[var(--border-subtle)]">
      <span className="text-[var(--text-primary)] font-medium">@{founder}</span>
      {' · '}
      <span className="tabular-nums">{stats.count} tweets</span>
      {' · '}
      <span className="tabular-nums">{stats.winRate}% up next day</span>
      {' · '}
      <span className={`tabular-nums ${stats.avgReturn >= 0 ? 'text-[var(--positive)]' : 'text-[var(--negative)]'}`}>
        {avgFormatted}
      </span>
      <span> avg</span>
      {stats.daysSince !== null && (
        <>
          {' · '}
          <span className="text-[var(--text-muted)]">last tweet: {stats.daysSince}d ago</span>
        </>
      )}
    </div>
  );
}

// ============================================================================
// MAIN DATA PAGE CONTENT
// ============================================================================

function DataPageContent() {
  const searchParams = useSearchParams();
  const router = useRouter();

  const [assets, setAssets] = useState<Asset[]>([]);
  const [selectedAsset, setSelectedAsset] = useState<Asset | null>(null);
  const [tweetEvents, setTweetEvents] = useState<TweetEvent[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [onlyMentions, setOnlyMentions] = useState(false);
  const [hasFilteredTweets, setHasFilteredTweets] = useState(false);

  const assetId = searchParams.get('asset') || 'pump';

  useEffect(() => {
    async function init() {
      try {
        const loadedAssets = await loadAssets();
        setAssets(loadedAssets);

        const asset = loadedAssets.find((a) => a.id === assetId);
        if (!asset) {
          throw new Error(`Invalid asset: "${assetId}"`);
        }

        setSelectedAsset(asset);

        // Check if asset has filter toggle available (founders only)
        const hasToggle = await hasFilterToggle(assetId);
        setHasFilteredTweets(hasToggle);

        // Load default tweets (all for founders, filtered for adopters)
        const eventsData = await loadTweetEvents(assetId, false);
        setTweetEvents(eventsData.events);

        // Reset filter when switching assets (default: show all)
        setOnlyMentions(false);
      } catch (err) {
        setError(err instanceof Error ? err.message : String(err));
      }
      setLoading(false);
    }
    init();
  }, [assetId]);

  const handleAssetSelect = useCallback(
    (asset: Asset) => {
      router.push(`/data?asset=${asset.id}`);
    },
    [router]
  );

  // Handle "Only mentions" toggle
  const handleOnlyMentionsToggle = useCallback(async () => {
    if (!selectedAsset) return;

    const newValue = !onlyMentions;
    setOnlyMentions(newValue);

    // Reload tweets: onlyMentions=true → filtered, false → all
    const eventsData = await loadTweetEvents(selectedAsset.id, newValue);
    setTweetEvents(eventsData.events);
  }, [selectedAsset, onlyMentions]);

  // Error state
  if (error) {
    return (
      <div className="min-h-screen bg-[var(--surface-0)] flex items-center justify-center p-8">
        <div className="max-w-lg w-full bg-[var(--negative-muted)]/30 border border-[var(--negative)]/50 rounded-xl p-6">
          <h2 className="text-[var(--negative)] font-bold text-lg mb-2">Error</h2>
          <pre className="text-[var(--negative)]/80 text-sm whitespace-pre-wrap font-mono">{error}</pre>
          <Link
            href="/data?asset=pump"
            className="inline-block mt-4 px-4 py-2 bg-[var(--negative)]/20 hover:bg-[var(--negative)]/30 text-[var(--negative)] rounded-lg transition-colors"
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
      <div className="min-h-screen bg-[var(--surface-0)] flex items-center justify-center">
        <div className="text-center">
          <div className="animate-spin w-8 h-8 border-2 border-[var(--accent)] border-t-transparent rounded-full mx-auto mb-4"></div>
          <p className="text-[var(--text-muted)]">Loading {assetId}...</p>
        </div>
      </div>
    );
  }

  return (
    <div className="min-h-screen bg-[var(--surface-0)] flex flex-col">
      {/* Minimal header - just asset selector + nav */}
      <header className="h-14 md:h-11 border-b border-[var(--border-subtle)] bg-[var(--surface-1)] flex items-center px-3 gap-3">
        <AssetSelector
          assets={assets}
          selectedAsset={selectedAsset}
          onSelect={handleAssetSelect}
        />

        {/* Spacer */}
        <div className="flex-1" />

        {/* Navigation */}
        <Link
          href={`/chart?asset=${selectedAsset.id}`}
          className="px-3 py-1.5 text-xs font-medium text-[var(--text-secondary)] hover:text-[var(--text-primary)] rounded-md transition-colors"
        >
          Chart
        </Link>
        <Link
          href="/about"
          className="px-3 py-1.5 text-xs font-medium text-[var(--text-secondary)] hover:text-[var(--text-primary)] rounded-md transition-colors"
        >
          About
        </Link>
        <a
          href={`https://twitter.com/${selectedAsset.founder}`}
          target="_blank"
          rel="noopener noreferrer"
          className="hidden md:block text-xs text-[var(--text-muted)] hover:text-[var(--text-primary)] transition-colors"
        >
          @{selectedAsset.founder}
        </a>
      </header>

      {/* Main content */}
      <main className="flex-1">
        <div className="max-w-4xl mx-auto">
          {/* Summary line */}
          <SummaryLine events={tweetEvents} founder={selectedAsset.founder} />

          {/* The table */}
          <DataTable
            events={tweetEvents}
            founder={selectedAsset.founder}
            assetName={selectedAsset.name}
            showOnlyMentionsToggle={hasFilteredTweets}
            onlyMentions={onlyMentions}
            onOnlyMentionsChange={handleOnlyMentionsToggle}
          />
        </div>
      </main>

      {/* Minimal footer */}
      <footer className="py-3 text-center text-xs text-[var(--text-muted)] border-t border-[var(--border-subtle)] pb-safe">
        X + GeckoTerminal · Not financial advice
      </footer>
    </div>
  );
}

// ============================================================================
// EXPORT
// ============================================================================

export default function DataPage() {
  return (
    <Suspense fallback={<DataPageLoading />}>
      <DataPageContent />
    </Suspense>
  );
}
