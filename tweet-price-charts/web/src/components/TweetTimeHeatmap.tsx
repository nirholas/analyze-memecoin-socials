/**
 * TweetTimeHeatmap Component
 * ==========================
 *
 * Horizontal heatmap showing when each founder tweets by hour of day (UTC).
 * Part of the "Context (optional)" section - helps interpret founder behavior
 * but not core to the Tool contract.
 *
 * Features:
 * - 24-hour grid per founder
 * - Blue intensity indicates tweet frequency at each hour
 * - Sorted by total tweet count (most active first)
 * - Shows top 10 founders
 *
 * Data source: Extracts hour from timestamp_iso in tweet_events.json
 */
'use client';

import { useState, useEffect, useMemo } from 'react';
import { Asset } from '@/lib/types';
import { loadAssets, loadTweetEvents } from '@/lib/dataLoader';

// =============================================================================
// Types
// =============================================================================

interface FounderData {
  founder: string;
  asset: Asset;
  hourCounts: number[];
  totalTweets: number;
}

// =============================================================================
// Component
// =============================================================================

export default function TweetTimeHeatmap() {
  const [founders, setFounders] = useState<FounderData[]>([]);
  const [loading, setLoading] = useState(true);

  // ---------------------------------------------------------------------------
  // Data Loading
  // ---------------------------------------------------------------------------
  useEffect(() => {
    async function loadData() {
      try {
        setLoading(true);
        const assets = await loadAssets();
        const founderDataList: FounderData[] = [];

        await Promise.all(
          assets.map(async (asset) => {
            try {
              const tweetsData = await loadTweetEvents(asset.id);

              // Count tweets per hour (0-23)
              const hourCounts = new Array(24).fill(0);

              tweetsData.events.forEach((event) => {
                if (event.timestamp_iso) {
                  const date = new Date(event.timestamp_iso);
                  const hour = date.getUTCHours();
                  hourCounts[hour]++;
                }
              });

              if (tweetsData.events.length > 0) {
                founderDataList.push({
                  founder: asset.founder,
                  asset,
                  hourCounts,
                  totalTweets: tweetsData.events.length,
                });
              }
            } catch (e) {
              console.warn(`Failed to load tweets for ${asset.id}:`, e);
            }
          })
        );

        // Sort by total tweets (most active first)
        founderDataList.sort((a, b) => b.totalTweets - a.totalTweets);
        setFounders(founderDataList);
      } catch (e) {
        console.error('Failed to load data:', e);
      } finally {
        setLoading(false);
      }
    }

    loadData();
  }, []);

  // ---------------------------------------------------------------------------
  // Render
  // ---------------------------------------------------------------------------
  if (loading) {
    return (
      <div className="bg-[var(--surface-1)] border border-[var(--border-subtle)] rounded-xl p-6">
        <div className="animate-pulse space-y-3">
          <div className="h-4 bg-[var(--surface-2)] rounded w-1/4"></div>
          <div className="h-[200px] bg-[var(--surface-2)] rounded"></div>
        </div>
      </div>
    );
  }

  return (
    <div className="bg-[var(--surface-1)] border border-[var(--border-subtle)] rounded-xl overflow-hidden">
      {/* Header */}
      <div className="p-4 border-b border-[var(--border-subtle)]">
        <h3 className="text-lg font-semibold text-[var(--text-primary)] mb-1">
          When they tweet
        </h3>
        <p className="text-sm text-[var(--text-secondary)]">
          Some founders tweet at 3am. Some only during market hours. See their patterns.
        </p>
      </div>

      {/* Heatmap */}
      <div className="p-4 overflow-x-auto">
        {/* Hour labels */}
        <div className="flex mb-2 ml-[140px]">
          {[0, 6, 12, 18].map((hour) => (
            <div
              key={hour}
              className="text-xs text-[var(--text-muted)]"
              style={{ width: '25%' }}
            >
              {hour.toString().padStart(2, '0')}:00
            </div>
          ))}
        </div>

        {/* Founder rows */}
        <div className="space-y-1">
          {founders.slice(0, 10).map((founder) => {
            const maxCount = Math.max(...founder.hourCounts);

            return (
              <div key={founder.asset.id} className="flex items-center gap-3">
                {/* Founder name */}
                <div className="w-[130px] flex-shrink-0 flex items-center gap-2">
                  <div
                    className="w-2 h-2 rounded-full flex-shrink-0"
                    style={{ backgroundColor: founder.asset.color }}
                  />
                  <span className="text-sm text-[var(--text-primary)] truncate">
                    {founder.founder}
                  </span>
                </div>

                {/* Hour cells */}
                <div className="flex flex-1 gap-[2px]">
                  {founder.hourCounts.map((count, hour) => {
                    const intensity = maxCount > 0 ? count / maxCount : 0;

                    return (
                      <div
                        key={hour}
                        className="flex-1 h-6 rounded-sm transition-colors"
                        style={{
                          backgroundColor: intensity > 0
                            ? `rgba(59, 130, 246, ${0.15 + intensity * 0.7})`
                            : 'var(--surface-2)',
                        }}
                        title={`${hour.toString().padStart(2, '0')}:00 UTC: ${count} tweets`}
                      />
                    );
                  })}
                </div>

                {/* Tweet count */}
                <div className="w-12 text-right text-xs text-[var(--text-muted)] tabular-nums">
                  {founder.totalTweets}
                </div>
              </div>
            );
          })}
        </div>

        {/* Legend */}
        <div className="mt-4 pt-3 border-t border-[var(--border-subtle)] flex items-center justify-between">
          <div className="flex items-center gap-2 text-xs text-[var(--text-muted)]">
            <span>Less</span>
            <div className="flex gap-[2px]">
              {[0.15, 0.35, 0.55, 0.75, 0.85].map((opacity, i) => (
                <div
                  key={i}
                  className="w-3 h-3 rounded-sm"
                  style={{ backgroundColor: `rgba(59, 130, 246, ${opacity})` }}
                />
              ))}
            </div>
            <span>More</span>
          </div>
          <div className="text-xs text-[var(--text-muted)]">
            Times in UTC
          </div>
        </div>
      </div>
    </div>
  );
}
