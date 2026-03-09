/**
 * SilencesExplorer Component
 * ==========================
 *
 * Shows gaps in founder tweeting activity with price context.
 * Part of the "Context (optional)" section - interesting pattern
 * but not core to the Tool contract.
 *
 * Features:
 * - Lists top 8 longest tweet gaps (7+ days)
 * - Shows price change during each silence period
 * - Links to chart for each token
 * - Sorted by gap length (longest first)
 *
 * Data source: quiet_periods array from stats.json (pre-computed)
 */
'use client';

import { useState, useEffect } from 'react';
import { Asset, Stats } from '@/lib/types';
import { loadAssets, loadStats } from '@/lib/dataLoader';
import Link from 'next/link';

// Human-readable date formatter
const formatDate = (dateStr: string): string => {
  const date = new Date(dateStr);
  return date.toLocaleDateString('en-US', {
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  });
};

// =============================================================================
// Types
// =============================================================================

interface Silence {
  asset: Asset;
  founder: string;
  gapDays: number;
  startDate: string;
  endDate: string;
  priceChange: number | null;
}

// =============================================================================
// Component
// =============================================================================

export default function SilencesExplorer() {
  const [silences, setSilences] = useState<Silence[]>([]);
  const [loading, setLoading] = useState(true);

  // ---------------------------------------------------------------------------
  // Data Loading
  // ---------------------------------------------------------------------------
  useEffect(() => {
    async function loadData() {
      try {
        setLoading(true);
        const assets = await loadAssets();
        const allSilences: Silence[] = [];

        await Promise.all(
          assets.map(async (asset) => {
            try {
              const stats = await loadStats(asset.id);

              // Get notable gaps (7+ days, not current)
              const quietPeriods = stats.quiet_periods || [];
              quietPeriods
                .filter((p) => p.gap_days >= 7 && !p.is_current)
                .forEach((period) => {
                  allSilences.push({
                    asset,
                    founder: asset.founder,
                    gapDays: period.gap_days,
                    startDate: period.start_date,
                    endDate: period.end_date,
                    priceChange: period.change_pct,
                  });
                });
            } catch (e) {
              // Stats might not exist for all assets
              console.warn(`No stats for ${asset.id}`);
            }
          })
        );

        // Sort by gap length (longest first)
        allSilences.sort((a, b) => b.gapDays - a.gapDays);
        setSilences(allSilences);
      } catch (e) {
        console.error('Failed to load silences:', e);
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
          <div className="space-y-2">
            {[1, 2, 3].map((i) => (
              <div key={i} className="h-16 bg-[var(--surface-2)] rounded"></div>
            ))}
          </div>
        </div>
      </div>
    );
  }

  // Show top 8 silences
  const topSilences = silences.slice(0, 8);

  return (
    <div className="bg-[var(--surface-1)] border border-[var(--border-subtle)] rounded-xl overflow-hidden">
      {/* Header */}
      <div className="p-4 border-b border-[var(--border-subtle)]">
        <h3 className="text-lg font-semibold text-[var(--text-primary)] mb-1">
          When they go quiet
        </h3>
        <p className="text-sm text-[var(--text-secondary)]">
          Longest gaps between tweets. What happened to price while they were silent?
        </p>
      </div>

      {/* Silences list */}
      <div className="divide-y divide-[var(--border-subtle)]">
        {topSilences.map((silence, i) => (
          <Link
            key={`${silence.asset.id}-${silence.startDate}`}
            href={`/chart?asset=${silence.asset.id}`}
            className="block p-4 hover:bg-[var(--surface-2)] transition-colors"
          >
            <div className="flex items-start justify-between gap-4">
              {/* Left side - founder info */}
              <div className="flex items-center gap-3">
                <div
                  className="w-3 h-3 rounded-full flex-shrink-0"
                  style={{ backgroundColor: silence.asset.color }}
                />
                <div>
                  <div className="flex items-center gap-2">
                    <span className="text-[var(--text-primary)] font-medium">
                      {silence.founder}
                    </span>
                    <span className="text-xs text-[var(--text-muted)]">
                      {silence.asset.name}
                    </span>
                  </div>
                  <div className="text-sm text-[var(--text-secondary)]">
                    {formatDate(silence.startDate)} → {formatDate(silence.endDate)}
                  </div>
                </div>
              </div>

              {/* Right side - gap and price */}
              <div className="text-right flex-shrink-0">
                <div className="text-lg font-semibold text-[var(--text-primary)] tabular-nums">
                  {Math.round(silence.gapDays)} days
                </div>
                {silence.priceChange !== null && (
                  <div
                    className={`text-sm tabular-nums ${
                      silence.priceChange >= 0
                        ? 'text-[var(--positive)]'
                        : 'text-[var(--negative)]'
                    }`}
                  >
                    {silence.priceChange >= 0 ? '+' : ''}
                    {silence.priceChange.toFixed(0)}% price
                  </div>
                )}
              </div>
            </div>
          </Link>
        ))}
      </div>

      {/* Footer */}
      {silences.length > 8 && (
        <div className="p-3 border-t border-[var(--border-subtle)] bg-[var(--surface-0)]">
          <div className="text-center text-sm text-[var(--text-muted)]">
            {silences.length} gaps of 7+ days across all founders
          </div>
        </div>
      )}
    </div>
  );
}
